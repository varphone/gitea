// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gitea.dev/modules/json"
	"gitea.dev/modules/log"
	"gitea.dev/modules/setting"
)

type syncJobRequest struct {
	Kind        string `json:"kind"`
	BaseJobID   string `json:"base_job_id,omitempty"`
	ResumeJobID string `json:"resume_job_id,omitempty"`
	RequestID   string `json:"request_id,omitempty"`
}

const (
	syncJobsPath             = "/api/v1/replication/sync-jobs"
	maxSyncJobRequestSize    = 1 << 20
	maxConcurrentChunkServes = 8
	minChunkCompressionSave  = 5
)

func validReplicationRequestID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, char := range id {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

type Snapshot struct {
	ID        string    `json:"id"`
	State     string    `json:"state"`
	CreatedAt time.Time `json:"created_at"`
	Size      int64     `json:"size,omitempty"`
	SHA256    string    `json:"sha256,omitempty"`
	Error     string    `json:"error,omitempty"`
	RootMode  uint32    `json:"root_mode"`
	RequestID string    `json:"-"`
	BaseJobID string    `json:"-"`
}

const snapshotStateCreating = "creating"

type controlServer struct {
	cfg                    *config
	mu                     sync.RWMutex
	chunkMu                sync.RWMutex
	jobs                   map[string]*Snapshot
	taskManifests          map[string]*SnapshotManifest
	taskChunkIndexes       map[string]map[string]chunkLocation
	busy                   bool
	primaryRecoveryPending bool
	session                *finalSyncSession
	dataRoot               string
	chunkSlots             chan struct{}
}

func (s *controlServer) root() string {
	if s.dataRoot != "" {
		return s.dataRoot
	}
	return setting.AppWorkPath
}

func ServeControl(ctx context.Context) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if len(cfg.ControlToken) < 32 {
		return errors.New("[replicate] CONTROL_TOKEN must contain at least 32 bytes")
	}
	if !setting.Database.Type.IsSQLite3() {
		return fmt.Errorf("disaster-recovery snapshots currently require sqlite3, got %s", setting.Database.Type)
	}
	if err := os.MkdirAll(cfg.SnapshotDir, 0o700); err != nil {
		return err
	}
	if err := validateAtomicLayout(cfg.SnapshotDir); err != nil {
		return err
	}
	jobs, err := loadManifests(cfg.SnapshotDir, cfg.ControlToken)
	if err != nil {
		return err
	}
	s := &controlServer{
		cfg: cfg, jobs: jobs, taskManifests: map[string]*SnapshotManifest{},
		taskChunkIndexes: map[string]map[string]chunkLocation{}, chunkSlots: make(chan struct{}, maxConcurrentChunkServes),
	}
	removeLegacyArchives(cfg.SnapshotDir)
	primaryRecoveryRequired := false
	for id, job := range jobs {
		switch job.State {
		case "ready", "preflight":
			manifest, err := loadTrustedManifest(manifestPath(cfg.SnapshotDir, id), cfg.ControlToken, job.State)
			if err != nil {
				log.Warn("Discard unavailable persisted replication job %s: %v", id, err)
				delete(jobs, id)
				continue
			}
			s.taskManifests[id] = manifest
			s.taskChunkIndexes[id] = indexManifest(manifest)
			jobs[id] = job
		case "transferring":
			primaryRecoveryRequired = true
			// A control-plane restart releases the in-memory session and its write
			// fence. Never resume such a manifest: the primary may already accept
			// writes again, so its chunk locations are no longer a stable snapshot.
			manifest, err := loadTrustedManifest(manifestPath(cfg.SnapshotDir, id), cfg.ControlToken, "transferring")
			if err != nil {
				// A malformed or unauthenticated interrupted checkpoint must never
				// prevent the control plane from recovering. It cannot be resumed
				// safely, so keep the file for diagnosis but hide it from the API.
				log.Warn("Discard interrupted final sync %s; retained untrusted checkpoint %s for diagnosis: %v", id, manifestPath(cfg.SnapshotDir, id), err)
				delete(jobs, id)
				continue
			}
			manifest.State = "failed"
			manifest.Error = "final sync interrupted by replication control service restart"
			if err := signIncrementalManifest(manifest, cfg.ControlToken); err != nil {
				return fmt.Errorf("sign interrupted final sync %s: %w", id, err)
			}
			if err := writeManifest(cfg.SnapshotDir, manifest); err != nil {
				return fmt.Errorf("persist interrupted final sync %s: %w", id, err)
			}
			job.State = "failed"
			job.Error = manifest.Error
			jobs[id] = job
		default:
			_ = os.Remove(manifestPath(cfg.SnapshotDir, id))
			delete(jobs, id)
		}
	}
	s.prune()
	if primaryRecoveryRequired {
		log.Warn("Recovering primary Gitea after interrupted final replication session")
		s.recoverPrimary()
	}
	log.Info("Starting replication control plane: mode=%s listen=%s snapshot_dir=%s persisted_jobs=%d", cfg.Mode, cfg.ControlListen, cfg.SnapshotDir, len(jobs))
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/replication/health", s.auth(s.health))
	mux.HandleFunc(syncJobsPath, s.auth(s.syncTasks))
	mux.HandleFunc(syncJobsPath+"/", s.auth(s.syncTask))
	server := &http.Server{Addr: cfg.ControlListen, Handler: mux, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute, MaxHeaderBytes: 16 << 10}
	go func() {
		<-ctx.Done()
		s.abortActiveSession()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		log.Info("Replication control plane stopped")
		return nil
	}
	log.Error("Replication control plane failed: %v", err)
	return err
}

func validateAtomicLayout(snapshotDir string) error {
	root, err := resolvedPath(setting.AppWorkPath)
	if err != nil {
		return fmt.Errorf("resolve APP_WORK_PATH: %w", err)
	}
	if filepath.Dir(root) == root {
		return errors.New("APP_WORK_PATH must not be the filesystem root")
	}
	resolvedSnapshotDir, err := resolvedPath(snapshotDir)
	if err != nil {
		return fmt.Errorf("resolve SNAPSHOT_DIR: %w", err)
	}
	if isWithin(root, resolvedSnapshotDir) {
		return fmt.Errorf("SNAPSHOT_DIR %q must be outside APP_WORK_PATH %q", snapshotDir, root)
	}
	if filepath.Dir(resolvedSnapshotDir) == resolvedSnapshotDir {
		return errors.New("SNAPSHOT_DIR must not be the filesystem root")
	}
	if isWithin(resolvedSnapshotDir, root) {
		return fmt.Errorf("SNAPSHOT_DIR %q must not contain APP_WORK_PATH %q", snapshotDir, root)
	}
	paths := []string{setting.Database.Path, setting.RepoRootPath, setting.CustomPath, setting.AppDataPath}
	storages := []*setting.Storage{
		setting.Attachment.Storage, setting.LFS.Storage, setting.Avatar.Storage,
		setting.RepoAvatar.Storage, setting.Packages.Storage,
		setting.Actions.LogStorage, setting.Actions.ArtifactStorage,
	}
	for _, storage := range storages {
		if storage != nil {
			if storage.Type != setting.LocalStorageType {
				return fmt.Errorf("snapshot requires local storage, got %s", storage.Type)
			}
			paths = append(paths, storage.Path)
		}
	}
	for _, p := range paths {
		resolved, err := resolvedPath(p)
		if err != nil {
			return fmt.Errorf("resolve data path %q: %w", p, err)
		}
		if !isWithin(root, resolved) {
			return fmt.Errorf("path %q resolves outside APP_WORK_PATH %q; atomic recovery is impossible", p, root)
		}
	}
	return nil
}

func (s *controlServer) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		got := sha256.Sum256([]byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")))
		want := sha256.Sum256([]byte(s.cfg.ControlToken))
		if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			log.Warn("Reject unauthorized replication control request: method=%s path=%s remote=%s", r.Method, r.URL.EscapedPath(), r.RemoteAddr)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (s *controlServer) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]string{"status": "ok"})
}

func (s *controlServer) syncTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var request syncJobRequest
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxSyncJobRequestSize))
		if err != nil {
			log.Warn("Reject replication sync job request with unreadable or oversized body: remote=%s error=%v", r.RemoteAddr, err)
			http.Error(w, "invalid sync job request", http.StatusBadRequest)
			return
		}
		if err := json.Unmarshal(body, &request); err != nil {
			log.Warn("Reject malformed replication sync job request: remote=%s error=%v", r.RemoteAddr, err)
			http.Error(w, "invalid sync job request", http.StatusBadRequest)
			return
		}
		if request.RequestID != "" && !validReplicationRequestID(request.RequestID) {
			log.Warn("Reject replication sync job request with invalid request ID: kind=%s remote=%s", request.Kind, r.RemoteAddr)
			http.Error(w, "invalid sync job request ID", http.StatusBadRequest)
			return
		}
		switch request.Kind {
		case "preflight":
			r.URL.RawQuery = "resume=" + request.ResumeJobID
			s.preflight(w, r)
		case "final":
			r.URL.RawQuery = "base=" + request.BaseJobID
			r.Header.Set("Idempotency-Key", request.RequestID)
			s.finalize(w, r)
		default:
			log.Warn("Reject replication sync job request with invalid kind %q: remote=%s", request.Kind, r.RemoteAddr)
			http.Error(w, "sync job kind must be preflight or final", http.StatusBadRequest)
		}
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.mu.RLock()
	jobs := make([]*Snapshot, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobCopy := *job
		jobs = append(jobs, &jobCopy)
	}
	s.mu.RUnlock()
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].CreatedAt.After(jobs[j].CreatedAt) })
	writeJSON(w, jobs)
}

func (s *controlServer) syncTask(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, syncJobsPath+"/"), "/")
	id := parts[0]
	if !validSnapshotID(id) {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 3 && (parts[1] == "chunks" || parts[1] == "session") {
		s.syncSnapshot(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.mu.RLock()
	stored := s.jobs[id]
	var job *Snapshot
	if stored != nil {
		jobCopy := *stored
		job = &jobCopy
	}
	s.mu.RUnlock()
	if job == nil {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 2 && parts[1] == "manifest" {
		if job.State == snapshotStateCreating {
			http.Error(w, "snapshot is not ready", http.StatusConflict)
			return
		}
		if manifest := s.getTaskManifest(id); manifest != nil {
			writeJSONMaybeGzip(w, r, manifest)
			return
		}
		manifest, err := loadTrustedManifest(manifestPath(s.cfg.SnapshotDir, id), s.cfg.ControlToken, job.State)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		writeJSONMaybeGzip(w, r, manifest)
		return
	}
	if len(parts) != 1 {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, job)
}

func (s *controlServer) prune() {
	pruneManifestFiles(s.cfg.SnapshotDir, s.cfg.SnapshotRetention, s.cfg.ControlToken)
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, job := range s.jobs {
		if job.State == snapshotStateCreating {
			continue
		}
		if _, err := os.Stat(manifestPath(s.cfg.SnapshotDir, id)); os.IsNotExist(err) {
			delete(s.jobs, id)
			delete(s.taskManifests, id)
			delete(s.taskChunkIndexes, id)
		}
	}
}

func (s *controlServer) getTaskManifest(id string) *SnapshotManifest {
	s.mu.RLock()
	defer s.mu.RUnlock()
	manifest := s.taskManifests[id]
	if manifest == nil {
		return nil
	}
	manifestCopy := *manifest
	return &manifestCopy
}

func (s *controlServer) getTaskChunkLocation(id, hash string) (chunkLocation, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	index, indexed := s.taskChunkIndexes[id]
	location, ok := index[hash]
	return location, ok, indexed
}

func (s *controlServer) tryAcquireChunkSlot() (chan struct{}, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.chunkSlots == nil {
		s.chunkSlots = make(chan struct{}, maxConcurrentChunkServes)
	}
	select {
	case s.chunkSlots <- struct{}{}:
		return s.chunkSlots, true
	default:
		return nil, false
	}
}

func (s *controlServer) setTaskManifest(manifest *SnapshotManifest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.taskManifests == nil {
		s.taskManifests = map[string]*SnapshotManifest{}
	}
	manifestCopy := *manifest
	s.taskManifests[manifest.ID] = &manifestCopy
	if s.taskChunkIndexes == nil {
		s.taskChunkIndexes = map[string]map[string]chunkLocation{}
	}
	s.taskChunkIndexes[manifest.ID] = indexManifest(&manifestCopy)
}

func isReplicationTemporaryFile(name string) bool {
	if strings.HasPrefix(name, "..install-stage.checkpoint.tmp-") {
		return true
	}
	for _, base := range []string{"baseline.json", "current.json"} {
		if strings.HasPrefix(name, "."+base+".tmp-") {
			return true
		}
	}
	if strings.HasSuffix(name, ".json.tmp") {
		base := strings.TrimSuffix(strings.TrimPrefix(name, "."), ".json.tmp")
		return base == "baseline" || base == "current" || validSnapshotID(base)
	}
	if baseName, ok := strings.CutPrefix(name, "."); ok {
		base, _, ok := strings.Cut(baseName, ".json.tmp-")
		return ok && validSnapshotID(base)
	}
	return false
}

func pruneManifestFiles(dir string, retention int, tokens ...string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Warn("Cannot list replication manifest directory %s for pruning: %v", dir, err)
		return
	}
	var manifests []string
	removed := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		path := filepath.Join(dir, name)
		if isReplicationTemporaryFile(name) {
			if err := os.Remove(path); err == nil {
				removed++
			} else {
				log.Warn("Cannot remove temporary replication file %s: %v", path, err)
			}
			continue
		}
		if strings.HasSuffix(name, ".json") && validSnapshotID(strings.TrimSuffix(name, ".json")) {
			if len(tokens) > 0 {
				manifest, err := loadManifestFile(path)
				if err != nil || manifest.ID != strings.TrimSuffix(name, ".json") || !verifyIncrementalSignature(manifest, tokens[0]) {
					continue
				}
			}
			manifests = append(manifests, path)
		}
	}
	sort.Strings(manifests)
	for len(manifests) > retention {
		if err := os.Remove(manifests[0]); err == nil {
			removed++
		} else {
			log.Warn("Cannot prune replication manifest %s: %v", manifests[0], err)
		}
		manifests = manifests[1:]
	}
	if removed > 0 {
		if err := syncDirectory(dir); err != nil {
			log.Warn("Cannot persist replication manifest pruning in %s: %v", dir, err)
		}
		log.Info("Pruned %d replication manifest files; retained %d snapshot manifests", removed, len(manifests))
	}
}

func writeJSON(w http.ResponseWriter, value any) {
	writeJSONStatus(w, http.StatusOK, value)
}

func writeJSONMaybeGzip(w http.ResponseWriter, r *http.Request, payload any) {
	w.Header().Add("Vary", "Accept-Encoding")
	for _, header := range r.Header.Values("Accept-Encoding") {
		for item := range strings.SplitSeq(header, ",") {
			encoding, parameters, _ := strings.Cut(strings.TrimSpace(item), ";")
			if !strings.EqualFold(encoding, "gzip") {
				continue
			}
			quality := 1.0
			for parameter := range strings.SplitSeq(parameters, ";") {
				name, parameterValue, ok := strings.Cut(strings.TrimSpace(parameter), "=")
				if !ok || !strings.EqualFold(name, "q") {
					continue
				}
				parsed, err := strconv.ParseFloat(parameterValue, 64)
				if err != nil || !(parsed >= 0 && parsed <= 1) {
					writeJSON(w, payload)
					return
				}
				quality = parsed
			}
			if quality == 0 {
				writeJSON(w, payload)
				return
			}
			writer, err := gzip.NewWriterLevel(w, gzip.BestSpeed)
			if err != nil {
				writeJSON(w, payload)
				return
			}
			w.Header().Set("Content-Encoding", "gzip")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(writer).Encode(payload)
			_ = writer.Close()
			return
		}
	}
	writeJSON(w, payload)
}

func requestAcceptsGzip(r *http.Request) bool {
	var gzipQuality, wildcardQuality float64
	gzipFound, wildcardFound := false, false
	for _, header := range r.Header.Values("Accept-Encoding") {
		for item := range strings.SplitSeq(header, ",") {
			encoding, parameters, _ := strings.Cut(strings.TrimSpace(item), ";")
			quality := 1.0
			valid := true
			for parameter := range strings.SplitSeq(parameters, ";") {
				name, value, ok := strings.Cut(strings.TrimSpace(parameter), "=")
				if !ok || !strings.EqualFold(name, "q") {
					continue
				}
				parsed, err := strconv.ParseFloat(value, 64)
				if err != nil || parsed < 0 || parsed > 1 {
					valid = false
					break
				}
				quality = parsed
			}
			if !valid {
				continue
			}
			switch {
			case strings.EqualFold(encoding, "gzip") && !gzipFound:
				gzipFound, gzipQuality = true, quality
			case encoding == "*" && !wildcardFound:
				wildcardFound, wildcardQuality = true, quality
			}
		}
	}
	if gzipFound {
		return gzipQuality > 0
	}
	return wildcardFound && wildcardQuality > 0
}

func gzipChunk(data []byte) ([]byte, bool) {
	var compressed bytes.Buffer
	writer, err := gzip.NewWriterLevel(&compressed, gzip.BestSpeed)
	if err != nil {
		return nil, false
	}
	if _, err := writer.Write(data); err != nil {
		_ = writer.Close()
		return nil, false
	}
	if err := writer.Close(); err != nil {
		return nil, false
	}
	if compressed.Len()*100 >= len(data)*(100-minChunkCompressionSave) {
		return nil, false
	}
	return compressed.Bytes(), true
}

func writeChunk(w http.ResponseWriter, r *http.Request, data []byte) (int, bool) {
	body := data
	compressed := false
	w.Header().Add("Vary", "Accept-Encoding")
	if requestAcceptsGzip(r) {
		if gzipData, ok := gzipChunk(data); ok {
			body = gzipData
			compressed = true
			w.Header().Set("Content-Encoding", "gzip")
		}
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	_, _ = w.Write(body)
	return len(body), compressed
}

func writeJSONStatus(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
