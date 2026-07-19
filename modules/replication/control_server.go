// Copyright 2024 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
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
	"strings"
	"sync"
	"time"

	"code.gitea.io/gitea/modules/json"
	"code.gitea.io/gitea/modules/log"
	"code.gitea.io/gitea/modules/setting"
)

type syncJobRequest struct {
	Kind        string `json:"kind"`
	BaseJobID   string `json:"base_job_id,omitempty"`
	ResumeJobID string `json:"resume_job_id,omitempty"`
}

const syncJobsPath = "/api/v1/replication/sync-jobs"

type Snapshot struct {
	ID        string    `json:"id"`
	State     string    `json:"state"`
	CreatedAt time.Time `json:"created_at"`
	Size      int64     `json:"size,omitempty"`
	SHA256    string    `json:"sha256,omitempty"`
	Error     string    `json:"error,omitempty"`
	RootMode  uint32    `json:"root_mode"`
}

const snapshotStateCreating = "creating"

type controlServer struct {
	cfg           *config
	mu            sync.RWMutex
	jobs          map[string]*Snapshot
	taskManifests map[string]*SnapshotManifest
	busy          bool
	session       *finalSyncSession
	dataRoot      string
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
	if setting.Database.Type.String() != "sqlite3" {
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
	s := &controlServer{cfg: cfg, jobs: jobs, taskManifests: map[string]*SnapshotManifest{}}
	removeLegacyArchives(cfg.SnapshotDir)
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
			jobs[id] = job
		case "transferring":
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
		return nil
	}
	return err
}

func validateAtomicLayout(snapshotDir string) error {
	root, err := resolvedPath(setting.AppWorkPath)
	if err != nil {
		return fmt.Errorf("resolve APP_WORK_PATH: %w", err)
	}
	if root == string(filepath.Separator) {
		return errors.New("APP_WORK_PATH must not be the filesystem root")
	}
	resolvedSnapshotDir, err := resolvedPath(snapshotDir)
	if err != nil {
		return fmt.Errorf("resolve SNAPSHOT_DIR: %w", err)
	}
	if isWithin(root, resolvedSnapshotDir) {
		return fmt.Errorf("SNAPSHOT_DIR %q must be outside APP_WORK_PATH %q", snapshotDir, root)
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
		decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
		if err := decoder.Decode(&request); err != nil {
			http.Error(w, "invalid sync job request", http.StatusBadRequest)
			return
		}
		switch request.Kind {
		case "preflight":
			r.URL.RawQuery = "resume=" + request.ResumeJobID
			s.preflight(w, r)
		case "final":
			r.URL.RawQuery = "base=" + request.BaseJobID
			s.finalize(w, r)
		default:
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
			writeJSON(w, manifest)
			return
		}
		http.ServeFile(w, r, manifestPath(s.cfg.SnapshotDir, id))
		return
	}
	if len(parts) != 1 {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, job)
}

func (s *controlServer) prune() {
	pruneManifestFiles(s.cfg.SnapshotDir, s.cfg.SnapshotRetention)
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.jobs {
		if _, err := os.Stat(manifestPath(s.cfg.SnapshotDir, id)); os.IsNotExist(err) {
			delete(s.jobs, id)
			delete(s.taskManifests, id)
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

func (s *controlServer) setTaskManifest(manifest *SnapshotManifest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.taskManifests == nil {
		s.taskManifests = map[string]*SnapshotManifest{}
	}
	manifestCopy := *manifest
	s.taskManifests[manifest.ID] = &manifestCopy
}

func pruneManifestFiles(dir string, retention int) {
	entries, err := os.ReadDir(dir)
	if err != nil {
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
		if strings.HasSuffix(name, ".json.tmp") || (strings.HasPrefix(name, ".") && strings.Contains(name, ".tmp-")) {
			if os.Remove(path) == nil {
				removed++
			}
			continue
		}
		if strings.HasSuffix(name, ".json") && validSnapshotID(strings.TrimSuffix(name, ".json")) {
			manifests = append(manifests, path)
		}
	}
	sort.Strings(manifests)
	for len(manifests) > retention {
		if os.Remove(manifests[0]) == nil {
			removed++
		}
		manifests = manifests[1:]
	}
	if removed > 0 {
		_ = syncDirectory(dir)
		log.Info("Pruned %d replication manifest files; retained %d snapshot manifests", removed, len(manifests))
	}
}

func writeJSON(w http.ResponseWriter, value any) {
	writeJSONStatus(w, http.StatusOK, value)
}

func writeJSONStatus(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
