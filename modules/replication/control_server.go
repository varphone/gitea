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
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	syncJobsPath                      = "/api/v1/replication/sync-jobs"
	maxSyncJobRequestSize             = 1024 * 1024
	maxConcurrentChunkServes          = 8
	maxConcurrentManifestServes       = 4
	maxTransientFailedJobs            = 64
	maxChunkSourceAlternates          = 2
	minChunkCompressionSave           = 5
	maxPooledChunkGzipBuffer          = 2 * 1024 * 1024
	chunkCompressionProbeSize         = 16 * 1024
	chunkCompressionProbes            = 8
	primaryOutageCheckpoint           = ".primary-outage"
	replicationEncodedBodyBytesHeader = "X-Replication-Server-Encoded-Body-Bytes"
)

var (
	chunkGzipBuffers = sync.Pool{New: func() any { return new(bytes.Buffer) }}
	chunkGzipWriters = sync.Pool{New: func() any {
		writer, err := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
		if err != nil {
			panic(err)
		}
		return writer
	}}
	primaryCheckpointDirectoryIdentity      atomic.Pointer[checkpointDirectoryIdentity]
	primaryCheckpointDirectoryIdentityMu    sync.Mutex
	primaryOutageCheckpointStateErrorLogged atomic.Bool
)

type checkpointDirectoryIdentity struct {
	path string
	info os.FileInfo
}

func getChunkGzipWriter(dst io.Writer) *gzip.Writer {
	writer := chunkGzipWriters.Get().(*gzip.Writer) //nolint:forcetypeassert // New and Put use only *gzip.Writer.
	writer.Reset(dst)
	return writer
}

func releaseChunkGzipWriter(writer *gzip.Writer) {
	writer.Reset(io.Discard)
	chunkGzipWriters.Put(writer)
}

func primaryOutageCheckpointPath(snapshotDir string) string {
	return filepath.Join(snapshotDir, primaryOutageCheckpoint)
}

func verifyPrimaryCheckpointDirectory(directory string, info os.FileInfo) error {
	directoryPath, err := filepath.Abs(directory)
	if err != nil {
		return fmt.Errorf("resolve primary outage recovery checkpoint directory %s: %w", directory, err)
	}
	if cached := primaryCheckpointDirectoryIdentity.Load(); cached != nil &&
		cached.path == directoryPath && os.SameFile(cached.info, info) {
		return nil
	}

	primaryCheckpointDirectoryIdentityMu.Lock()
	defer primaryCheckpointDirectoryIdentityMu.Unlock()
	if cached := primaryCheckpointDirectoryIdentity.Load(); cached != nil &&
		cached.path == directoryPath && os.SameFile(cached.info, info) {
		return nil
	}
	resolvedDirectory, err := resolvedPath(directory)
	if err != nil {
		return fmt.Errorf("resolve primary outage recovery checkpoint directory %s: %w", directory, err)
	}
	if resolvedDirectory != directoryPath {
		return fmt.Errorf("primary outage recovery checkpoint directory %s contains symlink components", directory)
	}
	current, err := os.Lstat(directory)
	if err != nil {
		return fmt.Errorf("reinspect primary outage recovery checkpoint directory %s: %w", directory, err)
	}
	if !current.IsDir() || current.Mode()&os.ModeSymlink != 0 || !os.SameFile(info, current) {
		return fmt.Errorf("primary outage recovery checkpoint directory %s changed while being verified", directory)
	}
	primaryCheckpointDirectoryIdentity.Store(&checkpointDirectoryIdentity{path: directoryPath, info: current})
	return nil
}

func primaryOutageCheckpointState(snapshotDir string) (bool, error) {
	path := primaryOutageCheckpointPath(snapshotDir)
	_, err := os.Lstat(path)
	if err == nil {
		return true, nil
	}
	if !os.IsNotExist(err) {
		return true, fmt.Errorf("inspect primary outage recovery checkpoint %s: %w", path, err)
	}
	directory := filepath.Clean(snapshotDir)
	info, err := os.Lstat(directory)
	if err != nil {
		return true, fmt.Errorf("inspect primary outage recovery checkpoint directory %s: %w", directory, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return true, fmt.Errorf("primary outage recovery checkpoint directory %s is not a real directory", directory)
	}
	if err := verifyPrimaryCheckpointDirectory(directory, info); err != nil {
		return true, err
	}
	return false, nil
}

func primaryOutageCheckpointExists(snapshotDir string) bool {
	exists, err := primaryOutageCheckpointState(snapshotDir)
	if err != nil {
		if primaryOutageCheckpointStateErrorLogged.CompareAndSwap(false, true) {
			log.Warn("Cannot verify primary outage recovery checkpoint state: %v", err)
		}
		return true
	}
	primaryOutageCheckpointStateErrorLogged.Store(false)
	return exists
}

func readPrimaryOutageCheckpointSnapshotID(snapshotDir string) (string, error) {
	path := primaryOutageCheckpointPath(snapshotDir)
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() > 128 {
		return "", errors.New("invalid primary outage recovery checkpoint")
	}
	file, openedInfo, err := openRegularFile(path, info)
	if err != nil {
		return "", fmt.Errorf("open primary outage recovery checkpoint: %w", err)
	}
	defer file.Close()
	if openedInfo.Size() > 128 {
		return "", errors.New("primary outage recovery checkpoint exceeds maximum size")
	}
	data, err := io.ReadAll(io.LimitReader(file, 129))
	if err != nil {
		return "", err
	}
	if len(data) > 128 {
		return "", errors.New("primary outage recovery checkpoint exceeds maximum size")
	}
	id := strings.TrimSpace(string(data))
	if !validSnapshotID(id) {
		return "", errors.New("invalid primary outage recovery checkpoint snapshot ID")
	}
	return id, nil
}

func (s *controlServer) primaryRecoveryBlocksReadySnapshot(id string) bool {
	s.mu.RLock()
	pending := s.primaryRecoveryPending
	s.mu.RUnlock()
	if !pending {
		return false
	}
	if s.cfg == nil {
		return true
	}
	recoveryID, err := readPrimaryOutageCheckpointSnapshotID(s.cfg.SnapshotDir)
	if err != nil {
		log.Debug("Cannot identify replication snapshot associated with pending primary recovery: snapshot=%s error=%v", id, err)
		return true
	}
	return recoveryID == id
}

func clearPrimaryOutageCheckpoint(snapshotDir string) error {
	return removeFileSynced(primaryOutageCheckpointPath(snapshotDir))
}

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
	ID               string    `json:"id"`
	State            string    `json:"state"`
	CreatedAt        time.Time `json:"created_at"`
	Size             int64     `json:"size,omitempty"`
	SHA256           string    `json:"sha256,omitempty"`
	Error            string    `json:"error,omitempty"`
	RootMode         uint32    `json:"root_mode"`
	RequestID        string    `json:"-"`
	BaseJobID        string    `json:"-"`
	transientFailure bool
}

const snapshotStateCreating = "creating"

type controlServer struct {
	cfg                     *config
	taskCtx                 context.Context
	taskWG                  sync.WaitGroup
	backgroundWG            sync.WaitGroup
	mu                      sync.RWMutex
	chunkMu                 sync.RWMutex
	jobs                    map[string]*Snapshot
	taskManifests           map[string]*SnapshotManifest
	taskChunkIndexes        map[string]map[string]chunkLocation
	taskChunkIndexFallbacks map[string]string
	busy                    bool
	shuttingDown            bool
	primaryRecoveryPending  bool
	session                 *finalSyncSession
	dataRoot                string
	resolvedRoot            string
	chunkSlots              chan struct{}
	manifestSlots           chan struct{}
	taskChunkAlternates     map[string]map[string][]chunkLocation
}

type chunkServePlan struct {
	location       chunkLocation
	alternates     []chunkLocation
	jobState       string
	allowed        bool
	primaryResumed bool
	indexed        bool
	found          bool
}

func (s *controlServer) root() string {
	if s.dataRoot != "" {
		return s.dataRoot
	}
	return setting.AppWorkPath
}

func (s *controlServer) taskContext() context.Context {
	if s.taskCtx != nil {
		return s.taskCtx
	}
	return context.Background()
}

func ServeControl(ctx context.Context) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if !cfg.Enabled {
		return errors.New("replication control service is disabled")
	}
	if len(cfg.ControlToken) < 32 {
		return errors.New("[replicate] CONTROL_TOKEN must contain at least 32 bytes")
	}
	if !setting.Database.Type.IsSQLite3() {
		return fmt.Errorf("disaster-recovery snapshots currently require sqlite3, got %s", setting.Database.Type)
	}
	if err := ensureSnapshotDirectoryTree(cfg.SnapshotDir); err != nil {
		return err
	}
	if err := validateAtomicLayout(cfg.SnapshotDir); err != nil {
		return err
	}
	if err := ensurePrivateSnapshotDirectory(cfg.SnapshotDir); err != nil {
		return err
	}
	controlLock, err := acquireControlRunLock(cfg.SnapshotDir)
	if err != nil {
		return fmt.Errorf("acquire replication control service lock: %w", err)
	}
	defer func() {
		if err := controlLock.Release(); err != nil {
			log.Error("Release replication control service lock failed: %v", err)
		}
	}()
	var startupRestoreLock *restoreRunLock
	if cfg.Mode == modeReplica {
		startupRestoreLock, err = acquireControlStartupRestoreLock(ctx, cfg.SnapshotDir)
		if err != nil {
			return fmt.Errorf("wait for standby restore before loading replication manifests: %w", err)
		}
		defer func() {
			if startupRestoreLock != nil {
				if err := startupRestoreLock.Release(); err != nil {
					log.Error("Release standby restore lock after control startup failed: %v", err)
				}
			}
		}()
	}
	taskCtx, cancelTasks := context.WithCancel(context.Background())
	defer cancelTasks()
	s := &controlServer{
		cfg: cfg, taskCtx: taskCtx, taskManifests: map[string]*SnapshotManifest{},
		taskChunkIndexes: map[string]map[string]chunkLocation{}, taskChunkIndexFallbacks: map[string]string{},
		taskChunkAlternates: map[string]map[string][]chunkLocation{},
		chunkSlots:          make(chan struct{}, maxConcurrentChunkServes),
	}
	primaryRecoveryAttempted := false
	recoverPrimaryAtStartup := func(reason, fallbackSnapshotID string) {
		recoverySnapshotID, checkpointErr := readPrimaryOutageCheckpointSnapshotID(cfg.SnapshotDir)
		if checkpointErr != nil {
			recoverySnapshotID = fallbackSnapshotID
			if recoverySnapshotID == "" {
				recoverySnapshotID = "unknown"
			}
			log.Warn("Cannot read primary outage recovery checkpoint ID: fallback_snapshot=%s error=%v", recoverySnapshotID, checkpointErr)
		}
		log.Warn("Recovering primary Gitea before rebuilding replication state: snapshot=%s trigger=startup reason=%s", recoverySnapshotID, reason)
		s.recoverPrimary(recoverySnapshotID, "startup")
		primaryRecoveryAttempted = true
	}
	if cfg.Mode == modePrimary && primaryOutageCheckpointExists(cfg.SnapshotDir) {
		recoverPrimaryAtStartup("outage_checkpoint", "")
	}
	jobs, transferCheckpointID, err := loadManifests(cfg.SnapshotDir, cfg.ControlToken)
	if err != nil {
		return err
	}
	transferCheckpointFound := transferCheckpointID != ""
	if cfg.Mode == modePrimary && transferCheckpointFound && !primaryRecoveryAttempted {
		if !primaryOutageCheckpointExists(cfg.SnapshotDir) {
			if err := writeFileSynced(primaryOutageCheckpointPath(cfg.SnapshotDir), []byte(transferCheckpointID)); err != nil {
				return fmt.Errorf("persist primary recovery checkpoint for interrupted final sync %s: %w", transferCheckpointID, err)
			}
			log.Warn("Restored primary outage checkpoint for interrupted final sync: snapshot=%s", transferCheckpointID)
		}
		recoverPrimaryAtStartup("interrupted_transfer_manifest", transferCheckpointID)
	}
	s.jobs = jobs
	s.resolvedRoot, err = resolvedPath(s.root())
	if err != nil {
		return fmt.Errorf("resolve replication data root: %w", err)
	}
	pruneReplicationTemporaryFiles(cfg.SnapshotDir)
	// Prune history before rebuilding persisted chunk indexes.
	s.prune()
	for id, job := range jobs {
		switch job.State {
		case "ready", "preflight":
			manifest, err := loadTrustedManifest(manifestPath(cfg.SnapshotDir, id), cfg.ControlToken, job.State)
			if err != nil {
				log.Warn("Discard unavailable persisted replication job %s: %v", id, err)
				delete(jobs, id)
				continue
			}
			if manifest.State == "preflight" {
				s.taskManifests[id] = manifest
			}
			if cfg.Mode == modePrimary && manifest.State == "preflight" {
				indexStarted := time.Now()
				index, alternates, err := indexManifestWithAlternatesContext(ctx, manifest)
				if err != nil {
					return fmt.Errorf("index persisted preflight manifest %s: %w", id, err)
				}
				s.taskChunkIndexes[id] = index
				s.taskChunkAlternates[id] = alternates
				log.Info("Rebuilt persisted preflight chunk index: snapshot=%s entries=%d unique_chunks=%d alternate_hashes=%d duration=%s", id, manifest.FileCount, len(index), len(alternates), time.Since(indexStarted))
			}
			jobs[id] = job
		case "transferring":
			if cfg.Mode != modePrimary {
				// On a replica this is a client checkpoint; the remote primary owns the write fence.
				jobs[id] = job
				continue
			}
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
			if err := signIncrementalManifestContext(ctx, manifest, cfg.ControlToken); err != nil {
				return fmt.Errorf("sign interrupted final sync %s: %w", id, err)
			}
			if err := writeManifestAtContext(ctx, manifestPath(cfg.SnapshotDir, id), manifest); err != nil {
				return fmt.Errorf("persist interrupted final sync %s: %w", id, err)
			}
			job.State = "failed"
			job.Error = manifest.Error
			jobs[id] = job
		case "failed":
			if job.RequestID != "" {
				jobs[id] = job
				continue
			}
			if err := removeFileSynced(manifestPath(cfg.SnapshotDir, id)); err != nil {
				log.Warn("Cannot remove discarded failed replication manifest: snapshot=%s error=%v", id, err)
			}
			delete(jobs, id)
		default:
			if err := removeFileSynced(manifestPath(cfg.SnapshotDir, id)); err != nil {
				log.Warn("Cannot remove replication manifest with unsupported task state: snapshot=%s state=%s error=%v", id, job.State, err)
			}
			delete(jobs, id)
		}
	}
	if cfg.Mode == modePrimary && transferCheckpointFound {
		// Interrupted transfers become failed manifests during startup recovery.
		pruneManifestFiles(cfg.SnapshotDir, cfg.SnapshotRetention, cfg.ControlToken)
		s.pruneMissingManifestJobs()
	}
	if startupRestoreLock != nil {
		lock := startupRestoreLock
		startupRestoreLock = nil
		if err := lock.Release(); err != nil {
			return fmt.Errorf("release standby restore lock after control startup: %w", err)
		}
	}
	log.Info("Starting replication control plane: mode=%s listen=%s snapshot_dir=%s persisted_jobs=%d", cfg.Mode, cfg.ControlListen, cfg.SnapshotDir, len(jobs))
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/replication/health", s.auth(s.health))
	mux.HandleFunc(syncJobsPath, s.auth(s.syncTasks))
	mux.HandleFunc(syncJobsPath+"/", s.auth(s.syncTask))
	server := newReplicationHTTPServer(cfg, mux)
	shutdownDone := make(chan struct{})
	shutdownTrigger := make(chan struct{})
	var shutdownOnce sync.Once
	shutdown := func() {
		shutdownOnce.Do(func() {
			log.Info("Stopping replication control plane: canceling active jobs")
			s.mu.Lock()
			s.shuttingDown = true
			s.mu.Unlock()
			cancelTasks()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := server.Shutdown(shutdownCtx); err != nil {
				log.Warn("Graceful replication control plane shutdown timed out: %v", err)
				_ = server.Close()
			}
			cancel()
			s.taskWG.Wait()
			s.abortActiveSession()
			s.backgroundWG.Wait()
			log.Info("Replication control plane stopped after task cleanup")
			close(shutdownDone)
		})
	}
	go func() {
		select {
		case <-ctx.Done():
			shutdown()
		case <-shutdownTrigger:
		}
	}()
	err = server.ListenAndServe()
	close(shutdownTrigger)
	if errors.Is(err, http.ErrServerClosed) {
		shutdown()
		<-shutdownDone
		log.Info("Replication control plane stopped")
		return nil
	}
	shutdown()
	<-shutdownDone
	log.Error("Replication control plane failed: %v", err)
	return err
}

func newReplicationHTTPServer(cfg *config, handler http.Handler) *http.Server {
	return &http.Server{
		Addr: cfg.ControlListen, Handler: handler,
		ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second,
		WriteTimeout: cfg.ControlWriteTimeout, IdleTimeout: 2 * time.Minute, MaxHeaderBytes: 16 * 1024,
	}
}

func validateAtomicLayout(snapshotDir string) error {
	root, err := resolvedPath(setting.AppWorkPath)
	if err != nil {
		return fmt.Errorf("resolve APP_WORK_PATH: %w", err)
	}
	rootPath, err := filepath.Abs(filepath.Clean(setting.AppWorkPath))
	if err != nil {
		return fmt.Errorf("resolve APP_WORK_PATH path: %w", err)
	}
	if root != rootPath {
		return errors.New("APP_WORK_PATH must not contain symlink components")
	}
	if filepath.Dir(root) == root {
		return errors.New("APP_WORK_PATH must not be the filesystem root")
	}
	if err := validateNoNestedMounts(root); err != nil {
		return fmt.Errorf("validate APP_WORK_PATH mounts: %w", err)
	}
	resolvedSnapshotDir, err := resolvedPath(snapshotDir)
	if err != nil {
		return fmt.Errorf("resolve SNAPSHOT_DIR: %w", err)
	}
	snapshotPath, err := filepath.Abs(filepath.Clean(snapshotDir))
	if err != nil {
		return fmt.Errorf("resolve SNAPSHOT_DIR path: %w", err)
	}
	if resolvedSnapshotDir != snapshotPath {
		return errors.New("SNAPSHOT_DIR must not contain symlink components")
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
	if setting.OAuth2.Enabled && usesFileBackedOAuth2SigningKey() {
		paths = append(paths, setting.OAuth2.JWTSigningPrivateKeyFile)
	}
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
		w.Header().Set("Cache-Control", "no-store")
		scheme, credentials, hasScheme := strings.Cut(strings.TrimSpace(r.Header.Get("Authorization")), " ")
		if !hasScheme || !strings.EqualFold(scheme, "Bearer") {
			credentials = ""
		}
		credentials = strings.TrimSpace(credentials)
		got := sha256.Sum256([]byte(credentials))
		want := sha256.Sum256([]byte(s.cfg.ControlToken))
		if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			log.Warn("Reject unauthorized replication control request: method=%s path=%s remote=%s", r.Method, r.URL.EscapedPath(), r.RemoteAddr)
			w.Header().Set("WWW-Authenticate", `Bearer realm="replication"`)
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
			r.Header.Set("Idempotency-Key", request.RequestID)
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
	for _, job := range jobs {
		if job.State == "ready" && s.primaryRecoveryBlocksReadySnapshot(job.ID) {
			w.Header().Set("Retry-After", "1")
			http.Error(w, errPrimaryRecoveryPending.Error(), http.StatusServiceUnavailable)
			return
		}
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].ID > jobs[j].ID })
	writeJSON(w, jobs)
}

func (s *controlServer) syncTask(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, syncJobsPath+"/"), "/")
	id := parts[0]
	if !validSnapshotID(id) {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 2 && parts[1] == "chunks" {
		s.syncChunkBatch(w, r)
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
	if job.State == "ready" && s.primaryRecoveryBlocksReadySnapshot(id) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, errPrimaryRecoveryPending.Error(), http.StatusServiceUnavailable)
		return
	}
	if len(parts) == 2 && parts[1] == "manifest" {
		if job.State == snapshotStateCreating {
			http.Error(w, "snapshot is not ready", http.StatusConflict)
			return
		}
		manifest := s.getTaskManifest(id)
		if manifest == nil {
			loaded, err := loadTrustedManifest(manifestPath(s.cfg.SnapshotDir, id), s.cfg.ControlToken, job.State)
			if err != nil {
				log.Warn("Cannot load trusted replication manifest for request: snapshot=%s state=%s error=%v", id, job.State, err)
				http.NotFound(w, r)
				return
			}
			manifest = loaded
		}
		if baseID := strings.TrimSpace(r.URL.Query().Get("base")); baseID != "" && baseID != id && validSnapshotID(baseID) {
			if base := s.loadManifestForPatch(baseID); base != nil {
				patch, patchErr := buildManifestPatch(base, manifest)
				if patchErr != nil {
					log.Warn("Cannot build replication manifest patch: snapshot=%s base=%s error=%v", id, baseID, patchErr)
				} else {
					log.Info("Serving replication manifest patch: snapshot=%s base=%s changed=%d removed=%d entries=%d", id, baseID, len(patch.Changed), len(patch.Removed), patch.Count)
					writeManifestPatchMaybeGzip(w, r, patch)
					return
				}
			}
		}
		// Manifest responses stream a large document, so bound how many run at once.
		slots, acquired := s.tryAcquireManifestSlot()
		if !acquired {
			w.Header().Set("Retry-After", "1")
			log.Warn("Rejected replication manifest request because concurrent serve limit was reached: snapshot=%s limit=%d", id, maxConcurrentManifestServes)
			http.Error(w, "replication manifest service is busy", http.StatusServiceUnavailable)
			return
		}
		defer func() { <-slots }()
		writeManifestMaybeGzip(w, r, manifest)
		return
	}
	if len(parts) != 1 {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, job)
}

// loadManifestForPatch returns the retained manifest a standby offers as patch base.
func (s *controlServer) loadManifestForPatch(id string) *SnapshotManifest {
	if manifest := s.getTaskManifest(id); manifest != nil {
		return manifest
	}
	manifest, err := loadTrustedManifestStates(manifestPath(s.cfg.SnapshotDir, id), s.cfg.ControlToken, "preflight", "transferring", "ready", "failed")
	if err != nil {
		log.Debug("Cannot load replication manifest patch base: snapshot=%s error=%v", id, err)
		return nil
	}
	return manifest
}

func (s *controlServer) prune() {
	pruneManifestFiles(s.cfg.SnapshotDir, s.cfg.SnapshotRetention, s.cfg.ControlToken)
	s.pruneMissingManifestJobs()
}

func (s *controlServer) pruneMissingManifestJobs() {
	type pruneCandidate struct {
		id        string
		job       *Snapshot
		requestID string
	}

	pruneStarted := time.Now()
	s.mu.RLock()
	candidates := make([]pruneCandidate, 0, len(s.jobs))
	protectedChunkIndexes := make(map[string]struct{}, len(s.taskChunkIndexFallbacks)*2)
	for deltaID, baseID := range s.taskChunkIndexFallbacks {
		protectedChunkIndexes[deltaID] = struct{}{}
		if baseID != "" {
			protectedChunkIndexes[baseID] = struct{}{}
		}
	}
	for id, job := range s.jobs {
		if job.State == snapshotStateCreating {
			continue
		}
		if _, referenced := protectedChunkIndexes[id]; referenced {
			continue
		}
		candidates = append(candidates, pruneCandidate{id: id, job: job, requestID: job.RequestID})
	}
	s.mu.RUnlock()

	missing := make([]pruneCandidate, 0)
	for _, candidate := range candidates {
		if _, err := os.Stat(manifestPath(s.cfg.SnapshotDir, candidate.id)); !os.IsNotExist(err) {
			continue
		}
		if candidate.requestID != "" {
			hasCheckpoint := false
			for _, checkpointPath := range []string{
				finalizeRequestCheckpointPath(s.cfg.SnapshotDir, candidate.id),
				preflightRequestCheckpointPath(s.cfg.SnapshotDir, candidate.id),
			} {
				if _, err := os.Stat(checkpointPath); err == nil || !os.IsNotExist(err) {
					hasCheckpoint = true
					break
				}
			}
			if hasCheckpoint {
				continue
			}
		}
		missing = append(missing, candidate)
	}

	removed := 0
	if len(missing) > 0 {
		s.mu.Lock()
		for _, candidate := range missing {
			job := s.jobs[candidate.id]
			if job != candidate.job || job.State == snapshotStateCreating || job.RequestID != candidate.requestID {
				continue
			}
			delete(s.jobs, candidate.id)
			delete(s.taskManifests, candidate.id)
			delete(s.taskChunkIndexes, candidate.id)
			delete(s.taskChunkIndexFallbacks, candidate.id)
			delete(s.taskChunkAlternates, candidate.id)
			removed++
		}
		s.mu.Unlock()
	}
	if removed > 0 {
		log.Info("Pruned in-memory replication jobs without manifests: checked=%d removed=%d duration=%s", len(candidates), removed, time.Since(pruneStarted))
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

func (s *controlServer) getServableChunkSources(id, hash string) chunkServePlan {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job := s.jobs[id]
	session := s.session
	primaryResumed := session != nil && session.id == id && session.primaryResumed
	if job == nil {
		return chunkServePlan{primaryResumed: primaryResumed}
	}
	// The standby may still be applying its in-place update after the primary was
	// released, so serve an active session even then. Every served chunk is read
	// from the frozen manifest location and verified against its hash, so a source
	// that changed after release fails instead of returning wrong data.
	allowed := job.State == "preflight" || (job.State == "transferring" && session != nil && session.id == id)
	if !allowed {
		return chunkServePlan{jobState: job.State, allowed: false, primaryResumed: primaryResumed}
	}
	index, indexed := s.taskChunkIndexes[id]
	location, found := index[hash]
	if !found {
		if baseID := s.taskChunkIndexFallbacks[id]; baseID != "" {
			baseIndex, baseIndexed := s.taskChunkIndexes[baseID]
			if baseIndexed {
				location, found = baseIndex[hash]
				indexed = true
			}
		}
	}
	baseID := s.taskChunkIndexFallbacks[id]
	alternates := s.taskChunkAlternates[id][hash]
	if baseID != "" {
		alternates = append([]chunkLocation(nil), alternates...)
	}
	primary, hasPrimary := s.taskChunkIndexes[id][hash]
	if !hasPrimary {
		if baseID != "" {
			primary, hasPrimary = s.taskChunkIndexes[baseID][hash]
		}
	}
	appendAlternate := func(candidate chunkLocation) {
		if candidate.Path == "" || (hasPrimary && candidate == primary) {
			return
		}
		if slices.Contains(alternates, candidate) {
			return
		}
		alternates = append(alternates, candidate)
	}
	if baseID != "" {
		appendAlternate(s.taskChunkIndexes[baseID][hash])
		for _, candidate := range s.taskChunkAlternates[baseID][hash] {
			appendAlternate(candidate)
		}
	}
	return chunkServePlan{
		location: location, alternates: alternates, allowed: allowed,
		jobState: job.State, primaryResumed: primaryResumed, indexed: indexed, found: found,
	}
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

func (s *controlServer) tryAcquireManifestSlot() (chan struct{}, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.manifestSlots == nil {
		s.manifestSlots = make(chan struct{}, maxConcurrentManifestServes)
	}
	select {
	case s.manifestSlots <- struct{}{}:
		return s.manifestSlots, true
	default:
		return nil, false
	}
}

func (s *controlServer) setTaskManifest(ctx context.Context, manifest *SnapshotManifest, baseManifestID string) error {
	manifestCopy := *manifest
	var index map[string]chunkLocation
	var alternates map[string][]chunkLocation
	indexFallbackID := ""
	indexKind := "full"
	indexed := false
	indexStarted := time.Now()
	switch manifestCopy.State {
	case "preflight":
		var err error
		index, alternates, err = indexManifestWithAlternatesContext(ctx, &manifestCopy)
		if err != nil {
			return fmt.Errorf("index preflight manifest %s: %w", manifest.ID, err)
		}
		indexed = true
	case "transferring":
		s.mu.RLock()
		baseIndex, hasBaseIndex := s.taskChunkIndexes[baseManifestID]
		s.mu.RUnlock()
		var err error
		if baseManifestID != "" && hasBaseIndex {
			index, alternates, err = indexManifestDeltaContext(ctx, &manifestCopy, baseIndex)
			indexFallbackID = baseManifestID
			indexKind = "delta"
		} else {
			index, alternates, err = indexManifestWithAlternatesContext(ctx, &manifestCopy)
		}
		if err != nil {
			return fmt.Errorf("index transferring manifest %s: %w", manifest.ID, err)
		}
		indexed = true
	}
	if indexed {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	s.mu.Lock()
	if s.taskManifests == nil {
		s.taskManifests = map[string]*SnapshotManifest{}
	}
	s.taskManifests[manifest.ID] = &manifestCopy
	if s.taskChunkIndexes == nil {
		s.taskChunkIndexes = map[string]map[string]chunkLocation{}
	}
	if s.taskChunkIndexFallbacks == nil {
		s.taskChunkIndexFallbacks = map[string]string{}
	}
	if indexed {
		s.taskChunkIndexes[manifest.ID] = index
		if indexFallbackID != "" {
			s.taskChunkIndexFallbacks[manifest.ID] = indexFallbackID
		} else {
			delete(s.taskChunkIndexFallbacks, manifest.ID)
		}
	} else {
		delete(s.taskChunkIndexes, manifest.ID)
		delete(s.taskChunkIndexFallbacks, manifest.ID)
	}
	if manifestCopy.State == "preflight" || manifestCopy.State == "transferring" {
		if s.taskChunkAlternates == nil {
			s.taskChunkAlternates = map[string]map[string][]chunkLocation{}
		}
		s.taskChunkAlternates[manifest.ID] = alternates
	} else if s.taskChunkAlternates != nil {
		delete(s.taskChunkAlternates, manifest.ID)
	}
	s.mu.Unlock()
	if indexed {
		log.Info("Indexed replication manifest chunks: snapshot=%s state=%s index_kind=%s index_entries=%d base_snapshot=%s alternate_hashes=%d duration=%s", manifest.ID, manifestCopy.State, indexKind, len(index), indexFallbackID, len(alternates), time.Since(indexStarted))
	}
	return nil
}

func isReplicationTemporaryFile(name string) bool {
	if strings.HasPrefix(name, ".pending-finalize.json.tmp-") {
		return true
	}
	if strings.HasPrefix(name, ".pending-preflight.json.tmp-") {
		return true
	}
	if strings.HasPrefix(name, ".primary-outage.tmp-") {
		return true
	}
	if strings.HasPrefix(name, "."+snapshotIDHighWaterName+".tmp-") {
		return true
	}
	if name, ok := strings.CutPrefix(name, ".."); ok {
		for _, suffix := range []string{finalizeRequestCheckpointSuffix, preflightRequestCheckpointSuffix} {
			if id, _, ok := strings.Cut(name, suffix+".tmp-"); ok && validSnapshotID(id) {
				return true
			}
		}
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

func pruneReplicationTemporaryFiles(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Warn("Cannot list replication snapshot directory %s for temporary file cleanup: %v", dir, err)
		return
	}
	removed := 0
	for _, entry := range entries {
		if entry.IsDir() || !isReplicationTemporaryFile(entry.Name()) {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if err := os.Remove(path); err != nil {
			if !os.IsNotExist(err) {
				log.Warn("Cannot remove stale replication temporary file %s: %v", path, err)
			}
			continue
		}
		removed++
	}
	if removed > 0 {
		if err := syncDirectory(dir); err != nil {
			log.Warn("Cannot persist stale replication temporary file cleanup in %s: %v", dir, err)
		}
		log.Info("Removed stale replication temporary files: directory=%s removed=%d", dir, removed)
	}
}

func pruneManifestFiles(dir string, retention int, tokens ...string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Warn("Cannot list replication manifest directory %s for pruning: %v", dir, err)
		return
	}
	var manifests []string
	var invalidManifests []string
	var checkpoints []string
	manifestIDs := make(map[string]struct{})
	activeTransferManifest := ""
	activeTransferID := ""
	activeTransferBaseID := ""
	latestManifestID := ""
	latestReadyManifest := ""
	latestReadyID := ""
	latestPreflightManifest := ""
	latestPreflightID := ""
	removed := 0
	removedCheckpoints := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		path := filepath.Join(dir, name)
		if isReplicationTemporaryFile(name) {
			continue
		}
		if id, ok := strings.CutPrefix(name, "."); ok {
			kind, suffix := "finalize", finalizeRequestCheckpointSuffix
			if strings.HasSuffix(id, preflightRequestCheckpointSuffix) {
				kind, suffix = "preflight", preflightRequestCheckpointSuffix
			} else if !strings.HasSuffix(id, finalizeRequestCheckpointSuffix) {
				kind = ""
			}
			if kind != "" {
				id = strings.TrimSuffix(id, suffix)
				if validSnapshotID(id) {
					if len(tokens) > 0 {
						var err error
						if kind == "preflight" {
							_, err = loadPreflightRequestCheckpoint(path, id, tokens[0])
						} else {
							_, err = loadFinalizeRequestCheckpoint(path, id, tokens[0])
						}
						if err != nil {
							log.Warn("Retain invalid %s request checkpoint for diagnosis %s: %v", kind, path, err)
							continue
						}
					}
					checkpoints = append(checkpoints, path)
				}
				continue
			}
		}
		if strings.HasSuffix(name, ".json") && validSnapshotID(strings.TrimSuffix(name, ".json")) {
			id := strings.TrimSuffix(name, ".json")
			manifestIDs[id] = struct{}{}
			if len(tokens) > 0 {
				manifest, err := loadManifestFile(path)
				if err != nil || manifest.ID != id || !verifyIncrementalSignature(manifest, tokens[0]) {
					invalidManifests = append(invalidManifests, path)
					continue
				}
				// Snapshot IDs are allocated monotonically, including after clock rollback.
				if latestManifestID == "" || manifest.ID > latestManifestID {
					latestManifestID = manifest.ID
				}
				if manifest.State == "ready" && (latestReadyID == "" || manifest.ID > latestReadyID) {
					latestReadyManifest = path
					latestReadyID = manifest.ID
				}
				if manifest.State == "preflight" && (latestPreflightID == "" || manifest.ID > latestPreflightID) {
					latestPreflightManifest = path
					latestPreflightID = manifest.ID
				}
				if manifest.State == "transferring" && (activeTransferID == "" || manifest.ID > activeTransferID) {
					activeTransferManifest = path
					activeTransferID = manifest.ID
					activeTransferBaseID = manifest.BaseJobID
					if activeTransferBaseID == "" && len(tokens) > 0 {
						checkpoint, err := loadFinalizeRequestCheckpoint(finalizeRequestCheckpointPath(dir, id), id, tokens[0])
						if err == nil {
							activeTransferBaseID = checkpoint.BaseJobID
						}
					}
				}
			}
			manifests = append(manifests, path)
		}
	}
	if activeTransferManifest != "" && activeTransferID < latestManifestID {
		activeTransferManifest = ""
		activeTransferBaseID = ""
	}
	sort.Strings(manifests)
	sort.Strings(invalidManifests)
	sort.Strings(checkpoints)
	var orphanCheckpoints []string
	for _, checkpoint := range checkpoints {
		name := strings.TrimPrefix(filepath.Base(checkpoint), ".")
		suffix := finalizeRequestCheckpointSuffix
		if strings.HasSuffix(name, preflightRequestCheckpointSuffix) {
			suffix = preflightRequestCheckpointSuffix
		}
		id := strings.TrimSuffix(name, suffix)
		if _, ok := manifestIDs[id]; !ok {
			orphanCheckpoints = append(orphanCheckpoints, checkpoint)
		}
	}
	checkpointRemoveCount := len(orphanCheckpoints) - retention
	for i := range checkpointRemoveCount {
		if err := os.Remove(orphanCheckpoints[i]); err == nil {
			removed++
			removedCheckpoints++
		} else if !os.IsNotExist(err) {
			log.Warn("Cannot prune sync job request checkpoint %s: %v", orphanCheckpoints[i], err)
		}
	}
	removedInvalidManifests := 0
	invalidRemoveCount := max(0, len(invalidManifests)-retention)
	for _, manifest := range invalidManifests[:invalidRemoveCount] {
		if err := os.Remove(manifest); err == nil {
			removed++
			removedInvalidManifests++
		} else if !os.IsNotExist(err) {
			log.Warn("Cannot prune invalid replication manifest %s: %v", manifest, err)
		}
	}
	removeCount := len(manifests) - retention
	removedManifests := 0
	for _, manifest := range manifests {
		if removedManifests >= removeCount {
			break
		}
		if manifest == activeTransferManifest {
			log.Debug("Retained active final sync manifest during pruning: snapshot=%s", strings.TrimSuffix(filepath.Base(manifest), ".json"))
			continue
		}
		if manifest == latestReadyManifest {
			log.Debug("Retained latest trusted ready manifest during pruning: snapshot=%s", latestReadyID)
			continue
		}
		if manifest == latestPreflightManifest {
			log.Debug("Retained latest preflight manifest during pruning: snapshot=%s", latestPreflightID)
			continue
		}
		if strings.TrimSuffix(filepath.Base(manifest), ".json") == activeTransferBaseID {
			log.Debug("Retained preflight chunk index for active final sync: snapshot=%s base_snapshot=%s", activeTransferID, activeTransferBaseID)
			continue
		}
		if err := os.Remove(manifest); err == nil {
			removed++
			removedManifests++
			id := strings.TrimSuffix(filepath.Base(manifest), ".json")
			for _, checkpointPath := range []string{finalizeRequestCheckpointPath(dir, id), preflightRequestCheckpointPath(dir, id)} {
				if err := os.Remove(checkpointPath); err == nil {
					removed++
					removedCheckpoints++
				} else if !os.IsNotExist(err) {
					log.Warn("Cannot prune sync job request checkpoint %s: %v", checkpointPath, err)
				}
			}
		} else {
			log.Warn("Cannot prune replication manifest %s: %v", manifest, err)
		}
	}
	if removed > 0 {
		if err := syncDirectory(dir); err != nil {
			log.Warn("Cannot persist replication manifest pruning in %s: %v", dir, err)
		}
	}
	if removedManifests > 0 {
		log.Info("Pruned %d replication manifest files; retained %d snapshot manifests", removedManifests, len(manifests)-removedManifests)
	}
	if removedInvalidManifests > 0 {
		log.Info("Pruned invalid replication manifest files: removed=%d retained_limit=%d", removedInvalidManifests, retention)
	}
	if removedCheckpoints > 0 {
		log.Info("Pruned sync job request checkpoints: removed=%d", removedCheckpoints)
	}
}

func writeJSON(w http.ResponseWriter, value any) {
	writeJSONStatus(w, http.StatusOK, value)
}

func writeManifestMaybeGzip(w http.ResponseWriter, r *http.Request, manifest *SnapshotManifest) {
	if rejectUnacceptableReplicationEncoding(w, r) {
		return
	}
	w.Header().Add("Vary", "Accept-Encoding")
	if requestPrefersGzip(r) {
		writer, err := gzip.NewWriterLevel(w, gzip.BestSpeed)
		if err != nil {
			log.Error("Cannot create compressed replication manifest response: snapshot=%s error=%v", manifest.ID, err)
			if requestRequiresGzip(r) {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		} else {
			w.Header().Set("Content-Encoding", "gzip")
			w.Header().Set("Content-Type", "application/json")
			encodeErr := writeManifestJSONContext(r.Context(), writer, manifest, false)
			closeErr := writer.Close()
			if err := errors.Join(encodeErr, closeErr); err != nil {
				log.Debug("Failed to write compressed replication manifest response: snapshot=%s error=%v", manifest.ID, err)
			}
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := writeManifestJSONContext(r.Context(), w, manifest, false); err != nil {
		log.Debug("Failed to write replication manifest response: snapshot=%s error=%v", manifest.ID, err)
	}
}

func requestAcceptsGzip(r *http.Request) bool {
	quality, found := requestGzipQuality(r)
	return found && quality > 0
}

func requestGzipQuality(r *http.Request) (float64, bool) {
	if quality, found := requestEncodingQuality(r, "gzip"); found {
		return quality, true
	}
	return requestEncodingQuality(r, "*")
}

func requestAcceptsIdentity(r *http.Request) bool {
	_, accepted := requestIdentityQuality(r)
	return accepted
}

func requestIdentityQuality(r *http.Request) (float64, bool) {
	if quality, found := requestEncodingQuality(r, "identity"); found {
		return quality, quality > 0
	}
	// An explicit identity quality overrides the wildcard; otherwise only *;q=0 excludes it.
	if quality, found := requestEncodingQuality(r, "*"); found && quality == 0 {
		return 0, false
	}
	return 1, true
}

func requestPrefersGzip(r *http.Request) bool {
	gzipQuality, gzipAccepted := requestGzipQuality(r)
	if !gzipAccepted || gzipQuality == 0 {
		return false
	}
	identityQuality, identityAccepted := requestIdentityQuality(r)
	return !identityAccepted || gzipQuality >= identityQuality
}

func requestRequiresGzip(r *http.Request) bool {
	gzipQuality, gzipAccepted := requestGzipQuality(r)
	if !gzipAccepted || gzipQuality == 0 {
		return false
	}
	identityQuality, identityAccepted := requestIdentityQuality(r)
	return !identityAccepted || gzipQuality > identityQuality
}

func rejectUnacceptableReplicationEncoding(w http.ResponseWriter, r *http.Request) bool {
	if requestAcceptsGzip(r) || requestAcceptsIdentity(r) {
		return false
	}
	w.Header().Add("Vary", "Accept-Encoding")
	w.WriteHeader(http.StatusNotAcceptable)
	return true
}

func requestEncodingQuality(r *http.Request, wanted string) (float64, bool) {
	for _, header := range r.Header.Values("Accept-Encoding") {
		for item := range strings.SplitSeq(header, ",") {
			encoding, parameters, _ := strings.Cut(strings.TrimSpace(item), ";")
			if !strings.EqualFold(strings.TrimSpace(encoding), wanted) {
				continue
			}
			quality := 1.0
			valid := true
			qualityFound := false
			for parameter := range strings.SplitSeq(parameters, ";") {
				name, value, ok := strings.Cut(strings.TrimSpace(parameter), "=")
				if !ok {
					if strings.EqualFold(strings.TrimSpace(parameter), "q") {
						valid = false
						break
					}
					continue
				}
				if !strings.EqualFold(strings.TrimSpace(name), "q") {
					continue
				}
				if qualityFound {
					valid = false
					break
				}
				parsed, ok := parseEncodingQuality(value)
				if !ok {
					valid = false
					break
				}
				quality = parsed
				qualityFound = true
			}
			if !valid {
				return 0, true
			}
			return quality, true
		}
	}
	return 0, false
}

func parseEncodingQuality(value string) (float64, bool) {
	value = strings.TrimSpace(value)
	if value == "0" {
		return 0, true
	}
	if value == "1" {
		return 1, true
	}
	if len(value) < 2 || value[1] != '.' || len(value) > 5 {
		return 0, false
	}
	quality := 0.0
	place := 0.1
	for _, digit := range value[2:] {
		if digit < '0' || digit > '9' {
			return 0, false
		}
		switch value[0] {
		case '1':
			if digit != '0' {
				return 0, false
			}
		case '0':
			quality += float64(digit-'0') * place
			place /= 10
		default:
			return 0, false
		}
	}
	switch value[0] {
	case '1':
		return 1, true
	case '0':
		return quality, true
	default:
		return 0, false
	}
}

func releaseChunkGzipBuffer(buffer *bytes.Buffer) {
	if buffer.Cap() <= maxPooledChunkGzipBuffer {
		buffer.Reset()
		chunkGzipBuffers.Put(buffer)
	}
}

func chunkCompressionLooksUseful(data []byte) bool {
	if len(data) <= chunkCompressionProbeSize*chunkCompressionProbes {
		return true
	}
	// Probe separated ranges to avoid a full gzip pass over incompressible chunks.
	probe := chunkGzipBuffers.Get().(*bytes.Buffer) //nolint:forcetypeassert // New and Put use only *bytes.Buffer.
	probe.Reset()
	writer := getChunkGzipWriter(probe)
	var compressedProbeBytes int
	for i := range chunkCompressionProbes {
		if i > 0 {
			probe.Reset()
			writer.Reset(probe)
		}
		start := (len(data) - chunkCompressionProbeSize) * i / (chunkCompressionProbes - 1)
		if _, err := writer.Write(data[start : start+chunkCompressionProbeSize]); err != nil {
			_ = writer.Close()
			releaseChunkGzipWriter(writer)
			releaseChunkGzipBuffer(probe)
			return true
		}
		if err := writer.Close(); err != nil {
			releaseChunkGzipWriter(writer)
			releaseChunkGzipBuffer(probe)
			return true
		}
		compressedProbeBytes += probe.Len()
	}
	releaseChunkGzipWriter(writer)
	releaseChunkGzipBuffer(probe)
	return compressedProbeBytes*100 <= chunkCompressionProbeSize*chunkCompressionProbes*(100-minChunkCompressionSave)
}

func gzipChunk(data []byte, force bool) (*bytes.Buffer, bool) {
	if !force && !chunkCompressionLooksUseful(data) {
		return nil, false
	}
	compressed := chunkGzipBuffers.Get().(*bytes.Buffer) //nolint:forcetypeassert // New and Put use only *bytes.Buffer.
	compressed.Reset()
	writer := getChunkGzipWriter(compressed)
	if _, err := writer.Write(data); err != nil {
		_ = writer.Close()
		releaseChunkGzipWriter(writer)
		releaseChunkGzipBuffer(compressed)
		return nil, false
	}
	if err := writer.Close(); err != nil {
		releaseChunkGzipWriter(writer)
		releaseChunkGzipBuffer(compressed)
		return nil, false
	}
	releaseChunkGzipWriter(writer)
	if !force && compressed.Len()*100 > len(data)*(100-minChunkCompressionSave) {
		releaseChunkGzipBuffer(compressed)
		return nil, false
	}
	return compressed, true
}

func writeChunk(w http.ResponseWriter, r *http.Request, data []byte) (int, bool, error) {
	w.Header().Add("Vary", "Accept-Encoding")
	if requestPrefersGzip(r) {
		forceGzip := requestRequiresGzip(r)
		if gzipData, ok := gzipChunk(data, forceGzip); ok {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Encoding", "gzip")
			expectedBytes := gzipData.Len()
			w.Header().Set("Content-Length", strconv.Itoa(expectedBytes))
			w.Header().Set(replicationEncodedBodyBytesHeader, strconv.Itoa(expectedBytes))
			responseBodyBytes, err := w.Write(gzipData.Bytes())
			releaseChunkGzipBuffer(gzipData)
			if err == nil && responseBodyBytes != expectedBytes {
				err = io.ErrShortWrite
			}
			return responseBodyBytes, true, err
		}
		if forceGzip {
			w.WriteHeader(http.StatusInternalServerError)
			return 0, false, errors.New("cannot encode replication chunk with required gzip coding")
		}
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set(replicationEncodedBodyBytesHeader, strconv.Itoa(len(data)))
	responseBodyBytes, err := w.Write(data)
	if err == nil && responseBodyBytes != len(data) {
		err = io.ErrShortWrite
	}
	return responseBodyBytes, false, err
}

func writeJSONStatus(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Debug("Failed to write replication JSON response: status=%d error=%v", status, err)
	}
}

func writeReplicationError(w http.ResponseWriter, code, message string) {
	w.Header().Set(replicationErrorCodeHeader, code)
	http.Error(w, message, http.StatusConflict)
}
