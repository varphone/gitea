// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"code.gitea.io/gitea/modules/log"
)

type finalSyncSession struct {
	id        string
	fence     *WriteFence
	cancel    context.CancelFunc
	expiresAt time.Time
	finished  chan struct{}
}

const baselineManifestName = "baseline.json"

func baselineManifestPath(dir string) string {
	return filepath.Join(dir, baselineManifestName)
}

func (s *controlServer) preflightPlan(now time.Time) (*SnapshotManifest, bool) {
	paths := []string{baselineManifestPath(s.cfg.SnapshotDir)}
	history, _ := filepath.Glob(filepath.Join(s.cfg.SnapshotDir, "*.json"))
	sort.Sort(sort.Reverse(sort.StringSlice(history)))
	paths = append(paths, history...)
	seen := map[string]struct{}{}
	var fallback *SnapshotManifest
	for _, path := range paths {
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		manifest, err := loadTrustedManifest(path, s.cfg.ControlToken, "ready")
		if err == nil {
			if manifest.FullScanAt.IsZero() ||
				(s.cfg.FullScanInterval > 0 && !now.Before(manifest.FullScanAt.Add(s.cfg.FullScanInterval))) {
				log.Info("Run a full disaster-recovery scan; the verification interval has elapsed")
				return manifest, true
			}
			return manifest, false
		}
		if fallback != nil {
			continue
		}
		manifest, err = loadTrustedManifestStates(path, s.cfg.ControlToken, "preflight", "transferring")
		if err != nil {
			continue
		}
		fallback = manifest
	}
	if fallback != nil {
		if fallback.FullScanAt.IsZero() ||
			(s.cfg.FullScanInterval > 0 && !now.Before(fallback.FullScanAt.Add(s.cfg.FullScanInterval))) {
			log.Info("Run a full disaster-recovery scan; the verification interval has elapsed")
			return fallback, true
		}
		log.Info("Reuse authenticated disaster-recovery manifest %s in state %s as the preflight base", fallback.ID, fallback.State)
		return fallback, false
	}
	log.Info("Run a full disaster-recovery scan; no trusted baseline is available")
	return nil, true
}

func (s *controlServer) startPrimary() error {
	if err := systemctlWithTimeout(s.cfg.ServiceTimeout, "start", s.cfg.GiteaServiceName); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ServiceTimeout)
	defer cancel()
	return readinessCheck(ctx, s.cfg.GiteaServiceName)
}

func (s *controlServer) retryPrimaryStart() {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.SnapshotTimeout)
		defer cancel()
		for {
			if err := s.startPrimary(); err == nil {
				log.Info("Recovered primary Gitea after incremental sync failure")
				return
			} else {
				log.Error("Failed to recover primary Gitea after incremental sync: %v", err)
			}
			select {
			case <-ctx.Done():
				log.Error("Giving up automatic primary Gitea recovery: %v", ctx.Err())
				return
			case <-time.After(5 * time.Second):
			}
		}
	}()
}

func (s *controlServer) recoverPrimary() {
	if err := s.startPrimary(); err != nil {
		log.Error("Failed to restart primary Gitea: %v", err)
		s.retryPrimaryStart()
	}
}

func (s *controlServer) preflight(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || s.cfg.Mode != modePrimary {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	if s.busy || s.session != nil {
		s.mu.Unlock()
		log.Warn("Reject preflight request: sync already in progress")
		http.Error(w, "sync already in progress", http.StatusConflict)
		return
	}
	if resumeID := r.URL.Query().Get("resume"); resumeID != "" {
		if !validSnapshotID(resumeID) {
			s.mu.Unlock()
			http.Error(w, "invalid preflight recovery checkpoint", http.StatusBadRequest)
			return
		}
		manifest, err := loadTrustedManifest(manifestPath(s.cfg.SnapshotDir, resumeID), s.cfg.ControlToken, "preflight")
		s.mu.Unlock()
		if err != nil {
			http.Error(w, "preflight recovery checkpoint is unavailable or invalid", http.StatusConflict)
			return
		}
		log.Info("Resuming preflight checkpoint %s", resumeID)
		writeJSON(w, manifest)
		return
	}
	id := time.Now().UTC().Format(snapshotIDLayout)
	job := &Snapshot{ID: id, State: snapshotStateCreating, CreatedAt: time.Now().UTC()}
	s.jobs[id] = job
	s.busy = true
	s.mu.Unlock()
	log.Info("Accepted preflight task %s", id)
	go s.runPreflightTask(id)
	writeJSONStatus(w, http.StatusAccepted, job)
}

func (s *controlServer) runPreflightTask(id string) {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.SnapshotTimeout)
	defer cancel()
	base, verifyAll := s.preflightPlan(time.Now().UTC())
	var manifest *SnapshotManifest
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		manifest, err = scanIncrementalTreeWithOptions(ctx, s.root(), base, verifyAll)
		if err == nil || !strings.Contains(err.Error(), "changed while scanning") {
			break
		}
		select {
		case <-ctx.Done():
			err = ctx.Err()
			attempt = 5
		case <-time.After(100 * time.Millisecond):
		}
	}
	if err != nil {
		log.Error("Preflight task %s failed: %v", id, err)
		s.failAsyncJob(id, err)
		return
	}
	manifest.ID = id
	manifest.State = "preflight"
	manifest.CreatedAt = time.Now().UTC()
	manifest.InstanceFingerprint = instanceFingerprint(s.cfg.ControlToken)
	if err := signIncrementalManifest(manifest, s.cfg.ControlToken); err != nil {
		log.Error("Preflight task %s signing failed: %v", id, err)
		s.failAsyncJob(id, err)
		return
	}
	if err := writeManifest(s.cfg.SnapshotDir, manifest); err != nil {
		log.Error("Preflight task %s persist failed: %v", id, err)
		s.failAsyncJob(id, err)
		return
	}
	s.setTaskManifest(manifest)
	log.Info("Preflight task %s completed", id)
	s.completeAsyncJob(id, manifest.Snapshot)
	pruneManifestFiles(s.cfg.SnapshotDir, s.cfg.SnapshotRetention)
}

func (s *controlServer) finalize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || s.cfg.Mode != modePrimary {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	if s.busy || s.session != nil {
		s.mu.Unlock()
		log.Warn("Reject finalize request: sync already in progress")
		http.Error(w, "sync already in progress", http.StatusConflict)
		return
	}
	baseID := r.URL.Query().Get("base")
	if !validSnapshotID(baseID) {
		s.mu.Unlock()
		log.Warn("Reject finalize request: invalid preflight base %q", baseID)
		http.Error(w, "a valid preflight base is required", http.StatusBadRequest)
		return
	}
	if manifest := s.taskManifests[baseID]; manifest != nil && manifest.State == "preflight" {
		// A just-completed preflight task is authoritative in memory and avoids
		// a second disk parse before finalize begins.
	} else if _, err := loadTrustedManifest(manifestPath(s.cfg.SnapshotDir, baseID), s.cfg.ControlToken, "preflight"); err != nil {
		s.mu.Unlock()
		log.Warn("Reject finalize request for base %s: %v", baseID, err)
		http.Error(w, "preflight base is unavailable or invalid: "+err.Error(), http.StatusConflict)
		return
	}
	id := time.Now().UTC().Format(snapshotIDLayout)
	job := &Snapshot{ID: id, State: snapshotStateCreating, CreatedAt: time.Now().UTC()}
	s.jobs[id] = job
	s.busy = true
	s.mu.Unlock()
	log.Info("Accepted finalize task %s for preflight base %s", id, baseID)
	go s.runFinalizeTask(id, baseID)
	writeJSONStatus(w, http.StatusAccepted, job)
}

func (s *controlServer) runFinalizeTask(id, baseID string) {
	base := s.getTaskManifest(baseID)
	if base == nil || base.State != "preflight" {
		var err error
		base, err = loadTrustedManifest(manifestPath(s.cfg.SnapshotDir, baseID), s.cfg.ControlToken, "preflight")
		if err != nil {
			log.Error("Finalize task %s cannot load preflight base %s: %v", id, baseID, err)
			s.failAsyncJob(id, fmt.Errorf("preflight base is unavailable or invalid: %w", err))
			return
		}
	}

	scanCtx, scanCancel := context.WithTimeout(context.Background(), s.cfg.SnapshotTimeout)
	fence, err := AcquireSnapshotFence(scanCtx)
	if err == nil {
		err = ensureSocketActivationDisabled(scanCtx, s.cfg.GiteaServiceName)
	}
	if err != nil {
		if fence != nil {
			_ = fence.Release()
		}
		scanCancel()
		log.Error("Finalize task %s failed before stopping primary: %v", id, err)
		s.failAsyncJob(id, err)
		return
	}
	scanCancel()
	outageCtx, outageCancel := context.WithTimeout(context.Background(), s.cfg.FinalSessionTimeout)
	stopAttempted := true
	if err = systemctl(outageCtx, "stop", s.cfg.GiteaServiceName); err != nil {
		if stopAttempted {
			s.recoverPrimary()
		}
		_ = fence.Release()
		outageCancel()
		log.Error("Finalize task %s failed while stopping primary: %v", id, err)
		s.failAsyncJob(id, err)
		return
	}

	manifest, err := scanIncrementalTreeWithBase(outageCtx, s.root(), base)
	if err != nil {
		s.recoverPrimary()
		_ = fence.Release()
		outageCancel()
		log.Error("Finalize task %s final scan failed within primary outage budget: %v", id, err)
		s.failAsyncJob(id, err)
		return
	}
	manifest.ID = id
	manifest.State = "transferring"
	manifest.CreatedAt = time.Now().UTC()
	manifest.InstanceFingerprint = instanceFingerprint(s.cfg.ControlToken)
	if err := signIncrementalManifest(manifest, s.cfg.ControlToken); err != nil {
		s.recoverPrimary()
		_ = fence.Release()
		outageCancel()
		log.Error("Finalize task %s signing failed: %v", id, err)
		s.failAsyncJob(id, err)
		return
	}
	if err := writeManifest(s.cfg.SnapshotDir, manifest); err != nil {
		s.recoverPrimary()
		_ = fence.Release()
		outageCancel()
		log.Error("Finalize task %s persist failed: %v", id, err)
		s.failAsyncJob(id, err)
		return
	}
	s.setTaskManifest(manifest)
	deadline, _ := outageCtx.Deadline()
	session := &finalSyncSession{id: manifest.ID, fence: fence, cancel: outageCancel, expiresAt: deadline, finished: make(chan struct{})}
	s.mu.Lock()
	s.session = session
	job := s.jobs[manifest.ID]
	if job == nil {
		job = &Snapshot{ID: manifest.ID, CreatedAt: manifest.CreatedAt}
		s.jobs[manifest.ID] = job
	}
	*job = manifest.Snapshot
	s.busy = false
	s.mu.Unlock()
	log.Info("Finalize task %s completed with transferring manifest %s", id, manifest.ID)
	go s.expireSession(session)
}

func (s *controlServer) failAsyncJob(id string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[id]
	if job == nil {
		job = &Snapshot{ID: id, CreatedAt: time.Now().UTC()}
		s.jobs[id] = job
	}
	job.State = "failed"
	job.Error = err.Error()
	s.busy = false
}

func (s *controlServer) completeAsyncJob(id string, snapshot Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[id]
	if job == nil {
		job = &Snapshot{ID: id, CreatedAt: snapshot.CreatedAt}
		s.jobs[id] = job
	}
	*job = snapshot
	job.Error = ""
	s.busy = false
}

func (s *controlServer) expireSession(session *finalSyncSession) {
	timeout := time.Until(session.expiresAt)
	if session.expiresAt.IsZero() {
		timeout = s.cfg.FinalSessionTimeout
		if timeout <= 0 {
			timeout = defaultConfig().FinalSessionTimeout
		}
	}
	if timeout < 0 {
		timeout = 0
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-timer.C:
		log.Warn("Final sync session %s received no completion within %s; restarting primary Gitea", session.id, timeout)
		_ = s.finishSession(session.id, false)
	case <-session.finished:
	}
}

func (s *controlServer) finishSession(id string, success bool) error {
	s.mu.Lock()
	if s.session == nil || s.session.id != id {
		s.mu.Unlock()
		return errors.New("sync session is not active")
	}
	session := s.session
	s.session = nil
	job := s.jobs[id]
	s.mu.Unlock()
	startErr := s.startPrimary()
	if startErr != nil {
		s.retryPrimaryStart()
	}
	releaseErr := session.fence.Release()
	finishErr := errors.Join(startErr, releaseErr)
	if job != nil {
		s.mu.Lock()
		if success && finishErr == nil {
			job.State, job.Error = "ready", ""
		} else {
			job.State = "failed"
			if finishErr != nil {
				job.Error = finishErr.Error()
			} else {
				job.Error = "sync session aborted"
			}
		}
		jobCopy := *job
		s.mu.Unlock()
		if manifest, err := loadManifestFile(manifestPath(s.cfg.SnapshotDir, id)); err == nil {
			manifest.Snapshot = jobCopy
			if err := signIncrementalManifest(manifest, s.cfg.ControlToken); err != nil {
				log.Error("Sign completed disaster-recovery manifest: %v", err)
			} else if err := writeManifest(s.cfg.SnapshotDir, manifest); err != nil {
				log.Error("Persist completed disaster-recovery manifest: %v", err)
			} else if success && finishErr == nil {
				s.setTaskManifest(manifest)
				if err := writeManifestAt(baselineManifestPath(s.cfg.SnapshotDir), manifest); err != nil {
					log.Error("Persist disaster-recovery scan baseline: %v", err)
				}
			} else {
				s.setTaskManifest(manifest)
			}
		}
	}
	if session.cancel != nil {
		session.cancel()
	}
	close(session.finished)
	s.mu.Lock()
	s.busy = false
	s.mu.Unlock()
	s.prune()
	return finishErr
}

func (s *controlServer) syncSnapshot(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, syncJobsPath+"/"), "/")
	if len(parts) != 3 || !validSnapshotID(parts[0]) {
		http.NotFound(w, r)
		return
	}
	id, action, value := parts[0], parts[1], parts[2]
	if action == "chunks" && r.Method == http.MethodGet {
		if len(value) != 64 {
			http.NotFound(w, r)
			return
		}
		manifest := s.getTaskManifest(id)
		if manifest == nil {
			var err error
			manifest, err = loadManifestFile(manifestPath(s.cfg.SnapshotDir, id))
			if err != nil {
				http.NotFound(w, r)
				return
			}
		}
		location, ok := indexManifest(manifest)[value]
		if !ok {
			http.NotFound(w, r)
			return
		}
		data, err := readChunk(s.root(), location, value)
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		_, _ = w.Write(data)
		return
	}
	if action == "session" && value == "complete" && r.Method == http.MethodPost {
		if err := s.finishSession(id, true); err != nil {
			s.mu.RLock()
			job := s.jobs[id]
			alreadyReady := job != nil && job.State == "ready"
			s.mu.RUnlock()
			if !alreadyReady {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
		}
		writeJSON(w, map[string]string{"status": "ready"})
		return
	}
	if action == "session" && value == "abort" && r.Method == http.MethodPost {
		if err := s.finishSession(id, false); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, map[string]string{"status": "aborted"})
		return
	}
	http.NotFound(w, r)
}

func (s *controlServer) abortActiveSession() {
	s.mu.RLock()
	session := s.session
	s.mu.RUnlock()
	if session != nil {
		_ = s.finishSession(session.id, false)
	}
}

func removeLegacyArchives(dir string) {
	files, _ := os.ReadDir(dir)
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".tar.gz") || strings.HasSuffix(file.Name(), ".tar.gz.tmp") {
			_ = os.Remove(filepath.Join(dir, file.Name()))
		}
	}
}
