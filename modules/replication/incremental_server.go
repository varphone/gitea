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
	"strings"
	"time"

	"gitea.dev/modules/log"
)

type finalSyncSession struct {
	id        string
	fence     *WriteFence
	cancel    context.CancelFunc
	expiresAt time.Time
	finished  chan struct{}
}

const (
	baselineManifestName = "baseline.json"
	// Reuse recent checkpoints for request retries; scheduled restores must rescan live data.
	reusablePreflightMaxAge = 5 * time.Minute
)

func baselineManifestPath(dir string) string {
	return filepath.Join(dir, baselineManifestName)
}

func (s *controlServer) fullScanDue(manifest *SnapshotManifest, now time.Time) bool {
	return manifest.FullScanAt.IsZero() ||
		(s.cfg.FullScanInterval > 0 && !now.Before(manifest.FullScanAt.Add(s.cfg.FullScanInterval)))
}

func (s *controlServer) finalSessionTimeout() time.Duration {
	if s.cfg.FinalSessionTimeout > 0 {
		return s.cfg.FinalSessionTimeout
	}
	return defaultConfig().FinalSessionTimeout
}

func (s *controlServer) reusablePreflightLocked(now time.Time) *SnapshotManifest {
	var latest *SnapshotManifest
	for _, manifest := range s.taskManifests {
		if manifest.State != "preflight" || s.fullScanDue(manifest, now) || !preflightIsFresh(manifest, now) {
			continue
		}
		if latest == nil || manifest.CreatedAt.After(latest.CreatedAt) {
			manifestCopy := *manifest
			latest = &manifestCopy
		}
	}
	return latest
}

func preflightIsFresh(manifest *SnapshotManifest, now time.Time) bool {
	age := now.Sub(manifest.CreatedAt)
	return age >= -reusablePreflightMaxAge && age <= reusablePreflightMaxAge
}

func (s *controlServer) preflightPlan(now time.Time) (*SnapshotManifest, bool) {
	paths := []string{baselineManifestPath(s.cfg.SnapshotDir)}
	history, err := listManifestPaths(s.cfg.SnapshotDir)
	if err != nil {
		log.Warn("Cannot list replication manifests in %s for preflight planning: %v", s.cfg.SnapshotDir, err)
	}
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
				log.Info("Run full disaster-recovery verification from baseline %s: last_full_scan=%s interval=%s", manifest.ID, manifest.FullScanAt, s.cfg.FullScanInterval)
				return manifest, true
			}
			log.Info("Run incremental disaster-recovery preflight from ready baseline %s: last_full_scan=%s", manifest.ID, manifest.FullScanAt)
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
			log.Info("Run full disaster-recovery verification from fallback %s in state %s: last_full_scan=%s interval=%s", fallback.ID, fallback.State, fallback.FullScanAt, s.cfg.FullScanInterval)
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
		return fmt.Errorf("start primary service %s: %w", s.cfg.GiteaServiceName, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ServiceTimeout)
	defer cancel()
	if err := readinessCheck(ctx, s.cfg.GiteaServiceName); err != nil {
		return fmt.Errorf("wait for primary service %s readiness: %w", s.cfg.GiteaServiceName, err)
	}
	return nil
}

func (s *controlServer) retryPrimaryStart() {
	s.mu.Lock()
	if s.primaryRecoveryPending {
		s.mu.Unlock()
		log.Info("Primary Gitea recovery retry is already active")
		return
	}
	s.primaryRecoveryPending = true
	s.mu.Unlock()
	log.Warn("Primary Gitea recovery is pending; new replication jobs are blocked")
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.SnapshotTimeout)
		defer cancel()
		attempt := 0
		lastFailureLog := time.Time{}
		for {
			attempt++
			started := time.Now()
			if err := s.startPrimary(); err == nil {
				s.mu.Lock()
				s.primaryRecoveryPending = false
				s.mu.Unlock()
				log.Info("Recovered primary Gitea after incremental sync failure: attempt=%d duration=%s", attempt, time.Since(started))
				return
			} else {
				if lastFailureLog.IsZero() || time.Since(lastFailureLog) >= time.Minute {
					log.Error("Failed to recover primary Gitea after incremental sync: attempt=%d duration=%s error=%v", attempt, time.Since(started), err)
					lastFailureLog = time.Now()
				} else {
					log.Debug("Primary Gitea recovery attempt failed: attempt=%d duration=%s error=%v", attempt, time.Since(started), err)
				}
			}
			select {
			case <-ctx.Done():
				log.Error("Automatic primary Gitea recovery timed out; replication jobs remain blocked: %v", ctx.Err())
				return
			case <-time.After(5 * time.Second):
			}
		}
	}()
}

func (s *controlServer) recoverPrimary() {
	started := time.Now()
	if err := s.startPrimary(); err != nil {
		log.Error("Failed to restart primary Gitea after %s: %v", time.Since(started), err)
		s.retryPrimaryStart()
		return
	}
	log.Info("Restarted primary Gitea after replication failure: duration=%s", time.Since(started))
}

func (s *controlServer) preflight(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || s.cfg.Mode != modePrimary {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	if s.busy || s.session != nil || s.primaryRecoveryPending {
		recoveryPending := s.primaryRecoveryPending
		s.mu.Unlock()
		w.Header().Set("Retry-After", "1")
		if recoveryPending {
			log.Debug("Preflight request deferred: primary Gitea recovery is pending")
			http.Error(w, "sync already in progress: primary Gitea recovery is pending", http.StatusConflict)
		} else {
			log.Debug("Preflight request deferred: sync already in progress")
			http.Error(w, "sync already in progress", http.StatusConflict)
		}
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
		writeJSONMaybeGzip(w, r, manifest)
		return
	}
	if manifest := s.reusablePreflightLocked(time.Now().UTC()); manifest != nil {
		s.mu.Unlock()
		log.Info("Reuse completed preflight checkpoint %s", manifest.ID)
		writeJSONMaybeGzip(w, r, manifest)
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
	taskStarted := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.SnapshotTimeout)
	defer cancel()
	base, verifyAll := s.preflightPlan(time.Now().UTC())
	scanMode, baseID := "incremental", "none"
	if verifyAll {
		scanMode = "full verification"
	}
	if base != nil {
		baseID = base.ID
	}
	log.Info("Preflight task %s scanning %s snapshot: base=%s", id, scanMode, baseID)
	scanStarted := time.Now()
	var manifest *SnapshotManifest
	var err error
scanAttempts:
	for attempt := 1; attempt <= 5; attempt++ {
		manifest, err = scanIncrementalTreeWithOptions(ctx, s.root(), base, verifyAll)
		if err == nil || !errors.Is(err, errIncrementalTreeChanged) {
			break
		}
		if attempt == 5 {
			log.Warn("Preflight task %s scan still found changing files on final attempt=%d/5: %v", id, attempt, err)
			break
		}
		log.Warn("Preflight task %s scan found changing files; retrying attempt=%d/5 error=%v", id, attempt+1, err)
		select {
		case <-ctx.Done():
			err = ctx.Err()
			break scanAttempts
		case <-time.After(100 * time.Millisecond):
		}
	}
	if err != nil {
		log.Error("Preflight task %s failed after %s: %v", id, time.Since(taskStarted), err)
		s.failAsyncJob(id, err)
		return
	}
	scanDuration := time.Since(scanStarted)
	manifest.ID = id
	manifest.State = "preflight"
	manifest.CreatedAt = time.Now().UTC()
	manifest.InstanceFingerprint = instanceFingerprint(s.cfg.ControlToken)
	if err := signIncrementalManifest(manifest, s.cfg.ControlToken); err != nil {
		log.Error("Preflight task %s signing failed after %s: %v", id, time.Since(taskStarted), err)
		s.failAsyncJob(id, err)
		return
	}
	if err := writeManifest(s.cfg.SnapshotDir, manifest); err != nil {
		log.Error("Preflight task %s persist failed after %s: %v", id, time.Since(taskStarted), err)
		s.failAsyncJob(id, err)
		return
	}
	s.setTaskManifest(manifest)
	log.Info("Preflight task %s completed: mode=%s base=%s files=%d bytes=%d scan_duration=%s total_duration=%s", id, scanMode, baseID, manifest.FileCount, manifest.Size, scanDuration, time.Since(taskStarted))
	s.completeAsyncJob(id, manifest.Snapshot)
	s.prune()
}

func (s *controlServer) finalize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || s.cfg.Mode != modePrimary {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	baseID := r.URL.Query().Get("base")
	if !validSnapshotID(baseID) {
		log.Warn("Reject finalize request: invalid preflight base %q", baseID)
		http.Error(w, "a valid preflight base is required", http.StatusBadRequest)
		return
	}
	requestID := r.Header.Get("Idempotency-Key")
	if requestID != "" && !validReplicationRequestID(requestID) {
		http.Error(w, "invalid sync job request ID", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	if requestID != "" {
		for _, existing := range s.jobs {
			if existing.RequestID != requestID {
				continue
			}
			if existing.BaseJobID != baseID {
				s.mu.Unlock()
				http.Error(w, "sync job request ID was already used for another preflight base", http.StatusConflict)
				return
			}
			job := *existing
			s.mu.Unlock()
			if job.State == snapshotStateCreating || job.State == "transferring" {
				writeJSONStatus(w, http.StatusAccepted, job)
				return
			}
			http.Error(w, fmt.Sprintf("sync job request has already ended in state %s: %s", job.State, job.Error), http.StatusConflict)
			return
		}
	}
	if s.busy || s.session != nil || s.primaryRecoveryPending {
		recoveryPending := s.primaryRecoveryPending
		s.mu.Unlock()
		w.Header().Set("Retry-After", "1")
		if recoveryPending {
			log.Debug("Finalize request deferred: primary Gitea recovery is pending")
			http.Error(w, "sync already in progress: primary Gitea recovery is pending", http.StatusConflict)
		} else {
			log.Debug("Finalize request deferred: sync already in progress")
			http.Error(w, "sync already in progress", http.StatusConflict)
		}
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
	job := &Snapshot{ID: id, State: snapshotStateCreating, CreatedAt: time.Now().UTC(), RequestID: requestID, BaseJobID: baseID}
	s.jobs[id] = job
	s.busy = true
	s.mu.Unlock()
	log.Info("Accepted finalize task %s for preflight base %s", id, baseID)
	go s.runFinalizeTask(id, baseID, requestID)
	writeJSONStatus(w, http.StatusAccepted, job)
}

func (s *controlServer) runFinalizeTask(id, baseID, requestID string) {
	taskStarted := time.Now()
	base := s.getTaskManifest(baseID)
	if base == nil || base.State != "preflight" {
		var err error
		base, err = loadTrustedManifest(manifestPath(s.cfg.SnapshotDir, baseID), s.cfg.ControlToken, "preflight")
		if err != nil {
			log.Error("Finalize task %s cannot load preflight base %s after %s: %v", id, baseID, time.Since(taskStarted), err)
			s.failAsyncJob(id, fmt.Errorf("preflight base is unavailable or invalid: %w", err))
			return
		}
	}
	log.Info("Finalize task %s starting from preflight %s: base_files=%d", id, baseID, base.FileCount)

	scanCtx, scanCancel := context.WithTimeout(context.Background(), s.cfg.SnapshotTimeout)
	fenceStarted := time.Now()
	fence, err := acquireSnapshotFence(scanCtx)
	if err == nil {
		err = ensureSocketActivationDisabled(scanCtx, s.cfg.GiteaServiceName)
	}
	if err != nil {
		if fence != nil {
			_ = fence.Release()
		}
		scanCancel()
		log.Error("Finalize task %s failed before stopping primary after %s: %v", id, time.Since(fenceStarted), err)
		s.failAsyncJob(id, err)
		return
	}
	log.Info("Finalize task %s acquired snapshot fence after %s", id, time.Since(fenceStarted))
	scanCancel()
	outageCtx, outageCancel := context.WithTimeout(context.Background(), s.finalSessionTimeout())
	outageStarted := time.Now()
	stopStarted := time.Now()
	stopAttempted := true
	if err = systemctl(outageCtx, "stop", s.cfg.GiteaServiceName); err != nil {
		if stopAttempted {
			s.recoverPrimary()
		}
		_ = fence.Release()
		outageCancel()
		log.Error("Finalize task %s failed while stopping primary after %s: %v", id, time.Since(stopStarted), err)
		s.failAsyncJob(id, err)
		return
	}

	log.Info("Finalize task %s stopped primary after %s", id, time.Since(stopStarted))
	finalScanStarted := time.Now()
	manifest, err := scanIncrementalTreeWithBase(outageCtx, s.root(), base)
	if err != nil {
		s.recoverPrimary()
		_ = fence.Release()
		outageCancel()
		log.Error("Finalize task %s final scan failed: scan_duration=%s primary_outage=%s error=%v", id, time.Since(finalScanStarted), time.Since(outageStarted), err)
		s.failAsyncJob(id, err)
		return
	}
	log.Info("Finalize task %s final scan completed: files=%d bytes=%d scan_duration=%s primary_outage=%s", id, manifest.FileCount, manifest.Size, time.Since(finalScanStarted), time.Since(outageStarted))
	manifest.ID = id
	manifest.State = "transferring"
	manifest.CreatedAt = time.Now().UTC()
	manifest.InstanceFingerprint = instanceFingerprint(s.cfg.ControlToken)
	manifest.RequestID, manifest.BaseJobID = requestID, baseID
	if err := signIncrementalManifest(manifest, s.cfg.ControlToken); err != nil {
		s.recoverPrimary()
		_ = fence.Release()
		outageCancel()
		log.Error("Finalize task %s signing failed: primary_outage=%s total_duration=%s error=%v", id, time.Since(outageStarted), time.Since(taskStarted), err)
		s.failAsyncJob(id, err)
		return
	}
	if err := writeManifest(s.cfg.SnapshotDir, manifest); err != nil {
		s.recoverPrimary()
		_ = fence.Release()
		outageCancel()
		log.Error("Finalize task %s persist failed: primary_outage=%s total_duration=%s error=%v", id, time.Since(outageStarted), time.Since(taskStarted), err)
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
	log.Info("Finalize task %s prepared transfer session %s: primary_outage=%s remaining_budget=%s total_duration=%s", id, manifest.ID, time.Since(outageStarted), time.Until(deadline), time.Since(taskStarted))
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
		timeout = s.finalSessionTimeout()
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
	s.chunkMu.Lock()
	defer s.chunkMu.Unlock()
	finishStarted := time.Now()
	requestedAction := "complete"
	if !success {
		requestedAction = "abort"
	}
	s.mu.Lock()
	if s.session == nil || s.session.id != id {
		s.mu.Unlock()
		log.Info("Final sync session %s ignored %s request: no active session", id, requestedAction)
		return errors.New("sync session is not active")
	}
	session := s.session
	s.session = nil
	s.busy = true
	job := s.jobs[id]
	s.mu.Unlock()
	log.Info("Final sync session %s finalizing: action=%s", id, requestedAction)
	primaryStartStarted := time.Now()
	startErr := s.startPrimary()
	primaryStartDuration := time.Since(primaryStartStarted)
	if startErr != nil {
		log.Error("Final sync session %s could not restart primary Gitea after %s: %v", id, primaryStartDuration, startErr)
		s.retryPrimaryStart()
	}
	fenceReleaseStarted := time.Now()
	releaseErr := session.fence.Release()
	fenceReleaseDuration := time.Since(fenceReleaseStarted)
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
		manifest, loadErr := loadManifestFile(manifestPath(s.cfg.SnapshotDir, id))
		if loadErr != nil {
			log.Warn("Cannot load finalized replication manifest %s from disk; using in-memory copy: %v", id, loadErr)
			manifest = s.getTaskManifest(id)
		}
		var manifestErr error
		if manifest == nil {
			if loadErr == nil {
				manifestErr = fmt.Errorf("load finalized replication manifest %s: no manifest available", id)
			} else {
				manifestErr = fmt.Errorf("load finalized replication manifest %s: %w", id, loadErr)
			}
		} else {
			manifest.Snapshot = jobCopy
			if err := signIncrementalManifest(manifest, s.cfg.ControlToken); err != nil {
				manifestErr = fmt.Errorf("sign finalized replication manifest %s: %w", id, err)
			} else if err := writeManifest(s.cfg.SnapshotDir, manifest); err != nil {
				manifestErr = fmt.Errorf("persist finalized replication manifest %s: %w", id, err)
			}
		}
		if manifestErr != nil {
			finishErr = errors.Join(finishErr, manifestErr)
			log.Error("Finalize replication manifest %s failed: %v", id, manifestErr)
			s.mu.Lock()
			job.State, job.Error = "failed", finishErr.Error()
			jobCopy = *job
			s.mu.Unlock()
			if manifest != nil {
				manifest.Snapshot = jobCopy
				if err := signIncrementalManifest(manifest, s.cfg.ControlToken); err != nil {
					log.Error("Sign failed disaster-recovery manifest: snapshot=%s error=%v", id, err)
				} else if err := writeManifest(s.cfg.SnapshotDir, manifest); err != nil {
					log.Error("Persist failed disaster-recovery manifest: snapshot=%s error=%v", id, err)
				}
				s.setTaskManifest(manifest)
			}
		} else {
			s.setTaskManifest(manifest)
			if success && finishErr == nil {
				if err := writeManifestAt(baselineManifestPath(s.cfg.SnapshotDir), manifest); err != nil {
					log.Error("Persist disaster-recovery scan baseline: %v", err)
				}
			}
		}
	}
	if session.cancel != nil {
		session.cancel()
	}
	close(session.finished)
	s.prune()
	s.mu.Lock()
	s.busy = false
	s.mu.Unlock()
	if finishErr != nil {
		log.Error("Final sync session %s finalized with errors: action=%s duration=%s primary_start=%s fence_release=%s error=%v", id, requestedAction, time.Since(finishStarted), primaryStartDuration, fenceReleaseDuration, finishErr)
	} else {
		log.Info("Final sync session %s finalized: action=%s duration=%s primary_start=%s fence_release=%s", id, requestedAction, time.Since(finishStarted), primaryStartDuration, fenceReleaseDuration)
	}
	return finishErr
}

func (s *controlServer) canServeChunk(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job := s.jobs[id]
	if job == nil {
		return false
	}
	if job.State == "preflight" {
		return true
	}
	return job.State == "transferring" && s.session != nil && s.session.id == id
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
		s.chunkMu.RLock()
		chunkLockHeld := true
		defer func() {
			if chunkLockHeld {
				s.chunkMu.RUnlock()
			}
		}()
		if !s.canServeChunk(id) {
			http.NotFound(w, r)
			return
		}
		slots, acquired := s.tryAcquireChunkSlot()
		if !acquired {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "replication chunk service is busy", http.StatusServiceUnavailable)
			return
		}
		defer func() { <-slots }()
		location, ok, indexed := s.getTaskChunkLocation(id, value)
		if !indexed {
			http.NotFound(w, r)
			return
		}
		if !ok {
			http.NotFound(w, r)
			return
		}
		data, err := readChunk(s.root(), location, value)
		if err != nil {
			for _, alternate := range s.getTaskChunkAlternates(id, value) {
				data, err = readChunk(s.root(), alternate, value)
				if err == nil {
					log.Debug("Served replication chunk from alternate manifest location: snapshot=%s hash=%s path=%s", id, value, alternate.Path)
					break
				}
				log.Debug("Alternate replication chunk location failed: snapshot=%s hash=%s path=%s error=%v", id, value, alternate.Path, err)
			}
		}
		s.chunkMu.RUnlock()
		chunkLockHeld = false
		if err != nil {
			log.Warn("Failed to serve replication chunk: snapshot=%s hash=%s error=%v", id, value, err)
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		bodyBytes, compressed, err := writeChunk(w, r, data)
		if err != nil {
			log.Warn("Failed to write replication chunk response: snapshot=%s hash=%s payload_bytes=%d response_body_bytes=%d gzip=%t error=%v", id, value, len(data), bodyBytes, compressed, err)
			return
		}
		log.Debug("Served replication chunk: snapshot=%s hash=%s payload_bytes=%d response_body_bytes=%d gzip=%t", id, value, len(data), bodyBytes, compressed)
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
	files, err := os.ReadDir(dir)
	if err != nil {
		log.Warn("Cannot list replication snapshot directory %s for legacy archive cleanup: %v", dir, err)
		return
	}
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".tar.gz") || strings.HasSuffix(file.Name(), ".tar.gz.tmp") {
			path := filepath.Join(dir, file.Name())
			if err := os.Remove(path); err != nil {
				log.Warn("Cannot remove legacy replication archive %s: %v", path, err)
			}
		}
	}
}
