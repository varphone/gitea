// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"

	"gitea.dev/modules/json"
	"gitea.dev/modules/log"
)

type finalSyncSession struct {
	id                    string
	fence                 *WriteFence
	expiresAt             time.Time // guarded by chunkMu
	preReleaseDeadline    time.Time // guarded by chunkMu
	postReleaseDeadline   time.Time // guarded by chunkMu
	finished              chan struct{}
	renew                 chan struct{}
	lastProgressRenewalID string // guarded by chunkMu
	primaryResumed        bool   // guarded by chunkMu
	expired               bool   // guarded by chunkMu
}

var (
	errFinalSyncSessionInactive         = errors.New("sync session is not active")
	errFinalSyncSessionExpired          = errors.New("sync session has expired")
	errFinalSyncSessionInvalidRenewalID = errors.New("invalid sync session renewal ID")
	errPrimaryRecoveryPending           = errors.New("primary Gitea recovery is still pending")
)

const (
	baselineManifestName    = "baseline.json"
	snapshotIDHighWaterName = ".snapshot-id-high-water"
	// How many session timeouts a pre-release standby may use while it reports progress.
	preReleaseSessionWindowMultiplier = 3
)

// primaryRecoveryRetryInterval is a variable so tests can shorten the retry loop.
var primaryRecoveryRetryInterval = 5 * time.Second

func scanReplicationDataTree(ctx context.Context, root string, base *SnapshotManifest, verifyAll bool, snapshotID string) (*SnapshotManifest, error) {
	if err := validateNoNestedMounts(root); err != nil {
		return nil, fmt.Errorf("validate replication data root mounts before scan: %w", err)
	}
	manifest, err := scanIncrementalTreeWithoutDigestForTask(ctx, root, base, verifyAll, snapshotID)
	if err != nil {
		return nil, err
	}
	manifest.OAuth2SigningConfig, err = oauth2SigningConfigIdentity()
	if err != nil {
		return nil, fmt.Errorf("identify OAuth2 signing config for replication manifest: %w", err)
	}
	if err := validateNoNestedMounts(root); err != nil {
		return nil, fmt.Errorf("validate replication data root mounts after scan: %w", err)
	}
	return manifest, nil
}

func baselineManifestPath(dir string) string {
	return filepath.Join(dir, baselineManifestName)
}

func (s *controlServer) fullScanDue(manifest *SnapshotManifest, now time.Time) bool {
	if manifest.FormatVersion < incrementalFormatVersion {
		return true
	}
	if manifest.FullScanAt.IsZero() {
		return true
	}
	if s.cfg.FullScanInterval <= 0 {
		return false
	}
	return now.Before(manifest.FullScanAt) || !now.Before(manifest.FullScanAt.Add(s.cfg.FullScanInterval))
}

func (s *controlServer) finalSessionTimeout() time.Duration {
	if s.cfg.FinalSessionTimeout > 0 {
		return s.cfg.FinalSessionTimeout
	}
	return defaultConfig().FinalSessionTimeout
}

func (s *controlServer) primaryOutageTimeout() time.Duration {
	if s.cfg != nil && s.cfg.PrimaryOutageTimeout > 0 {
		return s.cfg.PrimaryOutageTimeout
	}
	return defaultConfig().PrimaryOutageTimeout
}

func maxPreReleaseSessionAge(timeout time.Duration) time.Duration {
	const maxDuration = time.Duration(1<<63 - 1)
	age := timeout
	for range preReleaseSessionWindowMultiplier {
		if age > maxDuration-timeout {
			return maxDuration
		}
		age += timeout
	}
	return age
}

func sessionExpiry(now time.Time, timeout time.Duration, deadline time.Time) time.Time {
	expiresAt := now.Add(timeout)
	if !deadline.IsZero() && expiresAt.After(deadline) {
		return deadline
	}
	return expiresAt
}

func (s *controlServer) postReleaseSessionMaxAge() time.Duration {
	maxAge := s.finalSessionTimeout()
	if s.cfg != nil && s.cfg.SnapshotTimeout > maxAge {
		maxAge = s.cfg.SnapshotTimeout
	}
	return maxAge
}

func newerManifest(candidate, current *SnapshotManifest) bool {
	if current == nil {
		return true
	}
	return candidate.ID > current.ID
}

func (s *controlServer) preflightPlan(now time.Time) (*SnapshotManifest, bool) {
	paths := []string{baselineManifestPath(s.cfg.SnapshotDir)}
	history, err := listManifestPaths(s.cfg.SnapshotDir)
	if err != nil {
		log.Warn("Cannot list replication manifests in %s for preflight planning: %v", s.cfg.SnapshotDir, err)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(history)))
	paths = append(paths, history...)
	seenPaths := map[string]struct{}{}
	seenIDs := map[string]struct{}{}
	var latestReady, fallback *SnapshotManifest
	for _, path := range paths {
		if latestReady != nil {
			id := strings.TrimSuffix(filepath.Base(path), ".json")
			if validSnapshotID(id) && id <= latestReady.ID {
				break
			}
		}
		if _, ok := seenPaths[path]; ok {
			continue
		}
		seenPaths[path] = struct{}{}
		if id := strings.TrimSuffix(filepath.Base(path), ".json"); validSnapshotID(id) {
			if _, ok := seenIDs[id]; ok {
				continue
			}
		}
		manifest, err := loadManifestFile(path)
		if err != nil {
			if !os.IsNotExist(err) {
				log.Warn("Ignore invalid replication manifest during preflight planning: path=%s error=%v", path, err)
			}
			continue
		}
		if err := validateManifestIdentity(manifest, s.cfg.ControlToken); err != nil {
			log.Warn("Ignore untrusted replication manifest during preflight planning: path=%s error=%v", path, err)
			continue
		}
		if _, ok := seenIDs[manifest.ID]; ok {
			continue
		}
		switch manifest.State {
		case "ready":
			seenIDs[manifest.ID] = struct{}{}
			if newerManifest(manifest, latestReady) {
				latestReady = manifest
			}
		case "preflight", "transferring":
			seenIDs[manifest.ID] = struct{}{}
			if newerManifest(manifest, fallback) {
				fallback = manifest
			}
		}
	}
	if latestReady != nil {
		if latestReady.FormatVersion < incrementalFormatVersion {
			log.Info("Run full disaster-recovery scan to upgrade baseline manifest format: snapshot=%s baseline_format=%d current_format=%d", latestReady.ID, latestReady.FormatVersion, incrementalFormatVersion)
			return latestReady, true
		}
		if s.fullScanDue(latestReady, now) {
			log.Info("Run full disaster-recovery verification from baseline %s: last_full_scan=%s interval=%s", latestReady.ID, latestReady.FullScanAt, s.cfg.FullScanInterval)
			return latestReady, true
		}
		log.Info("Run incremental disaster-recovery preflight from ready baseline %s: last_full_scan=%s", latestReady.ID, latestReady.FullScanAt)
		return latestReady, false
	}
	if fallback != nil {
		if fallback.FormatVersion < incrementalFormatVersion {
			log.Info("Run full disaster-recovery scan to upgrade fallback manifest format: snapshot=%s state=%s baseline_format=%d current_format=%d", fallback.ID, fallback.State, fallback.FormatVersion, incrementalFormatVersion)
			return fallback, true
		}
		if s.fullScanDue(fallback, now) {
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
	return s.startPrimaryWithContext(context.Background())
}

// ensurePrimaryServiceReady starts the fenced Gitea service and waits for it to
// become ready. Clearing the recovery checkpoint is a separate step: a persistent
// cleanup failure must not cause the service to be started over and over.
func (s *controlServer) ensurePrimaryServiceReady(parent context.Context) error {
	started := time.Now()
	serviceCtx, cancelService := context.WithTimeout(parent, s.cfg.ServiceTimeout)
	defer cancelService()
	startStarted := time.Now()
	startErr := systemctl(serviceCtx, "start", s.cfg.GiteaServiceName)
	startDuration := time.Since(startStarted)
	if startErr != nil {
		return fmt.Errorf("start primary service %s after %s: %w", s.cfg.GiteaServiceName, startDuration, startErr)
	}
	readinessStarted := time.Now()
	readinessErr := readinessCheck(serviceCtx, s.cfg.GiteaServiceName)
	readinessDuration := time.Since(readinessStarted)
	if readinessErr != nil {
		return fmt.Errorf("wait for primary service %s readiness after %s: %w", s.cfg.GiteaServiceName, readinessDuration, readinessErr)
	}
	if err := serviceCtx.Err(); err != nil {
		return err
	}
	log.Info("Primary Gitea service is ready: service=%s start_duration=%s readiness_duration=%s total_duration=%s", s.cfg.GiteaServiceName, startDuration, readinessDuration, time.Since(started))
	return nil
}

func (s *controlServer) startPrimaryWithContext(parent context.Context) error {
	if err := s.ensurePrimaryServiceReady(parent); err != nil {
		return err
	}
	if err := clearPrimaryOutageCheckpoint(s.cfg.SnapshotDir); err != nil {
		return fmt.Errorf("primary service %s is ready but the outage recovery checkpoint could not be cleared: %w", s.cfg.GiteaServiceName, err)
	}
	return nil
}

func (s *controlServer) retryPrimaryStart(snapshotID, trigger string) {
	s.mu.Lock()
	if s.shuttingDown {
		s.mu.Unlock()
		log.Warn("Primary Gitea recovery retry deferred until the next control service startup: snapshot=%s trigger=%s", snapshotID, trigger)
		return
	}
	if s.primaryRecoveryPending {
		s.mu.Unlock()
		log.Info("Primary Gitea recovery retry is already active: snapshot=%s trigger=%s", snapshotID, trigger)
		return
	}
	s.primaryRecoveryPending = true
	s.backgroundWG.Add(1)
	s.mu.Unlock()
	log.Warn("Primary Gitea recovery is pending; new replication jobs are blocked: snapshot=%s trigger=%s", snapshotID, trigger)
	go func() {
		defer s.backgroundWG.Done()
		ctx := s.taskContext()
		attempt := 0
		serviceReady := false
		lastFailureLog := time.Time{}
		for {
			select {
			case <-ctx.Done():
				log.Warn("Automatic primary Gitea recovery stopped with the replication control plane; outage checkpoint remains for the next startup: snapshot=%s trigger=%s error=%v", snapshotID, trigger, ctx.Err())
				return
			default:
			}
			attempt++
			started := time.Now()
			var err error
			if !serviceReady {
				if err = s.ensurePrimaryServiceReady(ctx); err == nil {
					serviceReady = true
				}
			}
			if err == nil {
				// The service is up; from here only the checkpoint cleanup is retried,
				// so a persistent cleanup fault cannot restart Gitea in a loop.
				if clearErr := clearPrimaryOutageCheckpoint(s.cfg.SnapshotDir); clearErr == nil {
					s.mu.Lock()
					s.primaryRecoveryPending = false
					s.mu.Unlock()
					log.Info("Recovered primary Gitea: snapshot=%s trigger=%s attempt=%d duration=%s", snapshotID, trigger, attempt, time.Since(started))
					return
				} else {
					err = fmt.Errorf("primary service %s is ready but the outage recovery checkpoint could not be cleared: %w", s.cfg.GiteaServiceName, clearErr)
				}
			}
			if lastFailureLog.IsZero() || time.Since(lastFailureLog) >= time.Minute {
				log.Error("Failed to recover primary Gitea: snapshot=%s trigger=%s attempt=%d service_ready=%t duration=%s retry_in=%s error=%v", snapshotID, trigger, attempt, serviceReady, time.Since(started), primaryRecoveryRetryInterval, err)
				lastFailureLog = time.Now()
			} else {
				log.Debug("Primary Gitea recovery attempt failed: snapshot=%s trigger=%s attempt=%d service_ready=%t duration=%s retry_in=%s error=%v", snapshotID, trigger, attempt, serviceReady, time.Since(started), primaryRecoveryRetryInterval, err)
			}
			select {
			case <-ctx.Done():
				log.Warn("Automatic primary Gitea recovery stopped with the replication control plane; outage checkpoint remains for the next startup: snapshot=%s trigger=%s error=%v", snapshotID, trigger, ctx.Err())
				return
			case <-time.After(primaryRecoveryRetryInterval):
			}
		}
	}()
}

func (s *controlServer) recoverPrimary(snapshotID, trigger string) {
	started := time.Now()
	if err := s.startPrimary(); err != nil {
		log.Error("Failed to restart primary Gitea: snapshot=%s trigger=%s duration=%s error=%v", snapshotID, trigger, time.Since(started), err)
		s.retryPrimaryStart(snapshotID, trigger)
		return
	}
	log.Info("Restarted primary Gitea: snapshot=%s trigger=%s duration=%s", snapshotID, trigger, time.Since(started))
}

func (s *controlServer) preflight(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || s.cfg.Mode != modePrimary {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	if s.shuttingDown {
		s.mu.Unlock()
		http.Error(w, "replication control plane is stopping", http.StatusServiceUnavailable)
		return
	}
	requestID := r.Header.Get("Idempotency-Key")
	if requestID != "" && !validReplicationRequestID(requestID) {
		s.mu.Unlock()
		http.Error(w, "invalid preflight request ID", http.StatusBadRequest)
		return
	}
	if requestID != "" && r.URL.Query().Get("resume") == "" {
		for _, existing := range s.jobs {
			if existing.RequestID != requestID {
				continue
			}
			if existing.BaseJobID != "" {
				s.mu.Unlock()
				http.Error(w, "sync job request ID was already used for another job", http.StatusConflict)
				return
			}
			job := *existing
			s.mu.Unlock()
			switch job.State {
			case snapshotStateCreating:
				writeJSONStatus(w, http.StatusAccepted, job)
			case "preflight":
				manifest := s.getTaskManifest(job.ID)
				if manifest == nil {
					var err error
					manifest, err = loadTrustedManifest(manifestPath(s.cfg.SnapshotDir, job.ID), s.cfg.ControlToken, "preflight")
					if err != nil {
						log.Warn("Preflight request %s lost its completed checkpoint %s: %v", requestID, job.ID, err)
						writeReplicationError(w, errorCodePreflightCheckpoint, "preflight request checkpoint is unavailable or invalid")
						return
					}
				}
				log.Info("Reused preflight task for retried request: snapshot=%s request_id=%s", job.ID, requestID)
				writeManifestMaybeGzip(w, r, manifest)
			default:
				writeReplicationError(w, errorCodePreflightRequestEnded, fmt.Sprintf("preflight request has already ended in state %s: %s", job.State, job.Error))
			}
			return
		}
	}
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
			log.Warn("Reject preflight recovery checkpoint %s: %v", resumeID, err)
			writeReplicationError(w, errorCodePreflightCheckpoint, "preflight recovery checkpoint is unavailable or invalid")
			return
		}
		log.Info("Resuming preflight checkpoint %s", resumeID)
		writeManifestMaybeGzip(w, r, manifest)
		return
	}
	id, err := s.newSnapshotIDLocked()
	if err != nil {
		s.mu.Unlock()
		log.Error("Cannot allocate preflight snapshot ID: %v", err)
		http.Error(w, "could not allocate preflight snapshot ID", http.StatusInternalServerError)
		return
	}
	job := &Snapshot{ID: id, State: snapshotStateCreating, CreatedAt: time.Now().UTC(), RequestID: requestID}
	s.jobs[id] = job
	acceptedJob := *job
	s.busy = true
	s.taskWG.Add(1)
	s.mu.Unlock()
	if requestID != "" {
		if err := writePreflightRequestCheckpoint(s.cfg.SnapshotDir, id, requestID, s.cfg.ControlToken); err != nil {
			checkpointPath := preflightRequestCheckpointPath(s.cfg.SnapshotDir, id)
			cleanupErr := removeFileSynced(checkpointPath)
			s.mu.Lock()
			if cleanupErr != nil {
				job.State = "failed"
				job.Error = "could not persist preflight request checkpoint"
				job.transientFailure = true
			} else {
				delete(s.jobs, id)
			}
			s.busy = false
			s.mu.Unlock()
			s.taskWG.Done()
			if cleanupErr != nil {
				log.Error("Preflight task %s could not remove uncommitted idempotency checkpoint: %v", id, cleanupErr)
			}
			log.Error("Preflight task %s could not persist idempotency checkpoint: %v", id, err)
			http.Error(w, "could not persist preflight request checkpoint", http.StatusInternalServerError)
			return
		}
	}
	log.Info("Accepted preflight task %s: request_id=%s", id, requestID)
	go func() {
		defer s.taskWG.Done()
		s.runPreflightTask(id)
	}()
	writeJSONStatus(w, http.StatusAccepted, acceptedJob)
}

func (s *controlServer) runPreflightTask(id string) {
	taskStarted := time.Now()
	ctx, cancel := context.WithTimeout(s.taskContext(), s.cfg.SnapshotTimeout)
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
		manifest, err = scanReplicationDataTree(ctx, s.root(), base, verifyAll, id)
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
	manifest.GeneralTokenSecretFingerprint = generalTokenSecretFingerprint(s.cfg.ControlToken)
	if err := signIncrementalManifestContext(ctx, manifest, s.cfg.ControlToken); err != nil {
		log.Error("Preflight task %s signing failed after %s: %v", id, time.Since(taskStarted), err)
		s.failAsyncJob(id, err)
		return
	}
	if err := writeManifestAtContext(ctx, manifestPath(s.cfg.SnapshotDir, manifest.ID), manifest); err != nil {
		log.Error("Preflight task %s persist failed after %s: %v", id, time.Since(taskStarted), err)
		s.failAsyncJob(id, err)
		return
	}
	if err := s.setTaskManifest(ctx, manifest, ""); err != nil {
		log.Error("Preflight task %s chunk index failed after %s: %v", id, time.Since(taskStarted), err)
		s.failAsyncJob(id, err)
		return
	}
	log.Info("Preflight task %s completed: mode=%s base=%s entries=%d bytes=%d scan_duration=%s total_duration=%s", id, scanMode, baseID, manifest.FileCount, manifest.Size, scanDuration, time.Since(taskStarted))
	s.prune()
	s.completeAsyncJob(id, manifest.Snapshot)
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
		log.Warn("Reject finalize request with invalid idempotency key: remote=%s", r.RemoteAddr)
		http.Error(w, "invalid sync job request ID", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	if s.shuttingDown {
		s.mu.Unlock()
		http.Error(w, "replication control plane is stopping", http.StatusServiceUnavailable)
		return
	}
	if requestID != "" {
		for _, existing := range s.jobs {
			if existing.RequestID != requestID {
				continue
			}
			if existing.BaseJobID != baseID {
				existingBaseID := existing.BaseJobID
				s.mu.Unlock()
				log.Warn("Reject finalize request ID reused for another preflight base: request_id=%s existing_base=%s requested_base=%s", requestID, existingBaseID, baseID)
				http.Error(w, "sync job request ID was already used for another preflight base", http.StatusConflict)
				return
			}
			job := *existing
			s.mu.Unlock()
			log.Info("Finalize request matched existing task: task=%s base=%s state=%s request_id=%s", job.ID, baseID, job.State, requestID)
			if job.State == snapshotStateCreating || job.State == "transferring" {
				writeJSONStatus(w, http.StatusAccepted, job)
				return
			}
			writeReplicationError(w, errorCodeFinalizeRequestEnded, fmt.Sprintf("sync job request has already ended in state %s: %s", job.State, job.Error))
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
		writeReplicationError(w, errorCodePreflightBase, "preflight base is unavailable or invalid: "+err.Error())
		return
	}
	id, err := s.newSnapshotIDLocked()
	if err != nil {
		s.mu.Unlock()
		log.Error("Cannot allocate final sync snapshot ID: base=%s error=%v", baseID, err)
		http.Error(w, "could not allocate final sync snapshot ID", http.StatusInternalServerError)
		return
	}
	job := &Snapshot{ID: id, State: snapshotStateCreating, CreatedAt: time.Now().UTC(), RequestID: requestID, BaseJobID: baseID}
	s.jobs[id] = job
	acceptedJob := *job
	s.busy = true
	s.taskWG.Add(1)
	s.mu.Unlock()
	if requestID != "" {
		if err := writeFinalizeRequestCheckpoint(s.cfg.SnapshotDir, id, baseID, requestID, s.cfg.ControlToken); err != nil {
			checkpointPath := finalizeRequestCheckpointPath(s.cfg.SnapshotDir, id)
			cleanupErr := removeFileSynced(checkpointPath)
			s.mu.Lock()
			if cleanupErr != nil {
				job.State = "failed"
				job.Error = "could not persist sync job request checkpoint"
				job.transientFailure = true
			} else {
				delete(s.jobs, id)
			}
			s.busy = false
			s.mu.Unlock()
			s.taskWG.Done()
			if cleanupErr != nil {
				log.Error("Finalize task %s could not remove uncommitted idempotency checkpoint: %v", id, cleanupErr)
			}
			log.Error("Finalize task %s could not persist idempotency checkpoint: %v", id, err)
			http.Error(w, "could not persist sync job request checkpoint", http.StatusInternalServerError)
			return
		}
	}
	log.Info("Accepted finalize task %s for preflight base %s: request_id=%s", id, baseID, requestID)
	go func() {
		defer s.taskWG.Done()
		s.runFinalizeTask(id, baseID, requestID)
	}()
	writeJSONStatus(w, http.StatusAccepted, acceptedJob)
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
	log.Info("Finalize task %s starting from preflight %s: request_id=%s base_entries=%d", id, baseID, requestID, base.FileCount)
	if err := validateNoNestedMounts(s.root()); err != nil {
		err = fmt.Errorf("validate replication data root mounts: %w", err)
		log.Error("Finalize task %s rejected before stopping primary: %v", id, err)
		s.failAsyncJob(id, err)
		return
	}

	fenceCtx, fenceCancel := context.WithTimeout(s.taskContext(), s.cfg.SnapshotTimeout)
	fenceStarted := time.Now()
	fence, err := acquireSnapshotFence(fenceCtx)
	if err == nil {
		err = ensureSocketActivationDisabled(fenceCtx, s.cfg.GiteaServiceName)
	}
	if err != nil {
		err = releaseFinalizeFence(id, fence, err)
		fenceCancel()
		log.Error("Finalize task %s failed before stopping primary after %s: %v", id, time.Since(fenceStarted), err)
		s.failAsyncJob(id, err)
		return
	}
	log.Info("Finalize task %s acquired snapshot fence after %s", id, time.Since(fenceStarted))
	fenceCancel()
	outageStarted := time.Now()
	outageDeadline := outageStarted.Add(s.primaryOutageTimeout())
	outageCtx, outageCancel := context.WithDeadline(s.taskContext(), outageDeadline)
	defer outageCancel()
	checkpointStarted := time.Now()
	if err := writeFileSynced(primaryOutageCheckpointPath(s.cfg.SnapshotDir), []byte(id)); err != nil {
		err = releaseFinalizeFence(id, fence, err)
		log.Error("Finalize task %s could not persist primary outage recovery checkpoint after %s: %v", id, time.Since(checkpointStarted), err)
		s.failAsyncJob(id, err)
		return
	}
	log.Info("Persisted primary outage recovery checkpoint: snapshot=%s duration=%s", id, time.Since(checkpointStarted))
	stopStarted := time.Now()
	stopAttempted := true
	stopCtx, stopCancel := context.WithTimeout(outageCtx, s.cfg.ServiceTimeout)
	err = systemctl(stopCtx, "stop", s.cfg.GiteaServiceName)
	stopCancel()
	if err != nil {
		if stopAttempted {
			s.recoverPrimary(id, "stop_primary")
		}
		err = releaseFinalizeFence(id, fence, err)
		log.Error("Finalize task %s failed while stopping primary after %s: %v", id, time.Since(stopStarted), err)
		s.failAsyncJob(id, err)
		return
	}

	log.Info("Finalize task %s stopped primary after %s", id, time.Since(stopStarted))
	finalScanStarted := time.Now()
	scanCtx, scanCancel := context.WithCancel(outageCtx)
	defer scanCancel()
	var manifest *SnapshotManifest
finalScanAttempts:
	for attempt := 1; attempt <= 5; attempt++ {
		manifest, err = scanReplicationDataTree(scanCtx, s.root(), base, false, id)
		if err == nil || !errors.Is(err, errIncrementalTreeChanged) {
			break
		}
		if attempt == 5 {
			log.Warn("Finalize task %s final scan still found changing files on final attempt=%d/5: %v", id, attempt, err)
			break
		}
		log.Warn("Finalize task %s final scan found changing files; retrying while primary remains stopped attempt=%d/5 error=%v", id, attempt+1, err)
		select {
		case <-scanCtx.Done():
			err = scanCtx.Err()
			break finalScanAttempts
		case <-time.After(100 * time.Millisecond):
		}
	}
	if err != nil {
		scanCancel()
		s.recoverPrimary(id, "final_scan")
		err = releaseFinalizeFence(id, fence, err)
		log.Error("Finalize task %s final scan failed: scan_duration=%s primary_outage=%s error=%v", id, time.Since(finalScanStarted), time.Since(outageStarted), err)
		s.failAsyncJob(id, err)
		return
	}
	log.Info("Finalize task %s final scan completed: entries=%d bytes=%d scan_duration=%s primary_outage=%s", id, manifest.FileCount, manifest.Size, time.Since(finalScanStarted), time.Since(outageStarted))
	manifest.ID = id
	manifest.State = "transferring"
	manifest.CreatedAt = time.Now().UTC()
	manifest.InstanceFingerprint = instanceFingerprint(s.cfg.ControlToken)
	manifest.GeneralTokenSecretFingerprint = generalTokenSecretFingerprint(s.cfg.ControlToken)
	manifest.RequestID, manifest.BaseJobID = requestID, baseID
	if err := signIncrementalManifestContext(scanCtx, manifest, s.cfg.ControlToken); err != nil {
		scanCancel()
		s.recoverPrimary(id, "sign_manifest")
		err = releaseFinalizeFence(id, fence, err)
		log.Error("Finalize task %s signing failed: primary_outage=%s total_duration=%s error=%v", id, time.Since(outageStarted), time.Since(taskStarted), err)
		s.failAsyncJob(id, err)
		return
	}
	if err := writeManifestAtContext(scanCtx, manifestPath(s.cfg.SnapshotDir, manifest.ID), manifest); err != nil {
		scanCancel()
		s.recoverPrimary(id, "persist_manifest")
		err = releaseFinalizeFence(id, fence, err)
		log.Error("Finalize task %s persist failed: primary_outage=%s total_duration=%s error=%v", id, time.Since(outageStarted), time.Since(taskStarted), err)
		s.failAsyncJob(id, err)
		return
	}
	if err := s.setTaskManifest(scanCtx, manifest, baseID); err != nil {
		scanCancel()
		s.recoverPrimary(id, "index_chunks")
		err = releaseFinalizeFence(id, fence, err)
		log.Error("Finalize task %s chunk index failed: primary_outage=%s total_duration=%s error=%v", id, time.Since(outageStarted), time.Since(taskStarted), err)
		s.failAsyncJob(id, err)
		return
	}
	if err := outageCtx.Err(); err != nil {
		scanCancel()
		s.recoverPrimary(id, "outage_budget")
		err = releaseFinalizeFence(id, fence, err)
		log.Error("Finalize task %s exceeded primary outage budget: primary_outage=%s total_duration=%s error=%v", id, time.Since(outageStarted), time.Since(taskStarted), err)
		s.failAsyncJob(id, err)
		return
	}
	scanCancel()
	timeout := s.finalSessionTimeout()
	now := time.Now()
	deadline := sessionExpiry(now, timeout, outageDeadline)
	session := &finalSyncSession{
		id: manifest.ID, fence: fence, expiresAt: deadline,
		preReleaseDeadline: outageDeadline,
		finished:           make(chan struct{}), renew: make(chan struct{}, 1),
	}
	s.mu.Lock()
	s.session = session
	job := s.jobs[manifest.ID]
	if job == nil {
		job = &Snapshot{ID: manifest.ID, CreatedAt: manifest.CreatedAt}
		s.jobs[manifest.ID] = job
	}
	jobRequestID, jobBaseID := job.RequestID, job.BaseJobID
	*job = manifest.Snapshot
	job.RequestID, job.BaseJobID = jobRequestID, jobBaseID
	s.busy = false
	s.mu.Unlock()
	log.Info("Finalize task %s prepared transfer session %s: request_id=%s primary_outage=%s remaining_budget=%s total_duration=%s", id, manifest.ID, requestID, time.Since(outageStarted), time.Until(deadline), time.Since(taskStarted))
	s.backgroundWG.Go(func() { s.expireSession(session) })
}

func releaseFinalizeFence(id string, fence *WriteFence, cause error) error {
	if fence == nil {
		return cause
	}
	if err := fence.Release(); err != nil {
		releaseErr := fmt.Errorf("release primary write fence: %w", err)
		log.Error("Finalize task %s failed to release the primary write fence: %v", id, err)
		return errors.Join(cause, releaseErr)
	}
	return cause
}

func (s *controlServer) failAsyncJob(id string, err error) {
	s.mu.Lock()
	job := s.jobs[id]
	if job == nil {
		job = &Snapshot{ID: id, CreatedAt: time.Now().UTC()}
		s.jobs[id] = job
	}
	job.State = "failed"
	job.Error = err.Error()
	job.transientFailure = true
	// Keep request-keyed failures until their durable checkpoint expires.
	hasRequestCheckpoint := job.RequestID != ""
	pruneBeforeRestart := hasRequestCheckpoint && s.cfg != nil && s.cfg.SnapshotDir != ""
	// Keep new jobs out while pruning can remove their temporary manifests.
	if !pruneBeforeRestart {
		s.busy = false
	}
	pruned := s.pruneTransientFailedJobsLocked(id)
	s.mu.Unlock()
	if pruned > 0 {
		log.Info("Pruned old failed replication job history: removed=%d retained_limit=%d", pruned, maxTransientFailedJobs)
	}
	if pruneBeforeRestart {
		s.prune()
		s.mu.Lock()
		s.busy = false
		s.mu.Unlock()
	}
}

// newSnapshotIDLocked must be called with s.mu held; it preserves order across clock rollbacks and avoids reuse.
func (s *controlServer) newSnapshotIDLocked() (string, error) {
	candidate := time.Now().UTC()
	highWaterPath := filepath.Join(s.cfg.SnapshotDir, snapshotIDHighWaterName)
	highWaterID := ""
	info, err := os.Lstat(highWaterPath)
	if err == nil {
		if !info.Mode().IsRegular() || info.Size() != int64(len(snapshotIDLayout)) {
			return "", errors.New("invalid replication snapshot ID high-water file")
		}
		file, openedInfo, err := openRegularFile(highWaterPath, info)
		if err != nil {
			return "", fmt.Errorf("open replication snapshot ID high-water file: %w", err)
		}
		data, readErr := io.ReadAll(io.LimitReader(file, int64(len(snapshotIDLayout))+1))
		closeErr := file.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return "", fmt.Errorf("read replication snapshot ID high-water file: %w", err)
		}
		if openedInfo.Size() != int64(len(snapshotIDLayout)) || len(data) != len(snapshotIDLayout) || !validSnapshotID(string(data)) {
			return "", errors.New("invalid replication snapshot ID high-water file")
		}
		highWaterID = string(data)
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("inspect replication snapshot ID high-water file: %w", err)
	}
	for id := range s.jobs {
		existing, err := time.Parse(snapshotIDLayout, id)
		if err == nil && !candidate.After(existing) {
			candidate = existing.Add(time.Nanosecond)
		}
	}
	if existing, err := time.Parse(snapshotIDLayout, highWaterID); err == nil && !candidate.After(existing) {
		candidate = existing.Add(time.Nanosecond)
	}
	for {
		id := candidate.Format(snapshotIDLayout)
		if _, exists := s.jobs[id]; !exists {
			occupied := false
			for _, path := range []string{
				manifestPath(s.cfg.SnapshotDir, id),
				finalizeRequestCheckpointPath(s.cfg.SnapshotDir, id),
				preflightRequestCheckpointPath(s.cfg.SnapshotDir, id),
			} {
				if _, err := os.Lstat(path); err == nil {
					occupied = true
					break
				} else if !os.IsNotExist(err) {
					return "", fmt.Errorf("inspect replication snapshot ID path %q: %w", path, err)
				}
			}
			if !occupied {
				if err := writeFileSynced(highWaterPath, []byte(id)); err != nil {
					return "", fmt.Errorf("persist replication snapshot ID high-water mark: %w", err)
				}
				return id, nil
			}
		}
		candidate = candidate.Add(time.Nanosecond)
	}
}

func (s *controlServer) pruneTransientFailedJobsLocked(preserveID string) int {
	failedIDs := make([]string, 0)
	for id, job := range s.jobs {
		if job.State == "failed" && job.transientFailure && job.RequestID == "" {
			failedIDs = append(failedIDs, id)
		}
	}
	if len(failedIDs) <= maxTransientFailedJobs {
		return 0
	}
	slices.Sort(failedIDs)
	pruned := 0
	for _, id := range failedIDs {
		if len(failedIDs)-pruned <= maxTransientFailedJobs {
			break
		}
		if id == preserveID {
			continue
		}
		delete(s.jobs, id)
		delete(s.taskManifests, id)
		delete(s.taskChunkIndexes, id)
		delete(s.taskChunkIndexFallbacks, id)
		delete(s.taskChunkAlternates, id)
		pruned++
	}
	return pruned
}

func (s *controlServer) completeAsyncJob(id string, snapshot Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[id]
	if job == nil {
		job = &Snapshot{ID: id, CreatedAt: snapshot.CreatedAt}
		s.jobs[id] = job
	}
	requestID, baseJobID := job.RequestID, job.BaseJobID
	*job = snapshot
	job.RequestID, job.BaseJobID = requestID, baseJobID
	job.Error = ""
	s.busy = false
}

func (s *controlServer) expireSession(session *finalSyncSession) {
	s.chunkMu.RLock()
	deadline := session.expiresAt
	s.chunkMu.RUnlock()
	timeout := time.Until(deadline)
	if deadline.IsZero() {
		timeout = s.finalSessionTimeout()
	}
	if timeout < 0 {
		timeout = 0
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			s.chunkMu.Lock()
			s.mu.RLock()
			active := s.session == session
			s.mu.RUnlock()
			if !active {
				s.chunkMu.Unlock()
				return
			}
			remaining := time.Until(session.expiresAt)
			if remaining > 0 {
				s.chunkMu.Unlock()
				timer.Reset(remaining)
				continue
			}
			session.expired = true
			primaryAlreadyResumed := session.primaryResumed
			phase := "chunk transfer and standby update"
			if primaryAlreadyResumed {
				phase = "post-release standby verification"
			}
			expiryReason, timeout := "idle timeout", s.finalSessionTimeout()
			if !primaryAlreadyResumed && !session.preReleaseDeadline.IsZero() && !session.expiresAt.Before(session.preReleaseDeadline) {
				expiryReason, timeout = "maximum primary outage", s.primaryOutageTimeout()
			} else if !session.postReleaseDeadline.IsZero() && !session.expiresAt.Before(session.postReleaseDeadline) {
				expiryReason, timeout = "post-release maximum age", s.postReleaseSessionMaxAge()
			}
			s.chunkMu.Unlock()
			log.Warn("Final sync session %s expired during %s: reason=%s timeout=%s primary_already_resumed=%t", session.id, phase, expiryReason, timeout, primaryAlreadyResumed)
			_ = s.finishSession(session.id, false)
			return
		case <-session.renew:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			s.chunkMu.RLock()
			timeout := time.Until(session.expiresAt)
			s.chunkMu.RUnlock()
			if timeout < 0 {
				timeout = 0
			}
			timer.Reset(timeout)
		case <-session.finished:
			return
		}
	}
}

func (s *controlServer) resumeSessionPrimary(session *finalSyncSession) (time.Duration, time.Duration, error) {
	if session.primaryResumed {
		return 0, 0, nil
	}
	primaryStartStarted := time.Now()
	startErr := s.startPrimary()
	primaryStartDuration := time.Since(primaryStartStarted)
	if startErr != nil {
		log.Error("Final sync session %s could not restart primary Gitea: trigger=session_resume duration=%s error=%v", session.id, primaryStartDuration, startErr)
		s.retryPrimaryStart(session.id, "session_resume")
	}
	fenceReleaseStarted := time.Now()
	releaseErr := session.fence.Release()
	session.fence = nil
	session.primaryResumed = true
	fenceReleaseDuration := time.Since(fenceReleaseStarted)
	return primaryStartDuration, fenceReleaseDuration, errors.Join(startErr, releaseErr)
}

func (s *controlServer) releaseSessionPrimary(id string) error {
	s.chunkMu.Lock()
	defer s.chunkMu.Unlock()
	s.mu.RLock()
	session := s.session
	s.mu.RUnlock()
	if session == nil || session.id != id {
		return errFinalSyncSessionInactive
	}
	if session.expired {
		return errFinalSyncSessionExpired
	}
	if !time.Now().Before(session.expiresAt) {
		session.expired = true
		return errFinalSyncSessionExpired
	}
	wasResumed := session.primaryResumed
	primaryStartDuration, fenceReleaseDuration, err := s.resumeSessionPrimary(session)
	if !wasResumed {
		now := time.Now()
		session.postReleaseDeadline = now.Add(s.postReleaseSessionMaxAge())
		session.expiresAt = now.Add(s.finalSessionTimeout())
		if session.expiresAt.After(session.postReleaseDeadline) {
			session.expiresAt = session.postReleaseDeadline
		}
		select {
		case session.renew <- struct{}{}:
		default:
		}
	}
	if wasResumed {
		s.mu.RLock()
		recoveryPending := s.primaryRecoveryPending
		s.mu.RUnlock()
		if recoveryPending {
			return errPrimaryRecoveryPending
		}
	}
	if err != nil {
		log.Error("Final sync session %s released primary with errors: primary_start=%s fence_release=%s error=%v", id, primaryStartDuration, fenceReleaseDuration, err)
		return err
	}
	log.Info("Final sync session %s released primary: already_resumed=%t primary_start=%s fence_release=%s activation_budget=%s", id, wasResumed, primaryStartDuration, fenceReleaseDuration, s.finalSessionTimeout())
	return nil
}

func (s *controlServer) heartbeatSession(id, renewalID string) (time.Duration, error) {
	s.chunkMu.Lock()
	defer s.chunkMu.Unlock()
	s.mu.RLock()
	session := s.session
	s.mu.RUnlock()
	if session == nil || session.id != id {
		return 0, errFinalSyncSessionInactive
	}
	if session.expired || !time.Now().Before(session.expiresAt) {
		session.expired = true
		return 0, errFinalSyncSessionExpired
	}
	if !validReplicationRequestID(renewalID) {
		return 0, errFinalSyncSessionInvalidRenewalID
	}
	if session.lastProgressRenewalID == renewalID {
		return time.Until(session.expiresAt), nil
	}
	session.lastProgressRenewalID = renewalID
	now := time.Now()
	deadline := session.preReleaseDeadline
	if session.primaryResumed {
		deadline = session.postReleaseDeadline
	}
	if deadline.IsZero() {
		maxAge := maxPreReleaseSessionAge(s.finalSessionTimeout())
		if session.primaryResumed {
			maxAge = s.postReleaseSessionMaxAge()
		}
		deadline = now.Add(maxAge)
		if session.primaryResumed {
			session.postReleaseDeadline = deadline
		} else {
			session.preReleaseDeadline = deadline
		}
	}
	session.expiresAt = sessionExpiry(now, s.finalSessionTimeout(), deadline)
	select {
	case session.renew <- struct{}{}:
	default:
	}
	log.Debug("Extended final sync session for standby progress: snapshot=%s primary_released=%t timeout=%s", id, session.primaryResumed, s.finalSessionTimeout())
	return time.Until(session.expiresAt), nil
}

func (s *controlServer) finishSession(id string, success bool) error {
	s.chunkMu.Lock()
	chunkLockHeld := true
	defer func() {
		if chunkLockHeld {
			s.chunkMu.Unlock()
		}
	}()
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
	if success && (session.expired || (!session.expiresAt.IsZero() && !time.Now().Before(session.expiresAt))) {
		session.expired = true
		s.mu.Unlock()
		log.Warn("Final sync session %s completion rejected after its deadline", id)
		return errFinalSyncSessionExpired
	}
	if success && session.primaryResumed && s.primaryRecoveryPending {
		s.mu.Unlock()
		log.Warn("Final sync session %s completion deferred until primary Gitea recovery succeeds", id)
		return errPrimaryRecoveryPending
	}
	primaryAlreadyResumed := session.primaryResumed
	s.session = nil
	s.busy = true
	job := s.jobs[id]
	s.mu.Unlock()
	log.Info("Final sync session %s finalizing: action=%s", id, requestedAction)
	primaryStartDuration, fenceReleaseDuration, finishErr := s.resumeSessionPrimary(session)
	s.chunkMu.Unlock()
	chunkLockHeld = false
	if job != nil {
		s.mu.RLock()
		storedJob := s.jobs[id]
		var jobCopy Snapshot
		if storedJob != nil {
			jobCopy = *storedJob
		}
		s.mu.RUnlock()
		if storedJob != nil {
			if success && finishErr == nil {
				jobCopy.State, jobCopy.Error = "ready", ""
			} else {
				jobCopy.State = "failed"
				if finishErr != nil {
					jobCopy.Error = finishErr.Error()
				} else {
					jobCopy.Error = "sync session aborted"
				}
			}
			manifestPersistStarted := time.Now()
			manifest := s.getTaskManifest(id)
			var loadErr error
			if manifest == nil {
				manifest, loadErr = loadTrustedManifest(manifestPath(s.cfg.SnapshotDir, id), s.cfg.ControlToken, "transferring")
				if loadErr != nil {
					log.Warn("Cannot load trusted replication manifest %s during finalization: %v", id, loadErr)
				}
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
				log.Error("Finalize replication manifest failed: snapshot=%s duration=%s error=%v", id, time.Since(manifestPersistStarted), manifestErr)
				jobCopy.State, jobCopy.Error = "failed", finishErr.Error()
				if manifest != nil {
					manifest.Snapshot = jobCopy
					if err := signIncrementalManifest(manifest, s.cfg.ControlToken); err != nil {
						log.Error("Sign failed disaster-recovery manifest: snapshot=%s error=%v", id, err)
						manifest = nil
					} else if err := writeManifest(s.cfg.SnapshotDir, manifest); err != nil {
						log.Error("Persist failed disaster-recovery manifest: snapshot=%s error=%v", id, err)
					}
				}
			} else {
				log.Info("Persisted finalized replication manifest: snapshot=%s state=%s entries=%d bytes=%d duration=%s", id, manifest.State, manifest.FileCount, manifest.Size, time.Since(manifestPersistStarted))
			}
			if manifestErr == nil && success && finishErr == nil {
				baselineStarted := time.Now()
				if err := writeManifestAt(baselineManifestPath(s.cfg.SnapshotDir), manifest); err != nil {
					log.Error("Persist disaster-recovery scan baseline failed: snapshot=%s duration=%s error=%v", id, time.Since(baselineStarted), err)
				} else {
					log.Info("Persisted disaster-recovery scan baseline: snapshot=%s entries=%d bytes=%d duration=%s", id, manifest.FileCount, manifest.Size, time.Since(baselineStarted))
				}
			}
			s.publishFinalTaskState(id, jobCopy, manifest)
		}
	}
	close(session.finished)
	s.prune()
	s.mu.Lock()
	s.busy = false
	s.mu.Unlock()
	if finishErr != nil {
		log.Error("Final sync session %s finalized with errors: action=%s duration=%s primary_already_resumed=%t primary_start=%s fence_release=%s error=%v", id, requestedAction, time.Since(finishStarted), primaryAlreadyResumed, primaryStartDuration, fenceReleaseDuration, finishErr)
	} else {
		log.Info("Final sync session %s finalized: action=%s duration=%s primary_already_resumed=%t primary_start=%s fence_release=%s", id, requestedAction, time.Since(finishStarted), primaryAlreadyResumed, primaryStartDuration, fenceReleaseDuration)
	}
	return finishErr
}

func (s *controlServer) publishFinalTaskState(id string, job Snapshot, manifest *SnapshotManifest) {
	s.mu.Lock()
	if stored := s.jobs[id]; stored != nil {
		*stored = job
	}
	if manifest == nil || manifest.State == "ready" {
		delete(s.taskManifests, id)
	} else {
		if s.taskManifests == nil {
			s.taskManifests = map[string]*SnapshotManifest{}
		}
		manifestCopy := *manifest
		s.taskManifests[id] = &manifestCopy
	}
	delete(s.taskChunkIndexes, id)
	delete(s.taskChunkIndexFallbacks, id)
	delete(s.taskChunkAlternates, id)
	s.mu.Unlock()
}

func retryableChunkReadError(err error) bool {
	if shouldRetryRequestError(err) {
		return true
	}
	var pathErr *os.PathError
	if !errors.As(err, &pathErr) {
		return false
	}
	return errors.Is(pathErr.Err, syscall.EAGAIN) || errors.Is(pathErr.Err, syscall.EBUSY) || errors.Is(pathErr.Err, syscall.EIO)
}

// readChunkWithAlternates reads one servable chunk, trying the recorded alternate
// locations in order. readChunkFromRoot verifies the chunk hash on every attempt.
func (s *controlServer) readChunkWithAlternates(snapshotID string, sources chunkServePlan, hash string) (data []byte, sourcePath string, alternateAttempts int, retryable bool, err error) {
	data, err = readChunkFromRoot(s.root(), s.resolvedRoot, sources.location, hash)
	sourcePath = sources.location.Path
	retryable = retryableChunkReadError(err)
	if err == nil {
		return data, sourcePath, 0, false, nil
	}
	for _, alternate := range sources.alternates {
		alternateAttempts++
		alternateData, alternateErr := readChunkFromRoot(s.root(), s.resolvedRoot, alternate, hash)
		if alternateErr == nil {
			log.Debug("Served replication chunk from alternate manifest location: snapshot=%s hash=%s path=%s", snapshotID, hash, alternate.Path)
			return alternateData, alternate.Path, alternateAttempts, false, nil
		}
		retryable = retryable || retryableChunkReadError(alternateErr)
		err = alternateErr
		log.Debug("Alternate replication chunk location failed: snapshot=%s hash=%s path=%s error=%v", snapshotID, hash, alternate.Path, alternateErr)
	}
	return nil, sourcePath, alternateAttempts, retryable, err
}

const (
	maxBatchChunkCount          = 32
	maxBatchChunkRequestSize    = 256 * 1024
	maxBatchChunkResponseBytes  = 16 * 1024 * 1024
	chunkBatchStatusOK          = 0
	chunkBatchStatusUnavailable = 1
)

type chunkBatchRequest struct {
	Hashes []string `json:"hashes"`
}

// syncChunkBatch serves several chunks in one bounded response so small chunks do
// not cost one request each. A record is [status][index], followed by a 4-byte
// big-endian length and the payload when the status is OK. Unavailable entries are
// left for the client to fetch individually.
func (s *controlServer) syncChunkBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, syncJobsPath+"/"), "/")
	if len(parts) != 2 || !validSnapshotID(parts[0]) {
		http.NotFound(w, r)
		return
	}
	id := parts[0]
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBatchChunkRequestSize))
	if err != nil {
		log.Warn("Reject replication chunk batch with unreadable or oversized body: snapshot=%s remote=%s error=%v", id, r.RemoteAddr, err)
		http.Error(w, "invalid chunk batch request", http.StatusBadRequest)
		return
	}
	var request chunkBatchRequest
	if err := json.Unmarshal(body, &request); err != nil {
		log.Warn("Reject malformed replication chunk batch: snapshot=%s remote=%s error=%v", id, r.RemoteAddr, err)
		http.Error(w, "invalid chunk batch request", http.StatusBadRequest)
		return
	}
	if len(request.Hashes) == 0 || len(request.Hashes) > maxBatchChunkCount {
		http.Error(w, "invalid chunk batch size", http.StatusBadRequest)
		return
	}
	for _, hash := range request.Hashes {
		if len(hash) != sha256.Size*2 || !isLowerHex(hash) {
			http.Error(w, "invalid chunk hash", http.StatusBadRequest)
			return
		}
	}
	slots, acquired := s.tryAcquireChunkSlot()
	if !acquired {
		w.Header().Set("Retry-After", "1")
		log.Warn("Rejected replication chunk batch because concurrent serve limit was reached: snapshot=%s limit=%d", id, maxConcurrentChunkServes)
		http.Error(w, "replication chunk service is busy", http.StatusServiceUnavailable)
		return
	}
	defer func() { <-slots }()

	responseBytes, unavailable, served := 0, 0, 0
	records := make([][]byte, 0, len(request.Hashes))
	for index, hash := range request.Hashes {
		sources := s.getServableChunkSources(id, hash)
		if !sources.allowed || !sources.indexed || !sources.found {
			records = append(records, []byte{chunkBatchStatusUnavailable, byte(index)})
			unavailable++
			continue
		}
		if responseBytes > 0 && int64(responseBytes)+sources.location.Size > maxBatchChunkResponseBytes {
			records = append(records, []byte{chunkBatchStatusUnavailable, byte(index)})
			unavailable++
			continue
		}
		data, _, _, _, readErr := s.readChunkWithAlternates(id, sources, hash)
		if readErr != nil {
			log.Debug("Chunk batch entry unavailable: snapshot=%s hash=%s error=%v", id, hash, readErr)
			records = append(records, []byte{chunkBatchStatusUnavailable, byte(index)})
			unavailable++
			continue
		}
		record := make([]byte, 2+4+len(data))
		record[0] = chunkBatchStatusOK
		record[1] = byte(index)
		binary.BigEndian.PutUint32(record[2:6], uint32(len(data)))
		copy(record[6:], data)
		records = append(records, record)
		responseBytes += len(data)
		served++
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	for _, record := range records {
		if _, err := w.Write(record); err != nil {
			log.Debug("Chunk batch response write failed: snapshot=%s error=%v", id, err)
			return
		}
	}
	log.Debug("Served replication chunk batch: snapshot=%s requested=%d served=%d unavailable=%d payload_bytes=%d", id, len(request.Hashes), served, unavailable, responseBytes)
}

func (s *controlServer) syncSnapshot(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, syncJobsPath+"/"), "/")
	if len(parts) != 3 || !validSnapshotID(parts[0]) {
		http.NotFound(w, r)
		return
	}
	id, action, value := parts[0], parts[1], parts[2]
	if action == "chunks" && r.Method == http.MethodGet {
		requestStarted := time.Now()
		if len(value) != 64 {
			http.NotFound(w, r)
			return
		}
		if rejectUnacceptableReplicationEncoding(w, r) {
			return
		}
		s.chunkMu.RLock()
		chunkLockHeld := true
		defer func() {
			if chunkLockHeld {
				s.chunkMu.RUnlock()
			}
		}()
		sources := s.getServableChunkSources(id, value)
		if !sources.allowed {
			if sources.jobState != "" {
				log.Debug("Rejected replication chunk request because snapshot state does not allow serving: snapshot=%s hash=%s state=%s primary_resumed=%t", id, value, sources.jobState, sources.primaryResumed)
			} else if sources.primaryResumed {
				log.Debug("Rejected replication chunk request after primary release: snapshot=%s hash=%s", id, value)
			}
			http.NotFound(w, r)
			return
		}
		if !sources.indexed || !sources.found {
			log.Debug("Rejected replication chunk request without an indexed source: snapshot=%s hash=%s index_available=%t source_found=%t", id, value, sources.indexed, sources.found)
			http.NotFound(w, r)
			return
		}
		slots, acquired := s.tryAcquireChunkSlot()
		if !acquired {
			w.Header().Set("Retry-After", "1")
			log.Warn("Rejected replication chunk request because concurrent serve limit was reached: snapshot=%s hash=%s limit=%d", id, value, maxConcurrentChunkServes)
			http.Error(w, "replication chunk service is busy", http.StatusServiceUnavailable)
			return
		}
		defer func() { <-slots }()
		readStarted := time.Now()
		data, sourcePath, alternateAttempts, retryableReadFailure, err := s.readChunkWithAlternates(id, sources, value)
		readDuration := time.Since(readStarted)
		s.chunkMu.RUnlock()
		chunkLockHeld = false
		if err != nil {
			status := http.StatusConflict
			if retryableReadFailure {
				status = http.StatusServiceUnavailable
				w.Header().Set("Retry-After", "1")
			}
			log.Warn("Failed to serve replication chunk: snapshot=%s hash=%s primary_path=%s alternate_attempts=%d status=%d retryable=%t read_duration=%s duration=%s error=%v", id, value, sources.location.Path, alternateAttempts, status, retryableReadFailure, readDuration, time.Since(requestStarted), err)
			http.Error(w, err.Error(), status)
			return
		}
		responseStarted := time.Now()
		bodyBytes, compressed, err := writeChunk(w, r, data)
		responseDuration := time.Since(responseStarted)
		if err != nil {
			log.Warn("Failed to write replication chunk response: snapshot=%s hash=%s source_path=%s alternate_attempts=%d payload_bytes=%d response_body_bytes=%d gzip=%t read_duration=%s response_duration=%s duration=%s error=%v", id, value, sourcePath, alternateAttempts, len(data), bodyBytes, compressed, readDuration, responseDuration, time.Since(requestStarted), err)
			return
		}
		log.Debug("Served replication chunk: snapshot=%s hash=%s source_path=%s alternate_attempts=%d payload_bytes=%d response_body_bytes=%d gzip=%t read_duration=%s response_duration=%s duration=%s", id, value, sourcePath, alternateAttempts, len(data), bodyBytes, compressed, readDuration, responseDuration, time.Since(requestStarted))
		return
	}
	if action == "session" && value == "complete" && r.Method == http.MethodPost {
		if err := s.finishSession(id, true); err != nil {
			s.mu.RLock()
			job := s.jobs[id]
			alreadyReady := job != nil && job.State == "ready"
			s.mu.RUnlock()
			if alreadyReady && s.primaryRecoveryBlocksReadySnapshot(id) {
				w.Header().Set("Retry-After", "1")
				http.Error(w, errPrimaryRecoveryPending.Error(), http.StatusServiceUnavailable)
				return
			}
			if !alreadyReady {
				status := http.StatusConflict
				if errors.Is(err, errPrimaryRecoveryPending) {
					status = http.StatusServiceUnavailable
					w.Header().Set("Retry-After", "1")
				}
				http.Error(w, err.Error(), status)
				return
			}
		}
		writeJSON(w, map[string]string{"status": "ready"})
		return
	}
	if action == "session" && value == "heartbeat" && r.Method == http.MethodPost {
		lease, err := s.heartbeatSession(id, r.URL.Query().Get("renewal_id"))
		if err != nil {
			status := http.StatusConflict
			if errors.Is(err, errFinalSyncSessionInvalidRenewalID) {
				status = http.StatusBadRequest
			}
			http.Error(w, err.Error(), status)
			return
		}
		writeJSON(w, struct {
			Status  string `json:"status"`
			LeaseNS int64  `json:"lease_ns"`
		}{Status: "active", LeaseNS: lease.Nanoseconds()})
		return
	}
	if action == "session" && value == "release" && r.Method == http.MethodPost {
		if err := s.releaseSessionPrimary(id); err != nil {
			status := http.StatusServiceUnavailable
			if errors.Is(err, errFinalSyncSessionInactive) || errors.Is(err, errFinalSyncSessionExpired) {
				status = http.StatusConflict
			} else {
				w.Header().Set("Retry-After", "1")
			}
			http.Error(w, err.Error(), status)
			return
		}
		writeJSON(w, map[string]string{"status": "released"})
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
