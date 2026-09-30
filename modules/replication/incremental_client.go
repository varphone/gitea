// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitea.dev/modules/json"
	"gitea.dev/modules/log"
	"gitea.dev/modules/setting"
)

const (
	requestRetryLimit       = 5
	requestRetryBaseDelay   = 500 * time.Millisecond
	requestRetryMaxDelay    = 5 * time.Second
	chunkProgressLogStride  = 128
	chunkChangeWarnBurst    = 8
	manifestPollInterval    = time.Second
	chunkChangeStopStride   = 256
	finalChunkFetchWorkers  = 8
	preflightChunkWorkers   = 4
	statusBodyPreviewLimit  = 4 << 10
	maxRetryResponseDrain   = 64 << 10
	maxRetryDrainDuration   = 250 * time.Millisecond
	responseBodyIdleTimeout = 2 * time.Minute
)

var syncBusyRetryDelay = time.Second

var errRemoteSnapshotUnavailable = errors.New("remote snapshot is unavailable")

type chunkChangedError struct {
	hash   string
	status string
}

func (e *chunkChangedError) Error() string {
	if e.status == "" {
		return fmt.Sprintf("chunk %s changed during preflight", e.hash)
	}
	return fmt.Sprintf("chunk %s changed during preflight: %s", e.hash, e.status)
}

func retryDelay(attempt int) time.Duration {
	if attempt <= 1 {
		return requestRetryBaseDelay
	}
	delay := requestRetryBaseDelay << (attempt - 1)
	if delay > requestRetryMaxDelay {
		return requestRetryMaxDelay
	}
	return delay
}

func responseRetryDelay(resp *http.Response, fallback time.Duration) time.Duration {
	if resp == nil {
		return fallback
	}
	retryAfter := strings.TrimSpace(resp.Header.Get("Retry-After"))
	if retryAfter == "" {
		return fallback
	}
	if delay, err := time.ParseDuration(retryAfter + "s"); err == nil && delay > fallback {
		return delay
	}
	if retryAt, err := http.ParseTime(retryAfter); err == nil {
		if delay := time.Until(retryAt); delay > fallback {
			return delay
		}
	}
	return fallback
}

func shouldRetryHTTPStatus(status int) bool {
	return status == http.StatusInternalServerError || status == http.StatusTooManyRequests || status == http.StatusBadGateway || status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout
}

type cancelRequestBody struct {
	io.ReadCloser
	cancel       context.CancelFunc
	parent       context.Context
	idleTimer    *time.Timer
	idleTimedOut atomic.Bool
	timerMu      sync.Mutex
	closed       bool
}

func (b *cancelRequestBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		b.timerMu.Lock()
		if !b.closed {
			b.idleTimer.Reset(responseBodyIdleTimeout)
		}
		b.timerMu.Unlock()
	}
	if err != nil && b.idleTimedOut.Load() && b.parent.Err() == nil {
		return n, fmt.Errorf("replication response body idle timeout after %s: %v", responseBodyIdleTimeout, err)
	}
	return n, err
}

func (b *cancelRequestBody) Close() error {
	b.timerMu.Lock()
	b.closed = true
	b.timerMu.Unlock()
	err := b.ReadCloser.Close()
	b.idleTimer.Stop()
	b.cancel()
	return err
}

func keepRequestContextUntilBodyClose(parent context.Context, resp *http.Response, cancel context.CancelFunc) {
	if resp == nil || resp.Body == nil {
		cancel()
		return
	}
	body := &cancelRequestBody{ReadCloser: resp.Body, cancel: cancel, parent: parent}
	body.idleTimer = time.AfterFunc(responseBodyIdleTimeout, func() {
		body.idleTimedOut.Store(true)
		cancel()
	})
	resp.Body = body
}

func closeRetryResponse(resp *http.Response, cancel context.CancelFunc) {
	if resp == nil || resp.Body == nil {
		cancel()
		return
	}
	timer := time.AfterFunc(maxRetryDrainDuration, cancel)
	n, drainErr := io.Copy(io.Discard, io.LimitReader(resp.Body, maxRetryResponseDrain+1))
	drainTimedOut := !timer.Stop()
	if drainTimedOut || drainErr != nil || n > maxRetryResponseDrain {
		cancel()
	}
	_ = resp.Body.Close()
	cancel()
}

func shouldRetryRequestError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "unexpected eof") ||
		strings.Contains(message, "gzip:") ||
		strings.Contains(message, "connection reset") ||
		strings.Contains(message, "broken pipe") ||
		strings.Contains(message, "refused") ||
		strings.Contains(message, "timeout") ||
		strings.Contains(message, "reset by peer")
}

func waitForRetry(ctx context.Context, attempt int, operation string, reason error) error {
	return waitForRetryDelay(ctx, attempt, operation, reason, retryDelay(attempt))
}

func waitForRetryResponse(ctx context.Context, attempt int, operation string, reason error, resp *http.Response) error {
	return waitForRetryDelay(ctx, attempt, operation, reason, responseRetryDelay(resp, retryDelay(attempt)))
}

func waitForRetryDelay(ctx context.Context, attempt int, operation string, reason error, delay time.Duration) error {
	log.Warn("%s failed (%v); retrying in %s (%d/%d)", operation, reason, delay, attempt, requestRetryLimit)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func doRetryableJSONRequest(ctx context.Context, client *http.Client, method, url, token, operation string, payload []byte) (*http.Response, error) {
	var lastErr error
	for attempt := 1; attempt <= requestRetryLimit; attempt++ {
		reqCtx, cancel := context.WithCancel(ctx)
		req, err := http.NewRequestWithContext(reqCtx, method, url, bytes.NewReader(payload))
		if err != nil {
			cancel()
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err == nil {
			if shouldRetryHTTPStatus(resp.StatusCode) && attempt < requestRetryLimit {
				retryErr := fmt.Errorf("%s returned %s", operation, resp.Status)
				closeRetryResponse(resp, cancel)
				if err := waitForRetryResponse(ctx, attempt, operation, retryErr, resp); err != nil {
					return nil, err
				}
				lastErr = retryErr
				continue
			}
			keepRequestContextUntilBodyClose(ctx, resp, cancel)
			return resp, nil
		}
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		cancel()
		if attempt == requestRetryLimit || !shouldRetryRequestError(err) {
			return nil, err
		}
		if err := waitForRetry(ctx, attempt, operation, err); err != nil {
			return nil, err
		}
		lastErr = err
	}
	return nil, lastErr
}

func doRetryableRequest(ctx context.Context, client *http.Client, method, url, token, operation string) (*http.Response, error) {
	var lastErr error
	for attempt := 1; attempt <= requestRetryLimit; attempt++ {
		reqCtx, cancel := context.WithCancel(ctx)
		req, err := http.NewRequestWithContext(reqCtx, method, url, nil)
		if err != nil {
			cancel()
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := client.Do(req)
		if err != nil {
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			cancel()
			if attempt == requestRetryLimit || !shouldRetryRequestError(err) {
				return nil, err
			}
			if retryErr := waitForRetry(ctx, attempt, operation, err); retryErr != nil {
				return nil, retryErr
			}
			lastErr = err
			continue
		}
		if shouldRetryHTTPStatus(resp.StatusCode) && attempt < requestRetryLimit {
			retryErr := fmt.Errorf("%s returned %s", operation, resp.Status)
			closeRetryResponse(resp, cancel)
			if err := waitForRetryResponse(ctx, attempt, operation, retryErr, resp); err != nil {
				return nil, err
			}
			lastErr = retryErr
			continue
		}
		keepRequestContextUntilBodyClose(ctx, resp, cancel)
		return resp, nil
	}
	if lastErr == nil {
		lastErr = errors.New("request retries exhausted")
	}
	return nil, lastErr
}

func responseStatusError(prefix string, resp *http.Response) error {
	body, err := io.ReadAll(io.LimitReader(resp.Body, statusBodyPreviewLimit+1))
	if err != nil {
		return fmt.Errorf("%s returned %s (read body: %v)", prefix, resp.Status, err)
	}
	message := strings.TrimSpace(string(body))
	if len(body) > statusBodyPreviewLimit {
		message += "..."
	}
	if message == "" {
		return fmt.Errorf("%s returned %s", prefix, resp.Status)
	}
	return fmt.Errorf("%s returned %s: %s", prefix, resp.Status, message)
}

func decodeBoundedJSON(body io.Reader, maxSize int64, value any) error {
	data, err := io.ReadAll(io.LimitReader(body, maxSize+1))
	if err != nil {
		return err
	}
	if int64(len(data)) > maxSize {
		return fmt.Errorf("JSON response exceeds maximum size of %d bytes", maxSize)
	}
	return json.Unmarshal(data, value)
}

func decodeManifestResponse(body io.Reader) (SnapshotManifest, error) {
	data, readErr := io.ReadAll(io.LimitReader(body, maxManifestSize+1))
	if len(data) > maxManifestSize {
		return SnapshotManifest{}, errors.New("source manifest exceeds maximum size")
	}
	var manifest SnapshotManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		if readErr != nil {
			return SnapshotManifest{}, fmt.Errorf("read manifest: %w (decode: %v)", readErr, err)
		}
		return SnapshotManifest{}, err
	}
	// A signed JSON document is self-contained. Some broken proxies truncate the
	// chunked response trailer after forwarding all document bytes; accepting that
	// transport error is safe because signature validation follows this decode.
	return manifest, nil
}

func requestManifest(ctx context.Context, client *http.Client, base, token, endpoint string) (*SnapshotManifest, error) {
	request := syncJobRequest{}
	if strings.Contains(endpoint, "preflight") {
		request.Kind = "preflight"
		if _, value, ok := strings.Cut(endpoint, "resume="); ok {
			request.ResumeJobID = value
		}
	} else {
		request.Kind = "final"
		if _, value, ok := strings.Cut(endpoint, "base="); ok {
			request.BaseJobID = value
		}
		var requestID [16]byte
		if _, err := rand.Read(requestID[:]); err != nil {
			return nil, fmt.Errorf("generate sync job request ID: %w", err)
		}
		request.RequestID = hex.EncodeToString(requestID[:])
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	operation := "create sync job " + request.Kind
	busyWaits := 0
	busyStarted := time.Time{}
	for attempt := 1; ; attempt++ {
		resp, err := doRetryableJSONRequest(ctx, client, http.MethodPost, base+syncJobsPath, token, operation, payload)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusAccepted {
			var job Snapshot
			if err := decodeBoundedJSON(resp.Body, 1<<20, &job); err != nil {
				_ = resp.Body.Close()
				if attempt < requestRetryLimit && shouldRetryRequestError(err) {
					if retryErr := waitForRetry(ctx, attempt, operation, err); retryErr != nil {
						return nil, retryErr
					}
					continue
				}
				return nil, err
			}
			_ = resp.Body.Close()
			if !validSnapshotID(job.ID) {
				return nil, errors.New("sync job response contains an invalid snapshot ID")
			}
			return pollManifestTask(ctx, client, base, token, job.ID, endpoint)
		}
		if resp.StatusCode != http.StatusOK {
			statusErr := responseStatusError(endpoint, resp)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusConflict && strings.Contains(statusErr.Error(), "sync already in progress") {
				if busyWaits == 0 {
					busyStarted = time.Now()
				}
				busyWaits++
				if busyWaits == 1 || busyWaits%30 == 0 {
					log.Info("Primary replication is busy; waiting to create %s job: waits=%d elapsed=%s reason=%q", request.Kind, busyWaits, time.Since(busyStarted), statusErr)
				}
				timer := time.NewTimer(responseRetryDelay(resp, syncBusyRetryDelay))
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil, ctx.Err()
				case <-timer.C:
				}
				attempt = 0
				continue
			}
			return nil, statusErr
		}
		if resp.ContentLength > maxManifestSize {
			_ = resp.Body.Close()
			return nil, errors.New("source manifest exceeds maximum size")
		}
		manifest, err := decodeManifestResponse(resp.Body)
		if err != nil {
			_ = resp.Body.Close()
			if attempt < requestRetryLimit && shouldRetryRequestError(err) {
				if retryErr := waitForRetry(ctx, attempt, operation, err); retryErr != nil {
					return nil, retryErr
				}
				continue
			}
			return nil, err
		}
		if err := validateIncrementalManifest(&manifest); err != nil {
			_ = resp.Body.Close()
			return nil, err
		}
		expectedState := "transferring"
		if strings.Contains(endpoint, "preflight") {
			expectedState = "preflight"
		}
		if manifest.State != expectedState {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("%s returned manifest state %q", endpoint, manifest.State)
		}
		if err := validateManifestIdentity(&manifest, token); err != nil {
			_ = resp.Body.Close()
			return nil, err
		}
		_ = resp.Body.Close()
		return &manifest, nil
	}
}

func requestSnapshotStatus(ctx context.Context, client *http.Client, base, token, id string) (*Snapshot, error) {
	operation := "poll snapshot " + id
	for attempt := 1; attempt <= requestRetryLimit; attempt++ {
		resp, err := doRetryableRequest(ctx, client, http.MethodGet, base+"/api/v1/replication/sync-jobs/"+id, token, operation)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			if resp.StatusCode == http.StatusNotFound {
				_ = resp.Body.Close()
				return nil, fmt.Errorf("%w: %s", errRemoteSnapshotUnavailable, id)
			}
			err := responseStatusError("snapshot "+id, resp)
			_ = resp.Body.Close()
			return nil, err
		}
		var snapshot Snapshot
		err = decodeBoundedJSON(resp.Body, 1<<20, &snapshot)
		_ = resp.Body.Close()
		if err == nil {
			return &snapshot, nil
		}
		if attempt == requestRetryLimit || !shouldRetryRequestError(err) {
			return nil, err
		}
		if retryErr := waitForRetry(ctx, attempt, operation, err); retryErr != nil {
			return nil, retryErr
		}
	}
	return nil, errors.New("snapshot status retries exhausted")
}

func requestManifestByID(ctx context.Context, client *http.Client, base, token, id, expectedState string) (*SnapshotManifest, error) {
	operation := "fetch manifest " + id
	for attempt := 1; attempt <= requestRetryLimit; attempt++ {
		resp, err := doRetryableRequest(ctx, client, http.MethodGet, base+"/api/v1/replication/sync-jobs/"+id+"/manifest", token, operation)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			err := responseStatusError("snapshot manifest "+id, resp)
			_ = resp.Body.Close()
			return nil, err
		}
		if resp.ContentLength > maxManifestSize {
			_ = resp.Body.Close()
			return nil, errors.New("source manifest exceeds maximum size")
		}
		manifest, err := decodeManifestResponse(resp.Body)
		if err != nil {
			_ = resp.Body.Close()
			if attempt < requestRetryLimit && shouldRetryRequestError(err) {
				if retryErr := waitForRetry(ctx, attempt, operation, err); retryErr != nil {
					return nil, retryErr
				}
				continue
			}
			return nil, err
		}
		if err := validateIncrementalManifest(&manifest); err != nil {
			_ = resp.Body.Close()
			return nil, err
		}
		if manifest.State != expectedState {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("snapshot manifest %s returned state %q, expected %q", id, manifest.State, expectedState)
		}
		if err := validateManifestIdentity(&manifest, token); err != nil {
			_ = resp.Body.Close()
			return nil, err
		}
		_ = resp.Body.Close()
		return &manifest, nil
	}
	return nil, errors.New("manifest fetch retries exhausted")
}

func pollManifestTask(ctx context.Context, client *http.Client, base, token, id, endpoint string) (*SnapshotManifest, error) {
	expectedState := "transferring"
	phase := "finalize"
	if strings.Contains(endpoint, "preflight") {
		expectedState = "preflight"
		phase = "preflight"
	}
	log.Info("Submitted %s task %s; polling snapshot status", phase, id)
	pollStarted := time.Now()
	polls := 0
	ticker := time.NewTicker(manifestPollInterval)
	defer ticker.Stop()
	for {
		snapshot, err := requestSnapshotStatus(ctx, client, base, token, id)
		if err != nil {
			log.Error("Failed to poll %s task %s after %s: %v", phase, id, time.Since(pollStarted), err)
			return nil, err
		}
		polls++
		switch snapshot.State {
		case snapshotStateCreating:
			if polls%60 == 0 {
				log.Info("%s task %s is still creating: polls=%d elapsed=%s", phase, id, polls, time.Since(pollStarted))
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-ticker.C:
			}
			continue
		case "failed":
			if snapshot.Error != "" {
				log.Error("%s task %s failed after %s: %s", phase, id, time.Since(pollStarted), snapshot.Error)
				return nil, fmt.Errorf("%s task %s failed: %s", phase, id, snapshot.Error)
			}
			log.Error("%s task %s failed after %s", phase, id, time.Since(pollStarted))
			return nil, fmt.Errorf("%s task %s failed", phase, id)
		case expectedState:
			log.Info("%s task %s reached state %s after %s (%d polls); downloading manifest", phase, id, expectedState, time.Since(pollStarted), polls)
			return requestManifestByID(ctx, client, base, token, id, expectedState)
		default:
			return nil, fmt.Errorf("%s task %s entered unexpected state %q", phase, id, snapshot.State)
		}
	}
}

func requestChunk(ctx context.Context, client *http.Client, base, token, id, hash string) ([]byte, error) {
	operation := fmt.Sprintf("request chunk: snapshot=%s hash=%s", id, hash)
	for attempt := 1; attempt <= requestRetryLimit; attempt++ {
		resp, err := doRetryableRequest(ctx, client, http.MethodGet, base+"/api/v1/replication/sync-jobs/"+id+"/chunks/"+hash, token, operation)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusNotFound {
			_ = resp.Body.Close()
			return nil, &chunkChangedError{hash: hash, status: resp.Status}
		}
		if resp.StatusCode != http.StatusOK {
			err := responseStatusError("chunk "+hash, resp)
			_ = resp.Body.Close()
			return nil, err
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, chunkMaxSize+1))
		_ = resp.Body.Close()
		if err != nil {
			if attempt < requestRetryLimit && shouldRetryRequestError(err) {
				if retryErr := waitForRetry(ctx, attempt, operation, err); retryErr != nil {
					return nil, retryErr
				}
				continue
			}
			return nil, err
		}
		if len(data) > chunkMaxSize {
			return nil, errors.New("source chunk exceeds maximum size")
		}
		return data, nil
	}
	return nil, errors.New("chunk request retries exhausted")
}

func finishRemoteSession(ctx context.Context, client *http.Client, base, token, id, action string) error {
	resp, err := doRetryableRequest(ctx, client, http.MethodPost, base+"/api/v1/replication/sync-jobs/"+id+"/session/"+action, token, action+" remote sync session")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return responseStatusError(action+" sync session", resp)
	}
	return nil
}

func previousManifest(path, token string) *SnapshotManifest {
	manifest, err := loadTrustedManifest(path, token, "ready")
	if err == nil {
		return manifest
	}
	if !errors.Is(err, errManifestTrailingData) {
		if !os.IsNotExist(err) {
			log.Warn("Ignoring persisted standby baseline %s: %v", path, err)
		}
		return nil
	}
	manifest, recoveryErr := recoverTrustedBaseline(path, token)
	if recoveryErr != nil {
		log.Warn("Ignoring persisted standby baseline %s: %v", path, recoveryErr)
		return nil
	}
	if err := writeManifestAt(path, manifest); err != nil {
		log.Warn("Recovered persisted standby baseline %s but could not rewrite it: %v", path, err)
	} else {
		log.Warn("Recovered persisted standby baseline %s with trailing data; rewrote canonical manifest", path)
	}
	return manifest
}

// recoverTrustedBaseline accepts only the first JSON value in a local ready
// checkpoint that loadManifestFile rejected for trailing data. The checkpoint
// must still pass every normal structural and cryptographic verification, then
// is atomically rewritten by previousManifest before being reused.
func recoverTrustedBaseline(path, token string) (*SnapshotManifest, error) {
	file, err := openManifestFile(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var manifest SnapshotManifest
	if err := json.NewDecoder(io.LimitReader(file, int64(maxManifestSize)+1)).Decode(&manifest); err != nil {
		return nil, err
	}
	if err := validateIncrementalManifest(&manifest); err != nil {
		return nil, err
	}
	if manifest.State != "ready" {
		return nil, fmt.Errorf("manifest state is %q, expected %q", manifest.State, "ready")
	}
	if err := validateManifestIdentity(&manifest, token); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func cacheHas(cacheDir, hash string) bool {
	return verifyFile(cachePath(cacheDir, hash), hash) == nil
}

// Defer cache hashing until stage assembly to avoid reading chunks twice.
func cachedChunkAvailable(cacheDir, hash string, size int64) bool {
	info, err := os.Lstat(cachePath(cacheDir, hash))
	return err == nil && info.Mode().IsRegular() && info.Size() == size
}

func pruneChunkCache(ctx context.Context, cacheDir, snapshotID string, manifest *SnapshotManifest) error {
	keep := make(map[string]int64)
	if manifest != nil {
		for _, entry := range manifest.Files {
			for _, chunk := range entry.Chunks {
				if err := ctx.Err(); err != nil {
					return err
				}
				keep[chunk.Hash] = chunk.Size
			}
		}
	}

	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		return err
	}
	var removed, removedFileBytes int64
	var changedShards sync.Map
	for _, shard := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		shardPath := filepath.Join(cacheDir, shard.Name())
		if len(shard.Name()) != 2 || !isLowerHex(shard.Name()) {
			info, err := os.Lstat(shardPath)
			if err != nil {
				return err
			}
			if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
				if err := makeTreeRemovable(ctx, shardPath); err != nil {
					return fmt.Errorf("make unexpected cache entry removable: %w", err)
				}
			}
			if err := os.RemoveAll(shardPath); err != nil {
				return fmt.Errorf("remove unexpected cache entry %q: %w", shard.Name(), err)
			}
			removed++
			if info.Mode().IsRegular() {
				removedFileBytes += info.Size()
			}
			continue
		}
		shardInfo, err := os.Lstat(shardPath)
		if err != nil {
			return err
		}
		if !shardInfo.IsDir() || shardInfo.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("replication chunk cache shard %q is not a real directory", shard.Name())
		}
		chunks, err := os.ReadDir(shardPath)
		if err != nil {
			return err
		}
		for _, chunk := range chunks {
			if err := ctx.Err(); err != nil {
				return err
			}
			name := chunk.Name()
			chunkPath := filepath.Join(shardPath, name)
			info, err := os.Lstat(chunkPath)
			if err != nil {
				return err
			}
			wantSize, needed := keep[name]
			if needed && len(name) == sha256.Size*2 && isLowerHex(name) && strings.HasPrefix(name, shard.Name()) && info.Mode().IsRegular() && info.Size() == wantSize {
				continue
			}
			if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
				if err := makeTreeRemovable(ctx, chunkPath); err != nil {
					return fmt.Errorf("make stale chunk cache entry removable: %w", err)
				}
			}
			if err := os.RemoveAll(chunkPath); err != nil {
				return fmt.Errorf("remove stale chunk cache entry %q: %w", name, err)
			}
			removed++
			if info.Mode().IsRegular() {
				removedFileBytes += info.Size()
			}
			changedShards.LoadOrStore(shard.Name(), struct{}{})
		}
	}
	if removed == 0 {
		return nil
	}
	hasChangedShard := false
	changedShards.Range(func(_, _ any) bool {
		hasChangedShard = true
		return false
	})
	if !hasChangedShard {
		if err := syncDirectory(cacheDir); err != nil {
			return fmt.Errorf("persist replication chunk cache pruning: %w", err)
		}
	} else if err := syncChunkCacheDirectories(cacheDir, &changedShards); err != nil {
		return fmt.Errorf("persist replication chunk cache pruning: %w", err)
	}
	log.Info("Pruned stale replication chunk cache: snapshot=%s removed_entries=%d removed_file_bytes=%d retained_hashes=%d", snapshotID, removed, removedFileBytes, len(keep))
	return nil
}

func readCachedChunk(cacheDir, hash string) ([]byte, error) {
	path := cachePath(cacheDir, hash)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > chunkMaxSize {
		return nil, errors.New("cached chunk is not a regular file within the size limit")
	}
	file, openedInfo, err := openRegularFile(path, info)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if openedInfo.Size() > chunkMaxSize {
		return nil, errors.New("cached chunk is not a regular file within the size limit")
	}
	data, err := io.ReadAll(io.LimitReader(file, chunkMaxSize+1))
	if err != nil {
		return nil, err
	}
	if len(data) > chunkMaxSize {
		return nil, errors.New("cached chunk exceeds maximum size")
	}
	sum := sha256.Sum256(data)
	actual := hex.EncodeToString(sum[:])
	if actual != hash {
		return nil, fmt.Errorf("cached chunk sha256 mismatch: got %s want %s", actual, hash)
	}
	return data, nil
}

func transferRateMiBPerSecond(size int64, duration time.Duration) float64 {
	if duration <= 0 {
		return 0
	}
	return float64(size) / duration.Seconds() / (1 << 20)
}

type chunkPlanState uint8

const (
	chunkPlanReusable chunkPlanState = iota + 1
	chunkPlanSeen
)

type chunkFetchPlan struct {
	hashes       []string
	total        int
	reusable     int
	reusableSize int64
	missingSize  int64
}

func planMissingChunks(ctx context.Context, manifest, previous *SnapshotManifest, cacheDir string) (chunkFetchPlan, error) {
	plan := chunkFetchPlan{}
	states := make(map[string]chunkPlanState)
	if previous != nil {
		for _, entry := range previous.Files {
			for _, chunk := range entry.Chunks {
				if err := ctx.Err(); err != nil {
					return chunkFetchPlan{}, err
				}
				states[chunk.Hash] = chunkPlanReusable
			}
		}
	}
	for _, entry := range manifest.Files {
		for _, chunk := range entry.Chunks {
			if err := ctx.Err(); err != nil {
				return chunkFetchPlan{}, err
			}
			state := states[chunk.Hash]
			if state == chunkPlanSeen {
				continue
			}
			states[chunk.Hash] = chunkPlanSeen
			plan.total++
			if state == chunkPlanReusable || cachedChunkAvailable(cacheDir, chunk.Hash, chunk.Size) {
				plan.reusable++
				plan.reusableSize += chunk.Size
				continue
			}
			plan.hashes = append(plan.hashes, chunk.Hash)
			plan.missingSize += chunk.Size
		}
	}
	return plan, nil
}

func fetchMissingChunks(ctx context.Context, client *http.Client, base, token string, manifest, previous *SnapshotManifest, cacheDir string, tolerateChanges bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	passStarted := time.Now()
	plan, err := planMissingChunks(ctx, manifest, previous, cacheDir)
	if err != nil {
		return err
	}
	if !tolerateChanges {
		if err := fetchChunksConcurrently(ctx, client, base, token, manifest.ID, plan.hashes, cacheDir, plan.total, plan.reusable, plan.missingSize); err != nil {
			return err
		}
		log.Info("Final chunk pass prepared for snapshot %s: download_chunks=%d reusable_candidates=%d total_chunks=%d reusable_candidate_payload_bytes=%d expected_payload_bytes=%d duration=%s", manifest.ID, len(plan.hashes), plan.reusable, plan.total, plan.reusableSize, plan.missingSize, time.Since(passStarted))
		return nil
	}
	return fetchPreflightChunksConcurrently(ctx, client, base, token, manifest.ID, plan.hashes, cacheDir, plan.total, plan.reusable, plan.reusableSize, plan.missingSize, time.Since(passStarted))
}

func fetchChunksConcurrently(ctx context.Context, client *http.Client, base, token, id string, hashes []string, cacheDir string, total, cached int, totalBytes int64) error {
	if len(hashes) == 0 {
		return ctx.Err()
	}
	started := time.Now()
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan string)
	workerCount := min(finalChunkFetchWorkers, len(hashes))
	var workers sync.WaitGroup
	var firstErr error
	var firstErrOnce sync.Once
	var cacheShards sync.Map
	var fetched atomic.Int64
	var fetchedBytes atomic.Int64
	var lastProgress atomic.Int64
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for hash := range jobs {
				data, err := requestChunk(workerCtx, client, base, token, id, hash)
				if err == nil {
					err = storeChunkForBatch(cacheDir, hash, data)
				}
				if err != nil {
					firstErrOnce.Do(func() {
						firstErr = err
						cancel()
					})
					return
				}
				cacheShards.LoadOrStore(hash[:2], struct{}{})
				count := fetched.Add(1)
				bytes := fetchedBytes.Add(int64(len(data)))
				now := time.Now()
				logProgress := false
				if last := lastProgress.Load(); last == 0 {
					logProgress = lastProgress.CompareAndSwap(0, now.UnixNano())
				} else if now.Sub(time.Unix(0, last)) >= 30*time.Second && lastProgress.CompareAndSwap(last, now.UnixNano()) {
					logProgress = true
				}
				if logProgress {
					elapsed := time.Since(started)
					rate := transferRateMiBPerSecond(bytes, elapsed)
					log.Info("Final chunk transfer progress: snapshot=%s fetched_chunks=%d total_chunks=%d payload_bytes=%d/%d average_payload_mib_per_sec=%.2f cached_candidates=%d elapsed=%s", id, count, total, bytes, totalBytes, rate, cached, elapsed)
				}
			}
		}()
	}
sendJobs:
	for _, hash := range hashes {
		select {
		case <-workerCtx.Done():
			break sendJobs
		case jobs <- hash:
		}
	}
	close(jobs)
	workers.Wait()
	if err := syncChunkCacheDirectories(cacheDir, &cacheShards); err != nil {
		log.Error("Persist final chunk cache failed: snapshot=%s error=%v", id, err)
		firstErr = errors.Join(firstErr, err)
	}
	elapsed := time.Since(started)
	bytes := fetchedBytes.Load()
	if firstErr != nil {
		log.Error("Final chunk transfer failed: snapshot=%s fetched=%d/%d payload_bytes=%d/%d elapsed=%s error=%v", id, fetched.Load(), len(hashes), bytes, totalBytes, elapsed, firstErr)
		return firstErr
	}
	if err := ctx.Err(); err != nil {
		log.Error("Final chunk transfer canceled: snapshot=%s fetched=%d/%d payload_bytes=%d/%d elapsed=%s error=%v", id, fetched.Load(), len(hashes), bytes, totalBytes, elapsed, err)
		return err
	}
	rate := transferRateMiBPerSecond(bytes, elapsed)
	log.Info("Final chunk transfer completed: snapshot=%s fetched=%d/%d payload_bytes=%d/%d average_payload_mib_per_sec=%.2f cached_candidates=%d elapsed=%s", id, fetched.Load(), len(hashes), bytes, totalBytes, rate, cached, elapsed)
	return nil
}

func fetchPreflightChunksConcurrently(ctx context.Context, client *http.Client, base, token, id string, hashes []string, cacheDir string, total, cached int, cachedBytes, expectedBytes int64, preparationDuration time.Duration) error {
	if len(hashes) == 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		log.Info("Finished preflight chunk pass for snapshot %s: fetched=0 payload_bytes=0 reusable_candidates=%d reusable_candidate_payload_bytes=%d deferred=0 total=%d duration=%s", id, cached, cachedBytes, total, preparationDuration)
		return nil
	}
	started := time.Now()
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan string)
	workerCount := min(preflightChunkWorkers, len(hashes))
	var workers sync.WaitGroup
	var firstErr error
	var firstHash string
	var firstErrOnce sync.Once
	var fetched atomic.Int64
	var fetchedBytes atomic.Int64
	var deferred atomic.Int64
	var cacheShards sync.Map
	var lastProgress atomic.Int64
	var stopAfterChurn atomic.Bool
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for hash := range jobs {
				if stopAfterChurn.Load() {
					continue
				}
				data, err := requestChunk(workerCtx, client, base, token, id, hash)
				if err != nil {
					if stopAfterChurn.Load() && errors.Is(err, context.Canceled) {
						return
					}
					var changed *chunkChangedError
					if errors.As(err, &changed) {
						count := deferred.Add(1)
						if count <= chunkChangeWarnBurst {
							log.Warn("Preflight chunk changed; snapshot=%s hash=%s deferred_to=final_sync", id, hash)
						} else if count%chunkProgressLogStride == 0 {
							processed := int64(cached) + fetched.Load() + count
							log.Warn("Preflight progress for snapshot %s remains unstable: processed=%d/%d fetched=%d cached=%d deferred=%d", id, processed, total, fetched.Load(), cached, count)
						}
						if fetched.Load() == 0 && count >= chunkChangeStopStride && stopAfterChurn.CompareAndSwap(false, true) {
							log.Warn("Preflight prefetch for snapshot %s stopped scheduling after %d changed chunks before any successful fetch", id, count)
							cancel()
						}
						continue
					}
					firstErrOnce.Do(func() {
						firstErr, firstHash = err, hash
						cancel()
					})
					return
				}
				if err := storeChunkForBatch(cacheDir, hash, data); err != nil {
					firstErrOnce.Do(func() {
						firstErr, firstHash = err, hash
						cancel()
					})
					return
				}
				cacheShards.LoadOrStore(hash[:2], struct{}{})
				count := fetched.Add(1)
				bytes := fetchedBytes.Add(int64(len(data)))
				now := time.Now()
				logProgress := false
				if last := lastProgress.Load(); last == 0 {
					logProgress = lastProgress.CompareAndSwap(0, now.UnixNano())
				} else if now.Sub(time.Unix(0, last)) >= 30*time.Second && lastProgress.CompareAndSwap(last, now.UnixNano()) {
					logProgress = true
				}
				if logProgress {
					elapsed := time.Since(started)
					rate := transferRateMiBPerSecond(bytes, elapsed)
					log.Info("Preflight chunk transfer progress: snapshot=%s fetched_chunks=%d/%d payload_bytes=%d expected_payload_bytes=%d average_payload_mib_per_sec=%.2f reusable_candidates=%d deferred=%d elapsed=%s", id, count, total, bytes, expectedBytes, rate, cached, deferred.Load(), elapsed)
				}
			}
		}()
	}
sendJobs:
	for _, hash := range hashes {
		if stopAfterChurn.Load() {
			break
		}
		select {
		case <-workerCtx.Done():
			break sendJobs
		case jobs <- hash:
		}
	}
	close(jobs)
	workers.Wait()
	if err := syncChunkCacheDirectories(cacheDir, &cacheShards); err != nil {
		log.Error("Persist preflight chunk cache failed: snapshot=%s error=%v", id, err)
		firstErr = errors.Join(firstErr, err)
	}
	elapsed := preparationDuration + time.Since(started)
	fetchedCount, bytes, deferredCount := fetched.Load(), fetchedBytes.Load(), deferred.Load()
	if firstErr != nil {
		log.Error("Preflight chunk transfer failed: snapshot=%s hash=%s fetched=%d/%d payload_bytes=%d expected_payload_bytes=%d cached_candidates=%d deferred=%d elapsed=%s error=%v", id, firstHash, fetchedCount, len(hashes), bytes, expectedBytes, cached, deferredCount, elapsed, firstErr)
		return firstErr
	}
	if err := ctx.Err(); err != nil {
		log.Error("Preflight chunk transfer canceled: snapshot=%s fetched=%d/%d payload_bytes=%d expected_payload_bytes=%d cached_candidates=%d deferred=%d elapsed=%s error=%v", id, fetchedCount, len(hashes), bytes, expectedBytes, cached, deferredCount, elapsed, err)
		return err
	}
	if fetchedCount == 0 && deferredCount > 0 {
		log.Warn("Finished preflight chunk pass for snapshot %s without caching any chunks; source changed before every fetch (cached=%d deferred=%d total=%d elapsed=%s)", id, cached, deferredCount, total, elapsed)
	}
	log.Info("Finished preflight chunk pass for snapshot %s: fetched=%d payload_bytes=%d reusable_candidates=%d reusable_candidate_payload_bytes=%d average_payload_mib_per_sec=%.2f deferred=%d total=%d duration=%s", id, fetchedCount, bytes, cached, cachedBytes, transferRateMiBPerSecond(bytes, elapsed), deferredCount, total, elapsed)
	return nil
}

func sameFile(a, b TreeEntry) bool {
	return a.Type == "file" && b.Type == "file" && a.ChangeID != "" && b.ChangeID != "" &&
		a.Size == b.Size && a.Mode == b.Mode &&
		a.ModTimeNS == b.ModTimeNS && reflect.DeepEqual(a.Chunks, b.Chunks)
}

func reusableWholeFile(path string, entry TreeEntry) bool {
	_, ok := reusableWholeFileInfo(path, entry)
	return ok
}

func reusableWholeFileInfo(path string, entry TreeEntry) (os.FileInfo, bool) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() != entry.Size ||
		uint32(info.Mode().Perm()) != entry.Mode || info.ModTime().UnixNano() != entry.ModTimeNS {
		return nil, false
	}
	return info, true
}

func fileMatchesManifestChunks(ctx context.Context, path string, entry TreeEntry) (bool, error) {
	chunks, err := splitFile(ctx, path)
	if err != nil {
		return false, err
	}
	return sameChunks(chunks, entry.Chunks), nil
}

func makeTreeRemovable(ctx context.Context, root string) error {
	return filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if info.IsDir() {
			return os.Chmod(path, 0o700)
		}
		return nil
	})
}

func stagingPathError(operation, rel string, err error) error {
	return fmt.Errorf("%s staging path %q: %w", operation, rel, err)
}

func createStageTempDir(stage string, manifest *SnapshotManifest) (string, error) {
	for {
		dir, err := os.MkdirTemp(stage, ".replication-tmp-*")
		if err != nil {
			return "", err
		}
		rel := filepath.ToSlash(filepath.Base(dir))
		conflicts := false
		for _, entry := range manifest.Files {
			if entry.Path == rel || strings.HasPrefix(entry.Path, rel+"/") {
				conflicts = true
				break
			}
		}
		if !conflicts {
			return dir, nil
		}
		if err := os.Remove(dir); err != nil {
			return "", fmt.Errorf("remove conflicting staging temporary directory %q: %w", rel, err)
		}
	}
}

func pruneUnexpectedStageEntries(ctx context.Context, stage string, manifest *SnapshotManifest) error {
	expected := make(map[string]struct{}, len(manifest.Files))
	for _, entry := range manifest.Files {
		expected[entry.Path] = struct{}{}
	}
	removed := 0
	err := filepath.Walk(stage, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if path == stage {
			return os.Chmod(path, 0o700)
		}
		rel, err := filepath.Rel(stage, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if _, ok := expected[rel]; !ok {
			if info.IsDir() {
				if err := makeTreeRemovable(ctx, path); err != nil {
					return fmt.Errorf("make unexpected staging tree %q removable: %w", rel, err)
				}
			}
			if err := os.RemoveAll(path); err != nil {
				return fmt.Errorf("remove unexpected staging entry %q: %w", rel, err)
			}
			removed++
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if info.IsDir() {
			return os.Chmod(path, 0o700)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if removed > 0 {
		log.Info("Removed unexpected entries from resumed staging tree: snapshot=%s entries=%d", manifest.ID, removed)
	}
	return nil
}

func buildIncrementalStage(ctx context.Context, root, stage, cacheDir string, manifest, previous *SnapshotManifest, fetch func(string) ([]byte, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stageInfo, err := os.Lstat(stage)
	if err != nil {
		return fmt.Errorf("inspect incremental staging directory %q: %w", stage, err)
	}
	if !stageInfo.IsDir() || stageInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("incremental staging path %q must be a real directory", stage)
	}
	if err := pruneUnexpectedStageEntries(ctx, stage, manifest); err != nil {
		return err
	}
	stageTempDir, err := createStageTempDir(stage, manifest)
	if err != nil {
		return fmt.Errorf("create incremental staging temporary directory: %w", err)
	}
	stageTempDirOwned := true
	defer func() {
		if stageTempDirOwned {
			if err := os.RemoveAll(stageTempDir); err != nil {
				log.Warn("Remove incomplete staging temporary directory failed: snapshot=%s path=%s error=%v", manifest.ID, filepath.Base(stageTempDir), err)
			}
		}
	}()
	// Keep directories writable while their children are reconstructed;
	// the manifest permissions are restored after the complete tree exists.
	directories := make([]TreeEntry, 0)
	for _, entry := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.Type == "dir" {
			directories = append(directories, entry)
		}
	}
	slices.SortFunc(directories, func(a, b TreeEntry) int {
		depthA, depthB := strings.Count(a.Path, "/"), strings.Count(b.Path, "/")
		if depthA != depthB {
			return depthA - depthB
		}
		return strings.Compare(a.Path, b.Path)
	})
	for _, entry := range directories {
		if err := ctx.Err(); err != nil {
			return err
		}
		dst := filepath.Join(stage, filepath.FromSlash(entry.Path))
		info, err := os.Lstat(dst)
		if err == nil {
			if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
				if err := os.Chmod(dst, 0o700); err != nil {
					return stagingPathError("make directory writable", entry.Path, err)
				}
				continue
			}
			if err := os.RemoveAll(dst); err != nil {
				return stagingPathError("remove conflicting directory entry", entry.Path, err)
			}
		} else if !os.IsNotExist(err) {
			return stagingPathError("inspect directory", entry.Path, err)
		}
		if err := os.Mkdir(dst, 0o700); err != nil {
			return stagingPathError("create directory", entry.Path, err)
		}
	}
	oldEntries := map[string]TreeEntry{}
	oldChunks := map[string]chunkLocation{}
	oldChunkAlternates := map[string][]chunkLocation{}
	var resolvedRoot string
	verifyLocal := previous != nil && !manifest.FullScanAt.IsZero() && manifest.FullScanAt.After(previous.FullScanAt)
	if previous != nil {
		resolvedRoot, err = resolvedPath(root)
		if err != nil {
			return fmt.Errorf("resolve source data root %q: %w", root, err)
		}
		for _, entry := range previous.Files {
			if err := ctx.Err(); err != nil {
				return err
			}
			oldEntries[entry.Path] = entry
		}
		oldChunks, oldChunkAlternates, err = indexManifestWithAlternatesContext(ctx, previous)
		if err != nil {
			return fmt.Errorf("index previous manifest chunks: %w", err)
		}
	}
	for entryIndex, entry := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		dst := filepath.Join(stage, filepath.FromSlash(entry.Path))
		switch entry.Type {
		case "dir":
			continue
		case "symlink":
			if target, err := os.Readlink(dst); err == nil && target == entry.LinkTarget {
				continue
			}
			if err := os.RemoveAll(dst); err != nil {
				return stagingPathError("remove conflicting symlink entry", entry.Path, err)
			}
			if err := os.Symlink(entry.LinkTarget, dst); err != nil {
				return stagingPathError("create symlink", entry.Path, err)
			}
		case "file":
			if reusableWholeFile(dst, entry) {
				matches, err := fileMatchesManifestChunks(ctx, dst, entry)
				if err == nil && matches {
					continue
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
			}
			old := oldEntries[entry.Path]
			source := filepath.Join(root, filepath.FromSlash(entry.Path))
			if sameFile(entry, old) {
				sourceInfo, reusable := reusableWholeFileInfo(source, entry)
				if reusable {
					matches := !verifyLocal
					if verifyLocal {
						var err error
						matches, err = fileMatchesManifestChunks(ctx, source, entry)
						if err != nil && ctx.Err() != nil {
							return ctx.Err()
						}
					}
					if matches {
						if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
							return stagingPathError("create parent directory for reused file", entry.Path, err)
						}
						if err := os.Link(source, dst); err == nil {
							linkedInfo, statErr := os.Lstat(dst)
							if statErr == nil && linkedInfo.Mode().IsRegular() && os.SameFile(sourceInfo, linkedInfo) {
								continue
							}
							if removeErr := os.Remove(dst); removeErr != nil && !os.IsNotExist(removeErr) {
								return fmt.Errorf("remove invalid reused staging file %q: %w", entry.Path, removeErr)
							}
						}
					}
				}
			}
			if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
				return stagingPathError("create parent directory for file", entry.Path, err)
			}
			if info, err := os.Lstat(dst); err == nil && info.IsDir() {
				if err := os.RemoveAll(dst); err != nil {
					return stagingPathError("remove conflicting file entry", entry.Path, err)
				}
			} else if err != nil && !os.IsNotExist(err) {
				return stagingPathError("inspect file destination", entry.Path, err)
			}
			tmp := filepath.Join(stageTempDir, strconv.Itoa(entryIndex))
			out, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(entry.Mode))
			if err != nil {
				return stagingPathError("create temporary file", entry.Path, err)
			}
			for _, chunk := range entry.Chunks {
				var data []byte
				if cached, err := readCachedChunk(cacheDir, chunk.Hash); err == nil {
					data = cached
				} else if location, ok := oldChunks[chunk.Hash]; ok {
					data, err = readChunkFromRoot(root, resolvedRoot, location, chunk.Hash)
					if err != nil {
						for _, alternate := range oldChunkAlternates[chunk.Hash] {
							data, err = readChunkFromRoot(root, resolvedRoot, alternate, chunk.Hash)
							if err == nil {
								log.Debug("Reused replication chunk from alternate local manifest location: snapshot=%s hash=%s path=%s", manifest.ID, chunk.Hash, alternate.Path)
								break
							}
						}
						if err != nil {
							data = nil
						}
					}
				}
				if data == nil {
					data, err = fetch(chunk.Hash)
					if err == nil {
						err = storeChunk(cacheDir, chunk.Hash, data)
					}
					if err != nil {
						_ = out.Close()
						return fmt.Errorf("restore staging file %q from chunk %s: %w", entry.Path, chunk.Hash, err)
					}
				}
				if int64(len(data)) != chunk.Size {
					_ = out.Close()
					return fmt.Errorf("staging file %q chunk %s size mismatch: got %d want %d", entry.Path, chunk.Hash, len(data), chunk.Size)
				}
				if _, err := out.Write(data); err != nil {
					_ = out.Close()
					return stagingPathError("write file data", entry.Path, err)
				}
			}
			if err := out.Chmod(os.FileMode(entry.Mode)); err != nil {
				_ = out.Close()
				return stagingPathError("restore file permissions", entry.Path, err)
			}
			mtime := time.Unix(0, entry.ModTimeNS)
			if err := os.Chtimes(tmp, mtime, mtime); err != nil {
				_ = out.Close()
				return stagingPathError("restore file timestamps", entry.Path, err)
			}
			if err := out.Sync(); err != nil {
				_ = out.Close()
				return stagingPathError("sync file", entry.Path, err)
			}
			if err := out.Close(); err != nil {
				return stagingPathError("close file", entry.Path, err)
			}
			if err := os.Rename(tmp, dst); err != nil {
				return stagingPathError("activate temporary file", entry.Path, err)
			}
		}
	}
	if err := os.RemoveAll(stageTempDir); err != nil {
		return fmt.Errorf("remove incremental staging temporary directory %q: %w", filepath.Base(stageTempDir), err)
	}
	stageTempDirOwned = false
	for _, entry := range slices.Backward(directories) {
		dst := filepath.Join(stage, filepath.FromSlash(entry.Path))
		if err := os.Chmod(dst, os.FileMode(entry.Mode)); err != nil {
			return stagingPathError("restore directory permissions", entry.Path, err)
		}
		mtime := time.Unix(0, entry.ModTimeNS)
		if err := os.Chtimes(dst, mtime, mtime); err != nil {
			return stagingPathError("restore directory timestamps", entry.Path, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Chmod(stage, os.FileMode(manifest.RootMode)); err != nil {
		return fmt.Errorf("restore staging root permissions: %w", err)
	}
	return nil
}

func persistStandbyManifest(snapshotDir string, manifest *SnapshotManifest) error {
	return writeManifestAt(manifestPath(snapshotDir, manifest.ID), manifest)
}

type stageCheckpoint struct {
	SnapshotID  string `json:"snapshot_id"`
	ManifestSHA string `json:"manifest_sha"`
}

const maxStageCheckpointSize = 4 << 10

func stageCheckpointPath(cfg *config) string {
	return filepath.Join(cfg.SnapshotDir, ".install-stage.checkpoint")
}

func readStageCheckpoint(path string) (stageCheckpoint, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return stageCheckpoint{}, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxStageCheckpointSize {
		return stageCheckpoint{}, errors.New("invalid staging checkpoint file")
	}
	file, _, err := openRegularFile(path, info)
	if err != nil {
		return stageCheckpoint{}, fmt.Errorf("open staging checkpoint: %w", err)
	}
	defer file.Close()
	var checkpoint stageCheckpoint
	if err := decodeBoundedJSON(file, maxStageCheckpointSize, &checkpoint); err != nil {
		return stageCheckpoint{}, err
	}
	return checkpoint, nil
}

func prepareIncrementalStage(ctx context.Context, cfg *config, final *SnapshotManifest, stage string) (bool, error) {
	checkpointPath := stageCheckpointPath(cfg)
	if checkpoint, err := readStageCheckpoint(checkpointPath); err == nil && checkpoint.SnapshotID == final.ID && checkpoint.ManifestSHA == final.SHA256 {
		if info, err := os.Lstat(stage); err == nil && info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
			return true, nil
		}
	}
	if _, err := os.Lstat(stage); err == nil {
		if err := makeTreeRemovable(ctx, stage); err != nil {
			return false, fmt.Errorf("make previous staging tree removable: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return false, err
	}
	if err := os.RemoveAll(stage); err != nil {
		return false, err
	}
	if err := os.Remove(checkpointPath); err != nil && !os.IsNotExist(err) {
		return false, err
	}
	if err := os.Mkdir(stage, 0o700); err != nil {
		return false, err
	}
	data, err := json.Marshal(stageCheckpoint{SnapshotID: final.ID, ManifestSHA: final.SHA256})
	if err != nil {
		return false, err
	}
	if err := writeFileSynced(checkpointPath, data, 0o600); err != nil {
		return false, err
	}
	return false, nil
}

func resumablePreflightManifest(snapshotDir, token string) *SnapshotManifest {
	paths, err := listManifestPaths(snapshotDir)
	if err != nil {
		log.Warn("Cannot list standby manifests for preflight recovery in %s: %v", snapshotDir, err)
		return nil
	}
	now := time.Now().UTC()
	var latest *SnapshotManifest
	for _, path := range paths {
		if id := strings.TrimSuffix(filepath.Base(path), ".json"); !validSnapshotID(id) {
			continue
		}
		manifest, err := loadManifestFile(path)
		if err != nil {
			if !os.IsNotExist(err) {
				log.Warn("Ignore invalid standby preflight recovery manifest %s: %v", path, err)
			}
			continue
		}
		if manifest.State != "preflight" {
			continue
		}
		if err := validateManifestIdentity(manifest, token); err != nil {
			log.Warn("Ignore untrusted standby preflight recovery manifest %s: %v", path, err)
			continue
		}
		if !preflightIsFresh(manifest, now) {
			log.Debug("Ignore stale standby preflight recovery manifest %s: created_at=%s", path, manifest.CreatedAt)
			continue
		}
		if latest != nil && !manifest.CreatedAt.After(latest.CreatedAt) {
			continue
		}
		latest = manifest
	}
	return latest
}

func resumableFinalManifest(snapshotDir, token string) *SnapshotManifest {
	paths, err := listManifestPaths(snapshotDir)
	if err != nil {
		log.Warn("Cannot list standby manifests for final recovery in %s: %v", snapshotDir, err)
		return nil
	}
	var latest *SnapshotManifest
	for _, path := range paths {
		if id := strings.TrimSuffix(filepath.Base(path), ".json"); !validSnapshotID(id) {
			continue
		}
		manifest, err := loadManifestFile(path)
		if err != nil {
			if !os.IsNotExist(err) {
				log.Warn("Ignore invalid standby final recovery manifest %s: %v", path, err)
			}
			continue
		}
		if manifest.State != "transferring" {
			continue
		}
		if err := validateManifestIdentity(manifest, token); err != nil {
			log.Warn("Ignore untrusted standby final recovery manifest %s: %v", path, err)
			continue
		}
		if latest != nil && !manifest.CreatedAt.After(latest.CreatedAt) {
			continue
		}
		latest = manifest
	}
	return latest
}

func preserveFinalSession(err error) bool {
	if shouldRetryRequestError(err) {
		return true
	}
	message := err.Error()
	return strings.Contains(message, "429 Too Many Requests") ||
		strings.Contains(message, "502 Bad Gateway") ||
		strings.Contains(message, "503 Service Unavailable") ||
		strings.Contains(message, "504 Gateway Timeout")
}

func completeFinalSync(ctx context.Context, cfg *config, base string, client *http.Client, final, previous *SnapshotManifest, cacheDir, stage string) error {
	syncStarted := time.Now()
	completed := false
	abortSession := true
	defer func() {
		if !completed && abortSession {
			abortCtx, cancel := context.WithTimeout(context.Background(), cfg.ServiceTimeout)
			log.Warn("Final sync session %s did not complete; aborting remote session", final.ID)
			if err := finishRemoteSession(abortCtx, client, base, cfg.ControlToken, final.ID, "abort"); err != nil {
				log.Warn("Abort remote final sync session failed: snapshot=%s error=%v", final.ID, err)
			}
			cancel()
		}
	}()
	if err := pruneChunkCache(ctx, cacheDir, final.ID, final); err != nil {
		return fmt.Errorf("prune stale replication chunk cache: %w", err)
	}
	chunkPassStarted := time.Now()
	if err := fetchMissingChunks(ctx, client, base, cfg.ControlToken, final, previous, cacheDir, false); err != nil {
		log.Error("Final chunk preparation failed: snapshot=%s duration=%s error=%v", final.ID, time.Since(chunkPassStarted), err)
		if preserveFinalSession(err) {
			abortSession = false
			log.Warn("Final sync session %s is retained for retry after transient transport failure: %v", final.ID, err)
		}
		return err
	}
	log.Info("Final chunk preparation completed: snapshot=%s duration=%s", final.ID, time.Since(chunkPassStarted))
	resumedStage, err := prepareIncrementalStage(ctx, cfg, final, stage)
	if err != nil {
		log.Error("Cannot prepare incremental stage: snapshot=%s stage=%s error=%v", final.ID, stage, err)
		return err
	}
	if resumedStage {
		log.Info("Resuming prepared staging tree for snapshot %s", final.ID)
	}
	root := filepath.Clean(setting.AppWorkPath)
	stageStarted := time.Now()
	fetch := func(hash string) ([]byte, error) {
		started := time.Now()
		data, err := requestChunk(ctx, client, base, cfg.ControlToken, final.ID, hash)
		if err != nil {
			log.Warn("On-demand final chunk fetch failed: snapshot=%s hash=%s duration=%s error=%v", final.ID, hash, time.Since(started), err)
			return nil, err
		}
		log.Debug("Fetched on-demand final chunk: snapshot=%s hash=%s bytes=%d duration=%s", final.ID, hash, len(data), time.Since(started))
		return data, nil
	}
	if err := buildIncrementalStage(ctx, root, stage, cacheDir, final, previous, fetch); err != nil {
		log.Error("Incremental stage build failed: snapshot=%s duration=%s error=%v", final.ID, time.Since(stageStarted), err)
		if preserveFinalSession(err) {
			abortSession = false
			log.Warn("Final sync session %s is retained for retry after transient transport failure: %v", final.ID, err)
		}
		return err
	}
	log.Info("Prepared incremental stage: snapshot=%s duration=%s", final.ID, time.Since(stageStarted))
	log.Info("Activating incremental stage for snapshot %s on the standby", final.ID)
	activationStarted := time.Now()
	if err := installPreparedSnapshot(ctx, stage, &final.Snapshot, cfg); err != nil {
		var warning *cleanupWarning
		if !errors.As(err, &warning) {
			log.Error("Standby activation failed: snapshot=%s duration=%s error=%v", final.ID, time.Since(activationStarted), err)
			return err
		}
		log.Warn("Standby activation completed with cleanup warning: snapshot=%s error=%v", final.ID, warning)
	}
	log.Info("Standby activation completed: snapshot=%s duration=%s; marking remote session complete", final.ID, time.Since(activationStarted))
	remoteFinishStarted := time.Now()
	if err := finishRemoteSession(ctx, client, base, cfg.ControlToken, final.ID, "complete"); err != nil {
		log.Error("Remote final sync completion failed: snapshot=%s duration=%s error=%v", final.ID, time.Since(remoteFinishStarted), err)
		if preserveFinalSession(err) {
			abortSession = false
			log.Warn("Final sync session %s is retained for retry after transient transport failure: %v", final.ID, err)
		}
		return err
	}
	log.Info("Remote final sync session completed: snapshot=%s duration=%s", final.ID, time.Since(remoteFinishStarted))
	completed = true
	if err := os.Remove(stageCheckpointPath(cfg)); err != nil && !os.IsNotExist(err) {
		log.Warn("Remove completed staging checkpoint: snapshot=%s error=%v", final.ID, err)
	}
	final.State = "ready"
	if err := signIncrementalManifest(final, cfg.ControlToken); err != nil {
		log.Error("Sign restored standby manifest failed: snapshot=%s error=%v", final.ID, err)
		return err
	}
	if err := persistStandbyManifest(cfg.SnapshotDir, final); err != nil {
		log.Error("Persist restored standby manifest failed: snapshot=%s error=%v", final.ID, err)
		return err
	}
	if err := writeManifestAt(filepath.Join(cfg.SnapshotDir, "current.json"), final); err != nil {
		log.Error("Persist current standby manifest failed: snapshot=%s error=%v", final.ID, err)
		return err
	}
	if err := pruneFailedRestoreStages(cfg.SnapshotDir, 0); err != nil {
		log.Warn("Remove failed standby restore stages after successful sync: snapshot=%s error=%v", final.ID, err)
	}
	pruneManifestFiles(cfg.SnapshotDir, cfg.SnapshotRetention, cfg.ControlToken)
	if err := os.RemoveAll(cacheDir); err != nil {
		log.Warn("Remove completed incremental cache: snapshot=%s error=%v", final.ID, err)
	}
	log.Info("Standby restore completed successfully: snapshot=%s total_duration=%s", final.ID, time.Since(syncStarted))
	return nil
}

func resumeFinalSync(ctx context.Context, cfg *config, base string, client *http.Client, previous *SnapshotManifest, cacheDir, stage string) (bool, error) {
	final := resumableFinalManifest(cfg.SnapshotDir, cfg.ControlToken)
	if final == nil {
		return false, nil
	}
	status, err := requestSnapshotStatus(ctx, client, base, cfg.ControlToken, final.ID)
	if err != nil {
		if errors.Is(err, errRemoteSnapshotUnavailable) {
			log.Info("Remote final sync checkpoint is unavailable: snapshot=%s; starting a new sync", final.ID)
			return false, nil
		}
		log.Error("Cannot check remote status for final sync checkpoint: snapshot=%s error=%v", final.ID, err)
		return true, err
	}
	if status.State != "transferring" {
		log.Info("Remote final sync checkpoint is no longer active: snapshot=%s state=%s; starting a new sync", final.ID, status.State)
		return false, nil
	}
	remote, err := requestManifestByID(ctx, client, base, cfg.ControlToken, final.ID, "transferring")
	if err != nil {
		log.Error("Cannot load remote manifest for final sync checkpoint: snapshot=%s error=%v", final.ID, err)
		return true, err
	}
	if remote.SHA256 != final.SHA256 {
		log.Error("Remote final sync manifest does not match local checkpoint: snapshot=%s local_sha256=%s remote_sha256=%s", final.ID, final.SHA256, remote.SHA256)
		return true, errors.New("active final sync manifest does not match the local recovery checkpoint")
	}
	log.Info("Resuming active final sync session %s from verified local chunk cache", final.ID)
	return true, completeFinalSync(ctx, cfg, base, client, remote, previous, cacheDir, stage)
}

func restoreIncremental(ctx context.Context, cfg *config, base string, client *http.Client) error {
	if err := os.MkdirAll(cfg.SnapshotDir, 0o700); err != nil {
		return err
	}
	pruneManifestFiles(cfg.SnapshotDir, cfg.SnapshotRetention, cfg.ControlToken)
	if err := pruneFailedRestoreStages(cfg.SnapshotDir, 1); err != nil {
		log.Error("Cannot prune older failed standby restore stages: error=%v", err)
		return err
	}
	stage := installStagePath(cfg)
	currentPath := filepath.Join(cfg.SnapshotDir, "current.json")
	previous := previousManifest(currentPath, cfg.ControlToken)
	trustedBaseline := previous != nil
	if previous == nil {
		root := filepath.Clean(setting.AppWorkPath)
		indexStarted := time.Now()
		log.Info("Indexing existing standby data for verified chunk reuse: path=%s", root)
		local, err := scanIncrementalTree(ctx, root)
		if err != nil {
			log.Warn("Cannot index existing standby data for chunk reuse: duration=%s error=%v", time.Since(indexStarted), err)
		} else {
			previous = local
			log.Info("No trusted standby baseline; indexed local data for verified chunk reuse: entries=%d bytes=%d duration=%s", local.FileCount, local.Size, time.Since(indexStarted))
		}
	}
	cacheDir := filepath.Join(cfg.SnapshotDir, ".chunks")
	if err := prepareChunkCache(cacheDir); err != nil {
		return err
	}
	if resumed, err := resumeFinalSync(ctx, cfg, base, client, previous, cacheDir, stage); resumed {
		return err
	}
	if trustedBaseline {
		log.Info("Starting standby restore from trusted local baseline %s", previous.ID)
	} else if previous != nil {
		log.Info("Starting standby restore without a trusted baseline; local content will be verified and reused")
	} else {
		log.Info("Starting standby restore without reusable local data; a full preflight scan is expected")
	}
	recoveryPreflight := resumablePreflightManifest(cfg.SnapshotDir, cfg.ControlToken)
	for finalizeAttempt := 1; finalizeAttempt <= 2; finalizeAttempt++ {
		var preflight *SnapshotManifest
		var err error
		if recoveryPreflight != nil {
			checkpoint := recoveryPreflight
			recoveryPreflight = nil
			log.Info("Requesting verified preflight recovery checkpoint %s", checkpoint.ID)
			requestStarted := time.Now()
			preflight, err = requestManifest(ctx, client, base, cfg.ControlToken, "preflight?resume="+checkpoint.ID)
			if err != nil {
				if !strings.Contains(err.Error(), "preflight recovery checkpoint is unavailable or invalid") {
					log.Error("Preflight recovery checkpoint request failed: snapshot=%s duration=%s error=%v", checkpoint.ID, time.Since(requestStarted), err)
					return err
				}
				log.Warn("Preflight recovery checkpoint %s is unavailable; request a new preflight", checkpoint.ID)
			} else if preflight.SHA256 != checkpoint.SHA256 {
				log.Error("Preflight recovery checkpoint does not match local manifest: snapshot=%s", checkpoint.ID)
				return errors.New("preflight recovery checkpoint does not match the local manifest")
			}
		}
		if preflight == nil {
			log.Info("Requesting preflight manifest from %s", redactedEndpointLabel(base))
			requestStarted := time.Now()
			preflight, err = requestManifest(ctx, client, base, cfg.ControlToken, "preflight")
			if err != nil {
				log.Error("Preflight manifest request failed: duration=%s error=%v", time.Since(requestStarted), err)
				return err
			}
		}
		if err := persistStandbyManifest(cfg.SnapshotDir, preflight); err != nil {
			return err
		}
		pruneManifestFiles(cfg.SnapshotDir, cfg.SnapshotRetention, cfg.ControlToken)
		log.Info("Received preflight manifest %s with %d entries and %s of content", preflight.ID, preflight.FileCount, strconv.FormatInt(preflight.Size, 10))
		if err := pruneChunkCache(ctx, cacheDir, preflight.ID, preflight); err != nil {
			return fmt.Errorf("prune stale replication chunk cache: %w", err)
		}
		if err := fetchMissingChunks(ctx, client, base, cfg.ControlToken, preflight, previous, cacheDir, true); err != nil {
			return err
		}
		log.Info("Requesting final sync manifest based on preflight %s", preflight.ID)
		requestStarted := time.Now()
		final, err := requestManifest(ctx, client, base, cfg.ControlToken, "final?base="+preflight.ID)
		if err != nil {
			if finalizeAttempt == 1 && strings.Contains(err.Error(), "preflight base is unavailable or invalid") {
				log.Warn("Finalize rejected preflight base %s; rerun preflight once", preflight.ID)
				continue
			}
			log.Error("Final sync manifest request failed: preflight=%s duration=%s error=%v", preflight.ID, time.Since(requestStarted), err)
			return err
		}
		if err := persistStandbyManifest(cfg.SnapshotDir, final); err != nil {
			return err
		}
		pruneManifestFiles(cfg.SnapshotDir, cfg.SnapshotRetention, cfg.ControlToken)
		log.Info("Received final sync manifest %s with %d entries and %s of content", final.ID, final.FileCount, strconv.FormatInt(final.Size, 10))
		return completeFinalSync(ctx, cfg, base, client, final, previous, cacheDir, stage)
	}
	return errors.New("final sync manifest was rejected twice")
}

func writeManifestAt(path string, manifest *SnapshotManifest) error {
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmp := file.Name()
	defer os.Remove(tmp)
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	if err := writeManifestJSON(file, manifest, false); err != nil {
		return errors.Join(err, file.Close())
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func incrementalBase(cfg *config) string {
	base := strings.TrimRight(cfg.ControlSourceURL, "/")
	if base == "" {
		base = strings.TrimRight(cfg.SourceURL, "/") + "/_replication"
	}
	return base
}
