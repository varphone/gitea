// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"code.gitea.io/gitea/modules/json"
	"code.gitea.io/gitea/modules/log"
	"code.gitea.io/gitea/modules/setting"
)

const (
	requestRetryLimit      = 5
	requestRetryBaseDelay  = 500 * time.Millisecond
	requestRetryMaxDelay   = 5 * time.Second
	chunkProgressLogStride = 128
	chunkChangeWarnBurst   = 8
	manifestPollInterval   = time.Second
	chunkChangeStopStride  = 256
	statusBodyPreviewLimit = 4 << 10
)

var syncBusyRetryDelay = time.Second

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

func shouldRetryHTTPStatus(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusBadGateway || status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout
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
		strings.Contains(message, "connection reset") ||
		strings.Contains(message, "broken pipe") ||
		strings.Contains(message, "refused") ||
		strings.Contains(message, "timeout") ||
		strings.Contains(message, "reset by peer")
}

func waitForRetry(ctx context.Context, attempt int, operation string, reason error) error {
	delay := retryDelay(attempt)
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
		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err == nil {
			return resp, nil
		}
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
		req, err := http.NewRequestWithContext(ctx, method, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := client.Do(req)
		if err != nil {
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
			_ = resp.Body.Close()
			retryErr := fmt.Errorf("%s returned %s", operation, resp.Status)
			if err := waitForRetry(ctx, attempt, operation, retryErr); err != nil {
				return nil, err
			}
			lastErr = retryErr
			continue
		}
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
		if index := strings.Index(endpoint, "resume="); index >= 0 {
			request.ResumeJobID = endpoint[index+len("resume="):]
		}
	} else {
		request.Kind = "final"
		if index := strings.Index(endpoint, "base="); index >= 0 {
			request.BaseJobID = endpoint[index+len("base="):]
		}
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	operation := "create sync job " + request.Kind
	busyWaits := 0
	for attempt := 1; ; attempt++ {
		resp, err := doRetryableJSONRequest(ctx, client, http.MethodPost, base+syncJobsPath, token, operation, payload)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusAccepted {
			var job Snapshot
			if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&job); err != nil {
				_ = resp.Body.Close()
				return nil, err
			}
			_ = resp.Body.Close()
			return pollManifestTask(ctx, client, base, token, job.ID, endpoint)
		}
		if resp.StatusCode != http.StatusOK {
			statusErr := responseStatusError(endpoint, resp)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusConflict && strings.Contains(statusErr.Error(), "sync already in progress") {
				busyWaits++
				if busyWaits == 1 || busyWaits%30 == 0 {
					log.Info("Primary replication is busy; waiting to create %s job (%d waits)", request.Kind, busyWaits)
				}
				timer := time.NewTimer(syncBusyRetryDelay)
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
	return nil, errors.New("manifest request retries exhausted")
}

func requestSnapshotStatus(ctx context.Context, client *http.Client, base, token, id string) (*Snapshot, error) {
	resp, err := doRetryableRequest(ctx, client, http.MethodGet, base+"/api/v1/replication/sync-jobs/"+id, token, "poll snapshot "+id)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("snapshot %s returned %s", id, resp.Status)
	}
	var snapshot Snapshot
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&snapshot); err != nil {
		return nil, err
	}
	return &snapshot, nil
}

func requestManifestByID(ctx context.Context, client *http.Client, base, token, id, expectedState string) (*SnapshotManifest, error) {
	operation := "fetch manifest " + id
	for attempt := 1; attempt <= requestRetryLimit; attempt++ {
		resp, err := doRetryableRequest(ctx, client, http.MethodGet, base+"/api/v1/replication/sync-jobs/"+id+"/manifest", token, operation)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("snapshot manifest %s returned %s", id, resp.Status)
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
	ticker := time.NewTicker(manifestPollInterval)
	defer ticker.Stop()
	for {
		snapshot, err := requestSnapshotStatus(ctx, client, base, token, id)
		if err != nil {
			return nil, err
		}
		switch snapshot.State {
		case snapshotStateCreating:
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-ticker.C:
			}
			continue
		case "failed":
			if snapshot.Error != "" {
				return nil, fmt.Errorf("%s task %s failed: %s", phase, id, snapshot.Error)
			}
			return nil, fmt.Errorf("%s task %s failed", phase, id)
		case expectedState:
			log.Info("%s task %s completed; downloading manifest", phase, id)
			return requestManifestByID(ctx, client, base, token, id, expectedState)
		default:
			return nil, fmt.Errorf("%s task %s entered unexpected state %q", phase, id, snapshot.State)
		}
	}
}

func requestChunk(ctx context.Context, client *http.Client, base, token, id, hash string) ([]byte, error) {
	operation := "request chunk " + hash
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
			_ = resp.Body.Close()
			return nil, fmt.Errorf("chunk %s returned %s", hash, resp.Status)
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
		return fmt.Errorf("%s sync session returned %s", action, resp.Status)
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
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.Size() > maxManifestSize {
		return nil, errors.New("incremental manifest exceeds maximum size")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var manifest SnapshotManifest
	if err := json.NewDecoder(io.LimitReader(file, maxManifestSize)).Decode(&manifest); err != nil {
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

func readCachedChunk(cacheDir, hash string) ([]byte, error) {
	path := cachePath(cacheDir, hash)
	if err := verifyFile(path, hash); err != nil {
		return nil, err
	}
	return os.ReadFile(path)
}

func fetchMissingChunks(ctx context.Context, client *http.Client, base, token string, manifest, previous *SnapshotManifest, cacheDir string, tolerateChanges bool) error {
	available := map[string]struct{}{}
	if previous != nil {
		for hash := range indexManifest(previous) {
			available[hash] = struct{}{}
		}
	}
	total := len(indexManifest(manifest))
	fetched := 0
	deferred := 0
	skipped := 0
	processed := 0
	for hash := range indexManifest(manifest) {
		if _, ok := available[hash]; ok || cacheHas(cacheDir, hash) {
			skipped++
			processed++
			continue
		}
		data, err := requestChunk(ctx, client, base, token, manifest.ID, hash)
		if err != nil && tolerateChanges {
			var changed *chunkChangedError
			if errors.As(err, &changed) {
				deferred++
				processed++
				if deferred <= chunkChangeWarnBurst {
					log.Warn("Preflight chunk %s changed; defer it to final sync", hash)
				} else if deferred%chunkProgressLogStride == 0 {
					log.Warn("Preflight progress for snapshot %s remains unstable: processed=%d/%d fetched=%d cached=%d deferred=%d", manifest.ID, processed, total, fetched, skipped, deferred)
				}
				if fetched == 0 && deferred >= chunkChangeStopStride {
					log.Warn("Preflight prefetch for snapshot %s produced no stable chunks after %d deferrals; stop prefetch and continue to final sync", manifest.ID, deferred)
					break
				}
				continue
			}
		}
		if err != nil {
			return err
		}
		if err := storeChunk(cacheDir, hash, data); err != nil {
			return err
		}
		fetched++
		processed++
		if fetched == 1 || fetched%chunkProgressLogStride == 0 {
			phase := "final"
			if tolerateChanges {
				phase = "preflight"
			}
			log.Info("Fetched %d/%d %s chunks for snapshot %s (cached=%d deferred=%d)", fetched, total, phase, manifest.ID, skipped, deferred)
		}
	}
	phase := "final"
	if tolerateChanges {
		phase = "preflight"
	}
	if tolerateChanges && fetched == 0 && deferred > 0 {
		log.Warn("Finished %s chunk pass for snapshot %s without caching any chunks; source changed before every fetch (cached=%d deferred=%d total=%d)", phase, manifest.ID, skipped, deferred, total)
	}
	log.Info("Finished %s chunk pass for snapshot %s: fetched=%d cached=%d deferred=%d total=%d", phase, manifest.ID, fetched, skipped, deferred, total)
	return nil
}

func sameFile(a, b TreeEntry) bool {
	return a.Type == "file" && b.Type == "file" && a.Size == b.Size && a.Mode == b.Mode &&
		a.ModTimeNS == b.ModTimeNS && reflect.DeepEqual(a.Chunks, b.Chunks)
}

func reusableWholeFile(path string, entry TreeEntry) bool {
	info, err := os.Lstat(path)
	return err == nil && info.Mode().IsRegular() && info.Size() == entry.Size &&
		uint32(info.Mode().Perm()) == entry.Mode && info.ModTime().UnixNano() == entry.ModTimeNS
}

func buildIncrementalStage(ctx context.Context, root, stage, cacheDir string, manifest, previous *SnapshotManifest, fetch func(string) ([]byte, error)) error {
	oldEntries := map[string]TreeEntry{}
	oldChunks := map[string]chunkLocation{}
	if previous != nil {
		for _, entry := range previous.Files {
			oldEntries[entry.Path] = entry
		}
		oldChunks = indexManifest(previous)
	}
	var directories []TreeEntry
	for _, entry := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		dst := filepath.Join(stage, filepath.FromSlash(entry.Path))
		switch entry.Type {
		case "dir":
			// Keep directories writable while their children are reconstructed;
			// the manifest permissions are restored after the complete tree exists.
			if err := os.MkdirAll(dst, 0o700); err != nil {
				return err
			}
			directories = append(directories, entry)
		case "symlink":
			if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
				return err
			}
			if target, err := os.Readlink(dst); err == nil && target == entry.LinkTarget {
				continue
			}
			if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
				return err
			}
			if err := os.Symlink(entry.LinkTarget, dst); err != nil {
				return err
			}
		case "file":
			if reusableWholeFile(dst, entry) {
				continue
			}
			old := oldEntries[entry.Path]
			source := filepath.Join(root, filepath.FromSlash(entry.Path))
			if sameFile(entry, old) && reusableWholeFile(source, entry) {
				if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
					return err
				}
				if err := os.Link(source, dst); err == nil {
					continue
				}
			}
			if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
				return err
			}
			tmp := dst + ".replication-tmp"
			_ = os.Remove(tmp)
			out, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(entry.Mode))
			if err != nil {
				return err
			}
			for _, chunk := range entry.Chunks {
				var data []byte
				if cached, err := readCachedChunk(cacheDir, chunk.Hash); err == nil {
					data = cached
				} else if location, ok := oldChunks[chunk.Hash]; ok {
					data, err = readChunk(root, location, chunk.Hash)
					if err != nil {
						data = nil
					}
				}
				if data == nil {
					data, err = fetch(chunk.Hash)
					if err == nil {
						err = storeChunk(cacheDir, chunk.Hash, data)
					}
					if err != nil {
						_ = out.Close()
						return err
					}
				}
				if int64(len(data)) != chunk.Size {
					_ = out.Close()
					return fmt.Errorf("chunk %s size mismatch", chunk.Hash)
				}
				if _, err := out.Write(data); err != nil {
					_ = out.Close()
					return err
				}
			}
			if err := out.Close(); err != nil {
				return err
			}
			mtime := time.Unix(0, entry.ModTimeNS)
			if err := os.Chtimes(tmp, mtime, mtime); err != nil {
				return err
			}
			if err := os.Rename(tmp, dst); err != nil {
				return err
			}
		}
	}
	for i := len(directories) - 1; i >= 0; i-- {
		entry := directories[i]
		dst := filepath.Join(stage, filepath.FromSlash(entry.Path))
		if err := os.Chmod(dst, os.FileMode(entry.Mode)); err != nil {
			return err
		}
		mtime := time.Unix(0, entry.ModTimeNS)
		if err := os.Chtimes(dst, mtime, mtime); err != nil {
			return err
		}
	}
	return os.Chmod(stage, os.FileMode(manifest.RootMode))
}

func persistStandbyManifest(snapshotDir string, manifest *SnapshotManifest) error {
	return writeManifestAt(manifestPath(snapshotDir, manifest.ID), manifest)
}

type stageCheckpoint struct {
	SnapshotID  string `json:"snapshot_id"`
	ManifestSHA string `json:"manifest_sha"`
}

func stageCheckpointPath(cfg *config) string {
	return filepath.Join(cfg.SnapshotDir, ".install-stage.checkpoint")
}

func prepareIncrementalStage(cfg *config, final *SnapshotManifest, stage string) (bool, error) {
	checkpointPath := stageCheckpointPath(cfg)
	if data, err := os.ReadFile(checkpointPath); err == nil {
		var checkpoint stageCheckpoint
		if json.Unmarshal(data, &checkpoint) == nil && checkpoint.SnapshotID == final.ID && checkpoint.ManifestSHA == final.SHA256 {
			if info, err := os.Stat(stage); err == nil && info.IsDir() {
				return true, nil
			}
		}
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
	paths, err := filepath.Glob(filepath.Join(snapshotDir, "*.json"))
	if err != nil {
		return nil
	}
	var latest *SnapshotManifest
	for _, path := range paths {
		manifest, err := loadTrustedManifest(path, token, "preflight")
		if err != nil || (latest != nil && !manifest.CreatedAt.After(latest.CreatedAt)) {
			continue
		}
		latest = manifest
	}
	return latest
}

func removePreflightCheckpoints(snapshotDir, token string) {
	paths, err := filepath.Glob(filepath.Join(snapshotDir, "*.json"))
	if err != nil {
		return
	}
	for _, path := range paths {
		if _, err := loadTrustedManifest(path, token, "preflight"); err == nil {
			if err := os.Remove(path); err != nil {
				log.Warn("Remove completed preflight checkpoint %s: %v", path, err)
			}
		}
	}
}

func resumableFinalManifest(snapshotDir, token string) *SnapshotManifest {
	paths, err := filepath.Glob(filepath.Join(snapshotDir, "*.json"))
	if err != nil {
		return nil
	}
	var latest *SnapshotManifest
	for _, path := range paths {
		manifest, err := loadTrustedManifest(path, token, "transferring")
		if err != nil || (latest != nil && !manifest.CreatedAt.After(latest.CreatedAt)) {
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
	completed := false
	abortSession := true
	defer func() {
		if !completed && abortSession {
			abortCtx, cancel := context.WithTimeout(context.Background(), cfg.ServiceTimeout)
			log.Warn("Final sync session %s did not complete; aborting remote session", final.ID)
			_ = finishRemoteSession(abortCtx, client, base, cfg.ControlToken, final.ID, "abort")
			cancel()
		}
	}()
	if err := fetchMissingChunks(ctx, client, base, cfg.ControlToken, final, previous, cacheDir, false); err != nil {
		if preserveFinalSession(err) {
			abortSession = false
			log.Warn("Final sync session %s is retained for retry after transient transport failure: %v", final.ID, err)
		}
		return err
	}
	resumedStage, err := prepareIncrementalStage(cfg, final, stage)
	if err != nil {
		return err
	}
	if resumedStage {
		log.Info("Resuming prepared staging tree for snapshot %s", final.ID)
	}
	root := filepath.Clean(setting.AppWorkPath)
	fetch := func(hash string) ([]byte, error) {
		return requestChunk(ctx, client, base, cfg.ControlToken, final.ID, hash)
	}
	if err := buildIncrementalStage(ctx, root, stage, cacheDir, final, previous, fetch); err != nil {
		if preserveFinalSession(err) {
			abortSession = false
			log.Warn("Final sync session %s is retained for retry after transient transport failure: %v", final.ID, err)
		}
		return err
	}
	log.Info("Prepared incremental stage for snapshot %s", final.ID)
	if err := syncTree(ctx, stage); err != nil {
		return fmt.Errorf("persist incremental stage: %w", err)
	}
	log.Info("Persisted incremental stage for snapshot %s; atomically activating it on the standby", final.ID)
	if err := installPreparedSnapshot(ctx, stage, &final.Snapshot, cfg); err != nil {
		var warning *cleanupWarning
		if !errors.As(err, &warning) {
			return err
		}
		log.Warn("%v", warning)
	}
	log.Info("Standby activation completed for snapshot %s; marking remote session complete", final.ID)
	if err := finishRemoteSession(ctx, client, base, cfg.ControlToken, final.ID, "complete"); err != nil {
		if preserveFinalSession(err) {
			abortSession = false
			log.Warn("Final sync session %s is retained for retry after transient transport failure: %v", final.ID, err)
		}
		return err
	}
	completed = true
	if err := os.Remove(stageCheckpointPath(cfg)); err != nil && !os.IsNotExist(err) {
		log.Warn("Remove completed staging checkpoint: %v", err)
	}
	final.State = "ready"
	if err := signIncrementalManifest(final, cfg.ControlToken); err != nil {
		return err
	}
	if err := persistStandbyManifest(cfg.SnapshotDir, final); err != nil {
		return err
	}
	if err := writeManifestAt(filepath.Join(cfg.SnapshotDir, "current.json"), final); err != nil {
		return err
	}
	pruneManifestFiles(cfg.SnapshotDir, cfg.SnapshotRetention)
	if err := os.RemoveAll(cacheDir); err != nil {
		log.Warn("Remove incremental cache: %v", err)
	}
	log.Info("Standby restore completed successfully with snapshot %s", final.ID)
	return nil
}

func resumeFinalSync(ctx context.Context, cfg *config, base string, client *http.Client, previous *SnapshotManifest, cacheDir, stage string) (bool, error) {
	final := resumableFinalManifest(cfg.SnapshotDir, cfg.ControlToken)
	if final == nil {
		return false, nil
	}
	status, err := requestSnapshotStatus(ctx, client, base, cfg.ControlToken, final.ID)
	if err != nil {
		return true, err
	}
	if status.State != "transferring" {
		return false, nil
	}
	remote, err := requestManifestByID(ctx, client, base, cfg.ControlToken, final.ID, "transferring")
	if err != nil {
		return true, err
	}
	if remote.SHA256 != final.SHA256 {
		return true, errors.New("active final sync manifest does not match the local recovery checkpoint")
	}
	log.Info("Resuming active final sync session %s from verified local chunk cache", final.ID)
	return true, completeFinalSync(ctx, cfg, base, client, remote, previous, cacheDir, stage)
}

func restoreIncremental(ctx context.Context, cfg *config, base string, client *http.Client) error {
	if err := os.MkdirAll(cfg.SnapshotDir, 0o700); err != nil {
		return err
	}
	pruneManifestFiles(cfg.SnapshotDir, cfg.SnapshotRetention)
	stage := installStagePath(cfg)
	currentPath := filepath.Join(cfg.SnapshotDir, "current.json")
	previous := previousManifest(currentPath, cfg.ControlToken)
	trustedBaseline := previous != nil
	if previous == nil {
		local, err := scanIncrementalTree(ctx, filepath.Clean(setting.AppWorkPath))
		if err != nil {
			log.Warn("Cannot index existing standby data for chunk reuse: %v", err)
		} else {
			previous = local
			log.Info("No trusted standby baseline; indexed %d local files for verified chunk reuse", local.FileCount)
		}
	}
	cacheDir := filepath.Join(cfg.SnapshotDir, ".chunks")
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
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
			preflight, err = requestManifest(ctx, client, base, cfg.ControlToken, "preflight?resume="+checkpoint.ID)
			if err != nil {
				if !strings.Contains(err.Error(), "preflight recovery checkpoint is unavailable or invalid") {
					return err
				}
				log.Warn("Preflight recovery checkpoint %s is unavailable; request a new preflight", checkpoint.ID)
			} else if preflight.SHA256 != checkpoint.SHA256 {
				return errors.New("preflight recovery checkpoint does not match the local manifest")
			}
		}
		if preflight == nil {
			log.Info("Requesting preflight manifest from %s", base)
			preflight, err = requestManifest(ctx, client, base, cfg.ControlToken, "preflight")
			if err != nil {
				return err
			}
		}
		if err := persistStandbyManifest(cfg.SnapshotDir, preflight); err != nil {
			return err
		}
		pruneManifestFiles(cfg.SnapshotDir, cfg.SnapshotRetention)
		log.Info("Received preflight manifest %s with %d files and %s of content", preflight.ID, preflight.FileCount, strconv.FormatInt(preflight.Size, 10))
		if err := fetchMissingChunks(ctx, client, base, cfg.ControlToken, preflight, previous, cacheDir, true); err != nil {
			return err
		}
		log.Info("Requesting final sync manifest based on preflight %s", preflight.ID)
		final, err := requestManifest(ctx, client, base, cfg.ControlToken, "final?base="+preflight.ID)
		if err != nil {
			if finalizeAttempt == 1 && strings.Contains(err.Error(), "preflight base is unavailable or invalid") {
				log.Warn("Finalize rejected preflight base %s; rerun preflight once", preflight.ID)
				continue
			}
			return err
		}
		if err := persistStandbyManifest(cfg.SnapshotDir, final); err != nil {
			return err
		}
		pruneManifestFiles(cfg.SnapshotDir, cfg.SnapshotRetention)
		log.Info("Received final sync manifest %s with %d files and %s of content", final.ID, final.FileCount, strconv.FormatInt(final.Size, 10))
		return completeFinalSync(ctx, cfg, base, client, final, previous, cacheDir, stage)
	}
	return errors.New("final sync manifest was rejected twice")
}

func writeManifestAt(path string, manifest *SnapshotManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	if len(data) > maxManifestSize {
		return errors.New("incremental manifest exceeds maximum size")
	}
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
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
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
