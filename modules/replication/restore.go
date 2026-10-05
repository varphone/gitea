// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gitea.dev/modules/json"
	"gitea.dev/modules/log"
	"gitea.dev/modules/setting"
)

type SnapshotManifest struct {
	Snapshot
	FormatVersion                 int         `json:"format_version"`
	GiteaVersion                  string      `json:"gitea_version"`
	AppWorkPath                   string      `json:"app_work_path"`
	InstanceFingerprint           string      `json:"instance_fingerprint"`
	OAuth2SigningConfig           string      `json:"oauth2_signing_config,omitempty"`
	GeneralTokenSecretFingerprint string      `json:"general_token_secret_fingerprint,omitempty"`
	Signature                     string      `json:"signature,omitempty"`
	FullScanAt                    time.Time   `json:"full_scan_at,omitzero"`
	FileCount                     int         `json:"file_count,omitempty"`
	Files                         []TreeEntry `json:"files,omitempty"`
}

const (
	finalizeRequestCheckpointSuffix  = ".finalize"
	preflightRequestCheckpointSuffix = ".preflight"
)

const (
	standbyFinalizeRequestCheckpointName  = "pending-finalize.json"
	standbyPreflightRequestCheckpointName = "pending-preflight.json"
)

type standbyPreflightRequestCheckpoint struct {
	RequestID string `json:"request_id"`
	Signature string `json:"signature"`
}

type standbyPreflightRequestCheckpointPayload struct {
	RequestID string `json:"request_id"`
}

type standbyFinalizeRequestCheckpoint struct {
	BaseJobID string `json:"base_job_id"`
	RequestID string `json:"request_id"`
	Signature string `json:"signature"`
}

type standbyFinalizeRequestCheckpointPayload struct {
	BaseJobID string `json:"base_job_id"`
	RequestID string `json:"request_id"`
}

func standbyFinalizeRequestCheckpointPath(snapshotDir string) string {
	return filepath.Join(snapshotDir, standbyFinalizeRequestCheckpointName)
}

func signStandbyFinalizeRequestCheckpoint(checkpoint *standbyFinalizeRequestCheckpoint, token string) error {
	if !validSnapshotID(checkpoint.BaseJobID) || !validReplicationRequestID(checkpoint.RequestID) {
		return errors.New("invalid standby finalize request checkpoint metadata")
	}
	payload, err := json.Marshal(standbyFinalizeRequestCheckpointPayload{
		BaseJobID: checkpoint.BaseJobID,
		RequestID: checkpoint.RequestID,
	})
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = mac.Write(payload)
	checkpoint.Signature = hex.EncodeToString(mac.Sum(nil))
	return nil
}

func loadStandbyFinalizeRequestCheckpoint(snapshotDir, baseID, token string) (standbyFinalizeRequestCheckpoint, bool, error) {
	path := standbyFinalizeRequestCheckpointPath(snapshotDir)
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return standbyFinalizeRequestCheckpoint{}, false, nil
		}
		return standbyFinalizeRequestCheckpoint{}, false, err
	}
	if !info.Mode().IsRegular() || info.Size() > 4096 {
		return standbyFinalizeRequestCheckpoint{}, false, errors.New("invalid standby finalize request checkpoint file")
	}
	file, _, err := openRegularFile(path, info)
	if err != nil {
		return standbyFinalizeRequestCheckpoint{}, false, fmt.Errorf("open standby finalize request checkpoint: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil {
		return standbyFinalizeRequestCheckpoint{}, false, err
	}
	if len(data) > 4096 {
		return standbyFinalizeRequestCheckpoint{}, false, errors.New("standby finalize request checkpoint exceeds maximum size")
	}
	var checkpoint standbyFinalizeRequestCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return standbyFinalizeRequestCheckpoint{}, false, err
	}
	signature := checkpoint.Signature
	if err := signStandbyFinalizeRequestCheckpoint(&checkpoint, token); err != nil {
		return standbyFinalizeRequestCheckpoint{}, false, err
	}
	if !hmac.Equal([]byte(signature), []byte(checkpoint.Signature)) {
		return standbyFinalizeRequestCheckpoint{}, false, errors.New("invalid standby finalize request checkpoint signature")
	}
	checkpoint.Signature = signature
	if checkpoint.BaseJobID != baseID {
		return standbyFinalizeRequestCheckpoint{}, false, nil
	}
	return checkpoint, true, nil
}

func newStandbyFinalizeRequestCheckpoint(baseID, token string) (*standbyFinalizeRequestCheckpoint, error) {
	var requestID [16]byte
	if _, err := rand.Read(requestID[:]); err != nil {
		return nil, fmt.Errorf("generate sync job request ID: %w", err)
	}
	checkpoint := &standbyFinalizeRequestCheckpoint{BaseJobID: baseID, RequestID: hex.EncodeToString(requestID[:])}
	if err := signStandbyFinalizeRequestCheckpoint(checkpoint, token); err != nil {
		return nil, err
	}
	return checkpoint, nil
}

func persistStandbyFinalizeRequestCheckpoint(snapshotDir string, checkpoint *standbyFinalizeRequestCheckpoint, token string) error {
	if err := signStandbyFinalizeRequestCheckpoint(checkpoint, token); err != nil {
		return err
	}
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return writeFileSynced(standbyFinalizeRequestCheckpointPath(snapshotDir), data)
}

func removeStandbyFinalizeRequestCheckpoint(snapshotDir string) error {
	return removeFileSynced(standbyFinalizeRequestCheckpointPath(snapshotDir))
}

func standbyPreflightRequestCheckpointPath(snapshotDir string) string {
	return filepath.Join(snapshotDir, standbyPreflightRequestCheckpointName)
}

func signStandbyPreflightRequestCheckpoint(checkpoint *standbyPreflightRequestCheckpoint, token string) error {
	if !validReplicationRequestID(checkpoint.RequestID) {
		return errors.New("invalid standby preflight request checkpoint metadata")
	}
	payload, err := json.Marshal(standbyPreflightRequestCheckpointPayload{RequestID: checkpoint.RequestID})
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = mac.Write(payload)
	checkpoint.Signature = hex.EncodeToString(mac.Sum(nil))
	return nil
}

func newStandbyPreflightRequestCheckpoint(token string) (*standbyPreflightRequestCheckpoint, error) {
	requestID, err := newReplicationRequestID()
	if err != nil {
		return nil, err
	}
	checkpoint := &standbyPreflightRequestCheckpoint{RequestID: requestID}
	if err := signStandbyPreflightRequestCheckpoint(checkpoint, token); err != nil {
		return nil, err
	}
	return checkpoint, nil
}

func loadStandbyPreflightRequestCheckpoint(snapshotDir, token string) (*standbyPreflightRequestCheckpoint, bool, error) {
	path := standbyPreflightRequestCheckpointPath(snapshotDir)
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if !info.Mode().IsRegular() || info.Size() > 4096 {
		return nil, false, errors.New("invalid standby preflight request checkpoint file")
	}
	file, openedInfo, err := openRegularFile(path, info)
	if err != nil {
		return nil, false, fmt.Errorf("open standby preflight request checkpoint: %w", err)
	}
	defer file.Close()
	if openedInfo.Size() > 4096 {
		return nil, false, errors.New("invalid standby preflight request checkpoint file")
	}
	data, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil {
		return nil, false, err
	}
	if len(data) > 4096 {
		return nil, false, errors.New("standby preflight request checkpoint exceeds maximum size")
	}
	var checkpoint standbyPreflightRequestCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, false, err
	}
	signature := checkpoint.Signature
	if err := signStandbyPreflightRequestCheckpoint(&checkpoint, token); err != nil {
		return nil, false, err
	}
	if !hmac.Equal([]byte(signature), []byte(checkpoint.Signature)) {
		return nil, false, errors.New("invalid standby preflight request checkpoint signature")
	}
	checkpoint.Signature = signature
	return &checkpoint, true, nil
}

func persistStandbyPreflightRequestCheckpoint(snapshotDir string, checkpoint *standbyPreflightRequestCheckpoint, token string) error {
	if err := signStandbyPreflightRequestCheckpoint(checkpoint, token); err != nil {
		return err
	}
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return writeFileSynced(standbyPreflightRequestCheckpointPath(snapshotDir), data)
}

func removeStandbyPreflightRequestCheckpoint(snapshotDir string) error {
	return removeFileSynced(standbyPreflightRequestCheckpointPath(snapshotDir))
}

type syncJobRequestCheckpoint struct {
	SnapshotID string `json:"snapshot_id"`
	BaseJobID  string `json:"base_job_id"`
	RequestID  string `json:"request_id"`
	Kind       string `json:"kind,omitempty"`
	Signature  string `json:"signature"`
}

type syncJobRequestCheckpointPayload struct {
	SnapshotID string `json:"snapshot_id"`
	BaseJobID  string `json:"base_job_id"`
	RequestID  string `json:"request_id"`
	Kind       string `json:"kind,omitempty"`
}

func finalizeRequestCheckpointPath(dir, id string) string {
	return filepath.Join(dir, "."+id+finalizeRequestCheckpointSuffix)
}

func preflightRequestCheckpointPath(dir, id string) string {
	return filepath.Join(dir, "."+id+preflightRequestCheckpointSuffix)
}

func signSyncJobRequestCheckpoint(checkpoint *syncJobRequestCheckpoint, token string) error {
	payload, err := json.Marshal(syncJobRequestCheckpointPayload{
		SnapshotID: checkpoint.SnapshotID,
		BaseJobID:  checkpoint.BaseJobID,
		RequestID:  checkpoint.RequestID,
		Kind:       checkpoint.Kind,
	})
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = mac.Write(payload)
	checkpoint.Signature = hex.EncodeToString(mac.Sum(nil))
	return nil
}

func writeFinalizeRequestCheckpoint(dir, id, baseID, requestID, token string) error {
	if !validSnapshotID(id) || !validSnapshotID(baseID) || !validReplicationRequestID(requestID) {
		return errors.New("invalid finalize request checkpoint metadata")
	}
	checkpoint := syncJobRequestCheckpoint{SnapshotID: id, BaseJobID: baseID, RequestID: requestID}
	if err := signSyncJobRequestCheckpoint(&checkpoint, token); err != nil {
		return err
	}
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return writeFileSynced(finalizeRequestCheckpointPath(dir, id), data)
}

func writePreflightRequestCheckpoint(dir, id, requestID, token string) error {
	if !validSnapshotID(id) || !validReplicationRequestID(requestID) {
		return errors.New("invalid preflight request checkpoint metadata")
	}
	checkpoint := syncJobRequestCheckpoint{SnapshotID: id, RequestID: requestID, Kind: "preflight"}
	if err := signSyncJobRequestCheckpoint(&checkpoint, token); err != nil {
		return err
	}
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return writeFileSynced(preflightRequestCheckpointPath(dir, id), data)
}

func loadSyncJobRequestCheckpoint(path, id, token string) (*syncJobRequestCheckpoint, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > 4096 {
		return nil, errors.New("invalid sync job request checkpoint file")
	}
	file, openedInfo, err := openRegularFile(path, info)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if openedInfo.Size() > 4096 {
		return nil, errors.New("invalid sync job request checkpoint file")
	}
	data, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil {
		return nil, err
	}
	if len(data) > 4096 {
		return nil, errors.New("sync job request checkpoint exceeds maximum size")
	}
	var checkpoint syncJobRequestCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, err
	}
	if checkpoint.SnapshotID != id || !validSnapshotID(checkpoint.SnapshotID) || !validReplicationRequestID(checkpoint.RequestID) {
		return nil, errors.New("invalid sync job request checkpoint metadata")
	}
	switch checkpoint.Kind {
	case "":
		if !validSnapshotID(checkpoint.BaseJobID) {
			return nil, errors.New("invalid finalize request checkpoint metadata")
		}
	case "preflight":
		if checkpoint.BaseJobID != "" {
			return nil, errors.New("invalid preflight request checkpoint metadata")
		}
	default:
		return nil, errors.New("invalid sync job request checkpoint kind")
	}
	signature := checkpoint.Signature
	if err := signSyncJobRequestCheckpoint(&checkpoint, token); err != nil {
		return nil, err
	}
	if !hmac.Equal([]byte(signature), []byte(checkpoint.Signature)) {
		return nil, errors.New("invalid finalize request checkpoint signature")
	}
	checkpoint.Signature = signature
	return &checkpoint, nil
}

func loadFinalizeRequestCheckpoint(path, id, token string) (*syncJobRequestCheckpoint, error) {
	checkpoint, err := loadSyncJobRequestCheckpoint(path, id, token)
	if err != nil {
		return nil, err
	}
	if checkpoint.Kind == "preflight" {
		return nil, errors.New("invalid finalize request checkpoint kind")
	}
	return checkpoint, nil
}

func loadPreflightRequestCheckpoint(path, id, token string) (*syncJobRequestCheckpoint, error) {
	checkpoint, err := loadSyncJobRequestCheckpoint(path, id, token)
	if err != nil {
		return nil, err
	}
	if checkpoint.Kind != "preflight" {
		return nil, errors.New("invalid preflight request checkpoint kind")
	}
	return checkpoint, nil
}

func instanceFingerprint(token string) string {
	mac := hmac.New(sha256.New, []byte(token))
	values := [][]byte{[]byte(setting.SecretKey), []byte(setting.InternalToken), setting.LFS.JWTSecretBytes}
	for _, value := range values {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = mac.Write(length[:])
		_, _ = mac.Write(value)
	}
	return hex.EncodeToString(mac.Sum(nil))
}

func generalTokenSecretFingerprint(token string) string {
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = io.WriteString(mac, "gitea-replication/general-token-secret/v1\x00")
	_, _ = mac.Write(setting.GetGeneralTokenSigningSecret())
	return hex.EncodeToString(mac.Sum(nil))
}

func syncDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	return syncDirectoryInfo(path, info)
}

func syncDirectoryInfo(path string, expected os.FileInfo) error {
	dir, err := openDirectory(path, expected)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

const snapshotIDLayout = "20060102T150405.000000000Z"

func validSnapshotID(id string) bool { _, err := time.Parse(snapshotIDLayout, id); return err == nil }

func manifestPath(dir, id string) string { return filepath.Join(dir, id+".json") }

func listManifestPaths(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			paths = append(paths, filepath.Join(dir, entry.Name()))
		}
	}
	return paths, nil
}

func writeManifest(dir string, manifest *SnapshotManifest) error {
	return writeManifestAt(manifestPath(dir, manifest.ID), manifest)
}

func loadManifests(dir, token string) (map[string]*Snapshot, string, error) {
	paths, err := listManifestPaths(dir)
	if err != nil {
		return nil, "", err
	}
	result := make(map[string]*Snapshot, len(paths))
	transferCheckpointID := ""
	for _, path := range paths {
		fileID := strings.TrimSuffix(filepath.Base(path), ".json")
		if !validSnapshotID(fileID) {
			continue
		}
		file, err := openManifestFile(path)
		if err != nil {
			log.Warn("Skip oversized or unreadable snapshot manifest %s: %v", path, err)
			continue
		}
		var manifest SnapshotManifest
		decodeErr := decodeManifestJSON(file, &manifest)
		if decodeErr != nil {
			if _, err := file.Seek(0, io.SeekStart); err == nil {
				var transferHint struct {
					ID    string `json:"id"`
					State string `json:"state"`
				}
				if decodeManifestJSON(file, &transferHint) == nil && transferHint.State == "transferring" &&
					transferHint.ID == fileID && fileID > transferCheckpointID {
					transferCheckpointID = fileID
				}
			}
			_ = file.Close()
			log.Warn("Skip invalid snapshot manifest %s: %v", path, decodeErr)
			continue
		}
		_ = file.Close()
		if manifest.State == "transferring" && manifest.ID == fileID && fileID > transferCheckpointID {
			transferCheckpointID = fileID
		}
		if !supportedIncrementalFormatVersion(manifest.FormatVersion) || manifest.ID != fileID {
			log.Warn("Skip snapshot manifest with unsupported format or mismatched ID: path=%s manifest_id=%s file_id=%s format_version=%d", path, manifest.ID, fileID, manifest.FormatVersion)
			continue
		}
		if err := validateIncrementalManifest(&manifest); err != nil {
			log.Warn("Skip invalid incremental manifest %s: %v", path, err)
			continue
		}
		if !verifyIncrementalSignature(&manifest, token) {
			log.Warn("Skip increment manifest with invalid signature %s", path)
			continue
		}
		snapshotCopy := manifest.Snapshot
		if snapshotCopy.State == "ready" && (snapshotCopy.RootMode == 0 || snapshotCopy.RootMode > 0o777) {
			snapshotCopy.State = "failed"
			snapshotCopy.Error = "snapshot root mode is invalid"
		}
		result[snapshotCopy.ID] = &snapshotCopy
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, "", err
	}
	seenRequestIDs := make(map[string]struct{})
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		name := strings.TrimPrefix(entry.Name(), ".")
		kind, suffix := "final", finalizeRequestCheckpointSuffix
		if strings.HasSuffix(name, preflightRequestCheckpointSuffix) {
			kind, suffix = "preflight", preflightRequestCheckpointSuffix
		} else if !strings.HasSuffix(name, finalizeRequestCheckpointSuffix) {
			continue
		}
		id := strings.TrimSuffix(name, suffix)
		if !validSnapshotID(id) {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		var checkpoint *syncJobRequestCheckpoint
		var err error
		if kind == "preflight" {
			checkpoint, err = loadPreflightRequestCheckpoint(path, id, token)
		} else {
			checkpoint, err = loadFinalizeRequestCheckpoint(path, id, token)
		}
		if err != nil {
			log.Warn("Skip invalid %s request checkpoint %s: %v", kind, path, err)
			continue
		}
		if _, exists := seenRequestIDs[checkpoint.RequestID]; exists {
			log.Warn("Skip duplicate %s request checkpoint: snapshot=%s request_id=%s", kind, id, checkpoint.RequestID)
			continue
		}
		job := result[id]
		if job == nil {
			createdAt, _ := time.Parse(snapshotIDLayout, id)
			message := "finalize result was not persisted before replication control service restart"
			if kind == "preflight" {
				message = "preflight result was not persisted before replication control service restart"
			}
			job = &Snapshot{
				ID:        id,
				State:     "failed",
				CreatedAt: createdAt,
				Error:     message,
			}
			result[id] = job
		} else if kind == "final" && job.State != "transferring" && job.State != "ready" && job.State != "failed" {
			log.Warn("Skip finalize request checkpoint %s for unexpected snapshot state %s", path, job.State)
			continue
		} else if kind == "preflight" && job.State != "preflight" && job.State != "failed" {
			log.Warn("Skip preflight request checkpoint %s for unexpected snapshot state %s", path, job.State)
			continue
		}
		job.RequestID = checkpoint.RequestID
		job.BaseJobID = checkpoint.BaseJobID
		seenRequestIDs[checkpoint.RequestID] = struct{}{}
	}
	return result, transferCheckpointID, nil
}

func localControlBase(listen string) (string, error) {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", err
	}
	if host == "" || net.ParseIP(host).IsUnspecified() {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port), nil
}

func redactedEndpointLabel(raw string) string {
	endpoint, err := url.Parse(raw)
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" {
		return "configured control endpoint"
	}
	return endpoint.Scheme + "://" + endpoint.Host
}

func EnsurePrimaryService(ctx context.Context) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	// A persisted outage checkpoint means the node was fenced mid-sync. Recover the
	// fixed service regardless of the current MODE: leaving it stopped while a
	// checkpoint exists would keep every write blocked with no automatic way out.
	checkpointPending := primaryOutageCheckpointExists(cfg.SnapshotDir)
	if cfg.Mode != modePrimary && !checkpointPending {
		return nil
	}
	if cfg.Mode != modePrimary {
		log.Warn("Replication is not configured as primary but a primary outage checkpoint remains; starting the fixed Gitea service for recovery")
		cfg.GiteaServiceName = defaultConfig().GiteaServiceName
		cfg.ServiceTimeout = defaultConfig().ServiceTimeout
	} else if !cfg.Enabled {
		if !checkpointPending {
			return nil
		}
		log.Warn("Replication is disabled but a primary outage checkpoint remains; starting the fixed Gitea service for recovery")
		cfg.GiteaServiceName = defaultConfig().GiteaServiceName
		cfg.ServiceTimeout = defaultConfig().ServiceTimeout
	}
	started := time.Now()
	log.Info("Ensuring primary Gitea service is running: service=%s", cfg.GiteaServiceName)
	taskCtx, cancel := context.WithTimeout(ctx, cfg.ServiceTimeout)
	defer cancel()
	if err := systemctl(taskCtx, "start", cfg.GiteaServiceName); err != nil {
		wrappedErr := fmt.Errorf("start primary service %s: %w", cfg.GiteaServiceName, err)
		log.Error("Ensure primary Gitea service failed: service=%s duration=%s error=%v", cfg.GiteaServiceName, time.Since(started), wrappedErr)
		return wrappedErr
	}
	readinessStarted := time.Now()
	if err := readinessCheck(taskCtx, cfg.GiteaServiceName); err != nil {
		wrappedErr := fmt.Errorf("wait for primary service %s readiness: %w", cfg.GiteaServiceName, err)
		log.Error("Ensure primary Gitea readiness failed: service=%s duration=%s total_duration=%s error=%v", cfg.GiteaServiceName, time.Since(readinessStarted), time.Since(started), wrappedErr)
		return wrappedErr
	}
	readinessDuration := time.Since(readinessStarted)
	checkpointStarted := time.Now()
	if err := clearPrimaryOutageCheckpoint(cfg.SnapshotDir); err != nil {
		wrappedErr := fmt.Errorf("clear primary outage recovery checkpoint: %w", err)
		log.Error("Ensure primary Gitea checkpoint cleanup failed: service=%s duration=%s total_duration=%s error=%v", cfg.GiteaServiceName, time.Since(checkpointStarted), time.Since(started), wrappedErr)
		return wrappedErr
	}
	log.Info("Ensured primary Gitea service is ready: service=%s readiness_duration=%s checkpoint_cleanup_duration=%s total_duration=%s", cfg.GiteaServiceName, readinessDuration, time.Since(checkpointStarted), time.Since(started))
	return nil
}

func ControlStatus(ctx context.Context) ([]*Snapshot, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}
	base, err := localControlBase(cfg.ControlListen)
	if err != nil {
		return nil, err
	}
	statusCtx, cancel := context.WithTimeout(ctx, responseBodyIdleTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(statusCtx, http.MethodGet, base+"/api/v1/replication/sync-jobs", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+cfg.ControlToken)
	req.Header.Set("Accept-Encoding", "gzip")
	client := &http.Client{CheckRedirect: checkReplicationRedirect}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("control status returned %s", resp.Status)
	}
	var snapshots []*Snapshot
	if err := decodeBoundedJSONResponse(resp, &snapshots); err != nil {
		return nil, err
	}
	return snapshots, nil
}

func RestoreLatest(ctx context.Context) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if !cfg.Enabled {
		return errors.New("replication restore is disabled")
	}
	if !setting.Database.Type.IsSQLite3() {
		return fmt.Errorf("snapshot restore requires sqlite3, got %s", setting.Database.Type)
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
	if cfg.Mode != modeReplica {
		return errors.New("snapshot restore requires MODE=replica")
	}
	if cfg.ControlToken == "" {
		return errors.New("[replicate] CONTROL_TOKEN is required")
	}
	client, err := newReplicateHTTPClient(cfg)
	if err != nil {
		return err
	}
	defer client.CloseIdleConnections()
	lock, err := acquireRestoreRunLock(cfg.SnapshotDir)
	if err != nil {
		if errors.Is(err, errRestoreAlreadyRunning) {
			log.Info("Standby replication restore not started: another restore is already running")
		} else {
			log.Error("Cannot acquire standby replication restore lock: %v", err)
		}
		return err
	}
	defer func() {
		if err := lock.Release(); err != nil {
			log.Error("Release standby replication restore lock failed: %v", err)
		}
	}()
	// The in-place update rewrites the data tree directly, so Gitea must not be serving
	// from it. The standby is a static backup node and does not run Gitea between restores.
	if active, err := systemctlUnitActive(ctx, cfg.GiteaServiceName); err != nil {
		return fmt.Errorf("inspect standby Gitea service state: %w", err)
	} else if active {
		return errors.New("standby Gitea service is running; stop it before a replication restore so the data tree stays static")
	}
	deferredChunkCacheLimit = cfg.ChunkCacheMaxBytes
	base := incrementalBase(cfg)
	started := time.Now()
	log.Info("Starting standby replication restore: source=%s snapshot_dir=%s chunk_cache_max_bytes=%d", redactedEndpointLabel(base), cfg.SnapshotDir, cfg.ChunkCacheMaxBytes)
	if err := restoreIncremental(ctx, cfg, base, client); err != nil {
		log.Error("Standby replication restore failed: source=%s duration=%s error=%v", redactedEndpointLabel(base), time.Since(started), err)
		return err
	}
	log.Info("Standby replication restore finished: source=%s duration=%s", redactedEndpointLabel(base), time.Since(started))
	return nil
}

// Keep replication HTTP redirects visible so another endpoint cannot satisfy the operation.
func checkReplicationRedirect(_ *http.Request, via []*http.Request) error {
	if len(via) > 0 {
		return http.ErrUseLastResponse
	}
	return nil
}

func newReplicateHTTPClient(cfg *config) (*http.Client, error) {
	defaultTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("unexpected default HTTP transport type %T", http.DefaultTransport)
	}
	transport := defaultTransport.Clone()
	transport.DisableCompression = false // Keep gzip negotiation on; replication readers bound decoded bodies.
	transport.TLSHandshakeTimeout = 30 * time.Second
	transport.ResponseHeaderTimeout = 2 * time.Minute
	transport.MaxResponseHeaderBytes = 64 * 1024
	transport.ExpectContinueTimeout = time.Second
	transport.IdleConnTimeout = 2 * time.Minute
	transport.MaxConnsPerHost = finalChunkFetchWorkers + finalChunkControlConnReserve
	transport.MaxIdleConnsPerHost = finalChunkFetchWorkers + finalChunkControlConnReserve
	if cfg.ControlProxyURL != "" {
		proxyURL, err := url.Parse(cfg.ControlProxyURL)
		if err != nil {
			return nil, fmt.Errorf("parse CONTROL_PROXY_URL: %w", err)
		}
		transport.Proxy = http.ProxyURL(proxyURL)
		log.Info("Replication control traffic will use proxy %s", redactedEndpointLabel(proxyURL.String()))
	}
	return &http.Client{
		Timeout:       0,
		Transport:     transport,
		CheckRedirect: checkReplicationRedirect,
	}, nil
}

func verifyFile(path, expected string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > chunkMaxSize {
		return errors.New("replication chunk cache entry exceeds maximum size")
	}
	f, openedInfo, err := openRegularFile(path, info)
	if err != nil {
		return err
	}
	defer f.Close()
	if openedInfo.Size() > chunkMaxSize {
		return errors.New("replication chunk cache entry exceeds maximum size")
	}
	h := sha256.New()
	if n, err := io.Copy(h, io.LimitReader(f, chunkMaxSize+1)); err != nil {
		return err
	} else if n > chunkMaxSize {
		return errors.New("replication chunk cache entry exceeds maximum size")
	}
	var sum [sha256.Size]byte
	copy(sum[:], h.Sum(sum[:0]))
	if !matchesSHA256Hex(sum, expected) {
		actual := hex.EncodeToString(sum[:])
		return fmt.Errorf("snapshot sha256 mismatch: got %s want %s", actual, expected)
	}
	return nil
}
