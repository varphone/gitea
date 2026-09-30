// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"context"
	"crypto/hmac"
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
	FormatVersion       int         `json:"format_version"`
	GiteaVersion        string      `json:"gitea_version"`
	AppWorkPath         string      `json:"app_work_path"`
	InstanceFingerprint string      `json:"instance_fingerprint"`
	Signature           string      `json:"signature,omitempty"`
	FullScanAt          time.Time   `json:"full_scan_at,omitzero"`
	FileCount           int         `json:"file_count,omitempty"`
	Files               []TreeEntry `json:"files,omitempty"`
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

func syncDirectory(path string) error {
	dir, err := os.Open(path)
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

func loadManifests(dir, token string) (map[string]*Snapshot, bool, error) {
	paths, err := listManifestPaths(dir)
	if err != nil {
		return nil, false, err
	}
	result := make(map[string]*Snapshot, len(paths))
	transferCheckpointFound := false
	for _, path := range paths {
		fileID := strings.TrimSuffix(filepath.Base(path), ".json")
		if !validSnapshotID(fileID) {
			continue
		}
		data, err := readManifestData(path)
		if err != nil {
			log.Warn("Skip oversized or unreadable snapshot manifest %s: %v", path, err)
			continue
		}
		var transferHint struct {
			ID    string `json:"id"`
			State string `json:"state"`
		}
		_ = json.Unmarshal(data, &transferHint)
		if transferHint.State == "transferring" && transferHint.ID == fileID {
			transferCheckpointFound = true
		}
		var manifest SnapshotManifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			log.Warn("Skip invalid snapshot manifest %s: %v", path, err)
			continue
		}
		if manifest.FormatVersion != incrementalFormatVersion || manifest.ID != fileID {
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
	return result, transferCheckpointFound, nil
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
	if cfg.Mode != modePrimary {
		return nil
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
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("control status returned %s", resp.Status)
	}
	var snapshots []*Snapshot
	if err := decodeBoundedJSON(resp.Body, 1<<20, &snapshots); err != nil {
		return nil, err
	}
	return snapshots, nil
}

func RestoreLatest(ctx context.Context) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if !setting.Database.Type.IsSQLite3() {
		return fmt.Errorf("snapshot restore requires sqlite3, got %s", setting.Database.Type)
	}
	if err := os.MkdirAll(cfg.SnapshotDir, 0o700); err != nil {
		return err
	}
	if err := validateAtomicLayout(cfg.SnapshotDir); err != nil {
		return err
	}
	if cfg.Mode != modeReplica {
		return errors.New("snapshot restore requires MODE=replica")
	}
	if cfg.ControlToken == "" {
		return errors.New("[replicate] CONTROL_TOKEN is required")
	}
	if err := validateSwitchFilesystem(filepath.Clean(setting.AppWorkPath), cfg.SnapshotDir); err != nil {
		return err
	}
	client, err := newReplicateHTTPClient(cfg)
	if err != nil {
		return err
	}
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
	base := incrementalBase(cfg)
	started := time.Now()
	log.Info("Starting standby replication restore: source=%s snapshot_dir=%s", redactedEndpointLabel(base), cfg.SnapshotDir)
	if err := restoreIncremental(ctx, cfg, base, client); err != nil {
		log.Error("Standby replication restore failed: source=%s duration=%s error=%v", redactedEndpointLabel(base), time.Since(started), err)
		return err
	}
	return nil
}

func newReplicateHTTPClient(cfg *config) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSHandshakeTimeout = 30 * time.Second
	transport.ResponseHeaderTimeout = 2 * time.Minute
	transport.ExpectContinueTimeout = time.Second
	transport.IdleConnTimeout = 2 * time.Minute
	transport.MaxConnsPerHost = finalChunkFetchWorkers
	transport.MaxIdleConnsPerHost = finalChunkFetchWorkers
	if cfg.ControlProxyURL != "" {
		proxyURL, err := url.Parse(cfg.ControlProxyURL)
		if err != nil {
			return nil, fmt.Errorf("parse CONTROL_PROXY_URL: %w", err)
		}
		transport.Proxy = http.ProxyURL(proxyURL)
		log.Info("Replication control traffic will use proxy %s", redactedEndpointLabel(proxyURL.String()))
	}
	return &http.Client{Timeout: 0, Transport: transport}, nil
}

func verifyFile(path, expected string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	f, _, err := openRegularFile(path, info)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	actual := hex.EncodeToString(h.Sum(nil))
	if expected == "" || actual != expected {
		return fmt.Errorf("snapshot sha256 mismatch: got %s want %s", actual, expected)
	}
	return nil
}
