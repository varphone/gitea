// Copyright 2024 The Gitea Authors. All rights reserved.
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

	"code.gitea.io/gitea/modules/json"
	"code.gitea.io/gitea/modules/log"
	"code.gitea.io/gitea/modules/setting"
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

func writeManifest(dir string, manifest *SnapshotManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	if len(data) > maxManifestSize {
		return errors.New("incremental manifest exceeds maximum size")
	}
	tmp := manifestPath(dir, manifest.ID) + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, manifestPath(dir, manifest.ID)); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func loadManifests(dir, token string) (map[string]*Snapshot, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	result := make(map[string]*Snapshot, len(paths))
	for _, path := range paths {
		info, statErr := os.Stat(path)
		if statErr != nil || info.Size() > maxManifestSize {
			log.Warn("Skip oversized or unreadable snapshot manifest %s", path)
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			log.Warn("Skip unreadable snapshot manifest %s: %v", path, err)
			continue
		}
		var manifest SnapshotManifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			log.Warn("Skip invalid snapshot manifest %s: %v", path, err)
			continue
		}
		if manifest.FormatVersion != incrementalFormatVersion || !validSnapshotID(manifest.ID) || strings.TrimSuffix(filepath.Base(path), ".json") != manifest.ID {
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
	return result, nil
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

func EnsurePrimaryService(ctx context.Context) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if cfg.Mode != modePrimary {
		return nil
	}
	taskCtx, cancel := context.WithTimeout(ctx, cfg.ServiceTimeout)
	defer cancel()
	return systemctl(taskCtx, "start", cfg.GiteaServiceName)
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/v1/replication/sync-jobs", nil)
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
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&snapshots); err != nil {
		return nil, err
	}
	return snapshots, nil
}

func RestoreLatest(ctx context.Context) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if setting.Database.Type.String() != "sqlite3" {
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
	return restoreIncremental(ctx, cfg, incrementalBase(cfg), client)
}

func newReplicateHTTPClient(cfg *config) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSHandshakeTimeout = 30 * time.Second
	transport.ResponseHeaderTimeout = 2 * time.Minute
	transport.ExpectContinueTimeout = time.Second
	transport.IdleConnTimeout = 2 * time.Minute
	if cfg.ControlProxyURL != "" {
		proxyURL, err := url.Parse(cfg.ControlProxyURL)
		if err != nil {
			return nil, fmt.Errorf("parse CONTROL_PROXY_URL: %w", err)
		}
		transport.Proxy = http.ProxyURL(proxyURL)
		log.Info("Replication control traffic will use proxy %s", proxyURL.Redacted())
	}
	return &http.Client{Timeout: 0, Transport: transport}, nil
}

func verifyFile(path, expected string) error {
	f, err := os.Open(path)
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
