// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitea.dev/modules/log"
	"gitea.dev/modules/setting"
)

// config represents the configuration for instance replication.
type config struct {
	Enabled              bool          `ini:"ENABLED"`
	Mode                 string        `ini:"MODE"`
	SourceURL            string        `ini:"SOURCE_URL"`
	ControlListen        string        `ini:"CONTROL_LISTEN"`
	ControlSourceURL     string        `ini:"CONTROL_SOURCE_URL"`
	ControlProxyURL      string        `ini:"CONTROL_PROXY_URL"`
	ControlToken         string        `ini:"CONTROL_TOKEN"`
	SnapshotDir          string        `ini:"SNAPSHOT_DIR"`
	SnapshotRetention    int           `ini:"SNAPSHOT_RETENTION"`
	FullScanInterval     time.Duration `ini:"FULL_SCAN_INTERVAL"`
	GiteaServiceName     string        `ini:"GITEA_SERVICE_NAME"`
	ServiceTimeout       time.Duration `ini:"SERVICE_TIMEOUT"`
	SnapshotTimeout      time.Duration `ini:"SNAPSHOT_TIMEOUT"`
	FinalSessionTimeout  time.Duration `ini:"FINAL_SESSION_TIMEOUT"`
	PrimaryOutageTimeout time.Duration `ini:"PRIMARY_OUTAGE_TIMEOUT"`
	ControlWriteTimeout  time.Duration `ini:"CONTROL_WRITE_TIMEOUT"`
}

const (
	modeReplica = "replica"
	modePrimary = "primary"
)

// defaultConfig returns the default configuration values.
func defaultConfig() *config {
	return &config{
		Mode: modeReplica, ControlListen: "127.0.0.1:3001",
		SnapshotDir: "/var/lib/gitea-replication/snapshots", SnapshotRetention: 3, FullScanInterval: 168 * time.Hour,
		GiteaServiceName: "gitea.service", ServiceTimeout: 2 * time.Minute, SnapshotTimeout: 24 * time.Hour, FinalSessionTimeout: 5 * time.Minute,
		PrimaryOutageTimeout: 30 * time.Minute, ControlWriteTimeout: 10 * time.Minute,
	}
}

// loadConfig loads the replicate configuration from Gitea's config provider.
// This is called independently by the replicate subcommand, not through
// the global setting loading chain, to avoid modifying existing code.
func loadConfig() (*config, error) {
	sec, err := setting.CfgProvider.GetSection("replicate")
	if err != nil {
		// Section doesn't exist — that's OK if not enabled, use defaults
		log.Debug("No [replicate] section found in configuration; replication is disabled")
		return defaultConfig(), nil
	}
	return loadConfigFromSection(sec)
}

func loadConfigFromSection(sec setting.ConfigSection) (*config, error) {
	cfg := defaultConfig()
	var err error
	if err := sec.MapTo(cfg); err != nil {
		return nil, fmt.Errorf("failed to map replicate settings: %w", err)
	}

	if !cfg.Enabled {
		return cfg, nil
	}
	if runtime.GOOS != "linux" {
		return nil, errors.New("[replicate] disaster-recovery replication is supported only on Linux")
	}
	if sec.HasKey("FULL_SCAN_INTERVAL") {
		cfg.FullScanInterval, err = time.ParseDuration(strings.TrimSpace(sec.Key("FULL_SCAN_INTERVAL").String()))
		if err != nil {
			return nil, fmt.Errorf("[replicate] invalid FULL_SCAN_INTERVAL: %w", err)
		}
	}
	switch strings.ToLower(cfg.Mode) {
	case "", modeReplica:
		cfg.Mode = modeReplica
	case modePrimary:
		cfg.Mode = modePrimary
	default:
		return nil, fmt.Errorf("[replicate] MODE must be %q or %q, got %q", modeReplica, modePrimary, cfg.Mode)
	}
	if len(cfg.ControlToken) < 32 {
		return nil, errors.New("[replicate] CONTROL_TOKEN must contain at least 32 bytes")
	}
	if !filepath.IsAbs(cfg.SnapshotDir) {
		return nil, errors.New("[replicate] SNAPSHOT_DIR must be an absolute path")
	}
	host, port, err := net.SplitHostPort(cfg.ControlListen)
	if err != nil {
		return nil, fmt.Errorf("[replicate] invalid CONTROL_LISTEN: %w", err)
	}
	if !validNetworkPort(port) {
		return nil, errors.New("[replicate] invalid CONTROL_LISTEN: port must be a number between 1 and 65535")
	}
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, errors.New("[replicate] CONTROL_LISTEN must use a loopback address behind the reverse proxy")
	}
	if cfg.Mode == modeReplica {
		controlURL := cfg.ControlSourceURL
		if controlURL == "" {
			if cfg.SourceURL == "" {
				return nil, fmt.Errorf("[replicate] SOURCE_URL or CONTROL_SOURCE_URL is required when MODE=%q", modeReplica)
			}
			controlURL = strings.TrimRight(cfg.SourceURL, "/") + "/_replication"
		}
		parsed, err := url.Parse(controlURL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" || !validOptionalNetworkPort(parsed.Port()) ||
			parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || strings.Contains(controlURL, "#") {
			return nil, errors.New("[replicate] invalid control source URL: expected an absolute HTTP(S) URL without query or fragment")
		}
		if parsed.User != nil {
			return nil, errors.New("[replicate] replication control URL must not include user information; use CONTROL_TOKEN for authentication")
		}
		if parsed.Scheme == "http" {
			host := parsed.Hostname()
			ip := net.ParseIP(host)
			if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
				return nil, errors.New("[replicate] remote CONTROL_SOURCE_URL must use HTTPS")
			}
		}
		if cfg.ControlProxyURL != "" {
			proxyURL, err := url.Parse(cfg.ControlProxyURL)
			if err != nil || proxyURL.Hostname() == "" || !validOptionalNetworkPort(proxyURL.Port()) || (proxyURL.Path != "" && proxyURL.Path != "/") ||
				(proxyURL.Scheme != "http" && proxyURL.Scheme != "https" && proxyURL.Scheme != "socks5" && proxyURL.Scheme != "socks5h") ||
				proxyURL.RawQuery != "" || proxyURL.ForceQuery || proxyURL.Fragment != "" || strings.Contains(cfg.ControlProxyURL, "#") {
				return nil, errors.New("[replicate] invalid CONTROL_PROXY_URL: expected an absolute HTTP(S), SOCKS5, or SOCKS5H URL without query or fragment")
			}
		}
	}
	if cfg.SnapshotRetention < 1 {
		return nil, errors.New("[replicate] SNAPSHOT_RETENTION must be positive")
	}
	if sec.HasKey("FINAL_SESSION_TIMEOUT") {
		cfg.FinalSessionTimeout, err = time.ParseDuration(strings.TrimSpace(sec.Key("FINAL_SESSION_TIMEOUT").String()))
		if err != nil {
			return nil, fmt.Errorf("[replicate] invalid FINAL_SESSION_TIMEOUT: %w", err)
		}
	}
	for _, setting := range []struct {
		key   string
		value *time.Duration
	}{
		{key: "PRIMARY_OUTAGE_TIMEOUT", value: &cfg.PrimaryOutageTimeout},
		{key: "CONTROL_WRITE_TIMEOUT", value: &cfg.ControlWriteTimeout},
	} {
		if sec.HasKey(setting.key) {
			*setting.value, err = time.ParseDuration(strings.TrimSpace(sec.Key(setting.key).String()))
			if err != nil {
				return nil, fmt.Errorf("[replicate] invalid %s: %w", setting.key, err)
			}
		}
	}
	if cfg.FullScanInterval < 0 {
		return nil, errors.New("[replicate] FULL_SCAN_INTERVAL must not be negative")
	}
	if cfg.ServiceTimeout <= 0 || cfg.SnapshotTimeout <= 0 || cfg.FinalSessionTimeout <= 0 || cfg.PrimaryOutageTimeout <= 0 || cfg.ControlWriteTimeout <= 0 {
		return nil, errors.New("[replicate] timeouts must be positive")
	}
	if cfg.GiteaServiceName != "gitea.service" {
		return nil, fmt.Errorf("[replicate] GITEA_SERVICE_NAME must be %q", "gitea.service")
	}
	return cfg, nil
}

// ValidateReplicationPaths ensures snapshot data and replica-local indexes stay within APP_WORK_PATH.
func ValidateReplicationPaths(_ context.Context) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if !cfg.Enabled {
		return nil
	}
	type dataPath struct {
		description string
		path        string
	}
	var dataPaths []dataPath
	if setting.OAuth2.Enabled && usesFileBackedOAuth2SigningKey() {
		dataPaths = append(dataPaths, dataPath{description: "OAuth2 JWT signing key", path: setting.OAuth2.JWTSigningPrivateKeyFile})
	}
	if cfg.Mode == modeReplica {
		if setting.Indexer.IssueType == "bleve" {
			dataPaths = append(dataPaths, dataPath{description: "replica local Bleve issue index", path: setting.Indexer.IssuePath})
		}
		if setting.Indexer.RepoIndexerEnabled && setting.Indexer.RepoType == "bleve" {
			dataPaths = append(dataPaths, dataPath{description: "replica local Bleve code index", path: setting.Indexer.RepoPath})
		}
	}
	if len(dataPaths) == 0 {
		return nil
	}

	root, err := filepath.Abs(filepath.Clean(setting.AppWorkPath))
	if err != nil {
		return fmt.Errorf("resolve APP_WORK_PATH for replication path validation: %w", err)
	}
	resolvedRoot, err := resolvedPath(root)
	if err != nil {
		return fmt.Errorf("resolve APP_WORK_PATH for replication path validation: %w", err)
	}
	if resolvedRoot != root {
		return errors.New("APP_WORK_PATH must not contain symlink components for replication path validation")
	}

	validatePath := func(description, path string) error {
		if path == "" {
			return fmt.Errorf("%s path is empty", description)
		}
		absolutePath, err := filepath.Abs(filepath.Clean(path))
		if err != nil {
			return fmt.Errorf("resolve %s path %q: %w", description, path, err)
		}
		resolvedDataPath, err := resolvedPath(absolutePath)
		if err != nil {
			return fmt.Errorf("resolve %s path %q: %w", description, path, err)
		}
		if resolvedDataPath != absolutePath || absolutePath == root || !isWithin(root, absolutePath) {
			return fmt.Errorf("%s path %q must be inside APP_WORK_PATH %q for reliable snapshot restore", description, path, root)
		}
		return nil
	}

	for _, dataPath := range dataPaths {
		if err := validatePath(dataPath.description, dataPath.path); err != nil {
			return err
		}
	}
	return nil
}

func usesFileBackedOAuth2SigningKey() bool {
	switch setting.OAuth2.JWTSigningAlgorithm {
	case "RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "EdDSA":
		return true
	default:
		return false
	}
}

func oauth2SigningConfigIdentity() (string, error) {
	if !setting.OAuth2.Enabled {
		return "disabled", nil
	}
	identity := "enabled:" + setting.OAuth2.JWTSigningAlgorithm
	if !usesFileBackedOAuth2SigningKey() {
		return identity, nil
	}
	root, err := filepath.Abs(filepath.Clean(setting.AppWorkPath))
	if err != nil {
		return "", fmt.Errorf("resolve APP_WORK_PATH for OAuth2 signing config: %w", err)
	}
	keyPath, err := filepath.Abs(filepath.Clean(setting.OAuth2.JWTSigningPrivateKeyFile))
	if err != nil {
		return "", fmt.Errorf("resolve OAuth2 JWT signing key path %q: %w", setting.OAuth2.JWTSigningPrivateKeyFile, err)
	}
	relativePath, err := filepath.Rel(root, keyPath)
	if err != nil {
		return "", fmt.Errorf("resolve OAuth2 JWT signing key path %q relative to APP_WORK_PATH: %w", setting.OAuth2.JWTSigningPrivateKeyFile, err)
	}
	if relativePath == "." || relativePath == ".." || strings.HasPrefix(relativePath, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("OAuth2 JWT signing key path %q is outside APP_WORK_PATH %q", setting.OAuth2.JWTSigningPrivateKeyFile, root)
	}
	return identity + ":" + filepath.ToSlash(relativePath), nil
}

func validNetworkPort(port string) bool {
	if port == "" {
		return false
	}
	for _, digit := range port {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	value, err := strconv.Atoi(port)
	return err == nil && value > 0 && value <= 65535
}

func validOptionalNetworkPort(port string) bool {
	return port == "" || validNetworkPort(port)
}

// IsReplicaReadOnly reports whether the configured replica must reject writes.
func IsReplicaReadOnly() bool {
	readOnly, _, _, err := WriteProtection()
	if err != nil {
		logWriteProtectionConfigError(err)
		return true
	}
	return readOnly
}

// IsWriteProtected reports whether replica mode or primary recovery fencing currently blocks writes.
func IsWriteProtected() bool {
	readOnly, _, primaryRecoveryPending, err := WriteProtection()
	if err != nil {
		logWriteProtectionConfigError(err)
		return true
	}
	return readOnly || primaryRecoveryPending
}

type replicateConfigEntry struct {
	name  string
	value string
	has   bool
}

const writeProtectionConfigKeyCount = 16

var writeProtectionConfigKeys = [writeProtectionConfigKeyCount]string{
	"ENABLED", "MODE", "SOURCE_URL", "CONTROL_LISTEN", "CONTROL_SOURCE_URL", "CONTROL_PROXY_URL", "CONTROL_TOKEN",
	"SNAPSHOT_DIR", "SNAPSHOT_RETENTION", "FULL_SCAN_INTERVAL", "GITEA_SERVICE_NAME", "SERVICE_TIMEOUT",
	"SNAPSHOT_TIMEOUT", "FINAL_SESSION_TIMEOUT", "PRIMARY_OUTAGE_TIMEOUT", "CONTROL_WRITE_TIMEOUT",
}

type writeProtectionConfigSnapshot struct {
	entries     [writeProtectionConfigKeyCount]replicateConfigEntry
	cfg         *config
	err         error
	errorLogged atomic.Bool
}

var (
	writeProtectionConfig    atomic.Pointer[writeProtectionConfigSnapshot]
	writeProtectionConfigMu  sync.Mutex
	primaryRecoveryWaitLogMu sync.Mutex
	primaryRecoveryWaiting   bool
)

func sameReplicateConfigEntries(a, b [writeProtectionConfigKeyCount]replicateConfigEntry) bool {
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func loadWriteProtectionConfig(sec setting.ConfigSection) (*config, error) {
	// MapTo and HasKey can read inherited INI values, so include effective values for mapped fields.
	var entries [writeProtectionConfigKeyCount]replicateConfigEntry
	for i, name := range writeProtectionConfigKeys {
		has := sec.HasKey(name)
		entry := replicateConfigEntry{name: name, has: has}
		if has {
			entry.value = sec.Key(name).Value()
		}
		entries[i] = entry
	}

	if cached := writeProtectionConfig.Load(); cached != nil && sameReplicateConfigEntries(cached.entries, entries) {
		return cached.cfg, cached.err
	}

	writeProtectionConfigMu.Lock()
	defer writeProtectionConfigMu.Unlock()
	if cached := writeProtectionConfig.Load(); cached != nil && sameReplicateConfigEntries(cached.entries, entries) {
		return cached.cfg, cached.err
	}

	// Re-parse only when the section changes so write checks stay cheap and config edits take effect.
	cfg, err := loadConfigFromSection(sec)
	writeProtectionConfig.Store(&writeProtectionConfigSnapshot{entries: entries, cfg: cfg, err: err})
	return cfg, err
}

func logWriteProtectionConfigError(err error) {
	if cached := writeProtectionConfig.Load(); cached != nil && cached.err != nil && cached.err.Error() == err.Error() {
		if !cached.errorLogged.CompareAndSwap(false, true) {
			return
		}
	}
	log.Error("Invalid [replicate] configuration; denying writes: %v", err)
}

// WriteProtection reports replica read-only status, SSH fencing configuration, and pending primary recovery.
func WriteProtection() (readOnly, fencingEnabled, primaryRecoveryPending bool, err error) {
	sec, err := setting.CfgProvider.GetSection("replicate")
	if err != nil {
		return false, false, false, nil
	}
	cfg, err := loadWriteProtectionConfig(sec)
	if err != nil {
		return false, false, false, err
	}
	readOnly = cfg.Enabled && cfg.Mode == modeReplica
	fencingEnabled = cfg.Enabled && cfg.ControlToken != ""
	if cfg.Enabled && cfg.Mode == modePrimary {
		primaryRecoveryPending = primaryOutageCheckpointExists(cfg.SnapshotDir)
	}
	return readOnly, fencingEnabled, primaryRecoveryPending, nil
}

// WorkerRunner is the small part of a worker queue lifecycle needed by the primary recovery gate.
type WorkerRunner interface {
	Run()
	Cancel()
}

type primaryRecoveryGatedRunner struct {
	runner WorkerRunner
	ctx    context.Context
	cancel context.CancelFunc
}

var errReplicaWriteProtected = errors.New("writes are disabled in replica mode")

// RunAfterPrimaryRecovery delays a background worker until the primary recovery checkpoint clears.
func RunAfterPrimaryRecovery(runner WorkerRunner) WorkerRunner {
	ctx, cancel := context.WithCancel(context.Background())
	return &primaryRecoveryGatedRunner{runner: runner, ctx: ctx, cancel: cancel}
}

func (r *primaryRecoveryGatedRunner) Run() {
	if err := WaitForPrimaryRecovery(r.ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			log.Error("Background worker stopped while waiting for primary recovery: %v", err)
		}
		return
	}
	r.runner.Run()
}

func (r *primaryRecoveryGatedRunner) Cancel() {
	r.cancel()
	r.runner.Cancel()
}

// GuardPrimaryRecoveryHandler waits before processing each queue batch while primary recovery is pending.
func GuardPrimaryRecoveryHandler[T any](ctx context.Context, handler func(...T) []T) func(...T) []T {
	return func(items ...T) []T {
		if err := WaitForPrimaryRecovery(ctx); err != nil {
			if ctx.Err() != nil {
				return handler(items...)
			}
			if !errors.Is(err, context.Canceled) {
				log.Error("Cannot wait for primary recovery before processing background work: %v", err)
			}
			return items
		}
		return handler(items...)
	}
}

// WaitForPrimaryWritable blocks until the primary is writable or ctx is canceled.
func WaitForPrimaryWritable(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		readOnly, _, recoveryPending, err := WriteProtection()
		if err != nil {
			logWriteProtectionConfigError(err)
			return err
		}
		if readOnly {
			return errReplicaWriteProtected
		}
		if !recoveryPending {
			return nil
		}
		if err := WaitForPrimaryRecovery(ctx); err != nil {
			return err
		}
	}
}

func logPrimaryRecoveryWaitState(waiting bool) {
	primaryRecoveryWaitLogMu.Lock()
	defer primaryRecoveryWaitLogMu.Unlock()
	if primaryRecoveryWaiting == waiting {
		return
	}
	primaryRecoveryWaiting = waiting
	if waiting {
		log.Info("Waiting for primary recovery checkpoint state to clear before starting background work")
	} else {
		log.Info("Primary recovery checkpoint state is clear; background work may start")
	}
}

func resetPrimaryRecoveryWaitLogState() {
	primaryRecoveryWaitLogMu.Lock()
	primaryRecoveryWaiting = false
	primaryRecoveryWaitLogMu.Unlock()
}

// WaitForPrimaryRecovery blocks until the primary outage checkpoint is removed or ctx is canceled.
func WaitForPrimaryRecovery(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if !cfg.Enabled || cfg.Mode != modePrimary {
		resetPrimaryRecoveryWaitLogState()
		return nil
	}
	if !primaryOutageCheckpointExists(cfg.SnapshotDir) {
		logPrimaryRecoveryWaitState(false)
		return nil
	}
	logPrimaryRecoveryWaitState(true)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		if !primaryOutageCheckpointExists(cfg.SnapshotDir) {
			logPrimaryRecoveryWaitState(false)
			return nil
		}
	}
}
