// Copyright 2024 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"code.gitea.io/gitea/modules/log"
	"code.gitea.io/gitea/modules/setting"
)

// config represents the configuration for instance replication.
type config struct {
	Enabled             bool          `ini:"ENABLED"`
	Mode                string        `ini:"MODE"`
	SourceURL           string        `ini:"SOURCE_URL"`
	ControlListen       string        `ini:"CONTROL_LISTEN"`
	ControlSourceURL    string        `ini:"CONTROL_SOURCE_URL"`
	ControlProxyURL     string        `ini:"CONTROL_PROXY_URL"`
	ControlToken        string        `ini:"CONTROL_TOKEN"`
	SnapshotDir         string        `ini:"SNAPSHOT_DIR"`
	SnapshotRetention   int           `ini:"SNAPSHOT_RETENTION"`
	FullScanInterval    time.Duration `ini:"FULL_SCAN_INTERVAL"`
	GiteaServiceName    string        `ini:"GITEA_SERVICE_NAME"`
	ServiceTimeout      time.Duration `ini:"SERVICE_TIMEOUT"`
	SnapshotTimeout     time.Duration `ini:"SNAPSHOT_TIMEOUT"`
	FinalSessionTimeout time.Duration `ini:"FINAL_SESSION_TIMEOUT"`
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
		GiteaServiceName: "gitea.service", ServiceTimeout: 2 * time.Minute, SnapshotTimeout: 24 * time.Hour, FinalSessionTimeout: 15 * time.Minute,
	}
}

// loadConfig loads the replicate configuration from Gitea's config provider.
// This is called independently by the replicate subcommand, not through
// the global setting loading chain, to avoid modifying existing code.
func loadConfig() (*config, error) {
	cfg := defaultConfig()
	sec, err := setting.CfgProvider.GetSection("replicate")
	if err != nil {
		// Section doesn't exist — that's OK if not enabled, use defaults
		log.Warn("No [replicate] section found in configuration, using defaults")
		return cfg, nil
	}
	if err := sec.MapTo(cfg); err != nil {
		return nil, fmt.Errorf("failed to map replicate settings: %w", err)
	}

	if !cfg.Enabled {
		return cfg, nil
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
	host, _, err := net.SplitHostPort(cfg.ControlListen)
	if err != nil {
		return nil, fmt.Errorf("[replicate] invalid CONTROL_LISTEN: %w", err)
	}
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, errors.New("[replicate] CONTROL_LISTEN must use a loopback address behind the reverse proxy")
	}
	if cfg.Mode == modeReplica {
		if cfg.SourceURL == "" {
			return nil, fmt.Errorf("[replicate] SOURCE_URL is required when MODE=%q", modeReplica)
		}
		controlURL := cfg.ControlSourceURL
		if controlURL == "" {
			controlURL = strings.TrimRight(cfg.SourceURL, "/") + "/_replication"
		}
		parsed, err := url.Parse(controlURL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return nil, fmt.Errorf("[replicate] invalid control source URL %q", controlURL)
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
			if err != nil || proxyURL.Scheme == "" || proxyURL.Host == "" {
				return nil, fmt.Errorf("[replicate] invalid CONTROL_PROXY_URL %q", cfg.ControlProxyURL)
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
	if cfg.FullScanInterval < 0 {
		return nil, errors.New("[replicate] FULL_SCAN_INTERVAL must not be negative")
	}
	if cfg.ServiceTimeout <= 0 || cfg.SnapshotTimeout <= 0 || cfg.FinalSessionTimeout <= 0 {
		return nil, errors.New("[replicate] timeouts must be positive")
	}
	if cfg.GiteaServiceName != "gitea.service" {
		return nil, fmt.Errorf("[replicate] GITEA_SERVICE_NAME must be %q", "gitea.service")
	}
	return cfg, nil
}

// WriteFencingEnabled reports whether SSH writes must participate in the DR flock.
func IsReplicaReadOnly() bool {
	cfg, err := loadConfig()
	return err == nil && cfg.Enabled && cfg.Mode == modeReplica
}

func WriteFencingEnabled() bool {
	cfg, err := loadConfig()
	return err == nil && cfg.Enabled && cfg.ControlToken != ""
}
