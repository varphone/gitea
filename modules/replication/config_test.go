// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"strings"
	"testing"
	"time"

	"gitea.dev/modules/setting"
)

func TestLoadConfigValidation(t *testing.T) {
	oldProvider := setting.CfgProvider
	defer func() { setting.CfgProvider = oldProvider }()
	token := strings.Repeat("x", 32)
	tests := []struct {
		name               string
		config             string
		wantErr            bool
		wantFullScan       time.Duration
		checkFullScanValue bool
		wantOutageTimeout  time.Duration
		wantWriteTimeout   time.Duration
	}{
		{name: "primary", config: "[replicate]\nENABLED=true\nMODE=primary\nCONTROL_TOKEN=" + token + "\n"},
		{name: "replica", config: "[replicate]\nENABLED=true\nMODE=replica\nSOURCE_URL=https://primary.example\nCONTROL_TOKEN=" + token + "\n"},
		{name: "disabled full scan", config: "[replicate]\nENABLED=true\nMODE=primary\nCONTROL_TOKEN=" + token + "\nFULL_SCAN_INTERVAL=0\n", checkFullScanValue: true},
		{name: "custom full scan", config: "[replicate]\nENABLED=true\nMODE=primary\nCONTROL_TOKEN=" + token + "\nFULL_SCAN_INTERVAL=24h\n", wantFullScan: 24 * time.Hour, checkFullScanValue: true},
		{name: "custom outage and write timeouts", config: "[replicate]\nENABLED=true\nMODE=primary\nCONTROL_TOKEN=" + token + "\nPRIMARY_OUTAGE_TIMEOUT=15m\nCONTROL_WRITE_TIMEOUT=4m\n", wantOutageTimeout: 15 * time.Minute, wantWriteTimeout: 4 * time.Minute},
		{name: "zero outage timeout", config: "[replicate]\nENABLED=true\nMODE=primary\nCONTROL_TOKEN=" + token + "\nPRIMARY_OUTAGE_TIMEOUT=0s\n", wantErr: true},
		{name: "zero control write timeout", config: "[replicate]\nENABLED=true\nMODE=primary\nCONTROL_TOKEN=" + token + "\nCONTROL_WRITE_TIMEOUT=0s\n", wantErr: true},
		{name: "negative full scan", config: "[replicate]\nENABLED=true\nMODE=primary\nCONTROL_TOKEN=" + token + "\nFULL_SCAN_INTERVAL=-1h\n", wantErr: true},
		{name: "invalid full scan", config: "[replicate]\nENABLED=true\nMODE=primary\nCONTROL_TOKEN=" + token + "\nFULL_SCAN_INTERVAL=weekly\n", wantErr: true},
		{name: "short token", config: "[replicate]\nENABLED=true\nMODE=primary\nCONTROL_TOKEN=short\n", wantErr: true},
		{name: "public listen", config: "[replicate]\nENABLED=true\nMODE=primary\nCONTROL_TOKEN=" + token + "\nCONTROL_LISTEN=0.0.0.0:3001\n", wantErr: true},
		{name: "invalid source", config: "[replicate]\nENABLED=true\nMODE=replica\nSOURCE_URL=file:///tmp/source\nCONTROL_TOKEN=" + token + "\n", wantErr: true},
		{name: "proxy override", config: "[replicate]\nENABLED=true\nMODE=replica\nSOURCE_URL=https://primary.example\nCONTROL_PROXY_URL=http://proxy.example:3128\nCONTROL_TOKEN=" + token + "\n"},
		{name: "invalid proxy", config: "[replicate]\nENABLED=true\nMODE=replica\nSOURCE_URL=https://primary.example\nCONTROL_PROXY_URL=://bad\nCONTROL_TOKEN=" + token + "\n", wantErr: true},
		{name: "cleartext remote", config: "[replicate]\nENABLED=true\nMODE=replica\nSOURCE_URL=http://primary.example\nCONTROL_TOKEN=" + token + "\n", wantErr: true},
		{name: "wrong service", config: "[replicate]\nENABLED=true\nMODE=primary\nCONTROL_TOKEN=" + token + "\nGITEA_SERVICE_NAME=other.service\n", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider, err := setting.NewConfigProviderFromData(test.config)
			if err != nil {
				t.Fatal(err)
			}
			setting.CfgProvider = provider
			cfg, err := loadConfig()
			if (err != nil) != test.wantErr {
				t.Fatalf("loadConfig() error=%v, wantErr=%v", err, test.wantErr)
			}
			if err == nil && test.checkFullScanValue && cfg.FullScanInterval != test.wantFullScan {
				t.Fatalf("FullScanInterval=%s want=%s", cfg.FullScanInterval, test.wantFullScan)
			}
			if err == nil && test.wantOutageTimeout > 0 && cfg.PrimaryOutageTimeout != test.wantOutageTimeout {
				t.Fatalf("PrimaryOutageTimeout=%s want=%s", cfg.PrimaryOutageTimeout, test.wantOutageTimeout)
			}
			if err == nil && test.wantWriteTimeout > 0 && cfg.ControlWriteTimeout != test.wantWriteTimeout {
				t.Fatalf("ControlWriteTimeout=%s want=%s", cfg.ControlWriteTimeout, test.wantWriteTimeout)
			}
		})
	}
}

func TestDefaultFinalSessionTimeout(t *testing.T) {
	if got := defaultConfig().FinalSessionTimeout; got != 5*time.Minute {
		t.Fatalf("FinalSessionTimeout=%s want=5m", got)
	}
}

func TestDefaultOutageAndControlWriteTimeouts(t *testing.T) {
	cfg := defaultConfig()
	if cfg.PrimaryOutageTimeout != 30*time.Minute {
		t.Fatalf("PrimaryOutageTimeout=%s want=30m", cfg.PrimaryOutageTimeout)
	}
	if cfg.ControlWriteTimeout != 10*time.Minute {
		t.Fatalf("ControlWriteTimeout=%s want=10m", cfg.ControlWriteTimeout)
	}
}

func TestSessionExpiryRespectsPrimaryOutageDeadline(t *testing.T) {
	now := time.Now()
	deadline := now.Add(2 * time.Minute)
	if got := sessionExpiry(now, 5*time.Minute, deadline); !got.Equal(deadline) {
		t.Fatalf("session expiry=%s want outage deadline=%s", got, deadline)
	}
	if got := sessionExpiry(now, time.Minute, deadline); !got.Equal(now.Add(time.Minute)) {
		t.Fatalf("session expiry=%s want idle deadline=%s", got, now.Add(time.Minute))
	}
}

func TestSupportedIncrementalFormatVersions(t *testing.T) {
	for _, version := range []int{2, 3, 4, 5, 6} {
		if !supportedIncrementalFormatVersion(version) {
			t.Errorf("format %d is not supported", version)
		}
	}
	for _, version := range []int{1, 7} {
		if supportedIncrementalFormatVersion(version) {
			t.Errorf("format %d is unexpectedly supported", version)
		}
	}
}

func TestDefaultFullScanInterval(t *testing.T) {
	if got := defaultConfig().FullScanInterval; got != 168*time.Hour {
		t.Fatalf("FullScanInterval=%s want=168h", got)
	}
}
