// Copyright 2024 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"strings"
	"testing"
	"time"

	"code.gitea.io/gitea/modules/setting"
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
	}{
		{name: "primary", config: "[replicate]\nENABLED=true\nMODE=primary\nCONTROL_TOKEN=" + token + "\n"},
		{name: "replica", config: "[replicate]\nENABLED=true\nMODE=replica\nSOURCE_URL=https://primary.example\nCONTROL_TOKEN=" + token + "\n"},
		{name: "disabled full scan", config: "[replicate]\nENABLED=true\nMODE=primary\nCONTROL_TOKEN=" + token + "\nFULL_SCAN_INTERVAL=0\n", checkFullScanValue: true},
		{name: "custom full scan", config: "[replicate]\nENABLED=true\nMODE=primary\nCONTROL_TOKEN=" + token + "\nFULL_SCAN_INTERVAL=24h\n", wantFullScan: 24 * time.Hour, checkFullScanValue: true},
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
		})
	}
}

func TestDefaultFullScanInterval(t *testing.T) {
	if got := defaultConfig().FullScanInterval; got != 168*time.Hour {
		t.Fatalf("FullScanInterval=%s want=168h", got)
	}
}
