// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"context"
	"os"
	"testing"

	"gitea.dev/modules/setting"
)

func TestDisabledReplicationEntrypointsHaveNoSideEffects(t *testing.T) {
	oldProvider, oldSystemctl, oldReadiness := setting.CfgProvider, systemctlRunner, readinessCheck
	defer func() { setting.CfgProvider, systemctlRunner, readinessCheck = oldProvider, oldSystemctl, oldReadiness }()
	snapshotDir := t.TempDir()
	provider, err := setting.NewConfigProviderFromData("[replicate]\nENABLED=false\nMODE=primary\nCONTROL_LISTEN=0.0.0.0:3001\nCONTROL_TOKEN=short\nSNAPSHOT_DIR=" + snapshotDir + "\nGITEA_SERVICE_NAME=other.service\n")
	if err != nil {
		t.Fatal(err)
	}
	setting.CfgProvider = provider
	var actions []string
	systemctlRunner = func(_ context.Context, action, service string) error {
		actions = append(actions, action+":"+service)
		return nil
	}
	readinessCheck = func(context.Context, string) error { return nil }
	if err := ServeControl(context.Background()); err == nil {
		t.Fatal("disabled control service started")
	}
	if err := RestoreLatest(context.Background()); err == nil {
		t.Fatal("disabled restore succeeded")
	}
	if err := EnsurePrimaryService(context.Background()); err != nil {
		t.Fatalf("disabled primary without an outage checkpoint: %v", err)
	}
	if len(actions) != 0 {
		t.Fatalf("disabled entrypoints ran systemctl actions: %v", actions)
	}
	if err := writeFileSynced(primaryOutageCheckpointPath(snapshotDir), []byte("pending"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := EnsurePrimaryService(context.Background()); err != nil {
		t.Fatalf("recover primary with disabled replication: %v", err)
	}
	if len(actions) != 1 || actions[0] != "start:gitea.service" {
		t.Fatalf("recovery actions=%v want [start:gitea.service]", actions)
	}
	if _, err := os.Stat(primaryOutageCheckpointPath(snapshotDir)); !os.IsNotExist(err) {
		t.Fatalf("recovery checkpoint remains: %v", err)
	}
}
