// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"context"
	"errors"
	"net/http"
	"os"
	"testing"

	"gitea.dev/modules/setting"
)

type testRoundTripper struct{}

func (testRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("unexpected test transport request")
}

func TestNewReplicateHTTPClientRejectsUnexpectedDefaultTransport(t *testing.T) {
	previous := http.DefaultTransport
	http.DefaultTransport = testRoundTripper{}
	t.Cleanup(func() { http.DefaultTransport = previous })
	if _, err := newReplicateHTTPClient(defaultConfig()); err == nil {
		t.Fatal("custom default HTTP transport should fail clearly instead of panicking")
	}
}

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
	if err := writeFileSynced(primaryOutageCheckpointPath(snapshotDir), []byte("pending")); err != nil {
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

func TestEnsurePrimaryServiceRecoversWithoutPrimaryMode(t *testing.T) {
	oldProvider, oldSystemctl, oldReadiness := setting.CfgProvider, systemctlRunner, readinessCheck
	defer func() { setting.CfgProvider, systemctlRunner, readinessCheck = oldProvider, oldSystemctl, oldReadiness }()
	snapshotDir := t.TempDir()
	provider, err := setting.NewConfigProviderFromData("[replicate]\nENABLED=true\nMODE=replica\nSOURCE_URL=https://primary.example\nCONTROL_TOKEN=01234567890123456789012345678901\nSNAPSHOT_DIR=" + snapshotDir + "\n")
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

	if err := EnsurePrimaryService(context.Background()); err != nil {
		t.Fatalf("replica without an outage checkpoint: %v", err)
	}
	if len(actions) != 0 {
		t.Fatalf("replica without a checkpoint ran systemctl actions: %v", actions)
	}

	if err := writeFileSynced(primaryOutageCheckpointPath(snapshotDir), []byte("pending")); err != nil {
		t.Fatal(err)
	}
	if err := EnsurePrimaryService(context.Background()); err != nil {
		t.Fatalf("recover primary with a leftover checkpoint: %v", err)
	}
	if len(actions) != 1 || actions[0] != "start:gitea.service" {
		t.Fatalf("recovery actions=%v want [start:gitea.service]", actions)
	}
	if _, err := os.Stat(primaryOutageCheckpointPath(snapshotDir)); !os.IsNotExist(err) {
		t.Fatalf("recovery checkpoint remains: %v", err)
	}
}
