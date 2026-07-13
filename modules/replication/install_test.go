// Copyright 2024 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build linux

package replication

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"code.gitea.io/gitea/modules/setting"
)

func TestInstallSnapshotRollbackAndSuccess(t *testing.T) {
	oldWorkPath, oldDBPath, oldCustomConf := setting.AppWorkPath, setting.Database.Path, setting.CustomConf
	oldSystemctl, oldExecutable, oldAtomicSwitch := systemctlRunner, executablePath, atomicSwitchRunner
	oldFencePath := writeFencePath
	oldRegenerate, oldReadiness := regenerateKeys, readinessCheck
	oldCleanup := cleanupBackup
	defer func() {
		setting.AppWorkPath, setting.Database.Path, setting.CustomConf = oldWorkPath, oldDBPath, oldCustomConf
		systemctlRunner, executablePath, atomicSwitchRunner = oldSystemctl, oldExecutable, oldAtomicSwitch
		regenerateKeys, readinessCheck = oldRegenerate, oldReadiness
		cleanupBackup = oldCleanup
		writeFencePath = oldFencePath
	}()

	for _, test := range []struct {
		name          string
		regenerateErr error
		readinessErr  error
		wantData      string
		wantErr       bool
		wantWarning   bool
		wantBackup    bool
		cleanupErr    error
	}{
		{name: "regenerate failure rolls back", regenerateErr: errors.New("injected regenerate failure"), wantData: "old", wantErr: true},
		{name: "readiness failure rolls back", readinessErr: errors.New("injected readiness failure"), wantData: "old", wantErr: true},
		{name: "success activates snapshot", wantData: "new"},
		{name: "cleanup failure is warning after activation", wantData: "new", wantErr: true, wantWarning: true, wantBackup: true, cleanupErr: errors.New("injected cleanup failure")},
	} {
		t.Run(test.name, func(t *testing.T) {
			parent := t.TempDir()
			snapshotDir := filepath.Join(parent, "replication")
			if err := os.Mkdir(snapshotDir, 0o700); err != nil {
				t.Fatal(err)
			}
			writeFencePath = filepath.Join(t.TempDir(), "restore-write.lock")
			root := filepath.Join(parent, "gitea")
			setting.AppWorkPath = root
			setting.Database.Path = filepath.Join(root, "data", "gitea.db")
			setting.CustomConf = filepath.Join(root, "custom", "conf", "app.ini")
			requireWriteFile(t, setting.Database.Path, "old")
			requireWriteFile(t, setting.CustomConf, "standby-config")

			cfg := &config{
				SnapshotDir:      snapshotDir,
				GiteaServiceName: "gitea.service",
				ServiceTimeout:   5 * time.Second,
			}
			stage := installStagePath(cfg)
			if err := os.Mkdir(stage, 0o700); err != nil {
				t.Fatal(err)
			}
			requireWriteFile(t, filepath.Join(stage, "data", "gitea.db"), "new")
			requireWriteFile(t, filepath.Join(stage, "custom", "conf", "app.ini"), "primary-config")

			var actions []string
			systemctlRunner = func(_ context.Context, action, service string) error {
				actions = append(actions, action+":"+service)
				if action == "is-active" && strings.HasSuffix(service, ".socket") {
					return errors.New("inactive")
				}
				return nil
			}
			executablePath = func() (string, error) { return "/fake/gitea", nil }
			regenerateKeys = func(context.Context, string, string) error { return test.regenerateErr }
			readinessCheck = func(context.Context, string) error { return test.readinessErr }
			atomicSwitchRunner = func(context.Context, *config) error {
				return exchangeDirectories(root, stage)
			}
			cleanupBackup = func(path string) error {
				if test.cleanupErr != nil {
					return test.cleanupErr
				}
				return os.RemoveAll(path)
			}

			err := installPreparedSnapshot(context.Background(), stage, &Snapshot{ID: "test", RootMode: 0o700}, cfg)
			if (err != nil) != test.wantErr {
				t.Fatalf("installSnapshot() error = %v, wantErr=%v", err, test.wantErr)
			}
			var warning *cleanupWarning
			if errors.As(err, &warning) != test.wantWarning {
				t.Fatalf("cleanup warning=%v, want=%v, err=%v", warning, test.wantWarning, err)
			}
			data, readErr := os.ReadFile(setting.Database.Path)
			if readErr != nil || string(data) != test.wantData {
				t.Fatalf("active database = %q, err=%v, want %q", data, readErr, test.wantData)
			}
			configData, readErr := os.ReadFile(setting.CustomConf)
			if readErr != nil || string(configData) != "standby-config" {
				t.Fatalf("standby config = %q, err=%v", configData, readErr)
			}
			if !slices.Contains(actions, "stop:gitea.service") || !slices.Contains(actions, "start:gitea.service") {
				t.Fatalf("systemctl actions = %v", actions)
			}
			_, statErr := os.Stat(stage)
			if test.wantBackup && statErr != nil {
				t.Fatalf("expected retained backup: %v", statErr)
			}
			if !test.wantBackup && !os.IsNotExist(statErr) {
				t.Fatalf("backup directory remains after operation: %v", statErr)
			}
		})
	}
}

func TestSocketActivationIsRejected(t *testing.T) {
	old := systemctlRunner
	defer func() { systemctlRunner = old }()
	systemctlRunner = func(context.Context, string, string) error { return nil }
	if err := ensureSocketActivationDisabled(context.Background(), "gitea.service"); err == nil {
		t.Fatal("active gitea.socket was accepted")
	}
}

func TestResolvedPathDetectsSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "storage")); err != nil {
		t.Fatal(err)
	}
	resolvedRoot, err := resolvedPath(root)
	if err != nil {
		t.Fatal(err)
	}
	resolvedStorage, err := resolvedPath(filepath.Join(root, "storage", "packages"))
	if err != nil {
		t.Fatal(err)
	}
	if isWithin(resolvedRoot, resolvedStorage) {
		t.Fatalf("symlink escape %q incorrectly accepted within %q", resolvedStorage, resolvedRoot)
	}
}
