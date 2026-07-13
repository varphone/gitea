// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build linux

package replication

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"code.gitea.io/gitea/modules/setting"

	"golang.org/x/sys/unix"
)

const atomicSwitchServiceName = "gitea-replication-switch.service"

func installStagePath(cfg *config) string {
	return filepath.Join(cfg.SnapshotDir, ".install-stage")
}

func validateSwitchFilesystem(root, stageParent string) error {
	rootInfo, err := os.Stat(root)
	if err != nil {
		return err
	}
	stageInfo, err := os.Stat(stageParent)
	if err != nil {
		return err
	}
	rootStat, rootOK := rootInfo.Sys().(*syscall.Stat_t)
	stageStat, stageOK := stageInfo.Sys().(*syscall.Stat_t)
	if !rootOK || !stageOK || rootStat.Dev != stageStat.Dev {
		return errors.New("APP_WORK_PATH and SNAPSHOT_DIR must be on the same filesystem for atomic exchange")
	}
	return nil
}

func exchangeDirectories(root, stage string) error {
	for name, path := range map[string]string{"APP_WORK_PATH": root, "install stage": stage} {
		info, err := os.Lstat(path)
		if err != nil {
			return fmt.Errorf("stat %s: %w", name, err)
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%s must be a real directory", name)
		}
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil || filepath.Clean(resolved) != filepath.Clean(path) {
			return fmt.Errorf("%s must not contain symlink components", name)
		}
	}
	if err := validateSwitchFilesystem(root, filepath.Dir(stage)); err != nil {
		return err
	}
	if err := unix.Renameat2(unix.AT_FDCWD, root, unix.AT_FDCWD, stage, unix.RENAME_EXCHANGE); err != nil {
		return fmt.Errorf("atomically exchange APP_WORK_PATH and install stage: %w", err)
	}
	return errors.Join(syncDirectory(filepath.Dir(root)), syncDirectory(filepath.Dir(stage)))
}

// SwitchDataRoot is invoked only by the root-owned atomic switch systemd unit.
func SwitchDataRoot() error {
	if os.Geteuid() != 0 {
		return errors.New("atomic data switch must run as root")
	}
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if !cfg.Enabled || cfg.Mode != modeReplica {
		return errors.New("atomic data switch requires MODE=replica")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if systemctl(ctx, "is-active", cfg.GiteaServiceName) == nil {
		return fmt.Errorf("%s must be stopped before atomic data exchange", cfg.GiteaServiceName)
	}
	if err := ensureSocketActivationDisabled(ctx, cfg.GiteaServiceName); err != nil {
		return err
	}
	root := filepath.Clean(setting.AppWorkPath)
	stage := installStagePath(cfg)
	if err := validateAtomicLayout(cfg.SnapshotDir); err != nil {
		return err
	}
	return exchangeDirectories(root, stage)
}
