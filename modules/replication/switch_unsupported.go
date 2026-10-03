// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build !linux

package replication

import (
	"errors"
	"path/filepath"
)

const atomicSwitchServiceName = "gitea-replication-switch.service"

func installStagePath(cfg *config) string {
	return filepath.Join(cfg.SnapshotDir, ".install-stage")
}

func directoryIdentity(string) (uint64, uint64, error) {
	return 0, 0, errors.New("directory identity checkpoints are supported only on Linux")
}

func pathHasDirectoryIdentity(string, uint64, uint64) bool {
	return false
}

func validateSwitchFilesystem(string, string) error {
	return errors.New("atomic data exchange is supported only on Linux")
}

func validateNoNestedMounts(string) error {
	return errors.New("atomic data exchange is supported only on Linux")
}

func validateAtomicExchangeSupport(string) error {
	return errors.New("atomic data exchange is supported only on Linux")
}

func SwitchDataRoot() error {
	return errors.New("atomic data exchange is supported only on Linux")
}
