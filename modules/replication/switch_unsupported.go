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

func validateSwitchFilesystem(string, string) error {
	return errors.New("atomic data exchange is supported only on Linux")
}

func SwitchDataRoot() error {
	return errors.New("atomic data exchange is supported only on Linux")
}
