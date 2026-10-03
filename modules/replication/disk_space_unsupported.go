// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build !linux

package replication

import "errors"

const restoreDiskSafetyMargin = 64 << 20

func checkRestoreCapacity(string, string, int64, int64, int64, int64) error {
	return errors.New("replication disk capacity checks are supported only on Linux")
}
