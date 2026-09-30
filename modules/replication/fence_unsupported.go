// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build !linux

package replication

import (
	"context"
	"errors"
)

type WriteFence struct{}

type restoreRunLock struct{}

var errRestoreAlreadyRunning = errors.New("another replication restore is already running")

var acquireSnapshotFence = AcquireSnapshotFence

func acquireRestoreRunLock(string) (*restoreRunLock, error) {
	return nil, errors.New("disaster-recovery restore locking requires Linux")
}

func (*restoreRunLock) Release() error { return nil }

func AcquireSnapshotFence(context.Context) (*WriteFence, error) {
	return nil, errors.New("disaster-recovery fencing requires Linux")
}

func TryAcquireWriteLease() (*WriteFence, bool, error) {
	return &WriteFence{}, true, nil
}

func (*WriteFence) Release() error { return nil }
