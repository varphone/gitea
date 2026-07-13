// Copyright 2024 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build !linux

package replication

import (
	"context"
	"errors"
)

type WriteFence struct{}

func AcquireSnapshotFence(context.Context) (*WriteFence, error) {
	return nil, errors.New("disaster-recovery fencing requires Linux")
}

func TryAcquireWriteLease() (*WriteFence, bool, error) {
	return &WriteFence{}, true, nil
}

func (*WriteFence) Release() error { return nil }
