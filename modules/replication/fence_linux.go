// Copyright 2024 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build linux

package replication

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

var writeFencePath = "/run/gitea-replication/write.lock"

type WriteFence struct{ file *os.File }

func openFence() (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(writeFencePath), 0o750); err != nil {
		return nil, err
	}
	return os.OpenFile(writeFencePath, os.O_CREATE|os.O_RDWR, 0o640)
}

// AcquireSnapshotFence blocks new SSH writes and waits for active SSH writes.
func AcquireSnapshotFence(ctx context.Context) (*WriteFence, error) {
	file, err := openFence()
	if err != nil {
		return nil, err
	}
	for {
		err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return &WriteFence{file: file}, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			_ = file.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			_ = file.Close()
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// TryAcquireWriteLease protects the full lifetime of an SSH write operation.
func TryAcquireWriteLease() (*WriteFence, bool, error) {
	file, err := openFence()
	if err != nil {
		return nil, false, err
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_SH|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &WriteFence{file: file}, true, nil
}

func (f *WriteFence) Release() error {
	if f == nil || f.file == nil {
		return nil
	}
	err := unix.Flock(int(f.file.Fd()), unix.LOCK_UN)
	return errors.Join(err, f.file.Close())
}
