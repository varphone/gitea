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
	"time"

	"gitea.dev/modules/log"

	"golang.org/x/sys/unix"
)

var writeFencePath = "/run/gitea-replication/write.lock"

const restoreRunLockName = ".restore.lock"

var errRestoreAlreadyRunning = errors.New("another replication restore is already running")

var acquireSnapshotFence = AcquireSnapshotFence

type WriteFence struct{ file *os.File }

type restoreRunLock struct{ file *os.File }

func acquireRestoreRunLock(snapshotDir string) (*restoreRunLock, error) {
	path := filepath.Join(snapshotDir, restoreRunLockName)
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open replication restore lock: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("inspect replication restore lock: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG {
		_ = file.Close()
		return nil, errors.New("replication restore lock must be a regular file")
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, errRestoreAlreadyRunning
		}
		return nil, fmt.Errorf("acquire replication restore lock: %w", err)
	}
	return &restoreRunLock{file: file}, nil
}

func (l *restoreRunLock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := unix.Flock(int(l.file.Fd()), unix.LOCK_UN)
	return errors.Join(err, l.file.Close())
}

func openFence() (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(writeFencePath), 0o750); err != nil {
		return nil, err
	}
	fd, err := unix.Open(writeFencePath, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0o640)
	if err != nil {
		return nil, err
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG {
		_ = unix.Close(fd)
		return nil, errors.New("replication write fence must be a regular file")
	}
	return os.NewFile(uintptr(fd), writeFencePath), nil
}

// AcquireSnapshotFence blocks new SSH writes and waits for active SSH writes.
func AcquireSnapshotFence(ctx context.Context) (*WriteFence, error) {
	file, err := openFence()
	if err != nil {
		return nil, err
	}
	waitStarted := time.Now()
	nextProgressLog := waitStarted.Add(5 * time.Second)
	loggedWait := false
	for {
		err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return &WriteFence{file: file}, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			_ = file.Close()
			return nil, err
		}
		if now := time.Now(); !now.Before(nextProgressLog) {
			if loggedWait {
				log.Debug("Still waiting for active replication write leases: elapsed=%s", now.Sub(waitStarted))
			} else {
				log.Info("Waiting for active replication write leases to finish: elapsed=%s", now.Sub(waitStarted))
				loggedWait = true
			}
			nextProgressLog = now.Add(30 * time.Second)
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
