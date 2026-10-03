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

const (
	restoreRunLockName = ".restore.lock"
	controlRunLockName = ".control.lock"
)

var (
	errRestoreAlreadyRunning = errors.New("another replication restore is already running")
	errControlAlreadyRunning = errors.New("another replication control service is already running")
)

var acquireSnapshotFence = AcquireSnapshotFence

type WriteFence struct {
	file *os.File
	gate *os.File
}

type (
	restoreRunLock struct{ file *os.File }
	controlRunLock struct{ file *os.File }
)

func flockRetryInterrupted(fd, operation int) error {
	for {
		err := unix.Flock(fd, operation)
		if !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}

func acquireReplicationRunFileLock(snapshotDir, lockName, lockKind string, alreadyRunning error) (*os.File, error) {
	path := filepath.Join(snapshotDir, lockName)
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open replication %s lock: %w", lockKind, err)
	}
	file := os.NewFile(uintptr(fd), path)
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("inspect replication %s lock: %w", lockKind, err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG {
		_ = file.Close()
		return nil, fmt.Errorf("replication %s lock must be a regular file", lockKind)
	}
	if err := flockRetryInterrupted(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, alreadyRunning
		}
		return nil, fmt.Errorf("acquire replication %s lock: %w", lockKind, err)
	}
	return file, nil
}

func acquireRestoreRunLock(snapshotDir string) (*restoreRunLock, error) {
	file, err := acquireReplicationRunFileLock(snapshotDir, restoreRunLockName, "restore", errRestoreAlreadyRunning)
	if err != nil {
		return nil, err
	}
	return &restoreRunLock{file: file}, nil
}

func acquireControlRunLock(snapshotDir string) (*controlRunLock, error) {
	file, err := acquireReplicationRunFileLock(snapshotDir, controlRunLockName, "control service", errControlAlreadyRunning)
	if err != nil {
		return nil, err
	}
	return &controlRunLock{file: file}, nil
}

func (l *controlRunLock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := flockRetryInterrupted(int(l.file.Fd()), unix.LOCK_UN)
	return errors.Join(err, l.file.Close())
}

func acquireControlStartupRestoreLock(ctx context.Context, snapshotDir string) (*restoreRunLock, error) {
	waitStarted := time.Now()
	loggedWait := false
	for {
		lock, err := acquireRestoreRunLock(snapshotDir)
		if err == nil {
			return lock, nil
		}
		if !errors.Is(err, errRestoreAlreadyRunning) {
			return nil, err
		}
		if !loggedWait && time.Since(waitStarted) >= 5*time.Second {
			log.Info("Waiting for active standby restore before loading replication manifests: elapsed=%s", time.Since(waitStarted))
			loggedWait = true
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (l *restoreRunLock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := flockRetryInterrupted(int(l.file.Fd()), unix.LOCK_UN)
	return errors.Join(err, l.file.Close())
}

func openFenceAt(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0o640)
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
	return os.NewFile(uintptr(fd), path), nil
}

func openFence() (*os.File, error) {
	return openFenceAt(writeFencePath)
}

func openFenceGate() (*os.File, error) {
	return openFenceAt(writeFencePath + ".gate")
}

func releaseFenceFile(file *os.File) error {
	if file == nil {
		return nil
	}
	return errors.Join(flockRetryInterrupted(int(file.Fd()), unix.LOCK_UN), file.Close())
}

func acquireExclusiveFence(ctx context.Context, file *os.File, waitingFor string) error {
	waitStarted := time.Now()
	nextProgressLog := waitStarted.Add(5 * time.Second)
	loggedWait := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return nil
		}
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			return err
		}
		if now := time.Now(); !now.Before(nextProgressLog) {
			if loggedWait {
				log.Debug("Still waiting for %s: elapsed=%s", waitingFor, now.Sub(waitStarted))
			} else {
				log.Info("Waiting for %s: elapsed=%s", waitingFor, now.Sub(waitStarted))
				loggedWait = true
			}
			nextProgressLog = now.Add(30 * time.Second)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// AcquireSnapshotFence blocks new SSH writes and waits for active SSH writes.
func AcquireSnapshotFence(ctx context.Context) (*WriteFence, error) {
	gate, err := openFenceGate()
	if err != nil {
		return nil, err
	}
	if err := acquireExclusiveFence(ctx, gate, "new SSH write leases to register"); err != nil {
		_ = gate.Close()
		return nil, err
	}
	file, err := openFence()
	if err != nil {
		return nil, errors.Join(err, releaseFenceFile(gate))
	}
	if err := acquireExclusiveFence(ctx, file, "active replication write leases to finish"); err != nil {
		return nil, errors.Join(err, file.Close(), releaseFenceFile(gate))
	}
	return &WriteFence{file: file, gate: gate}, nil
}

// TryAcquireWriteLease protects the full lifetime of an SSH write operation.
func TryAcquireWriteLease() (*WriteFence, bool, error) {
	gate, err := openFenceGate()
	if err != nil {
		return nil, false, err
	}
	if err := flockRetryInterrupted(int(gate.Fd()), unix.LOCK_SH|unix.LOCK_NB); err != nil {
		_ = gate.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, false, nil
		}
		return nil, false, err
	}
	file, err := openFence()
	if err != nil {
		return nil, false, errors.Join(err, releaseFenceFile(gate))
	}
	if err := flockRetryInterrupted(int(file.Fd()), unix.LOCK_SH|unix.LOCK_NB); err != nil {
		_ = file.Close()
		gateErr := releaseFenceFile(gate)
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, false, gateErr
		}
		return nil, false, errors.Join(err, gateErr)
	}
	if err := releaseFenceFile(gate); err != nil {
		return nil, false, errors.Join(err, releaseFenceFile(file))
	}
	return &WriteFence{file: file}, true, nil
}

func (f *WriteFence) Release() error {
	if f == nil {
		return nil
	}
	return errors.Join(releaseFenceFile(f.file), releaseFenceFile(f.gate))
}
