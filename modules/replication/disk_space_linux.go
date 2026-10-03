// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build linux

package replication

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

const restoreDiskSafetyMargin = 64 << 20

var statFSAvailable = statFSAvailableLinux

func statFSAvailableLinux(path string) (int64, int64, uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, 0, 0, fmt.Errorf("inspect free space for %s: %w", path, err)
	}
	if stat.Bsize <= 0 {
		return 0, 0, 0, fmt.Errorf("invalid block size reported for %s", path)
	}
	blockSize := uint64(stat.Bsize)
	if stat.Bavail > math.MaxInt64/blockSize || stat.Ffree > math.MaxInt64 {
		return 0, 0, 0, fmt.Errorf("invalid available space reported for %s", path)
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("inspect filesystem for %s: %w", path, err)
	}
	statInfo, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, 0, fmt.Errorf("inspect filesystem identity for %s", path)
	}
	return int64(stat.Bavail * blockSize), int64(stat.Ffree), statInfo.Dev, nil
}

func checkRestoreCapacity(cacheDir, stageDir string, stageBytes, fetchBytes, stageInodes, fetchInodes int64) error {
	if stageBytes < 0 || fetchBytes < 0 || stageInodes < 0 || fetchInodes < 0 ||
		stageBytes > math.MaxInt64-fetchBytes || stageInodes > math.MaxInt64-fetchInodes {
		return fmt.Errorf("invalid replication capacity estimate: stage=%d bytes/%d entries chunk_cache=%d bytes/%d chunks", stageBytes, stageInodes, fetchBytes, fetchInodes)
	}
	cachePath := filepath.Dir(cacheDir)
	stagePath := stageDir
	if stagePath == "" {
		stagePath = cachePath
	}
	cacheBytes, cacheInodes, cacheFS, err := statFSAvailable(cachePath)
	if err != nil {
		return err
	}
	stageAvailableBytes, stageAvailableInodes, stageFS, err := statFSAvailable(stagePath)
	if err != nil {
		return err
	}
	if cacheFS == stageFS {
		return checkFilesystemCapacity(cachePath, cacheBytes, cacheInodes, stageBytes+fetchBytes, stageInodes+fetchInodes)
	}
	if err := checkFilesystemCapacity(cachePath, cacheBytes, cacheInodes, fetchBytes, fetchInodes); err != nil {
		return err
	}
	return checkFilesystemCapacity(stagePath, stageAvailableBytes, stageAvailableInodes, stageBytes, stageInodes)
}

func checkFilesystemCapacity(path string, availableBytes, availableInodes, bytesNeeded, inodesNeeded int64) error {
	byteMargin := max(int64(restoreDiskSafetyMargin), bytesNeeded/20)
	if bytesNeeded > math.MaxInt64-byteMargin {
		return fmt.Errorf("replication byte capacity estimate overflows for %s", path)
	}
	bytesNeeded += byteMargin
	inodeMargin := max(int64(256), inodesNeeded/20)
	if inodesNeeded > math.MaxInt64-inodeMargin {
		return fmt.Errorf("replication inode capacity estimate overflows for %s", path)
	}
	inodesNeeded += inodeMargin
	if availableBytes < bytesNeeded {
		return fmt.Errorf("insufficient free space for replication restore in %s: need at least %d bytes for staging and chunk cache, have %d", path, bytesNeeded, availableBytes)
	}
	if availableInodes < inodesNeeded {
		return fmt.Errorf("insufficient free inodes for replication restore in %s: need at least %d for staging and chunk cache, have %d", path, inodesNeeded, availableInodes)
	}
	return nil
}
