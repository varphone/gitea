// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build linux

package replication

import (
	"math"
	"strings"
	"testing"
)

func TestCheckRestoreCapacityIncludesSpaceAndInodes(t *testing.T) {
	oldStat := statFSAvailable
	defer func() { statFSAvailable = oldStat }()
	statFSAvailable = func(path string) (int64, int64, uint64, error) {
		if path == "/replication" {
			return 1 << 30, 10000, 1, nil
		}
		return 1 << 30, 10000, 1, nil
	}
	if err := checkRestoreCapacity("/replication/cache", "/gitea", 10<<20, 1<<20, 400, 20); err != nil {
		t.Fatalf("adequate capacity rejected: %v", err)
	}
	statFSAvailable = func(string) (int64, int64, uint64, error) { return 1, 10000, 1, nil }
	if err := checkRestoreCapacity("/replication/cache", "/gitea", 10<<20, 1<<20, 400, 20); err == nil || !strings.Contains(err.Error(), "insufficient free space") {
		t.Fatalf("insufficient byte capacity error=%v", err)
	}
	statFSAvailable = func(string) (int64, int64, uint64, error) { return 1 << 30, 1, 1, nil }
	if err := checkRestoreCapacity("/replication/cache", "/gitea", 10<<20, 1<<20, 400, 20); err == nil || !strings.Contains(err.Error(), "insufficient free inodes") {
		t.Fatalf("insufficient inode capacity error=%v", err)
	}
}

func TestCheckRestoreCapacitySeparatesFilesystemsAndRejectsOverflow(t *testing.T) {
	oldStat := statFSAvailable
	defer func() { statFSAvailable = oldStat }()
	statFSAvailable = func(path string) (int64, int64, uint64, error) {
		if path == "/replication" {
			return restoreDiskSafetyMargin + 1, 300, 1, nil
		}
		return restoreDiskSafetyMargin + 100, 300, 2, nil
	}
	if err := checkRestoreCapacity("/replication/cache", "/gitea", 100, 1, 1, 1); err != nil {
		t.Fatalf("separate filesystems with sufficient capacity rejected: %v", err)
	}
	if err := checkRestoreCapacity("/replication/cache", "/gitea", math.MaxInt64, 1, 1, 1); err == nil {
		t.Fatal("overflowing capacity estimate accepted")
	}
}
