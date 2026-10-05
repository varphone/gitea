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
			return 1024 * 1024 * 1024, 10000, 1, nil
		}
		return 1024 * 1024 * 1024, 10000, 1, nil
	}
	if err := checkRestoreCapacity("/replication/cache", "/gitea", 10<<20, 1<<20, 400, 20); err != nil {
		t.Fatalf("adequate capacity rejected: %v", err)
	}
	statFSAvailable = func(string) (int64, int64, uint64, error) { return 1, 10000, 1, nil }
	if err := checkRestoreCapacity("/replication/cache", "/gitea", 10<<20, 1<<20, 400, 20); err == nil || !strings.Contains(err.Error(), "insufficient free space") {
		t.Fatalf("insufficient byte capacity error=%v", err)
	}
	statFSAvailable = func(string) (int64, int64, uint64, error) { return 1024 * 1024 * 1024, 1, 1, nil }
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

func TestCheckRestoreCapacityChecksCacheDirectoryItself(t *testing.T) {
	oldStat := statFSAvailable
	defer func() { statFSAvailable = oldStat }()
	cacheDir := t.TempDir()
	treeDir := t.TempDir()
	var paths []string
	statFSAvailable = func(path string) (int64, int64, uint64, error) {
		paths = append(paths, path)
		return 1024 * 1024 * 1024, 10000, 1, nil
	}
	if err := checkRestoreCapacity(cacheDir, treeDir, 10, 10, 1, 1); err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 || paths[0] != cacheDir {
		t.Fatalf("cache filesystem checked at %v, want first path %q", paths, cacheDir)
	}
}

func TestCheckApplyCapacityBeforeFinalize(t *testing.T) {
	oldStat := statFSAvailable
	defer func() { statFSAvailable = oldStat }()
	cacheDir := t.TempDir()
	entry := TreeEntry{
		Path: "a", Type: "file", Size: 1024, Mode: 0o600, ModTimeNS: 1,
		Chunks: []ChunkDescriptor{{Hash: strings.Repeat("a", 64), Offset: 0, Size: 1024}},
	}
	preflight := &SnapshotManifest{Snapshot: Snapshot{Size: 1024}, Files: []TreeEntry{entry}}
	preflight.FileCount = 1

	statFSAvailable = func(string) (int64, int64, uint64, error) { return 1024 * 1024 * 1024, 10000, 1, nil }
	if err := checkApplyCapacityBeforeFinalize(preflight, nil, cacheDir); err != nil {
		t.Fatalf("adequate capacity rejected: %v", err)
	}
	statFSAvailable = func(string) (int64, int64, uint64, error) { return 1, 10000, 1, nil }
	if err := checkApplyCapacityBeforeFinalize(preflight, nil, cacheDir); err == nil || !strings.Contains(err.Error(), "insufficient free space") {
		t.Fatalf("insufficient capacity accepted: %v", err)
	}

	// A baseline that matches the target needs no apply bytes.
	previous := &SnapshotManifest{Files: []TreeEntry{entry}}
	previous.FileCount = 1
	statFSAvailable = func(string) (int64, int64, uint64, error) { return restoreDiskSafetyMargin + 1, 10000, 1, nil }
	if err := checkApplyCapacityBeforeFinalize(preflight, previous, cacheDir); err != nil {
		t.Fatalf("reusable baseline rejected: %v", err)
	}
}
