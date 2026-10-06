// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build linux

package replication

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSyncFilesystemFlushesTree(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncFilesystem(root); err != nil {
		t.Fatalf("syncFilesystem() error=%v", err)
	}
	if err := syncFilesystem(filepath.Join(root, "missing")); err == nil {
		t.Fatal("syncFilesystem() accepted a missing path")
	}
	if !canDeferTreeSync() {
		t.Fatal("deferred tree sync must be available on linux")
	}
}
