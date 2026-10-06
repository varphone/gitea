// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build linux

package replication

import (
	"os"

	"golang.org/x/sys/unix"
)

// syncFilesystem flushes every pending write on the filesystem that holds path. The
// standby data root lives on one dedicated filesystem, so a single call replaces the
// per-file and per-directory fsyncs of an in-place update.
func syncFilesystem(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return unix.Syncfs(int(dir.Fd()))
}

// canDeferTreeSync reports whether one filesystem flush can replace per-file syncs.
func canDeferTreeSync() bool { return true }
