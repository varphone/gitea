// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build linux

package replication

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func openRegularFile(path string, expected os.FileInfo) (*os.File, os.FileInfo, error) {
	if expected == nil || !expected.Mode().IsRegular() {
		return nil, nil, errors.New("file is not regular")
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	if !info.Mode().IsRegular() || !os.SameFile(expected, info) {
		_ = file.Close()
		return nil, nil, errors.New("file changed while opening")
	}
	return file, info, nil
}
