// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build !linux

package replication

import (
	"errors"
	"os"
)

func openRegularFile(path string, expected os.FileInfo) (*os.File, os.FileInfo, error) {
	if expected == nil || !expected.Mode().IsRegular() {
		return nil, nil, errors.New("file is not regular")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
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
