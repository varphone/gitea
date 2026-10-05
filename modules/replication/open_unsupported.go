// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build !linux

package replication

import (
	"errors"
	"os"
	"path/filepath"
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

func openRegularFileBeneath(root, relative string) (*os.File, os.FileInfo, error) {
	if err := validateTreePath(relative); err != nil {
		return nil, nil, err
	}
	path := filepath.Join(root, filepath.FromSlash(relative))
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, nil, err
	}
	if !isWithin(root, resolved) {
		return nil, nil, errors.New("chunk source resolves outside data root")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, nil, err
	}
	return openRegularFile(path, info)
}

func openDirectory(path string, expected os.FileInfo) (*os.File, error) {
	if expected == nil || !expected.IsDir() {
		return nil, errors.New("file is not a directory")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if !info.IsDir() || !os.SameFile(expected, info) {
		_ = file.Close()
		return nil, errors.New("directory changed while opening")
	}
	return file, nil
}
