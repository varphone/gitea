// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build linux

package replication

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

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

func openRegularFileBeneath(root, relative string) (*os.File, os.FileInfo, error) {
	if err := validateTreePath(relative); err != nil {
		return nil, nil, err
	}
	root, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return nil, nil, fmt.Errorf("resolve chunk source root %q: %w", root, err)
	}
	rootFD, err := openDirectoryPathNoSymlinks(root)
	if err != nil {
		return nil, nil, fmt.Errorf("open chunk source root %q: %w", root, err)
	}
	defer unix.Close(rootFD)
	flags := uint64(unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NONBLOCK)
	fd, err := unix.Openat2(rootFD, relative, &unix.OpenHow{
		Flags:   flags,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS,
	})
	if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EPERM) {
		currentFD := rootFD
		parts := strings.Split(relative, "/")
		for _, part := range parts[:len(parts)-1] {
			// Resolve each parent from the opened directory to prevent symlink escapes.
			nextFD, openErr := unix.Openat(currentFD, part, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_DIRECTORY, 0)
			if openErr != nil {
				if currentFD != rootFD {
					_ = unix.Close(currentFD)
				}
				return nil, nil, fmt.Errorf("open chunk source directory %q: %w", part, openErr)
			}
			if currentFD != rootFD {
				_ = unix.Close(currentFD)
			}
			currentFD = nextFD
		}
		fd, err = unix.Openat(currentFD, parts[len(parts)-1], unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
		if currentFD != rootFD {
			_ = unix.Close(currentFD)
		}
	}
	if err != nil {
		return nil, nil, fmt.Errorf("open chunk source file %q: %w", relative, err)
	}
	file := os.NewFile(uintptr(fd), relative)
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, nil, errors.New("chunk source is not a regular file")
	}
	return file, info, nil
}

func openDirectoryPathNoSymlinks(path string) (int, error) {
	flags := uint64(unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NONBLOCK | unix.O_DIRECTORY)
	fd, err := unix.Openat2(unix.AT_FDCWD, path, &unix.OpenHow{
		Flags:   flags,
		Resolve: unix.RESOLVE_NO_SYMLINKS,
	})
	if err == nil || (!errors.Is(err, unix.ENOSYS) && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.EPERM)) {
		return fd, err
	}
	fd, err = unix.Open(string(filepath.Separator), unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NONBLOCK|unix.O_DIRECTORY, 0)
	if err != nil {
		return -1, err
	}
	for part := range strings.SplitSeq(strings.TrimPrefix(path, string(filepath.Separator)), string(filepath.Separator)) {
		if part == "" {
			continue
		}
		nextFD, err := unix.Openat(fd, part, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_DIRECTORY, 0)
		if err != nil {
			_ = unix.Close(fd)
			return -1, fmt.Errorf("open directory %q: %w", part, err)
		}
		_ = unix.Close(fd)
		fd = nextFD
	}
	return fd, nil
}

func openDirectory(path string, expected os.FileInfo) (*os.File, error) {
	if expected == nil || !expected.IsDir() {
		return nil, errors.New("file is not a directory")
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_DIRECTORY, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
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
