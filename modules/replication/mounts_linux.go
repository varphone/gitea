// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build linux

package replication

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type mountInfoEntry struct {
	id         uint64
	mountPoint string
}

var mountPointDecoder = strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`).Replace

func validateNoNestedMounts(root string) error {
	absolute, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return err
	}
	entries, err := readMountInfo()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.mountPoint != absolute && isWithin(absolute, entry.mountPoint) {
			return fmt.Errorf("directory tree %q contains nested mount point %q; replication requires one mount tree", absolute, entry.mountPoint)
		}
	}
	return nil
}

func readMountInfo() ([]mountInfoEntry, error) {
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, fmt.Errorf("open /proc/self/mountinfo: %w", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	entries := make([]mountInfoEntry, 0, 64)
	for scanner.Scan() {
		line := scanner.Text()
		mountFields, _, found := strings.Cut(line, " - ")
		if !found {
			return nil, errors.New("malformed entry in /proc/self/mountinfo")
		}
		fields := strings.Fields(mountFields)
		if len(fields) < 5 {
			return nil, errors.New("malformed entry in /proc/self/mountinfo")
		}
		mountID, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse mount ID from /proc/self/mountinfo: %w", err)
		}
		mountPoint := mountPointDecoder(fields[4])
		if !filepath.IsAbs(mountPoint) {
			return nil, fmt.Errorf("mount point %q in /proc/self/mountinfo is not absolute", mountPoint)
		}
		entries = append(entries, mountInfoEntry{id: mountID, mountPoint: filepath.Clean(mountPoint)})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read /proc/self/mountinfo: %w", err)
	}
	return entries, nil
}
