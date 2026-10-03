// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build linux

package replication

import (
	"os"
	"strconv"
	"syscall"
)

func fileChangeID(info os.FileInfo) string {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return ""
	}
	var buffer [83]byte
	result := strconv.AppendUint(buffer[:0], stat.Dev, 10)
	result = append(result, ':')
	result = strconv.AppendUint(result, stat.Ino, 10)
	result = append(result, ':')
	result = strconv.AppendInt(result, stat.Ctim.Sec, 10)
	result = append(result, ':')
	result = strconv.AppendInt(result, stat.Ctim.Nsec, 10)
	return string(result)
}
