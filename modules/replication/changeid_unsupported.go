// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build !linux

package replication

import (
	"fmt"
	"os"
)

func fileChangeID(info os.FileInfo) string {
	return fmt.Sprintf("%d:%d:%d", info.Size(), info.Mode(), info.ModTime().UnixNano())
}
