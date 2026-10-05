// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build !linux && !darwin

package replication

import (
	"os"
)

// Without a stable file identity, callers must rehash files instead of reusing old chunks.
func fileChangeID(_ os.FileInfo) string { return "" }
