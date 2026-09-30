// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build !linux

package replication

import "os"

func fileHasMultipleLinks(os.FileInfo) bool { return false }
