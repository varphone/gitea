// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build !linux

package replication

import "errors"

func syncFilesystem(string) error {
	return errors.New("filesystem sync is not supported on this platform")
}

func canDeferTreeSync() bool { return false }
