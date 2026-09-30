// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build !linux

package replication

import (
	"errors"
	"os"
)

func cloneFileData(_, _ *os.File) error {
	return errors.New("file reflink is unavailable")
}
