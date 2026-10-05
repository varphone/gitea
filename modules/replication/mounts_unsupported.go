// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build !linux

package replication

import "errors"

func validateNoNestedMounts(string) error {
	return errors.New("nested mount validation is supported only on Linux")
}
