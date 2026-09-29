// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
)

func syncTree(ctx context.Context, root string) error {
	type directoryMode struct {
		path string
		mode os.FileMode
	}
	var directories []directoryMode
	err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if info.IsDir() {
			directories = append(directories, directoryMode{path: path, mode: info.Mode().Perm()})
			return os.Chmod(path, 0o700)
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			if os.IsPermission(err) {
				// Files created by buildIncrementalStage are synced before close when
				// their restored mode prevents opening them for reading.
				return nil
			}
			return err
		}
		return errors.Join(file.Sync(), file.Close())
	})
	var syncErrors []error
	if err != nil {
		syncErrors = append(syncErrors, err)
	} else {
		for _, directory := range slices.Backward(directories) {
			if err := syncDirectory(directory.path); err != nil {
				syncErrors = append(syncErrors, err)
			}
		}
	}
	for _, directory := range slices.Backward(directories) {
		file, openErr := os.Open(directory.path)
		chmodErr := os.Chmod(directory.path, directory.mode)
		var syncErr, closeErr error
		if file != nil {
			syncErr = file.Sync()
			closeErr = file.Close()
		}
		syncErrors = append(syncErrors, errors.Join(openErr, chmodErr, syncErr, closeErr))
	}
	return errors.Join(syncErrors...)
}

func writeFileSynced(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmp := file.Name()
	defer os.Remove(tmp)
	if err := file.Chmod(mode); err != nil {
		_ = file.Close()
		return err
	}
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDirectory(dir)
}
