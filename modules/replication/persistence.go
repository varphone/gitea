// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"gitea.dev/modules/log"
)

func syncTree(ctx context.Context, root string) error {
	started := time.Now()
	nextProgressLog := started.Add(30 * time.Second)
	type directoryMode struct {
		path string
		mode os.FileMode
		info os.FileInfo
	}
	var directories []directoryMode
	var entriesSeen int64
	// Staged files are synced as they are materialized; this pass persists directory entries and final modes.
	err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		entriesSeen++
		if info.IsDir() {
			directories = append(directories, directoryMode{path: path, mode: info.Mode().Perm(), info: info})
			if info.Mode().Perm() != 0o700 {
				if err := os.Chmod(path, 0o700); err != nil {
					return err
				}
			}
		}
		if entriesSeen&0x3ff == 0 {
			if now := time.Now(); !now.Before(nextProgressLog) {
				log.Info("Standby install tree persistence progress: phase=walk entries_seen=%d directories_seen=%d elapsed=%s", entriesSeen, len(directories), now.Sub(started))
				nextProgressLog = now.Add(30 * time.Second)
			}
		}
		return nil
	})
	var syncErrors []error
	if err != nil {
		syncErrors = append(syncErrors, err)
	}
	contextErrRecorded := errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
	log.Info("Persisting standby install directory entries: directories=%d walk_entries=%d walk_duration=%s", len(directories), entriesSeen, time.Since(started))
	var directoriesProcessed, directoriesSynced int
	// Restore changed modes even after cancellation so the staged tree remains resumable.
	for _, directory := range slices.Backward(directories) {
		if ctxErr := ctx.Err(); ctxErr != nil && !contextErrRecorded {
			syncErrors = append(syncErrors, ctxErr)
			contextErrRecorded = true
		}
		file, openErr := openDirectory(directory.path, directory.info)
		var directoryErrors []error
		if openErr != nil {
			directoryErrors = append(directoryErrors, fmt.Errorf("open directory %q: %w", directory.path, openErr))
		}
		if file != nil {
			if directory.mode != 0o700 {
				if chmodErr := file.Chmod(directory.mode); chmodErr != nil {
					directoryErrors = append(directoryErrors, fmt.Errorf("restore permissions on directory %q: %w", directory.path, chmodErr))
				}
			}
			if err := file.Sync(); err != nil {
				directoryErrors = append(directoryErrors, fmt.Errorf("sync directory %q: %w", directory.path, err))
			}
			if err := file.Close(); err != nil {
				directoryErrors = append(directoryErrors, fmt.Errorf("close directory %q: %w", directory.path, err))
			}
		}
		directoryErr := errors.Join(directoryErrors...)
		if directoryErr != nil {
			syncErrors = append(syncErrors, directoryErr)
		}
		directoriesProcessed++
		if directoryErr == nil {
			directoriesSynced++
		}
		if now := time.Now(); !now.Before(nextProgressLog) {
			log.Info("Standby install tree persistence progress: phase=directory_sync directories_processed=%d/%d directories_synced=%d elapsed=%s", directoriesProcessed, len(directories), directoriesSynced, now.Sub(started))
			nextProgressLog = now.Add(30 * time.Second)
		}
	}
	if ctxErr := ctx.Err(); ctxErr != nil && !contextErrRecorded {
		syncErrors = append(syncErrors, ctxErr)
	}
	result := errors.Join(syncErrors...)
	log.Info("Standby install tree persistence completed: directories=%d processed=%d synced=%d errors=%d duration=%s", len(directories), directoriesProcessed, directoriesSynced, len(syncErrors), time.Since(started))
	return result
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

func removeFileSynced(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func ensureSnapshotDirectoryTree(path string) error {
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return err
	}
	probe := absolute
	var missing []string
	for {
		info, err := os.Lstat(probe)
		if err == nil {
			if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("replication snapshot path component %q must be a real directory", probe)
			}
			resolved, err := resolvedPath(probe)
			if err != nil {
				return err
			}
			if resolved != probe {
				return errors.New("replication snapshot directory must not contain symlink components")
			}
			break
		}
		if !os.IsNotExist(err) {
			return err
		}
		parent := filepath.Dir(probe)
		if parent == probe {
			return err
		}
		missing = append(missing, probe)
		probe = parent
	}
	for _, directoryPath := range slices.Backward(missing) {
		if err := os.Mkdir(directoryPath, 0o700); err != nil && !os.IsExist(err) {
			return err
		}
		info, err := os.Lstat(directoryPath)
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("replication snapshot path component %q must be a real directory", directoryPath)
		}
		resolved, err := resolvedPath(directoryPath)
		if err != nil {
			return err
		}
		if resolved != directoryPath {
			return errors.New("replication snapshot directory must not contain symlink components")
		}
	}
	// Retry syncing existing ancestors too; a prior call may have failed after creating them.
	for directoryPath := absolute; ; directoryPath = filepath.Dir(directoryPath) {
		if err := syncDirectory(directoryPath); err != nil {
			return fmt.Errorf("persist replication snapshot directory path component %q: %w", directoryPath, err)
		}
		if parent := filepath.Dir(directoryPath); parent == directoryPath {
			break
		}
	}
	return nil
}

func ensurePrivateSnapshotDirectory(path string) error {
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return err
	}
	resolved, err := resolvedPath(absolute)
	if err != nil {
		return err
	}
	if resolved != absolute {
		return errors.New("replication snapshot directory must not contain symlink components")
	}
	info, err := os.Lstat(absolute)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("replication snapshot path must be a real directory")
	}
	if info.Mode().Perm() == 0o700 && info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) == 0 {
		return nil
	}
	directory, err := openDirectory(absolute, info)
	if err != nil {
		return fmt.Errorf("open replication snapshot directory: %w", err)
	}
	chmodErr := directory.Chmod(0o700)
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if err := errors.Join(chmodErr, syncErr, closeErr); err != nil {
		return fmt.Errorf("secure replication snapshot directory: %w", err)
	}
	log.Info("Restricted replication snapshot directory permissions: path=%s previous_mode=%s mode=0700", absolute, info.Mode().String())
	return nil
}

func linkFileSyncedAt(ctx context.Context, sourcePath, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if filepath.Dir(sourcePath) != dir {
		return errors.New("hard-linked replication manifests must share a directory")
	}
	sourceInfo, err := os.Lstat(sourcePath)
	if err != nil {
		return err
	}
	if !sourceInfo.Mode().IsRegular() {
		return errors.New("hard-linked replication manifest source must be a regular file")
	}
	temp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	if err := errors.Join(temp.Close(), os.Remove(tempPath)); err != nil {
		return err
	}
	if err := os.Link(sourcePath, tempPath); err != nil {
		return err
	}
	defer os.Remove(tempPath)
	linkedInfo, err := os.Lstat(tempPath)
	if err != nil {
		return err
	}
	if !linkedInfo.Mode().IsRegular() || !os.SameFile(sourceInfo, linkedInfo) {
		return errors.New("replication manifest changed while creating hard link")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	return syncDirectory(dir)
}
