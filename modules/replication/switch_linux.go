// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build linux

package replication

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gitea.dev/modules/log"
	"gitea.dev/modules/setting"

	"golang.org/x/sys/unix"
)

const atomicSwitchServiceName = "gitea-replication-switch.service"

type mountInfoEntry struct {
	id         uint64
	mountPoint string
}

var mountPointDecoder = strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`).Replace

func installStagePath(cfg *config) string {
	return filepath.Join(cfg.SnapshotDir, ".install-stage")
}

func directoryIdentity(path string) (uint64, uint64, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return 0, 0, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return 0, 0, fmt.Errorf("%q is not a real directory", path)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, fmt.Errorf("cannot inspect filesystem identity for %q", path)
	}
	return stat.Dev, stat.Ino, nil
}

func pathHasDirectoryIdentity(path string, device, inode uint64) bool {
	actualDevice, actualInode, err := directoryIdentity(path)
	return err == nil && actualDevice == device && actualInode == inode
}

func validateSwitchFilesystem(root, stageParent string) error {
	rootInfo, err := os.Stat(root)
	if err != nil {
		return err
	}
	stageInfo, err := os.Stat(stageParent)
	if err != nil {
		return err
	}
	rootStat, rootOK := rootInfo.Sys().(*syscall.Stat_t)
	stageStat, stageOK := stageInfo.Sys().(*syscall.Stat_t)
	if !rootOK || !stageOK || rootStat.Dev != stageStat.Dev {
		return errors.New("APP_WORK_PATH and SNAPSHOT_DIR must be on the same filesystem for atomic exchange")
	}
	rootMountID, err := pathMountID(root)
	if err != nil {
		return fmt.Errorf("inspect APP_WORK_PATH mount: %w", err)
	}
	rootParentMountID, err := pathMountID(filepath.Dir(root))
	if err != nil {
		return fmt.Errorf("inspect APP_WORK_PATH parent mount: %w", err)
	}
	stageMountID, err := pathMountID(stageParent)
	if err != nil {
		return fmt.Errorf("inspect SNAPSHOT_DIR mount: %w", err)
	}
	if rootMountID != rootParentMountID {
		return errors.New("APP_WORK_PATH must not be a mount point for atomic exchange")
	}
	if rootParentMountID != stageMountID {
		return errors.New("APP_WORK_PATH parent and SNAPSHOT_DIR must be on the same mount for atomic exchange")
	}
	return nil
}

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

func validateExchangeTreeMounts(root, stage string) error {
	if err := validateNoNestedMounts(root); err != nil {
		return err
	}
	if err := validateNoNestedMounts(stage); err != nil {
		return fmt.Errorf("validate install stage mounts: %w", err)
	}
	stageMountID, err := pathMountID(stage)
	if err != nil {
		return fmt.Errorf("inspect install stage mount: %w", err)
	}
	stageParentMountID, err := pathMountID(filepath.Dir(stage))
	if err != nil {
		return fmt.Errorf("inspect install stage parent mount: %w", err)
	}
	if stageMountID != stageParentMountID {
		return errors.New("install stage must not be a mount point for atomic exchange")
	}
	return nil
}

// pathMountID falls back to mountinfo on kernels without STATX_MNT_ID.
func pathMountID(path string) (uint64, error) {
	var info unix.Statx_t
	statxErr := unix.Statx(unix.AT_FDCWD, path, 0, unix.STATX_MNT_ID, &info)
	if statxErr == nil && info.Mask&unix.STATX_MNT_ID != 0 {
		return info.Mnt_id, nil
	}
	if statxErr != nil && !errors.Is(statxErr, unix.ENOSYS) && !errors.Is(statxErr, unix.EINVAL) && !errors.Is(statxErr, unix.EOPNOTSUPP) {
		return 0, statxErr
	}
	mountID, err := pathMountIDFromProc(path)
	if err != nil {
		return 0, errors.Join(statxErr, err)
	}
	return mountID, nil
}

func pathMountIDFromProc(path string) (uint64, error) {
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return 0, err
	}
	entries, err := readMountInfo()
	if err != nil {
		return 0, err
	}
	bestLength := -1
	var bestID uint64
	var bestMountPoint string
	for _, entry := range entries {
		if !isWithin(entry.mountPoint, absolute) {
			continue
		}
		if len(entry.mountPoint) > bestLength {
			bestLength, bestID, bestMountPoint = len(entry.mountPoint), entry.id, entry.mountPoint
		} else if entry.mountPoint == bestMountPoint && entry.id != bestID {
			return 0, fmt.Errorf("mount point %q is ambiguous in /proc/self/mountinfo", entry.mountPoint)
		}
	}
	if bestLength < 0 {
		return 0, fmt.Errorf("no mount point found for %q in /proc/self/mountinfo", absolute)
	}
	return bestID, nil
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

func validateAtomicExchangeSupport(dir string) (retErr error) {
	first, err := os.MkdirTemp(dir, ".replication-exchange-probe-")
	if err != nil {
		return fmt.Errorf("create atomic exchange probe directory: %w", err)
	}
	defer func() {
		if err := os.Remove(first); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("remove atomic exchange probe directory: %w", err))
		}
	}()
	second, err := os.MkdirTemp(dir, ".replication-exchange-probe-")
	if err != nil {
		return fmt.Errorf("create second atomic exchange probe directory: %w", err)
	}
	defer func() {
		if err := os.Remove(second); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("remove second atomic exchange probe directory: %w", err))
		}
	}()
	if err := unix.Renameat2(unix.AT_FDCWD, first, unix.AT_FDCWD, second, unix.RENAME_EXCHANGE); err != nil {
		return fmt.Errorf("SNAPSHOT_DIR filesystem does not support atomic directory exchange: %w", err)
	}
	return nil
}

func exchangeDirectories(root, stage string) error {
	for name, path := range map[string]string{"APP_WORK_PATH": root, "install stage": stage} {
		info, err := os.Lstat(path)
		if err != nil {
			return fmt.Errorf("stat %s: %w", name, err)
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%s must be a real directory", name)
		}
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil || filepath.Clean(resolved) != filepath.Clean(path) {
			return fmt.Errorf("%s must not contain symlink components", name)
		}
	}
	if err := validateExchangeTreeMounts(root, stage); err != nil {
		return err
	}
	if err := validateSwitchFilesystem(root, filepath.Dir(stage)); err != nil {
		return err
	}
	if err := unix.Renameat2(unix.AT_FDCWD, root, unix.AT_FDCWD, stage, unix.RENAME_EXCHANGE); err != nil {
		return fmt.Errorf("atomically exchange APP_WORK_PATH and install stage: %w", err)
	}
	return errors.Join(syncDirectory(filepath.Dir(root)), syncDirectory(filepath.Dir(stage)))
}

// SwitchDataRoot is invoked only by the root-owned atomic switch systemd unit.
func SwitchDataRoot() (retErr error) {
	started := time.Now()
	log.Info("Starting atomic standby data exchange")
	defer func() {
		if retErr != nil {
			log.Error("Atomic standby data exchange failed: duration=%s error=%v", time.Since(started), retErr)
			return
		}
		log.Info("Atomic standby data exchange completed: duration=%s", time.Since(started))
	}()
	if os.Geteuid() != 0 {
		return errors.New("atomic data switch must run as root")
	}
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if !cfg.Enabled || cfg.Mode != modeReplica {
		return errors.New("atomic data switch requires MODE=replica")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	active, err := systemctlUnitActive(ctx, cfg.GiteaServiceName)
	if err != nil {
		return err
	}
	if active {
		return fmt.Errorf("%s must be stopped before atomic data exchange", cfg.GiteaServiceName)
	}
	if err := ensureSocketActivationDisabled(ctx, cfg.GiteaServiceName); err != nil {
		return err
	}
	root := filepath.Clean(setting.AppWorkPath)
	stage := installStagePath(cfg)
	if err := validateAtomicLayout(cfg.SnapshotDir); err != nil {
		return err
	}
	return exchangeDirectories(root, stage)
}
