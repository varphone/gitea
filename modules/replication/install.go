// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"gitea.dev/modules/log"
	"gitea.dev/modules/setting"
)

func installPreparedSnapshot(ctx context.Context, stage string, snapshot *Snapshot, cfg *config) error {
	return installPreparedSnapshotWithVerifier(ctx, stage, snapshot, cfg, nil)
}

func installPreparedSnapshotWithVerifier(ctx context.Context, stage string, snapshot *Snapshot, cfg *config, verifyRestored func() error) error {
	return installPreparedSnapshotWithVerifierAndHooks(ctx, stage, snapshot, cfg, verifyRestored, snapshotInstallHooks{})
}

type snapshotInstallHooks struct {
	afterStageSync      func() error
	beforeStandbyStart  func() error
	beforeReadiness     func() error
	afterStandbyStopped func() error
	requireCheckpoint   bool
}

func resetStageReadinessCheckpointAfterRollback(cfg *config, snapshot *Snapshot) error {
	if err := setStageCheckpointStandbyReady(cfg, snapshot, false, false); err == nil {
		return nil
	} else {
		resetErr := err
		path := stageCheckpointPath(cfg)
		checkpoint, inspectErr := readStageCheckpoint(path)
		if os.IsNotExist(inspectErr) {
			return nil
		} else if inspectErr != nil {
			return errors.Join(resetErr, fmt.Errorf("inspect standby install checkpoint after reset failure: %w", inspectErr))
		} else if checkpoint.SnapshotID != snapshot.ID || checkpoint.ManifestSHA != snapshot.SHA256 {
			return errors.Join(resetErr, errors.New("standby install checkpoint changed during rollback"))
		}
		return errors.Join(resetErr, errors.New("standby install checkpoint retained for recovery because readiness reset was not confirmed"))
	}
}

type standbyConfigSnapshot struct {
	data         []byte
	info         os.FileInfo
	isSymlink    bool
	linkTarget   string
	resolvedPath string
}

func inspectStandbyConfig(path string, captureData bool) (*standbyConfigSnapshot, error) {
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect standby configuration path: %w", err)
	}
	snapshot := &standbyConfigSnapshot{isSymlink: pathInfo.Mode()&os.ModeSymlink != 0}
	if snapshot.isSymlink {
		snapshot.linkTarget, err = os.Readlink(path)
		if err != nil {
			return nil, fmt.Errorf("read standby configuration symlink: %w", err)
		}
	}
	snapshot.resolvedPath, err = resolvedPath(path)
	if err != nil {
		return nil, fmt.Errorf("resolve standby configuration: %w", err)
	}
	targetInfo, err := os.Lstat(snapshot.resolvedPath)
	if err != nil {
		return nil, fmt.Errorf("inspect standby configuration target: %w", err)
	}
	file, openedInfo, err := openRegularFile(snapshot.resolvedPath, targetInfo)
	if err != nil {
		return nil, fmt.Errorf("open standby configuration: %w", err)
	}
	defer file.Close()
	if openedInfo.Size() < 0 {
		return nil, errors.New("standby configuration has an invalid size")
	}
	if captureData {
		root, err := filepath.Abs(filepath.Clean(setting.AppWorkPath))
		if err != nil {
			return nil, fmt.Errorf("resolve APP_WORK_PATH while preserving standby configuration: %w", err)
		}
		if isWithin(root, snapshot.resolvedPath) {
			snapshot.data, err = io.ReadAll(io.LimitReader(file, openedInfo.Size()))
			if err != nil {
				return nil, fmt.Errorf("read standby configuration: %w", err)
			}
			if int64(len(snapshot.data)) != openedInfo.Size() {
				return nil, errors.New("standby configuration changed while reading")
			}
		}
	}
	afterFileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("restat standby configuration: %w", err)
	}
	afterTargetInfo, err := os.Lstat(snapshot.resolvedPath)
	if err != nil {
		return nil, fmt.Errorf("reinspect standby configuration target: %w", err)
	}
	afterPathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("reinspect standby configuration path: %w", err)
	}
	if !sameStableRegularFile(openedInfo, afterFileInfo) || !sameStableRegularFile(openedInfo, afterTargetInfo) {
		return nil, errors.New("standby configuration changed while reading")
	}
	if snapshot.isSymlink {
		afterLinkTarget, err := os.Readlink(path)
		if err != nil {
			return nil, fmt.Errorf("reread standby configuration symlink: %w", err)
		}
		afterResolvedPath, err := resolvedPath(path)
		if err != nil {
			return nil, fmt.Errorf("reresolve standby configuration symlink: %w", err)
		}
		if afterPathInfo.Mode()&os.ModeSymlink == 0 || !os.SameFile(pathInfo, afterPathInfo) ||
			afterLinkTarget != snapshot.linkTarget || filepath.Clean(afterResolvedPath) != snapshot.resolvedPath {
			return nil, errors.New("standby configuration symlink changed while reading")
		}
	} else {
		afterResolvedPath, err := resolvedPath(path)
		if err != nil {
			return nil, fmt.Errorf("reresolve standby configuration: %w", err)
		}
		if !sameStableRegularFile(pathInfo, afterPathInfo) || filepath.Clean(afterResolvedPath) != snapshot.resolvedPath {
			return nil, errors.New("standby configuration path changed while reading")
		}
	}
	snapshot.info = openedInfo
	return snapshot, nil
}

func sameStableRegularFile(before, after os.FileInfo) bool {
	if before == nil || after == nil || !before.Mode().IsRegular() || !after.Mode().IsRegular() ||
		!os.SameFile(before, after) || before.Size() != after.Size() ||
		before.Mode().Perm() != after.Mode().Perm() || !before.ModTime().Equal(after.ModTime()) {
		return false
	}
	return fileChangeID(before) == fileChangeID(after)
}

func installPreparedSnapshotWithVerifierAndHooks(ctx context.Context, stage string, snapshot *Snapshot, cfg *config, verifyRestored func() error, hooks snapshotInstallHooks) error {
	installStarted := time.Now()
	log.Info("Validating standby snapshot for installation: snapshot=%s bytes=%d stage=%s", snapshot.ID, snapshot.Size, filepath.Base(stage))
	root, err := filepath.Abs(filepath.Clean(setting.AppWorkPath))
	if err != nil {
		return fmt.Errorf("resolve APP_WORK_PATH: %w", err)
	}
	stageInfo, err := os.Lstat(stage)
	if err != nil {
		return fmt.Errorf("stat install stage: %w", err)
	}
	if !stageInfo.IsDir() || stageInfo.Mode()&os.ModeSymlink != 0 {
		return errors.New("install stage must be a real directory")
	}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return fmt.Errorf("stat APP_WORK_PATH: %w", err)
	}
	if !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return errors.New("APP_WORK_PATH must be a real directory")
	}
	var standbyConfig *standbyConfigSnapshot
	if _, err := inspectStandbyConfig(setting.CustomConf, false); err != nil {
		return err
	}
	stageOwned := true
	defer func() {
		if !stageOwned {
			return
		}
		if _, err := os.Lstat(stage); err != nil {
			if !os.IsNotExist(err) {
				log.Warn("Inspect failed standby install stage: snapshot=%s stage=%s error=%v", snapshot.ID, filepath.Base(stage), err)
			}
			return
		}
		cleanupErr := errors.Join(makeTreeRemovable(context.Background(), stage), os.RemoveAll(stage))
		if cleanupErr != nil {
			log.Warn("Remove failed standby install stage: snapshot=%s stage=%s error=%v", snapshot.ID, filepath.Base(stage), cleanupErr)
		}
	}()
	if snapshot.RootMode == 0 || snapshot.RootMode > 0o777 {
		return fmt.Errorf("invalid APP_WORK_PATH mode %#o", snapshot.RootMode)
	}
	if err := os.Chmod(stage, os.FileMode(snapshot.RootMode)); err != nil {
		return fmt.Errorf("restore APP_WORK_PATH mode: %w", err)
	}
	syncStarted := time.Now()
	if err := syncTree(ctx, stage); err != nil {
		log.Error("Persist extracted standby snapshot failed: snapshot=%s duration=%s error=%v", snapshot.ID, time.Since(syncStarted), err)
		return fmt.Errorf("persist extracted snapshot: %w", err)
	}
	log.Info("Persisted extracted standby snapshot: snapshot=%s duration=%s", snapshot.ID, time.Since(syncStarted))
	if hooks.afterStageSync != nil {
		if err := hooks.afterStageSync(); err != nil {
			return fmt.Errorf("renew final session after durable standby staging: %w", err)
		}
	}
	dbRel, err := filepath.Rel(root, setting.Database.Path)
	if err != nil || dbRel == ".." || strings.HasPrefix(dbRel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("invalid restored database path %q", setting.Database.Path)
	}
	if _, err := os.Stat(filepath.Join(stage, dbRel)); err != nil {
		return fmt.Errorf("restored SQLite database is missing: %w", err)
	}

	preparationCtx, cancelPreparation := context.WithTimeout(ctx, cfg.ServiceTimeout)
	defer cancelPreparation()
	if err := ensureSocketActivationDisabled(preparationCtx, cfg.GiteaServiceName); err != nil {
		return err
	}
	fenceStarted := time.Now()
	log.Debug("Waiting for standby replication write fence: snapshot=%s", snapshot.ID)
	fence, err := acquireSnapshotFence(preparationCtx)
	if err != nil {
		log.Error("Acquire standby replication write fence failed: snapshot=%s duration=%s error=%v", snapshot.ID, time.Since(fenceStarted), err)
		return fmt.Errorf("acquire standby write fence: %w", err)
	}
	log.Info("Acquired standby replication write fence: snapshot=%s wait=%s", snapshot.ID, time.Since(fenceStarted))
	defer func() {
		if err := fence.Release(); err != nil {
			log.Error("Release standby replication write fence failed: snapshot=%s error=%v", snapshot.ID, err)
		}
	}()
	wasActive, err := systemctlUnitActive(preparationCtx, cfg.GiteaServiceName)
	if err != nil {
		return err
	}
	if err := setStageCheckpointStandbyWasActive(cfg, snapshot, wasActive, hooks.requireCheckpoint); err != nil {
		return fmt.Errorf("persist previous standby service state: %w", err)
	}
	cancelPreparation()
	taskCtx, cancelTask := context.WithTimeout(ctx, cfg.ServiceTimeout)
	defer cancelTask()
	stopStarted := time.Now()
	if err := systemctl(taskCtx, "stop", cfg.GiteaServiceName); err != nil {
		log.Error("Stop standby service failed: snapshot=%s duration=%s error=%v", snapshot.ID, time.Since(stopStarted), err)
		stopErr := fmt.Errorf("stop standby gitea: %w", err)
		if wasActive {
			if restartErr := ensureStandbyService(context.Background(), cfg); restartErr != nil {
				log.Error("Restart standby service after stop failure failed: snapshot=%s error=%v", snapshot.ID, restartErr)
				return errors.Join(stopErr, fmt.Errorf("restart standby after stop failure: %w", restartErr))
			}
			log.Info("Ensured standby service is started after stop failure: snapshot=%s", snapshot.ID)
		}
		return stopErr
	}
	log.Info("Stopped standby service for snapshot installation: snapshot=%s was_active=%t duration=%s", snapshot.ID, wasActive, time.Since(stopStarted))
	standbyConfig, err = inspectStandbyConfig(setting.CustomConf, true)
	if err != nil {
		refreshErr := fmt.Errorf("refresh standby configuration after stopping Gitea: %w", err)
		if wasActive {
			if restartErr := ensureStandbyService(context.Background(), cfg); restartErr != nil {
				log.Error("Restart standby service after configuration refresh failure failed: snapshot=%s error=%v", snapshot.ID, restartErr)
				return errors.Join(refreshErr, fmt.Errorf("restart standby after configuration refresh failure: %w", restartErr))
			}
			log.Info("Restarted standby service after configuration refresh failure: snapshot=%s", snapshot.ID)
		}
		return refreshErr
	}
	activated := false
	primaryReleaseAttempted := false
	var executable string
	keysMayHaveChanged := false
	rollback := func(cause error) error {
		if primaryReleaseAttempted {
			log.Error("Standby rollback skipped after primary release request; primary may have resumed, leaving standby service stopped: snapshot=%s cause=%v", snapshot.ID, cause)
			return errors.Join(cause, errors.New("primary release was attempted; standby rollback could risk serving concurrently"))
		}
		log.Warn("Rolling back standby snapshot installation: snapshot=%s activated=%t cause=%v", snapshot.ID, activated, cause)
		var rollbackErrors []error
		if err := systemctlWithTimeout(cfg.ServiceTimeout, "stop", cfg.GiteaServiceName); err != nil {
			log.Error("Rollback cannot stop standby service: snapshot=%s error=%v", snapshot.ID, err)
			return errors.Join(cause, fmt.Errorf("cannot safely stop failed restored service: %w", err))
		}
		rollbackAttempted := activated
		rollbackExchangeDurable := !rollbackAttempted
		failedStagePreserved := false
		if rollbackAttempted {
			if err := setStageCheckpointRollbackKeysPending(cfg, snapshot, true, hooks.requireCheckpoint); err != nil {
				log.Error("Cannot persist rollback key recovery checkpoint: snapshot=%s error=%v", snapshot.ID, err)
				rollbackErrors = append(rollbackErrors, fmt.Errorf("persist rollback key recovery checkpoint: %w", err))
			} else {
				rollbackStarted := time.Now()
				if err := atomicSwitchWithTimeout(context.Background(), cfg.ServiceTimeout, cfg); err != nil {
					log.Error("Rollback data exchange failed: snapshot=%s duration=%s error=%v", snapshot.ID, time.Since(rollbackStarted), err)
					rollbackErrors = append(rollbackErrors, fmt.Errorf("atomically restore previous data: %w", err))
				} else {
					rollbackExchangeDurable = true
					log.Info("Rollback data exchange completed: snapshot=%s duration=%s", snapshot.ID, time.Since(rollbackStarted))
				}
			}
		}
		rootAfter, rootErr := os.Lstat(root)
		previousRootActive := rootErr == nil && rootAfter.IsDir() && rootAfter.Mode()&os.ModeSymlink == 0 && os.SameFile(rootAfter, rootInfo)
		if rootErr != nil {
			rollbackErrors = append(rollbackErrors, fmt.Errorf("inspect standby data root after rollback: %w", rootErr))
		}
		readinessCheckpointReset := true
		if err := resetStageReadinessCheckpointAfterRollback(cfg, snapshot); err != nil {
			readinessCheckpointReset = false
			log.Error("Cannot reset standby readiness checkpoint after rollback: snapshot=%s error=%v", snapshot.ID, err)
			rollbackErrors = append(rollbackErrors, fmt.Errorf("reset standby readiness checkpoint after rollback: %w", err))
		}
		if !previousRootActive {
			stateErr := errors.New("cannot verify that previous standby data is active after rollback")
			log.Error("Rollback could not verify previous standby data; leaving service stopped: snapshot=%s error=%v", snapshot.ID, stateErr)
			rollbackErrors = append(rollbackErrors, stateErr)
		} else if rollbackAttempted {
			if !rollbackExchangeDurable {
				syncErr := errors.Join(syncDirectory(filepath.Dir(root)), syncDirectory(filepath.Dir(stage)))
				if syncErr != nil {
					log.Error("Cannot confirm rollback data exchange durability; leaving standby service stopped: snapshot=%s error=%v", snapshot.ID, syncErr)
					rollbackErrors = append(rollbackErrors, fmt.Errorf("confirm rollback data exchange durability: %w", syncErr))
				} else {
					rollbackExchangeDurable = true
					log.Warn("Rollback helper reported failure, but retrying parent directory sync confirmed durability: snapshot=%s", snapshot.ID)
				}
			}
			if rollbackExchangeDurable {
				log.Info("Verified previous standby data after failed activation: snapshot=%s", snapshot.ID)
				activated = false
				stageAfter, stageErr := os.Lstat(stage)
				if stageErr == nil && stageAfter.IsDir() && stageAfter.Mode()&os.ModeSymlink == 0 && os.SameFile(stageAfter, stageInfo) {
					failed := filepath.Join(cfg.SnapshotDir, ".failed-"+snapshot.ID)
					if _, err := os.Lstat(failed); err == nil {
						failed += "-" + time.Now().UTC().Format("20060102T150405.000000000Z")
					}
					if err := os.Rename(stage, failed); err != nil {
						log.Error("Preserve failed standby restore failed: snapshot=%s error=%v", snapshot.ID, err)
						rollbackErrors = append(rollbackErrors, fmt.Errorf("preserve failed restore: %w", err))
					} else {
						stageOwned = false
						if err := syncDirectory(cfg.SnapshotDir); err != nil {
							log.Error("Persist failed standby restore stage failed: snapshot=%s error=%v", snapshot.ID, err)
							rollbackErrors = append(rollbackErrors, fmt.Errorf("persist failed restore stage: %w", err))
						} else {
							failedStagePreserved = true
						}
					}
				} else {
					if stageErr == nil {
						stageErr = errors.New("install stage no longer contains the failed snapshot")
					}
					log.Warn("Cannot preserve failed standby restore stage: snapshot=%s error=%v", snapshot.ID, stageErr)
					rollbackErrors = append(rollbackErrors, fmt.Errorf("preserve failed restore: %w", stageErr))
				}
			}
		}
		keysRestored := true
		if previousRootActive && keysMayHaveChanged {
			keysStarted := time.Now()
			keysCtx, keysCancel := context.WithTimeout(context.Background(), cfg.ServiceTimeout)
			keysErr := regenerateKeys(keysCtx, executable, setting.CustomConf)
			keysCancel()
			if keysErr != nil {
				keysRestored = false
				log.Error("Restore previous standby authorized keys failed: snapshot=%s duration=%s error=%v", snapshot.ID, time.Since(keysStarted), keysErr)
				rollbackErrors = append(rollbackErrors, fmt.Errorf("restore previous authorized_keys: %w", keysErr))
			} else {
				keysMayHaveChanged = false
				log.Info("Restored previous standby authorized keys: snapshot=%s duration=%s", snapshot.ID, time.Since(keysStarted))
			}
		}
		if previousRootActive && keysRestored {
			if err := setStageCheckpointRollbackKeysPending(cfg, snapshot, false, hooks.requireCheckpoint); err != nil {
				keysRestored = false
				log.Error("Cannot clear standby rollback key recovery checkpoint: snapshot=%s error=%v", snapshot.ID, err)
				rollbackErrors = append(rollbackErrors, fmt.Errorf("clear standby rollback key recovery checkpoint: %w", err))
			}
		}
		if wasActive {
			if !previousRootActive {
				log.Error("Previous standby service remains stopped because its data root is unverified: snapshot=%s", snapshot.ID)
			} else if !rollbackExchangeDurable {
				log.Error("Previous standby service remains stopped because rollback durability is unconfirmed: snapshot=%s", snapshot.ID)
			} else if !readinessCheckpointReset {
				log.Error("Previous standby service remains stopped because its readiness checkpoint is ambiguous: snapshot=%s", snapshot.ID)
			} else if !keysRestored {
				log.Error("Previous standby service remains stopped because its authorized keys were not restored: snapshot=%s", snapshot.ID)
			} else {
				restartStarted := time.Now()
				if err := ensureStandbyService(context.Background(), cfg); err != nil {
					log.Error("Rollback could not restart previous standby service: snapshot=%s duration=%s error=%v", snapshot.ID, time.Since(restartStarted), err)
					rollbackErrors = append(rollbackErrors, fmt.Errorf("restart previous gitea: %w", err))
				} else {
					log.Info("Restarted previous standby service after rollback: snapshot=%s duration=%s", snapshot.ID, time.Since(restartStarted))
				}
			}
		}
		if failedStagePreserved {
			if err := pruneFailedRestoreStages(cfg.SnapshotDir, 1); err != nil {
				log.Warn("Prune older failed standby restore stages failed: snapshot=%s error=%v", snapshot.ID, err)
				rollbackErrors = append(rollbackErrors, fmt.Errorf("prune old failed restore stages: %w", err))
			}
		}
		return errors.Join(append([]error{cause}, rollbackErrors...)...)
	}

	if err := setStageCheckpointSwitchPrepared(cfg, snapshot, root, stage, hooks.requireCheckpoint); err != nil {
		return rollback(fmt.Errorf("persist standby install switch checkpoint: %w", err))
	}
	switchStarted := time.Now()
	if err := atomicSwitchWithTimeout(taskCtx, cfg.ServiceTimeout, cfg); err != nil {
		log.Error("Atomic standby data exchange failed: snapshot=%s duration=%s error=%v", snapshot.ID, time.Since(switchStarted), err)
		rootAfter, rootErr := os.Lstat(root)
		stageAfter, stageErr := os.Lstat(stage)
		rootIsOriginal := rootErr == nil && rootAfter.IsDir() && rootAfter.Mode()&os.ModeSymlink == 0 && os.SameFile(rootAfter, rootInfo)
		rootIsStage := rootErr == nil && rootAfter.IsDir() && rootAfter.Mode()&os.ModeSymlink == 0 && os.SameFile(rootAfter, stageInfo)
		stageIsOriginalRoot := stageErr == nil && stageAfter.IsDir() && stageAfter.Mode()&os.ModeSymlink == 0 && os.SameFile(stageAfter, rootInfo)
		stageIsOriginalStage := stageErr == nil && stageAfter.IsDir() && stageAfter.Mode()&os.ModeSymlink == 0 && os.SameFile(stageAfter, stageInfo)
		if rootIsStage && stageIsOriginalRoot {
			stageOwned = false
			activated = true
			return rollback(fmt.Errorf("atomic data exchange completed but its service reported an error: %w", err))
		}
		if rootIsOriginal {
			if !stageIsOriginalStage {
				stageOwned = false
			}
			if wasActive {
				if startErr := ensureStandbyService(context.Background(), cfg); startErr != nil {
					return errors.Join(fmt.Errorf("atomically activate restored data: %w", err), fmt.Errorf("restart unchanged gitea: %w", startErr))
				}
			}
			return fmt.Errorf("atomically activate restored data: %w", err)
		}
		stageOwned = false
		stateErr := errors.Join(rootErr, stageErr, errors.New("cannot verify active data root after atomic exchange; preserving install stage and leaving service stopped"))
		log.Error("Cannot verify standby data after failed atomic exchange; leaving service stopped: snapshot=%s error=%v", snapshot.ID, stateErr)
		return errors.Join(fmt.Errorf("atomically activate restored data: %w", err), stateErr)
	}
	stageOwned = false
	activated = true
	log.Info("Atomically activated standby snapshot: snapshot=%s duration=%s", snapshot.ID, time.Since(switchStarted))

	if err := preserveLocalStandbyConfiguration(root, setting.CustomConf, standbyConfig.resolvedPath, standbyConfig.data, standbyConfig.info, standbyConfig.isSymlink, standbyConfig.linkTarget); err != nil {
		return rollback(fmt.Errorf("preserve standby configuration: %w", err))
	}
	if configPath, err := filepath.Abs(filepath.Clean(setting.CustomConf)); err == nil &&
		(isWithin(root, configPath) || isWithin(root, standbyConfig.resolvedPath)) {
		log.Debug("Preserved local standby configuration: snapshot=%s symlink=%t target_inside_root=%t", snapshot.ID, standbyConfig.isSymlink, isWithin(root, standbyConfig.resolvedPath))
	}

	executable, err = executablePath()
	if err != nil {
		return rollback(fmt.Errorf("locate gitea executable: %w", err))
	}
	keysStarted := time.Now()
	keysMayHaveChanged = true
	if err := regenerateKeys(taskCtx, executable, setting.CustomConf); err != nil {
		log.Error("Regenerate standby authorized keys failed: snapshot=%s duration=%s error=%v", snapshot.ID, time.Since(keysStarted), err)
		return rollback(fmt.Errorf("regenerate authorized_keys: %w", err))
	}
	log.Info("Regenerated standby authorized keys: snapshot=%s duration=%s", snapshot.ID, time.Since(keysStarted))
	if hooks.beforeStandbyStart != nil {
		if err := hooks.beforeStandbyStart(); err != nil {
			return rollback(fmt.Errorf("renew final session before starting restored standby: %w", err))
		}
	}
	startStarted := time.Now()
	if err := systemctl(taskCtx, "start", cfg.GiteaServiceName); err != nil {
		log.Error("Start restored standby service failed: snapshot=%s duration=%s error=%v", snapshot.ID, time.Since(startStarted), err)
		return rollback(fmt.Errorf("start restored gitea: %w", err))
	}
	log.Info("Started restored standby service: snapshot=%s duration=%s", snapshot.ID, time.Since(startStarted))
	if hooks.beforeReadiness != nil {
		if err := hooks.beforeReadiness(); err != nil {
			return rollback(fmt.Errorf("renew final session before standby readiness check: %w", err))
		}
	}
	readinessStarted := time.Now()
	if err := readinessCheck(taskCtx, cfg.GiteaServiceName); err != nil {
		log.Error("Restored standby readiness check failed: snapshot=%s duration=%s error=%v", snapshot.ID, time.Since(readinessStarted), err)
		return rollback(fmt.Errorf("restored gitea failed readiness: %w", err))
	}
	log.Info("Restored standby passed readiness check: snapshot=%s duration=%s", snapshot.ID, time.Since(readinessStarted))
	verifiedStopStarted := time.Now()
	if err := systemctl(taskCtx, "stop", cfg.GiteaServiceName); err != nil {
		log.Error("Stop verified standby service failed: snapshot=%s duration=%s error=%v", snapshot.ID, time.Since(verifiedStopStarted), err)
		return rollback(fmt.Errorf("stop verified standby gitea: %w", err))
	}
	log.Info("Stopped verified standby service: snapshot=%s duration=%s", snapshot.ID, time.Since(verifiedStopStarted))
	if verifyRestored != nil {
		verifyStarted := time.Now()
		if err := verifyRestored(); err != nil {
			log.Error("Verify restored standby files after readiness failed: snapshot=%s duration=%s error=%v", snapshot.ID, time.Since(verifyStarted), err)
			return rollback(fmt.Errorf("verify restored standby files after readiness: %w", err))
		}
		log.Info("Verified restored standby files after readiness: snapshot=%s duration=%s", snapshot.ID, time.Since(verifyStarted))
	}
	if err := setStageCheckpointStandbyReady(cfg, snapshot, true, hooks.requireCheckpoint); err != nil {
		return rollback(fmt.Errorf("persist standby readiness checkpoint: %w", err))
	}
	if hooks.afterStandbyStopped != nil {
		// A lost response can follow a successful release, so rollback must treat the request as ambiguous.
		primaryReleaseAttempted = true
		if err := hooks.afterStandbyStopped(); err != nil {
			return rollback(fmt.Errorf("release primary after standby service stopped: %w", err))
		}
	}

	// The new service is healthy. The restore process may still have its working
	// directory in the old root, which became stage after the atomic exchange.
	// Leave it before recursively removing that old tree.
	if err := leaveStageWorkingDirectory(stage); err != nil {
		return &cleanupWarning{err: err}
	}
	// Cleanup failure must not turn a successful activation into a retry loop; a
	// later maintenance job may remove the backup.
	cleanupStarted := time.Now()
	makeRemovableErr := makeTreeRemovable(context.Background(), stage)
	if err := cleanupBackup(stage); err != nil {
		cleanupErr := errors.Join(makeRemovableErr, err)
		log.Warn("Remove previous standby data backup failed: snapshot=%s duration=%s error=%v", snapshot.ID, time.Since(cleanupStarted), cleanupErr)
		return &cleanupWarning{err: cleanupErr}
	}
	if err := syncDirectory(cfg.SnapshotDir); err != nil {
		return &cleanupWarning{err: err}
	}
	log.Info("Standby snapshot installation finished: snapshot=%s duration=%s", snapshot.ID, time.Since(installStarted))
	return nil
}

func preserveLocalStandbyConfiguration(root, configPath, resolvedConfigPath string, data []byte, info os.FileInfo, configIsSymlink bool, linkTarget string) error {
	configPath, err := filepath.Abs(filepath.Clean(configPath))
	if err != nil {
		return err
	}
	configPathInsideRoot := isWithin(root, configPath)
	targetPathInsideRoot := isWithin(root, resolvedConfigPath)
	if !configPathInsideRoot && !targetPathInsideRoot {
		return nil
	}
	var targetPath string
	if targetPathInsideRoot {
		targetRel, err := filepath.Rel(root, resolvedConfigPath)
		if err != nil || targetRel == "." || targetRel == ".." || strings.HasPrefix(targetRel, ".."+string(filepath.Separator)) {
			return fmt.Errorf("invalid resolved configuration path %q", resolvedConfigPath)
		}
		targetPath = filepath.Join(root, targetRel)
		if err := ensureRealDirectoryPath(root, filepath.Dir(targetPath)); err != nil {
			return fmt.Errorf("create standby configuration target directory: %w", err)
		}
		if err := writeFileSynced(targetPath, data, info.Mode().Perm()); err != nil {
			return err
		}
	}
	if !configIsSymlink || !configPathInsideRoot {
		return nil
	}
	linkRel, err := filepath.Rel(root, configPath)
	if err != nil || linkRel == "." || linkRel == ".." || strings.HasPrefix(linkRel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("invalid standby configuration symlink path %q", configPath)
	}
	linkPath := filepath.Join(root, linkRel)
	if targetPath != "" && linkPath == targetPath {
		return errors.New("standby configuration symlink resolves to its own path")
	}
	if err := ensureRealDirectoryPath(root, filepath.Dir(linkPath)); err != nil {
		return fmt.Errorf("create standby configuration symlink directory: %w", err)
	}
	if existing, err := os.Lstat(linkPath); err == nil {
		if existing.IsDir() && existing.Mode()&os.ModeSymlink == 0 {
			return fmt.Errorf("standby configuration symlink path %q is occupied by a directory", linkRel)
		}
		if !existing.Mode().IsRegular() && existing.Mode()&os.ModeSymlink == 0 {
			return fmt.Errorf("standby configuration symlink path %q is occupied by an unsupported entry", linkRel)
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(linkPath), ".replication-config-link-*")
	if err != nil {
		return fmt.Errorf("create temporary standby configuration symlink: %w", err)
	}
	tempPath := temp.Name()
	if err := temp.Close(); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("close temporary standby configuration symlink: %w", err)
	}
	if err := os.Remove(tempPath); err != nil {
		return fmt.Errorf("prepare temporary standby configuration symlink: %w", err)
	}
	defer os.Remove(tempPath)
	if err := os.Symlink(linkTarget, tempPath); err != nil {
		return fmt.Errorf("create temporary standby configuration symlink: %w", err)
	}
	if err := os.Rename(tempPath, linkPath); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(linkPath))
}

func ensureRealDirectoryPath(root, path string) error {
	root, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return err
	}
	path, err = filepath.Abs(filepath.Clean(path))
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("directory path %q is outside APP_WORK_PATH %q", path, root)
	}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("APP_WORK_PATH %q must be a real directory", root)
	}
	if rel == "." {
		return nil
	}
	current := root
	for part := range strings.SplitSeq(rel, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if os.IsNotExist(err) {
			if err := os.Mkdir(current, 0o700); err != nil {
				return err
			}
			if err := syncDirectory(filepath.Dir(current)); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("path component %q is not a real directory", current)
		}
	}
	return nil
}

func leaveStageWorkingDirectory(stage string) error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(stage, cwd)
	if err != nil || (rel != "." && (rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)))) {
		return nil
	}
	return os.Chdir(filepath.Dir(stage))
}

func pruneFailedRestoreStages(snapshotDir string, keep int) error {
	entries, err := os.ReadDir(snapshotDir)
	if err != nil {
		return err
	}
	var stages []string
	for _, entry := range entries {
		failedID, ok := strings.CutPrefix(entry.Name(), ".failed-")
		if !ok {
			continue
		}
		if strings.Contains(failedID, "-") {
			failedID, _, _ = strings.Cut(failedID, "-")
		}
		if !validSnapshotID(failedID) {
			continue
		}
		path := filepath.Join(snapshotDir, entry.Name())
		info, err := os.Lstat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
			stages = append(stages, path)
		}
	}
	if keep < 0 {
		keep = 0
	}
	removeCount := len(stages) - keep
	if removeCount <= 0 {
		return nil
	}
	var cleanupErrors []error
	removed := 0
	for _, stage := range stages[:removeCount] {
		makeRemovableErr := makeTreeRemovable(context.Background(), stage)
		removeErr := os.RemoveAll(stage)
		if removeErr != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("remove failed standby restore stage %q: %w", filepath.Base(stage), errors.Join(makeRemovableErr, removeErr)))
			continue
		}
		removed++
	}
	if removed > 0 {
		if err := syncDirectory(snapshotDir); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("sync failed restore stage cleanup: %w", err))
		}
		log.Info("Pruned failed standby restore stages: removed=%d retained=%d", removed, len(stages)-removed)
	}
	return errors.Join(cleanupErrors...)
}

type cleanupWarning struct{ err error }

func (e *cleanupWarning) Error() string {
	return "restored Gitea is healthy but old data cleanup failed: " + e.err.Error()
}
func (e *cleanupWarning) Unwrap() error { return e.err }

var systemctlRunner = func(ctx context.Context, action, service string) error {
	output, err := exec.CommandContext(ctx, "systemctl", action, service).CombinedOutput()
	if err == nil {
		return nil
	}
	message := strings.TrimSpace(string(output))
	if message == "" {
		return err
	}
	return fmt.Errorf("%w: %s", err, message)
}

var executablePath = os.Executable

var regenerateKeys = func(ctx context.Context, executable, config string) error {
	return exec.CommandContext(ctx, executable, "admin", "regenerate", "keys", "--config", config).Run()
}

var readinessCheck = waitForGitea

var cleanupBackup = os.RemoveAll

var atomicSwitchRunner = func(ctx context.Context, _ *config) error {
	return systemctl(ctx, "start", atomicSwitchServiceName)
}

func atomicSwitchWithTimeout(parent context.Context, timeout time.Duration, cfg *config) error {
	if err := parent.Err(); err != nil {
		return err
	}
	// Wait past the helper deadlines before deciding whether the exchange succeeded.
	timeout = max(timeout, 45*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return atomicSwitchRunner(ctx, cfg)
}

func systemctl(ctx context.Context, action, service string) error {
	return systemctlRunner(ctx, action, service)
}

var errSystemctlUnitInactive = errors.New("inactive")

func systemctlUnitActive(ctx context.Context, service string) (bool, error) {
	err := systemctl(ctx, "is-active", service)
	if err == nil {
		return true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		switch exitErr.ExitCode() {
		case 3:
			return false, nil
		case 4:
			if strings.HasSuffix(service, ".socket") {
				return false, nil
			}
		}
		return false, fmt.Errorf("query systemd state for %s: %w", service, err)
	}
	if errors.Is(err, errSystemctlUnitInactive) {
		return false, nil
	}
	return false, fmt.Errorf("query systemd state for %s: %w", service, err)
}

func ensureSocketActivationDisabled(ctx context.Context, service string) error {
	socket := strings.TrimSuffix(service, ".service") + ".socket"
	active, err := systemctlUnitActive(ctx, socket)
	if err != nil {
		return err
	}
	if active {
		return fmt.Errorf("socket activation unit %s must be disabled for consistent snapshots", socket)
	}
	return nil
}

func systemctlWithTimeout(timeout time.Duration, action, service string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return systemctl(ctx, action, service)
}

func waitForGitea(ctx context.Context, service string) error {
	started := time.Now()
	nextProgressLog := started.Add(10 * time.Second)
	client := &http.Client{Timeout: 2 * time.Second, CheckRedirect: checkReplicationRedirect}
	healthURL := strings.TrimRight(setting.LocalURL, "/") + "/api/healthz"
	checkHTTPHealth := setting.Protocol == setting.HTTP || setting.Protocol == setting.HTTPS
	var lastErr error
	for {
		if err := systemctl(ctx, "is-active", service); err == nil {
			if !checkHTTPHealth {
				return nil // Unix/FCGI deployments rely on systemd active state.
			}
			req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
			if reqErr == nil {
				resp, err := client.Do(req)
				if err == nil {
					_, drainErr := io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
					_ = resp.Body.Close()
					if resp.StatusCode == http.StatusOK {
						return nil
					}
					lastErr = fmt.Errorf("health endpoint returned %s", resp.Status)
					if drainErr != nil {
						lastErr = errors.Join(lastErr, fmt.Errorf("read health response body: %w", drainErr))
					}
				} else {
					lastErr = err
				}
			} else {
				lastErr = reqErr
			}
		} else {
			lastErr = err
		}
		if now := time.Now(); !now.Before(nextProgressLog) {
			log.Info("Waiting for Gitea readiness: service=%s elapsed=%s last_error=%v", service, now.Sub(started), lastErr)
			nextProgressLog = now.Add(15 * time.Second)
		}
		select {
		case <-ctx.Done():
			waitErr := errors.Join(lastErr, ctx.Err())
			log.Error("Gitea readiness check ended: service=%s elapsed=%s error=%v", service, time.Since(started), waitErr)
			return waitErr
		case <-time.After(250 * time.Millisecond):
		}
	}
}
