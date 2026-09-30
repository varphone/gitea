// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"context"
	"errors"
	"fmt"
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
	installStarted := time.Now()
	log.Info("Validating standby snapshot for installation: snapshot=%s bytes=%d stage=%s", snapshot.ID, snapshot.Size, filepath.Base(stage))
	root := filepath.Clean(setting.AppWorkPath)
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
	localConfig, err := os.ReadFile(setting.CustomConf)
	if err != nil {
		return fmt.Errorf("read standby configuration: %w", err)
	}
	configInfo, err := os.Stat(setting.CustomConf)
	if err != nil {
		return fmt.Errorf("stat standby configuration: %w", err)
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
	dbRel, err := filepath.Rel(root, setting.Database.Path)
	if err != nil || dbRel == ".." || strings.HasPrefix(dbRel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("invalid restored database path %q", setting.Database.Path)
	}
	if _, err := os.Stat(filepath.Join(stage, dbRel)); err != nil {
		return fmt.Errorf("restored SQLite database is missing: %w", err)
	}

	taskCtx, cancel := context.WithTimeout(ctx, cfg.ServiceTimeout)
	defer cancel()
	if err := ensureSocketActivationDisabled(taskCtx, cfg.GiteaServiceName); err != nil {
		return err
	}
	fenceStarted := time.Now()
	log.Debug("Waiting for standby replication write fence: snapshot=%s", snapshot.ID)
	fence, err := acquireSnapshotFence(taskCtx)
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
	wasActive, err := systemctlUnitActive(taskCtx, cfg.GiteaServiceName)
	if err != nil {
		return err
	}
	stopStarted := time.Now()
	if err := systemctl(taskCtx, "stop", cfg.GiteaServiceName); err != nil {
		log.Error("Stop standby service failed: snapshot=%s duration=%s error=%v", snapshot.ID, time.Since(stopStarted), err)
		stopErr := fmt.Errorf("stop standby gitea: %w", err)
		if wasActive {
			if restartErr := systemctlWithTimeout(cfg.ServiceTimeout, "start", cfg.GiteaServiceName); restartErr != nil {
				log.Error("Restart standby service after stop failure failed: snapshot=%s error=%v", snapshot.ID, restartErr)
				return errors.Join(stopErr, fmt.Errorf("restart standby after stop failure: %w", restartErr))
			}
			log.Info("Ensured standby service is started after stop failure: snapshot=%s", snapshot.ID)
		}
		return stopErr
	}
	log.Info("Stopped standby service for snapshot installation: snapshot=%s duration=%s", snapshot.ID, time.Since(stopStarted))
	activated := false
	rollback := func(cause error) error {
		log.Warn("Rolling back standby snapshot installation: snapshot=%s activated=%t cause=%v", snapshot.ID, activated, cause)
		var rollbackErrors []error
		if err := systemctlWithTimeout(cfg.ServiceTimeout, "stop", cfg.GiteaServiceName); err != nil {
			log.Error("Rollback cannot stop standby service: snapshot=%s error=%v", snapshot.ID, err)
			return errors.Join(cause, fmt.Errorf("cannot safely stop failed restored service: %w", err))
		}
		if activated {
			rollbackStarted := time.Now()
			if err := atomicSwitchWithTimeout(cfg.ServiceTimeout, cfg); err != nil {
				log.Error("Rollback data exchange failed: snapshot=%s duration=%s error=%v", snapshot.ID, time.Since(rollbackStarted), err)
				rollbackErrors = append(rollbackErrors, fmt.Errorf("atomically restore previous data: %w", err))
			} else {
				log.Info("Restored previous standby data after failed activation: snapshot=%s duration=%s", snapshot.ID, time.Since(rollbackStarted))
				activated = false
				failed := filepath.Join(cfg.SnapshotDir, ".failed-"+snapshot.ID)
				if _, err := os.Lstat(failed); err == nil {
					failed += "-" + time.Now().UTC().Format("20060102T150405.000000000Z")
				}
				if err := os.Rename(stage, failed); err != nil {
					rollbackErrors = append(rollbackErrors, fmt.Errorf("preserve failed restore: %w", err))
				} else {
					stageOwned = false
				}
			}
		}
		if wasActive {
			restartStarted := time.Now()
			if err := systemctlWithTimeout(cfg.ServiceTimeout, "start", cfg.GiteaServiceName); err != nil {
				log.Error("Rollback could not restart previous standby service: snapshot=%s duration=%s error=%v", snapshot.ID, time.Since(restartStarted), err)
				rollbackErrors = append(rollbackErrors, fmt.Errorf("restart previous gitea: %w", err))
			} else {
				log.Info("Restarted previous standby service after rollback: snapshot=%s duration=%s", snapshot.ID, time.Since(restartStarted))
			}
		}
		return errors.Join(append([]error{cause}, rollbackErrors...)...)
	}

	switchStarted := time.Now()
	if err := atomicSwitchRunner(taskCtx, cfg); err != nil {
		log.Error("Atomic standby data exchange failed: snapshot=%s duration=%s error=%v", snapshot.ID, time.Since(switchStarted), err)
		rootAfter, rootErr := os.Lstat(root)
		stageAfter, stageErr := os.Lstat(stage)
		if rootErr == nil && stageErr == nil {
			switched := os.SameFile(rootAfter, stageInfo) && os.SameFile(stageAfter, rootInfo)
			unchanged := os.SameFile(rootAfter, rootInfo) && os.SameFile(stageAfter, stageInfo)
			if switched {
				stageOwned = false
				activated = true
				return rollback(fmt.Errorf("atomic data exchange completed but its service reported an error: %w", err))
			}
			if !unchanged {
				stageOwned = false
				stateErr := errors.New("cannot determine whether atomic data exchange completed; preserving install stage")
				if wasActive {
					if startErr := systemctlWithTimeout(cfg.ServiceTimeout, "start", cfg.GiteaServiceName); startErr != nil {
						stateErr = errors.Join(stateErr, fmt.Errorf("restart gitea after uncertain data exchange: %w", startErr))
					}
				}
				return errors.Join(fmt.Errorf("atomically activate restored data: %w", err), stateErr)
			}
		} else {
			stageOwned = false
			stateErr := errors.Join(rootErr, stageErr, errors.New("cannot determine whether atomic data exchange completed; preserving install stage"))
			if wasActive {
				if startErr := systemctlWithTimeout(cfg.ServiceTimeout, "start", cfg.GiteaServiceName); startErr != nil {
					stateErr = errors.Join(stateErr, fmt.Errorf("restart gitea after uncertain data exchange: %w", startErr))
				}
			}
			return errors.Join(fmt.Errorf("atomically activate restored data: %w", err), stateErr)
		}
		if wasActive {
			if startErr := systemctlWithTimeout(cfg.ServiceTimeout, "start", cfg.GiteaServiceName); startErr != nil {
				return errors.Join(fmt.Errorf("atomically activate restored data: %w", err), fmt.Errorf("restart unchanged gitea: %w", startErr))
			}
		}
		return fmt.Errorf("atomically activate restored data: %w", err)
	}
	stageOwned = false
	activated = true
	log.Info("Atomically activated standby snapshot: snapshot=%s duration=%s", snapshot.ID, time.Since(switchStarted))

	if rel, err := filepath.Rel(root, setting.CustomConf); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		configPath := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
			return rollback(fmt.Errorf("create standby configuration directory: %w", err))
		}
		if err := writeFileSynced(configPath, localConfig, configInfo.Mode().Perm()); err != nil {
			return rollback(fmt.Errorf("preserve standby configuration: %w", err))
		}
		log.Debug("Preserved local standby configuration: snapshot=%s", snapshot.ID)
	}

	executable, err := executablePath()
	if err != nil {
		return rollback(fmt.Errorf("locate gitea executable: %w", err))
	}
	keysStarted := time.Now()
	if err := regenerateKeys(taskCtx, executable, setting.CustomConf); err != nil {
		log.Error("Regenerate standby authorized keys failed: snapshot=%s duration=%s error=%v", snapshot.ID, time.Since(keysStarted), err)
		return rollback(fmt.Errorf("regenerate authorized_keys: %w", err))
	}
	log.Info("Regenerated standby authorized keys: snapshot=%s duration=%s", snapshot.ID, time.Since(keysStarted))
	startStarted := time.Now()
	if err := systemctl(taskCtx, "start", cfg.GiteaServiceName); err != nil {
		log.Error("Start restored standby service failed: snapshot=%s duration=%s error=%v", snapshot.ID, time.Since(startStarted), err)
		return rollback(fmt.Errorf("start restored gitea: %w", err))
	}
	log.Info("Started restored standby service: snapshot=%s duration=%s", snapshot.ID, time.Since(startStarted))
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

	// The new service is healthy. The restore process may still have its working
	// directory in the old root, which became stage after the atomic exchange.
	// Leave it before recursively removing that old tree.
	if err := leaveStageWorkingDirectory(stage); err != nil {
		return &cleanupWarning{err: err}
	}
	// Cleanup failure must not turn a successful activation into a retry loop; a
	// later maintenance job may remove the backup.
	cleanupStarted := time.Now()
	if err := cleanupBackup(stage); err != nil {
		log.Warn("Remove previous standby data backup failed: snapshot=%s duration=%s error=%v", snapshot.ID, time.Since(cleanupStarted), err)
		return &cleanupWarning{err: err}
	}
	if err := syncDirectory(cfg.SnapshotDir); err != nil {
		return &cleanupWarning{err: err}
	}
	log.Info("Standby snapshot installation finished: snapshot=%s duration=%s", snapshot.ID, time.Since(installStarted))
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

func atomicSwitchWithTimeout(timeout time.Duration, cfg *config) error {
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
	client := &http.Client{Timeout: 2 * time.Second}
	healthURL := strings.TrimRight(setting.LocalURL, "/") + "/api/healthz"
	var lastErr error
	for {
		if err := systemctl(ctx, "is-active", service); err == nil {
			req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
			if reqErr == nil && (strings.HasPrefix(healthURL, "http://") || strings.HasPrefix(healthURL, "https://")) {
				resp, err := client.Do(req)
				if err == nil {
					_ = resp.Body.Close()
					if resp.StatusCode == http.StatusOK {
						return nil
					}
					lastErr = fmt.Errorf("health endpoint returned %s", resp.Status)
				} else {
					lastErr = err
				}
			} else if reqErr == nil {
				return nil // Unix/FCGI deployments rely on systemd active state.
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
