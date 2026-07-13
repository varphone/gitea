// Copyright 2024 The Gitea Authors. All rights reserved.
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

	"code.gitea.io/gitea/modules/setting"
)

func installPreparedSnapshot(ctx context.Context, stage string, snapshot *Snapshot, cfg *config) error {
	root := filepath.Clean(setting.AppWorkPath)
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
		if stageOwned {
			_ = os.RemoveAll(stage)
		}
	}()
	if snapshot.RootMode == 0 || snapshot.RootMode > 0o777 {
		return fmt.Errorf("invalid APP_WORK_PATH mode %#o", snapshot.RootMode)
	}
	if err := os.Chmod(stage, os.FileMode(snapshot.RootMode)); err != nil {
		return fmt.Errorf("restore APP_WORK_PATH mode: %w", err)
	}
	if err := syncTree(ctx, stage); err != nil {
		return fmt.Errorf("persist extracted snapshot: %w", err)
	}
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
	fence, err := AcquireSnapshotFence(taskCtx)
	if err != nil {
		return fmt.Errorf("acquire standby write fence: %w", err)
	}
	defer func() { _ = fence.Release() }()
	wasActive := systemctl(taskCtx, "is-active", cfg.GiteaServiceName) == nil
	if err := systemctl(taskCtx, "stop", cfg.GiteaServiceName); err != nil {
		return fmt.Errorf("stop standby gitea: %w", err)
	}
	activated := false
	rollback := func(cause error) error {
		var rollbackErrors []error
		if err := systemctlWithTimeout(cfg.ServiceTimeout, "stop", cfg.GiteaServiceName); err != nil {
			return errors.Join(cause, fmt.Errorf("cannot safely stop failed restored service: %w", err))
		}
		if activated {
			if err := atomicSwitchWithTimeout(cfg.ServiceTimeout, cfg); err != nil {
				rollbackErrors = append(rollbackErrors, fmt.Errorf("atomically restore previous data: %w", err))
			} else {
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
			if err := systemctlWithTimeout(cfg.ServiceTimeout, "start", cfg.GiteaServiceName); err != nil {
				rollbackErrors = append(rollbackErrors, fmt.Errorf("restart previous gitea: %w", err))
			}
		}
		return errors.Join(append([]error{cause}, rollbackErrors...)...)
	}

	if err := atomicSwitchRunner(taskCtx, cfg); err != nil {
		if wasActive {
			if startErr := systemctlWithTimeout(cfg.ServiceTimeout, "start", cfg.GiteaServiceName); startErr != nil {
				return errors.Join(fmt.Errorf("atomically activate restored data: %w", err), fmt.Errorf("restart unchanged gitea: %w", startErr))
			}
		}
		return fmt.Errorf("atomically activate restored data: %w", err)
	}
	stageOwned = false
	activated = true

	if rel, err := filepath.Rel(root, setting.CustomConf); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		configPath := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
			return rollback(fmt.Errorf("create standby configuration directory: %w", err))
		}
		if err := writeFileSynced(configPath, localConfig, configInfo.Mode().Perm()); err != nil {
			return rollback(fmt.Errorf("preserve standby configuration: %w", err))
		}
	}

	executable, err := executablePath()
	if err != nil {
		return rollback(fmt.Errorf("locate gitea executable: %w", err))
	}
	if err := regenerateKeys(taskCtx, executable, setting.CustomConf); err != nil {
		return rollback(fmt.Errorf("regenerate authorized_keys: %w", err))
	}
	if err := systemctl(taskCtx, "start", cfg.GiteaServiceName); err != nil {
		return rollback(fmt.Errorf("start restored gitea: %w", err))
	}
	if err := readinessCheck(taskCtx, cfg.GiteaServiceName); err != nil {
		return rollback(fmt.Errorf("restored gitea failed readiness: %w", err))
	}
	if err := systemctl(taskCtx, "stop", cfg.GiteaServiceName); err != nil {
		return rollback(fmt.Errorf("stop verified standby gitea: %w", err))
	}

	// The new service is healthy. The restore process may still have its working
	// directory in the old root, which became stage after the atomic exchange.
	// Leave it before recursively removing that old tree.
	if err := leaveStageWorkingDirectory(stage); err != nil {
		return &cleanupWarning{err: err}
	}
	// Cleanup failure must not turn a successful activation into a retry loop; a
	// later maintenance job may remove the backup.
	if err := cleanupBackup(stage); err != nil {
		return &cleanupWarning{err: err}
	}
	if err := syncDirectory(cfg.SnapshotDir); err != nil {
		return &cleanupWarning{err: err}
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

func ensureSocketActivationDisabled(ctx context.Context, service string) error {
	socket := strings.TrimSuffix(service, ".service") + ".socket"
	if err := systemctl(ctx, "is-active", socket); err == nil {
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
		select {
		case <-ctx.Done():
			return errors.Join(lastErr, ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}
