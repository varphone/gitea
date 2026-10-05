// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"gitea.dev/modules/log"
	"gitea.dev/modules/process"
	"gitea.dev/modules/setting"
)

var systemctlRunner = func(ctx context.Context, action, service string) error {
	output, err := process.CommandContext(ctx, "systemctl", action, service).CombinedOutput()
	if err == nil {
		return nil
	}
	message := strings.TrimSpace(string(output))
	if message == "" {
		return err
	}
	return fmt.Errorf("%w: %s", err, message)
}

var readinessCheck = waitForGitea

func systemctl(ctx context.Context, action, service string) error {
	return systemctlRunner(ctx, action, service)
}

var errSystemctlUnitInactive = errors.New("inactive")

func systemctlUnitActive(ctx context.Context, service string) (bool, error) {
	err := systemctl(ctx, "is-active", service)
	if err == nil {
		return true, nil
	}
	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
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
