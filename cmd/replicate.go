// Copyright 2024 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package cmd

import (
	"context"
	"os"

	"code.gitea.io/gitea/modules/json"
	replication "code.gitea.io/gitea/modules/replication"
	"code.gitea.io/gitea/modules/setting"

	"github.com/urfave/cli/v3"
)

// CmdReplicate represents the available replicate sub-command.
func newReplicateCommand() *cli.Command {
	return &cli.Command{
		Name:        "replicate",
		Usage:       "Instance replication — create a warm standby of this Gitea instance",
		Description: `Replicate provides SQLite disaster recovery through a separate authenticated HTTP control service. A standby prefetches content-defined chunks online, transfers only the final delta while the primary is stopped, and atomically installs it.`,

		Commands: []*cli.Command{
			{
				Name:   "serve",
				Usage:  "Run the independent disaster-recovery HTTP control service",
				Action: runReplicateServe,
			},
			{
				Name: "ensure-primary", Hidden: true,
				Action: runReplicateEnsurePrimary,
			},
			{
				Name: "switch-data-root", Hidden: true,
				Action: runReplicateSwitchDataRoot,
			},
			{
				Name:   "status",
				Usage:  "Show persisted snapshot state from the local control service",
				Action: runReplicateStatus,
			},
			{
				Name:   "restore",
				Usage:  "Incrementally synchronize, verify, and atomically install the primary",
				Action: runReplicateRestore,
			},
		},
	}
}

func runReplicateSwitchDataRoot(ctx context.Context, c *cli.Command) error {
	setting.MustInstalled()
	setting.LoadSettings()
	return replication.SwitchDataRoot()
}

func runReplicateEnsurePrimary(ctx context.Context, c *cli.Command) error {
	setting.MustInstalled()
	setting.LoadSettings()
	return replication.EnsurePrimaryService(ctx)
}

func runReplicateStatus(ctx context.Context, c *cli.Command) error {
	setting.MustInstalled()
	setting.LoadSettings()
	snapshots, err := replication.ControlStatus(ctx)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(snapshots, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = os.Stdout.Write(data)
	return err
}

func runReplicateRestore(ctx context.Context, c *cli.Command) error {
	setting.MustInstalled()
	setting.LoadSettings()
	return replication.RestoreLatest(ctx)
}

func runReplicateServe(ctx context.Context, c *cli.Command) error {
	setting.MustInstalled()
	setting.LoadSettings()
	return replication.ServeControl(ctx)
}
