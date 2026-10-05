// Copyright 2024 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package cmd

import (
	"context"
	"os"

	"gitea.dev/modules/json"
	replication "gitea.dev/modules/replication"
	"gitea.dev/modules/setting"

	"github.com/urfave/cli/v3"
)

// CmdReplicate represents the available replicate sub-command.
func newReplicateCommand() *cli.Command {
	return &cli.Command{
		Name:        "replicate",
		Usage:       "Instance replication — keep a static standby of this Gitea instance",
		Description: `Replicate provides SQLite disaster recovery through a separate authenticated HTTP control service. A static standby prefetches content-defined chunks online, transfers only the final delta while the primary is stopped, and patches its data tree in place.`,

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
				Name:   "status",
				Usage:  "Show persisted snapshot state from the local control service",
				Action: runReplicateStatus,
			},
			{
				Name:   "restore",
				Usage:  "Incrementally synchronize and update the standby data tree in place",
				Action: runReplicateRestore,
			},
		},
	}
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
