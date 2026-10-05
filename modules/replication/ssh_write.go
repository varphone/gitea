// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"errors"

	"gitea.dev/modules/log"
)

var (
	// ErrReplicaReadOnly reports that a replica refused an SSH write.
	ErrReplicaReadOnly = errors.New("the disaster-recovery replica is read-only until it is promoted")
	// ErrPrimaryRecoveryPending reports that pending primary recovery blocks writes.
	ErrPrimaryRecoveryPending = errors.New("Gitea is temporarily read-only while primary recovery is pending")
	// ErrSnapshotFenceActive reports that an active snapshot fence blocks writes.
	ErrSnapshotFenceActive = errors.New("Gitea is temporarily read-only while a disaster-recovery snapshot is created")
)

// AuthorizeSSHWrite applies the replication write policy to an SSH write and
// returns a release function that is safe to call when no lease was acquired.
func AuthorizeSSHWrite() (release func(), err error) {
	readOnly, fencingEnabled, primaryRecoveryPending, err := WriteProtection()
	if err != nil {
		return nil, err
	}
	if readOnly {
		return nil, ErrReplicaReadOnly
	}
	if primaryRecoveryPending {
		return nil, ErrPrimaryRecoveryPending
	}
	if !fencingEnabled {
		return func() {}, nil
	}
	lease, ok, err := TryAcquireWriteLease()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrSnapshotFenceActive
	}
	return func() {
		if err := lease.Release(); err != nil {
			log.Error("Release replication SSH write lease failed: %v", err)
		}
	}, nil
}
