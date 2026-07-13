// Copyright 2024 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

//go:build linux

package replication

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"code.gitea.io/gitea/modules/json"
	"code.gitea.io/gitea/modules/setting"
)

func TestIncrementalHTTPRoundTripAndSecondDelta(t *testing.T) {
	oldFencePath := writeFencePath
	oldWorkPath, oldVersion := setting.AppWorkPath, setting.AppVer
	oldDBPath, oldCustomConf := setting.Database.Path, setting.CustomConf
	oldSystemctl, oldReadiness := systemctlRunner, readinessCheck
	oldExecutable, oldRegenerate, oldCleanup, oldAtomicSwitch := executablePath, regenerateKeys, cleanupBackup, atomicSwitchRunner
	defer func() {
		writeFencePath = oldFencePath
		setting.AppWorkPath, setting.AppVer = oldWorkPath, oldVersion
		setting.Database.Path, setting.CustomConf = oldDBPath, oldCustomConf
		systemctlRunner, readinessCheck = oldSystemctl, oldReadiness
		executablePath, regenerateKeys, cleanupBackup, atomicSwitchRunner = oldExecutable, oldRegenerate, oldCleanup, oldAtomicSwitch
	}()

	parent := t.TempDir()
	standbyRoot := filepath.Join(parent, "current")
	sourceRoot := t.TempDir()
	setting.AppWorkPath, setting.AppVer = standbyRoot, "test-version"
	setting.Database.Path = filepath.Join(standbyRoot, "data", "gitea.db")
	setting.CustomConf = filepath.Join(standbyRoot, "custom", "conf", "app.ini")
	requireWriteFile(t, setting.Database.Path, "old-db")
	requireWriteFile(t, setting.CustomConf, "standby-config")
	requireWriteFile(t, filepath.Join(sourceRoot, "data", "gitea.db"), "primary-db-v1")
	requireWriteFile(t, filepath.Join(sourceRoot, "custom", "conf", "app.ini"), "primary-config")
	requireWriteFile(t, filepath.Join(sourceRoot, "repositories", "owner", "repo.git", "objects", "object"), "git-object")
	if err := os.WriteFile(filepath.Join(sourceRoot, "large-lfs"), deterministicBytes(2<<20), 0o600); err != nil {
		t.Fatal(err)
	}

	writeFencePath = filepath.Join(t.TempDir(), "write.lock")
	systemctlRunner = func(_ context.Context, action, service string) error {
		if action == "is-active" && service == "gitea.socket" {
			return errors.New("inactive")
		}
		return nil
	}
	readinessCheck = func(context.Context, string) error { return nil }
	executablePath = func() (string, error) { return "/fake/gitea", nil }
	regenerateKeys = func(context.Context, string, string) error { return nil }
	cleanupBackup = os.RemoveAll
	atomicSwitchRunner = func(_ context.Context, cfg *config) error {
		return exchangeDirectories(setting.AppWorkPath, installStagePath(cfg))
	}

	token := "01234567890123456789012345678901"
	source := &controlServer{cfg: &config{
		Mode: modePrimary, ControlToken: token, SnapshotDir: t.TempDir(), SnapshotRetention: 3,
		GiteaServiceName: "gitea.service", ServiceTimeout: 2 * time.Second, SnapshotTimeout: 10 * time.Second,
	}, jobs: map[string]*Snapshot{}, dataRoot: sourceRoot}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/replication/sync-jobs", source.auth(source.syncTasks))
	mux.HandleFunc("/api/v1/replication/sync-jobs/", source.auth(source.syncTask))
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()
	replica := &config{
		Mode: modeReplica, ControlToken: token, SnapshotDir: t.TempDir(),
		GiteaServiceName: "gitea.service", ServiceTimeout: 2 * time.Second, SnapshotTimeout: 10 * time.Second,
	}

	if err := restoreIncremental(context.Background(), replica, httpServer.URL, httpServer.Client()); err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, setting.Database.Path, "primary-db-v1")
	assertFileContent(t, setting.CustomConf, "standby-config")
	assertFileContent(t, filepath.Join(standbyRoot, "repositories", "owner", "repo.git", "objects", "object"), "git-object")

	time.Sleep(time.Millisecond)
	requireWriteFile(t, filepath.Join(sourceRoot, "data", "gitea.db"), "primary-db-v2")
	requireWriteFile(t, filepath.Join(sourceRoot, "lfs", "new-object"), "new-lfs")
	if err := os.RemoveAll(filepath.Join(sourceRoot, "repositories")); err != nil {
		t.Fatal(err)
	}
	if err := restoreIncremental(context.Background(), replica, httpServer.URL, httpServer.Client()); err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, setting.Database.Path, "primary-db-v2")
	assertFileContent(t, setting.CustomConf, "standby-config")
	assertFileContent(t, filepath.Join(standbyRoot, "lfs", "new-object"), "new-lfs")
	if _, err := os.Stat(filepath.Join(standbyRoot, "repositories")); !os.IsNotExist(err) {
		t.Fatalf("source deletion was not propagated: %v", err)
	}
	if _, err := os.Stat(filepath.Join(replica.SnapshotDir, ".chunks")); !os.IsNotExist(err) {
		t.Fatalf("verified chunk cache was not cleaned: %v", err)
	}
}

func TestIncrementalSnapshotServesTaskManifestFromMemory(t *testing.T) {
	root := t.TempDir()
	requireWriteFile(t, filepath.Join(root, "data"), "chunk data")
	manifest, err := scanIncrementalTree(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	manifest.ID = "20260101T000000.000000000Z"
	manifest.State = "transferring"

	server := &controlServer{
		cfg:           &config{SnapshotDir: t.TempDir()},
		taskManifests: map[string]*SnapshotManifest{manifest.ID: manifest},
		dataRoot:      root,
	}
	hash := manifest.Files[0].Chunks[0].Hash
	response := httptest.NewRecorder()
	server.syncSnapshot(response, httptest.NewRequest(http.MethodGet, "/api/v1/replication/sync-jobs/"+manifest.ID+"/chunks/"+hash, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("chunk status=%d body=%s", response.Code, response.Body.String())
	}
	if response.Body.String() != "chunk data" {
		t.Fatalf("chunk body=%q", response.Body.String())
	}
}

func assertFileContent(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil || string(data) != want {
		t.Fatalf("%s=%q err=%v want=%q", path, data, err, want)
	}
}

func TestPrimaryRecoveryRetriesAfterTransientFailure(t *testing.T) {
	oldSystemctl, oldReadiness := systemctlRunner, readinessCheck
	defer func() { systemctlRunner, readinessCheck = oldSystemctl, oldReadiness }()
	systemctlRunner = func(context.Context, string, string) error { return nil }
	recovered := make(chan struct{})
	attempts := 0
	readinessCheck = func(context.Context, string) error {
		attempts++
		if attempts == 1 {
			return errors.New("transient readiness failure")
		}
		close(recovered)
		return nil
	}
	server := &controlServer{cfg: &config{
		GiteaServiceName: "gitea.service", ServiceTimeout: time.Second, SnapshotTimeout: time.Second,
	}}
	server.recoverPrimary()
	select {
	case <-recovered:
	case <-time.After(time.Second):
		t.Fatal("primary recovery was not retried")
	}
}

func TestFinalScanDetectsSameSizeAndRestoredMtime(t *testing.T) {
	oldRoot, oldVersion, oldChunker := setting.AppWorkPath, setting.AppVer, chunkFileForManifest
	defer func() {
		setting.AppWorkPath, setting.AppVer, chunkFileForManifest = oldRoot, oldVersion, oldChunker
	}()
	root := t.TempDir()
	setting.AppWorkPath, setting.AppVer = root, "test"
	path := filepath.Join(root, "data")
	requireWriteFile(t, path, "aaaa")
	base, err := scanIncrementalTree(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	requireWriteFile(t, path, "bbbb")
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	calls := 0
	chunkFileForManifest = func(ctx context.Context, path string) ([]ChunkDescriptor, error) {
		calls++
		return splitFile(ctx, path)
	}
	final, err := scanIncrementalTreeWithBase(context.Background(), root, base)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("changed file was not rechunked, calls=%d", calls)
	}
	if base.Files[0].Chunks[0].Hash == final.Files[0].Chunks[0].Hash {
		t.Fatal("same-size content change was not detected")
	}
}

func TestIncrementalFinalizationOnlyRechunksChanges(t *testing.T) {
	oldFencePath := writeFencePath
	oldWorkPath, oldVersion := setting.AppWorkPath, setting.AppVer
	oldSystemctl, oldReadiness, oldChunker := systemctlRunner, readinessCheck, chunkFileForManifest
	defer func() {
		writeFencePath = oldFencePath
		setting.AppWorkPath, setting.AppVer = oldWorkPath, oldVersion
		systemctlRunner, readinessCheck, chunkFileForManifest = oldSystemctl, oldReadiness, oldChunker
	}()
	writeFencePath = filepath.Join(t.TempDir(), "write.lock")
	setting.AppWorkPath, setting.AppVer = t.TempDir(), "test-version"
	requireWriteFile(t, filepath.Join(setting.AppWorkPath, "data", "gitea.db"), "database")
	requireWriteFile(t, filepath.Join(setting.AppWorkPath, "repositories", "a.git", "objects", "x"), "object")
	snapshotDir := t.TempDir()
	token := "01234567890123456789012345678901"
	server := &controlServer{cfg: &config{
		Mode: modePrimary, ControlToken: token, SnapshotDir: snapshotDir, SnapshotRetention: 3,
		GiteaServiceName: "gitea.service", ServiceTimeout: time.Second, SnapshotTimeout: 5 * time.Second,
	}, jobs: map[string]*Snapshot{}}
	var actions []string
	systemctlRunner = func(_ context.Context, action, service string) error {
		actions = append(actions, action+":"+service)
		if action == "is-active" && service == "gitea.socket" {
			return errors.New("inactive")
		}
		return nil
	}
	readinessCheck = func(context.Context, string) error { return nil }
	chunkCalls := 0
	chunkFileForManifest = func(ctx context.Context, path string) ([]ChunkDescriptor, error) {
		chunkCalls++
		return splitFile(ctx, path)
	}

	preflightResponse := httptest.NewRecorder()
	server.preflight(preflightResponse, httptest.NewRequest(http.MethodPost, "preflight", nil))
	if preflightResponse.Code != http.StatusAccepted {
		t.Fatalf("preflight status=%d body=%s", preflightResponse.Code, preflightResponse.Body.String())
	}
	var preflightJob Snapshot
	if err := json.NewDecoder(preflightResponse.Body).Decode(&preflightJob); err != nil {
		t.Fatal(err)
	}
	preflight := awaitSnapshotManifestState(t, server, preflightJob.ID, "preflight")
	if chunkCalls != 2 {
		t.Fatalf("preflight chunk calls=%d want=2", chunkCalls)
	}

	time.Sleep(time.Millisecond)
	requireWriteFile(t, filepath.Join(setting.AppWorkPath, "data", "gitea.db"), "changed-database")
	finalResponse := httptest.NewRecorder()
	finalURL := "/v1/sync/finalize?base=" + preflight.ID
	server.finalize(finalResponse, httptest.NewRequest(http.MethodPost, finalURL, nil))
	if finalResponse.Code != http.StatusAccepted {
		t.Fatalf("finalize status=%d body=%s", finalResponse.Code, finalResponse.Body.String())
	}
	var finalJob Snapshot
	if err := json.NewDecoder(finalResponse.Body).Decode(&finalJob); err != nil {
		t.Fatal(err)
	}
	final := awaitSnapshotManifestState(t, server, finalJob.ID, "transferring")
	if chunkCalls != 3 {
		t.Fatalf("final chunk calls=%d want=3 (only changed SQLite file)", chunkCalls)
	}
	if !slices.Contains(actions, "stop:gitea.service") {
		t.Fatalf("actions=%v", actions)
	}

	completeResponse := httptest.NewRecorder()
	completePath := "/api/v1/replication/sync-jobs/" + final.ID + "/session/complete"
	server.syncSnapshot(completeResponse, httptest.NewRequest(http.MethodPost, completePath, nil))
	if completeResponse.Code != http.StatusOK {
		t.Fatalf("complete status=%d body=%s", completeResponse.Code, completeResponse.Body.String())
	}
	completeResponse = httptest.NewRecorder()
	server.syncSnapshot(completeResponse, httptest.NewRequest(http.MethodPost, completePath, nil))
	if completeResponse.Code != http.StatusOK {
		t.Fatalf("idempotent complete status=%d body=%s", completeResponse.Code, completeResponse.Body.String())
	}
	if !slices.Contains(actions, "start:gitea.service") {
		t.Fatalf("actions=%v", actions)
	}
	baseline, err := loadTrustedManifest(baselineManifestPath(snapshotDir), token, "ready")
	if err != nil || baseline.ID != final.ID || !baseline.FullScanAt.Equal(final.FullScanAt) {
		t.Fatalf("baseline=%+v err=%v", baseline, err)
	}
	archives, err := filepath.Glob(filepath.Join(snapshotDir, "*.tar.gz*"))
	if err != nil || len(archives) != 0 {
		t.Fatalf("primary archives=%v err=%v", archives, err)
	}
}

func TestWriteFenceCoordinatesSSHAndSnapshot(t *testing.T) {
	oldPath := writeFencePath
	writeFencePath = filepath.Join(t.TempDir(), "write.lock")
	defer func() { writeFencePath = oldPath }()

	lease, ok, err := TryAcquireWriteLease()
	if err != nil || !ok {
		t.Fatalf("acquire write lease: ok=%v err=%v", ok, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := AcquireSnapshotFence(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("exclusive fence with active writer returned %v", err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}

	fence, err := AcquireSnapshotFence(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if lease, ok, err := TryAcquireWriteLease(); err != nil || ok || lease != nil {
		t.Fatalf("write lease while fenced: lease=%v ok=%v err=%v", lease, ok, err)
	}
	if err := fence.Release(); err != nil {
		t.Fatal(err)
	}
	if lease, ok, err := TryAcquireWriteLease(); err != nil || !ok {
		t.Fatalf("write lease after release: ok=%v err=%v", ok, err)
	} else {
		_ = lease.Release()
	}
}
