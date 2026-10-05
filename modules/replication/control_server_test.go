// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"bytes"
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitea.dev/modules/json"
	"gitea.dev/modules/setting"
)

func TestPreflightResumesVerifiedCheckpoint(t *testing.T) {
	oldRoot, oldVersion := setting.AppWorkPath, setting.AppVer
	defer func() { setting.AppWorkPath, setting.AppVer = oldRoot, oldVersion }()
	root := t.TempDir()
	setting.AppWorkPath, setting.AppVer = root, "test"
	if err := os.WriteFile(filepath.Join(root, "data"), []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	token := "01234567890123456789012345678901"
	manifest, err := scanIncrementalTree(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	manifest.ID = "20260101T000000.000000000Z"
	manifest.State = "preflight"
	manifest.CreatedAt = time.Unix(1, 0).UTC()
	manifest.InstanceFingerprint = instanceFingerprint(token)
	signTestManifest(t, manifest, token)
	snapshotDir := t.TempDir()
	if err := writeManifest(snapshotDir, manifest); err != nil {
		t.Fatal(err)
	}
	server := &controlServer{cfg: &config{Mode: modePrimary, ControlToken: token, SnapshotDir: snapshotDir}, jobs: map[string]*Snapshot{}}
	response := httptest.NewRecorder()
	server.preflight(response, httptest.NewRequest(http.MethodPost, "/v1/sync/preflight?resume="+manifest.ID, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("resume status=%d body=%s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), manifest.ID) {
		t.Fatalf("resume body does not contain checkpoint ID: %s", response.Body.String())
	}
}

func TestPreflightReusesCompletedCheckpoint(t *testing.T) {
	oldRoot, oldVersion := setting.AppWorkPath, setting.AppVer
	defer func() { setting.AppWorkPath, setting.AppVer = oldRoot, oldVersion }()
	root := t.TempDir()
	setting.AppWorkPath, setting.AppVer = root, "test"
	if err := os.WriteFile(filepath.Join(root, "data"), []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	token := "01234567890123456789012345678901"
	manifest, err := scanIncrementalTree(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	manifest.ID, manifest.State, manifest.CreatedAt = "20260101T000000.000000000Z", "preflight", time.Now().UTC()
	manifest.RequestID = "01234567890123456789012345678901"
	manifest.InstanceFingerprint = instanceFingerprint(token)
	signTestManifest(t, manifest, token)
	server := &controlServer{
		cfg:           &config{Mode: modePrimary, ControlToken: token, SnapshotDir: t.TempDir(), FullScanInterval: time.Hour},
		jobs:          map[string]*Snapshot{manifest.ID: {ID: manifest.ID, State: manifest.State, CreatedAt: manifest.CreatedAt, RequestID: manifest.RequestID}},
		taskManifests: map[string]*SnapshotManifest{manifest.ID: manifest},
	}
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, syncJobsPath, nil)
	request.Header.Set("Idempotency-Key", manifest.RequestID)
	server.preflight(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), manifest.ID) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if server.busy || len(server.jobs) != 1 {
		t.Fatalf("busy=%t jobs=%d", server.busy, len(server.jobs))
	}
}

func TestControlAuthenticationAndRouting(t *testing.T) {
	id := "20260101T000000.000000000Z"
	server := &controlServer{
		cfg:  &config{ControlToken: "01234567890123456789012345678901", SnapshotDir: t.TempDir()},
		jobs: map[string]*Snapshot{id: {ID: id, State: "creating", CreatedAt: time.Now()}},
	}
	handler := server.auth(server.syncTasks)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/replication/sync-jobs", nil)
	response := httptest.NewRecorder()
	handler(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d", response.Code)
	}
	request = httptest.NewRequest(http.MethodGet, "/api/v1/replication/sync-jobs", nil)
	request.Header.Set("Authorization", "Bearer "+server.cfg.ControlToken)
	response = httptest.NewRecorder()
	handler(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("authenticated status = %d", response.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "/api/v1/replication/sync-jobs/"+id+"/manifest", nil)
	response = httptest.NewRecorder()
	server.syncTask(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("non-ready manifest status = %d", response.Code)
	}
	request = httptest.NewRequest(http.MethodGet, "/api/v1/replication/sync-jobs/../../etc/passwd", nil)
	response = httptest.NewRecorder()
	server.syncTask(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("invalid snapshot ID status = %d", response.Code)
	}

	if got := manifestPath(server.cfg.SnapshotDir, id); filepath.Dir(got) != server.cfg.SnapshotDir {
		t.Fatalf("manifest escaped snapshot dir: %s", got)
	}
}

func TestReplicationHTTPServerSetsResponseWriteTimeout(t *testing.T) {
	cfg := &config{ControlListen: "127.0.0.1:3001", ControlWriteTimeout: 37 * time.Second}
	server := newReplicationHTTPServer(cfg, http.NotFoundHandler())
	if server.WriteTimeout != cfg.ControlWriteTimeout {
		t.Fatalf("WriteTimeout=%s want=%s", server.WriteTimeout, cfg.ControlWriteTimeout)
	}
}

func TestManifestServeConcurrencyIsBounded(t *testing.T) {
	server := &controlServer{}
	slots := make([]chan struct{}, 0, maxConcurrentManifestServes)
	for i := range maxConcurrentManifestServes {
		slot, ok := server.tryAcquireManifestSlot()
		if !ok {
			t.Fatalf("manifest slot %d was rejected", i)
		}
		slots = append(slots, slot)
	}
	if _, ok := server.tryAcquireManifestSlot(); ok {
		t.Fatal("manifest serve limit was not enforced")
	}
	<-slots[0]
	if _, ok := server.tryAcquireManifestSlot(); !ok {
		t.Fatal("released manifest slot was not reusable")
	}
}

func TestChunkServingContinuesAfterPrimaryRelease(t *testing.T) {
	oldRoot, oldVersion := setting.AppWorkPath, setting.AppVer
	t.Cleanup(func() { setting.AppWorkPath, setting.AppVer = oldRoot, oldVersion })
	root := t.TempDir()
	setting.AppWorkPath, setting.AppVer = root, "test"
	requireWriteFile(t, filepath.Join(root, "data"), "content")
	manifest, err := scanIncrementalTree(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	manifest.ID = "20260101T000000.000000000Z"
	manifest.State = "transferring"
	manifest.CreatedAt = time.Now().UTC()
	if len(manifest.Files) != 1 || len(manifest.Files[0].Chunks) != 1 {
		t.Fatalf("unexpected manifest shape: %d entries", len(manifest.Files))
	}
	id, hash := manifest.ID, manifest.Files[0].Chunks[0].Hash

	server := &controlServer{
		cfg:              &config{Mode: modePrimary, ControlToken: "token", SnapshotDir: t.TempDir()},
		jobs:             map[string]*Snapshot{id: {ID: id, State: "transferring"}},
		taskChunkIndexes: map[string]map[string]chunkLocation{id: indexManifest(manifest)},
	}
	server.mu.Lock()
	server.session = &finalSyncSession{id: id, primaryResumed: true}
	server.mu.Unlock()
	plan := server.getServableChunkSources(id, hash)
	if !plan.allowed || !plan.found {
		t.Fatalf("released session chunk request rejected: allowed=%t found=%t", plan.allowed, plan.found)
	}

	server.mu.Lock()
	server.session = nil
	server.mu.Unlock()
	if plan := server.getServableChunkSources(id, hash); plan.allowed {
		t.Fatal("chunk was served without an active session")
	}
}

func TestWriteProtectionMiddlewareFailsClosedOnInvalidConfig(t *testing.T) {
	oldProvider := setting.CfgProvider
	t.Cleanup(func() { setting.CfgProvider = oldProvider })
	provider, err := setting.NewConfigProviderFromData("[replicate]\nENABLED=true\nMODE=bogus\n")
	if err != nil {
		t.Fatal(err)
	}
	setting.CfgProvider = provider

	handler := WriteProtectionMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/user/login", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("write with unknown replication policy status=%d", response.Code)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("read with unknown replication policy status=%d", response.Code)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/owner/repo/git-upload-pack", nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("git fetch with unknown replication policy status=%d", response.Code)
	}
}

func TestWriteProtectionMiddlewareModes(t *testing.T) {
	oldProvider := setting.CfgProvider
	t.Cleanup(func() { setting.CfgProvider = oldProvider })
	token := strings.Repeat("x", 32)
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	check := func(handler http.Handler, method, path string, want int) {
		t.Helper()
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(method, path, nil))
		if response.Code != want {
			t.Fatalf("%s %s status=%d want=%d", method, path, response.Code, want)
		}
	}

	// Replica mode rejects state-changing requests but keeps reads and git fetches working.
	provider, err := setting.NewConfigProviderFromData("[replicate]\nENABLED=true\nMODE=replica\nSOURCE_URL=https://primary.example\nCONTROL_TOKEN=" + token + "\n")
	if err != nil {
		t.Fatal(err)
	}
	setting.CfgProvider = provider
	handler := WriteProtectionMiddleware(next)
	check(handler, http.MethodGet, "/owner/repo", http.StatusNoContent)
	check(handler, http.MethodPost, "/user/login", http.StatusServiceUnavailable)
	check(handler, http.MethodPut, "/api/v1/repos/owner/repo", http.StatusServiceUnavailable)
	check(handler, http.MethodPost, "/owner/repo/git-upload-pack", http.StatusNoContent)

	// A primary accepts writes until an outage checkpoint appears.
	snapshotDir := t.TempDir()
	provider, err = setting.NewConfigProviderFromData("[replicate]\nENABLED=true\nMODE=primary\nCONTROL_TOKEN=" + token + "\nSNAPSHOT_DIR=" + snapshotDir + "\n")
	if err != nil {
		t.Fatal(err)
	}
	setting.CfgProvider = provider
	handler = WriteProtectionMiddleware(next)
	check(handler, http.MethodPost, "/user/login", http.StatusNoContent)
	if err := os.WriteFile(primaryOutageCheckpointPath(snapshotDir), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	check(handler, http.MethodPost, "/user/login", http.StatusServiceUnavailable)
	check(handler, http.MethodGet, "/owner/repo", http.StatusNoContent)
	check(handler, http.MethodPost, "/owner/repo/git-upload-pack", http.StatusNoContent)
}

func TestPrimaryRecoveryRetriesCheckpointCleanupWithoutRestarting(t *testing.T) {
	oldSystemctl, oldReadiness, oldInterval := systemctlRunner, readinessCheck, primaryRecoveryRetryInterval
	t.Cleanup(func() {
		systemctlRunner, readinessCheck, primaryRecoveryRetryInterval = oldSystemctl, oldReadiness, oldInterval
	})
	primaryRecoveryRetryInterval = time.Millisecond

	snapshotDir := t.TempDir()
	checkpointPath := primaryOutageCheckpointPath(snapshotDir)
	if err := os.Mkdir(checkpointPath, 0o700); err != nil {
		t.Fatal(err)
	}
	requireWriteFile(t, filepath.Join(checkpointPath, "block"), "x")

	var startCalls, readinessCalls atomic.Int64
	systemctlRunner = func(_ context.Context, action, _ string) error {
		if action == "start" {
			startCalls.Add(1)
		}
		return nil
	}
	readinessCheck = func(context.Context, string) error {
		readinessCalls.Add(1)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := &controlServer{
		cfg:     &config{GiteaServiceName: "gitea.service", ServiceTimeout: time.Second, SnapshotDir: snapshotDir},
		taskCtx: ctx,
	}
	server.retryPrimaryStart("20260101T000000.000000000Z", "test")

	deadline := time.Now().Add(10 * time.Second)
	for startCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// Let the loop run several cleanup-only retries.
	time.Sleep(20 * time.Millisecond)
	if startCalls.Load() != 1 || readinessCalls.Load() != 1 {
		t.Fatalf("service was restarted while only checkpoint cleanup failed: start=%d readiness=%d", startCalls.Load(), readinessCalls.Load())
	}

	if err := os.RemoveAll(checkpointPath); err != nil {
		t.Fatal(err)
	}
	for {
		server.mu.RLock()
		pending := server.primaryRecoveryPending
		server.mu.RUnlock()
		if !pending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("recovery did not finish after the checkpoint became removable")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	server.backgroundWG.Wait()
}

func BenchmarkPrimaryOutageCheckpointExists(b *testing.B) {
	dir := b.TempDir()
	for b.Loop() {
		primaryOutageCheckpointExists(dir)
	}
}

func TestChunkBatchServesMultipleChunks(t *testing.T) {
	oldRoot, oldVersion := setting.AppWorkPath, setting.AppVer
	t.Cleanup(func() { setting.AppWorkPath, setting.AppVer = oldRoot, oldVersion })
	root := t.TempDir()
	setting.AppWorkPath, setting.AppVer = root, "test"
	content := strings.Repeat("a", 4096)
	requireWriteFile(t, filepath.Join(root, "data"), content)
	manifest, err := scanIncrementalTree(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Files) != 1 || len(manifest.Files[0].Chunks) != 1 {
		t.Fatalf("unexpected manifest shape: %d entries", len(manifest.Files))
	}
	manifest.ID, manifest.State = "20260101T000000.000000000Z", "transferring"
	hash := manifest.Files[0].Chunks[0].Hash
	id := manifest.ID

	server := &controlServer{
		cfg:              &config{Mode: modePrimary, ControlToken: "token", SnapshotDir: t.TempDir()},
		jobs:             map[string]*Snapshot{id: {ID: id, State: "transferring"}},
		taskChunkIndexes: map[string]map[string]chunkLocation{id: indexManifest(manifest)},
	}
	server.mu.Lock()
	server.session = &finalSyncSession{id: id}
	server.mu.Unlock()

	body, err := json.Marshal(chunkBatchRequest{Hashes: []string{hash, strings.Repeat("b", 64)}})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, syncJobsPath+"/"+id+"/chunks", bytes.NewReader(body))
	response := httptest.NewRecorder()
	server.syncChunkBatch(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	payload := response.Body.Bytes()
	if len(payload) < 6 || payload[0] != chunkBatchStatusOK || payload[1] != 0 {
		t.Fatalf("unexpected first record header: %v", payload[:min(len(payload), 8)])
	}
	length := int(binary.BigEndian.Uint32(payload[2:6]))
	if length != len(content) {
		t.Fatalf("payload length=%d want=%d", length, len(content))
	}
	if string(payload[6:6+length]) != content {
		t.Fatal("chunk payload mismatch")
	}
	rest := payload[6+length:]
	if len(rest) != 2 || rest[0] != chunkBatchStatusUnavailable || rest[1] != 1 {
		t.Fatalf("unexpected unavailable record: %v", rest)
	}
}
