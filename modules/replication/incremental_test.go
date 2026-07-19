// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"code.gitea.io/gitea/modules/json"
	"code.gitea.io/gitea/modules/setting"
)

func deterministicBytes(size int) []byte {
	data := make([]byte, size)
	_, _ = rand.New(rand.NewSource(42)).Read(data)
	return data
}

func chunkHashSet(chunks []ChunkDescriptor) map[string]struct{} {
	result := make(map[string]struct{}, len(chunks))
	for _, chunk := range chunks {
		result[chunk.Hash] = struct{}{}
	}
	return result
}

func TestContentDefinedChunksResynchronizeAfterInsertion(t *testing.T) {
	dir := t.TempDir()
	original := deterministicBytes(8 << 20)
	first := filepath.Join(dir, "first")
	if err := os.WriteFile(first, original, 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := splitFile(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	changed := append(append(append([]byte{}, original[:600000]...), []byte("inserted-data")...), original[600000:]...)
	second := filepath.Join(dir, "second")
	if err := os.WriteFile(second, changed, 0o600); err != nil {
		t.Fatal(err)
	}
	after, err := splitFile(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	set := chunkHashSet(before)
	reused := 0
	for _, chunk := range after {
		if _, ok := set[chunk.Hash]; ok {
			reused++
		}
	}
	if reused == 0 {
		t.Fatalf("no chunks were reused after a small insertion: before=%d after=%d", len(before), len(after))
	}
}

func TestIncrementalManifestSignatureAndTamperDetection(t *testing.T) {
	oldRoot, oldVersion := setting.AppWorkPath, setting.AppVer
	defer func() { setting.AppWorkPath, setting.AppVer = oldRoot, oldVersion }()
	setting.AppWorkPath, setting.AppVer = t.TempDir(), "test"
	requireWriteFile(t, filepath.Join(setting.AppWorkPath, "data", "gitea.db"), "db")
	manifest, err := scanIncrementalTree(context.Background(), setting.AppWorkPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest.ID = "20260101T000000.000000000Z"
	manifest.State = "preflight"
	manifest.CreatedAt = time.Unix(1, 0)
	if err := signIncrementalManifest(manifest, "token"); err != nil {
		t.Fatal(err)
	}
	if err := validateIncrementalManifest(manifest); err != nil || !verifyIncrementalSignature(manifest, "token") {
		t.Fatalf("valid signed manifest rejected: %v", err)
	}
	manifest.Files[0].Mode ^= 1
	if err := validateIncrementalManifest(manifest); err == nil {
		t.Fatal("tampered manifest accepted")
	}
}

func TestBuildIncrementalStageReusesAndDeletes(t *testing.T) {
	oldRoot, oldVersion := setting.AppWorkPath, setting.AppVer
	defer func() { setting.AppWorkPath, setting.AppVer = oldRoot, oldVersion }()
	root := t.TempDir()
	setting.AppWorkPath, setting.AppVer = root, "test"
	requireWriteFile(t, filepath.Join(root, "keep"), "unchanged")
	requireWriteFile(t, filepath.Join(root, "delete"), "obsolete")
	previous, err := scanIncrementalTree(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	previous.ID, previous.State, previous.CreatedAt = "20260101T000000.000000000Z", "ready", time.Unix(1, 0)
	if err := signIncrementalManifest(previous, "token"); err != nil {
		t.Fatal(err)
	}

	source := t.TempDir()
	setting.AppWorkPath = source
	requireWriteFile(t, filepath.Join(source, "keep"), "unchanged")
	requireWriteFile(t, filepath.Join(source, "new"), "replacement")
	final, err := scanIncrementalTree(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	final.ID, final.State, final.CreatedAt = "20260101T000001.000000000Z", "transferring", time.Unix(2, 0)
	if err := signIncrementalManifest(final, "token"); err != nil {
		t.Fatal(err)
	}
	cache := t.TempDir()
	for hash, location := range indexManifest(final) {
		if err := storeChunk(cache, hash, mustReadChunk(t, source, location, hash)); err != nil {
			t.Fatal(err)
		}
	}
	stage := t.TempDir()
	if err := buildIncrementalStage(context.Background(), root, stage, cache, final, previous, func(string) ([]byte, error) {
		t.Fatal("unexpected network fetch")
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(stage, "delete")); !os.IsNotExist(err) {
		t.Fatalf("deleted path retained: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(stage, "new"))
	if err != nil || string(data) != "replacement" {
		t.Fatalf("new data=%q err=%v", data, err)
	}
}

type truncatedManifestReader struct {
	data []byte
	sent bool
}

func (r *truncatedManifestReader) Read(p []byte) (int, error) {
	if r.sent {
		return 0, io.ErrUnexpectedEOF
	}
	r.sent = true
	return copy(p, r.data), io.ErrUnexpectedEOF
}

func TestDecodeManifestAcceptsCompleteDocumentWithTruncatedTransportTrailer(t *testing.T) {
	data, err := json.Marshal(SnapshotManifest{Snapshot: Snapshot{ID: "20260101T000000.000000000Z"}})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := decodeManifestResponse(&truncatedManifestReader{data: data})
	if err != nil {
		t.Fatal(err)
	}
	if manifest.ID != "20260101T000000.000000000Z" {
		t.Fatalf("manifest ID=%s", manifest.ID)
	}
}

func TestDecodeManifestRejectsTrailingData(t *testing.T) {
	if _, err := decodeManifestResponse(strings.NewReader(`{"id":"20260101T000000.000000000Z"}{}`)); err == nil {
		t.Fatal("trailing manifest data was accepted")
	}
}

func TestRequestManifestRetriesTransientGatewayErrors(t *testing.T) {
	oldRoot, oldVersion := setting.AppWorkPath, setting.AppVer
	defer func() { setting.AppWorkPath, setting.AppVer = oldRoot, oldVersion }()
	root := t.TempDir()
	setting.AppWorkPath, setting.AppVer = root, "test"
	requireWriteFile(t, filepath.Join(root, "data", "gitea.db"), "content")
	token := "01234567890123456789012345678901"
	manifest, err := scanIncrementalTree(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	manifest.ID = "20260101T000000.000000000Z"
	manifest.State = "preflight"
	manifest.CreatedAt = time.Unix(1, 0).UTC()
	manifest.InstanceFingerprint = instanceFingerprint(token)
	if err := signIncrementalManifest(manifest, token); err != nil {
		t.Fatal(err)
	}

	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			http.Error(w, "upstream unavailable", http.StatusBadGateway)
			return
		}
		writeJSON(w, manifest)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := requestManifest(ctx, server.Client(), server.URL, token, "preflight")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != manifest.ID {
		t.Fatalf("manifest id=%s want=%s", got.ID, manifest.ID)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want=3", attempts)
	}
}

func TestFetchMissingChunksDefersChangedPreflightChunks(t *testing.T) {
	manifest := &SnapshotManifest{
		Snapshot: Snapshot{ID: "20260101T000000.000000000Z"},
		Files: []TreeEntry{{
			Path: "data/gitea.db",
			Type: "file",
			Mode: 0o600,
			Chunks: []ChunkDescriptor{{
				Hash: strings.Repeat("a", 64),
				Size: 3,
			}},
		}},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer server.Close()

	cacheDir := t.TempDir()
	err := fetchMissingChunks(context.Background(), server.Client(), server.URL, "token", manifest, nil, cacheDir, true)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("cache entries=%d want=0", len(entries))
	}
}

func TestResumableFinalManifestSelectsLatestTrustedCheckpoint(t *testing.T) {
	oldRoot, oldVersion := setting.AppWorkPath, setting.AppVer
	defer func() { setting.AppWorkPath, setting.AppVer = oldRoot, oldVersion }()
	root := t.TempDir()
	setting.AppWorkPath, setting.AppVer = root, "test"
	requireWriteFile(t, filepath.Join(root, "data"), "content")
	token := "01234567890123456789012345678901"
	base, err := scanIncrementalTree(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	snapshotDir := t.TempDir()
	for index, id := range []string{"20260101T000000.000000000Z", "20260101T000001.000000000Z"} {
		manifest := *base
		manifest.ID = id
		manifest.State = "transferring"
		manifest.CreatedAt = time.Unix(int64(index+1), 0).UTC()
		manifest.InstanceFingerprint = instanceFingerprint(token)
		if err := signIncrementalManifest(&manifest, token); err != nil {
			t.Fatal(err)
		}
		if err := persistStandbyManifest(snapshotDir, &manifest); err != nil {
			t.Fatal(err)
		}
	}
	manifest := resumableFinalManifest(snapshotDir, token)
	if manifest == nil || manifest.ID != "20260101T000001.000000000Z" {
		t.Fatalf("resumable manifest=%+v", manifest)
	}
}

func mustReadChunk(t *testing.T, root string, location chunkLocation, hash string) []byte {
	t.Helper()
	data, err := readChunk(root, location, hash)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestCorruptChunkCacheIsNotTrusted(t *testing.T) {
	cache := t.TempDir()
	data := []byte("valid")
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])
	if err := storeChunk(cache, hash, data); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cachePath(cache, hash), []byte("bad"), 0o600); err != nil {
		t.Fatal(err)
	}
	if cacheHas(cache, hash) {
		t.Fatal("corrupt cache entry accepted")
	}
}

func requireWriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func awaitSnapshotManifestState(t *testing.T, server *controlServer, id, wantState string) *SnapshotManifest {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		response := httptest.NewRecorder()
		server.syncTask(response, httptest.NewRequest(http.MethodGet, "/api/v1/replication/sync-jobs/"+id+"/manifest", nil))
		if response.Code == http.StatusOK {
			var manifest SnapshotManifest
			if err := json.NewDecoder(response.Body).Decode(&manifest); err != nil {
				t.Fatal(err)
			}
			if manifest.State != wantState {
				t.Fatalf("manifest state=%s want=%s", manifest.State, wantState)
			}
			return &manifest
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("snapshot manifest %s did not become available", id)
	return nil
}

func TestPruneManifestHistoryPreservesCurrentBaseline(t *testing.T) {
	dir := t.TempDir()
	requireWriteFile(t, filepath.Join(dir, "current.json"), "baseline")
	ids := []string{
		"20260101T000000.000000000Z",
		"20260101T000001.000000000Z",
		"20260101T000002.000000000Z",
	}
	for _, id := range ids {
		requireWriteFile(t, manifestPath(dir, id), id)
	}
	requireWriteFile(t, filepath.Join(dir, ids[0]+".json.tmp"), "interrupted")
	requireWriteFile(t, filepath.Join(dir, ".current.json.tmp-interrupted"), "interrupted")
	pruneManifestFiles(dir, 2)
	if _, err := os.Stat(filepath.Join(dir, "current.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(manifestPath(dir, ids[0])); !os.IsNotExist(err) {
		t.Fatalf("oldest history was not pruned: %v", err)
	}
	for _, id := range ids[1:] {
		if _, err := os.Stat(manifestPath(dir, id)); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{filepath.Join(dir, ids[0]+".json.tmp"), filepath.Join(dir, ".current.json.tmp-interrupted")} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("interrupted manifest temporary file was not pruned: %s: %v", path, err)
		}
	}
}

func TestPreviousManifestRequiresValidSignature(t *testing.T) {
	oldRoot, oldVersion := setting.AppWorkPath, setting.AppVer
	defer func() { setting.AppWorkPath, setting.AppVer = oldRoot, oldVersion }()
	setting.AppWorkPath, setting.AppVer = t.TempDir(), "test"
	requireWriteFile(t, filepath.Join(setting.AppWorkPath, "data"), "content")
	manifest, err := scanIncrementalTree(context.Background(), setting.AppWorkPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest.ID, manifest.State, manifest.CreatedAt = "20260101T000000.000000000Z", "ready", time.Unix(1, 0)
	manifest.InstanceFingerprint = instanceFingerprint("correct-token")
	if err := signIncrementalManifest(manifest, "wrong-token"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "current.json")
	if err := writeManifestAt(path, manifest); err != nil {
		t.Fatal(err)
	}
	if previousManifest(path, "correct-token") != nil {
		t.Fatal("baseline signed by a different token was trusted")
	}
}

func TestPreviousManifestAcceptsSameReleaseDifferentBuild(t *testing.T) {
	oldRoot, oldVersion := setting.AppWorkPath, setting.AppVer
	defer func() { setting.AppWorkPath, setting.AppVer = oldRoot, oldVersion }()
	setting.AppWorkPath, setting.AppVer = t.TempDir(), "1.26.4+4-gold"
	requireWriteFile(t, filepath.Join(setting.AppWorkPath, "data"), "content")
	manifest, err := scanIncrementalTree(context.Background(), setting.AppWorkPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest.ID, manifest.State, manifest.CreatedAt = "20260101T000000.000000000Z", "ready", time.Unix(1, 0)
	manifest.InstanceFingerprint = instanceFingerprint("token")
	if err := signIncrementalManifest(manifest, "token"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "current.json")
	if err := writeManifestAt(path, manifest); err != nil {
		t.Fatal(err)
	}
	setting.AppVer = "1.26.4+7-gnew"
	if previousManifest(path, "token") == nil {
		t.Fatal("baseline from the same Gitea release was not trusted")
	}
}

func TestLoadManifestRejectsTrailingJSON(t *testing.T) {
	oldRoot, oldVersion := setting.AppWorkPath, setting.AppVer
	defer func() { setting.AppWorkPath, setting.AppVer = oldRoot, oldVersion }()
	setting.AppWorkPath, setting.AppVer = t.TempDir(), "test"
	requireWriteFile(t, filepath.Join(setting.AppWorkPath, "data"), "content")
	manifest, err := scanIncrementalTree(context.Background(), setting.AppWorkPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest.ID, manifest.State, manifest.CreatedAt = "20260101T000000.000000000Z", "ready", time.Unix(1, 0)
	if err := signIncrementalManifest(manifest, "token"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "current.json")
	if err := writeManifestAt(path, manifest); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, []byte("{}")...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadManifestFile(path); err == nil {
		t.Fatal("manifest with trailing JSON was accepted")
	}
}

func TestPreviousManifestRecoversSignedTrailingBaseline(t *testing.T) {
	oldRoot, oldVersion := setting.AppWorkPath, setting.AppVer
	defer func() { setting.AppWorkPath, setting.AppVer = oldRoot, oldVersion }()
	setting.AppWorkPath, setting.AppVer = t.TempDir(), "test"
	requireWriteFile(t, filepath.Join(setting.AppWorkPath, "data"), "content")
	manifest, err := scanIncrementalTree(context.Background(), setting.AppWorkPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest.ID, manifest.State, manifest.CreatedAt = "20260101T000000.000000000Z", "ready", time.Unix(1, 0)
	manifest.InstanceFingerprint = instanceFingerprint("token")
	if err := signIncrementalManifest(manifest, "token"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "current.json")
	if err := writeManifestAt(path, manifest); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, []byte("{}")...), 0o600); err != nil {
		t.Fatal(err)
	}
	if previousManifest(path, "token") == nil {
		t.Fatal("signed baseline with trailing data was not recovered")
	}
	if _, err := loadManifestFile(path); err != nil {
		t.Fatalf("recovered baseline was not rewritten: %v", err)
	}
}

func TestBuildIncrementalStageSupportsReadOnlyDirectories(t *testing.T) {
	oldRoot, oldVersion := setting.AppWorkPath, setting.AppVer
	defer func() { setting.AppWorkPath, setting.AppVer = oldRoot, oldVersion }()
	source := t.TempDir()
	setting.AppWorkPath, setting.AppVer = source, "test"
	dir := filepath.Join(source, "readonly")
	requireWriteFile(t, filepath.Join(dir, "child"), "content")
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	manifest, err := scanIncrementalTree(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	cache := t.TempDir()
	for hash, location := range indexManifest(manifest) {
		if err := storeChunk(cache, hash, mustReadChunk(t, source, location, hash)); err != nil {
			t.Fatal(err)
		}
	}
	stage := t.TempDir()
	if err := buildIncrementalStage(context.Background(), source, stage, cache, manifest, nil, func(string) ([]byte, error) {
		t.Fatal("unexpected network fetch")
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(filepath.Join(stage, "readonly"), 0o700) })
	info, err := os.Stat(filepath.Join(stage, "readonly"))
	if err != nil || info.Mode().Perm() != 0o500 {
		t.Fatalf("read-only directory mode=%v err=%v", info.Mode().Perm(), err)
	}
	data, err := os.ReadFile(filepath.Join(stage, "readonly", "child"))
	if err != nil || string(data) != "content" {
		t.Fatalf("child data=%q err=%v", data, err)
	}
}

func writeReadyBaseline(t *testing.T, root, snapshotDir, token string, fullScanAt time.Time) *SnapshotManifest {
	t.Helper()
	manifest, err := scanIncrementalTree(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	manifest.ID = "20260101T000000.000000000Z"
	manifest.State = "ready"
	manifest.FullScanAt = fullScanAt.UTC()
	manifest.CreatedAt = time.Now().UTC()
	manifest.InstanceFingerprint = instanceFingerprint(token)
	if err := signIncrementalManifest(manifest, token); err != nil {
		t.Fatal(err)
	}
	if err := writeManifestAt(baselineManifestPath(snapshotDir), manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func writeTrustedManifest(t *testing.T, root, snapshotDir, token, id, state string, fullScanAt time.Time) *SnapshotManifest {
	t.Helper()
	manifest, err := scanIncrementalTree(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	manifest.ID = id
	manifest.State = state
	manifest.FullScanAt = fullScanAt.UTC()
	manifest.CreatedAt = time.Now().UTC()
	manifest.InstanceFingerprint = instanceFingerprint(token)
	if err := signIncrementalManifest(manifest, token); err != nil {
		t.Fatal(err)
	}
	if err := writeManifestAt(manifestPath(snapshotDir, id), manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func TestPreflightBaselineOnlyHashesChangedFiles(t *testing.T) {
	oldRoot, oldVersion, oldChunker := setting.AppWorkPath, setting.AppVer, chunkFileForManifest
	defer func() {
		setting.AppWorkPath, setting.AppVer, chunkFileForManifest = oldRoot, oldVersion, oldChunker
	}()
	root, snapshotDir := t.TempDir(), t.TempDir()
	setting.AppWorkPath, setting.AppVer = root, "test"
	requireWriteFile(t, filepath.Join(root, "repositories", "unchanged.pack"), "unchanged")
	requireWriteFile(t, filepath.Join(root, "data", "gitea.db"), "database")
	token := "01234567890123456789012345678901"
	baseline := writeReadyBaseline(t, root, snapshotDir, token, time.Now())
	requireWriteFile(t, filepath.Join(root, "data", "gitea.db"), "changed-database")

	chunkCalls := 0
	chunkFileForManifest = func(ctx context.Context, path string) ([]ChunkDescriptor, error) {
		chunkCalls++
		return splitFile(ctx, path)
	}
	server := &controlServer{cfg: &config{
		Mode: modePrimary, ControlToken: token, SnapshotDir: snapshotDir, SnapshotRetention: 5,
		FullScanInterval: 168 * time.Hour, SnapshotTimeout: time.Minute,
	}, jobs: map[string]*Snapshot{}, dataRoot: root}
	response := httptest.NewRecorder()
	server.preflight(response, httptest.NewRequest(http.MethodPost, "preflight", nil))
	if response.Code != http.StatusAccepted {
		t.Fatalf("preflight status=%d body=%s", response.Code, response.Body.String())
	}
	var job Snapshot
	if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}
	manifest := awaitSnapshotManifestState(t, server, job.ID, "preflight")
	if chunkCalls != 1 {
		t.Fatalf("chunk calls=%d want=1 changed file", chunkCalls)
	}
	if !manifest.FullScanAt.Equal(baseline.FullScanAt) {
		t.Fatalf("full scan timestamp changed: got=%s want=%s", manifest.FullScanAt, baseline.FullScanAt)
	}
}

func TestPreflightPerformsPeriodicFullScan(t *testing.T) {
	oldRoot, oldVersion, oldChunker := setting.AppWorkPath, setting.AppVer, chunkFileForManifest
	defer func() {
		setting.AppWorkPath, setting.AppVer, chunkFileForManifest = oldRoot, oldVersion, oldChunker
	}()
	root, snapshotDir := t.TempDir(), t.TempDir()
	setting.AppWorkPath, setting.AppVer = root, "test"
	requireWriteFile(t, filepath.Join(root, "repositories", "one.pack"), "one")
	requireWriteFile(t, filepath.Join(root, "repositories", "two.pack"), "two")
	token := "01234567890123456789012345678901"
	oldFullScan := time.Now().Add(-2 * time.Hour)
	writeReadyBaseline(t, root, snapshotDir, token, oldFullScan)

	chunkCalls := 0
	chunkFileForManifest = func(ctx context.Context, path string) ([]ChunkDescriptor, error) {
		chunkCalls++
		return splitFile(ctx, path)
	}
	server := &controlServer{cfg: &config{
		Mode: modePrimary, ControlToken: token, SnapshotDir: snapshotDir, SnapshotRetention: 5,
		FullScanInterval: time.Hour, SnapshotTimeout: time.Minute,
	}, jobs: map[string]*Snapshot{}, dataRoot: root}
	response := httptest.NewRecorder()
	server.preflight(response, httptest.NewRequest(http.MethodPost, "preflight", nil))
	if response.Code != http.StatusAccepted {
		t.Fatalf("preflight status=%d body=%s", response.Code, response.Body.String())
	}
	var job Snapshot
	if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}
	manifest := awaitSnapshotManifestState(t, server, job.ID, "preflight")
	if chunkCalls != 2 {
		t.Fatalf("chunk calls=%d want=2 for periodic full scan", chunkCalls)
	}
	if !manifest.FullScanAt.After(oldFullScan) {
		t.Fatalf("full scan timestamp was not refreshed: %s", manifest.FullScanAt)
	}
}

func TestPreflightReusesTrustedPreflightManifest(t *testing.T) {
	oldRoot, oldVersion, oldChunker := setting.AppWorkPath, setting.AppVer, chunkFileForManifest
	defer func() {
		setting.AppWorkPath, setting.AppVer, chunkFileForManifest = oldRoot, oldVersion, oldChunker
	}()
	root, snapshotDir := t.TempDir(), t.TempDir()
	setting.AppWorkPath, setting.AppVer = root, "test"
	requireWriteFile(t, filepath.Join(root, "repositories", "unchanged.pack"), "unchanged")
	requireWriteFile(t, filepath.Join(root, "data", "gitea.db"), "database")
	token := "01234567890123456789012345678901"
	preflightBase := writeTrustedManifest(t, root, snapshotDir, token, "20260101T000000.000000000Z", "preflight", time.Now())
	requireWriteFile(t, filepath.Join(root, "data", "gitea.db"), "changed-database")

	chunkCalls := 0
	chunkFileForManifest = func(ctx context.Context, path string) ([]ChunkDescriptor, error) {
		chunkCalls++
		return splitFile(ctx, path)
	}
	server := &controlServer{cfg: &config{
		Mode: modePrimary, ControlToken: token, SnapshotDir: snapshotDir, SnapshotRetention: 5,
		FullScanInterval: 168 * time.Hour, SnapshotTimeout: time.Minute,
	}, jobs: map[string]*Snapshot{}, dataRoot: root}
	response := httptest.NewRecorder()
	server.preflight(response, httptest.NewRequest(http.MethodPost, "preflight", nil))
	if response.Code != http.StatusAccepted {
		t.Fatalf("preflight status=%d body=%s", response.Code, response.Body.String())
	}
	var job Snapshot
	if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}
	manifest := awaitSnapshotManifestState(t, server, job.ID, "preflight")
	if chunkCalls != 1 {
		t.Fatalf("chunk calls=%d want=1 changed file", chunkCalls)
	}
	if !manifest.FullScanAt.Equal(preflightBase.FullScanAt) {
		t.Fatalf("full scan timestamp changed: got=%s want=%s", manifest.FullScanAt, preflightBase.FullScanAt)
	}
}

func TestFullVerificationRejectsContentChangeWithUnchangedMetadata(t *testing.T) {
	oldRoot, oldVersion := setting.AppWorkPath, setting.AppVer
	defer func() { setting.AppWorkPath, setting.AppVer = oldRoot, oldVersion }()
	root := t.TempDir()
	setting.AppWorkPath, setting.AppVer = root, "test"
	path := filepath.Join(root, "object")
	requireWriteFile(t, path, "original")
	baseline, err := scanIncrementalTree(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	requireWriteFile(t, path, "corrupt!")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := range baseline.Files {
		if baseline.Files[i].Path == "object" {
			baseline.Files[i].Size = info.Size()
			baseline.Files[i].Mode = uint32(info.Mode().Perm())
			baseline.Files[i].ModTimeNS = info.ModTime().UnixNano()
			baseline.Files[i].ChangeID = fileChangeID(info)
		}
	}
	if _, err := scanIncrementalTreeWithOptions(context.Background(), root, baseline, true); err == nil ||
		!strings.Contains(err.Error(), "content verification failed") {
		t.Fatalf("silent content change returned %v", err)
	}
}
