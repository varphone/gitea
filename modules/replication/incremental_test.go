// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"gitea.dev/modules/json"
	"gitea.dev/modules/setting"
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
	original := deterministicBytes(8 * 1024 * 1024)
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
	signTestManifest(t, manifest, "token")
	if err := validateIncrementalManifest(manifest); err != nil || !verifyIncrementalSignature(manifest, "token") {
		t.Fatalf("valid signed manifest rejected: %v", err)
	}
	manifest.Files[0].Mode ^= 1
	if err := validateIncrementalManifest(manifest); err == nil {
		t.Fatal("tampered manifest accepted")
	}
}

func TestManifestEncodingUsesFixedJSONV1Rules(t *testing.T) {
	manifest := &SnapshotManifest{
		Snapshot:      Snapshot{ID: "20260101T000000.000000000Z"},
		FormatVersion: incrementalFormatVersion,
		Files:         []TreeEntry{{Path: "special/<&>", Type: "file", Mode: 0o600, Size: 10000, Chunks: make([]ChunkDescriptor, 10000)}},
	}
	for i := range manifest.Files[0].Chunks {
		manifest.Files[0].Chunks[i] = ChunkDescriptor{Hash: strings.Repeat("a", 64), Offset: int64(i), Size: 1, Zero: i == 0}
	}
	want, err := marshalManifestJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	if err := writeManifestJSONContext(context.Background(), &got, manifest, false); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("streaming manifest encoding differs from JSON v1: got=%d bytes want=%d bytes", got.Len(), len(want))
	}
}

type truncatedManifestReader struct {
	data   []byte
	offset int
}

func (r *truncatedManifestReader) Read(p []byte) (int, error) {
	if r.offset == len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	n := copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

func signTestManifest(t *testing.T, manifest *SnapshotManifest, token string) {
	t.Helper()
	manifest.GeneralTokenSecretFingerprint = generalTokenSecretFingerprint(token)
	if err := signIncrementalManifest(manifest, token); err != nil {
		t.Fatal(err)
	}
}

func setReplicaTestConfig(t *testing.T, token string) {
	t.Helper()
	oldProvider, oldOAuthEnabled := setting.CfgProvider, setting.OAuth2.Enabled
	t.Cleanup(func() {
		setting.CfgProvider = oldProvider
		setting.OAuth2.Enabled = oldOAuthEnabled
	})
	provider, err := setting.NewConfigProviderFromData("[replicate]\nENABLED=true\nMODE=replica\nSOURCE_URL=https://primary.example\nCONTROL_TOKEN=" + token + "\nSNAPSHOT_DIR=" + t.TempDir() + "\n")
	if err != nil {
		t.Fatal(err)
	}
	setting.CfgProvider = provider
	setting.OAuth2.Enabled = false
}

func TestRequestManifestWaitsForActiveReplication(t *testing.T) {
	oldRoot, oldVersion, oldDelay := setting.AppWorkPath, setting.AppVer, syncBusyRetryDelay
	defer func() { setting.AppWorkPath, setting.AppVer, syncBusyRetryDelay = oldRoot, oldVersion, oldDelay }()
	token := "01234567890123456789012345678901"
	setReplicaTestConfig(t, token)
	setting.AppWorkPath, setting.AppVer = t.TempDir(), "test"
	requireWriteFile(t, filepath.Join(setting.AppWorkPath, "data"), "content")
	manifest, err := scanIncrementalTree(context.Background(), setting.AppWorkPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest.ID, manifest.State, manifest.CreatedAt = "20260101T000000.000000000Z", "preflight", time.Unix(1, 0)
	manifest.InstanceFingerprint = instanceFingerprint(token)
	signTestManifest(t, manifest, token)
	syncBusyRetryDelay = time.Millisecond
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			http.Error(w, "sync already in progress", http.StatusConflict)
			return
		}
		writeJSON(w, manifest)
	}))
	defer server.Close()
	got, err := requestManifest(context.Background(), server.Client(), server.URL, token, "preflight")
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || got.ID != manifest.ID {
		t.Fatalf("attempts=%d manifest=%+v", attempts, got)
	}
}

func TestRequestManifestGivesUpWhenPrimaryStaysBusy(t *testing.T) {
	oldDelay := syncBusyRetryDelay
	t.Cleanup(func() { syncBusyRetryDelay = oldDelay })
	syncBusyRetryDelay = time.Millisecond
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		http.Error(w, "sync already in progress", http.StatusConflict)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := requestManifestWithRequestID(ctx, server.Client(), server.URL, "token", "preflight", "", 5*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "stayed busy") {
		t.Fatalf("busy wait returned %v", err)
	}
	if attempts < 2 {
		t.Fatalf("busy wait gave up before retrying: attempts=%d", attempts)
	}
}

func TestDecodeManifestAcceptsCompleteDocumentWithTruncatedTransportTrailer(t *testing.T) {
	data, err := marshalManifestJSON(SnapshotManifest{Snapshot: Snapshot{ID: "20260101T000000.000000000Z"}})
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

func TestDecodeManifestAcceptsWhitespaceAndRejectsTruncatedTrailingValue(t *testing.T) {
	manifest, err := decodeManifestResponse(strings.NewReader("{\"id\":\"20260101T000000.000000000Z\"} \t\r\n"))
	if err != nil || manifest.ID != "20260101T000000.000000000Z" {
		t.Fatalf("whitespace trailer: manifest=%+v err=%v", manifest, err)
	}
	if _, err := decodeManifestResponse(strings.NewReader("{\"id\":\"20260101T000000.000000000Z\"}{\"state\":")); err == nil {
		t.Fatal("truncated trailing JSON value was accepted")
	}
}

func TestDecodeManifestRejectsCorruptGzipTrailer(t *testing.T) {
	data, err := json.Marshal(SnapshotManifest{Snapshot: Snapshot{ID: "20260101T000000.000000000Z"}})
	if err != nil {
		t.Fatal(err)
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	encoded := compressed.Bytes()
	encoded[len(encoded)-8] ^= 0xff
	response := &http.Response{Header: http.Header{"Content-Encoding": []string{"gzip"}}, Body: io.NopCloser(bytes.NewReader(encoded))}
	if _, err := decodeBoundedManifestResponse(response); err == nil {
		t.Fatal("manifest with a corrupt gzip checksum was accepted")
	}
}

func TestRestoredTreeIgnoresOnlyNodeLocalConfigChanges(t *testing.T) {
	oldRoot, oldVersion, oldCustomConf := setting.AppWorkPath, setting.AppVer, setting.CustomConf
	defer func() { setting.AppWorkPath, setting.AppVer, setting.CustomConf = oldRoot, oldVersion, oldCustomConf }()
	root := t.TempDir()
	setting.AppWorkPath, setting.AppVer = root, "test"
	setting.CustomConf = filepath.Join(root, "custom", "conf", "app.ini")
	requireWriteFile(t, setting.CustomConf, "replica config")
	requireWriteFile(t, filepath.Join(root, "data", "gitea.db"), "snapshot data")
	manifest, err := scanIncrementalTree(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	requireWriteFile(t, setting.CustomConf, "updated replica config")
	if err := verifyRestoredStandbyTree(context.Background(), root, manifest, nil); err != nil {
		t.Fatalf("node-local config change invalidated the tree: %v", err)
	}
	requireWriteFile(t, filepath.Join(root, "custom", "conf", "unexpected.ini"), "unexpected replicated file")
	if err := verifyRestoredStandbyTree(context.Background(), root, manifest, nil); err == nil {
		t.Fatal("unexpected non-local entry was ignored")
	}
}

func TestRequestManifestRetriesTransientGatewayErrors(t *testing.T) {
	oldRoot, oldVersion := setting.AppWorkPath, setting.AppVer
	defer func() { setting.AppWorkPath, setting.AppVer = oldRoot, oldVersion }()
	root := t.TempDir()
	setting.AppWorkPath, setting.AppVer = root, "test"
	requireWriteFile(t, filepath.Join(root, "data", "gitea.db"), "content")
	token := "01234567890123456789012345678901"
	setReplicaTestConfig(t, token)
	manifest, err := scanIncrementalTree(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	manifest.ID = "20260101T000000.000000000Z"
	manifest.State = "preflight"
	manifest.CreatedAt = time.Unix(1, 0).UTC()
	manifest.InstanceFingerprint = instanceFingerprint(token)
	signTestManifest(t, manifest, token)

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

func TestPreflightCapacityReservesOnlyRetainedChunks(t *testing.T) {
	oldStat, oldLimit := statFSAvailable, deferredChunkCacheLimit
	t.Cleanup(func() { statFSAvailable, deferredChunkCacheLimit = oldStat, oldLimit })

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

	// The cache limit defers every chunk, so no cache space is reserved for them.
	deferredChunkCacheLimit = 1
	statFSAvailable = func(string) (int64, int64, uint64, error) {
		return restoreDiskSafetyMargin, 1000, 1, nil
	}
	if _, err := fetchMissingChunks(context.Background(), server.Client(), server.URL, "token", manifest, nil, t.TempDir(), true); err != nil {
		t.Fatalf("deferred chunks were reserved in the cache footprint: %v", err)
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
	if _, err := fetchMissingChunks(context.Background(), server.Client(), server.URL, "token", manifest, nil, cacheDir, true); err != nil {
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
		manifest.GeneralTokenSecretFingerprint = generalTokenSecretFingerprint(token)
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
	pruneReplicationTemporaryFiles(dir)
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
	manifest.GeneralTokenSecretFingerprint = generalTokenSecretFingerprint("correct-token")
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
	signTestManifest(t, manifest, "token")
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
	signTestManifest(t, manifest, "token")
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
	signTestManifest(t, manifest, "token")
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
	signTestManifest(t, manifest, token)
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
	signTestManifest(t, manifest, token)
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
	setReplicaTestConfig(t, token)
	baseline := writeReadyBaseline(t, root, snapshotDir, token, time.Now())
	requireWriteFile(t, filepath.Join(root, "data", "gitea.db"), "changed-database")

	chunkCalls := 0
	chunkFileForManifest = func(ctx context.Context, path string, _ os.FileInfo) ([]ChunkDescriptor, error) {
		chunkCalls++
		return splitFile(ctx, path)
	}
	server := &controlServer{cfg: &config{
		Mode: modePrimary, ControlToken: token, SnapshotDir: snapshotDir, SnapshotRetention: 5,
		FullScanInterval: 168 * time.Hour, SnapshotTimeout: time.Minute,
	}, jobs: map[string]*Snapshot{}, dataRoot: root}
	response := httptest.NewRecorder()
	server.preflight(response, httptest.NewRequest(http.MethodPost, "http://example.com/preflight", nil))
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
	setReplicaTestConfig(t, token)
	oldFullScan := time.Now().Add(-2 * time.Hour)
	writeReadyBaseline(t, root, snapshotDir, token, oldFullScan)

	chunkCalls := 0
	chunkFileForManifest = func(ctx context.Context, path string, _ os.FileInfo) ([]ChunkDescriptor, error) {
		chunkCalls++
		return splitFile(ctx, path)
	}
	server := &controlServer{cfg: &config{
		Mode: modePrimary, ControlToken: token, SnapshotDir: snapshotDir, SnapshotRetention: 5,
		FullScanInterval: time.Hour, SnapshotTimeout: time.Minute,
	}, jobs: map[string]*Snapshot{}, dataRoot: root}
	response := httptest.NewRecorder()
	server.preflight(response, httptest.NewRequest(http.MethodPost, "http://example.com/preflight", nil))
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
	setReplicaTestConfig(t, token)
	preflightBase := writeTrustedManifest(t, root, snapshotDir, token, "20260101T000000.000000000Z", "preflight", time.Now())
	requireWriteFile(t, filepath.Join(root, "data", "gitea.db"), "changed-database")

	chunkCalls := 0
	chunkFileForManifest = func(ctx context.Context, path string, _ os.FileInfo) ([]ChunkDescriptor, error) {
		chunkCalls++
		return splitFile(ctx, path)
	}
	server := &controlServer{cfg: &config{
		Mode: modePrimary, ControlToken: token, SnapshotDir: snapshotDir, SnapshotRetention: 5,
		FullScanInterval: 168 * time.Hour, SnapshotTimeout: time.Minute,
	}, jobs: map[string]*Snapshot{}, dataRoot: root}
	response := httptest.NewRecorder()
	server.preflight(response, httptest.NewRequest(http.MethodPost, "http://example.com/preflight", nil))
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

func TestSigningReservesSpaceForSignedFields(t *testing.T) {
	oldLimit := manifestSizeLimit
	t.Cleanup(func() { manifestSizeLimit = oldLimit })
	manifest := &SnapshotManifest{
		Snapshot:      Snapshot{ID: "20260101T000000.000000000Z"},
		FormatVersion: incrementalFormatVersion,
		Files: []TreeEntry{{
			Path: "a", Type: "file", Mode: 0o600, Size: 10,
			Chunks: []ChunkDescriptor{{Hash: strings.Repeat("a", sha256.Size*2), Size: 10}},
		}},
	}
	_, digestSize, err := manifestDigestWithSizeContext(context.Background(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	// The digest omits sha256/signature, so the limit must leave room for them.
	manifestSizeLimit = digestSize + manifestSignedFieldsSize
	if err := signIncrementalManifestContext(context.Background(), manifest, "token"); err != nil {
		t.Fatalf("manifest at the persisted size limit was rejected: %v", err)
	}
	var persisted bytes.Buffer
	if err := writeManifestJSONContext(context.Background(), &persisted, manifest, false); err != nil {
		t.Fatal(err)
	}
	if int64(persisted.Len()) > manifestSizeLimit {
		t.Fatalf("persisted manifest is %d bytes, limit %d", persisted.Len(), manifestSizeLimit)
	}

	manifestSizeLimit = digestSize + manifestSignedFieldsSize - 1
	if err := signIncrementalManifestContext(context.Background(), manifest, "token"); !errors.Is(err, errManifestTooLarge) {
		t.Fatalf("manifest that cannot be persisted returned %v, want %v", err, errManifestTooLarge)
	}
}

func TestSignRestoredStandbyManifestDropsOversizedLocalIdentities(t *testing.T) {
	oldLimit := manifestSizeLimit
	t.Cleanup(func() { manifestSizeLimit = oldLimit })
	token := strings.Repeat("t", 32)
	manifest := &SnapshotManifest{
		Snapshot:      Snapshot{ID: "20260101T000000.000000000Z", State: "ready", SHA256: strings.Repeat("a", sha256.Size*2)},
		Signature:     strings.Repeat("b", sha256.Size*2),
		FormatVersion: incrementalFormatVersion,
	}
	for i := range 8 {
		manifest.Files = append(manifest.Files, TreeEntry{
			Path: strings.Repeat("f", i+1), Type: "file", Mode: 0o644, Size: 1, ModTimeNS: 1, ChangeID: "change",
			LocalChangeID: strings.Repeat("d", 24),
		})
	}
	manifest.FileCount = len(manifest.Files)

	trimmedFiles := make([]TreeEntry, len(manifest.Files))
	copy(trimmedFiles, manifest.Files)
	for i := range trimmedFiles {
		trimmedFiles[i].LocalChangeID = ""
	}
	trimmed := *manifest
	trimmed.Files = trimmedFiles
	var baseline bytes.Buffer
	if err := writeManifestJSONContext(context.Background(), &baseline, &trimmed, false); err != nil {
		t.Fatal(err)
	}
	manifestSizeLimit = int64(baseline.Len()) + 8

	if err := signRestoredStandbyManifest(context.Background(), manifest, token); err != nil {
		t.Fatalf("oversized ready manifest was not degraded: %v", err)
	}
	for i := range manifest.Files {
		if manifest.Files[i].LocalChangeID != "" {
			t.Fatalf("local identity %q was kept", manifest.Files[i].LocalChangeID)
		}
	}
	var persisted bytes.Buffer
	if err := writeManifestJSONContext(context.Background(), &persisted, manifest, false); err != nil {
		t.Fatal(err)
	}
	if int64(persisted.Len()) > manifestSizeLimit {
		t.Fatalf("persisted ready manifest is %d bytes, limit %d", persisted.Len(), manifestSizeLimit)
	}
}

func TestCachedChunkAvailabilityRejectsMalformedHash(t *testing.T) {
	dir := t.TempDir()
	if cacheHas(dir, "ab") {
		t.Fatal("short hash was treated as cached")
	}
	if cachedChunkAvailable(dir, "", 1) {
		t.Fatal("empty hash was treated as cached")
	}
	if cachedChunkAvailable(dir, strings.Repeat("A", sha256.Size*2), 1) {
		t.Fatal("non-lowercase hash was treated as cached")
	}
}

func TestDecodeManifestDecodeErrorIsNotTrailingData(t *testing.T) {
	var manifest SnapshotManifest
	err := decodeManifestJSON(strings.NewReader(`{"id":"20260101T000000.000000000Z"`), &manifest)
	if err == nil {
		t.Fatal("truncated manifest was accepted")
	}
	if errors.Is(err, errManifestTrailingData) {
		t.Fatalf("decode error was misclassified as trailing data: %v", err)
	}
	err = decodeManifestJSON(strings.NewReader(`{"id":"20260101T000000.000000000Z"}{}`), &manifest)
	if !errors.Is(err, errManifestTrailingData) {
		t.Fatalf("trailing data returned %v, want %v", err, errManifestTrailingData)
	}
}

func TestInstalledContentVerificationIgnoresLocalIdentities(t *testing.T) {
	oldRoot, oldVersion := setting.AppWorkPath, setting.AppVer
	t.Cleanup(func() { setting.AppWorkPath, setting.AppVer = oldRoot, oldVersion })
	root := t.TempDir()
	setting.AppWorkPath, setting.AppVer = root, "test"
	path := filepath.Join(root, "data")
	requireWriteFile(t, path, "AAAA")
	manifest, err := scanIncrementalTree(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	manifest.ID, manifest.State, manifest.CreatedAt = "20260101T000000.000000000Z", "ready", time.Now().UTC()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	other := sha256.Sum256([]byte("BBBB"))
	for i := range manifest.Files {
		manifest.Files[i].LocalChangeID = fileChangeID(info)
		manifest.Files[i].Chunks[0].Hash = hex.EncodeToString(other[:])
	}

	matches, err := installedTreeMatchesManifest(context.Background(), root, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !matches {
		t.Fatal("identity-based verification rejected the tree")
	}
	if err := verifyInstalledStandbyContent(context.Background(), root, manifest); err == nil {
		t.Fatal("content verification accepted chunks that do not match the installed bytes")
	}
}

func TestScanSkipsSpecialFiles(t *testing.T) {
	oldRoot, oldVersion := setting.AppWorkPath, setting.AppVer
	t.Cleanup(func() { setting.AppWorkPath, setting.AppVer = oldRoot, oldVersion })
	root := t.TempDir()
	setting.AppWorkPath, setting.AppVer = root, "test"
	requireWriteFile(t, filepath.Join(root, "data"), "content")
	if err := syscall.Mkfifo(filepath.Join(root, "pipe"), 0o644); err != nil {
		t.Skipf("mkfifo unavailable: %v", err)
	}
	listener, err := net.Listen("unix", filepath.Join(root, "socket"))
	if err != nil {
		t.Skipf("unix socket unavailable: %v", err)
	}
	defer listener.Close()

	manifest, err := scanIncrementalTree(context.Background(), root)
	if err != nil {
		t.Fatalf("scan rejected a tree with node-local special files: %v", err)
	}
	if manifest.FileCount != 1 || len(manifest.Files) != 1 || manifest.Files[0].Path != "data" {
		t.Fatalf("unexpected manifest entries: file_count=%d files=%+v", manifest.FileCount, manifest.Files)
	}
	matches, err := installedTreeMatchesManifest(context.Background(), root, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !matches {
		t.Fatal("node-local special files invalidated the installed tree comparison")
	}
}

func TestScanPathErrorReportsSizeLimit(t *testing.T) {
	err := scanPathError("data/big", errManifestTooLarge)
	if !errors.Is(err, errManifestTooLarge) {
		t.Fatalf("size limit error lost its type: %v", err)
	}
	if !strings.Contains(err.Error(), "size limit") || !strings.Contains(err.Error(), "data/big") {
		t.Fatalf("unhelpful size limit error: %v", err)
	}
}

func TestInstalledTreeToleratesExtraNodeLocalLogDirectory(t *testing.T) {
	oldRoot, oldVersion, oldProvider := setting.AppWorkPath, setting.AppVer, setting.CfgProvider
	t.Cleanup(func() { setting.AppWorkPath, setting.AppVer, setting.CfgProvider = oldRoot, oldVersion, oldProvider })
	root := t.TempDir()
	setting.AppWorkPath, setting.AppVer = root, "test"
	requireWriteFile(t, filepath.Join(root, "data"), "content")
	manifest, err := scanIncrementalTree(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}

	logDir := filepath.Join(root, "logs")
	provider, err := setting.NewConfigProviderFromData("[log]\nlogger.default.MODE=file\n[log.file]\nMODE=file\nFILE_NAME=" + filepath.Join(logDir, "gitea.log") + "\n")
	if err != nil {
		t.Fatal(err)
	}
	setting.CfgProvider = provider
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatal(err)
	}
	requireWriteFile(t, filepath.Join(logDir, "gitea.log"), "log line")

	matches, err := installedTreeMatchesManifest(context.Background(), root, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !matches {
		t.Fatal("a directory created only for node-local logs invalidated the installed tree comparison")
	}
}

func TestChunkFetchUsesBatchEndpointForSmallChunks(t *testing.T) {
	token := strings.Repeat("t", 32)
	id := "20260101T000000.000000000Z"
	payloads := map[string][]byte{}
	hashes := make([]string, 0, 2)
	sizes := make([]int64, 0, 2)
	for _, content := range []string{"first chunk", "second chunk"} {
		sum := sha256.Sum256([]byte(content))
		hash := hex.EncodeToString(sum[:])
		payloads[hash] = []byte(content)
		hashes = append(hashes, hash)
		sizes = append(sizes, int64(len(content)))
	}

	var batchCalls, singleCalls atomic.Int64
	writeBatch := func(w http.ResponseWriter, hashes []string) {
		var body bytes.Buffer
		for index, hash := range hashes {
			data, ok := payloads[hash]
			if !ok {
				body.Write([]byte{chunkBatchStatusUnavailable, byte(index)})
				continue
			}
			body.Write([]byte{chunkBatchStatusOK, byte(index)})
			var length [4]byte
			binary.BigEndian.PutUint32(length[:], uint32(len(data)))
			body.Write(length[:])
			body.Write(data)
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(body.Bytes())
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/chunks"):
			batchCalls.Add(1)
			var request chunkBatchRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			writeBatch(w, request.Hashes)
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/chunks/"):
			singleCalls.Add(1)
			hash := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			data, ok := payloads[hash]
			if !ok {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(data)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	if err := fetchChunksConcurrently(context.Background(), server.Client(), server.URL, token, id, hashes, sizes, t.TempDir(), len(hashes), 0, 0, 0, 0, int64(len(payloads["first chunk"])+len(payloads["second chunk"]))); err != nil {
		t.Fatalf("batched chunk fetch failed: %v", err)
	}
	if batchCalls.Load() != 1 || singleCalls.Load() != 0 {
		t.Fatalf("expected one batch request and no single requests: batch=%d single=%d", batchCalls.Load(), singleCalls.Load())
	}

	// A failing batch endpoint must fall back to individual chunk requests.
	batchCalls.Store(0)
	singleCalls.Store(0)
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			batchCalls.Add(1)
			// A non-retryable failure makes the fallback immediate.
			http.Error(w, "batch unavailable", http.StatusConflict)
			return
		}
		singleCalls.Add(1)
		hash := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		data, ok := payloads[hash]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(data)
	}))
	defer fallback.Close()
	if err := fetchChunksConcurrently(context.Background(), fallback.Client(), fallback.URL, token, id, hashes, sizes, t.TempDir(), len(hashes), 0, 0, 0, 0, int64(len(payloads["first chunk"])+len(payloads["second chunk"]))); err != nil {
		t.Fatalf("fallback chunk fetch failed: %v", err)
	}
	if batchCalls.Load() != 1 || singleCalls.Load() != int64(len(hashes)) {
		t.Fatalf("expected one failed batch and %d single requests: batch=%d single=%d", len(hashes), batchCalls.Load(), singleCalls.Load())
	}
}

func TestFinalPlanDownloadsChunksOnlyAvailableInRewrittenFiles(t *testing.T) {
	oldWorkPath, oldVersion := setting.AppWorkPath, setting.AppVer
	t.Cleanup(func() { setting.AppWorkPath, setting.AppVer = oldWorkPath, oldVersion })
	root := t.TempDir()
	setting.AppWorkPath, setting.AppVer = root, "test"

	// The standby holds the chunk of "AAA" only inside data/source, which this
	// update rewrites: reusing that location would leave the chunk unavailable
	// once the file is patched, so it must be scheduled for download.
	requireWriteFile(t, filepath.Join(root, "data", "source"), "AAA")
	requireWriteFile(t, filepath.Join(root, "data", "target"), "BBB")
	previous := scanTreeForApplyTest(t, root)

	sourceHash := sha256.Sum256([]byte("AAA"))
	targetHash := sha256.Sum256([]byte("BBB"))
	manifest := &SnapshotManifest{
		Snapshot:  Snapshot{ID: "20260101T000000.000000000Z", Size: 6},
		FileCount: 2,
		Files: []TreeEntry{
			{Path: "data/source", Type: "file", Size: 3, Mode: 0o600, Chunks: []ChunkDescriptor{{Hash: hex.EncodeToString(targetHash[:]), Size: 3}}},
			{Path: "data/target", Type: "file", Size: 3, Mode: 0o600, Chunks: []ChunkDescriptor{{Hash: hex.EncodeToString(sourceHash[:]), Size: 3}}},
		},
	}
	plan, err := planMissingChunks(context.Background(), manifest, previous, t.TempDir(), false, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := hex.EncodeToString(sourceHash[:])
	if !slices.Contains(plan.hashes, want) {
		t.Fatalf("chunk %s is only available in a rewritten file but was not scheduled for download: download_chunks=%d reusable=%d", want[:12], len(plan.hashes), plan.reusable)
	}
}

func TestIncrementalDeltaTransfersOnlyChangedChunks(t *testing.T) {
	random := rand.New(rand.NewSource(23))
	content := make([]byte, 8*1024*1024)
	if _, err := random.Read(content); err != nil {
		t.Fatal(err)
	}

	source, standby := t.TempDir(), t.TempDir()
	sourcePath := filepath.Join(source, "data", "big.bin")
	standbyPath := filepath.Join(standby, "data", "big.bin")
	requireWriteFile(t, sourcePath, string(content))
	cacheDir := filepath.Join(t.TempDir(), "chunks")
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		t.Fatal(err)
	}

	var previous *SnapshotManifest
	sync := func(label string) (int, int64) {
		t.Helper()
		manifest := scanTreeForApplyTest(t, source)

		oldWorkPath := setting.AppWorkPath
		setting.AppWorkPath = standby
		plan, err := planMissingChunks(context.Background(), manifest, previous, cacheDir, false, nil)
		setting.AppWorkPath = oldWorkPath
		if err != nil {
			t.Fatal(err)
		}

		baseFetch := chunkFetchFromTree(t, source, manifest)
		fetched, fetchedBytes := 0, int64(0)
		applyManifestOptionsForTest(t, standby, cacheDir, inPlaceApplyOptions{
			Manifest: manifest, Previous: previous, CacheDir: cacheDir,
			LocalCandidates: plan.localCandidates,
			Fetch: func(ctx context.Context, hash string) ([]byte, error) {
				data, err := baseFetch(ctx, hash)
				if err != nil {
					return nil, err
				}
				fetched++
				fetchedBytes += int64(len(data))
				return data, nil
			},
		})
		if fetched != len(plan.hashes) {
			t.Fatalf("%s: fetched %d chunks but planned %d", label, fetched, len(plan.hashes))
		}
		if fetchedBytes != plan.missingSize {
			t.Fatalf("%s: fetched %d bytes but planned %d", label, fetchedBytes, plan.missingSize)
		}
		if err := recordLocalChangeIDs(context.Background(), standby, manifest); err != nil {
			t.Fatal(err)
		}
		t.Logf("%-18s chunks_transferred=%-3d payload_bytes=%-9d total_chunks=%d reusable=%d unavailable=%d skipped=%d reindex=%d file_bytes=%d", label, len(plan.hashes), plan.missingSize, plan.total, plan.reusable, plan.previousLocalFilesUnavailable, plan.previousLocalFilesSkipped, plan.localReindexAttempts, len(content))
		previous = manifest
		return len(plan.hashes), plan.missingSize
	}

	if chunks, payload := sync("initial full sync"); chunks == 0 || payload < int64(len(content))/2 {
		t.Fatalf("initial sync transferred too little: chunks=%d bytes=%d", chunks, payload)
	}
	requireFileContent(t, standbyPath, string(content))

	edit := func(offset, size int) {
		for i := range size {
			content[offset+i] ^= 0x5a
		}
		requireWriteFile(t, sourcePath, string(content))
	}
	for _, test := range []struct {
		label  string
		offset int
	}{
		{label: "64B at start", offset: 0},
		{label: "64B in middle", offset: len(content) / 2},
		{label: "64B at end", offset: len(content) - 64},
	} {
		edit(test.offset, 64)
		chunks, payload := sync(test.label)
		if chunks > 2 {
			t.Fatalf("%s: %d chunks transferred, want at most 2", test.label, chunks)
		}
		if payload > 2*chunkMaxSize {
			t.Fatalf("%s: %d bytes transferred, want at most %d", test.label, payload, 2*chunkMaxSize)
		}
		requireFileContent(t, standbyPath, string(content))
	}

	if chunks, payload := sync("no change"); chunks != 0 || payload != 0 {
		t.Fatalf("unchanged tree transferred %d chunks / %d bytes", chunks, payload)
	}
}
