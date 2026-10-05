// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"gitea.dev/modules/setting"
)

func scanTreeForApplyTest(t *testing.T, root string) *SnapshotManifest {
	t.Helper()
	oldWorkPath, oldVersion := setting.AppWorkPath, setting.AppVer
	setting.AppWorkPath, setting.AppVer = root, "test"
	defer func() { setting.AppWorkPath, setting.AppVer = oldWorkPath, oldVersion }()
	manifest, err := scanIncrementalTree(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func chunkFetchFromTree(t *testing.T, sourceRoot string, manifest *SnapshotManifest) func(context.Context, string) ([]byte, error) {
	t.Helper()
	index := indexManifest(manifest)
	resolved, err := resolvedPath(sourceRoot)
	if err != nil {
		t.Fatal(err)
	}
	return func(_ context.Context, hash string) ([]byte, error) {
		location, ok := index[hash]
		if !ok {
			return nil, fmt.Errorf("chunk %s is not present in the apply test source", hash)
		}
		return readChunkFromRoot(sourceRoot, resolved, location, hash)
	}
}

func applyManifestOptionsForTest(t *testing.T, target, cacheDir string, opts inPlaceApplyOptions) *inPlaceApplyStats {
	t.Helper()
	oldWorkPath, oldVersion := setting.AppWorkPath, setting.AppVer
	setting.AppWorkPath, setting.AppVer = target, "test"
	defer func() { setting.AppWorkPath, setting.AppVer = oldWorkPath, oldVersion }()
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		t.Fatal(err)
	}
	opts.CacheDir = cacheDir
	stats, err := applyInPlace(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return stats
}

func applyManifestForTest(t *testing.T, target, cacheDir string, manifest, previous *SnapshotManifest, fetch func(context.Context, string) ([]byte, error)) *inPlaceApplyStats {
	t.Helper()
	oldWorkPath, oldVersion := setting.AppWorkPath, setting.AppVer
	setting.AppWorkPath, setting.AppVer = target, "test"
	defer func() { setting.AppWorkPath, setting.AppVer = oldWorkPath, oldVersion }()
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		t.Fatal(err)
	}
	stats, err := applyInPlace(context.Background(), inPlaceApplyOptions{Manifest: manifest, Previous: previous, CacheDir: cacheDir, Fetch: fetch})
	if err != nil {
		t.Fatal(err)
	}
	return stats
}

func manifestFileEntry(t *testing.T, manifest *SnapshotManifest, path string) TreeEntry {
	t.Helper()
	for _, entry := range manifest.Files {
		if entry.Path == path && entry.Type == "file" {
			return entry
		}
	}
	t.Fatalf("file entry %q is missing from the manifest", path)
	return TreeEntry{}
}

func requireFileContent(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != want {
		t.Fatalf("content of %s = %q, want %q", path, data, want)
	}
}

func TestInPlaceApplyCreatesTree(t *testing.T) {
	source := t.TempDir()
	requireWriteFile(t, filepath.Join(source, "data", "file"), "hello world")
	requireWriteFile(t, filepath.Join(source, "custom", "conf", "app.ini"), "cfg")
	if err := os.Chmod(filepath.Join(source, "data"), 0o750); err != nil {
		t.Fatal(err)
	}
	manifest := scanTreeForApplyTest(t, source)

	target, cacheDir := t.TempDir(), filepath.Join(t.TempDir(), "chunks")
	stats := applyManifestForTest(t, target, cacheDir, manifest, nil, chunkFetchFromTree(t, source, manifest))
	if stats.FilesCreated == 0 {
		t.Fatal("expected files to be created")
	}
	requireFileContent(t, filepath.Join(target, "data", "file"), "hello world")
	requireFileContent(t, filepath.Join(target, "custom", "conf", "app.ini"), "cfg")
	info, err := os.Lstat(filepath.Join(target, "data"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o750 {
		t.Fatalf("directory mode = %o, want 750", info.Mode().Perm())
	}
	for _, entry := range manifest.Files {
		if entry.Type != "file" {
			continue
		}
		applied, err := os.Lstat(filepath.Join(target, filepath.FromSlash(entry.Path)))
		if err != nil {
			t.Fatal(err)
		}
		if applied.ModTime().UnixNano() != entry.ModTimeNS {
			t.Fatalf("mtime of %s = %d, want %d", entry.Path, applied.ModTime().UnixNano(), entry.ModTimeNS)
		}
	}
	if _, err := os.Lstat(filepath.Join(target, ".install-stage")); !os.IsNotExist(err) {
		t.Fatalf("in-place apply must not create a staging tree: %v", err)
	}
	// Consumed chunks are released from the cache as they land in the tree, so the standby
	// never needs the data tree and the transferred chunks on disk at the same time.
	cached := 0
	if err := filepath.Walk(cacheDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			cached++
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if cached != 0 {
		t.Fatalf("chunk cache still holds %d files after the apply", cached)
	}
}

func TestInPlaceApplyPatchesOnlyChangedChunks(t *testing.T) {
	random := rand.New(rand.NewSource(7))
	original := make([]byte, 3*1024*1024)
	if _, err := random.Read(original); err != nil {
		t.Fatal(err)
	}
	updated := bytes.Clone(original)
	copy(updated[:64], bytes.Repeat([]byte("x"), 64))

	oldSource, newSource := t.TempDir(), t.TempDir()
	requireWriteFile(t, filepath.Join(oldSource, "data", "blob"), string(original))
	requireWriteFile(t, filepath.Join(newSource, "data", "blob"), string(updated))
	oldManifest := scanTreeForApplyTest(t, oldSource)
	newManifest := scanTreeForApplyTest(t, newSource)
	oldFile := manifestFileEntry(t, oldManifest, "data/blob")
	newFile := manifestFileEntry(t, newManifest, "data/blob")
	if len(oldFile.Chunks) < 2 {
		t.Fatalf("expected a multi-chunk file, got %d chunks", len(oldFile.Chunks))
	}

	target, cacheDir := t.TempDir(), filepath.Join(t.TempDir(), "chunks")
	applyManifestForTest(t, target, cacheDir, oldManifest, nil, chunkFetchFromTree(t, oldSource, oldManifest))
	requireWriteFile(t, filepath.Join(target, "data", "blob"), string(original))
	if err := recordLocalChangeIDs(context.Background(), target, oldManifest); err != nil {
		t.Fatal(err)
	}

	stats := applyManifestForTest(t, target, cacheDir, newManifest, oldManifest, chunkFetchFromTree(t, newSource, newManifest))
	if stats.FilesPatched != 1 {
		t.Fatalf("patched files = %d, want 1", stats.FilesPatched)
	}
	if stats.ChunksPatched == 0 || stats.ChunksPatched >= len(newFile.Chunks) {
		t.Fatalf("patched chunks = %d of %d, expected a partial patch", stats.ChunksPatched, len(newFile.Chunks))
	}
	if stats.BytesWritten >= newFile.Size {
		t.Fatalf("wrote %d bytes for a %d byte file; expected only the changed chunks", stats.BytesWritten, newFile.Size)
	}
	requireFileContent(t, filepath.Join(target, "data", "blob"), string(updated))
}

func TestInPlaceApplySkipsUnchangedFiles(t *testing.T) {
	source := t.TempDir()
	requireWriteFile(t, filepath.Join(source, "data", "file"), strings.Repeat("a", 1024))
	requireWriteFile(t, filepath.Join(source, "data", "other"), "second")
	manifest := scanTreeForApplyTest(t, source)

	target, cacheDir := t.TempDir(), filepath.Join(t.TempDir(), "chunks")
	applyManifestForTest(t, target, cacheDir, manifest, nil, chunkFetchFromTree(t, source, manifest))
	if err := recordLocalChangeIDs(context.Background(), target, manifest); err != nil {
		t.Fatal(err)
	}
	stats := applyManifestForTest(t, target, cacheDir, manifest, manifest, func(context.Context, string) ([]byte, error) {
		return nil, errors.New("unchanged files must not fetch chunks")
	})
	if stats.FilesReused != 2 || stats.ChunksPatched != 0 || stats.BytesWritten != 0 {
		t.Fatalf("unexpected reuse stats: reused=%d patched=%d bytes=%d", stats.FilesReused, stats.ChunksPatched, stats.BytesWritten)
	}
}

func TestInPlaceApplyDeletesObsoleteEntries(t *testing.T) {
	source := t.TempDir()
	requireWriteFile(t, filepath.Join(source, "data", "keep"), "keep")
	requireWriteFile(t, filepath.Join(source, "data", "drop", "file"), "drop")
	manifest := scanTreeForApplyTest(t, source)

	target, cacheDir := t.TempDir(), filepath.Join(t.TempDir(), "chunks")
	applyManifestForTest(t, target, cacheDir, manifest, nil, chunkFetchFromTree(t, source, manifest))
	requireFileContent(t, filepath.Join(target, "data", "drop", "file"), "drop")

	trimmed := t.TempDir()
	requireWriteFile(t, filepath.Join(trimmed, "data", "keep"), "keep")
	newManifest := scanTreeForApplyTest(t, trimmed)
	stats := applyManifestForTest(t, target, cacheDir, newManifest, manifest, chunkFetchFromTree(t, trimmed, newManifest))
	if stats.EntriesDeleted == 0 {
		t.Fatal("expected obsolete entries to be deleted")
	}
	if _, err := os.Lstat(filepath.Join(target, "data", "drop")); !os.IsNotExist(err) {
		t.Fatalf("obsolete directory remains: %v", err)
	}
	requireFileContent(t, filepath.Join(target, "data", "keep"), "keep")
}

func TestInPlaceApplyRebuildsHardlinkedFile(t *testing.T) {
	source := t.TempDir()
	requireWriteFile(t, filepath.Join(source, "data", "file"), "new content")
	manifest := scanTreeForApplyTest(t, source)

	target := t.TempDir()
	shared := filepath.Join(t.TempDir(), "shared")
	requireWriteFile(t, shared, "old content")
	if err := os.MkdirAll(filepath.Join(target, "data"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(shared, filepath.Join(target, "data", "file")); err != nil {
		t.Fatal(err)
	}
	cacheDir := filepath.Join(t.TempDir(), "chunks")
	stats := applyManifestForTest(t, target, cacheDir, manifest, nil, chunkFetchFromTree(t, source, manifest))
	if stats.FilesReplaced != 1 {
		t.Fatalf("replaced files = %d, want 1", stats.FilesReplaced)
	}
	requireFileContent(t, filepath.Join(target, "data", "file"), "new content")
	requireFileContent(t, shared, "old content")
	info, err := os.Lstat(filepath.Join(target, "data", "file"))
	if err != nil {
		t.Fatal(err)
	}
	if fileHasMultipleLinks(info) {
		t.Fatal("hard-linked file was patched in place instead of being replaced")
	}
}

func TestInPlaceApplySymlinks(t *testing.T) {
	source := t.TempDir()
	requireWriteFile(t, filepath.Join(source, "data", "target"), "target")
	if err := os.Symlink("target", filepath.Join(source, "data", "link")); err != nil {
		t.Fatal(err)
	}
	manifest := scanTreeForApplyTest(t, source)

	target, cacheDir := t.TempDir(), filepath.Join(t.TempDir(), "chunks")
	applyManifestForTest(t, target, cacheDir, manifest, nil, chunkFetchFromTree(t, source, manifest))
	link, err := os.Readlink(filepath.Join(target, "data", "link"))
	if err != nil {
		t.Fatal(err)
	}
	if link != "target" {
		t.Fatalf("symlink target = %q, want target", link)
	}
}

func TestRestoredTreeVerificationDetectsContentChangeWithRestoredMetadata(t *testing.T) {
	oldRoot, oldVersion := setting.AppWorkPath, setting.AppVer
	t.Cleanup(func() { setting.AppWorkPath, setting.AppVer = oldRoot, oldVersion })
	root := t.TempDir()
	setting.AppWorkPath, setting.AppVer = root, "test"
	path := filepath.Join(root, "data", "blob")
	requireWriteFile(t, path, strings.Repeat("a", 1024))
	previous := scanTreeForApplyTest(t, root)
	if err := recordLocalChangeIDs(context.Background(), root, previous); err != nil {
		t.Fatal(err)
	}
	previous.State = "ready"
	file := manifestFileEntry(t, previous, "data/blob")

	if err := os.WriteFile(path, []byte(strings.Repeat("b", 1024)), 0o600); err != nil {
		t.Fatal(err)
	}
	mtime := time.Unix(0, file.ModTimeNS)
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	target := *previous
	target.Files = slices.Clone(previous.Files)
	if err := verifyRestoredStandbyTree(context.Background(), root, &target, previous); err == nil {
		t.Fatal("content change with restored metadata was accepted through the identity base")
	}
}

func TestInPlaceApplyUsesProvidedLocalCandidates(t *testing.T) {
	random := rand.New(rand.NewSource(11))
	content := make([]byte, 2*1024*1024)
	if _, err := random.Read(content); err != nil {
		t.Fatal(err)
	}
	prefix := content[:1024]
	shifted := append(append([]byte{}, prefix...), content...)

	oldSource, newSource := t.TempDir(), t.TempDir()
	requireWriteFile(t, filepath.Join(oldSource, "data", "a"), string(content))
	requireWriteFile(t, filepath.Join(newSource, "data", "b"), string(shifted))
	oldManifest := scanTreeForApplyTest(t, oldSource)
	newManifest := scanTreeForApplyTest(t, newSource)
	newFile := manifestFileEntry(t, newManifest, "data/b")

	oldIndex := indexManifest(oldManifest)
	var hash string
	var size, offset int64
	for _, candidate := range newFile.Chunks {
		location, ok := oldIndex[candidate.Hash]
		if !ok || location.Size != candidate.Size {
			continue
		}
		hash, size, offset = candidate.Hash, candidate.Size, location.Offset
		break
	}
	if hash == "" {
		t.Skip("content-defined chunking produced no shared chunk for the shifted file")
	}

	newIndex := indexManifest(newManifest)
	resolvedNew, err := resolvedPath(newSource)
	if err != nil {
		t.Fatal(err)
	}
	target, cacheDir := t.TempDir(), filepath.Join(t.TempDir(), "chunks")
	requireWriteFile(t, filepath.Join(target, "data", "a"), string(content))
	stats := applyManifestOptionsForTest(t, target, cacheDir, inPlaceApplyOptions{
		Manifest: newManifest, Previous: nil,
		LocalCandidates: map[string][]chunkLocation{hash: {{Path: "data/a", Offset: offset, Size: size}}},
		Fetch: func(_ context.Context, requested string) ([]byte, error) {
			if requested == hash {
				return nil, errors.New("the shared chunk must come from the local candidate")
			}
			return readChunkFromRoot(newSource, resolvedNew, newIndex[requested], requested)
		},
	})
	if stats.ChunksReused == 0 {
		t.Fatalf("provided local candidates were not used: reused=%d fetched=%d", stats.ChunksReused, stats.ChunksFetched)
	}
	if got, err := os.ReadFile(filepath.Join(target, "data", "b")); err != nil || !bytes.Equal(got, shifted) {
		t.Fatalf("shifted target content mismatch: err=%v", err)
	}
}

func TestInPlaceApplyDeletesFromTrustedBaselineAndCleansTemps(t *testing.T) {
	source := t.TempDir()
	requireWriteFile(t, filepath.Join(source, "data", "keep"), "keep")
	requireWriteFile(t, filepath.Join(source, "data", "drop", "file"), "drop")
	previous := scanTreeForApplyTest(t, source)

	target, cacheDir := t.TempDir(), filepath.Join(t.TempDir(), "chunks")
	applyManifestForTest(t, target, cacheDir, previous, nil, chunkFetchFromTree(t, source, previous))
	if err := recordLocalChangeIDs(context.Background(), target, previous); err != nil {
		t.Fatal(err)
	}
	previous.State = "ready"

	trimmed := t.TempDir()
	requireWriteFile(t, filepath.Join(trimmed, "data", "keep"), "keep")
	manifest := scanTreeForApplyTest(t, trimmed)
	requireWriteFile(t, filepath.Join(target, "data", ".replication-apply-stale"), "stale")
	// A file that neither the baseline nor the target manifest lists, for example a
	// queue file an interrupted earlier update created, must be removed as well.
	requireWriteFile(t, filepath.Join(target, "data", "leftover.ldb"), "leftover")
	stats := applyManifestForTest(t, target, cacheDir, manifest, previous, chunkFetchFromTree(t, trimmed, manifest))
	if stats.EntriesDeleted == 0 {
		t.Fatal("expected obsolete entries to be deleted")
	}
	if _, err := os.Lstat(filepath.Join(target, "data", "leftover.ldb")); !os.IsNotExist(err) {
		t.Fatalf("unlisted leftover file remains: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(target, "data", "drop")); !os.IsNotExist(err) {
		t.Fatalf("obsolete directory remains: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(target, "data", ".replication-apply-stale")); !os.IsNotExist(err) {
		t.Fatalf("stale apply temporary file remains: %v", err)
	}
	requireFileContent(t, filepath.Join(target, "data", "keep"), "keep")
}

func TestInPlaceApplyReleasesSharedCachedChunkAfterLastUse(t *testing.T) {
	source := t.TempDir()
	content := strings.Repeat("shared", 512)
	requireWriteFile(t, filepath.Join(source, "data", "one"), content)
	requireWriteFile(t, filepath.Join(source, "data", "two"), content)
	manifest := scanTreeForApplyTest(t, source)
	file := manifestFileEntry(t, manifest, "data/one")
	if len(file.Chunks) != 1 {
		t.Fatalf("expected a single chunk, got %d", len(file.Chunks))
	}
	hash := file.Chunks[0].Hash
	data, err := os.ReadFile(filepath.Join(source, "data", "one"))
	if err != nil {
		t.Fatal(err)
	}

	target, cacheDir := t.TempDir(), filepath.Join(t.TempDir(), "chunks")
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := storeChunkForBatch(cacheDir, hash, data); err != nil {
		t.Fatal(err)
	}
	stats := applyManifestOptionsForTest(t, target, cacheDir, inPlaceApplyOptions{
		Manifest: manifest, CacheDir: cacheDir,
		CachedHashes: map[string]struct{}{hash: {}},
		Fetch:        func(context.Context, string) ([]byte, error) { return nil, errors.New("fetch must not be used") },
	})
	if stats.ChunksFromCache+stats.ChunksReused != 2 || stats.ChunksFetched != 0 {
		t.Fatalf("shared cached chunk was not reused for both files: from_cache=%d reused=%d fetched=%d", stats.ChunksFromCache, stats.ChunksReused, stats.ChunksFetched)
	}
	requireFileContent(t, filepath.Join(target, "data", "one"), content)
	requireFileContent(t, filepath.Join(target, "data", "two"), content)
	if _, err := os.Lstat(cachePath(cacheDir, hash)); !os.IsNotExist(err) {
		t.Fatalf("cached chunk remains after its last use: %v", err)
	}
}
