// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func patchTestChunk(seed byte) ChunkDescriptor {
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = seed
	}
	return ChunkDescriptor{Hash: hex.EncodeToString(hash), Offset: 0, Size: 4096}
}

func patchTestFile(path string, modTime int64, chunks ...ChunkDescriptor) TreeEntry {
	size := int64(0)
	for _, chunk := range chunks {
		size += chunk.Size
	}
	offset := int64(0)
	for i := range chunks {
		chunks[i].Offset = offset
		offset += chunks[i].Size
	}
	return TreeEntry{Path: path, Type: "file", Size: size, Mode: 0o644, ModTimeNS: modTime, ChangeID: path + ":1", Chunks: chunks}
}

func patchTestManifest(id, state string, files ...TreeEntry) *SnapshotManifest {
	return &SnapshotManifest{
		Snapshot:      Snapshot{ID: id, State: state, CreatedAt: time.Unix(1700000000, 0).UTC(), Size: 1, RootMode: 0o755},
		FormatVersion: incrementalFormatVersion, GiteaVersion: "test", AppWorkPath: "/tmp/gitea",
		FileCount: len(files), Files: files,
	}
}

func TestManifestPatchRebuildsTarget(t *testing.T) {
	base := patchTestManifest("20260101T000000.000000000Z", "ready",
		TreeEntry{Path: "data", Type: "dir", Mode: 0o755, ModTimeNS: 1},
		patchTestFile("data/keep.bin", 10, patchTestChunk(1), patchTestChunk(2)),
		patchTestFile("data/changed.bin", 20, patchTestChunk(3)),
		patchTestFile("data/removed.bin", 30, patchTestChunk(4)),
		TreeEntry{Path: "data/link", Type: "symlink", LinkTarget: "keep.bin", Mode: 0o777, ModTimeNS: 40},
	)
	target := patchTestManifest("20260101T010000.000000000Z", "transferring",
		TreeEntry{Path: "data", Type: "dir", Mode: 0o755, ModTimeNS: 1},
		patchTestFile("data/keep.bin", 10, patchTestChunk(1), patchTestChunk(2)),
		patchTestFile("data/changed.bin", 21, patchTestChunk(5)),
		TreeEntry{Path: "data/link", Type: "symlink", LinkTarget: "keep.bin", Mode: 0o777, ModTimeNS: 40},
		patchTestFile("data/added.bin", 50, patchTestChunk(6), patchTestChunk(7)),
	)

	patch, err := buildManifestPatch(base, target)
	if err != nil {
		t.Fatal(err)
	}
	if patch.Base != base.ID || patch.Count != len(target.Files) {
		t.Fatalf("patch base=%q count=%d", patch.Base, patch.Count)
	}
	if len(patch.Changed) != 2 || len(patch.Removed) != 1 {
		t.Fatalf("changed=%d removed=%d want 2/1", len(patch.Changed), len(patch.Removed))
	}
	if patch.Manifest.Files != nil {
		t.Fatal("patch header must not carry files")
	}
	rebuilt, err := applyManifestPatch(base, patch)
	if err != nil {
		t.Fatal(err)
	}
	wantDigest, err := manifestDigestWithoutLocalChangeIDs(target)
	if err != nil {
		t.Fatal(err)
	}
	gotDigest, err := manifestDigestWithoutLocalChangeIDs(rebuilt)
	if err != nil {
		t.Fatal(err)
	}
	if gotDigest != wantDigest {
		t.Fatalf("rebuilt digest %s want %s", gotDigest, wantDigest)
	}
	if len(rebuilt.Files) != len(target.Files) {
		t.Fatalf("rebuilt files=%d want %d", len(rebuilt.Files), len(target.Files))
	}
	for i := range target.Files {
		if rebuilt.Files[i].Path != target.Files[i].Path {
			t.Fatalf("rebuilt[%d]=%s want %s", i, rebuilt.Files[i].Path, target.Files[i].Path)
		}
	}
}

func TestManifestPatchReusesLocalIdentities(t *testing.T) {
	base := patchTestManifest("20260101T000000.000000000Z", "ready",
		patchTestFile("data/keep.bin", 10, patchTestChunk(1)),
		patchTestFile("data/changed.bin", 20, patchTestChunk(2)),
	)
	local := *base
	local.Files = append([]TreeEntry(nil), base.Files...)
	for i := range local.Files {
		local.Files[i].LocalChangeID = "standby:" + local.Files[i].Path
	}
	target := patchTestManifest("20260101T010000.000000000Z", "preflight",
		patchTestFile("data/keep.bin", 10, patchTestChunk(1)),
		patchTestFile("data/changed.bin", 21, patchTestChunk(3)),
	)
	patch, err := buildManifestPatch(base, target)
	if err != nil {
		t.Fatal(err)
	}
	rebuilt, err := applyManifestPatch(&local, patch)
	if err != nil {
		t.Fatal(err)
	}
	if rebuilt.Files[0].LocalChangeID != "standby:data/keep.bin" {
		t.Fatalf("unchanged entry lost its local identity: %q", rebuilt.Files[0].LocalChangeID)
	}
	if rebuilt.Files[1].LocalChangeID != "" {
		t.Fatalf("changed entry kept a stale local identity: %q", rebuilt.Files[1].LocalChangeID)
	}
	if same, err := manifestsHaveSameContent(base, target); err != nil || same {
		t.Fatalf("same content check: same=%v err=%v", same, err)
	}
	zero, err := buildManifestPatch(base, base)
	if err != nil {
		t.Fatal(err)
	}
	if len(zero.Changed) != 0 || len(zero.Removed) != 0 {
		t.Fatalf("self patch must be empty: %+v", zero)
	}
	if same, err := manifestsHaveSameContent(base, base); err != nil || !same {
		t.Fatalf("identical manifests compare unequal: same=%v err=%v", same, err)
	}
}

func TestManifestPatchRejectsInconsistentInput(t *testing.T) {
	base := patchTestManifest("20260101T000000.000000000Z", "ready", patchTestFile("data/a", 1, patchTestChunk(1)))
	target := patchTestManifest("20260101T010000.000000000Z", "ready", patchTestFile("data/b", 1, patchTestChunk(2)))
	patch, err := buildManifestPatch(base, target)
	if err != nil {
		t.Fatal(err)
	}
	other := patchTestManifest("20260102T000000.000000000Z", "ready", patchTestFile("data/c", 1, patchTestChunk(3)))
	if _, err := applyManifestPatch(other, patch); err == nil {
		t.Fatal("patch accepted a foreign base")
	}
	bad := patch
	bad.Changed = []manifestPatchItem{{Index: 5, Entry: target.Files[0]}}
	if _, err := applyManifestPatch(base, bad); err == nil {
		t.Fatal("patch accepted an out of range index")
	}
}

func TestDecodeManifestPatchResponse(t *testing.T) {
	base := patchTestManifest("20260101T000000.000000000Z", "ready",
		patchTestFile("data/keep.bin", 10, patchTestChunk(1)),
		patchTestFile("data/changed.bin", 20, patchTestChunk(2)),
	)
	target := patchTestManifest("20260101T010000.000000000Z", "transferring",
		patchTestFile("data/keep.bin", 10, patchTestChunk(1)),
		patchTestFile("data/changed.bin", 21, patchTestChunk(3)),
	)
	if err := signIncrementalManifest(target, "token"); err != nil {
		t.Fatal(err)
	}
	patch, err := buildManifestPatch(base, target)
	if err != nil {
		t.Fatal(err)
	}
	body, err := marshalManifestJSON(patch)
	if err != nil {
		t.Fatal(err)
	}
	response := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{manifestPatchContentType}},
		Body:       io.NopCloser(strings.NewReader(string(body))),
	}
	rebuilt, err := decodeManifestOrPatchResponse(response, base, "token")
	if err != nil {
		t.Fatal(err)
	}
	if rebuilt.ID != target.ID || rebuilt.FileCount != target.FileCount {
		t.Fatalf("rebuilt id=%s files=%d", rebuilt.ID, rebuilt.FileCount)
	}
	if !verifyIncrementalSignature(&rebuilt, "token") {
		t.Fatal("rebuilt manifest signature does not verify")
	}
	// A tampered patch must not be accepted.
	tampered := patch
	tampered.Changed = append([]manifestPatchItem(nil), patch.Changed...)
	tampered.Changed[0].Entry.ChangeID = "tampered"
	tamperedBody, err := marshalManifestJSON(tampered)
	if err != nil {
		t.Fatal(err)
	}
	response = &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{manifestPatchContentType}},
		Body:       io.NopCloser(strings.NewReader(string(tamperedBody))),
	}
	if _, err := decodeManifestOrPatchResponse(response, base, "token"); err == nil {
		t.Fatal("tampered patch was accepted")
	}
}
