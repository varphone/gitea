// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"gitea.dev/modules/log"
)

const manifestPatchContentType = "application/vnd.gitea.replication.manifest-patch+json"

// manifestPatch carries the entries that differ from a base manifest, so a standby that
// already holds the base transfers only the snapshot header and the changed entries.
type manifestPatch struct {
	Base     string              `json:"base"`
	Count    int                 `json:"count"`
	Manifest SnapshotManifest    `json:"manifest"`
	Changed  []manifestPatchItem `json:"changed,omitempty"`
	Removed  []string            `json:"removed,omitempty"`
}

type manifestPatchItem struct {
	Index int       `json:"index"`
	Entry TreeEntry `json:"entry"`
}

// manifestContentDigest covers the replicated content and ignores the snapshot identity,
// state and signature, so two snapshots of the same tree compare equal.
func manifestContentDigest(manifest *SnapshotManifest) (string, error) {
	copied := *manifest
	copied.ID, copied.State, copied.CreatedAt, copied.Error = "", "", time.Time{}, ""
	return manifestDigestWithoutLocalChangeIDs(&copied)
}

func manifestsHaveSameContent(a, b *SnapshotManifest) (bool, error) {
	if a == nil || b == nil {
		return false, nil
	}
	left, err := manifestContentDigest(a)
	if err != nil {
		return false, err
	}
	right, err := manifestContentDigest(b)
	if err != nil {
		return false, err
	}
	return left == right, nil
}

func treeEntriesMatchForPatch(a, b TreeEntry) bool {
	if a.Path != b.Path || a.Type != b.Type || a.Size != b.Size || a.Mode != b.Mode ||
		a.ModTimeNS != b.ModTimeNS || a.ChangeID != b.ChangeID || a.LinkTarget != b.LinkTarget ||
		len(a.Chunks) != len(b.Chunks) {
		return false
	}
	for i := range a.Chunks {
		if a.Chunks[i] != b.Chunks[i] {
			return false
		}
	}
	return true
}

func buildManifestPatch(base, target *SnapshotManifest) (manifestPatch, error) {
	if base == nil || target == nil {
		return manifestPatch{}, errors.New("manifest patch needs a base and a target manifest")
	}
	patch := manifestPatch{Base: base.ID, Count: len(target.Files), Manifest: *target}
	patch.Manifest.Files = nil
	baseEntries := make(map[string]TreeEntry, len(base.Files))
	for _, entry := range base.Files {
		baseEntries[entry.Path] = entry
	}
	targetPaths := make(map[string]struct{}, len(target.Files))
	for i := range target.Files {
		entry := target.Files[i]
		targetPaths[entry.Path] = struct{}{}
		if previous, ok := baseEntries[entry.Path]; ok && treeEntriesMatchForPatch(previous, entry) {
			continue
		}
		patch.Changed = append(patch.Changed, manifestPatchItem{Index: i, Entry: entry})
	}
	for _, entry := range base.Files {
		if _, ok := targetPaths[entry.Path]; !ok {
			patch.Removed = append(patch.Removed, entry.Path)
		}
	}
	return patch, nil
}

func applyManifestPatch(base *SnapshotManifest, patch manifestPatch) (*SnapshotManifest, error) {
	if base == nil {
		return nil, errors.New("manifest patch needs a local base manifest")
	}
	if patch.Base == "" || patch.Base != base.ID {
		return nil, fmt.Errorf("manifest patch base %q does not match local base %q", patch.Base, base.ID)
	}
	if patch.Count < 0 {
		return nil, errors.New("manifest patch has a negative entry count")
	}
	removed := make(map[string]struct{}, len(patch.Removed))
	for _, path := range patch.Removed {
		removed[path] = struct{}{}
	}
	changed := make(map[string]struct{}, len(patch.Changed))
	for i, item := range patch.Changed {
		if item.Index < 0 || item.Index >= patch.Count {
			return nil, errors.New("manifest patch entry index is out of range")
		}
		if i > 0 && item.Index <= patch.Changed[i-1].Index {
			return nil, errors.New("manifest patch entries are not ordered by index")
		}
		changed[item.Entry.Path] = struct{}{}
	}
	rebuilt := patch.Manifest
	rebuilt.Files = make([]TreeEntry, 0, patch.Count)
	next := 0
	for _, entry := range base.Files {
		for next < len(patch.Changed) && patch.Changed[next].Index == len(rebuilt.Files) {
			rebuilt.Files = append(rebuilt.Files, patch.Changed[next].Entry)
			next++
		}
		if _, ok := removed[entry.Path]; ok {
			continue
		}
		if _, ok := changed[entry.Path]; ok {
			continue
		}
		rebuilt.Files = append(rebuilt.Files, entry)
	}
	for next < len(patch.Changed) && patch.Changed[next].Index == len(rebuilt.Files) {
		rebuilt.Files = append(rebuilt.Files, patch.Changed[next].Entry)
		next++
	}
	if next != len(patch.Changed) || len(rebuilt.Files) != patch.Count {
		return nil, fmt.Errorf("manifest patch rebuilt %d of %d entries", len(rebuilt.Files), patch.Count)
	}
	if rebuilt.State != "ready" {
		for i := range rebuilt.Files {
			rebuilt.Files[i].LocalChangeID = ""
		}
	}
	return &rebuilt, nil
}

// decodeBoundedManifestPatchResponse reads a patch with the manifest size budget, which
// is larger than the generic JSON response limit.
func decodeBoundedManifestPatchResponse(resp *http.Response) (manifestPatch, error) {
	response, err := newBoundedEncodedResponse(resp, maxEncodedManifestSize)
	if err != nil {
		return manifestPatch{}, err
	}
	data, decodeErr := io.ReadAll(io.LimitReader(response.reader, maxManifestSize+1))
	if response.exceedsLimit() {
		decodeErr = errors.New("encoded manifest patch response exceeds maximum size")
	}
	if closeErr := response.Close(); decodeErr == nil {
		decodeErr = closeErr
	}
	if decodeErr != nil {
		return manifestPatch{}, decodeErr
	}
	if int64(len(data)) > maxManifestSize {
		return manifestPatch{}, errors.New("manifest patch response exceeds maximum size")
	}
	var patch manifestPatch
	if err := newManifestDecoder(bytes.NewReader(data)).Decode(&patch); err != nil {
		return manifestPatch{}, err
	}
	return patch, nil
}

func writeManifestPatchMaybeGzip(w http.ResponseWriter, r *http.Request, patch manifestPatch) {
	if rejectUnacceptableReplicationEncoding(w, r) {
		return
	}
	w.Header().Add("Vary", "Accept-Encoding")
	body, err := marshalManifestJSON(patch)
	if err != nil {
		log.Error("Cannot encode replication manifest patch: base=%s error=%v", patch.Base, err)
		http.Error(w, "encode replication manifest patch", http.StatusInternalServerError)
		return
	}
	log.Info("Wrote replication manifest patch: base=%s entries=%d changed=%d removed=%d bytes=%d", patch.Base, patch.Count, len(patch.Changed), len(patch.Removed), len(body))
	if requestPrefersGzip(r) {
		writer, err := gzip.NewWriterLevel(w, gzip.BestSpeed)
		if err == nil {
			w.Header().Set("Content-Encoding", "gzip")
			w.Header().Set("Content-Type", manifestPatchContentType)
			_, writeErr := writer.Write(body)
			if closeErr := writer.Close(); writeErr != nil || closeErr != nil {
				log.Debug("Failed to write compressed replication manifest patch: base=%s error=%v", patch.Base, errors.Join(writeErr, closeErr))
			}
			return
		}
		if requestRequiresGzip(r) {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
	w.Header().Set("Content-Type", manifestPatchContentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	if _, err := w.Write(body); err != nil {
		log.Debug("Failed to write replication manifest patch: base=%s error=%v", patch.Base, err)
	}
}
