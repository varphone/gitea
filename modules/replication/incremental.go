// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitea.dev/modules/json"
	"gitea.dev/modules/log"
	"gitea.dev/modules/setting"
)

const (
	incrementalFormatVersion = 2
	chunkMinSize             = 256 << 10
	chunkAverageSize         = 1 << 20
	chunkMaxSize             = 4 << 20
	maxManifestSymlinkDepth  = 40
	maxManifestSize          = 256 << 20
	manifestEncodeBatchSize  = 1 << 20
)

var (
	errManifestTrailingData   = errors.New("incremental manifest contains oversized or trailing data")
	errManifestTooLarge       = errors.New("incremental manifest exceeds maximum size")
	errIncrementalTreeChanged = errors.New("filesystem tree changed during scan")
)

type ChunkDescriptor struct {
	Hash   string `json:"hash"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
}

type TreeEntry struct {
	Path          string            `json:"path"`
	Type          string            `json:"type"`
	Mode          uint32            `json:"mode"`
	Size          int64             `json:"size,omitempty"`
	ModTimeNS     int64             `json:"mtime_ns,omitempty"`
	ChangeID      string            `json:"change_id,omitempty"`
	LocalChangeID string            `json:"local_change_id,omitempty"`
	LinkTarget    string            `json:"link_target,omitempty"`
	Chunks        []ChunkDescriptor `json:"chunks,omitempty"`
}

type chunkLocation struct {
	Path         string
	Offset, Size int64
}

func gearValue(b byte) uint64 {
	x := uint64(b) + 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

var gearValueTable = func() [256]uint64 {
	var table [256]uint64
	for i := range table {
		table[i] = gearValue(byte(i))
	}
	return table
}()

// splitFile uses content-defined boundaries, so an insertion does not
// invalidate every following chunk as fixed-size blocks would.
func splitFile(ctx context.Context, path string) ([]ChunkDescriptor, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	f, info, err := openRegularFile(path, info)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if info.Size() == 0 {
		return nil, nil
	}
	bufferSize := 128 << 10
	if info.Size() < int64(bufferSize) {
		bufferSize = int(info.Size())
	}
	buffer := make([]byte, bufferSize)
	hash := sha256.New()
	var chunks []ChunkDescriptor
	var rolling uint64
	var offset, start, size int64
	flush := func() {
		chunks = append(chunks, ChunkDescriptor{hex.EncodeToString(hash.Sum(nil)), start, size})
		hash.Reset()
		rolling, start, size = 0, offset, 0
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, readErr := f.Read(buffer)
		segmentStart := 0
		for i, b := range buffer[:n] {
			rolling = (rolling << 1) + gearValueTable[b]
			offset++
			size++
			if size >= chunkMinSize && ((rolling&uint64(chunkAverageSize-1)) == 0 || size >= chunkMaxSize) {
				_, _ = hash.Write(buffer[segmentStart : i+1])
				flush()
				segmentStart = i + 1
			}
		}
		if segmentStart < n {
			_, _ = hash.Write(buffer[segmentStart:n])
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	if size > 0 {
		flush()
	}
	return chunks, nil
}

var chunkFileForManifest = splitFile

func validateTreePath(rel string) error {
	if len(rel) > 4096 {
		return errors.New("manifest path is too long")
	}
	clean := filepath.Clean(filepath.FromSlash(rel))
	if rel == "" || clean == "." || filepath.IsAbs(clean) || clean == ".." ||
		strings.HasPrefix(clean, ".."+string(filepath.Separator)) || filepath.ToSlash(clean) != rel {
		return fmt.Errorf("unsafe manifest path %q", rel)
	}
	return nil
}

func validateTreeLink(rel, target string) error {
	if err := validateTreePath(rel); err != nil {
		return err
	}
	if target == "" || len(target) > 4096 {
		return errors.New("invalid symlink target length")
	}
	link := filepath.Clean(filepath.FromSlash(target))
	if filepath.IsAbs(link) {
		return fmt.Errorf("unsafe absolute symlink %q", target)
	}
	resolved := filepath.Clean(filepath.Join(filepath.Dir(filepath.FromSlash(rel)), link))
	if resolved == ".." || strings.HasPrefix(resolved, ".."+string(filepath.Separator)) {
		return fmt.Errorf("unsafe symlink target %q", target)
	}
	return nil
}

func validateTreeTopology(entries []TreeEntry) error {
	entryDirectories := make(map[string]bool, len(entries))
	symlinkTargets := make(map[string]string)
	for _, entry := range entries {
		if err := validateTreePath(entry.Path); err != nil {
			return err
		}
		if _, ok := entryDirectories[entry.Path]; ok {
			return fmt.Errorf("duplicate manifest path %q", entry.Path)
		}
		entryDirectories[entry.Path] = entry.Type == "dir"
		if entry.Type == "symlink" {
			symlinkTargets[entry.Path] = entry.LinkTarget
		}
	}
	for _, entry := range entries {
		parent := path.Dir(entry.Path)
		if parent != "." {
			isDirectory, ok := entryDirectories[parent]
			if !ok || !isDirectory {
				return fmt.Errorf("manifest parent %q for %q is not a directory", parent, entry.Path)
			}
		}
		if entry.Type != "symlink" {
			continue
		}
		parts := []string{}
		if parent != "." {
			parts = strings.Split(parent, "/")
		}
		pending := strings.Split(filepath.ToSlash(entry.LinkTarget), "/")
		followed := 1
		for len(pending) > 0 {
			part := pending[0]
			pending = pending[1:]
			switch part {
			case "", ".":
				continue
			case "..":
				if len(parts) == 0 {
					return fmt.Errorf("unsafe symlink target %q", entry.LinkTarget)
				}
				parts = parts[:len(parts)-1]
			default:
				parts = append(parts, part)
				if target, ok := symlinkTargets[strings.Join(parts, "/")]; ok {
					followed++
					if followed > maxManifestSymlinkDepth {
						return fmt.Errorf("symlink target %q traverses too many links", entry.LinkTarget)
					}
					parts = parts[:len(parts)-1]
					pending = append(strings.Split(filepath.ToSlash(target), "/"), pending...)
				}
			}
		}
	}
	return nil
}

func scanIncrementalTree(ctx context.Context, root string) (*SnapshotManifest, error) {
	return scanIncrementalTreeWithOptions(ctx, root, nil, true)
}

func scanIncrementalTreeWithBase(ctx context.Context, root string, base *SnapshotManifest) (*SnapshotManifest, error) {
	return scanIncrementalTreeWithOptions(ctx, root, base, false)
}

func scanPathError(rel string, err error) error {
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: %s", errIncrementalTreeChanged, rel)
	}
	return fmt.Errorf("scan %q: %w", rel, err)
}

func replicationTreeExclusions(root string) map[string]string {
	type candidate struct {
		path string
		kind string
	}
	candidates := make([]candidate, 0, 3)
	if setting.Indexer.IssueType == "bleve" {
		candidates = append(candidates, candidate{path: setting.Indexer.IssuePath, kind: "issue index"})
	}
	if setting.Indexer.RepoIndexerEnabled && setting.Indexer.RepoType == "bleve" {
		candidates = append(candidates, candidate{path: setting.Indexer.RepoPath, kind: "code index"})
	}
	if setting.RepoArchive.Storage != nil && setting.RepoArchive.Storage.Type == setting.LocalStorageType {
		candidates = append(candidates, candidate{path: setting.RepoArchive.Storage.Path, kind: "repository archive cache"})
	}
	if len(candidates) == 0 {
		return nil
	}

	root, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return nil
	}
	appWorkPath, err := filepath.Abs(filepath.Clean(setting.AppWorkPath))
	if err != nil || appWorkPath != root {
		return nil
	}
	resolvedRoot, err := resolvedPath(root)
	if err != nil || resolvedRoot != root {
		return nil
	}
	appDataPath, err := resolvedPath(setting.AppDataPath)
	if err != nil || !isWithin(root, appDataPath) {
		return nil
	}
	protectedPaths := []string{setting.Database.Path, setting.RepoRootPath, setting.CustomPath}
	for _, storage := range []*setting.Storage{
		setting.Attachment.Storage, setting.LFS.Storage, setting.Avatar.Storage,
		setting.RepoAvatar.Storage, setting.Packages.Storage,
		setting.Actions.LogStorage, setting.Actions.ArtifactStorage,
	} {
		if storage != nil {
			protectedPaths = append(protectedPaths, storage.Path)
		}
	}

	paths := make([]string, len(candidates))
	eligible := make([]bool, len(candidates))
	for i, candidate := range candidates {
		if candidate.path == "" || !filepath.IsAbs(candidate.path) {
			continue
		}
		candidatePath, err := filepath.Abs(filepath.Clean(candidate.path))
		if err != nil || candidatePath == root || !isWithin(root, candidatePath) {
			continue
		}
		// Keep APP_DATA_PATH and its contents whenever an exclusion would hide the whole data directory.
		if isWithin(candidatePath, appDataPath) {
			continue
		}
		resolvedCandidate, resolveErr := resolvedPath(candidatePath)
		if resolveErr != nil || resolvedCandidate != candidatePath {
			continue
		}
		unsafe := false
		for _, protected := range protectedPaths {
			if protected == "" {
				continue
			}
			if !filepath.IsAbs(protected) {
				protected = filepath.Join(root, protected)
			}
			resolvedProtected, resolveErr := resolvedPath(protected)
			if resolveErr != nil || isWithin(candidatePath, resolvedProtected) || isWithin(resolvedProtected, candidatePath) {
				unsafe = true
				break
			}
		}
		if unsafe {
			continue
		}
		paths[i] = candidatePath
		eligible[i] = true
	}

	excluded := make(map[string]string, len(candidates))
	for i, candidate := range candidates {
		if !eligible[i] {
			continue
		}
		overlaps := false
		for j := range candidates {
			if i != j && eligible[j] && (isWithin(paths[i], paths[j]) || isWithin(paths[j], paths[i])) {
				overlaps = true
				break
			}
		}
		if overlaps {
			continue
		}
		rel, err := filepath.Rel(root, paths[i])
		if err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			excluded[filepath.ToSlash(rel)] = candidate.kind
		}
	}
	return excluded
}

func scanIncrementalTreeWithOptions(ctx context.Context, root string, base *SnapshotManifest, verifyAll bool) (*SnapshotManifest, error) {
	return scanIncrementalTreeWithOptionsForTask(ctx, root, base, verifyAll, "local")
}

func scanIncrementalTreeWithOptionsForTask(ctx context.Context, root string, base *SnapshotManifest, verifyAll bool, snapshotID string) (*SnapshotManifest, error) {
	scanStarted := time.Now()
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return nil, fmt.Errorf("inspect replication root %q: %w", root, err)
	}
	if !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("APP_WORK_PATH %q must be a real directory", root)
	}
	m := &SnapshotManifest{
		FormatVersion: incrementalFormatVersion, GiteaVersion: setting.AppVer,
		AppWorkPath: setting.AppWorkPath, Snapshot: Snapshot{RootMode: uint32(rootInfo.Mode().Perm())},
	}
	if base != nil {
		m.FullScanAt = base.FullScanAt
	}
	baseEntries := map[string]TreeEntry{}
	if base != nil {
		for _, entry := range base.Files {
			baseEntries[entry.Path] = entry
		}
	}
	excludedPaths := replicationTreeExclusions(root)
	var entriesSeen, filesSeen, filesCompleted, filesReused, filesChunked, logicalFileBytesSeen, contentBytesChunked atomic.Int64
	progressDone := make(chan struct{})
	var progressWorkers sync.WaitGroup
	progressWorkers.Go(func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-progressDone:
				return
			case <-ticker.C:
				log.Info("Replication tree scan progress: snapshot=%s entries_seen=%d files_seen=%d files_completed=%d reused_files=%d chunked_files=%d logical_file_bytes_seen=%d content_bytes_chunked=%d verify_all=%t elapsed=%s", snapshotID, entriesSeen.Load(), filesSeen.Load(), filesCompleted.Load(), filesReused.Load(), filesChunked.Load(), logicalFileBytesSeen.Load(), contentBytesChunked.Load(), verifyAll, time.Since(scanStarted))
			}
		}
	})
	defer func() {
		close(progressDone)
		progressWorkers.Wait()
	}()
	err = filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			return scanPathError(filepath.ToSlash(rel), walkErr)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil || rel == "." {
			return err
		}
		entriesSeen.Add(1)
		rel = filepath.ToSlash(rel)
		if kind, exclude := excludedPaths[rel]; exclude && info.IsDir() {
			current, err := os.Lstat(path)
			if err != nil {
				return scanPathError(rel, err)
			}
			if !current.IsDir() || current.Mode()&os.ModeSymlink != 0 || !os.SameFile(info, current) {
				return fmt.Errorf("%w: %s", errIncrementalTreeChanged, rel)
			}
			log.Info("Excluded regenerable data from replication snapshot: snapshot=%s kind=%s path=%s", snapshotID, kind, rel)
			return filepath.SkipDir
		}
		e := TreeEntry{Path: rel, Mode: uint32(info.Mode().Perm())}
		switch {
		case info.IsDir():
			e.Type = "dir"
			e.ModTimeNS = info.ModTime().UnixNano()
		case info.Mode()&os.ModeSymlink != 0:
			e.Type = "symlink"
			e.LinkTarget, err = os.Readlink(path)
			if err != nil {
				return scanPathError(rel, err)
			}
			if err := validateTreeLink(rel, e.LinkTarget); err != nil {
				return scanPathError(rel, err)
			}
		case info.Mode().IsRegular():
			e.Type, e.Size = "file", info.Size()
			if m.Size > math.MaxInt64-e.Size {
				return fmt.Errorf("manifest logical size overflow at %q", rel)
			}
			filesSeen.Add(1)
			logicalFileBytesSeen.Add(e.Size)
			e.ModTimeNS, e.ChangeID = info.ModTime().UnixNano(), fileChangeID(info)
			old, hasOld := baseEntries[rel]
			metadataUnchanged := hasOld && old.Type == "file" && old.Size == info.Size() &&
				old.Mode == uint32(info.Mode().Perm()) && old.ModTimeNS == info.ModTime().UnixNano() &&
				old.ChangeID != "" && old.ChangeID == e.ChangeID
			if metadataUnchanged && !verifyAll {
				e.Chunks = old.Chunks
				m.Size += e.Size
				m.Files = append(m.Files, e)
				filesCompleted.Add(1)
				filesReused.Add(1)
				return nil
			}
			beforeSize, beforeTime, beforeChangeID := info.Size(), info.ModTime(), e.ChangeID
			e.Chunks, err = chunkFileForManifest(ctx, path)
			if err != nil {
				return scanPathError(rel, err)
			}
			after, err := os.Lstat(path)
			if err != nil {
				return scanPathError(rel, err)
			}
			if !after.Mode().IsRegular() || after.Size() != beforeSize || after.ModTime() != beforeTime || fileChangeID(after) != beforeChangeID {
				return fmt.Errorf("%w: %s", errIncrementalTreeChanged, rel)
			}
			if verifyAll && metadataUnchanged && !sameChunks(e.Chunks, old.Chunks) {
				return fmt.Errorf("content verification failed with unchanged metadata: %s", rel)
			}
			m.Size += e.Size
			filesCompleted.Add(1)
			filesChunked.Add(1)
			contentBytesChunked.Add(e.Size)
		default:
			return fmt.Errorf("unsupported filesystem entry %q", rel)
		}
		m.Files = append(m.Files, e)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := validateTreeTopology(m.Files); err != nil {
		return nil, err
	}
	if verifyAll {
		m.FullScanAt = time.Now().UTC()
	}
	m.FileCount = len(m.Files)
	m.SHA256, err = manifestDigest(m)
	return m, err
}

func sameChunks(a, b []ChunkDescriptor) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type manifestSizeWriter struct {
	writer io.Writer
	size   int64
}

func (w *manifestSizeWriter) Write(data []byte) (int, error) {
	if int64(len(data)) > int64(maxManifestSize)-w.size {
		return 0, errManifestTooLarge
	}
	n, err := w.writer.Write(data)
	w.size += int64(n)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	return n, err
}

func treeEntryEncodedSizeLowerBound(entry *TreeEntry) int64 {
	size := int64(30 + len(entry.Path) + len(entry.Type) + len(entry.ChangeID) + len(entry.LocalChangeID) + len(entry.LinkTarget))
	if entry.LocalChangeID != "" {
		size += int64(len(`,"local_change_id":""`))
	}
	for _, chunk := range entry.Chunks {
		chunkSize := int64(32 + len(chunk.Hash))
		if chunkSize > int64(maxManifestSize)-size {
			return int64(maxManifestSize) + 1
		}
		size += chunkSize
	}
	if len(entry.Chunks) > 0 {
		size += 11
	}
	return size
}

func writeManifestJSON(writer io.Writer, manifest *SnapshotManifest, clearDigest bool) error {
	manifestCopy := *manifest
	if clearDigest {
		manifestCopy.SHA256 = ""
		manifestCopy.Signature = ""
	}
	// Files is the final manifest field, so it can be encoded in bounded batches.
	files := manifestCopy.Files
	manifestCopy.Files = nil
	header, err := json.Marshal(&manifestCopy)
	if err != nil {
		return err
	}
	sizedWriter := &manifestSizeWriter{writer: writer}
	if len(files) == 0 {
		_, err := sizedWriter.Write(header)
		return err
	}
	if len(header) == 0 || header[len(header)-1] != '}' {
		return errors.New("invalid encoded incremental manifest header")
	}
	if _, err := sizedWriter.Write(header[:len(header)-1]); err != nil {
		return err
	}
	if _, err := sizedWriter.Write([]byte(`,"files":[`)); err != nil {
		return err
	}
	for start := 0; start < len(files); {
		end := start
		minimumSize := int64(0)
		for end < len(files) {
			entrySize := treeEntryEncodedSizeLowerBound(&files[end])
			if end > 0 {
				entrySize++
			}
			if entrySize+2 > int64(maxManifestSize)-sizedWriter.size-minimumSize {
				return errManifestTooLarge
			}
			if end > start && minimumSize+entrySize > manifestEncodeBatchSize {
				break
			}
			minimumSize += entrySize
			end++
		}
		if end == start {
			end++
		}
		if start > 0 {
			if _, err := sizedWriter.Write([]byte{','}); err != nil {
				return err
			}
		}
		batch, err := json.Marshal(files[start:end])
		if err != nil {
			return err
		}
		if len(batch) < 2 {
			return errors.New("invalid encoded incremental manifest entries")
		}
		if _, err := sizedWriter.Write(batch[1 : len(batch)-1]); err != nil {
			return err
		}
		start = end
	}
	_, err = sizedWriter.Write([]byte(`]}`))
	return err
}

func manifestDigest(manifest *SnapshotManifest) (string, error) {
	h := sha256.New()
	if err := writeManifestJSON(h, manifest, true); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func signIncrementalManifest(manifest *SnapshotManifest, token string) error {
	digest, err := manifestDigest(manifest)
	if err != nil {
		return err
	}
	manifest.SHA256 = digest
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = io.WriteString(mac, digest)
	manifest.Signature = hex.EncodeToString(mac.Sum(nil))
	return nil
}

func verifyIncrementalSignature(manifest *SnapshotManifest, token string) bool {
	if len(manifest.Signature) != sha256.Size*2 {
		return false
	}
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = io.WriteString(mac, manifest.SHA256)
	return hmac.Equal([]byte(manifest.Signature), []byte(hex.EncodeToString(mac.Sum(nil))))
}

// compatibleGiteaVersion accepts builds from the same Gitea release. The
// build metadata contains the local commit count and revision, which changes
// on every local rebuild but does not change the replication wire format.
func compatibleGiteaVersion(source, standby string) bool {
	source, _, _ = strings.Cut(strings.TrimSpace(source), "+")
	standby, _, _ = strings.Cut(strings.TrimSpace(standby), "+")
	return source != "" && source == standby
}

func validateManifestIdentity(manifest *SnapshotManifest, token string) error {
	if !compatibleGiteaVersion(manifest.GiteaVersion, setting.AppVer) {
		return fmt.Errorf("Gitea release mismatch: source %s standby %s", manifest.GiteaVersion, setting.AppVer)
	}
	if filepath.Clean(manifest.AppWorkPath) != filepath.Clean(setting.AppWorkPath) {
		return fmt.Errorf("APP_WORK_PATH mismatch: source %s standby %s", manifest.AppWorkPath, setting.AppWorkPath)
	}
	if !verifyIncrementalSignature(manifest, token) {
		return errors.New("incremental manifest signature is invalid")
	}
	if !hmac.Equal([]byte(manifest.InstanceFingerprint), []byte(instanceFingerprint(token))) {
		return errors.New("standby instance secrets do not match the primary")
	}
	return nil
}

func validateIncrementalManifest(m *SnapshotManifest) error {
	if m.FormatVersion != incrementalFormatVersion || !validSnapshotID(m.ID) || m.Size < 0 ||
		m.CreatedAt.IsZero() || m.RootMode == 0 || m.RootMode > 0o777 || m.FileCount != len(m.Files) {
		return errors.New("invalid incremental manifest metadata")
	}
	switch m.State {
	case "preflight", "transferring", "ready", "failed":
	default:
		return errors.New("invalid incremental manifest state")
	}
	var logicalSize int64
	for _, e := range m.Files {
		if e.Mode > 0o777 || len(e.ChangeID) > 128 || len(e.LocalChangeID) > 128 {
			return fmt.Errorf("invalid metadata in %q", e.Path)
		}
		switch e.Type {
		case "dir":
			if e.Size != 0 || e.LocalChangeID != "" || e.LinkTarget != "" || len(e.Chunks) != 0 {
				return fmt.Errorf("invalid directory fields in %q", e.Path)
			}
		case "symlink":
			if e.Size != 0 || e.LocalChangeID != "" || len(e.Chunks) != 0 {
				return fmt.Errorf("invalid symlink fields in %q", e.Path)
			}
			if err := validateTreeLink(e.Path, e.LinkTarget); err != nil {
				return fmt.Errorf("invalid symlink %q: %w", e.Path, err)
			}
		case "file":
			if e.LinkTarget != "" {
				return fmt.Errorf("invalid file link target in %q", e.Path)
			}
			if e.LocalChangeID != "" && m.State != "ready" {
				return fmt.Errorf("local file identity is only valid in ready manifest %q", e.Path)
			}
			var offset int64
			for _, c := range e.Chunks {
				if len(c.Hash) != sha256.Size*2 || c.Offset != offset || c.Size <= 0 || c.Size > chunkMaxSize {
					return fmt.Errorf("invalid chunk in %q", e.Path)
				}
				decoded, err := hex.DecodeString(c.Hash)
				if err != nil || hex.EncodeToString(decoded) != c.Hash || offset > math.MaxInt64-c.Size {
					return fmt.Errorf("invalid chunk hash or size in %q", e.Path)
				}
				offset += c.Size
			}
			if offset != e.Size || (e.Size == 0 && len(e.Chunks) != 0) {
				return fmt.Errorf("invalid file size in %q", e.Path)
			}
			if logicalSize > math.MaxInt64-e.Size {
				return fmt.Errorf("manifest logical size overflow at %q", e.Path)
			}
			logicalSize += e.Size
		default:
			return fmt.Errorf("invalid entry type %q", e.Type)
		}
	}
	if err := validateTreeTopology(m.Files); err != nil {
		return err
	}
	if logicalSize != m.Size {
		return errors.New("manifest logical size mismatch")
	}
	digest, err := manifestDigest(m)
	if err != nil || digest != m.SHA256 {
		return errors.New("manifest digest mismatch")
	}
	return nil
}

func indexManifest(m *SnapshotManifest) map[string]chunkLocation {
	index := make(map[string]chunkLocation)
	for _, e := range m.Files {
		for _, c := range e.Chunks {
			if _, ok := index[c.Hash]; !ok {
				index[c.Hash] = chunkLocation{e.Path, c.Offset, c.Size}
			}
		}
	}
	return index
}

func indexManifestWithAlternates(m *SnapshotManifest) (map[string]chunkLocation, map[string][]chunkLocation) {
	primary, alternates, _ := indexManifestWithAlternatesContext(context.Background(), m)
	return primary, alternates
}

func indexManifestWithAlternatesContext(ctx context.Context, m *SnapshotManifest) (map[string]chunkLocation, map[string][]chunkLocation, error) {
	primary := make(map[string]chunkLocation)
	alternates := make(map[string][]chunkLocation)
	for _, entry := range m.Files {
		for _, chunk := range entry.Chunks {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			candidate := chunkLocation{entry.Path, chunk.Offset, chunk.Size}
			first, ok := primary[chunk.Hash]
			if !ok {
				primary[chunk.Hash] = candidate
				continue
			}
			addChunkAlternate(alternates, chunk.Hash, first, candidate)
		}
	}
	return primary, alternates, nil
}

func addChunkAlternate(alternates map[string][]chunkLocation, hash string, primary, candidate chunkLocation) {
	locations := alternates[hash]
	if candidate.Path != primary.Path {
		for _, location := range locations {
			if location.Path == candidate.Path {
				return
			}
		}
	}
	if len(locations) < maxChunkSourceAlternates {
		alternates[hash] = append(locations, candidate)
		return
	}
	if candidate.Path != primary.Path {
		for i := range slices.Backward(locations) {
			if locations[i].Path == primary.Path {
				locations[i] = candidate
				alternates[hash] = locations
				return
			}
		}
	}
}

func readChunk(root string, loc chunkLocation, expected string) ([]byte, error) {
	return readChunkFromRoot(root, "", loc, expected)
}

func readChunkFromRoot(root, resolvedRoot string, loc chunkLocation, expected string) ([]byte, error) {
	if err := validateTreePath(loc.Path); err != nil {
		return nil, err
	}
	if loc.Offset < 0 || loc.Size <= 0 || loc.Size > chunkMaxSize || loc.Offset > math.MaxInt64-loc.Size {
		return nil, errors.New("invalid chunk source range")
	}
	path := filepath.Join(root, filepath.FromSlash(loc.Path))
	if resolvedRoot == "" {
		var err error
		resolvedRoot, err = resolvedPath(root)
		if err != nil {
			return nil, err
		}
	}
	path, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("chunk source is not a regular file")
	}
	pathResolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	if !isWithin(resolvedRoot, pathResolved) {
		return nil, errors.New("chunk source resolves outside data root")
	}
	f, _, err := openRegularFile(path, info)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data := make([]byte, loc.Size)
	if _, err := f.ReadAt(data, loc.Offset); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != expected {
		return nil, errors.New("source chunk changed after manifest creation")
	}
	return data, nil
}

func cachePath(cacheDir, hash string) string { return filepath.Join(cacheDir, hash[:2], hash) }

func isLowerHex(value string) bool {
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return value != ""
}

func ensureRealDirectory(path string) error {
	if err := os.Mkdir(path, 0o700); err != nil && !os.IsExist(err) {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("replication chunk cache path %q must be a real directory", path)
	}
	return nil
}

func prepareChunkCache(cacheDir string) error {
	if err := ensureRealDirectory(cacheDir); err != nil {
		return err
	}
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		return err
	}
	removedTemps := 0
	for _, entry := range entries {
		name := entry.Name()
		if len(name) != 2 || !isLowerHex(name) {
			continue
		}
		shardPath := filepath.Join(cacheDir, name)
		if err := ensureRealDirectory(shardPath); err != nil {
			return err
		}
		shardEntries, err := os.ReadDir(shardPath)
		if err != nil {
			return err
		}
		for _, shardEntry := range shardEntries {
			tempName, ok := strings.CutPrefix(shardEntry.Name(), ".")
			if !ok {
				continue
			}
			hash, suffix, ok := strings.Cut(tempName, ".tmp-")
			if !ok || suffix == "" || len(hash) != 64 || !isLowerHex(hash) || !strings.HasPrefix(hash, name) {
				continue
			}
			tempPath := filepath.Join(shardPath, shardEntry.Name())
			info, err := os.Lstat(tempPath)
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 {
				continue
			}
			if err := os.Remove(tempPath); err != nil {
				return fmt.Errorf("remove orphaned replication chunk cache temp file %q: %w", tempPath, err)
			}
			removedTemps++
		}
	}
	if removedTemps > 0 {
		log.Info("Removed orphaned replication chunk cache temp files: count=%d", removedTemps)
	}
	return nil
}

func storeChunk(cacheDir, hash string, data []byte) error {
	return storeChunkWithDirectorySync(cacheDir, hash, data, true)
}

func storeChunkForBatch(cacheDir, hash string, data []byte) error {
	return storeChunkWithDirectorySync(cacheDir, hash, data, false)
}

func storeChunkWithDirectorySync(cacheDir, hash string, data []byte, syncDirectories bool) error {
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != hash {
		return errors.New("received chunk hash mismatch")
	}
	path := cachePath(cacheDir, hash)
	dir := filepath.Dir(path)
	if err := ensureRealDirectory(cacheDir); err != nil {
		return err
	}
	if err := ensureRealDirectory(dir); err != nil {
		return err
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode().IsRegular() && verifyFile(path, hash) == nil {
			return nil
		}
		if info.IsDir() {
			if err := makeTreeRemovable(context.Background(), path); err != nil {
				return fmt.Errorf("make invalid chunk cache directory removable: %w", err)
			}
			if err := os.RemoveAll(path); err != nil {
				return fmt.Errorf("remove invalid chunk cache directory: %w", err)
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	file, err := os.CreateTemp(dir, "."+hash+".tmp-*")
	if err != nil {
		return err
	}
	tmp := file.Name()
	defer os.Remove(tmp)
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	n, writeErr := file.Write(data)
	if writeErr == nil && n != len(data) {
		writeErr = io.ErrShortWrite
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if syncDirectories {
		return errors.Join(syncDirectory(cacheDir), syncDirectory(dir))
	}
	return nil
}

func syncChunkCacheDirectories(cacheDir string, shardSet *sync.Map) error {
	var shards []string
	var syncErrors []error
	shardSet.Range(func(key, _ any) bool {
		shard, ok := key.(string)
		if !ok {
			syncErrors = append(syncErrors, fmt.Errorf("invalid chunk cache shard key %T", key))
			return true
		}
		shards = append(shards, shard)
		return true
	})
	slices.Sort(shards)
	if len(shards) == 0 {
		return errors.Join(syncErrors...)
	}
	if err := syncDirectory(cacheDir); err != nil {
		syncErrors = append(syncErrors, fmt.Errorf("sync chunk cache directory: %w", err))
	}
	for _, name := range shards {
		if len(name) != 2 || !isLowerHex(name) {
			syncErrors = append(syncErrors, fmt.Errorf("invalid chunk cache shard %q", name))
			continue
		}
		shardPath := filepath.Join(cacheDir, name)
		info, err := os.Lstat(shardPath)
		if err != nil {
			syncErrors = append(syncErrors, fmt.Errorf("inspect chunk cache shard %q: %w", name, err))
			continue
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			syncErrors = append(syncErrors, fmt.Errorf("chunk cache shard %q is not a real directory", name))
			continue
		}
		if err := syncDirectory(shardPath); err != nil {
			syncErrors = append(syncErrors, fmt.Errorf("sync chunk cache shard %q: %w", name, err))
		}
	}
	return errors.Join(syncErrors...)
}

func openManifestFile(path string) (*os.File, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("incremental manifest is not a regular file")
	}
	file, openedInfo, err := openRegularFile(path, info)
	if err != nil {
		return nil, fmt.Errorf("open incremental manifest: %w", err)
	}
	if openedInfo.Size() > int64(maxManifestSize) {
		_ = file.Close()
		return nil, errors.New("incremental manifest exceeds maximum size")
	}
	return file, nil
}

func readManifestData(path string) ([]byte, error) {
	file, err := openManifestFile(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, int64(maxManifestSize)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxManifestSize {
		return nil, errors.New("incremental manifest exceeds maximum size")
	}
	return data, nil
}

func loadManifestFile(path string) (*SnapshotManifest, error) {
	data, err := readManifestData(path)
	if err != nil {
		return nil, err
	}
	var m SnapshotManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: %v", errManifestTrailingData, err)
	}
	if err := validateIncrementalManifest(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

func loadTrustedManifest(path, token, state string) (*SnapshotManifest, error) {
	return loadTrustedManifestStates(path, token, state)
}

func loadTrustedManifestStates(path, token string, states ...string) (*SnapshotManifest, error) {
	manifest, err := loadManifestFile(path)
	if err != nil {
		return nil, err
	}
	if !slices.Contains(states, manifest.State) {
		return nil, fmt.Errorf("manifest state is %q, expected one of %q", manifest.State, states)
	}
	if err := validateManifestIdentity(manifest, token); err != nil {
		return nil, err
	}
	return manifest, nil
}
