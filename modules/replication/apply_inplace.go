// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"gitea.dev/modules/log"
	"gitea.dev/modules/setting"
)

// inPlaceApplyStats reports what the in-place reconciler changed.
type inPlaceApplyStats struct {
	FilesReused     int
	FilesCreated    int
	FilesPatched    int
	FilesReplaced   int
	Symlinks        int
	ChunksReused    int
	ChunksPatched   int
	ChunksFromCache int
	ChunksFetched   int
	BytesWritten    int64
	EntriesDeleted  int
	Directories     int
}

type inPlaceApplyOptions struct {
	Manifest *SnapshotManifest
	Previous *SnapshotManifest
	CacheDir string
	// Fetch returns the bytes of a chunk that is unavailable locally. It must verify the
	// chunk hash before returning.
	Fetch func(ctx context.Context, hash string) ([]byte, error)
	// LocalCandidates are chunk locations the planning pass already found in the standby
	// tree. When empty the applier builds the previous manifest index on first use.
	LocalCandidates map[string][]chunkLocation
	// CachedHashes are the chunks the transfer pass stored in the chunk cache. Occurrence
	// counting for cache release is limited to them.
	CachedHashes map[string]struct{}
}

// applyInPlace reconciles the standby data root to manifest by patching changed chunks in
// place. It never creates a second data tree.
func applyInPlace(ctx context.Context, opts inPlaceApplyOptions) (*inPlaceApplyStats, error) {
	manifest := opts.Manifest
	if manifest == nil {
		return nil, errors.New("in-place apply requires a target manifest")
	}
	root := filepath.Clean(setting.AppWorkPath)
	if root == "" || root == "." || root == string(filepath.Separator) {
		return nil, fmt.Errorf("refusing to apply replication data in %q", setting.AppWorkPath)
	}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return nil, fmt.Errorf("inspect standby data root %q: %w", root, err)
	}
	if !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("standby data root %q must be a real directory", root)
	}
	started := time.Now()
	excluded := replicationTreeExclusions(root)
	localOnly := replicationLocalOnlyFiles(root)
	previousEntries := make(map[string]TreeEntry, len(manifest.Files))
	if opts.Previous != nil {
		for _, entry := range opts.Previous.Files {
			previousEntries[entry.Path] = entry
		}
	}
	target := make(map[string]struct{}, len(manifest.Files))
	directories := make([]TreeEntry, 0, 64)
	for _, entry := range manifest.Files {
		target[entry.Path] = struct{}{}
		if entry.Type == "dir" {
			directories = append(directories, entry)
		}
	}
	slices.SortFunc(directories, func(a, b TreeEntry) int {
		depthA, depthB := strings.Count(a.Path, "/"), strings.Count(b.Path, "/")
		if depthA != depthB {
			return depthA - depthB
		}
		return strings.Compare(a.Path, b.Path)
	})
	stats := &inPlaceApplyStats{}
	touchedDirs := map[string]struct{}{".": {}}
	touchDir := func(rel string) {
		directory := path.Dir(rel)
		touchedDirs[directory] = struct{}{}
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return nil, fmt.Errorf("make standby data root writable: %w", err)
	}
	// Directories become writable before their children are reconciled; the manifest modes
	// and timestamps are restored after the whole tree matches.
	for _, entry := range directories {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		dst := filepath.Join(root, filepath.FromSlash(entry.Path))
		info, err := os.Lstat(dst)
		switch {
		case err == nil && info.IsDir() && info.Mode()&os.ModeSymlink == 0:
			if err := os.Chmod(dst, 0o700); err != nil {
				return nil, applyPathError("make directory writable", entry.Path, err)
			}
		case err == nil:
			if err := removePathForReplace(dst); err != nil {
				return nil, applyPathError("remove conflicting directory entry", entry.Path, err)
			}
			if err := os.MkdirAll(dst, 0o700); err != nil {
				return nil, applyPathError("create directory", entry.Path, err)
			}
			stats.Directories++
		case os.IsNotExist(err):
			if err := os.MkdirAll(dst, 0o700); err != nil {
				return nil, applyPathError("create directory", entry.Path, err)
			}
			stats.Directories++
		default:
			return nil, applyPathError("inspect directory", entry.Path, err)
		}
		touchDir(entry.Path)
	}
	remainingUses := make(map[string]int)
	for _, entry := range manifest.Files {
		for _, chunk := range entry.Chunks {
			if chunk.Zero {
				continue
			}
			if _, cached := opts.CachedHashes[chunk.Hash]; !cached {
				continue
			}
			remainingUses[chunk.Hash]++
		}
	}
	source := &inPlaceChunkSource{
		root: root, resolvedRoot: root, cacheDir: opts.CacheDir,
		candidates: opts.LocalCandidates, previous: opts.Previous, fetch: opts.Fetch,
		remaining: remainingUses,
		memory:    newStageChunkMemoryCache(chunkMemoryCacheLimit), stats: stats,
	}
	chunkBuffer := make([]byte, chunkMaxSize)
	filesStarted := 0
	for i := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entry := &manifest.Files[i]
		switch entry.Type {
		case "dir":
			continue
		case "file":
			filesStarted++
			previous, hasPrevious := previousEntries[entry.Path]
			if err := applyInPlaceFile(ctx, root, source, entry, previous, hasPrevious, stats, chunkBuffer); err != nil {
				return nil, err
			}
			touchDir(entry.Path)
		case "symlink":
			if err := applyInPlaceSymlink(ctx, root, entry, previousEntries, stats); err != nil {
				return nil, err
			}
			touchDir(entry.Path)
		default:
			return nil, fmt.Errorf("unsupported manifest entry type %q at %q", entry.Type, entry.Path)
		}
		if filesStarted > 0 && filesStarted&0x3ff == 0 {
			log.Info("In-place apply progress: snapshot=%s files_started=%d patched=%d created=%d reused=%d bytes_written=%d elapsed=%s", manifest.ID, filesStarted, stats.FilesPatched, stats.FilesCreated, stats.FilesReused, stats.BytesWritten, time.Since(started))
		}
	}
	if err := removeStaleApplyTemps(ctx, root, directories); err != nil {
		return nil, err
	}
	deleted, err := removeObsoleteEntries(ctx, root, target, opts.Previous, excluded, localOnly)
	if err != nil {
		return nil, err
	}
	stats.EntriesDeleted = deleted
	// Restore the manifest modes and timestamps bottom-up so the tree matches the snapshot.
	for _, entry := range slices.Backward(directories) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		dst := filepath.Join(root, filepath.FromSlash(entry.Path))
		info, err := os.Lstat(dst)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		if err := os.Chmod(dst, os.FileMode(entry.Mode)); err != nil {
			return nil, applyPathError("restore directory permissions", entry.Path, err)
		}
		mtime := time.Unix(0, entry.ModTimeNS)
		if err := os.Chtimes(dst, mtime, mtime); err != nil {
			return nil, applyPathError("restore directory timestamps", entry.Path, err)
		}
	}
	if err := os.Chmod(root, os.FileMode(manifest.RootMode)); err != nil {
		return nil, fmt.Errorf("restore standby data root permissions: %w", err)
	}
	for _, directory := range directories {
		touchedDirs[directory.Path] = struct{}{}
	}
	for rel := range touchedDirs {
		full := root
		if rel != "." {
			full = filepath.Join(root, filepath.FromSlash(rel))
		}
		if err := syncDirectory(full); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("sync standby directory %q: %w", rel, err)
		}
	}
	log.Info("Applied standby snapshot in place: snapshot=%s files_reused=%d files_created=%d files_patched=%d files_replaced=%d symlinks=%d chunks_reused=%d chunks_patched=%d chunks_from_cache=%d chunks_fetched=%d bytes_written=%d entries_deleted=%d directories_created=%d elapsed=%s", manifest.ID, stats.FilesReused, stats.FilesCreated, stats.FilesPatched, stats.FilesReplaced, stats.Symlinks, stats.ChunksReused, stats.ChunksPatched, stats.ChunksFromCache, stats.ChunksFetched, stats.BytesWritten, stats.EntriesDeleted, stats.Directories, time.Since(started))
	return stats, nil
}

func applyInPlaceSymlink(ctx context.Context, root string, entry *TreeEntry, previousEntries map[string]TreeEntry, stats *inPlaceApplyStats) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dst := filepath.Join(root, filepath.FromSlash(entry.Path))
	if previous, ok := previousEntries[entry.Path]; ok && previous.Type == "symlink" && previous.LinkTarget == entry.LinkTarget {
		if info, err := os.Lstat(dst); err == nil && info.Mode()&os.ModeSymlink != 0 {
			if target, err := os.Readlink(dst); err == nil && target == entry.LinkTarget {
				stats.FilesReused++
				return nil
			}
		}
	}
	if err := removePathForReplace(dst); err != nil {
		return applyPathError("remove previous symlink", entry.Path, err)
	}
	if err := os.Symlink(entry.LinkTarget, dst); err != nil {
		return applyPathError("create symlink", entry.Path, err)
	}
	stats.Symlinks++
	return nil
}

// applyInPlaceFile makes dst match entry, patching only the chunks that differ. Files that
// cannot be patched safely (new files, hard-linked files) are rebuilt through a temporary
// file in the same directory and renamed into place.
func applyInPlaceFile(ctx context.Context, root string, source *inPlaceChunkSource, entry *TreeEntry, previous TreeEntry, hasPrevious bool, stats *inPlaceApplyStats, buffer []byte) error {
	dst := filepath.Join(root, filepath.FromSlash(entry.Path))
	info, err := os.Lstat(dst)
	exists := err == nil
	if err != nil && !os.IsNotExist(err) {
		return applyPathError("inspect file", entry.Path, err)
	}
	if exists && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
		if err := removePathForReplace(dst); err != nil {
			return applyPathError("remove conflicting file entry", entry.Path, err)
		}
		exists = false
	}
	// The standby recorded its own file identities after the previous install, so an
	// unchanged file is recognised without reading it.
	if exists && hasPrevious && previous.LocalChangeID != "" && sameManifestFileData(*entry, previous) &&
		uint32(info.Mode().Perm()) == entry.Mode && info.ModTime().UnixNano() == entry.ModTimeNS &&
		fileChangeID(info) == previous.LocalChangeID {
		stats.FilesReused++
		return nil
	}
	if exists && fileHasMultipleLinks(info) {
		if err := source.rebuildFileThroughTemp(ctx, dst, entry); err != nil {
			return applyPathError("rebuild file", entry.Path, err)
		}
		stats.FilesReplaced++
		return nil
	}
	if !exists {
		if err := source.rebuildFileThroughTemp(ctx, dst, entry); err != nil {
			return applyPathError("create file", entry.Path, err)
		}
		stats.FilesCreated++
		return nil
	}
	file, err := os.OpenFile(dst, os.O_RDWR, 0)
	if err != nil {
		return applyPathError("open file", entry.Path, err)
	}
	defer file.Close()
	mismatches, reused, err := inPlaceChunkMismatches(ctx, file, entry, buffer)
	if err != nil {
		return applyPathError("verify file chunks", entry.Path, err)
	}
	stats.ChunksReused += reused
	if len(mismatches) == 0 {
		if err := finalizeInPlaceFile(file, dst, entry); err != nil {
			return applyPathError("finalize file", entry.Path, err)
		}
		stats.FilesReused++
		return nil
	}
	for i := range mismatches {
		chunk := mismatches[i]
		data, err := source.chunkBytes(ctx, chunk)
		if err != nil {
			return applyPathError("restore chunk "+chunk.Hash, entry.Path, err)
		}
		if _, err := file.WriteAt(data, chunk.Offset); err != nil {
			return applyPathError("write chunk", entry.Path, err)
		}
		stats.ChunksPatched++
		stats.BytesWritten += int64(len(data))
	}
	if err := finalizeInPlaceFile(file, dst, entry); err != nil {
		return applyPathError("finalize file", entry.Path, err)
	}
	stats.FilesPatched++
	return nil
}

// inPlaceChunkMismatches returns the chunks whose bytes on disk do not match entry.
func inPlaceChunkMismatches(ctx context.Context, file *os.File, entry *TreeEntry, buffer []byte) ([]ChunkDescriptor, int, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, 0, err
	}
	requested := make([]ChunkDescriptor, 0)
	reused := 0
	for i := range entry.Chunks {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		chunk := entry.Chunks[i]
		if chunk.Offset < 0 || chunk.Size <= 0 || chunk.Size > chunkMaxSize {
			return nil, 0, fmt.Errorf("invalid manifest chunk range at offset %d", chunk.Offset)
		}
		if info.Size() < chunk.Offset+chunk.Size {
			requested = append(requested, chunk)
			continue
		}
		data := buffer[:chunk.Size]
		read, err := file.ReadAt(data, chunk.Offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, 0, err
		}
		if int64(read) != chunk.Size {
			requested = append(requested, chunk)
			continue
		}
		if chunk.Zero {
			if isZeroChunk(data) {
				reused++
				continue
			}
			requested = append(requested, chunk)
			continue
		}
		if !chunkMatchesHash(data, chunk.Hash) {
			requested = append(requested, chunk)
			continue
		}
		reused++
	}
	return requested, reused, nil
}

func chunkMatchesHash(data []byte, expected string) bool {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]) == expected
}

func finalizeInPlaceFile(file *os.File, dst string, entry *TreeEntry) error {
	if err := file.Truncate(entry.Size); err != nil {
		return err
	}
	if err := file.Chmod(os.FileMode(entry.Mode)); err != nil {
		return err
	}
	mtime := time.Unix(0, entry.ModTimeNS)
	if err := os.Chtimes(dst, mtime, mtime); err != nil {
		return err
	}
	return file.Sync()
}

func removePathForReplace(dst string) error {
	info, err := os.Lstat(dst)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
		if err := makeTreeRemovable(context.Background(), dst); err != nil {
			return err
		}
	}
	if err := os.RemoveAll(dst); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(dst))
}

// removeObsoleteEntries deletes tree entries that the target manifest does not contain. A
// trusted previous manifest lists every installed entry, so the candidates come from it
// without walking the whole tree; anything else falls back to a walk.
func removeObsoleteEntries(ctx context.Context, root string, target map[string]struct{}, previous *SnapshotManifest, excluded, localOnly map[string]string) (int, error) {
	var obsolete []string
	if previous != nil && previous.State == "ready" {
		ignoredPrefixes := make([]string, 0)
		for _, entry := range previous.Files {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			if slices.ContainsFunc(ignoredPrefixes, func(prefix string) bool {
				return strings.HasPrefix(entry.Path, prefix)
			}) {
				continue
			}
			if _, kept := target[entry.Path]; kept {
				continue
			}
			if _, ignored := localOnly[entry.Path]; ignored {
				if entry.Type == "dir" {
					ignoredPrefixes = append(ignoredPrefixes, entry.Path+"/")
				}
				continue
			}
			if _, ignored := configuredReplicationLogEntry(entry.Path, localOnly); ignored {
				continue
			}
			if _, regenerable := excluded[entry.Path]; regenerable && entry.Type != "symlink" {
				if entry.Type == "dir" {
					ignoredPrefixes = append(ignoredPrefixes, entry.Path+"/")
				}
				continue
			}
			obsolete = append(obsolete, entry.Path)
		}
	} else {
		err := filepath.Walk(root, func(filePath string, info os.FileInfo, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if filePath == root {
				return nil
			}
			rel, err := filepath.Rel(root, filePath)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			if _, kept := target[rel]; kept {
				return nil
			}
			if _, ignored := localOnly[rel]; ignored {
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if _, ignored := configuredReplicationLogEntry(rel, localOnly); ignored {
				return nil
			}
			if _, regenerable := excluded[rel]; regenerable && info.Mode()&os.ModeSymlink == 0 {
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			obsolete = append(obsolete, rel)
			return nil
		})
		if err != nil {
			return 0, err
		}
	}
	if len(obsolete) == 0 {
		return 0, nil
	}
	slices.SortFunc(obsolete, func(a, b string) int {
		depthA, depthB := strings.Count(a, "/"), strings.Count(b, "/")
		if depthA != depthB {
			return depthB - depthA
		}
		return strings.Compare(b, a)
	})
	removed := 0
	for _, rel := range obsolete {
		if err := ctx.Err(); err != nil {
			return removed, err
		}
		full := filepath.Join(root, filepath.FromSlash(rel))
		info, err := os.Lstat(full)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return removed, applyPathError("inspect obsolete entry", rel, err)
		}
		if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
			if err := makeTreeRemovable(ctx, full); err != nil {
				return removed, applyPathError("make obsolete directory removable", rel, err)
			}
			// Remove the whole subtree: entries the trusted baseline did not list (stale
			// temporary files) must not keep an obsolete directory alive.
			if err := os.RemoveAll(full); err != nil {
				return removed, applyPathError("remove obsolete directory", rel, err)
			}
			removed++
			continue
		}
		if err := os.Remove(full); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return removed, applyPathError("remove obsolete entry", rel, err)
		}
		removed++
	}
	return removed, nil
}

// removeStaleApplyTemps removes temporary files a previous interrupted apply left behind.
func removeStaleApplyTemps(ctx context.Context, root string, directories []TreeEntry) error {
	const tempPrefix = ".replication-apply-"
	removed := 0
	for _, entry := range directories {
		if err := ctx.Err(); err != nil {
			return err
		}
		dir := filepath.Join(root, filepath.FromSlash(entry.Path))
		names, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, name := range names {
			if !strings.HasPrefix(name.Name(), tempPrefix) {
				continue
			}
			full := filepath.Join(dir, name.Name())
			if err := os.RemoveAll(full); err != nil && !os.IsNotExist(err) {
				return applyPathError("remove stale apply temporary file", filepath.ToSlash(filepath.Join(entry.Path, name.Name())), err)
			}
			removed++
		}
	}
	if removed > 0 {
		log.Warn("Removed stale in-place apply temporary files: count=%d", removed)
	}
	return nil
}

// inPlaceChunkSource resolves chunk bytes without trusting any unverified content: every
// local candidate is checked against the requested hash before it is used.
type inPlaceChunkSource struct {
	root, resolvedRoot string
	cacheDir           string
	candidates         map[string][]chunkLocation
	previous           *SnapshotManifest
	indexTried         bool
	fetch              func(ctx context.Context, hash string) ([]byte, error)
	remaining          map[string]int
	memory             *chunkMemoryCache
	shards             sync.Map
	stats              *inPlaceApplyStats
}

// chunkLocations returns the known local locations of a chunk. The planning pass normally
// supplies them; otherwise the previous manifest index is built once, on first use.
func (s *inPlaceChunkSource) chunkLocations(hash string) []chunkLocation {
	if s.candidates == nil && !s.indexTried {
		s.indexTried = true
		if s.previous != nil {
			primary, alternates, err := indexManifestWithAlternatesContext(context.Background(), s.previous)
			if err != nil {
				log.Warn("Cannot index previous manifest chunk locations for local reuse: error=%v", err)
			} else {
				merged := make(map[string][]chunkLocation, len(primary)+len(alternates))
				for key, location := range primary {
					merged[key] = append(merged[key], location)
				}
				for key, locations := range alternates {
					merged[key] = append(merged[key], locations...)
				}
				s.candidates = merged
			}
		}
	}
	return s.candidates[hash]
}

// consume releases a cached chunk once every manifest occurrence has been written, so the
// chunk cache shrinks while the data tree grows and the peak stays near one data tree.
func (s *inPlaceChunkSource) consume(hash string) {
	if s.remaining == nil || s.cacheDir == "" {
		return
	}
	count, tracked := s.remaining[hash]
	if tracked && count > 1 {
		s.remaining[hash] = count - 1
		return
	}
	if tracked {
		delete(s.remaining, hash)
	}
	if err := os.Remove(cachePath(s.cacheDir, hash)); err != nil && !os.IsNotExist(err) {
		log.Debug("Cannot release consumed chunk from the standby cache: hash=%s error=%v", hash, err)
	}
}

func (s *inPlaceChunkSource) zeroBytes(chunk ChunkDescriptor) []byte {
	data := make([]byte, chunk.Size)
	s.stats.ChunksReused++
	return data
}

func (s *inPlaceChunkSource) chunkBytes(ctx context.Context, chunk ChunkDescriptor) ([]byte, error) {
	if chunk.Zero {
		return s.zeroBytes(chunk), nil
	}
	if data := s.memory.get(chunk.Hash, chunk.Size); data != nil {
		s.stats.ChunksReused++
		s.consume(chunk.Hash)
		return data, nil
	}
	data, err := readCachedChunkForBatch(s.cacheDir, chunk.Hash, &s.shards)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if data != nil {
		s.stats.ChunksFromCache++
		s.memory.add(chunk.Hash, data)
		s.consume(chunk.Hash)
		return data, nil
	}
	for _, location := range s.chunkLocations(chunk.Hash) {
		if data, err := readChunkFromRoot(s.root, s.resolvedRoot, location, chunk.Hash); err == nil {
			s.stats.ChunksReused++
			s.memory.add(chunk.Hash, data)
			s.consume(chunk.Hash)
			return data, nil
		}
	}
	if s.fetch == nil {
		return nil, fmt.Errorf("chunk %s is unavailable locally and no fetch source is configured", chunk.Hash)
	}
	data, err = s.fetch(ctx, chunk.Hash)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != chunk.Size {
		return nil, fmt.Errorf("chunk %s size mismatch: got %d want %d", chunk.Hash, len(data), chunk.Size)
	}
	if !chunkMatchesHash(data, chunk.Hash) {
		return nil, fmt.Errorf("chunk %s failed hash verification", chunk.Hash)
	}
	if err := storeChunkForBatch(s.cacheDir, chunk.Hash, data); err != nil {
		log.Debug("Cannot persist fetched chunk in the standby cache: hash=%s error=%v", chunk.Hash, err)
	}
	s.stats.ChunksFetched++
	s.memory.add(chunk.Hash, data)
	s.consume(chunk.Hash)
	return data, nil
}

// rebuildFileThroughTemp writes a complete replacement file next to dst and renames it into
// place. The previous file, when present, is only read; it is replaced by the final rename.
func (s *inPlaceChunkSource) rebuildFileThroughTemp(ctx context.Context, dst string, entry *TreeEntry) error {
	temp, err := os.CreateTemp(filepath.Dir(dst), ".replication-apply-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	var existing *os.File
	if info, err := os.Lstat(dst); err == nil && info.Mode().IsRegular() {
		if opened, err := os.Open(dst); err == nil {
			existing = opened
			defer existing.Close()
		}
	}
	for i := range entry.Chunks {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk := entry.Chunks[i]
		var data []byte
		if chunk.Zero {
			data = s.zeroBytes(chunk)
		} else if existing != nil {
			if fromExisting, err := readChunkFromFile(existing, chunk); err == nil {
				data = fromExisting
			}
		}
		if data == nil {
			data, err = s.chunkBytes(ctx, chunk)
			if err != nil {
				return err
			}
		}
		if int64(len(data)) != chunk.Size {
			return fmt.Errorf("chunk %s size mismatch: got %d want %d", chunk.Hash, len(data), chunk.Size)
		}
		if _, err := temp.Write(data); err != nil {
			return err
		}
		s.stats.BytesWritten += int64(len(data))
	}
	if err := temp.Truncate(entry.Size); err != nil {
		return err
	}
	if err := temp.Chmod(os.FileMode(entry.Mode)); err != nil {
		return err
	}
	mtime := time.Unix(0, entry.ModTimeNS)
	if err := os.Chtimes(tempPath, mtime, mtime); err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, dst); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(dst))
}

func readChunkFromFile(file *os.File, chunk ChunkDescriptor) ([]byte, error) {
	if chunk.Offset < 0 || chunk.Size <= 0 || chunk.Size > chunkMaxSize {
		return nil, errors.New("invalid chunk range")
	}
	data := make([]byte, chunk.Size)
	read, err := file.ReadAt(data, chunk.Offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if int64(read) != chunk.Size || !chunkMatchesHash(data, chunk.Hash) {
		return nil, errors.New("chunk source does not match the manifest")
	}
	return data, nil
}
