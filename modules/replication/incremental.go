// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	pathpkg "path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"gitea.dev/modules/log"
	"gitea.dev/modules/setting"
)

const (
	incrementalFormatVersion       = 6
	replicationErrorCodeHeader     = "X-Replication-Error-Code"
	errorCodePreflightCheckpoint   = "preflight_checkpoint_unavailable"
	errorCodePreflightBase         = "preflight_base_unavailable"
	errorCodePreflightRequestEnded = "preflight_request_ended"
	errorCodeFinalizeRequestEnded  = "finalize_request_ended"
	chunkMinSize                   = 256 * 1024
	chunkAverageSize               = 1024 * 1024
	chunkMaxSize                   = 4 * 1024 * 1024
	// Leave room for gzip framing and incompressible deflate block overhead.
	maxEncodedChunkSize         = chunkMaxSize + (64 * 1024)
	chunkReadBufferSize         = 128 * 1024
	maxCachedZeroChunkHashSizes = 64
	reusablePreflightMaxAge     = 5 * time.Minute
	maxManifestSymlinkDepth     = 40
	maxManifestSize             = 256 * 1024 * 1024
	maxEncodedManifestSize      = maxManifestSize + (1024 * 1024)
	manifestEncodeBatchSize     = 1024 * 1024
	manifestStreamBufferSize    = 64 * 1024
)

// manifestSizeLimit bounds the encoded manifest. It stays a variable so tests can
// exercise the boundary without materializing a 256 MiB manifest.
var manifestSizeLimit = int64(maxManifestSize)

// manifestSignedFieldsSize is the encoded size of the signature fields that the
// digest omits but every persisted manifest carries. Signing reserves it so a
// digest-only size check cannot accept a manifest that then fails to be written.
const manifestSignedFieldsSize = int64(len(`,"sha256":""`) + len(`,"signature":""`) + sha256.Size*4)

func supportedIncrementalFormatVersion(version int) bool {
	return version == incrementalFormatVersion
}

func preflightIsFresh(manifest *SnapshotManifest, now time.Time) bool {
	age := now.Sub(manifest.CreatedAt)
	return age >= -reusablePreflightMaxAge && age <= reusablePreflightMaxAge
}

var (
	errManifestTrailingData   = errors.New("incremental manifest contains oversized or trailing data")
	errManifestTooLarge       = errors.New("incremental manifest exceeds maximum size")
	errIncrementalTreeChanged = errors.New("filesystem tree changed during scan")
	errChunkHashMismatch      = errors.New("received chunk hash mismatch")
	errChunkSizeMismatch      = errors.New("received chunk size mismatch")
	chunkReadBuffers          = sync.Pool{New: func() any { return new([chunkReadBufferSize]byte) }}
)

type ChunkDescriptor struct {
	Hash   string `json:"hash"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
	Zero   bool   `json:"zero,omitempty"`
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
	Path           string
	Offset, Size   int64
	SourceChangeID string
}

func gearValue(b byte) uint64 {
	x := uint64(b) + 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func matchesSHA256Hex(sum [sha256.Size]byte, expected string) bool {
	if len(expected) != sha256.Size*2 {
		return false
	}
	const digits = "0123456789abcdef"
	for i, value := range sum {
		if expected[2*i] != digits[value>>4] || expected[2*i+1] != digits[value&0x0f] {
			return false
		}
	}
	return true
}

var gearValueTable = func() [256]uint64 {
	var table [256]uint64
	for i := range table {
		table[i] = gearValue(byte(i))
	}
	return table
}()

var (
	zeroChunkBoundaryOnce     sync.Once
	zeroChunkBoundary         int64
	zeroChunkBoundaryHashOnce sync.Once
	zeroChunkBoundaryHash     [sha256.Size]byte
)

// Full zero chunks have a repeatable boundary because the rolling hash resets after each chunk.
func fullZeroChunkBoundary() int64 {
	zeroChunkBoundaryOnce.Do(func() {
		var rolling uint64
		for size := int64(1); size <= chunkMaxSize; size++ {
			rolling = (rolling << 1) + gearValueTable[0]
			if size >= chunkMinSize && ((rolling&uint64(chunkAverageSize-1)) == 0 || size >= chunkMaxSize) {
				zeroChunkBoundary = size
				return
			}
		}
	})
	return zeroChunkBoundary
}

func fullZeroChunkHash() [sha256.Size]byte {
	zeroChunkBoundaryHashOnce.Do(func() {
		zeroChunkBoundaryHash = sha256ZeroBytes(fullZeroChunkBoundary())
	})
	return zeroChunkBoundaryHash
}

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
	return splitFileFromOpen(ctx, f, info, path)
}

func splitFileForManifest(ctx context.Context, path string, expected os.FileInfo) ([]ChunkDescriptor, error) {
	f, openedInfo, err := openRegularFile(path, expected)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", errIncrementalTreeChanged, path)
		}
		current, statErr := os.Lstat(path)
		if statErr != nil || !os.SameFile(expected, current) {
			return nil, fmt.Errorf("%w: %s", errIncrementalTreeChanged, path)
		}
		return nil, err
	}
	defer f.Close()
	if openedInfo.Size() != expected.Size() || openedInfo.Mode().Perm() != expected.Mode().Perm() ||
		!openedInfo.ModTime().Equal(expected.ModTime()) || fileChangeID(openedInfo) != fileChangeID(expected) {
		return nil, fmt.Errorf("%w: %s", errIncrementalTreeChanged, path)
	}
	return splitFileFromOpen(ctx, f, openedInfo, path)
}

func splitFileFromOpen(ctx context.Context, f *os.File, info os.FileInfo, path string) ([]ChunkDescriptor, error) {
	started := time.Now()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if info.Size() == 0 {
		return nil, nil
	}
	buffer := chunkReadBuffers.Get().(*[chunkReadBufferSize]byte) //nolint:forcetypeassert // New and Put use only this fixed-size buffer.
	defer chunkReadBuffers.Put(buffer)
	readBuffer := buffer[:]
	if info.Size() < int64(len(readBuffer)) {
		readBuffer = readBuffer[:int(info.Size())]
	}
	hash := sha256.New()
	var chunks []ChunkDescriptor
	var zeroChunkHashes map[int64][sha256.Size]byte
	var rolling uint64
	var offset, start, size int64
	var descriptorBytes int64
	var skippedZeroBytes int64 // deferred all-zero prefix from earlier reads
	zero := true
	tooLarge := false
	emptyReads := 0
	nextProgressLog := started.Add(30 * time.Second)
	remaining := info.Size()
	flush := func() {
		descriptorSize := chunkDescriptorEncodedSize(int64(sha256.Size*2+2), start, size, zero)
		if len(chunks) > 0 {
			descriptorSize++
		}
		if descriptorBytes > int64(maxManifestSize)-descriptorSize {
			tooLarge = true
			return
		}
		descriptorBytes += descriptorSize
		var sum [sha256.Size]byte
		if zero {
			if size == fullZeroChunkBoundary() {
				sum = fullZeroChunkHash()
			} else {
				var ok bool
				sum, ok = zeroChunkHashes[size]
				if !ok {
					sum = sha256ZeroBytes(size)
					if len(zeroChunkHashes) < maxCachedZeroChunkHashSizes {
						if zeroChunkHashes == nil {
							zeroChunkHashes = make(map[int64][sha256.Size]byte, 4)
						}
						zeroChunkHashes[size] = sum
					}
				}
			}
		} else {
			hash.Sum(sum[:0])
		}
		chunks = append(chunks, ChunkDescriptor{Hash: hex.EncodeToString(sum[:]), Offset: start, Size: size, Zero: zero})
		hash.Reset()
		rolling, start, size = 0, offset, 0
		skippedZeroBytes = 0
		zero = true
	}
	// Bound chunking to the size observed at open so concurrent appends cannot extend the scan indefinitely.
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		readSize := min(int64(len(readBuffer)), remaining)
		n, readErr := f.Read(readBuffer[:int(readSize)])
		if n == 0 && readErr == nil {
			emptyReads++
			if emptyReads >= 100 {
				return nil, io.ErrNoProgress
			}
			continue
		}
		emptyReads = 0
		remaining -= int64(n)
		segmentStart := 0
		for i, b := range readBuffer[:n] {
			if zero && b != 0 && skippedZeroBytes > 0 {
				writeZeroBytesToHash(hash, skippedZeroBytes)
				skippedZeroBytes = 0
			}
			zero = zero && b == 0
			rolling = (rolling << 1) + gearValueTable[b]
			offset++
			size++
			if size >= chunkMinSize && ((rolling&uint64(chunkAverageSize-1)) == 0 || size >= chunkMaxSize) {
				if !zero {
					_, _ = hash.Write(readBuffer[segmentStart : i+1])
				}
				flush()
				if tooLarge {
					return nil, errManifestTooLarge
				}
				segmentStart = i + 1
			}
		}
		if segmentStart < n {
			if zero {
				skippedZeroBytes += int64(n - segmentStart)
			} else {
				_, _ = hash.Write(readBuffer[segmentStart:n])
			}
		}
		if info.Size() >= 64<<20 {
			if now := time.Now(); !now.Before(nextProgressLog) {
				log.Info("Replication file chunking progress: path=%q bytes_read=%d/%d chunks_completed=%d elapsed=%s", path, offset, info.Size(), len(chunks), now.Sub(started))
				nextProgressLog = now.Add(30 * time.Second)
			}
		}
		if errors.Is(readErr, io.EOF) {
			if remaining > 0 {
				return nil, fmt.Errorf("%w: file shrank while chunking %q", errIncrementalTreeChanged, path)
			}
			break
		}
		if readErr != nil {
			return nil, readErr
		}
		if remaining == 0 {
			break
		}
	}
	if size > 0 {
		flush()
		if tooLarge {
			return nil, errManifestTooLarge
		}
	}
	return chunks, nil
}

var chunkFileForManifest = splitFileForManifest

func validateTreePath(rel string) error {
	if len(rel) > 4096 {
		return errors.New("manifest path is too long")
	}
	if !utf8.ValidString(rel) || strings.IndexByte(rel, 0) >= 0 {
		return errors.New("manifest path must be valid UTF-8 and cannot contain NUL")
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
	if !utf8.ValidString(target) || strings.IndexByte(target, 0) >= 0 {
		return errors.New("symlink target must be valid UTF-8 and cannot contain NUL")
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

func validateTreeTopologyContext(ctx context.Context, entries []TreeEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entryDirectories := make(map[string]bool, len(entries))
	symlinkTargets := make(map[string]string)
	type symlinkPathFrame struct {
		components []string
		next       int
	}
	for i, entry := range entries {
		if i&0xff == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
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
	for i, entry := range entries {
		if i&0xff == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		parent := pathpkg.Dir(entry.Path)
		if parent != "." {
			isDirectory, ok := entryDirectories[parent]
			if !ok || !isDirectory {
				return fmt.Errorf("manifest parent %q for %q is not a directory", parent, entry.Path)
			}
		}
		if entry.Type != "symlink" {
			continue
		}
		prefix := []byte{}
		prefixEnds := []int{}
		if parent != "." {
			prefix = append(prefix, parent...)
			for index := range parent {
				if parent[index] == '/' {
					prefixEnds = append(prefixEnds, index)
				}
			}
			prefixEnds = append(prefixEnds, len(parent))
		}
		frames := []symlinkPathFrame{{components: strings.Split(filepath.ToSlash(entry.LinkTarget), "/")}}
		followed := 1
		for len(frames) > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			frameIndex := len(frames) - 1
			if frames[frameIndex].next == len(frames[frameIndex].components) {
				frames = frames[:frameIndex]
				continue
			}
			part := frames[frameIndex].components[frames[frameIndex].next]
			frames[frameIndex].next++
			switch part {
			case "", ".":
				continue
			case "..":
				if len(prefixEnds) == 0 {
					return fmt.Errorf("unsafe symlink target %q", entry.LinkTarget)
				}
				prefixEnds = prefixEnds[:len(prefixEnds)-1]
				if len(prefixEnds) == 0 {
					prefix = prefix[:0]
				} else {
					prefix = prefix[:prefixEnds[len(prefixEnds)-1]]
				}
			default:
				prefixLength := len(prefix)
				if prefixLength > 0 {
					prefix = append(prefix, '/')
				}
				prefix = append(prefix, part...)
				prefixEnds = append(prefixEnds, len(prefix))
				if target, ok := symlinkTargets[string(prefix)]; ok {
					followed++
					if followed > maxManifestSymlinkDepth {
						return fmt.Errorf("symlink target %q traverses too many links", entry.LinkTarget)
					}
					prefixEnds = prefixEnds[:len(prefixEnds)-1]
					prefix = prefix[:prefixLength]
					frames = append(frames, symlinkPathFrame{components: strings.Split(filepath.ToSlash(target), "/")})
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
	if errors.Is(err, errManifestTooLarge) {
		return fmt.Errorf("incremental manifest size limit reached at %q: %w", rel, err)
	}
	return fmt.Errorf("scan %q: %w", rel, err)
}

func replicationTreeExclusions(root string) map[string]string {
	type candidate struct {
		path string
		kind string
	}
	candidates := make([]candidate, 0, 6)
	if setting.Indexer.IssueType == "bleve" {
		candidates = append(candidates, candidate{path: setting.Indexer.IssuePath, kind: "issue index"})
	}
	if setting.Indexer.RepoIndexerEnabled && setting.Indexer.RepoType == "bleve" {
		candidates = append(candidates, candidate{path: setting.Indexer.RepoPath, kind: "code index"})
	}
	if setting.RepoArchive.Storage != nil && setting.RepoArchive.Storage.Type == setting.LocalStorageType {
		candidates = append(candidates, candidate{path: setting.RepoArchive.Storage.Path, kind: "repository archive cache"})
	}
	if setting.Log.RootPath != "" {
		candidates = append(candidates, candidate{path: setting.Log.RootPath, kind: "node-local logs"})
	}
	if setting.AppDataPath != "" {
		if tempPath := setting.AppDataTempDir("").JoinPath(); filepath.IsAbs(tempPath) {
			candidates = append(candidates, candidate{path: tempPath, kind: "temporary application data"})
		}
	}
	if setting.PprofDataPath != "" {
		candidates = append(candidates, candidate{path: setting.PprofDataPath, kind: "node-local profiling data"})
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
	for _, localOnlyPath := range []string{
		setting.CustomConf,
		filepath.Join(setting.SSH.RootPath, "authorized_keys"),
		setting.SSH.TrustedUserCAKeysFile,
	} {
		if filepath.IsAbs(localOnlyPath) {
			protectedPaths = append(protectedPaths, localOnlyPath)
		}
	}
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
			protected, resolveErr := filepath.Abs(filepath.Clean(protected))
			if resolveErr != nil || isWithin(candidatePath, protected) || isWithin(protected, candidatePath) {
				unsafe = true
				break
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
		rel, err := filepath.Rel(root, paths[i])
		if err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			excluded[filepath.ToSlash(rel)] = candidate.kind
		}
	}
	return excluded
}

func replicationLocalOnlyFiles(root string) map[string]string {
	root, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return nil
	}
	candidates := []struct {
		path string
		kind string
	}{
		{path: setting.CustomConf, kind: "node-local configuration"},
		{path: filepath.Join(setting.SSH.RootPath, "authorized_keys"), kind: "node-local authorized keys"},
		{path: setting.SSH.TrustedUserCAKeysFile, kind: "node-local SSH trusted user CA keys"},
	}
	localOnly := make(map[string]string, len(candidates))
	addPath := func(path, kind string) {
		if path == root || !isWithin(root, path) {
			return
		}
		rel, err := filepath.Rel(root, path)
		if err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			localOnly[filepath.ToSlash(rel)] = kind
		}
	}
	for _, candidate := range candidates {
		if candidate.path == "" || !filepath.IsAbs(candidate.path) {
			continue
		}
		path, err := filepath.Abs(filepath.Clean(candidate.path))
		if err != nil {
			continue
		}
		addPath(path, candidate.kind)
		resolved, err := resolvedPath(path)
		if err == nil {
			addPath(resolved, candidate.kind)
		}
	}
	for _, logPath := range replicationConfiguredLogFiles() {
		for _, candidate := range []string{logPath, logPath + "." + strconv.Itoa(os.Getpid())} {
			addPath(candidate, "node-local log file")
			if resolved, err := resolvedPath(candidate); err == nil {
				addPath(resolved, "node-local log file")
			}
		}
	}
	return localOnly
}

func replicationConfiguredLogFiles() []string {
	if setting.CfgProvider == nil {
		return nil
	}

	logSection := setting.CfgProvider.Section("log")
	var paths []string
	for _, loggerName := range []string{"default", "access", "router", "xorm"} {
		modeValue := logSection.Key("logger." + loggerName + ".MODE").String()
		if modeValue == "," {
			modeValue = setting.Log.Mode
		}
		for modeName := range strings.SplitSeq(modeValue, ",") {
			modeName = strings.TrimSpace(modeName)
			if modeName == "" {
				continue
			}
			section := setting.CfgProvider.Section("log." + modeName)
			writerType := setting.ConfigSectionKeyString(section, "MODE")
			if writerType == "" {
				writerType = modeName
			}
			if writerType != "file" {
				continue
			}
			defaultFileName := "gitea.log"
			if loggerName == "access" {
				defaultFileName = "access.log"
			}
			fileName := setting.ConfigInheritedKeyString(section, "FILE_NAME")
			if fileName == "" {
				fileName = defaultFileName
			}
			if !filepath.IsAbs(fileName) {
				fileName = filepath.Join(setting.Log.RootPath, fileName)
			}
			paths = append(paths, filepath.Clean(fileName))
		}
	}
	return paths
}

func isLogRotationSuffix(suffix string) bool {
	suffix = strings.TrimSuffix(suffix, ".gz")
	if len(suffix) != 14 || suffix[4] != '-' || suffix[7] != '-' || suffix[10] != '.' {
		return false
	}
	for i := range suffix {
		if i == 4 || i == 7 || i == 10 {
			continue
		}
		if suffix[i] < '0' || suffix[i] > '9' {
			return false
		}
	}
	return true
}

func isLogPID(suffix string) bool {
	if suffix == "" || len(suffix) > 10 {
		return false
	}
	for i := range suffix {
		if suffix[i] < '0' || suffix[i] > '9' {
			return false
		}
	}
	return true
}

func configuredReplicationLogEntry(rel string, logFiles map[string]string) (string, bool) {
	entryDir, entryName := pathpkg.Split(rel)
	for file, kind := range logFiles {
		if kind != "node-local log file" {
			continue
		}
		fileDir, fileName := pathpkg.Split(file)
		if fileDir != entryDir || !strings.HasPrefix(entryName, fileName+".") {
			continue
		}
		suffix := strings.TrimPrefix(entryName, fileName+".")
		if isLogPID(suffix) || isLogRotationSuffix(suffix) {
			return kind, true
		}
		pid, rotation, hasRotation := strings.Cut(suffix, ".")
		if hasRotation && isLogPID(pid) && isLogRotationSuffix(rotation) {
			return kind, true
		}
	}
	return "", false
}

func replicationHasLocalOnlyFilesInDirectory(directoryRel string, localOnlyFiles map[string]string) bool {
	for file, kind := range localOnlyFiles {
		if kind != "" && pathpkg.Dir(file) == directoryRel {
			return true
		}
	}
	return false
}

func replicationHasLocalOnlyFileBelow(directoryRel string, localOnlyFiles map[string]string) bool {
	for file, kind := range localOnlyFiles {
		if kind == "" {
			continue
		}
		for parent := pathpkg.Dir(file); parent != "."; parent = pathpkg.Dir(parent) {
			if parent == directoryRel {
				return true
			}
		}
	}
	return false
}

func appendReplicationDirectoryEntryHash(h hash.Hash, name string) {
	var nameLength [8]byte
	binary.BigEndian.PutUint64(nameLength[:], uint64(len(name)))
	_, _ = h.Write(nameLength[:])
	_, _ = io.WriteString(h, name)
}

func replicationLocalOnlyDirectoryEntries(directoryPath, directoryRel string, localOnlyFiles map[string]string) ([]byte, bool, error) {
	if !replicationHasLocalOnlyFilesInDirectory(directoryRel, localOnlyFiles) {
		return nil, false, nil
	}
	entries, err := os.ReadDir(directoryPath)
	if err != nil {
		return nil, false, err
	}
	nonLogEntriesHash := sha256.New()
	for _, entry := range entries {
		entryRel := pathpkg.Join(directoryRel, entry.Name())
		kind, excluded := localOnlyFiles[entryRel]
		if !excluded || kind != "node-local log file" {
			_, excluded = configuredReplicationLogEntry(entryRel, localOnlyFiles)
		}
		if !excluded {
			appendReplicationDirectoryEntryHash(nonLogEntriesHash, entry.Name())
		}
	}
	return nonLogEntriesHash.Sum(nil), true, nil
}

func scanIncrementalTreeWithOptions(ctx context.Context, root string, base *SnapshotManifest, verifyAll bool) (*SnapshotManifest, error) {
	return scanIncrementalTreeWithOptionsForTask(ctx, root, base, verifyAll, "local")
}

func scanIncrementalTreeWithOptionsForTask(ctx context.Context, root string, base *SnapshotManifest, verifyAll bool, snapshotID string) (*SnapshotManifest, error) {
	return scanIncrementalTreeWithProgressForTask(ctx, root, base, verifyAll, snapshotID, nil)
}

func scanIncrementalTreeWithProgressForTask(ctx context.Context, root string, base *SnapshotManifest, verifyAll bool, snapshotID string, onProgress func()) (*SnapshotManifest, error) {
	return scanIncrementalTreeWithProgressForTaskAndDigest(ctx, root, base, verifyAll, snapshotID, onProgress, true, false)
}

func scanIncrementalTreeWithoutDigestForTask(ctx context.Context, root string, base *SnapshotManifest, verifyAll bool, snapshotID string) (*SnapshotManifest, error) {
	return scanIncrementalTreeWithProgressForTaskAndDigest(ctx, root, base, verifyAll, snapshotID, nil, false, false)
}

func scanIncrementalTreeWithoutDigestUsingLocalChangeIDs(ctx context.Context, root string, base *SnapshotManifest, snapshotID string) (*SnapshotManifest, error) {
	return scanIncrementalTreeWithProgressForTaskAndDigest(ctx, root, base, false, snapshotID, nil, false, true)
}

func scanIncrementalTreeWithProgressForTaskAndDigest(ctx context.Context, root string, base *SnapshotManifest, verifyAll bool, snapshotID string, onProgress func(), calculateDigest, useLocalChangeIDs bool) (*SnapshotManifest, error) {
	type scannedDirectory struct {
		path                   string
		rel                    string
		info                   os.FileInfo
		nonLogEntryNamesHash   hash.Hash
		verifyNonLogEntryNames bool
	}

	scanStarted := time.Now()
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return nil, fmt.Errorf("inspect replication root %q: %w", root, err)
	}
	if !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("APP_WORK_PATH %q must be a real directory", root)
	}
	manifestCapacity := 0
	if base != nil {
		manifestCapacity = len(base.Files)
	}
	m := &SnapshotManifest{
		FormatVersion: incrementalFormatVersion, GiteaVersion: setting.AppVer,
		AppWorkPath: setting.AppWorkPath, Snapshot: Snapshot{RootMode: uint32(rootInfo.Mode().Perm())},
		Files: make([]TreeEntry, 0, manifestCapacity),
	}
	if base != nil {
		m.FullScanAt = base.FullScanAt
	}
	var baseEntryIndexes map[string]int
	if base != nil {
		baseEntryIndexes = make(map[string]int, len(base.Files))
		for i := range base.Files {
			baseEntryIndexes[base.Files[i].Path] = i
		}
	}
	var manifestEntriesSize int64
	appendManifestEntry := func(entry TreeEntry) error {
		entrySize, err := treeEntryEncodedSize(ctx, &entry)
		if err != nil {
			return err
		}
		if len(m.Files) > 0 {
			entrySize++
		}
		if entrySize > int64(maxManifestSize)-manifestEntriesSize {
			return errManifestTooLarge
		}
		manifestEntriesSize += entrySize
		m.Files = append(m.Files, entry)
		return nil
	}
	directories := []scannedDirectory{{path: root, rel: ".", info: rootInfo}}
	excludedPaths := replicationTreeExclusions(root)
	localOnlyFiles := replicationLocalOnlyFiles(root)
	directories[0].verifyNonLogEntryNames = replicationHasLocalOnlyFilesInDirectory(".", localOnlyFiles)
	if directories[0].verifyNonLogEntryNames {
		directories[0].nonLogEntryNamesHash = sha256.New()
	}
	directoryIndexes := map[string]int{".": 0}
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
				if onProgress != nil {
					onProgress()
				}
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
		if err := validateTreePath(rel); err != nil {
			return scanPathError(rel, err)
		}
		kind, localOnly := localOnlyFiles[rel]
		if !localOnly {
			kind, localOnly = configuredReplicationLogEntry(rel, localOnlyFiles)
		}
		if parentIndex, ok := directoryIndexes[pathpkg.Dir(rel)]; ok && directories[parentIndex].verifyNonLogEntryNames {
			if !localOnly {
				appendReplicationDirectoryEntryHash(directories[parentIndex].nonLogEntryNamesHash, pathpkg.Base(rel))
			}
		}
		if kind, exclude := excludedPaths[rel]; exclude && info.Mode()&os.ModeSymlink == 0 {
			_, includedByBase := baseEntryIndexes[rel]
			if useLocalChangeIDs && includedByBase {
				log.Debug("Include configured regenerable data present in replication manifest: snapshot=%s kind=%s path=%s", snapshotID, kind, rel)
			} else {
				current, err := os.Lstat(path)
				if err != nil {
					return scanPathError(rel, err)
				}
				if info.Mode()&os.ModeType != current.Mode()&os.ModeType || !os.SameFile(info, current) {
					return fmt.Errorf("%w: %s", errIncrementalTreeChanged, rel)
				}
				log.Info("Excluded regenerable data from replication snapshot: snapshot=%s kind=%s path=%s", snapshotID, kind, rel)
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
		}
		if localOnly {
			current, err := os.Lstat(path)
			if err != nil {
				return scanPathError(rel, err)
			}
			if info.Mode()&os.ModeType != current.Mode()&os.ModeType || !os.SameFile(info, current) {
				return fmt.Errorf("%w: %s", errIncrementalTreeChanged, rel)
			}
			log.Info("Excluded node-local entry from replication snapshot: snapshot=%s kind=%s path=%s", snapshotID, kind, rel)
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if info.IsDir() {
			directory := scannedDirectory{path: path, rel: rel, info: info}
			directory.verifyNonLogEntryNames = replicationHasLocalOnlyFilesInDirectory(rel, localOnlyFiles)
			if directory.verifyNonLogEntryNames {
				directory.nonLogEntryNamesHash = sha256.New()
			}
			directories = append(directories, directory)
			directoryIndexes[rel] = len(directories) - 1
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
			after, err := os.Lstat(path)
			if err != nil {
				return scanPathError(rel, err)
			}
			if after.Mode()&os.ModeSymlink == 0 || !os.SameFile(info, after) {
				return fmt.Errorf("%w: %s", errIncrementalTreeChanged, rel)
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
			oldIndex, hasOld := baseEntryIndexes[rel]
			var old TreeEntry
			if hasOld {
				old = base.Files[oldIndex]
			}
			oldChangeID := old.ChangeID
			if useLocalChangeIDs {
				oldChangeID = old.LocalChangeID
			}
			metadataUnchanged := hasOld && old.Type == "file" && old.Size == info.Size() &&
				old.Mode == uint32(info.Mode().Perm()) && old.ModTimeNS == info.ModTime().UnixNano() &&
				oldChangeID != "" && oldChangeID == e.ChangeID
			if metadataUnchanged && !verifyAll {
				after, err := os.Lstat(path)
				if err != nil {
					return scanPathError(rel, err)
				}
				if !after.Mode().IsRegular() || !os.SameFile(info, after) || after.Size() != e.Size ||
					uint32(after.Mode().Perm()) != e.Mode || after.ModTime().UnixNano() != e.ModTimeNS ||
					fileChangeID(after) != e.ChangeID {
					return fmt.Errorf("%w: %s", errIncrementalTreeChanged, rel)
				}
				e.Chunks = old.Chunks
				m.Size += e.Size
				if err := appendManifestEntry(e); err != nil {
					return err
				}
				filesCompleted.Add(1)
				filesReused.Add(1)
				return nil
			}
			beforeSize, beforeTime, beforeChangeID := info.Size(), info.ModTime(), e.ChangeID
			e.Chunks, err = chunkFileForManifest(ctx, path, info)
			if err != nil {
				return scanPathError(rel, err)
			}
			after, err := os.Lstat(path)
			if err != nil {
				return scanPathError(rel, err)
			}
			if !after.Mode().IsRegular() || !os.SameFile(info, after) || after.Size() != beforeSize ||
				after.ModTime() != beforeTime || fileChangeID(after) != beforeChangeID {
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
			// Sockets, FIFOs and device nodes are node-local runtime artifacts, so a
			// stray one must not block the whole snapshot. Their absence on the other
			// node is expected and they are never part of the replicated content.
			log.Warn("Skipping unsupported filesystem entry during replication scan: path=%q mode=%s", rel, info.Mode().Type())
			return nil
		}
		return appendManifestEntry(e)
	})
	if err != nil {
		return nil, err
	}
	for _, directory := range directories {
		after, err := os.Lstat(directory.path)
		if err != nil {
			return nil, scanPathError(directory.rel, err)
		}
		beforeChangeID := fileChangeID(directory.info)
		if !after.IsDir() || after.Mode()&os.ModeSymlink != 0 || !os.SameFile(directory.info, after) ||
			uint32(after.Mode().Perm()) != uint32(directory.info.Mode().Perm()) {
			return nil, fmt.Errorf("%w: %s", errIncrementalTreeChanged, directory.rel)
		}
		metadataChanged := after.ModTime().UnixNano() != directory.info.ModTime().UnixNano() ||
			(beforeChangeID != "" && beforeChangeID != fileChangeID(after))
		if metadataChanged {
			if !directory.verifyNonLogEntryNames {
				return nil, fmt.Errorf("%w: %s", errIncrementalTreeChanged, directory.rel)
			}
			nonLogNames, verify, err := replicationLocalOnlyDirectoryEntries(directory.path, directory.rel, localOnlyFiles)
			if err != nil {
				return nil, scanPathError(directory.rel, err)
			}
			if !verify || !slices.Equal(directory.nonLogEntryNamesHash.Sum(nil), nonLogNames) {
				return nil, fmt.Errorf("%w: %s", errIncrementalTreeChanged, directory.rel)
			}
			log.Debug("Replication directory metadata changed during node-local activity; non-local entries are unchanged: snapshot=%s path=%s", snapshotID, directory.rel)
		}
	}
	if err := validateTreeTopologyContext(ctx, m.Files); err != nil {
		return nil, err
	}
	if verifyAll {
		m.FullScanAt = time.Now().UTC()
	}
	m.FileCount = len(m.Files)
	if calculateDigest {
		m.SHA256, err = manifestDigestContext(ctx, m)
		if err != nil {
			return m, err
		}
	}
	log.Info("Replication tree scan completed: snapshot=%s entries_seen=%d files_seen=%d files_completed=%d reused_files=%d chunked_files=%d logical_file_bytes=%d content_bytes_chunked=%d verify_all=%t duration=%s", snapshotID, entriesSeen.Load(), filesSeen.Load(), filesCompleted.Load(), filesReused.Load(), filesChunked.Load(), logicalFileBytesSeen.Load(), contentBytesChunked.Load(), verifyAll, time.Since(scanStarted))
	return m, nil
}

func sameChunks(a, b []ChunkDescriptor) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Hash != b[i].Hash || a[i].Offset != b[i].Offset || a[i].Size != b[i].Size {
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
	if int64(len(data)) > manifestSizeLimit-w.size {
		return 0, errManifestTooLarge
	}
	n, err := w.writer.Write(data)
	w.size += int64(n)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	return n, err
}

// digestCountingWriter counts the exact digest input size, since the persisted
// manifest also carries the signature fields the digest leaves out.
type digestCountingWriter struct {
	writer io.Writer
	size   int64
}

func (w *digestCountingWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	w.size += int64(n)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	return n, err
}

func encodedJSONStringSize(value string) (int64, error) {
	needsMarshal := false
	for offset := 0; offset < len(value); {
		if value[offset] < utf8.RuneSelf {
			char := value[offset]
			if char < 0x20 || char == '"' || char == '\\' || char == '<' || char == '>' || char == '&' {
				needsMarshal = true
				break
			}
			offset++
			continue
		}
		r, width := utf8.DecodeRuneInString(value[offset:])
		if (r == utf8.RuneError && width == 1) || r == '\u2028' || r == '\u2029' {
			needsMarshal = true
			break
		}
		offset += width
	}
	if needsMarshal {
		encoded, err := marshalManifestJSON(value)
		if err != nil {
			return 0, err
		}
		return int64(len(encoded)), nil
	}
	return int64(len(value) + 2), nil
}

func decimalSize(value uint64) int64 {
	size := int64(1)
	for value >= 10 {
		value /= 10
		size++
	}
	return size
}

func signedDecimalSize(value int64) int64 {
	if value < 0 {
		return decimalSize(uint64(-(value+1))+1) + 1
	}
	return decimalSize(uint64(value))
}

func chunkDescriptorEncodedSize(hashJSONSize, offset, chunkSize int64, zero bool) int64 {
	size := int64(len(`{"hash":`) + len(`,"offset":`) + len(`,"size":`) + 1)
	size += hashJSONSize
	size += signedDecimalSize(offset) + signedDecimalSize(chunkSize)
	if zero {
		size += int64(len(`,"zero":true`))
	}
	return size
}

func treeEntryEncodedSize(ctx context.Context, entry *TreeEntry) (int64, error) {
	return treeEntryEncodedSizeWithOptions(ctx, entry, false)
}

func treeEntryEncodedSizeWithOptions(ctx context.Context, entry *TreeEntry, clearLocalChangeID bool) (int64, error) {
	const oversized = int64(maxManifestSize) + 1
	size := int64(0)
	var sizeErr error
	add := func(value int64) bool {
		if value < 0 || value > int64(maxManifestSize)-size {
			size = oversized
			sizeErr = errManifestTooLarge
			return false
		}
		size += value
		return true
	}
	addString := func(value string) bool {
		encodedSize, err := encodedJSONStringSize(value)
		if err != nil {
			sizeErr = err
			return false
		}
		return add(encodedSize)
	}
	if !add(int64(len(`{"path":`))) || !addString(entry.Path) ||
		!add(int64(len(`,"type":`))) || !addString(entry.Type) ||
		!add(int64(len(`,"mode":`))) || !add(decimalSize(uint64(entry.Mode))) {
		return oversized, sizeErr
	}
	if entry.Size != 0 && (!add(int64(len(`,"size":`))) || !add(signedDecimalSize(entry.Size))) {
		return oversized, sizeErr
	}
	if entry.ModTimeNS != 0 && (!add(int64(len(`,"mtime_ns":`))) || !add(signedDecimalSize(entry.ModTimeNS))) {
		return oversized, sizeErr
	}
	if entry.ChangeID != "" && (!add(int64(len(`,"change_id":`))) || !addString(entry.ChangeID)) {
		return oversized, sizeErr
	}
	if !clearLocalChangeID && entry.LocalChangeID != "" && (!add(int64(len(`,"local_change_id":`))) || !addString(entry.LocalChangeID)) {
		return oversized, sizeErr
	}
	if entry.LinkTarget != "" && (!add(int64(len(`,"link_target":`))) || !addString(entry.LinkTarget)) {
		return oversized, sizeErr
	}
	if len(entry.Chunks) > 0 {
		if !add(int64(len(`,"chunks":[`))) {
			return oversized, sizeErr
		}
		for i, chunk := range entry.Chunks {
			if i&0xff == 0 {
				if err := ctx.Err(); err != nil {
					return oversized, err
				}
			}
			if i > 0 && !add(1) {
				return oversized, sizeErr
			}
			hashJSONSize, err := encodedJSONStringSize(chunk.Hash)
			if err != nil {
				return oversized, err
			}
			if !add(chunkDescriptorEncodedSize(hashJSONSize, chunk.Offset, chunk.Size, chunk.Zero)) {
				return oversized, sizeErr
			}
		}
		if !add(1) {
			return oversized, sizeErr
		}
	}
	if !add(1) {
		return oversized, sizeErr
	}
	return size, nil
}

func writeManifestJSONContext(ctx context.Context, writer io.Writer, manifest *SnapshotManifest, clearDigest bool) error {
	return writeManifestJSONWithOptionsContext(ctx, writer, manifest, clearDigest, false)
}

func writeManifestJSONString(writer io.Writer, value string) error {
	encoded, err := marshalManifestJSON(value)
	if err != nil {
		return err
	}
	_, err = writer.Write(encoded)
	return err
}

func writeManifestJSONHash(writer io.Writer, value string) error {
	if len(value) == sha256.Size*2 && isLowerHex(value) {
		if _, err := io.WriteString(writer, `"`); err != nil {
			return err
		}
		if _, err := io.WriteString(writer, value); err != nil {
			return err
		}
		_, err := io.WriteString(writer, `"`)
		return err
	}
	return writeManifestJSONString(writer, value)
}

func writeManifestJSONInt64(writer io.Writer, value int64) error {
	var buffer [20]byte
	_, err := writer.Write(strconv.AppendInt(buffer[:0], value, 10))
	return err
}

func writeManifestJSONUint64(writer io.Writer, value uint64) error {
	var buffer [20]byte
	_, err := writer.Write(strconv.AppendUint(buffer[:0], value, 10))
	return err
}

// Stream large entries because json.Marshal would buffer all chunk descriptors at once.
func writeManifestTreeEntryJSONContext(ctx context.Context, writer io.Writer, entry *TreeEntry, clearLocalChangeID bool) error {
	write := func(value string) error {
		_, err := io.WriteString(writer, value)
		return err
	}
	if err := write(`{"path":`); err != nil {
		return err
	}
	if err := writeManifestJSONString(writer, entry.Path); err != nil {
		return err
	}
	if err := write(`,"type":`); err != nil {
		return err
	}
	if err := writeManifestJSONString(writer, entry.Type); err != nil {
		return err
	}
	if err := write(`,"mode":`); err != nil {
		return err
	}
	if err := writeManifestJSONUint64(writer, uint64(entry.Mode)); err != nil {
		return err
	}
	if entry.Size != 0 {
		if err := write(`,"size":`); err != nil {
			return err
		}
		if err := writeManifestJSONInt64(writer, entry.Size); err != nil {
			return err
		}
	}
	if entry.ModTimeNS != 0 {
		if err := write(`,"mtime_ns":`); err != nil {
			return err
		}
		if err := writeManifestJSONInt64(writer, entry.ModTimeNS); err != nil {
			return err
		}
	}
	if entry.ChangeID != "" {
		if err := write(`,"change_id":`); err != nil {
			return err
		}
		if err := writeManifestJSONString(writer, entry.ChangeID); err != nil {
			return err
		}
	}
	if !clearLocalChangeID && entry.LocalChangeID != "" {
		if err := write(`,"local_change_id":`); err != nil {
			return err
		}
		if err := writeManifestJSONString(writer, entry.LocalChangeID); err != nil {
			return err
		}
	}
	if entry.LinkTarget != "" {
		if err := write(`,"link_target":`); err != nil {
			return err
		}
		if err := writeManifestJSONString(writer, entry.LinkTarget); err != nil {
			return err
		}
	}
	if len(entry.Chunks) > 0 {
		if err := write(`,"chunks":[`); err != nil {
			return err
		}
		for i, chunk := range entry.Chunks {
			if i&0xff == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if i > 0 {
				if err := write(","); err != nil {
					return err
				}
			}
			if err := write(`{"hash":`); err != nil {
				return err
			}
			if err := writeManifestJSONHash(writer, chunk.Hash); err != nil {
				return err
			}
			if err := write(`,"offset":`); err != nil {
				return err
			}
			if err := writeManifestJSONInt64(writer, chunk.Offset); err != nil {
				return err
			}
			if err := write(`,"size":`); err != nil {
				return err
			}
			if err := writeManifestJSONInt64(writer, chunk.Size); err != nil {
				return err
			}
			if chunk.Zero {
				if err := write(`,"zero":true`); err != nil {
					return err
				}
			}
			if err := write("}"); err != nil {
				return err
			}
		}
		if err := write("]"); err != nil {
			return err
		}
	}
	return write("}")
}

type treeEntryWithoutLocalChangeID struct {
	Path       string            `json:"path"`
	Type       string            `json:"type"`
	Mode       uint32            `json:"mode"`
	Size       int64             `json:"size,omitempty"`
	ModTimeNS  int64             `json:"mtime_ns,omitempty"`
	ChangeID   string            `json:"change_id,omitempty"`
	LinkTarget string            `json:"link_target,omitempty"`
	Chunks     []ChunkDescriptor `json:"chunks,omitempty"`
}

func withoutLocalChangeID(entry TreeEntry) treeEntryWithoutLocalChangeID {
	return treeEntryWithoutLocalChangeID{
		Path: entry.Path, Type: entry.Type, Mode: entry.Mode, Size: entry.Size,
		ModTimeNS: entry.ModTimeNS, ChangeID: entry.ChangeID, LinkTarget: entry.LinkTarget, Chunks: entry.Chunks,
	}
}

func writeManifestJSONWithOptionsContext(ctx context.Context, writer io.Writer, manifest *SnapshotManifest, clearDigest, clearLocalChangeIDs bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	manifestCopy := *manifest
	if clearDigest {
		manifestCopy.SHA256 = ""
		manifestCopy.Signature = ""
	}
	// Files is the final manifest field, so it can be encoded in bounded batches.
	files := manifestCopy.Files
	manifestCopy.Files = nil
	header, err := marshalManifestJSON(&manifestCopy)
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
		if err := ctx.Err(); err != nil {
			return err
		}
		end := start
		minimumSize := int64(0)
		streamLargeEntry := false
		for end < len(files) {
			entrySize, err := treeEntryEncodedSizeWithOptions(ctx, &files[end], clearLocalChangeIDs)
			if err != nil {
				return err
			}
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
			if end == start+1 && minimumSize > manifestEncodeBatchSize {
				streamLargeEntry = true
				break
			}
		}
		if end == start {
			end++
		}
		if start > 0 {
			if _, err := sizedWriter.Write([]byte{','}); err != nil {
				return err
			}
		}
		if streamLargeEntry {
			entryWriter := bufio.NewWriterSize(sizedWriter, manifestStreamBufferSize)
			if err := writeManifestTreeEntryJSONContext(ctx, entryWriter, &files[start], clearLocalChangeIDs); err != nil {
				return err
			}
			if err := entryWriter.Flush(); err != nil {
				return err
			}
		} else {
			var batch []byte
			if clearLocalChangeIDs {
				transferFiles := make([]treeEntryWithoutLocalChangeID, end-start)
				for i := range transferFiles {
					if i&0xff == 0 {
						if err := ctx.Err(); err != nil {
							return err
						}
					}
					transferFiles[i] = withoutLocalChangeID(files[start+i])
				}
				batch, err = marshalManifestJSON(transferFiles)
			} else {
				batch, err = marshalManifestJSON(files[start:end])
			}
			if err != nil {
				return err
			}
			if len(batch) < 2 {
				return errors.New("invalid encoded incremental manifest entries")
			}
			if _, err := sizedWriter.Write(batch[1 : len(batch)-1]); err != nil {
				return err
			}
		}
		start = end
	}
	_, err = sizedWriter.Write([]byte(`]}`))
	return err
}

func manifestDigestContext(ctx context.Context, manifest *SnapshotManifest) (string, error) {
	digest, _, err := manifestDigestWithSizeContext(ctx, manifest)
	return digest, err
}

func manifestDigestWithSizeContext(ctx context.Context, manifest *SnapshotManifest) (string, int64, error) {
	h := sha256.New()
	counting := &digestCountingWriter{writer: h}
	if err := writeManifestJSONContext(ctx, counting, manifest, true); err != nil {
		return "", counting.size, err
	}
	return hex.EncodeToString(h.Sum(nil)), counting.size, nil
}

func manifestDigestWithoutLocalChangeIDs(manifest *SnapshotManifest) (string, error) {
	return manifestDigestWithoutLocalChangeIDsContext(context.Background(), manifest)
}

func manifestDigestWithoutLocalChangeIDsContext(ctx context.Context, manifest *SnapshotManifest) (string, error) {
	h := sha256.New()
	if err := writeManifestJSONWithOptionsContext(ctx, h, manifest, true, true); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func signIncrementalManifest(manifest *SnapshotManifest, token string) error {
	return signIncrementalManifestContext(context.Background(), manifest, token)
}

func applyIncrementalManifestSignature(manifest *SnapshotManifest, digest, token string) {
	manifest.SHA256 = digest
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = io.WriteString(mac, digest)
	manifest.Signature = hex.EncodeToString(mac.Sum(nil))
}

func signIncrementalManifestContext(ctx context.Context, manifest *SnapshotManifest, token string) error {
	digest, digestSize, err := manifestDigestWithSizeContext(ctx, manifest)
	if err != nil {
		return err
	}
	if digestSize > manifestSizeLimit-manifestSignedFieldsSize {
		return errManifestTooLarge
	}
	applyIncrementalManifestSignature(manifest, digest, token)
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
	if fingerprint := manifest.GeneralTokenSecretFingerprint; fingerprint != "" &&
		!hmac.Equal([]byte(fingerprint), []byte(generalTokenSecretFingerprint(token))) {
		return errors.New("standby general token signing secret does not match the primary")
	}
	return nil
}

func validateIncrementalManifest(m *SnapshotManifest) error {
	return validateIncrementalManifestContext(context.Background(), m)
}

func validateIncrementalManifestContext(ctx context.Context, m *SnapshotManifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !supportedIncrementalFormatVersion(m.FormatVersion) || !validSnapshotID(m.ID) || m.Size < 0 ||
		m.CreatedAt.IsZero() || m.RootMode == 0 || m.RootMode > 0o777 || m.FileCount != len(m.Files) {
		return errors.New("invalid incremental manifest metadata")
	}
	if len(m.GeneralTokenSecretFingerprint) != sha256.Size*2 || !isLowerHex(m.GeneralTokenSecretFingerprint) {
		return errors.New("invalid general token signing secret fingerprint")
	}
	switch m.State {
	case "preflight", "transferring", "ready", "failed":
	default:
		return errors.New("invalid incremental manifest state")
	}
	var logicalSize int64
	var zeroHashes map[int64][sha256.Size]byte
	for i, e := range m.Files {
		if i&0xff == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
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
			for i, c := range e.Chunks {
				if i&0xff == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				if len(c.Hash) != sha256.Size*2 || c.Offset != offset || c.Size <= 0 || c.Size > chunkMaxSize ||
					(i < len(e.Chunks)-1 && c.Size < chunkMinSize) {
					return fmt.Errorf("invalid chunk in %q", e.Path)
				}
				if !isLowerHex(c.Hash) || offset > math.MaxInt64-c.Size {
					return fmt.Errorf("invalid chunk hash or size in %q", e.Path)
				}
				if c.Zero {
					if zeroHashes == nil {
						zeroHashes = make(map[int64][sha256.Size]byte)
					}
					zeroHashes[c.Size] = [sha256.Size]byte{}
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
	if len(zeroHashes) > 0 {
		if err := populateZeroChunkHashes(ctx, zeroHashes); err != nil {
			return err
		}
		for _, entry := range m.Files {
			for _, chunk := range entry.Chunks {
				if err := ctx.Err(); err != nil {
					return err
				}
				if chunk.Zero && !matchesSHA256Hex(zeroHashes[chunk.Size], chunk.Hash) {
					return fmt.Errorf("invalid zero chunk hash in %q", entry.Path)
				}
			}
		}
	}
	if err := validateTreeTopologyContext(ctx, m.Files); err != nil {
		return err
	}
	if logicalSize != m.Size {
		return errors.New("manifest logical size mismatch")
	}
	digest, err := manifestDigestContext(ctx, m)
	if err != nil {
		return err
	}
	if digest != m.SHA256 {
		return errors.New("manifest digest mismatch")
	}
	return nil
}

func populateZeroChunkHashes(ctx context.Context, hashes map[int64][sha256.Size]byte) error {
	orderedSizes := make([]int64, 0, len(hashes))
	for size := range hashes {
		if err := ctx.Err(); err != nil {
			return err
		}
		orderedSizes = append(orderedSizes, size)
	}
	slices.Sort(orderedSizes)
	if err := ctx.Err(); err != nil {
		return err
	}
	hash := sha256.New()
	var zeros [64 * 1024]byte
	var hashedBytes int64
	for _, size := range orderedSizes {
		if err := ctx.Err(); err != nil {
			return err
		}
		for hashedBytes < size {
			n := min(int64(len(zeros)), size-hashedBytes)
			_, _ = hash.Write(zeros[:n])
			hashedBytes += n
		}
		var sum [sha256.Size]byte
		copy(sum[:], hash.Sum(sum[:0]))
		hashes[size] = sum
	}
	return nil
}

func writeZeroBytesToHash(hash hash.Hash, size int64) {
	var zeros [64 * 1024]byte
	for remaining := size; remaining > 0; {
		n := min(int64(len(zeros)), remaining)
		_, _ = hash.Write(zeros[:n])
		remaining -= n
	}
}

func sha256ZeroBytes(size int64) [sha256.Size]byte {
	hash := sha256.New()
	writeZeroBytesToHash(hash, size)
	var sum [sha256.Size]byte
	copy(sum[:], hash.Sum(nil))
	return sum
}

func indexManifest(m *SnapshotManifest) map[string]chunkLocation {
	index, _ := indexManifestContext(context.Background(), m)
	return index
}

func startPeriodicProgressLog(logProgress func()) func() {
	done := make(chan struct{})
	var worker sync.WaitGroup
	worker.Go(func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				select {
				case <-done:
					return
				default:
					logProgress()
				}
			}
		}
	})
	var stop sync.Once
	return func() {
		stop.Do(func() {
			close(done)
			worker.Wait()
		})
	}
}

func indexManifestContext(ctx context.Context, m *SnapshotManifest) (map[string]chunkLocation, error) {
	index := make(map[string]chunkLocation)
	for _, e := range m.Files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for _, c := range e.Chunks {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if c.Zero {
				continue
			}
			if _, ok := index[c.Hash]; !ok {
				index[c.Hash] = chunkLocation{Path: e.Path, Offset: c.Offset, Size: c.Size}
			}
		}
	}
	return index, nil
}

func indexManifestDeltaContext(ctx context.Context, manifest *SnapshotManifest, base map[string]chunkLocation) (map[string]chunkLocation, map[string][]chunkLocation, error) {
	delta := make(map[string]chunkLocation)
	alternates := make(map[string][]chunkLocation)
	var processedEntries, processedChunks, uniqueHashes atomic.Int64
	started := time.Now()
	stopProgress := startPeriodicProgressLog(func() {
		log.Info("Indexing replication manifest chunks progress: snapshot=%s index_kind=delta entries_processed=%d/%d chunks_processed=%d unique_hashes_added=%d elapsed=%s", manifest.ID, processedEntries.Load(), len(manifest.Files), processedChunks.Load(), uniqueHashes.Load(), time.Since(started))
	})
	defer stopProgress()
	for _, entry := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		for _, chunk := range entry.Chunks {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			if chunk.Zero {
				processedChunks.Add(1)
				continue
			}
			candidate := chunkLocation{Path: entry.Path, Offset: chunk.Offset, Size: chunk.Size}
			if primary, seen := delta[chunk.Hash]; seen {
				addChunkAlternate(alternates, chunk.Hash, primary, candidate)
				processedChunks.Add(1)
				continue
			}
			if baseLocation, ok := base[chunk.Hash]; ok {
				if baseLocation == candidate {
					processedChunks.Add(1)
					continue
				}
				// Keep a new source location even when an unchanged base location was encountered first.
				delta[chunk.Hash] = candidate
				addChunkAlternate(alternates, chunk.Hash, candidate, baseLocation)
				uniqueHashes.Add(1)
				processedChunks.Add(1)
				continue
			}
			delta[chunk.Hash] = candidate
			uniqueHashes.Add(1)
			processedChunks.Add(1)
		}
		processedEntries.Add(1)
	}
	stopProgress()
	return delta, alternates, nil
}

func indexManifestWithAlternatesContext(ctx context.Context, m *SnapshotManifest) (map[string]chunkLocation, map[string][]chunkLocation, error) {
	primary := make(map[string]chunkLocation)
	alternates := make(map[string][]chunkLocation)
	var processedEntries, processedChunks, uniqueHashes atomic.Int64
	started := time.Now()
	stopProgress := startPeriodicProgressLog(func() {
		log.Info("Indexing replication manifest chunks progress: snapshot=%s index_kind=full entries_processed=%d/%d chunks_processed=%d unique_hashes=%d elapsed=%s", m.ID, processedEntries.Load(), len(m.Files), processedChunks.Load(), uniqueHashes.Load(), time.Since(started))
	})
	defer stopProgress()
	for _, entry := range m.Files {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		for _, chunk := range entry.Chunks {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			if chunk.Zero {
				processedChunks.Add(1)
				continue
			}
			candidate := chunkLocation{Path: entry.Path, Offset: chunk.Offset, Size: chunk.Size}
			first, ok := primary[chunk.Hash]
			if !ok {
				primary[chunk.Hash] = candidate
				uniqueHashes.Add(1)
			} else {
				addChunkAlternate(alternates, chunk.Hash, first, candidate)
			}
			processedChunks.Add(1)
		}
		processedEntries.Add(1)
	}
	stopProgress()
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

func readChunkFromRoot(root, resolvedRoot string, loc chunkLocation, expected string) ([]byte, error) {
	if err := validateTreePath(loc.Path); err != nil {
		return nil, err
	}
	if loc.Offset < 0 || loc.Size <= 0 || loc.Size > chunkMaxSize || loc.Offset > math.MaxInt64-loc.Size {
		return nil, errors.New("invalid chunk source range")
	}
	if resolvedRoot == "" {
		var err error
		resolvedRoot, err = resolvedPath(root)
		if err != nil {
			return nil, err
		}
	}
	f, _, err := openRegularFileBeneath(resolvedRoot, loc.Path)
	if err != nil {
		return nil, fmt.Errorf("open chunk source %q: %w", loc.Path, err)
	}
	defer f.Close()
	data := make([]byte, loc.Size)
	if _, err := f.ReadAt(data, loc.Offset); err != nil {
		return nil, fmt.Errorf("read chunk source %q at offset %d size %d: %w", loc.Path, loc.Offset, loc.Size, err)
	}
	sum := sha256.Sum256(data)
	if !matchesSHA256Hex(sum, expected) {
		return nil, fmt.Errorf("source chunk changed after manifest creation: path=%q offset=%d size=%d", loc.Path, loc.Offset, loc.Size)
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
	created := false
	if err := os.Mkdir(path, 0o700); err != nil {
		if !os.IsExist(err) {
			return err
		}
	} else {
		created = true
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("replication chunk cache path %q must be a real directory", path)
	}
	permissionsNeedSync := info.Mode().Perm() != 0o700 || info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0
	if created || permissionsNeedSync {
		dir, err := openDirectory(path, info)
		if err != nil {
			return fmt.Errorf("open replication chunk cache directory %q for persistence: %w", path, err)
		}
		var chmodErr error
		if permissionsNeedSync {
			chmodErr = dir.Chmod(0o700)
		}
		syncErr := dir.Sync()
		closeErr := dir.Close()
		if err := errors.Join(chmodErr, syncErr, closeErr); err != nil {
			return fmt.Errorf("persist replication chunk cache directory %q: %w", path, err)
		}
		if permissionsNeedSync {
			log.Debug("Restored private permissions on replication chunk cache directory: path=%s previous_mode=%s mode=0700", path, info.Mode().String())
		}
	}
	return nil
}

func prepareChunkCache(cacheDir string) error {
	if err := ensureRealDirectory(cacheDir); err != nil {
		return err
	}
	if err := syncDirectory(filepath.Dir(cacheDir)); err != nil {
		return fmt.Errorf("persist parent of replication chunk cache directory %q: %w", cacheDir, err)
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
		removedInShard := false
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
			removedInShard = true
		}
		if removedInShard {
			if err := syncDirectory(shardPath); err != nil {
				return fmt.Errorf("persist removal of orphaned replication chunk cache temp files in %q: %w", shardPath, err)
			}
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
	if len(hash) != sha256.Size*2 || !isLowerHex(hash) {
		return errors.New("invalid replication chunk hash")
	}
	if len(data) > chunkMaxSize {
		return errors.New("replication chunk exceeds maximum size")
	}
	sum := sha256.Sum256(data)
	if !matchesSHA256Hex(sum, hash) {
		actual := hex.EncodeToString(sum[:])
		return fmt.Errorf("%w: got %s want %s", errChunkHashMismatch, actual, hash)
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
		if info.Mode().IsRegular() && info.Size() == int64(len(data)) && verifyFile(path, hash) == nil {
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
		return errors.Join(syncDirectory(filepath.Dir(cacheDir)), syncDirectory(cacheDir), syncDirectory(dir))
	}
	return nil
}

func syncChunkCacheDirectories(cacheDir, snapshotID, phase string, shardSet *sync.Map) error {
	started := time.Now()
	logResult := func(shards int, err error) error {
		if err != nil {
			log.Error("Replication chunk cache directory sync failed: snapshot=%s phase=%s shards=%d duration=%s error=%v", snapshotID, phase, shards, time.Since(started), err)
		} else if shards > 0 {
			log.Info("Replication chunk cache directories synced: snapshot=%s phase=%s shards=%d duration=%s", snapshotID, phase, shards, time.Since(started))
		}
		return err
	}
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
		return logResult(0, errors.Join(syncErrors...))
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
	return logResult(len(shards), errors.Join(syncErrors...))
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

func decodeManifestJSON(reader io.Reader, value any) error {
	limited := &io.LimitedReader{R: reader, N: int64(maxManifestSize) + 1}
	decoder := newManifestDecoder(limited)
	if err := decoder.Decode(value); err != nil {
		if limited.N == 0 {
			return errors.New("incremental manifest exceeds maximum size")
		}
		return fmt.Errorf("decode incremental manifest: %w", err)
	}
	if err := consumeJSONDocumentTail(decoder.Buffered(), limited); err != nil {
		if limited.N == 0 {
			return errors.New("incremental manifest exceeds maximum size")
		}
		return fmt.Errorf("%w: %v", errManifestTrailingData, err)
	}
	if limited.N == 0 {
		return errors.New("incremental manifest exceeds maximum size")
	}
	return nil
}

func loadManifestFile(path string) (*SnapshotManifest, error) {
	file, err := openManifestFile(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var m SnapshotManifest
	if err := decodeManifestJSON(file, &m); err != nil {
		return nil, err
	}
	if err := validateIncrementalManifest(&m); err != nil {
		return nil, err
	}
	if err := validateManifestPathID(path, m.ID); err != nil {
		return nil, err
	}
	return &m, nil
}

func loadTrustedManifest(path, token, state string) (*SnapshotManifest, error) {
	return loadTrustedManifestStates(path, token, state)
}

func validateManifestPathID(path, manifestID string) error {
	name := filepath.Base(path)
	if name == baselineManifestName || name == "current.json" {
		return nil
	}
	fileID, ok := strings.CutSuffix(name, ".json")
	if !ok {
		return nil
	}
	if !validSnapshotID(fileID) {
		return fmt.Errorf("manifest file name %q does not contain a valid snapshot ID", name)
	}
	if fileID == manifestID {
		return nil
	}
	return fmt.Errorf("manifest ID %q does not match file name %q", manifestID, name)
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
