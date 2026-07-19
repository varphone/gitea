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
	"path/filepath"
	"slices"
	"strings"
	"time"

	"code.gitea.io/gitea/modules/json"
	"code.gitea.io/gitea/modules/setting"
)

const (
	incrementalFormatVersion = 2
	chunkMinSize             = 256 << 10
	chunkAverageSize         = 1 << 20
	chunkMaxSize             = 4 << 20
	maxManifestSize          = 256 << 20
)

var errManifestTrailingData = errors.New("incremental manifest contains oversized or trailing data")

type ChunkDescriptor struct {
	Hash   string `json:"hash"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
}

type TreeEntry struct {
	Path       string            `json:"path"`
	Type       string            `json:"type"`
	Mode       uint32            `json:"mode"`
	Size       int64             `json:"size,omitempty"`
	ModTimeNS  int64             `json:"mtime_ns,omitempty"`
	ChangeID   string            `json:"change_id,omitempty"`
	LinkTarget string            `json:"link_target,omitempty"`
	Chunks     []ChunkDescriptor `json:"chunks,omitempty"`
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

// splitFile uses content-defined boundaries, so an insertion does not
// invalidate every following chunk as fixed-size blocks would.
func splitFile(ctx context.Context, path string) ([]ChunkDescriptor, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buffer := make([]byte, 128<<10)
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
			rolling = (rolling << 1) + gearValue(b)
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

func scanIncrementalTree(ctx context.Context, root string) (*SnapshotManifest, error) {
	return scanIncrementalTreeWithOptions(ctx, root, nil, true)
}

func scanIncrementalTreeWithBase(ctx context.Context, root string, base *SnapshotManifest) (*SnapshotManifest, error) {
	return scanIncrementalTreeWithOptions(ctx, root, base, false)
}

func scanIncrementalTreeWithOptions(ctx context.Context, root string, base *SnapshotManifest, verifyAll bool) (*SnapshotManifest, error) {
	rootInfo, err := os.Stat(root)
	if err != nil {
		return nil, err
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
	err = filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil || rel == "." {
			return err
		}
		rel = filepath.ToSlash(rel)
		e := TreeEntry{Path: rel, Mode: uint32(info.Mode().Perm()), ModTimeNS: info.ModTime().UnixNano(), ChangeID: fileChangeID(info)}
		switch {
		case info.IsDir():
			e.Type = "dir"
		case info.Mode()&os.ModeSymlink != 0:
			e.Type = "symlink"
			e.LinkTarget, err = os.Readlink(path)
			if err == nil {
				err = validateTreeLink(rel, e.LinkTarget)
			}
			if err != nil {
				return err
			}
		case info.Mode().IsRegular():
			e.Type, e.Size = "file", info.Size()
			old, hasOld := baseEntries[rel]
			metadataUnchanged := hasOld && old.Type == "file" && old.Size == info.Size() &&
				old.Mode == uint32(info.Mode().Perm()) && old.ModTimeNS == info.ModTime().UnixNano() &&
				old.ChangeID != "" && old.ChangeID == e.ChangeID
			if metadataUnchanged && !verifyAll {
				e.Chunks = old.Chunks
				m.Size += e.Size
				m.Files = append(m.Files, e)
				return nil
			}
			beforeSize, beforeTime, beforeChangeID := info.Size(), info.ModTime(), e.ChangeID
			e.Chunks, err = chunkFileForManifest(ctx, path)
			if err != nil {
				return err
			}
			after, err := os.Stat(path)
			if err != nil || after.Size() != beforeSize || after.ModTime() != beforeTime || fileChangeID(after) != beforeChangeID {
				return fmt.Errorf("file changed while scanning: %s", rel)
			}
			if verifyAll && metadataUnchanged && !sameChunks(e.Chunks, old.Chunks) {
				return fmt.Errorf("content verification failed with unchanged metadata: %s", rel)
			}
			m.Size += e.Size
		default:
			return fmt.Errorf("unsupported filesystem entry %q", rel)
		}
		m.Files = append(m.Files, e)
		return nil
	})
	if err != nil {
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

func manifestDigest(manifest *SnapshotManifest) (string, error) {
	manifestCopy := *manifest
	manifestCopy.SHA256 = ""
	manifestCopy.Signature = ""
	data, err := json.Marshal(&manifestCopy)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
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
	seen := make(map[string]struct{}, len(m.Files))
	for _, e := range m.Files {
		if err := validateTreePath(e.Path); err != nil {
			return err
		}
		if _, ok := seen[e.Path]; ok {
			return fmt.Errorf("duplicate manifest path %q", e.Path)
		}
		seen[e.Path] = struct{}{}
		if e.Mode > 0o777 || len(e.ChangeID) > 128 {
			return fmt.Errorf("invalid metadata in %q", e.Path)
		}
		switch e.Type {
		case "dir":
			if e.Size != 0 || e.LinkTarget != "" || len(e.Chunks) != 0 {
				return fmt.Errorf("invalid directory fields in %q", e.Path)
			}
		case "symlink":
			if e.Size != 0 || len(e.Chunks) != 0 {
				return fmt.Errorf("invalid symlink fields in %q", e.Path)
			}
			if err := validateTreeLink(e.Path, e.LinkTarget); err != nil {
				return err
			}
		case "file":
			if e.LinkTarget != "" {
				return fmt.Errorf("invalid file link target in %q", e.Path)
			}
			if e.ChangeID == "" {
				return fmt.Errorf("missing file change identity in %q", e.Path)
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
				return errors.New("manifest logical size overflow")
			}
			logicalSize += e.Size
		default:
			return fmt.Errorf("invalid entry type %q", e.Type)
		}
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

func readChunk(root string, loc chunkLocation, expected string) ([]byte, error) {
	if err := validateTreePath(loc.Path); err != nil {
		return nil, err
	}
	path := filepath.Join(root, filepath.FromSlash(loc.Path))
	rootResolved, err := resolvedPath(root)
	if err != nil {
		return nil, err
	}
	pathResolved, err := resolvedPath(path)
	if err != nil || !isWithin(rootResolved, pathResolved) {
		return nil, errors.New("chunk source resolves outside data root")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("chunk source is not a regular file")
	}
	f, err := os.Open(path)
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

func storeChunk(cacheDir, hash string, data []byte) error {
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != hash {
		return errors.New("received chunk hash mismatch")
	}
	path := cachePath(cacheDir, hash)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	return writeFileSynced(path, data, 0o600)
}

func loadManifestFile(path string) (*SnapshotManifest, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.Size() > maxManifestSize {
		return nil, errors.New("incremental manifest exceeds maximum size")
	}
	data, err := os.ReadFile(path)
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
