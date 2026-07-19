// Copyright 2024 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"code.gitea.io/gitea/modules/setting"
)

func TestReadOnlyMiddleware(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler := ReadOnlyMiddleware(next)
	for _, test := range []struct {
		method string
		want   int
	}{{http.MethodGet, http.StatusNoContent}, {http.MethodHead, http.StatusNoContent}, {http.MethodOptions, http.StatusNoContent}, {http.MethodPost, http.StatusServiceUnavailable}, {http.MethodPut, http.StatusServiceUnavailable}, {http.MethodDelete, http.StatusServiceUnavailable}} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(test.method, "/", nil))
		if response.Code != test.want {
			t.Fatalf("%s status=%d want=%d", test.method, response.Code, test.want)
		}
	}
}

func TestReadOnlyMiddlewareRendersReplicaNoticeForBrowsers(t *testing.T) {
	handler := ReadOnlyMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("write request reached handler") }))
	request := httptest.NewRequest(http.MethodPost, "/user/login", nil)
	request.Header.Set("Accept", "text/html")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", response.Code)
	}
	if !strings.Contains(response.Body.String(), "此备用节点处于只读灾难恢复模式") {
		t.Fatalf("unexpected notice body: %s", response.Body.String())
	}
	if contentType := response.Header().Get("Content-Type"); !strings.Contains(contentType, "text/html") {
		t.Fatalf("content type=%q", contentType)
	}
}

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
	if err := signIncrementalManifest(manifest, token); err != nil {
		t.Fatal(err)
	}
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
	manifest.InstanceFingerprint = instanceFingerprint(token)
	if err := signIncrementalManifest(manifest, token); err != nil {
		t.Fatal(err)
	}
	server := &controlServer{
		cfg:           &config{Mode: modePrimary, ControlToken: token, SnapshotDir: t.TempDir(), FullScanInterval: time.Hour},
		jobs:          map[string]*Snapshot{manifest.ID: &manifest.Snapshot},
		taskManifests: map[string]*SnapshotManifest{manifest.ID: manifest},
	}
	response := httptest.NewRecorder()
	server.preflight(response, httptest.NewRequest(http.MethodPost, syncJobsPath, nil))
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
