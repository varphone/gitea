// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"bytes"
	"io"
	"net/http"
	"strings"

	"gitea.dev/modules/json"
	"gitea.dev/modules/log"
	"gitea.dev/modules/setting"
)

const (
	primaryRecoveryPendingMessage = "Gitea is temporarily read-only while primary recovery is pending"
	replicaReadOnlyMessage        = "the disaster-recovery replica is read-only; writes are disabled until an operator promotes this node"
)

const maxPrimaryRecoveryLFSBatchBytes = 1024 * 1024

var primaryRecoveryStateChangingRoutes = [...]string{
	"/user/activate",
	"/user/activate_email",
	"/user/forgot_password",
	"/user/link_account",
	"/user/login",
	"/user/login/openid",
	"/user/logout",
	"/user/openid",
	"/user/oauth2",
	"/user/recover_account",
	"/user/sign_up",
	"/user/settings/security/two_factor/enroll",
	"/user/two_factor",
	"/user/webauthn",
	"/login/oauth",
}

// WriteProtectionMiddleware rejects state-changing requests while replica mode or a
// pending primary recovery disables writes.
func WriteProtectionMiddleware(next http.Handler) http.Handler {
	cfg, err := loadConfig()
	if err != nil {
		// The write policy cannot be evaluated, so reject state-changing requests
		// instead of trusting an unknown state. Reads stay available.
		log.Error("Cannot load replication configuration; rejecting state-changing requests while the policy is unknown: %v", err)
		return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			switch request.Method {
			case http.MethodGet, http.MethodHead, http.MethodOptions:
				next.ServeHTTP(w, request)
				return
			case http.MethodPost:
				if primaryRecoverySafePost(request) {
					next.ServeHTTP(w, request)
					return
				}
			}
			log.Warn("Rejected request with unknown replication policy: method=%s path=%s remote=%s", request.Method, request.URL.EscapedPath(), request.RemoteAddr)
			http.Error(w, primaryRecoveryPendingMessage, http.StatusServiceUnavailable)
		})
	}
	if !cfg.Enabled {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		safePostPath, needsLFSBodyCheck := false, false
		if request.Method == http.MethodPost {
			safePostPath, needsLFSBodyCheck = primaryRecoverySafePostPath(request)
			if safePostPath && !needsLFSBodyCheck {
				next.ServeHTTP(w, request)
				return
			}
		}
		mayChangeState := true
		switch request.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			mayChangeState = isPrimaryRecoveryStateChangingRoute(request.URL.Path)
		}
		writeProtected := cfg.Mode == modeReplica
		if !writeProtected && cfg.Mode == modePrimary {
			writeProtected = primaryOutageCheckpointExists(cfg.SnapshotDir)
		}
		if !mayChangeState || !writeProtected {
			next.ServeHTTP(w, request)
			return
		}
		if safePostPath && (!needsLFSBodyCheck || isPrimaryRecoveryLFSDownload(request)) {
			next.ServeHTTP(w, request)
			return
		}
		log.Warn("Rejected request while replication write protection is active: method=%s path=%s remote=%s", request.Method, request.URL.EscapedPath(), request.RemoteAddr)
		message := primaryRecoveryPendingMessage
		if cfg.Mode == modeReplica {
			message = replicaReadOnlyMessage
		}
		http.Error(w, message, http.StatusServiceUnavailable)
	})
}

func primaryRecoverySafePost(request *http.Request) bool {
	safe, needsLFSBodyCheck := primaryRecoverySafePostPath(request)
	return safe && (!needsLFSBodyCheck || isPrimaryRecoveryLFSDownload(request))
}

func primaryRecoverySafePostPath(request *http.Request) (safe, needsLFSBodyCheck bool) {
	path := primaryRecoveryRoutePath(request.URL.Path)
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 3 {
		if parts[0] == "login" && parts[1] == "oauth" {
			return parts[2] == "userinfo" || parts[2] == "introspect", false
		}
		if parts[0] != "" && parts[1] != "" {
			// Git smart HTTP uses POST for fetch and archive reads.
			return parts[2] == "git-upload-pack" || parts[2] == "git-upload-archive", false
		}
	}
	if len(parts) >= 4 && parts[0] != "" && parts[1] != "" && parts[2] == "_preview" {
		return true, false
	}
	if len(parts) != 6 || parts[0] == "" || parts[1] == "" ||
		parts[2] != "info" || parts[3] != "lfs" || parts[4] != "objects" || parts[5] != "batch" {
		return false, false
	}
	return true, true
}

func isPrimaryRecoveryLFSDownload(request *http.Request) bool {
	if request.Body == nil {
		return false
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, maxPrimaryRecoveryLFSBatchBytes+1))
	if err != nil || len(body) > maxPrimaryRecoveryLFSBatchBytes {
		return false
	}
	request.Body = io.NopCloser(bytes.NewReader(body))
	var batch struct {
		Operation string `json:"operation"`
	}
	return json.Unmarshal(body, &batch) == nil && batch.Operation == "download"
}

func isPrimaryRecoveryStateChangingRoute(path string) bool {
	path = primaryRecoveryRoutePath(path)
	// These OAuth endpoints only expose identity and public signing metadata.
	if path == "/login/oauth/userinfo" || path == "/login/oauth/keys" {
		return false
	}
	for _, prefix := range primaryRecoveryStateChangingRoutes {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return true
		}
	}
	return false
}

func primaryRecoveryRoutePath(path string) string {
	subURL := strings.TrimSuffix(setting.AppSubURL, "/")
	if subURL != "" && strings.HasPrefix(path, subURL+"/") {
		return strings.TrimPrefix(path, subURL)
	}
	return path
}
