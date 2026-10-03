// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"bytes"
	"fmt"
	"html"
	"io"
	"mime"
	"net/http"
	"strings"

	"gitea.dev/modules/json"
	"gitea.dev/modules/log"
	"gitea.dev/modules/setting"
)

const (
	replicaReadOnlyMessage        = "disaster-recovery replica is read-only; login and write operations are disabled until an operator promotes this node after fencing the primary"
	primaryRecoveryPendingMessage = "Gitea is temporarily read-only while primary recovery is pending"
)

const maxReplicaReadOnlyLFSBatchBytes = 1 << 20

var replicaStateChangingRoutes = [...]string{
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

func writeReplicaReadOnlyResponse(w http.ResponseWriter, request *http.Request) {
	log.Warn("Rejected replica request while read-only: method=%s path=%s remote=%s", request.Method, request.URL.EscapedPath(), request.RemoteAddr)
	w.Header().Add("Vary", "Accept")
	if !requestAcceptsHTML(request) {
		http.Error(w, replicaReadOnlyMessage, http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)
	homePath := strings.TrimRight(setting.AppSubURL, "/") + "/"
	_, _ = fmt.Fprint(w, `<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>灾难恢复备用节点 - Gitea</title>
<style>:root{--g:#609926;--t:#24292f;--m:#57606a;--b:#d0d7de;--bg:#f6f8fa}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--t);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif}.nav{height:54px;background:var(--g);color:#fff;display:flex;align-items:center;padding:0 max(24px,calc((100% - 1120px)/2));font-size:20px;font-weight:600}.mark{width:25px;height:20px;border:3px solid #fff;border-radius:5px;display:inline-block;margin-right:9px}.page{max-width:760px;margin:72px auto;padding:0 20px}.card{background:#fff;border:1px solid var(--b);border-radius:7px;box-shadow:0 1px 2px #1b1f240a;padding:34px 38px}.status{color:#9a6700;font-size:14px;font-weight:600;letter-spacing:.08em}h1{font-size:28px;margin:10px 0 16px}h2{font-size:18px;margin:28px 0 10px}p,li{line-height:1.65}p,li,.foot{color:var(--m)}ol{padding-left:24px}.notice{border-left:4px solid #bf8700;background:#fff8c5;padding:12px 16px;margin:22px 0;border-radius:3px}.button{display:inline-block;margin-top:8px;background:var(--g);color:#fff;text-decoration:none;border-radius:5px;padding:9px 14px;font-weight:600}.foot{margin-top:18px;font-size:13px}</style></head>
<body><header class="nav"><span class="mark"></span>GITEA</header><main class="page"><section class="card"><div class="status">503 · DISASTER RECOVERY REPLICA</div><h1>此备用节点处于只读灾难恢复模式</h1><p>为保证与主节点的数据一致性，此节点拒绝登录以及所有会写入数据的操作。浏览、克隆和其他只读访问不受影响。</p><div class="notice"><strong>请勿通过修改数据库或绕过限制登录。</strong> 这会破坏后续同步和故障切换的可靠性。</div><h2>需要在此节点恢复服务？</h2><ol><li>先隔离或确认主节点已停止，避免双主写入。</li><li>停止备用节点的 restore timer。</li><li>将 <code>[replicate] MODE</code> 改为 <code>primary</code>，重启 replication 服务后再启动 Gitea。</li></ol><a class="button" href="`, html.EscapeString(homePath), `">返回首页</a><div class="foot">该限制由 Gitea disaster-recovery replication 保护机制强制执行。</div></section></main></body></html>`)
}

func requestAcceptsHTML(request *http.Request) bool {
	bestSpecificity := -1
	bestQuality := 0.0
	for _, header := range request.Header.Values("Accept") {
		for item := range strings.SplitSeq(header, ",") {
			mediaType, parameters, err := mime.ParseMediaType(strings.TrimSpace(item))
			if err != nil {
				continue
			}
			quality := 1.0
			if value, ok := parameters["q"]; ok {
				var valid bool
				quality, valid = parseEncodingQuality(value)
				if !valid {
					continue
				}
			}
			specificity := -1
			switch strings.ToLower(mediaType) {
			case "text/html":
				specificity = 2
			case "text/*":
				specificity = 1
			case "*/*":
				specificity = 0
			}
			if specificity > bestSpecificity {
				bestSpecificity, bestQuality = specificity, quality
			}
		}
	}
	return bestSpecificity >= 0 && bestQuality > 0
}

// ReadOnlyMiddleware blocks writes and known state-changing GET routes on a replica.
func ReadOnlyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			if isReplicaStateChangingRoute(request.URL.Path) {
				writeReplicaReadOnlyResponse(w, request)
				return
			}
			next.ServeHTTP(w, request)
		case http.MethodPost:
			if isReplicaReadOnlySafeSSHPost(request) || isReplicaReadOnlySafePost(request) {
				next.ServeHTTP(w, request)
				return
			}
			writeReplicaReadOnlyResponse(w, request)
		default:
			writeReplicaReadOnlyResponse(w, request)
		}
	})
}

func isReplicaReadOnlySafeSSHPost(request *http.Request) bool {
	if request.Method != http.MethodPost {
		return false
	}
	path := replicaRoutePath(request.URL.Path)
	if path == "/api/internal/ssh/authorized_keys" || path == "/api/internal/ssh/log" {
		return true
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 6 || parts[0] != "api" || parts[1] != "internal" || parts[2] != "ssh" || parts[4] != "update" {
		return false
	}
	for _, index := range []int{3, 5} {
		if parts[index] == "" {
			return false
		}
		for _, char := range parts[index] {
			if char < '0' || char > '9' {
				return false
			}
		}
	}
	return true
}

// PrimaryRecoveryMiddleware rejects mutations while a failed primary recovery is pending.
func PrimaryRecoveryMiddleware(next http.Handler) http.Handler {
	cfg, err := loadConfig()
	if err != nil || !cfg.Enabled || cfg.Mode != modePrimary {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		safePostPath, needsLFSBodyCheck := false, false
		if request.Method == http.MethodPost {
			safePostPath, needsLFSBodyCheck = replicaReadOnlySafePostPath(request)
			if safePostPath && !needsLFSBodyCheck {
				next.ServeHTTP(w, request)
				return
			}
		}
		mayChangeState := true
		switch request.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			mayChangeState = isReplicaStateChangingRoute(request.URL.Path)
		}
		if !mayChangeState || !primaryOutageCheckpointExists(cfg.SnapshotDir) {
			next.ServeHTTP(w, request)
			return
		}
		if safePostPath && (!needsLFSBodyCheck || isReplicaReadOnlyLFSDownload(request)) {
			next.ServeHTTP(w, request)
			return
		}
		log.Warn("Rejected request while primary recovery is pending: method=%s path=%s remote=%s", request.Method, request.URL.EscapedPath(), request.RemoteAddr)
		http.Error(w, primaryRecoveryPendingMessage, http.StatusServiceUnavailable)
	})
}

func isReplicaReadOnlySafePost(request *http.Request) bool {
	safe, needsLFSBodyCheck := replicaReadOnlySafePostPath(request)
	return safe && (!needsLFSBodyCheck || isReplicaReadOnlyLFSDownload(request))
}

func replicaReadOnlySafePostPath(request *http.Request) (safe, needsLFSBodyCheck bool) {
	path := replicaRoutePath(request.URL.Path)
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

func isReplicaReadOnlyLFSDownload(request *http.Request) bool {
	if request.Body == nil {
		return false
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, maxReplicaReadOnlyLFSBatchBytes+1))
	if err != nil || len(body) > maxReplicaReadOnlyLFSBatchBytes {
		return false
	}
	request.Body = io.NopCloser(bytes.NewReader(body))
	var batch struct {
		Operation string `json:"operation"`
	}
	return json.Unmarshal(body, &batch) == nil && batch.Operation == "download"
}

func isReplicaStateChangingRoute(path string) bool {
	path = replicaRoutePath(path)
	// These OAuth endpoints only expose identity and public signing metadata.
	if path == "/login/oauth/userinfo" || path == "/login/oauth/keys" {
		return false
	}
	for _, prefix := range replicaStateChangingRoutes {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return true
		}
	}
	return false
}

func replicaRoutePath(path string) string {
	subURL := strings.TrimSuffix(setting.AppSubURL, "/")
	if subURL != "" && strings.HasPrefix(path, subURL+"/") {
		return strings.TrimPrefix(path, subURL)
	}
	return path
}
