// Copyright 2024 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"fmt"
	"net/http"
	"strings"
)

const replicaReadOnlyMessage = "disaster-recovery replica is read-only; login and write operations are disabled until an operator promotes this node after fencing the primary"

func writeReplicaReadOnlyResponse(w http.ResponseWriter, request *http.Request) {
	if !strings.Contains(request.Header.Get("Accept"), "text/html") {
		http.Error(w, replicaReadOnlyMessage, http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = fmt.Fprint(w, `<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>灾难恢复备用节点 - Gitea</title>
<style>:root{--g:#609926;--t:#24292f;--m:#57606a;--b:#d0d7de;--bg:#f6f8fa}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--t);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif}.nav{height:54px;background:var(--g);color:#fff;display:flex;align-items:center;padding:0 max(24px,calc((100% - 1120px)/2));font-size:20px;font-weight:600}.mark{width:25px;height:20px;border:3px solid #fff;border-radius:5px;display:inline-block;margin-right:9px}.page{max-width:760px;margin:72px auto;padding:0 20px}.card{background:#fff;border:1px solid var(--b);border-radius:7px;box-shadow:0 1px 2px #1b1f240a;padding:34px 38px}.status{color:#9a6700;font-size:14px;font-weight:600;letter-spacing:.08em}h1{font-size:28px;margin:10px 0 16px}h2{font-size:18px;margin:28px 0 10px}p,li{line-height:1.65}p,li,.foot{color:var(--m)}ol{padding-left:24px}.notice{border-left:4px solid #bf8700;background:#fff8c5;padding:12px 16px;margin:22px 0;border-radius:3px}.button{display:inline-block;margin-top:8px;background:var(--g);color:#fff;text-decoration:none;border-radius:5px;padding:9px 14px;font-weight:600}.foot{margin-top:18px;font-size:13px}</style></head>
<body><header class="nav"><span class="mark"></span>GITEA</header><main class="page"><section class="card"><div class="status">503 · DISASTER RECOVERY REPLICA</div><h1>此备用节点处于只读灾难恢复模式</h1><p>为保证与主节点的数据一致性，此节点拒绝登录以及所有会写入数据的操作。浏览、克隆和其他只读访问不受影响。</p><div class="notice"><strong>请勿通过修改数据库或绕过限制登录。</strong> 这会破坏后续同步和故障切换的可靠性。</div><h2>需要在此节点恢复服务？</h2><ol><li>先隔离或确认主节点已停止，避免双主写入。</li><li>停止备用节点的 restore timer。</li><li>将 <code>[replicate] MODE</code> 改为 <code>primary</code>，重启 replication 服务后再启动 Gitea。</li></ol><a class="button" href="/">返回首页</a><div class="foot">该限制由 Gitea disaster-recovery replication 保护机制强制执行。</div></section></main></body></html>`)
}

// ReadOnlyMiddleware permits only side-effect-free HTTP methods on a replica.
func ReadOnlyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			next.ServeHTTP(w, request)
		default:
			writeReplicaReadOnlyResponse(w, request)
		}
	})
}
