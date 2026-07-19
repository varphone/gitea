# Incremental SQLite disaster recovery over HTTP

This design uses the Gitea binary with an independent
`gitea-replication.service`. The control plane remains reachable through
Nginx while `gitea.service` is stopped for the short final consistency phase.
The primary never writes a full data archive.

## Data and filesystem layout

SQLite, repositories, LFS, attachments, avatars, packages, Actions logs and
artifacts must use local storage below the same `APP_WORK_PATH` on both
servers. Keep the normal Gitea layout, such as `/var/lib/gitea`; no `current`
subdirectory or symlink convention is required. Paths are resolved and storage
outside this root is rejected.

`SNAPSHOT_DIR` contains signed JSON manifests on the primary, not copies of
Git, LFS, or attachment data. On the standby it also contains a temporary
content-addressed chunk cache while a synchronization is in progress. Verified
chunks survive an interrupted run for resumption and are removed after a
successful atomic installation. On the standby, `SNAPSHOT_DIR` and
`APP_WORK_PATH` must be on the same local filesystem.

Provision `app.ini`, system OpenSSH host keys, TLS certificates, Nginx,
systemd, and Polkit independently on both nodes. Gitea versions and
`APP_WORK_PATH` values must match. Numeric UID/GID may differ.
`SECRET_KEY`, `INTERNAL_TOKEN`, the LFS JWT secret, and the independent
replication token must match. Every manifest has both an instance-secret
fingerprint and an HMAC signature.

## Synchronization protocol

The standby drives every synchronization:

1. While the primary is online, it walks the filesystem metadata and compares
   it with the last authenticated successful baseline. Files with unchanged
   size, mode, nanosecond modification time, inode, and Linux ctime reuse their
   prior chunk list without being read. New or changed files are split into
   content-defined chunks, and the standby downloads only chunks absent from
   its currently installed manifest. Changed live chunks are safely deferred.
2. The standby asks for finalization. The primary acquires the cross-process
   write fence, stops `gitea.service`, and rechecks the tree. Files whose size, mode, nanosecond modification time, inode, and Linux ctime
   match the preflight manifest reuse
   their hashes without rereading their contents.
3. Only chunks changed since preflight are transferred while the primary is
   stopped. The standby reconstructs a durable staging tree, hard-linking
   unchanged complete files where possible. Missing paths in the new manifest
   are deletions and are not reconstructed.
4. After the standby confirms the staging tree is durable, the primary starts
   Gitea, passes its health check, releases the write fence, and marks the
   signed manifest ready.
5. A narrowly scoped root-owned systemd helper uses Linux `RENAME_EXCHANGE` to
   atomically switch the standard `APP_WORK_PATH` with the prepared staging
   directory. The standby preserves its local
   configuration, regenerates `authorized_keys`, verifies Gitea readiness,
   then stops the standby service again.

Chunk boundaries average 1 MiB, with 256 KiB minimum and 4 MiB maximum.
Content-defined boundaries allow later chunks to be reused after insertions.
A final session is automatically aborted and the primary restarted when
`FINAL_SESSION_TIMEOUT` expires or the control service stops. `SNAPSHOT_TIMEOUT`
bounds manifest scans and does not extend the primary outage.

## Control API

The replication control plane exposes one stable, internal resource API. Create a job with `POST /api/v1/replication/sync-jobs` and a JSON body of either `{"kind":"preflight","resume_job_id":"..."}` or `{"kind":"final","base_job_id":"..."}`. The response is `202 Accepted` while the job is running.

- `GET /api/v1/replication/sync-jobs` and `GET /api/v1/replication/sync-jobs/<id>` expose job status.
- `GET /api/v1/replication/sync-jobs/<id>/manifest` retrieves the signed job manifest.
- `GET /api/v1/replication/sync-jobs/<id>/chunks/<sha256>` transfers chunk content.
- `POST /api/v1/replication/sync-jobs/<id>/session/{complete,abort}` finishes a final session.

`/api/v1/replication/health` remains the health endpoint. These endpoints are internal to the
replication controller and require its bearer token.

## Configuration

```ini
[database]
DB_TYPE = sqlite3

[replicate]
ENABLED = true
MODE = primary
CONTROL_LISTEN = 127.0.0.1:3001
CONTROL_TOKEN = replace-with-at-least-32-random-bytes
SNAPSHOT_DIR = /var/lib/gitea-replication/snapshots
SNAPSHOT_RETENTION = 3
FULL_SCAN_INTERVAL = 168h
GITEA_SERVICE_NAME = gitea.service
SERVICE_TIMEOUT = 2m
SNAPSHOT_TIMEOUT = 24h
FINAL_SESSION_TIMEOUT = 15m
```

`SNAPSHOT_RETENTION` controls small manifest history only. It no longer
multiplies primary data usage. `baseline.json` is a small signed copy of the
latest successful primary manifest and is kept independently of that history.

`FULL_SCAN_INTERVAL` defaults to `168h`. Once that interval has elapsed, the
next preflight deliberately rereads and rehashes every regular file to detect
silent storage corruption. If content differs while its protected metadata is
unchanged, synchronization stops instead of propagating the suspect bytes.
After a successful verification it starts a new incremental baseline. Set it to
`0` to disable scheduled full verification. Incremental preflights still walk
all paths and perform metadata checks, so their cost is proportional to the
number of filesystem entries, while bytes read are proportional to changed
files. Repositories with millions of loose Git objects may therefore still
benefit from normal Git maintenance and repacking.

On the standby use `MODE = replica`, set `SOURCE_URL` to the primary Gitea
URL and optionally set `CONTROL_SOURCE_URL` to
`https://primary.example/_replication`. Remote control URLs must use HTTPS;
cleartext HTTP is accepted only for loopback.

If the standby must reach the primary through an explicit network proxy, set
`CONTROL_PROXY_URL` on the standby. This overrides environment proxy settings
for replication traffic only. Example: `CONTROL_PROXY_URL = http://proxy.example:3128`.

Disable `gitea.socket`; the controller refuses finalization or installation
while socket activation is active. Do not run administrative CLI jobs outside
the fenced Gitea service during the final phase.

## systemd and Nginx

Install the control and restore units on both nodes, but enable the restore
timer only on the standby:

```sh
install -d -o git -g git -m 0700 /var/lib/gitea /var/lib/gitea-replication/snapshots
install -m 0644 contrib/systemd/gitea-replication.service \
  contrib/systemd/gitea-replication-switch.service \
  contrib/systemd/gitea-replication-restore.service \
  contrib/systemd/gitea-replication-restore.timer /etc/systemd/system/
install -m 0644 contrib/polkit/60-gitea-replication.rules /etc/polkit-1/rules.d/
systemctl daemon-reload
systemctl enable --now gitea-replication.service
# standby only:
systemctl enable --now gitea-replication-restore.timer
```

The restore worker uses a non-blocking `Type=simple` unit: starting it does not
wait for a potentially long synchronization to finish. The timer schedules the
next run one hour after the worker exits, so a slow or retrying transfer never
overlaps with another restore run.

Keep ordinary `gitea.service` on its standard
`GITEA_WORK_DIR=/var/lib/gitea`. Adjust every systemd `ReadWritePaths` entry
if paths differ. The switch unit runs as root because renaming
`/var/lib/gitea` requires access to its root-owned parent, but its command can
exchange only the configured `APP_WORK_PATH` and fixed `.install-stage` below
`SNAPSHOT_DIR`. The supplied Polkit rule lets `git` start only this fixed
helper and manage `gitea.service`. Restrict `/_replication/` by source IP, TLS,
and the independent bearer token.

## Capacity and failure behavior

The primary needs space only for manifests. The standby needs its active data,
the current delta chunk cache, and staging metadata. Unchanged files are
hard-linked into staging, so they consume no second copy. Changed files require
temporary space equal to their reconstructed size until the atomic switch and
old-tree cleanup complete.

An interrupted preflight leaves Gitea online. An interrupted final session is
bounded by `FINAL_SESSION_TIMEOUT`; transient transport failures retain the active
final session and the standby service retries after 30 seconds, resuming it
from the signed final manifest and verified chunk cache without a new preflight
scan. If the primary control service itself restarts, its in-memory write fence is lost and the interrupted final session is deliberately rejected; the next run performs a fresh preflight to preserve snapshot consistency. The primary restarts automatically when the session expires or is explicitly aborted.

## Failover

1. Fence the failed primary to prevent split brain.
2. Disable `gitea-replication-restore.timer` on the standby.
3. Confirm the last restore service completed successfully.
4. Change standby `[replicate] MODE` to `primary` and restart
   `gitea-replication.service`.
5. Start `gitea.service` and switch DNS/VIP.
6. Enable the restore timer only after a new standby is provisioned.

Never promote both nodes. RPO equals the standby restore-timer interval. RTO is
dominated by the final delta, atomic directory switch, and Gitea health check.
