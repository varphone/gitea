# Incremental SQLite disaster recovery over HTTP

Replication is supported on Linux only. It relies on systemd, Linux write
fencing, and the atomic directory exchange operation `RENAME_EXCHANGE`.

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
Filesystem paths and symlink targets included in a snapshot must be valid
UTF-8 because manifests serialize them as JSON strings.
Neither server may mount another filesystem below `APP_WORK_PATH`; nested
mounts would remain attached to the old directory tree after an atomic switch.
The controller checks this at startup and around every snapshot scan; the
standby checks again before switching.

`SNAPSHOT_DIR` contains signed JSON manifests on the primary, not copies of
Git, LFS, or attachment data. On the standby it also contains a temporary
content-addressed chunk cache while a synchronization is in progress. Verified
chunks survive an interrupted run for resumption. Before each preflight and
final chunk transfer, stale chunks outside the current target manifest are
pruned; the cache is removed after a successful atomic installation. Set
`SNAPSHOT_DIR` to an absolute path so every Gitea and replication process uses
the same checkpoint and cache directory regardless of its working directory.
On the standby, `APP_WORK_PATH` must be a normal directory, not a mount point.
Its parent directory and `SNAPSHOT_DIR` must be on the same mount; two bind
mounts with matching device numbers are not sufficient for atomic exchange.

Provision `app.ini`, system OpenSSH host keys, TLS certificates, Nginx,
systemd, and Polkit independently on both nodes. Gitea-generated SSH
`authorized_keys` and trusted user CA key files are node-local when their paths
are below `APP_WORK_PATH`. Gitea versions and
`APP_WORK_PATH` values must match. Numeric UID/GID may differ.
The configured Gitea log directory, file logger outputs (including PID-suffixed
and rotated files), managed temporary directory, and profiling data directory
are excluded when they are below `APP_WORK_PATH`. A parent directory created
only for a node-local log is ignored during standby verification; directories
with other contents remain replicated. Use the systemd journal or an external
logging service to retain log history across restores; temporary uploads and
diagnostic profiles are not replicated.
`SECRET_KEY`, `INTERNAL_TOKEN`, the LFS JWT secret, and the independent
replication token must match. Every manifest has both an instance-secret
fingerprint and an HMAC signature.
OAuth2 `ENABLED` and `JWT_SIGNING_ALGORITHM` settings must also match. The
effective `JWT_SECRET` values must match; format 5 manifests verify the general
token signing secret with an HMAC.
For file-backed algorithms, configure `JWT_SIGNING_PRIVATE_KEY_FILE` at the
same path relative to `APP_WORK_PATH` on both nodes; the private key is included
in the replicated data. Both replication binaries must support format 6 to
exchange new manifests. Readers continue to accept manifests in formats 2, 3,
4, and 5. Format 6 fixes manifest digests and wire serialization to JSON v1.
An older format 5 manifest produced with JSON v2 serialization fails digest
validation and is treated as a stale checkpoint; the next sync rebuilds its
baseline and may need to re-index local standby chunks.

## Synchronization protocol

The standby drives every synchronization:

A completed preflight is reused for up to five minutes to recover request
retries; later restore runs scan current primary metadata again.

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
   stopped. The standby fetches final chunks with up to eight concurrent
   requests, then reconstructs a durable staging tree. It uses filesystem
   reflinks for unchanged complete files where supported and copies them when
   reflinks are unavailable. Missing paths in the new manifest are deletions
   and are not reconstructed.
4. After the standby confirms the staging tree is durable, it acquires its
   write fence. A narrowly scoped root-owned systemd helper uses Linux
   `RENAME_EXCHANGE` to atomically switch the standard `APP_WORK_PATH` with the
   prepared staging directory. The standby preserves its local configuration,
   regenerates `authorized_keys`, starts Gitea for a readiness check, and stops
   the standby service. It then verifies the installed tree against the final
   manifest and persists its readiness checkpoint. Only after those steps does
   the primary start Gitea and release its write fence.
5. The standby records local file identities, marks its signed manifest ready,
   and completes the final session. Once the restore command returns, it starts
   Gitea and confirms readiness, returning the replica to read-only browsing and
   cloning.

Chunk boundaries average 1 MiB, with 256 KiB minimum and 4 MiB maximum.
Content-defined boundaries allow later chunks to be reused after insertions.
An unfinished final session closes automatically when `FINAL_SESSION_TIMEOUT`
expires or the control service stops. Its timer starts after the final manifest
has been scanned and indexed. Progress heartbeats extend the idle timer during
chunk transfer, staging, activation, and installed-tree verification; before
primary release, the total outage is capped by `PRIMARY_OUTAGE_TIMEOUT`,
including primary stop, final scan, signing, manifest persistence, chunk index,
standby transfer, activation, and readiness verification. If that deadline
expires before release, the controller starts primary recovery and fails the
session. The timer resets when the primary write fence is released
after standby readiness, service stop, and installed-tree verification. During
post-release identity collection and finalization, progress heartbeats keep the
session open while the standby is working. That post-release window is capped at
the larger of `FINAL_SESSION_TIMEOUT` and
`SNAPSHOT_TIMEOUT`. `SNAPSHOT_TIMEOUT` bounds online and preflight manifest
scans and chunk-index construction. The final scan and transfer after stopping
the primary are bounded by `PRIMARY_OUTAGE_TIMEOUT`, which also includes write
fence acquisition, primary stop, signing, persistence, standby activation, and
readiness checks. It does not limit primary recovery retries. If starting the
primary fails, recovery retries until it succeeds or
the control service stops; the persisted outage checkpoint lets the next
startup continue recovery. SSH write commands remain blocked while that
checkpoint exists, including after the cross-process fence is released for
recovery retries.

Replication is a warm standby, not an independent backup. Deleted files are
removed from the standby on the next successful installation, and replicated
corruption can also reach it. Keep separate backups and regularly verify that
they can be restored.

## Control API

The replication control plane exposes one stable, internal resource API. Create a job with `POST /api/v1/replication/sync-jobs` and a JSON body of either `{"kind":"preflight","resume_job_id":"..."}` or `{"kind":"final","base_job_id":"..."}`. The response is `202 Accepted` while the job is running.

- `GET /api/v1/replication/sync-jobs` and `GET /api/v1/replication/sync-jobs/<id>` expose job status.
- `GET /api/v1/replication/sync-jobs/<id>/manifest` retrieves the signed job manifest.
- `GET /api/v1/replication/sync-jobs/<id>/chunks/<sha256>` transfers chunk content.
- `POST /api/v1/replication/sync-jobs/<id>/session/renew?renewal_id=<id>` extends an active final session, up to three renewals before primary release. Reuse the same ID for request retries.
- `POST /api/v1/replication/sync-jobs/<id>/session/heartbeat?renewal_id=<id>` reports progress before or after primary release. Its response includes the remaining idle lease as `lease_ns`, allowing the standby to schedule heartbeats against the primary's timeout. The lease is capped by the corresponding session maximum age. Reuse the same ID for request retries.
- `POST /api/v1/replication/sync-jobs/<id>/session/release` restarts the primary after standby readiness and service stop, then releases its write fence.
- `POST /api/v1/replication/sync-jobs/<id>/session/{complete,abort}` finishes a final session.

`/api/v1/replication/health` remains the health endpoint. These endpoints are internal to the
replication controller and require its bearer token.
Manifest responses use gzip when the client advertises it, reducing transfer size
for large trees. Chunk responses use gzip when it saves at least 5% of the chunk
body; the standby client decompresses before verifying the chunk hash.

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
FINAL_SESSION_TIMEOUT = 5m
PRIMARY_OUTAGE_TIMEOUT = 30m
CONTROL_WRITE_TIMEOUT = 10m
```

`PRIMARY_OUTAGE_TIMEOUT` is the hard upper bound on one primary stop, including
the final scan and the standby's fenced transfer and activation. Choose a value
that covers normal final-delta and readiness time. If it expires, the primary
is started for recovery and the current restore fails. `CONTROL_WRITE_TIMEOUT`
limits the duration of each control-plane response, including manifest
downloads; raise it if a large manifest cannot be delivered within the limit.

Before fetching chunks, the standby checks available bytes and inodes on both
the snapshot-cache filesystem and the staging filesystem. It budgets a full
target-tree copy for staging when reflinks are unavailable, plus missing chunk
cache data and a safety margin. This conservative preflight does not reserve
space and cannot rule out later `ENOSPC` from quotas, concurrent writers, or
filesystem errors.

`SNAPSHOT_RETENTION` controls small manifest history only. It no longer
multiplies primary data usage. `baseline.json` is a small signed copy of the
latest successful primary manifest and is kept independently of that history.
Pruning may also retain the active final transfer, its preflight base, and the
latest trusted `ready` manifest as a standby recovery fallback. Invalid
manifests have a separate retention limit, so the number of files in the
snapshot directory can exceed `SNAPSHOT_RETENTION`.

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

Those byte-read figures describe the primary's incremental preflight. If the
standby's `current.json` has no trusted ready baseline, each restore attempt
also reads and chunks its eligible local files to find content it can reuse,
including chunks found at a different path. Reused chunks are checked against
the target manifest while staging. This scan can read the whole existing
standby tree, and repeats until a restore installs a trusted ready baseline.

When a full verification advances the manifest checkpoint, the standby also
rehashes locally reused files before installation. Corrupt local files are
rebuilt from the authenticated source chunks.

On the standby use `MODE = replica`. Set `CONTROL_SOURCE_URL` to the primary
control endpoint, such as `https://primary.example/_replication`. If it is
omitted, set `SOURCE_URL` to the primary Gitea URL and the controller endpoint
is derived by appending `/_replication`. Remote control URLs must use HTTPS;
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
install -m 0644 contrib/service/systemd/gitea-replication.service \
  contrib/service/systemd/gitea-replication-switch.service \
  contrib/service/systemd/gitea-replication-restore.service \
  contrib/service/systemd/gitea-replication-restore.timer /etc/systemd/system/
install -m 0644 contrib/polkit/60-gitea-replication.rules /etc/polkit-1/rules.d/
systemctl daemon-reload
systemctl enable --now gitea-replication.service
# standby only:
systemctl enable --now gitea-replication-restore.timer
```

The control unit's `TimeoutStopSec` must exceed `[replicate] SERVICE_TIMEOUT`;
its `ExecStopPost` waits for the primary readiness check before clearing an
outage checkpoint.

The restore worker uses a non-blocking `Type=simple` unit: starting it does not
wait for a potentially long synchronization to finish. The timer schedules the
next run one hour after the worker exits, so a slow or retrying transfer never
overlaps with another restore run.

Keep ordinary `gitea.service` on its standard
`GITEA_WORK_DIR=/var/lib/gitea`. Adjust every systemd `ReadWritePaths` entry
if paths differ. The switch unit runs as root because renaming
`/var/lib/gitea` requires access to its root-owned parent, but its command can
exchange only the configured `APP_WORK_PATH` and fixed `.install-stage` below
`SNAPSHOT_DIR`. Its 35-second start deadline and 5-second stop deadline bound
the helper; the restore process waits at least 45 seconds before deciding that
an exchange failed, so it does not restart Gitea while a systemd job may still
be running. The supplied Polkit rule lets `git` start only this fixed helper
and manage `gitea.service`. Restrict `/_replication/` by source IP, TLS, and
the independent bearer token.

## Capacity and failure behavior

The primary needs space only for manifests. The standby needs its active data,
the current delta chunk cache, and staging space. Filesystem reflinks share
unchanged file extents initially, but files must be copied when reflinks are
unavailable. Changed files are reconstructed in staging and require space equal
to their target size until the atomic switch and old-tree cleanup complete.
Plan for a full additional copy of unchanged files when the filesystem does not
support reflinks. A failed activation retains one failed staging tree for
diagnosis; the next failed activation replaces it, and a successful restore
clears it. The automatic byte/inode preflight described above is a lower bound
for the planned work, not a filesystem reservation.

An interrupted preflight leaves Gitea online. Transient transport failures
retain the active final session, and the standby retries after 30 seconds,
resuming from the signed final manifest and verified chunk cache without a new
preflight scan. If the primary control service itself restarts, its in-memory
write fence is lost and the interrupted final session is deliberately rejected;
the next run performs a fresh preflight to preserve snapshot consistency. If
the session expires or is aborted before primary release, the primary restarts
automatically; after release, it remains running.

## Failover

1. Fence the failed primary to prevent split brain.
2. Disable `gitea-replication-restore.timer` on the standby.
3. Confirm the last restore service completed successfully.
4. Change standby `[replicate] MODE` to `primary` and restart
   `gitea-replication.service`.
5. Start `gitea.service` and switch DNS/VIP.
6. Enable the restore timer only after a new standby is provisioned.

Never promote both nodes. With the supplied timer, the next restore starts one
hour after the previous run exits, plus up to two minutes of randomized delay.
The data age at the next successful restore also includes that run's duration;
repeated failures extend RPO until a restore succeeds. RTO is dominated by the
final delta, atomic directory switch, and Gitea health check.
