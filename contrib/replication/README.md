# Incremental SQLite disaster recovery over HTTP

Replication is supported on Linux only. It relies on systemd, Linux write
fencing, and a single data tree that the standby updates in place.

This design uses the Gitea binary with an independent
`gitea-replication.service`. The control plane remains reachable through
Nginx while `gitea.service` is stopped for the short final consistency phase.
The primary never writes a full data archive.

The standby is a static backup node: it must not run Gitea, because the update
rewrites `APP_WORK_PATH` in place. The restore worker refuses to run while
`gitea.service` is active, and replication does not pause Gitea's background
workers or serve a read-only replica UI, so never start `gitea.service` there.

## Data and filesystem layout

SQLite, repositories, LFS, attachments, avatars, packages, Actions logs and
artifacts must use local storage below the same `APP_WORK_PATH` on both
servers. Keep the normal Gitea layout, such as `/var/lib/gitea`; no `current`
subdirectory or symlink convention is required. Paths are resolved and storage
outside this root is rejected.
Filesystem paths and symlink targets included in a snapshot must be valid
UTF-8 because manifests serialize them as JSON strings.
Neither server may mount another filesystem below `APP_WORK_PATH`, so a single
snapshot scan sees one consistent tree.

`SNAPSHOT_DIR` contains signed JSON manifests on the primary, not copies of
Git, LFS, or attachment data. On the standby it also contains a content-addressed chunk cache while a
synchronization is in progress. Verified chunks survive an interrupted run for
resumption, and each chunk is released from the cache once it has been written
to every manifest location, so the cache and the data tree together stay near
one data tree even when almost everything changes. Before each preflight and
final chunk transfer, stale chunks outside the current target manifest are
pruned; the cache is empty after a successful restore. Set `SNAPSHOT_DIR` to an
absolute path so every Gitea and replication process uses the same checkpoint
and cache directory regardless of its working directory. On the standby,
`APP_WORK_PATH` must be a real directory without symlink components.
`SNAPSHOT_DIR` may live on another filesystem or mount: no directory exchange is
used.

Provision `app.ini`, system OpenSSH host keys, TLS certificates, Nginx and
systemd on both nodes, and the Polkit rule on the primary, where the
control service starts and stops `gitea.service`. Gitea-generated SSH
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
effective `JWT_SECRET` values must match; every manifest carries and verifies the
general token signing secret fingerprint.
For file-backed algorithms, configure `JWT_SIGNING_PRIVATE_KEY_FILE` at the
same path relative to `APP_WORK_PATH` on both nodes; the private key is included
in the replicated data. Only manifest format 6 is supported. Both nodes must run a build with the same
format; a manifest in any other format is rejected as unsupported and treated as
a stale checkpoint, so the next sync rebuilds its baseline and may need to
re-index local standby chunks. Format 6 fixes manifest digests and wire
serialization to JSON v1.

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
   requests, bounded by `CHUNK_CACHE_MAX_BYTES`; any remainder is deferred and
   fetched on demand while the update writes it. Once that bounded transfer
   completes, the standby releases the primary, which starts Gitea again and
   resumes writing; the outage therefore covers the stop, the final scan, and the
   bounded final delta transfer only.
4. The standby then updates `APP_WORK_PATH` in place: unchanged files keep their
   contents, changed files are patched chunk by chunk at their manifest offsets,
   new files and symlinks are created, obsolete entries are removed last, and
   directory modes and timestamps are restored. Files that cannot be patched
   safely (new files and hard-linked files) are rebuilt through a temporary file
   in the same directory. There is no staging tree and no directory exchange, so
   the data tree never needs a second full copy.
5. The standby verifies the updated tree against the final manifest, records
   local file identities, marks its signed manifest ready, and completes the
   final session. The restore worker does not start Gitea; the standby stays a
   static backup node until it is promoted.

Chunk boundaries average 1 MiB, with 256 KiB minimum and 4 MiB maximum.
Content-defined boundaries allow later chunks to be reused after insertions.
An unfinished final session closes automatically when `FINAL_SESSION_TIMEOUT`
expires or the control service stops. Its timer starts after the final manifest
has been scanned and indexed. Progress heartbeats extend the idle timer during
chunk transfer, the in-place update, and installed-tree verification.
`PRIMARY_OUTAGE_TIMEOUT` caps the primary outage: write fence acquisition,
primary stop, final scan, signing, manifest persistence, chunk-index
construction, and the final chunk transfer. If that deadline expires before
release, the controller starts primary recovery and fails the session. The
in-place update, installed-tree verification, and finalization run after release with
the primary online; progress heartbeats keep the session open while the standby
is working, and that post-release window is capped at the larger of
`FINAL_SESSION_TIMEOUT` and `SNAPSHOT_TIMEOUT`. `SNAPSHOT_TIMEOUT` bounds online
and preflight manifest scans and chunk-index construction. `PRIMARY_OUTAGE_TIMEOUT`
does not limit primary recovery retries. If starting the primary fails, recovery
retries until it succeeds or
the control service stops; the persisted outage checkpoint lets the next
startup continue recovery. SSH write commands remain blocked while that
checkpoint exists, including after the cross-process fence is released for
recovery retries. While write protection is active — replica mode, or a primary
whose outage checkpoint still exists — state-changing HTTP requests are rejected
as well. Background workers are not paused: the standby never runs Gitea, and a
recovering primary resumes normal work once its checkpoint clears.

Replication keeps a static standby, not an independent backup. Deleted files
are removed from the standby on the next successful update, and replicated
corruption can also reach it. Keep separate backups and regularly verify that
they can be restored.

## Standby update semantics

The standby keeps one data tree and never builds a second copy:

- Unchanged files are recognised through the identities recorded at the previous
  verification and are not rewritten.
- Changed files are patched chunk by chunk at their manifest offsets, so only
  transferred deltas touch the disk.
- New files, hard-linked files and type conflicts are rebuilt through a
  temporary file in the same directory and renamed into place.
- Every entry the target manifest does not list is removed, including files an
  interrupted earlier update left behind, so keep no node-local data below
  `APP_WORK_PATH`.
- The update is verified again: unchanged files through their recorded
  identities, new and changed content by hashing, and the whole tree whenever
  `FULL_SCAN_INTERVAL` is due.
- Only a verified run writes the signed `ready` manifest and `current.json`.
  There is no rollback: an interrupted update leaves the tree partially updated,
  and the next run repairs it against the then-current manifest. Do not promote
  a standby whose last run did not complete successfully.

The standby never takes the primary's cross-process write fence and never starts
or stops `gitea.service`; keep that service stopped and disabled on the standby.

## Control API

The replication control plane exposes one stable, internal resource API. Create a job with `POST /api/v1/replication/sync-jobs` and a JSON body of either `{"kind":"preflight","resume_job_id":"..."}` or `{"kind":"final","base_job_id":"..."}`. The response is `202 Accepted` while the job is running.

- `GET /api/v1/replication/sync-jobs` and `GET /api/v1/replication/sync-jobs/<id>` expose job status.
- `GET /api/v1/replication/sync-jobs/<id>/manifest` retrieves the signed job manifest.
- `GET /api/v1/replication/sync-jobs/<id>/chunks/<sha256>` transfers chunk content.
- `POST /api/v1/replication/sync-jobs/<id>/chunks` transfers up to 32 chunks in one bounded response, so small chunks do not cost one request each. The request body is `{"hashes":["<sha256>", …]}`; the response is `application/octet-stream` with one record per requested hash: a status byte and the request index, followed by a 4-byte big-endian length and the payload when the status is `0`. Status `1` means the chunk was not served (not indexed, read failure, or the response byte budget was reached) and the client should request that hash individually.
- `POST /api/v1/replication/sync-jobs/<id>/session/heartbeat?renewal_id=<id>` reports progress before or after primary release. Its response includes the remaining idle lease as `lease_ns`, allowing the standby to schedule heartbeats against the primary's timeout. The lease is capped by the corresponding session maximum age. Reuse the same ID for request retries.
- `POST /api/v1/replication/sync-jobs/<id>/session/release` restarts the primary as soon as the bounded final transfer completes, then releases its write fence. The in-place update continues online. Chunk requests keep being served until the session completes; each chunk is read from the frozen manifest location and verified against its hash, so a source that changed after release fails instead of returning different bytes.
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
CHUNK_CACHE_MAX_BYTES = 1073741824
FULL_SCAN_INTERVAL = 168h
GITEA_SERVICE_NAME = gitea.service
SERVICE_TIMEOUT = 2m
SNAPSHOT_TIMEOUT = 24h
FINAL_SESSION_TIMEOUT = 5m
PRIMARY_OUTAGE_TIMEOUT = 30m
CONTROL_WRITE_TIMEOUT = 10m
```

`PRIMARY_OUTAGE_TIMEOUT` is the hard upper bound on one primary stop, including
the final scan and the standby's fenced transfer. Choose a value that covers
normal final-delta time; the in-place update runs after release and does not
extend the outage. If it expires, the primary
is started for recovery and the current restore fails. `CONTROL_WRITE_TIMEOUT`
limits the duration of each control-plane response, including manifest
downloads; raise it if a large manifest cannot be delivered within the limit.

Before fetching chunks, the standby checks available bytes and inodes on the
snapshot-cache and data filesystems. It budgets the missing chunk-cache data and
the entries that do not exist on the standby yet, plus a safety margin; changed
files are patched in place and need no additional space. This conservative
preflight does not reserve space and cannot rule out later `ENOSPC` from quotas,
concurrent writers, or
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
After a successful verification it starts a new incremental baseline. The same
interval governs the standby: after an in-place update it verifies unchanged
files through the identities recorded at the previous verification and rehashes
only new or changed content, while a due full scan rehashes every file. Set it
to `0` to disable scheduled full verification. Incremental preflights still walk
all paths and perform metadata checks, so their cost is proportional to the
number of filesystem entries, while bytes read are proportional to changed
files. Repositories with millions of loose Git objects may therefore still
benefit from normal Git maintenance and repacking.

Those byte-read figures describe the primary's incremental preflight. If the
standby's `current.json` has no trusted ready baseline, each restore attempt
also reads and chunks its eligible local files to find content it can reuse,
including chunks found at a different path. Reused chunks are checked against
the target manifest while updating in place. This scan can read the whole existing
standby tree, and repeats until a restore installs a trusted ready baseline.

When a scheduled full verification is due, the standby ignores the recorded
identities and rehashes every file while updating in place; corrupt local files
are rebuilt from the authenticated source chunks.

On the standby use `MODE = replica`. Set `CONTROL_SOURCE_URL` to the primary
control endpoint, such as `https://primary.example/_replication`. If it is
omitted, set `SOURCE_URL` to the primary Gitea URL and the controller endpoint
is derived by appending `/_replication`. Remote control URLs must use HTTPS;
cleartext HTTP is accepted only for loopback.

If the standby must reach the primary through an explicit network proxy, set
`CONTROL_PROXY_URL` on the standby. This overrides environment proxy settings
for replication traffic only. Example: `CONTROL_PROXY_URL = http://proxy.example:3128`.

Disable `gitea.socket` on the primary; the control service refuses
finalization while socket activation is active. Do not run administrative CLI
jobs outside the fenced Gitea service during the final phase.

## systemd and Nginx

Install the control and restore units on both nodes, but enable the restore
timer only on the standby:

```sh
install -d -o git -g git -m 0700 /var/lib/gitea /var/lib/gitea-replication/snapshots
install -m 0644 contrib/service/systemd/gitea-replication.service \
  contrib/service/systemd/gitea-replication-restore.service \
  contrib/service/systemd/gitea-replication-restore.timer /etc/systemd/system/
# primary only: lets the control service start and stop gitea.service
install -m 0644 contrib/polkit/60-gitea-replication.rules /etc/polkit-1/rules.d/
# primary only: gives gitea.service the /run/gitea-replication write-fence directory
install -D -m 0644 contrib/service/systemd/gitea.service.d/10-replication-fence.conf \
  /etc/systemd/system/gitea.service.d/10-replication-fence.conf
systemctl daemon-reload
systemctl enable --now gitea-replication.service
# standby only:
systemctl enable --now gitea-replication-restore.timer
```

The control unit's `TimeoutStopSec` must exceed `[replicate] SERVICE_TIMEOUT`;
its `ExecStopPost` waits for the primary readiness check before clearing an
outage checkpoint.

`APP_WORK_PATH` and `SNAPSHOT_DIR` may live on different filesystems, including
bind mounts from separate disks. Both units order themselves after those mounts
with `RequiresMountsFor=`, so define them as systemd mount units (or `/etc/fstab`
entries) rather than mounting them from an ad-hoc script: a mount that appears
after the worker starts would shadow the tree, and the update would write to the
underlying filesystem instead. Do not mount anything below `APP_WORK_PATH`: the
scan requires one mount tree and refuses nested mount points.

The restore worker uses a non-blocking `Type=simple` unit: starting it does not
wait for a potentially long synchronization to finish. The timer schedules the
next run one hour after the worker exits, so a slow transfer never overlaps with
another restore run; a failed run is retried after thirty minutes, because
every attempt stops and restarts the primary for the fenced final phase.

Keep ordinary `gitea.service` on its standard
`GITEA_WORK_DIR=/var/lib/gitea`, and keep it stopped on the standby: the restore
worker refuses to run while it is active and never starts it. Adjust every
systemd `ReadWritePaths` entry if paths differ. Restrict `/_replication/` by
source IP, TLS, and the independent bearer token.

## Migrating from the staging-tree layout

Earlier builds kept a retained shadow tree at `SNAPSHOT_DIR/.install-stage`,
recorded `.install-stage.checkpoint` and `.stage-current.json`, activated a
snapshot by exchanging directories through a root-owned helper, and could leave
full-data `*.tar.gz` archives and `SNAPSHOT_DIR/recovery-backups` rollback copies
behind. Current builds neither read nor clean up that state, so remove it once
after upgrading to release the second full copy of the data:

```sh
systemctl disable --now gitea-replication-switch.service
rm -f /etc/systemd/system/gitea-replication-switch.service
rm -rf /var/lib/gitea-replication/snapshots/.install-stage
rm -f /var/lib/gitea-replication/snapshots/*.tar.gz \
  /var/lib/gitea-replication/snapshots/*.tar.gz.tmp \
  /var/lib/gitea-replication/snapshots/.install-stage.checkpoint \
  /var/lib/gitea-replication/snapshots/.stage-current.json
rm -f /var/lib/gitea-replication/snapshots/.replication-exchange-probe-* \
  /var/lib/gitea-replication/snapshots/..install-stage.checkpoint.tmp-*
rm -rf /var/lib/gitea-replication/recovery-backups
systemctl daemon-reload
```

Adjust the paths if `SNAPSHOT_DIR` differs. Keep `baseline.json`, `current.json`
and the signed manifests: they are part of the current protocol.

## Capacity and failure behavior

The primary needs space only for manifests. The standby needs one data tree
plus the delta chunk cache. The update is applied in place: changed files are
patched chunk by chunk and each cached chunk is released once it has been
written, so a fresh install or an almost complete change stays near one data
tree instead of two. `CHUNK_CACHE_MAX_BYTES` (default 1 GiB) bounds how much
transferred chunk data the standby keeps from the preflight and final passes;
chunks beyond the limit are deferred and fetched on demand while the update
writes them, so the cache never has to hold a whole tree. Set it to 0 to disable
the limit. Place `SNAPSHOT_DIR` on a separate
filesystem if the delta itself is large. A rebuild through a temporary file is
limited to new or hard-linked files and needs room for those files only. The automatic byte/inode
preflight described above is a lower bound for the planned work, not a
filesystem reservation.

Neither the supplied control unit nor the restore unit sets a memory limit, and
the 256 MiB manifest limit bounds the encoded document, not the decoded entries
and chunk indexes held in memory. Set `MemoryMax` and `TasksMax` on both units
according to the expected tree size, and watch the control service's resident
memory after startup, when every retained preflight manifest is indexed.

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
repeated failures extend RPO until a restore succeeds. An interrupted in-place
update leaves the tree partially updated; the next run repairs it, and promotion
requires a successful verified restore. RTO is dominated by the final delta and
the in-place update.
