# Disaster-recovery replication

Gitea's incremental replication feature maintains a cold standby for SQLite
deployments. It is supported on Linux and requires local storage under one
`APP_WORK_PATH` mount tree, matching Gitea release versions and paths, and
systemd. The standby does not run Gitea: the restore worker refuses to update the
data tree while `gitea.service` is active, and applies each snapshot in place
without a second copy of the tree. It does not provide quorum, automatic leader
election, or automatic fencing of a failed primary.

Use the [replication deployment and operations guide](../contrib/replication/README.md)
for the full configuration reference, systemd/Nginx setup, capacity planning,
failure handling, and failover steps. A commented `[replicate]` example is in
[`custom/conf/app.example.ini`](../custom/conf/app.example.ini).

The source and standby must share the same `CONTROL_TOKEN` and Gitea instance
secrets. Keep the control listener on loopback and expose it only through TLS
and an IP-restricted reverse proxy. The standby restore timer should be enabled
only on the replica.

`gitea replicate status` reports the control service's recent synchronization
jobs. The health endpoint indicates that the control service responds; it does
not prove that the latest snapshot was applied or that the standby is current.
For data freshness, inspect the latest successful `ready` job and the standby's
`current.json` creation time, then check the restore timer and service logs.
Replication mirrors deletions and can copy silent corruption, so keep independent
backups and test restoring them. An interrupted in-place update leaves the tree
partially updated; the next run repairs it, and only a run that completed
verification writes the `ready` manifest.

To promote a standby, first isolate or fence the old primary, stop
`gitea-replication-restore.timer`, confirm the most recent restore succeeded,
change `MODE` to `primary`, restart the replication control service, and start
Gitea. Never promote both nodes. The operator is responsible for proving that
the old primary cannot accept writes before directing traffic to the standby.
