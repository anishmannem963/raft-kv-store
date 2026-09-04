# Changelog

## v1.0.0 - Unreleased

### Added

- Three-node Raft-style replicated key/value service with HTTP `PUT` and `GET` operations
- Randomized leader election, heartbeats, majority replication, and higher-term step-down
- Durable terms, votes, logs, commit indexes, snapshots, and restart reconstruction
- Leader-only linearizable reads and explicit stale diagnostic reads
- Idempotent write retries with persistent client/request deduplication
- Automatic log compaction and snapshot installation for lagging followers
- Race-enabled unit tests and live Docker recovery tests
- Reproducible multi-concurrency benchmark suite with JSON artifacts
- Fifty-scenario process, partition, snapshot, and restart fault matrix

### Fixed

- Prevented leaders from starting elections when follower election timers expire
- Serialized concurrent replication rounds that share follower progress
- Disabled persistent peer HTTP connections so live Docker partitions are observed promptly

### Notes

This release marks the portfolio-ready educational baseline. Fixed membership and other limitations are listed in `docs/architecture.md`.
