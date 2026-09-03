# Fault-Tolerant Distributed Key-Value Store

An educational distributed key-value store implemented in Go. It implements a Raft-style replicated state machine with randomized leader elections, heartbeats, majority-quorum log replication, durable node state, snapshots, and an HTTP API.

> This is an active learning project, not a production database. Full Raft safety verification and broader adversarial testing remain planned milestones.

## Run a three-node cluster

```bash
docker compose up --build
```

Find the leader:

```bash
curl -s localhost:8081/status
curl -s localhost:8082/status
curl -s localhost:8083/status
```

Write to the node whose status is `leader`, then read from a replica after the entry is committed:

```bash
curl -X PUT localhost:8081/kv/message \
  -H 'Content-Type: application/json' \
  -H 'X-Client-ID: example-client' \
  -H 'X-Request-ID: request-1' \
  -d '{"value":"hello raft"}'
curl localhost:8081/kv/message
```

A write sent to a follower returns HTTP 307 and includes the current leader address in the JSON response.

`GET /kv/{key}` is linearizable by default: the leader first ensures an entry is committed in its current term, then confirms contact with a majority before serving the value. A follower-local diagnostic read is available explicitly through `GET /kv/{key}?consistency=stale`.

Every write requires `X-Client-ID` and `X-Request-ID`. Retrying the same client/request pair with the same key and value returns success without appending another log entry. Reusing the pair for a different command returns HTTP 409.

## Current guarantees

- One vote per server per term while the process is running
- Log consistency checks using previous log index and term
- A write is applied only after replication to a majority
- A three-node cluster can continue committing with one unavailable follower
- Higher-term RPC responses force a leader or candidate to step down
- Default reads are served only by a leader that confirms a current-term quorum
- Committed client request IDs survive restarts and make write retries idempotent

Each Docker node stores its current term, vote, log, and commit index in a separate named volume. Updates use a write-sync-rename-directory-sync sequence so an acknowledged state transition is not left only in process memory. A restarted node rebuilds its key-value state machine from committed log entries.

For a local binary, set `DATA_DIR` to enable file-backed state. Leaving it unset uses in-memory storage, which is useful for tests.

## Test

```bash
go test -race ./...
```

The CI fault-injection test identifies the active leader, commits a value, stops that leader, measures replacement election time, commits through the surviving two-node quorum, restarts the old leader, and verifies that all three nodes converge. The measured `election_ms` value is written to the GitHub Actions job summary; resume claims should use observed results across repeated benchmark runs rather than the test's five-second safety limit.

The partition test keeps the old leader process alive but disconnects it from the cluster network. It verifies that the isolated leader cannot serve a linearizable read, the two-node majority elects a leader and commits, and the healed cluster converges to one leader with identical committed state. Election and healing durations are recorded in the Actions summary.

Committed logs are compacted into snapshots after 100 entries. A snapshot contains the key/value state and request-deduplication records at an absolute log index and term. When a follower falls behind the compacted prefix, the leader installs the snapshot and then streams the remaining log suffix. CI validates this by stopping a follower for 105 writes and requiring it to recover after restart.

## Roadmap

- [x] Leader election and heartbeats
- [x] Replicated `PUT` and local `GET`
- [x] Three-node Docker Compose cluster
- [x] Durable log, commit index, and term/vote persistence
- [x] Linearizable reads and request deduplication
- [x] Snapshotting, log compaction, and lagging-follower installation
- [x] Automated container-restart recovery test
- [x] Automated leader-failure recovery test
- [x] Automated network-partition recovery test
- [ ] Benchmarks for election, throughput, and recovery time
