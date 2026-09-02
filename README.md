# Fault-Tolerant Distributed Key-Value Store

An educational distributed key-value store implemented in Go. The first milestone implements a Raft-style replicated state machine with randomized leader elections, heartbeats, majority-quorum log replication, and an HTTP API.

> This is an active learning project, not a production database. Persistence, snapshots, network-partition testing, and full Raft safety verification are planned milestones.

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
  -d '{"value":"hello raft"}'
curl localhost:8082/kv/message
```

A write sent to a follower returns HTTP 307 and includes the current leader address in the JSON response.

## Current guarantees

- One vote per server per term while the process is running
- Log consistency checks using previous log index and term
- A write is applied only after replication to a majority
- A three-node cluster can continue committing with one unavailable follower
- Higher-term RPC responses force a leader or candidate to step down

State is currently in memory. Restarting all nodes loses data; durable storage is the next milestone.

## Test

```bash
go test -race ./...
```

## Roadmap

- [x] Leader election and heartbeats
- [x] Replicated `PUT` and local `GET`
- [x] Three-node Docker Compose cluster
- [ ] Durable write-ahead log and term/vote persistence
- [ ] Linearizable reads and request deduplication
- [ ] Snapshotting and log compaction
- [ ] Automated leader-failure and network-partition tests
- [ ] Benchmarks for election, throughput, and recovery time
