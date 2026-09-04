# Fault-injection methodology and results

## Matrix design

The default matrix executes exactly 50 independent scenarios:

| Category | Runs | Required behavior |
|---|---:|---|
| Leader process failure | 15 | Replace the stopped leader, commit through the majority, restart it, and converge |
| Live network partition | 15 | Deny a quorum-confirmed read on the isolated leader, elect and commit on the majority side, heal, and converge |
| Snapshot recovery | 10 | Move a follower behind the compaction boundary, install a snapshot, replicate the suffix, and converge |
| Durable container restart | 10 | Restart nodes with persistent volumes and retain committed data |

Every scenario starts with fresh containers and volumes. The harness preserves a scenario log and one NDJSON record containing pass/fail state and timing. `cmd/fault-summary` aggregates success rate and per-category mean/max duration, election, and healing times.

## Reproduce locally

```bash
./scripts/fault-matrix.sh 15 15 10 10 fault-matrix-results
```

The `Fault Matrix` GitHub Actions workflow runs the same defaults and uploads the entire directory even on failure. Pull-request CI executes one scenario from every category as a bounded regression gate.

## Latest verified results

<!-- FAULT_RESULTS_START -->
Post-Milestone-9 run [#33897246922](https://github.com/anishmannem963/raft-kv-store/actions/runs/33897246922), commit `16932f6`, passed **50/50 scenarios (100%)** with no failures.

| Category | Passed | Mean scenario | Mean election | Max election | Mean healing | Max healing |
|---|---:|---:|---:|---:|---:|---:|
| Leader process failure | 15/15 | 1,175 ms | 471.9 ms | 840 ms | — | — |
| Live network partition | 15/15 | 1,233.2 ms | 433.5 ms | 496 ms | 120.7 ms | 185 ms |
| Snapshot recovery | 10/10 | 3,399.4 ms | — | — | — | — |
| Durable container restart | 10/10 | 2,294.9 ms | — | — | — | — |

The workflow artifact contains 50 individual logs, the NDJSON records, and the aggregate `summary.json`.
<!-- FAULT_RESULTS_END -->

## Interpretation limits

This is systematic failure injection, not exhaustive distributed-systems verification. The matrix covers the named process, Docker-network, lag, and restart conditions on a three-node topology. It does not prove correctness under every message reordering, Byzantine behavior, disk fault, clock anomaly, or arbitrary operation history.
