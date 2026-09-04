# Benchmark methodology and results

## What is measured

The Go load generator sends uniquely identified `PUT` requests, rediscovers the leader when necessary, and records end-to-end client latency. A run reports successful and failed writes, operations per second, and min/p50/p95/p99/max latency.

After the workload, the harness waits for every node to report the same commit index, key count, and deterministic state hash. A run is not considered converged merely because all HTTP requests succeeded.

## Reproduce locally

Requirements: Go 1.23+, Docker, Docker Compose, Bash, and `curl`.

```bash
./scripts/benchmark.sh 10000 16 benchmark-results.json 10m
./scripts/benchmark-suite.sh 10000 5 "1 8 16" benchmark-suite-results 10m
```

The suite performs five repetitions at concurrency 1, 8, and 16. Every repetition starts with fresh containers and volumes, for 15 independent runs and 150,000 attempted writes. Raw run JSON and `summary.json` remain together in the output directory.

The same full suite is available through the `Benchmark` GitHub Actions workflow. Pull-request CI uses smaller workloads as correctness gates, not as headline performance results.

## Latest verified results

Post-release-hardening run [#33902571389](https://github.com/anishmannem963/raft-kv-store/actions/runs/33902571389), commit `e5ae87a`, completed 15 independent runs with **150,000/150,000 successful writes**, zero failures, and full three-replica convergence in every run.

<!-- BENCHMARK_RESULTS_START -->
| Concurrency | Runs | Successful writes | Mean throughput | Mean p50 | Mean p95 | Mean p99 | Max p99 | Converged |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 5 | 50,000/50,000 | 21.52 ops/s | 45.97 ms | 86.20 ms | 106.59 ms | 111.22 ms | Yes |
| 8 | 5 | 50,000/50,000 | 24.99 ops/s | 325.18 ms | 576.37 ms | 609.42 ms | 615.61 ms | Yes |
| 16 | 5 | 50,000/50,000 | 25.50 ops/s | 625.29 ms | 1,134.82 ms | 1,206.84 ms | 1,219.25 ms | Yes |

Throughput plateaus near 25 operations per second because write replication is deliberately single-flight and each command follows the synchronous durable quorum path. The result is a correctness baseline and also identifies batching or pipelined replication as the clearest future performance improvement.
<!-- BENCHMARK_RESULTS_END -->

## Interpretation limits

GitHub-hosted runners are shared, virtualized CI machines. Their load and exact hardware can vary, so these figures demonstrate repeatable behavior under the documented environment; they are not hardware-independent capacity claims. The harness measures HTTP, JSON, replication, and synchronous persistence together. It does not isolate network-only or storage-only latency.
