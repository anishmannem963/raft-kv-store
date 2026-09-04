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

The release table is populated from the post-Milestone-9 GitHub Actions artifact so it reflects the corrected leader election behavior.

<!-- BENCHMARK_RESULTS_START -->
Results pending completion of the release evidence workflow.
<!-- BENCHMARK_RESULTS_END -->

## Interpretation limits

GitHub-hosted runners are shared, virtualized CI machines. Their load and exact hardware can vary, so these figures demonstrate repeatable behavior under the documented environment; they are not hardware-independent capacity claims. The harness measures HTTP, JSON, replication, and synchronous persistence together. It does not isolate network-only or storage-only latency.
