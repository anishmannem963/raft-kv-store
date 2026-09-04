#!/usr/bin/env bash
set -euo pipefail

requests=${1:-10000}
concurrency=${2:-16}
output=${3:-benchmark-results.json}
timeout=${4:-10m}

go run ./cmd/benchmark \
  -requests "$requests" \
  -concurrency "$concurrency" \
  -timeout "$timeout" \
  >"$output"

cat "$output"
