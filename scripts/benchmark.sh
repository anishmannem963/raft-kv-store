#!/usr/bin/env bash
set -euo pipefail

requests=${1:-10000}
concurrency=${2:-16}
output=${3:-benchmark-results.json}

go run ./cmd/benchmark \
  -requests "$requests" \
  -concurrency "$concurrency" \
  >"$output"

cat "$output"
