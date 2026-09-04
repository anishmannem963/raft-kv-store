#!/usr/bin/env bash
set -euo pipefail

requests=${1:-10000}
repetitions=${2:-5}
concurrency_levels=${3:-"1 8 16"}
output_dir=${4:-benchmark-suite-results}
timeout=${5:-10m}

if ! [[ "$requests" =~ ^[1-9][0-9]*$ && "$repetitions" =~ ^[1-9][0-9]*$ ]]; then
  echo "requests and repetitions must be positive integers" >&2
  exit 1
fi

mkdir -p "$output_dir"
rm -f "$output_dir"/run-*.json "$output_dir"/summary.json
trap 'docker compose down --volumes >/dev/null 2>&1 || true' EXIT
docker compose build

for concurrency in $concurrency_levels; do
  if ! [[ "$concurrency" =~ ^[1-9][0-9]*$ ]]; then
    echo "concurrency levels must be positive integers" >&2
    exit 1
  fi
  for repetition in $(seq 1 "$repetitions"); do
    docker compose down --volumes >/dev/null 2>&1 || true
    docker compose up --detach
    result="$output_dir/run-c${concurrency}-r${repetition}.json"
    ./scripts/benchmark.sh "$requests" "$concurrency" "$result" "$timeout"
  done
done

go run ./cmd/benchmark-summary "$output_dir"/run-*.json >"$output_dir/summary.json"
cat "$output_dir/summary.json"
