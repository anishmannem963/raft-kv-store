#!/usr/bin/env bash
set -uo pipefail

leader_scenarios=${1:-15}
partition_scenarios=${2:-15}
snapshot_scenarios=${3:-10}
restart_scenarios=${4:-10}
output_dir=${5:-fault-matrix-results}

for count in "$leader_scenarios" "$partition_scenarios" "$snapshot_scenarios" "$restart_scenarios"; do
  if ! [[ "$count" =~ ^[0-9]+$ ]]; then
    echo "scenario counts must be non-negative integers" >&2
    exit 1
  fi
done
if (( leader_scenarios + partition_scenarios + snapshot_scenarios + restart_scenarios == 0 )); then
  echo "at least one scenario is required" >&2
  exit 1
fi

mkdir -p "$output_dir"
results="$output_dir/scenarios.ndjson"
summary="$output_dir/summary.json"
: >"$results"
rm -f "$summary"
trap 'docker compose down --volumes >/dev/null 2>&1 || true' EXIT
docker compose build || exit 1
failures=0

run_scenario() {
  local kind=$1 index=$2 script=$3
  local name="${kind}-$(printf '%02d' "$index")"
  local log="$output_dir/${name}.log"
  docker compose down --volumes >/dev/null 2>&1 || true
  if ! docker compose up --detach >"$log" 2>&1; then
    printf '{"scenario":"%s","type":"%s","passed":false,"duration_ms":0,"election_ms":0,"healing_ms":0}\n' "$name" "$kind" >>"$results"
    failures=$((failures + 1))
    return
  fi
  local started_ms completed_ms duration_ms passed=true election_ms=0 healing_ms=0
  started_ms=$(date +%s%3N)
  if ! "$script" >>"$log" 2>&1; then
    passed=false
    failures=$((failures + 1))
  fi
  completed_ms=$(date +%s%3N)
  duration_ms=$((completed_ms - started_ms))
  if [[ "$kind" == "leader_failure" || "$kind" == "network_partition" ]]; then
    election_ms=$(sed -n 's/.*election_ms=\([0-9][0-9]*\).*/\1/p' "$log" | tail -1)
    election_ms=${election_ms:-0}
  fi
  if [[ "$kind" == "network_partition" ]]; then
    healing_ms=$(sed -n 's/.*healing_ms=\([0-9][0-9]*\).*/\1/p' "$log" | tail -1)
    healing_ms=${healing_ms:-0}
  fi
  printf '{"scenario":"%s","type":"%s","passed":%s,"duration_ms":%d,"election_ms":%d,"healing_ms":%d}\n' \
    "$name" "$kind" "$passed" "$duration_ms" "$election_ms" "$healing_ms" >>"$results"
  echo "FAULT_SCENARIO name=${name} passed=${passed} duration_ms=${duration_ms}"
}

for index in $(seq 1 "$leader_scenarios"); do run_scenario leader_failure "$index" ./scripts/leader-failure-test.sh; done
for index in $(seq 1 "$partition_scenarios"); do run_scenario network_partition "$index" ./scripts/network-partition-test.sh; done
for index in $(seq 1 "$snapshot_scenarios"); do run_scenario snapshot_recovery "$index" ./scripts/snapshot-test.sh; done
for index in $(seq 1 "$restart_scenarios"); do run_scenario container_restart "$index" ./scripts/smoke-test.sh; done

go run ./cmd/fault-summary "$results" >"$summary" || exit 1
cat "$summary"
if (( failures > 0 )); then exit 1; fi
