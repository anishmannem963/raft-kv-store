#!/usr/bin/env bash
set -euo pipefail

ports=(8081 8082 8083)
declare -A services=([8081]=node1 [8082]=node2 [8083]=node3)

find_leader() {
  local excluded_port=${1:-}
  local attempts=${2:-100}
  for _ in $(seq 1 "$attempts"); do
    for port in "${ports[@]}"; do
      if [[ "$port" == "$excluded_port" ]]; then
        continue
      fi
      status=$(curl --silent --fail "http://localhost:${port}/status" 2>/dev/null || true)
      if [[ "$status" == *'"state":"leader"'* ]]; then
        echo "$port"
        return 0
      fi
    done
    sleep 0.05
  done
  return 1
}

write_value() {
  local port=$1 request_id=$2 key=$3 value=$4
  curl --silent --fail-with-body \
    --request PUT "http://localhost:${port}/kv/${key}" \
    --header 'Content-Type: application/json' \
    --header 'X-Client-ID: fault-test' \
    --header "X-Request-ID: ${request_id}" \
    --data "{\"value\":\"${value}\"}" >/dev/null
}

wait_for_local_value() {
  local port=$1 key=$2 value=$3
  for _ in $(seq 1 100); do
    response=$(curl --silent "http://localhost:${port}/kv/${key}?consistency=stale" || true)
    if [[ "$response" == *"\"value\":\"${value}\""* ]]; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

old_leader_port=$(find_leader "" 600) || {
  echo "No initial leader found"
  docker compose logs
  exit 1
}
old_leader_service=${services[$old_leader_port]}

write_value "$old_leader_port" before-failure before-failure committed

failure_started_ms=$(date +%s%3N)
docker compose stop "$old_leader_service" >/dev/null
new_leader_port=$(find_leader "$old_leader_port" 200) || {
  echo "No replacement leader elected after stopping ${old_leader_service}"
  docker compose logs
  exit 1
}
election_completed_ms=$(date +%s%3N)
election_ms=$((election_completed_ms - failure_started_ms))

if (( election_ms > 5000 )); then
  echo "Leader replacement took ${election_ms} ms, exceeding the 5000 ms test limit"
  exit 1
fi

write_value "$new_leader_port" after-failure after-failure committed
docker compose start "$old_leader_service" >/dev/null

for port in "${ports[@]}"; do
  if ! wait_for_local_value "$port" before-failure committed; then
    echo "Node on port ${port} lost the pre-failure committed value"
    docker compose logs
    exit 1
  fi
  if ! wait_for_local_value "$port" after-failure committed; then
    echo "Node on port ${port} did not catch up after leader recovery"
    docker compose logs
    exit 1
  fi
done

echo "FAILOVER_RESULT old_leader=${old_leader_service} new_leader_port=${new_leader_port} election_ms=${election_ms}"
