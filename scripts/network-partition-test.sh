#!/usr/bin/env bash
set -euo pipefail

ports=(8081 8082 8083)
declare -A services=([8081]=node1 [8082]=node2 [8083]=node3)
isolated_container=""
cluster_network=""
isolated_service=""
partition_active=false

heal_partition() {
  if [[ "$partition_active" == true ]]; then
    if docker network connect --alias "$isolated_service" "$cluster_network" "$isolated_container" 2>/dev/null; then
      partition_active=false
    else
      return 1
    fi
  fi
}
trap heal_partition EXIT

find_leader() {
  local excluded_port=${1:-}
  for _ in $(seq 1 300); do
    for port in "${ports[@]}"; do
      [[ "$port" == "$excluded_port" ]] && continue
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
    --header 'X-Client-ID: partition-test' \
    --header "X-Request-ID: ${request_id}" \
    --data "{\"value\":\"${value}\"}" >/dev/null
}

wait_for_local_value() {
  local port=$1 key=$2 value=$3
  for _ in $(seq 1 150); do
    response=$(curl --silent "http://localhost:${port}/kv/${key}?consistency=stale" || true)
    if [[ "$response" == *"\"value\":\"${value}\""* ]]; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

old_leader_port=$(find_leader) || {
  echo "No leader found before partition"
  exit 1
}
isolated_service=${services[$old_leader_port]}
isolated_container=$(docker compose ps --quiet "$isolated_service")
cluster_network=$(docker inspect --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}' "$isolated_container")

write_value "$old_leader_port" before-partition before-partition committed
for port in "${ports[@]}"; do
  wait_for_local_value "$port" before-partition committed || {
    echo "Baseline value did not reach node on port ${port}"
    exit 1
  }
done

partition_started_ms=$(date +%s%3N)
docker network disconnect "$cluster_network" "$isolated_container"
partition_active=true

# Let RPCs started before the disconnect finish before checking quorum loss.
sleep 0.2

if docker exec "$isolated_container" wget -qO- http://127.0.0.1:8080/kv/before-partition >/dev/null 2>&1; then
  echo "Isolated leader served a linearizable read without quorum"
  exit 1
fi

new_leader_port=$(find_leader "$old_leader_port") || {
  echo "Majority partition did not elect a replacement leader"
  exit 1
}
partition_election_ms=$(($(date +%s%3N) - partition_started_ms))
write_value "$new_leader_port" during-partition during-partition committed

heal_partition
healed_ms=$(date +%s%3N)

for port in "${ports[@]}"; do
  wait_for_local_value "$port" before-partition committed || {
    echo "Node on port ${port} lost baseline state after healing"
    exit 1
  }
  wait_for_local_value "$port" during-partition committed || {
    echo "Node on port ${port} did not converge after healing"
    exit 1
  }
done

for _ in $(seq 1 150); do
  leaders=0
  for port in "${ports[@]}"; do
    status=$(curl --silent --fail "http://localhost:${port}/status" 2>/dev/null || true)
    [[ "$status" == *'"state":"leader"'* ]] && leaders=$((leaders + 1))
  done
  [[ "$leaders" == 1 ]] && break
  sleep 0.1
done
if [[ "$leaders" != 1 ]]; then
  echo "Cluster did not converge to exactly one leader after healing"
  docker compose logs
  exit 1
fi

healing_ms=$(($(date +%s%3N) - healed_ms))
echo "PARTITION_RESULT isolated=${isolated_service} majority_leader_port=${new_leader_port} election_ms=${partition_election_ms} healing_ms=${healing_ms}"
