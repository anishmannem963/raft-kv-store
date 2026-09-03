#!/usr/bin/env bash
set -euo pipefail

ports=(8081 8082 8083)
declare -A services=([8081]=node1 [8082]=node2 [8083]=node3)

leader_port=""
for _ in $(seq 1 300); do
  for port in "${ports[@]}"; do
    status=$(curl --silent --fail "http://localhost:${port}/status" 2>/dev/null || true)
    if [[ "$status" == *'"state":"leader"'* ]]; then leader_port=$port; break 2; fi
  done
  sleep 0.05
done
if [[ -z "$leader_port" ]]; then echo "No leader found"; exit 1; fi

lagging_port=""
for port in "${ports[@]}"; do
  if [[ "$port" != "$leader_port" ]]; then lagging_port=$port; break; fi
done
lagging_service=${services[$lagging_port]}
docker compose stop "$lagging_service" >/dev/null

for index in $(seq 1 105); do
  curl --silent --fail-with-body \
    --request PUT "http://localhost:${leader_port}/kv/snapshot-${index}" \
    --header 'Content-Type: application/json' \
    --header 'X-Client-ID: snapshot-test' \
    --header "X-Request-ID: write-${index}" \
    --data "{\"value\":\"value-${index}\"}" >/dev/null
done

leader_status=$(curl --silent --fail "http://localhost:${leader_port}/status")
if [[ "$leader_status" == *'"snapshot_index":-1'* ]]; then
  echo "Leader did not compact after 105 writes: ${leader_status}"
  exit 1
fi

docker compose start "$lagging_service" >/dev/null
recovered=false
for _ in $(seq 1 300); do
  response=$(curl --silent "http://localhost:${lagging_port}/kv/snapshot-105?consistency=stale" || true)
  if [[ "$response" == *'"value":"value-105"'* ]]; then recovered=true; break; fi
  sleep 0.1
done
if [[ "$recovered" != true ]]; then
  echo "Lagging follower did not recover through snapshot installation"
  docker compose logs "$lagging_service"
  exit 1
fi

follower_status=$(curl --silent --fail "http://localhost:${lagging_port}/status")
if [[ "$follower_status" == *'"snapshot_index":-1'* ]]; then
  echo "Follower caught up without recording an installed snapshot: ${follower_status}"
  exit 1
fi

echo "SNAPSHOT_RESULT lagging=${lagging_service} leader_status=${leader_status} follower_status=${follower_status}"
