#!/usr/bin/env bash
set -euo pipefail

ports=(8081 8082 8083)
leader_port=""

for _ in $(seq 1 30); do
  for port in "${ports[@]}"; do
    status=$(curl --silent --fail "http://localhost:${port}/status" 2>/dev/null || true)
    if [[ "$status" == *'"state":"leader"'* ]]; then
      leader_port=$port
      break 2
    fi
  done
  sleep 1
done

if [[ -z "$leader_port" ]]; then
  echo "No leader elected within 30 seconds"
  docker compose logs
  exit 1
fi

curl --silent --fail-with-body \
  --request PUT "http://localhost:${leader_port}/kv/ci-key" \
  --header 'Content-Type: application/json' \
  --data '{"value":"replicated"}'

for port in "${ports[@]}"; do
  replicated=false
  for _ in $(seq 1 10); do
    response=$(curl --silent "http://localhost:${port}/kv/ci-key" || true)
    if [[ "$response" == *'"value":"replicated"'* ]]; then
      replicated=true
      break
    fi
    sleep 1
  done
  if [[ "$replicated" != true ]]; then
    echo "Node on port ${port} did not apply the committed value"
    docker compose logs
    exit 1
  fi
done

docker compose restart node3 >/dev/null
recovered=false
for _ in $(seq 1 15); do
  response=$(curl --silent "http://localhost:8083/kv/ci-key" || true)
  if [[ "$response" == *'"value":"replicated"'* ]]; then
    recovered=true
    break
  fi
  sleep 1
done
if [[ "$recovered" != true ]]; then
  echo "Node 3 did not recover its committed value after restart"
  docker compose logs node3
  exit 1
fi

echo "Leader elected on port ${leader_port}; value replicated and recovered after restart"
