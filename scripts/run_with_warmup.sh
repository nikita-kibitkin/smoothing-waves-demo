#!/usr/bin/env bash
set -euo pipefail

run_phase() {
  local label="$1" minutes="$2" lambda="$3"
  export DURATION_MINUTES="$minutes"
  export LAMBDA="$lambda"
  export EXPERIMENT_ID="$label"

  echo "[$label] starting for ${minutes} min..."
  docker compose up -d --build app

  local cid="$(docker compose ps -q app)"
  docker wait "$cid" >/dev/null

  echo "[$label] finished."
  docker compose rm -f app >/dev/null
}

docker compose up -d zookeeper kafka prometheus grafana

#warmup
run_phase warmup 5 40
#main
run_phase main 20 40
