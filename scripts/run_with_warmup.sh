#!/usr/bin/env bash
set -euo pipefail

run_phase() {
  local label="$1" minutes="$2" backpressure="$3" credits="$4" db="$5"
  export DURATION_MINUTES="$minutes"
  export EXPERIMENT_ID="$label"
  export BACKPRESSURE_ENABLED="$backpressure"
  export BACKPRESSURE_CREDITS="$credits"
  export DB_ENABLED="$db"

  echo "[$label] starting for ${minutes} min..."
  docker compose up -d --build app

  local cid="$(docker compose ps -q app)"
  docker wait "$cid" >/dev/null

  echo "[$label] finished."
  docker compose rm -f app >/dev/null
}

docker compose up -d zookeeper kafka prometheus grafana pg pg_standby

run_phase pg_bp_700 5 true 1000 true
run_phase pg_bp_no 5 false 0 true
run_phase pg_bp_1500 2 true 1500 true


#run_phase warmup 2 true 1000 false
#run_phase main 2 false 0 false


