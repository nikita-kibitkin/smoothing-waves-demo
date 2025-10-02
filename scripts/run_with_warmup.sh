#!/usr/bin/env bash
set -euo pipefail


kafka_recreate_topic(){
  docker compose exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic topic \
    --partitions 1 --replication-factor 1
  docker compose exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic topic
  docker compose exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic topic \
    --partitions 1 --replication-factor 1
}

pg_truncate_vacuum(){
  PGPASSWORD='postgres' psql -h localhost -p 5433 -U postgres -d postgres -c "TRUNCATE public.events, public.events_audit RESTART IDENTITY;"
  PGPASSWORD='postgres' psql -h localhost -p 5433 -U postgres -d postgres -c "VACUUM (ANALYZE);"
}

run_phase() {
  local label="$1" minutes="$2" backpressure="$3" credits="$4" db="$5" rate="$6"
  export DURATION_MINUTES="$minutes"
  export EXPERIMENT_ID="$label"
  export BACKPRESSURE_ENABLED="$backpressure"
  export BACKPRESSURE_CREDITS="$credits"
  export DB_ENABLED="$db"
  export HIGH_RATE="$rate"

  echo "[$label] starting for ${minutes} min..."
  docker compose up -d --build app

  local cid="$(docker compose ps -q app)"
  docker wait "$cid" >/dev/null

  echo "[$label] finished."
  docker compose rm -f app >/dev/null

  kafka_recreate_topic
  pg_truncate_vacuum
}

docker compose up -d  zookeeper kafka prometheus grafana pg pg_standby #loki promtail

#run_phase pg_bp_no 1 false 0 true 30

#run_phase pg_bp_no 5 false 0 true 30
#run_phase pg_bp_500 2 true 500 true 30
#run_phase pg_bp_1000 5 true 1000 true 30
#run_phase pg_bp_1500 2 true 1500 true 30
#
#run_phase pg_bp_no 5 false 0 true 40
#run_phase pg_bp_500 2 true 500 true 40
#run_phase pg_bp_1000 5 true 1000 true 40
#run_phase pg_bp_1500 2 true 1500 true 40

run_phase pg_bp_1000 10 true 1000 true 50
run_phase pg_bp_no 10 false 0 true 50
run_phase pg_bp_500 10 true 500 true 50
run_phase pg_bp_1500 3 true 1500 true 50
run_phase pg_bp_1000 3 true 1000 true 50


#run_phase pg_bp_no 3 false 0 true 55
##run_phase pg_bp_500 3 true 500 true 55
##run_phase pg_bp_1000 3 true 1000 true 55
#run_phase pg_bp_1500 3 true 1500 true 55
#run_phase pg_bp_2000 3 true 2000 true 55

#run_phase pg_bp_no 3 false 0 true 65
#run_phase pg_bp_500 3 true 500 true 65
#run_phase pg_bp_1000 3 true 1000 true 65
#run_phase pg_bp_1500 3 true 1500 true 65

#run_phase pg_bp_no 10 false 0 true 70
#run_phase pg_bp_500 10 true 500 true 70
#run_phase pg_bp_1000 10 true 1000 true 70
#run_phase pg_bp_1500 10 true 1500 true 70
#
#run_phase pg_bp_no 5 false 0 true 80
#run_phase pg_bp_500 2 true 500 true 80
##run_phase pg_bp_1000 5 true 1000 true 80
#run_phase pg_bp_1500 2 true 1500 true 80
#
#run_phase pg_bp_no 5 false 0 true 100
#run_phase pg_bp_500 2 true 500 true 100
#run_phase pg_bp_1000 5 true 1000 true 100
#run_phase pg_bp_1500 2 true 1500 true 100

#run_phase pg_bp_750 2 true 750 true
#run_phase pg_bp_750 2 true 750 true
#run_phase pg_bp_1000 2 true 1000 true
#run_phase pg_bp_no 2 false 0 true


#run_phase pg_bp_no 2 false 0 true
#run_phase pg_bp_no 2 false 0 true
#run_phase pg_bp_no 2 false 0 true
#run_phase pg_bp_no 2 false 0 true
#run_phase pg_bp_no 2 false 0 true
#
#run_phase pg_bp_no 2 false 0 true
#run_phase pg_bp_no 2 false 0 true
#run_phase pg_bp_no 2 false 0 true
#run_phase pg_bp_no 2 false 0 true
#run_phase pg_bp_no 2 false 0 true




