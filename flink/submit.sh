#!/usr/bin/env bash
# Soumet chaque job Flink SQL (un fichier par job dans /opt/sql) au cluster,
# sauf si un job du même nom tourne déjà.
set -euo pipefail

JM="${JOBMANAGER_HOST:-jobmanager}:8081"

echo "Attente du JobManager ($JM)..."
until curl -sf "http://$JM/overview" | grep -Eq '"taskmanagers":[1-9]'; do sleep 3; done

running=$(/opt/flink/bin/flink list -r -m "$JM" 2>/dev/null || true)

for sql in /opt/sql/*.sql; do
  job="rennes-$(basename "$sql" .sql)"
  if grep -q ": $job (" <<<"$running"; then
    echo "Le job $job tourne déjà."
    continue
  fi
  echo "Soumission de $job"
  envsubst '${KAFKA_BOOTSTRAP_SERVERS} ${POSTGRES_JDBC_URL} ${POSTGRES_USER} ${POSTGRES_PASSWORD}' < "$sql" > "/tmp/$job.sql"
  /opt/flink/bin/sql-client.sh -f "/tmp/$job.sql"
done
