#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

docker compose exec -T spark /opt/spark/bin/spark-submit \
  --driver-class-path /opt/spark-extra-jars/postgresql-42.7.4.jar:/opt/spark-extra-jars/clickhouse-jdbc-0.6.0-all.jar \
  --jars /opt/spark-extra-jars/postgresql-42.7.4.jar,/opt/spark-extra-jars/clickhouse-jdbc-0.6.0-all.jar \
  /opt/spark-apps/reports.py \
  --ch-user bd_ch_user \
  --ch-password bd_ch_pass \
  --ch-database bd_lab2
