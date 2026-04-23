#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

docker compose exec -T spark /opt/spark/bin/spark-submit \
  --driver-class-path /opt/spark-extra-jars/postgresql-42.7.4.jar \
  --jars /opt/spark-extra-jars/postgresql-42.7.4.jar \
  /opt/spark-apps/etl_to_star_postgres.py
