#!/bin/bash
set -e

echo "Starting Data POC Platform..."

# 1. Start core stack
echo "Starting PostgreSQL, Airflow, Superset, DBT..."
docker compose up -d

cd airbyte-docker
docker compose up -d

cd ..
echo "All services started"