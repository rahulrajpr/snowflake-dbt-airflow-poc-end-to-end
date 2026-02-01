#!/bin/bash
set -e

echo "Starting Data POC Platform..."
cd docker

# 1. Start core stack
echo "Starting PostgreSQL, Airflow, Superset, DBT..."
docker compose up -d

cd airbyte-docker
# 2. Start Airbyte Stack

echo "Starting Airbyte"
docker compose up -d

# 3. Back to the Root Directory
cd ..
cd ..
echo "All services started"