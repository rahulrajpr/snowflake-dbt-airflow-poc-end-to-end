
#!/bin/bash
set -e

echo "Starting Data POC Platform..."
cd docker

# 1. Start core stack
echo "Starting PostgreSQL, Airflow, Superset, DBT, Airbyte..."
docker compose up -d

# 2. Back to the Root Directory
cd ..
echo "All services started"