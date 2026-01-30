#!/bin/bash
set -e
echo "stopping airbyte"
cd airbyte-docker
docker compose down
cd ..
echo "Stopping core platform..."
docker compose down
echo "All services stopped successfully"