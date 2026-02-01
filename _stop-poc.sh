#!/bin/bash
set -e
echo "stopping airbyte"
cd docker/airbyte-docker
docker compose down
cd ..
echo "Stopping core platform..."
docker compose down
cd ..
echo "All services stopped successfully"