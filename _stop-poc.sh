#!/bin/bash
set -e
echo "Stopping core platform..."
cd docker
docker compose down
cd ..
echo "All services stopped successfully"