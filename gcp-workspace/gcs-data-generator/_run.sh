#!/bin/bash

# GCS Data Generation and Upload Script

echo "Installing Python dependencies..."
pip install -r requirements.txt

echo ""
echo "Running GCS data generation..."
python generate-and-put-gcs-files.py

echo ""
echo "Done!"