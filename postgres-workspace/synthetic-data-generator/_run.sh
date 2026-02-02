#!/bin/bash

# Install Python requirements
echo "Installing requirements..."
pip install -r requirements.txt

# Run data generation script
echo "Running data generation..."
python generate-insert-into-pg.py

echo "Done!"