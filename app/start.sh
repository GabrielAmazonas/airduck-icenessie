#!/bin/bash
set -e

echo "Starting FastAPI Search Engine..."

# Ensure EFS directory exists
mkdir -p ${EFS_PATH:-/mnt/efs}

# Start the application
exec uvicorn main:app --host 0.0.0.0 --port 8000 --reload




