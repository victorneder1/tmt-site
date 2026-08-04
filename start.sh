#!/usr/bin/env bash
set -e

# If Railway provides a persistent volume, use it for all mutable data
# so submissions and databases survive deployments.
if [ -z "${DATA_DIR:-}" ] && [ -d "/data" ]; then
    export DATA_DIR="/data"
elif [ -z "${DATA_DIR:-}" ] && [ -d "/persist" ]; then
    export DATA_DIR="/persist/tmt-data"
fi

mkdir -p "${DATA_DIR:-data}"

exec gunicorn app:app --bind "0.0.0.0:${PORT:-8080}" --workers 1 --timeout 120
