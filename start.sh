#!/usr/bin/env bash
set -e

# If Railway provides a /persist volume, use it for all SQLite databases
# so they survive deployments. Otherwise fall back to the in-container data/ dir.
if [ -d "/persist" ]; then
    export DATA_DIR="/persist/tmt-data"
fi

mkdir -p "${DATA_DIR:-data}"

exec gunicorn app:app --bind "0.0.0.0:${PORT:-8080}" --workers 1 --timeout 120
