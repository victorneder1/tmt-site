#!/usr/bin/env bash
set -e

# Parked mode: with REDIRECT_URL set (Railway variable), serve only the redirect
# app. The full site and its CVM/SEC monitors are not started; deleting the
# variable brings everything back.
if [ -n "${REDIRECT_URL:-}" ]; then
    echo "REDIRECT_URL is set: serving redirect_app.py only"
    exec gunicorn redirect_app:app --bind "0.0.0.0:${PORT:-8080}" --workers 1
fi

# If Railway provides a persistent volume, use it for all mutable data
# so submissions and databases survive deployments.
if [ -z "${DATA_DIR:-}" ] && [ -d "/data" ]; then
    export DATA_DIR="/data"
elif [ -z "${DATA_DIR:-}" ] && [ -d "/persist" ]; then
    export DATA_DIR="/persist/tmt-data"
fi

mkdir -p "${DATA_DIR:-data}"

exec gunicorn app:app --bind "0.0.0.0:${PORT:-8080}" --workers 1 --timeout 120
