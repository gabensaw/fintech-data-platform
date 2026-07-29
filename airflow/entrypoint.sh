#!/usr/bin/env sh
set -e

HOST="${WAIT_FOR_HOST:-postgres}"
PORT="${WAIT_FOR_PORT:-5432}"
USER="${WAIT_FOR_USER:-fintech}"
TIMEOUT="${WAIT_FOR_TIMEOUT:-60}"

/opt/airflow/wait-for-postgres.sh "$HOST" "$PORT" "$USER" "$TIMEOUT"

exec /entrypoint "$@"
