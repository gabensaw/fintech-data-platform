#!/usr/bin/env sh
set -e

HOST="$1"
PORT="${2:-5432}"
USER="${3:-fintech}"
TIMEOUT="${4:-60}"
INTERVAL=5

if [ -z "$HOST" ]; then
  echo "Usage: $0 <host> [port] [user] [timeout]" >&2
  exit 1
fi

end_time=$(expr $(date +%s) + "$TIMEOUT")

printf 'Waiting for Postgres at %s:%s as %s...\n' "$HOST" "$PORT" "$USER"

while [ $(date +%s) -lt "$end_time" ]; do
  if pg_isready -h "$HOST" -p "$PORT" -U "$USER" >/dev/null 2>&1; then
    echo 'Postgres is ready.'
    exit 0
  fi
  echo 'Postgres unavailable; retrying in' "$INTERVAL" 'seconds...'
  sleep "$INTERVAL"
done

echo "Postgres did not become ready after ${TIMEOUT}s" >&2
exit 1
