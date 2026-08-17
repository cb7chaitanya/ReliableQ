#!/usr/bin/env bash
# Kills the background processes started by record-demo-setup.sh.
set -euo pipefail

if [ -f /tmp/reliableq-record-pids ]; then
  while read -r pid; do
    kill -9 "$pid" >/dev/null 2>&1 || true
  done </tmp/reliableq-record-pids
  rm -f /tmp/reliableq-record-pids
fi
