#!/usr/bin/env bash
# Quiet background setup used only to produce the recorded asset
# (docs/assets/reliableq-demo.gif) via scripts/reliableq-demo.tape.
# This is *not* the reproducible walkthrough -- see scripts/demo.sh
# for that. Assumes `make up` and `make migrate` already ran and the
# binaries are already built.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

export DATABASE_URL="${DATABASE_URL:-postgres://reliableq:reliableq@localhost:5432/reliableq}"
export WORKER_LEASE_DURATION_SECS=5

: >/tmp/reliableq-record-pids

wait_for() {
  local url="$1" tries=0 code
  while true; do
    code=$(curl -s -o /dev/null -w '%{http_code}' "$url" || echo "000")
    [ "$code" = "200" ] && break
    tries=$((tries + 1))
    if [ "$tries" -gt 50 ]; then
      echo "timed out waiting for $url" >&2
      exit 1
    fi
    sleep 0.2
  done
}

FAKE_CHARGE_ENABLE_TEST_CONTROL=true ./target/debug/fake-charge >/tmp/reliableq-record-charge.log 2>&1 &
echo $! >>/tmp/reliableq-record-pids
./target/debug/reliableq-api >/tmp/reliableq-record-api.log 2>&1 &
echo $! >>/tmp/reliableq-record-pids

wait_for "http://127.0.0.1:8081/v1/test/inflight"
wait_for "http://127.0.0.1:8080/health/live"

WORKER_CHARGE_SERVICE_URL="http://127.0.0.1:8081" ./target/debug/reliableq-worker >/tmp/reliableq-record-worker1.log 2>&1 &
echo $! >>/tmp/reliableq-record-pids
wait_for "http://127.0.0.1:9091/metrics"

echo "setup complete"
