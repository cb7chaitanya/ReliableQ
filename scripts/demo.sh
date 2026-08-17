#!/usr/bin/env bash
# 5-10 minute guided demo of ReliableQ (spec sec. 21).
#
# Walks through: normal submission -> success, a transient downstream
# failure that retries and recovers, a real worker crash (kill -9) and
# lease-based reclaim, idempotent replay after that crash (exactly one
# charge for the job that ran twice), and dead-job inspection + replay.
#
# Requires: docker, cargo, curl, python3 (for tiny JSON field
# extraction — no jq dependency assumed), psql via `docker exec`.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

export DATABASE_URL="${DATABASE_URL:-postgres://reliableq:reliableq@localhost:5432/reliableq}"
export LOG_FORMAT="pretty"
# Short on purpose so the crash/reclaim demo (step 3-4) doesn't need a
# 30s wait: lease duration is set by whichever worker *claims* a job,
# so every worker in this script uses the same short value.
export WORKER_LEASE_DURATION_SECS=5
API_URL="http://127.0.0.1:8080"
CHARGE_URL="http://127.0.0.1:8081"
WORKER_METRICS_URL="http://127.0.0.1:9091"

PIDS=()
cleanup() {
  echo
  echo "--- cleaning up background processes ---"
  for pid in "${PIDS[@]:-}"; do
    kill -9 "$pid" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

step() {
  echo
  echo "=================================================================="
  echo "  $1"
  echo "=================================================================="
}

wait_for() {
  local url="$1" tries=0 code
  while true; do
    code=$(curl -s -o /dev/null -w '%{http_code}' "$url" || echo "000")
    [ "$code" = "200" ] && break
    tries=$((tries + 1))
    if [ "$tries" -gt 50 ]; then
      echo "timed out waiting for $url (last status: $code)" >&2
      exit 1
    fi
    sleep 0.2
  done
}

json_field() {
  python3 -c "import sys,json; print(json.load(sys.stdin)$1)"
}

# axum's Json extractor rejects a body with no (or the wrong)
# Content-Type header — every chaos-control call must set it
# explicitly, or fake-charge silently stays in Normal mode.
chaos_control() {
  local response
  response=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$CHARGE_URL/v1/test/control" \
    -H 'Content-Type: application/json' -d "$1")
  if [ "$response" != "200" ]; then
    echo "chaos_control($1) failed: HTTP $response" >&2
    exit 1
  fi
}

step "0/6  Setup: postgres, migrations, fake-charge, api"
make up
make migrate
cargo build -p reliableq-api -p fake-charge -p reliableq-worker >/dev/null

FAKE_CHARGE_ENABLE_TEST_CONTROL=true ./target/debug/fake-charge >/tmp/reliableq-demo-charge.log 2>&1 &
PIDS+=($!)
./target/debug/reliableq-api >/tmp/reliableq-demo-api.log 2>&1 &
PIDS+=($!)
wait_for "$CHARGE_URL/v1/test/inflight"
wait_for "$API_URL/health/live"
echo "fake-charge and api are up."

step "1/6  Normal submission and success"
JOB1=$(curl -s -X POST "$API_URL/v1/jobs" -H 'Content-Type: application/json' \
  -d '{"kind":"charge","payload":{"customer_id":"demo-1","amount_cents":500,"currency":"INR"},"max_attempts":5}')
echo "submitted: $JOB1"
JOB1_ID=$(echo "$JOB1" | json_field '["id"]')

WORKER_CHARGE_SERVICE_URL="$CHARGE_URL" ./target/debug/reliableq-worker >/tmp/reliableq-demo-worker.log 2>&1 &
WORKER_PID=$!
PIDS+=($WORKER_PID)
wait_for "$WORKER_METRICS_URL/metrics"

sleep 1.5
echo "job $JOB1_ID after worker processes it:"
curl -s "$API_URL/v1/jobs/$JOB1_ID" | python3 -m json.tool

step "2/6  Transient downstream failure -> automatic retry -> success"
chaos_control '{"mode":"fail_next","n":2,"status":503}'
JOB2=$(curl -s -X POST "$API_URL/v1/jobs" -H 'Content-Type: application/json' \
  -d '{"kind":"charge","payload":{"customer_id":"demo-2","amount_cents":700,"currency":"INR"},"max_attempts":5}')
JOB2_ID=$(echo "$JOB2" | json_field '["id"]')
echo "submitted $JOB2_ID with the next 2 downstream calls forced to 503"
echo "waiting for capped-backoff retries to catch up..."
sleep 4
echo "job $JOB2_ID's attempt history (expect >1 attempt, ending SUCCEEDED):"
curl -s "$API_URL/v1/jobs/$JOB2_ID" | python3 -m json.tool

step "3/6  Real worker crash: kill -9 mid-call, job stranded RUNNING"
chaos_control '{"mode":"delay_ms","ms":4000}'
JOB3=$(curl -s -X POST "$API_URL/v1/jobs" -H 'Content-Type: application/json' \
  -d '{"kind":"charge","payload":{"customer_id":"demo-3","amount_cents":900,"currency":"INR"},"max_attempts":5}')
JOB3_ID=$(echo "$JOB3" | json_field '["id"]')
echo "submitted $JOB3_ID; fake-charge will hang for 4s on every call from here"
sleep 1
echo "killing the worker (-9) while it is almost certainly mid-call on $JOB3_ID..."
kill -9 "$WORKER_PID" || true
sleep 1
echo "job $JOB3_ID is now stranded RUNNING (no worker alive to finish it):"
curl -s "$API_URL/v1/jobs/$JOB3_ID" | python3 -c 'import sys,json; d=json.load(sys.stdin); print("status:", d["status"])'

chaos_control '{"mode":"normal"}'

step "4/6  Lease expires; a new worker reclaims and finishes it"
echo "waiting past the lease duration (default 30s; using a short one for this demo)..."
WORKER_LEASE_DURATION_SECS=5 WORKER_CHARGE_SERVICE_URL="$CHARGE_URL" \
  ./target/debug/reliableq-worker >/tmp/reliableq-demo-worker2.log 2>&1 &
WORKER2_PID=$!
PIDS+=($WORKER2_PID)
sleep 8
echo "job $JOB3_ID after reclaim:"
curl -s "$API_URL/v1/jobs/$JOB3_ID" | python3 -m json.tool

step "5/6  Idempotent replay: exactly one charge exists for the job that ran twice"
CHARGE_COUNT=$(docker exec reliableq-postgres-1 psql -U reliableq -d reliableq -tAc \
  "SELECT count(*) FROM charges WHERE idempotency_key = 'reliableq:charge:$JOB3_ID'")
echo "charges with idempotency_key reliableq:charge:$JOB3_ID -> $CHARGE_COUNT"
if [ "$CHARGE_COUNT" != "1" ]; then
  echo "UNEXPECTED: expected exactly 1 charge row" >&2
  exit 1
fi
echo "confirmed: one committed charge, even though the job was attempted twice."

step "6/6  Dead job: permanent rejection, inspection, and explicit replay"
chaos_control '{"mode":"permanent_reject"}'
JOB4=$(curl -s -X POST "$API_URL/v1/jobs" -H 'Content-Type: application/json' \
  -d '{"kind":"charge","payload":{"customer_id":"demo-4","amount_cents":250,"currency":"INR"},"max_attempts":5}')
JOB4_ID=$(echo "$JOB4" | json_field '["id"]')
echo "submitted $JOB4_ID with the downstream forced to permanently reject (422)"
sleep 1.5
echo "job $JOB4_ID (expect DEAD after exactly one attempt):"
curl -s "$API_URL/v1/jobs/$JOB4_ID" | python3 -c 'import sys,json; d=json.load(sys.stdin); print("status:", d["status"], "attempts:", len(d["attempts"]))'

echo "GET /v1/dead-jobs:"
curl -s "$API_URL/v1/dead-jobs" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(len(d["items"]), "dead job(s)")'

chaos_control '{"mode":"normal"}'
echo "downstream reset to normal; retrying $JOB4_ID..."
curl -s -X POST "$API_URL/v1/jobs/$JOB4_ID/retry" | python3 -m json.tool
sleep 1.5
echo "job $JOB4_ID after replay:"
curl -s "$API_URL/v1/jobs/$JOB4_ID" | python3 -c 'import sys,json; d=json.load(sys.stdin); print("status:", d["status"], "attempts:", len(d["attempts"]))'

step "Done. See docs/operations.md for day-to-day procedures."
