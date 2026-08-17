# Reproducible source for docs/assets/reliableq-demo.gif.
#
# Regenerate with (from the repo root, after `make up`, `make migrate`,
# and `cargo build -p reliableq-api -p fake-charge -p reliableq-worker`):
#
#   vhs scripts/reliableq-demo.tape
#
# Covers, against real running processes (no simulated output): a
# normal submission that succeeds, a genuine worker crash (kill -9)
# and lease-based reclaim by a second worker, proof that the
# twice-attempted job produced exactly one committed charge, a
# permanently-rejected job going DEAD, and an explicit operator retry
# that succeeds. See scripts/demo.sh for the full guided walkthrough
# this recording is a condensed version of.

Output docs/assets/reliableq-demo.gif

Set Shell "bash"
Set FontSize 14
Set Width 1100
Set Height 640
Set Padding 16
Set Theme "Dracula"
Set TypingSpeed 15ms
Set Framerate 24

Hide
Type "clear && export DATABASE_URL=postgres://reliableq:reliableq@localhost:5432/reliableq && ./scripts/record-demo-setup.sh"
Enter
Wait+Screen@30s /setup complete/
Type "clear"
Enter
Show

Type `echo "# 1/5  Normal submission -> success"`
Enter
Sleep 500ms
Type `JOB1=$(curl -s -X POST http://127.0.0.1:8080/v1/jobs -H 'Content-Type: application/json' -d '{"kind":"charge","payload":{"customer_id":"demo-1","amount_cents":500,"currency":"INR"},"max_attempts":5}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')`
Enter
Sleep 1.5s
Type `curl -s http://127.0.0.1:8080/v1/jobs/$JOB1 | python3 -m json.tool`
Enter
Sleep 1.5s

Type `echo "# 2/5  Worker crash (kill -9) -> lease expires -> reclaimed"`
Enter
Sleep 500ms
Type `curl -s -X POST http://127.0.0.1:8081/v1/test/control -H 'Content-Type: application/json' -d '{"mode":"delay_ms","ms":4000}' >/dev/null`
Enter
Sleep 300ms
Type `JOB2=$(curl -s -X POST http://127.0.0.1:8080/v1/jobs -H 'Content-Type: application/json' -d '{"kind":"charge","payload":{"customer_id":"demo-2","amount_cents":900,"currency":"INR"},"max_attempts":5}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')`
Enter
Sleep 1s
Type `kill -9 $(tail -n1 /tmp/reliableq-record-pids)`
Enter
Sleep 1s
Type `curl -s http://127.0.0.1:8080/v1/jobs/$JOB2 | python3 -c 'import sys,json;d=json.load(sys.stdin);print("status:",d["status"])'`
Enter
Sleep 1s
Type `curl -s -X POST http://127.0.0.1:8081/v1/test/control -H 'Content-Type: application/json' -d '{"mode":"normal"}' >/dev/null`
Enter
Sleep 300ms
Type `WORKER_CHARGE_SERVICE_URL=http://127.0.0.1:8081 WORKER_LEASE_DURATION_SECS=5 ./target/debug/reliableq-worker >/tmp/reliableq-record-worker2.log 2>&1 & echo $! >>/tmp/reliableq-record-pids`
Enter
Sleep 300ms
Type `echo "waiting for lease expiry + reclaim..." && sleep 8`
Enter
Sleep 8.5s
Type `curl -s http://127.0.0.1:8080/v1/jobs/$JOB2 | python3 -c 'import sys,json;d=json.load(sys.stdin);print("status:",d["status"],"| attempts:",[a["outcome"] for a in d["attempts"]])'`
Enter
Sleep 1.5s

Type `echo "# 3/5  Exactly one committed charge for the job that ran twice"`
Enter
Sleep 500ms
Type `docker exec reliableq-postgres-1 psql -U reliableq -d reliableq -tAc "SELECT count(*) FROM charges WHERE idempotency_key = 'reliableq:charge:$JOB2'"`
Enter
Sleep 1.5s

Type `echo "# 4/5  Permanent rejection -> DEAD"`
Enter
Sleep 500ms
Type `curl -s -X POST http://127.0.0.1:8081/v1/test/control -H 'Content-Type: application/json' -d '{"mode":"permanent_reject"}' >/dev/null`
Enter
Sleep 300ms
Type `JOB3=$(curl -s -X POST http://127.0.0.1:8080/v1/jobs -H 'Content-Type: application/json' -d '{"kind":"charge","payload":{"customer_id":"demo-3","amount_cents":250,"currency":"INR"},"max_attempts":5}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')`
Enter
Sleep 1.5s
Type `curl -s http://127.0.0.1:8080/v1/jobs/$JOB3 | python3 -c 'import sys,json;d=json.load(sys.stdin);print("status:",d["status"],"attempts:",len(d["attempts"]))'`
Enter
Sleep 1.5s

Type `echo "# 5/5  Explicit operator retry -> succeeds"`
Enter
Sleep 500ms
Type `curl -s -X POST http://127.0.0.1:8081/v1/test/control -H 'Content-Type: application/json' -d '{"mode":"normal"}' >/dev/null && curl -s -X POST http://127.0.0.1:8080/v1/jobs/$JOB3/retry >/dev/null`
Enter
Sleep 1.5s
Type `curl -s http://127.0.0.1:8080/v1/jobs/$JOB3 | python3 -c 'import sys,json;d=json.load(sys.stdin);print("status:",d["status"],"attempts:",len(d["attempts"]))'`
Enter
Sleep 2s

Hide
Type "clear && ./scripts/record-demo-cleanup.sh"
Enter
Sleep 500ms
Show
