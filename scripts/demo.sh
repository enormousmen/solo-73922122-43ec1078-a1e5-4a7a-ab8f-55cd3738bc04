#!/usr/bin/env bash
# Demo: shared diagnostic SQL service.
# Prereq: service running (docker compose up, or local uvicorn) at $BASE.
set -euo pipefail
BASE="${BASE:-http://localhost:8000}"
JSON='content-type: application/json'

pp() { python3 -m json.tool 2>/dev/null || cat; }
jid() { python3 -c "import json,sys;print(json.load(sys.stdin)['job_id'])"; }

echo "=== 1) submit a 3-statement job; statements run in order, autocommitted ==="
JOB=$(curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{
  "statements": [
    "CREATE TEMP TABLE demo_t (a int)",
    "INSERT INTO demo_t VALUES (1),(2),(3)",
    "SELECT sum(a) AS total FROM demo_t"
  ]}' | jid)
echo "job_id=$JOB"
sleep 1
curl -s "$BASE/jobs/$JOB" | pp

echo
echo "=== 2) session state does NOT leak to the next job on the same connection ==="
JOB=$(curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{
  "statements": [
    "SET application_name = '"'"'leaky'"'"'",
    "CREATE TEMP TABLE leak_t (x int)",
    "SELECT pg_advisory_lock(99)",
    "SELECT 1"
  ]}' | jid)
sleep 1
CONN=$(curl -s "$BASE/jobs/$JOB" | python3 -c "import json,sys;print(json.load(sys.stdin)['connection_id'])")
echo "dirty job ran on connection $CONN; submitting a check job (both conns may serve it;"
echo "run the demo twice or occupy the other conn to force the same one)"
JOB2=$(curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{
  "statements": [
    "SHOW application_name",
    "SELECT count(*) AS temp_objects FROM pg_class WHERE relnamespace = pg_my_temp_schema()",
    "SELECT pg_try_advisory_lock(99) AS lock_was_free"
  ]}' | jid)
sleep 1
curl -s "$BASE/jobs/$JOB2" | pp

echo
echo "=== 3) cancel a running job; cancel is accepted immediately ==="
JOB=$(curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{
  "statements": ["SELECT 1", "SELECT pg_sleep(60)", "SELECT 3"]}' | jid)
sleep 1
echo "-- cancel with a 3s delayed send (test hook delay_ms); the API answers at once:"
time curl -s -XPOST "$BASE/jobs/$JOB/cancel" -H "$JSON" -d '{"delay_ms": 3000}' | pp
echo "-- the running statement is interrupted once the cancel is sent (~3s),"
echo "-- statement 3 is skipped, statement 1 stays committed:"
sleep 5
curl -s "$BASE/jobs/$JOB" | pp

echo
echo "=== 4) slow cleanup (test hook) does not block the other connection ==="
JOB=$(curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{
  "statements": ["SELECT 1"],
  "options": {"cleanup_delay_ms": 6000}}' | jid)
sleep 2
echo "-- pool while connection cleans up (state=cleaning):"
curl -s "$BASE/pool" | pp
JOB2=$(curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{"statements": ["SELECT 2 AS still_fast"]}' | jid)
sleep 1
echo "-- a job on the OTHER connection finished meanwhile:"
curl -s "$BASE/jobs/$JOB2" | pp

echo
echo "=== 5) late cancel after natural finish is a no-op (409) ==="
JOB=$(curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{"statements": ["SELECT 1"]}' | jid)
sleep 1
curl -s -o /dev/null -w "cancel after finish -> HTTP %{http_code}\n" \
  -XPOST "$BASE/jobs/$JOB/cancel" -H "$JSON" -d '{}'

echo
echo "=== 6) explicit transaction control is rejected (422) ==="
curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{"statements": ["BEGIN", "SELECT 1"]}' | pp

echo
echo "=== 7) history-capacity pressure: unfinished jobs are never evicted ==="
echo "-- (docker-compose sets JOB_HISTORY=2; with the old bug the queued job"
echo "--  below would already return 404 once newer jobs were submitted)"
J1=$(curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{"statements": ["SELECT pg_sleep(6)"]}' | jid)
J2=$(curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{"statements": ["SELECT pg_sleep(6)"]}' | jid)
echo "-- both connections busy with 6s sleepers; submitting 3 more jobs:"
Q1=$(curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{"statements": ["SELECT 11"]}' | jid)
Q2=$(curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{"statements": ["SELECT 22"]}' | jid)
TARGET=$(curl -s -XPOST "$BASE/jobs" -H "$JSON" -d '{"statements": ["SELECT 33"]}' | jid)
echo "-- 5 jobs known, JOB_HISTORY=2; the still-queued target ($TARGET) must resolve:"
curl -s "$BASE/jobs/$TARGET" | pp
echo "-- and must still be cancellable:"
curl -s -XPOST "$BASE/jobs/$TARGET/cancel" -H "$JSON" -d '{}' | pp
echo "-- cancelled while queued; its record is still there:"
curl -s "$BASE/jobs/$TARGET" | pp
echo "-- job list: active records are all kept; finished history is capped:"
curl -s "$BASE/jobs" | pp
echo "-- cleaning up the sleepers (queued jobs Q1=$Q1 Q2=$Q2 then run):"
curl -s -o /dev/null -w "cancel sleeper 1 -> HTTP %{http_code}\n" -XPOST "$BASE/jobs/$J1/cancel" -H "$JSON" -d '{}'
curl -s -o /dev/null -w "cancel sleeper 2 -> HTTP %{http_code}\n" -XPOST "$BASE/jobs/$J2/cancel" -H "$JSON" -d '{}'
sleep 2
echo "-- afterwards only the newest JOB_HISTORY finished records are retained:"
curl -s "$BASE/jobs" | pp
