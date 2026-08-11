#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

COMMANDER_URL="${COMMANDER_URL:-http://localhost:18080}"
COMPOSE=(docker compose)

pass() { echo "PASS: $*"; }
fail() { echo "FAIL: $*"; exit 1; }

wait_healthy() {
  echo "Waiting for Commander at $COMMANDER_URL/health ..."
  for i in $(seq 1 60); do
    if curl -fsS "$COMMANDER_URL/health" >/dev/null 2>&1; then
      echo "Commander is healthy"
      return 0
    fi
    sleep 2
  done
  fail "Commander did not become healthy in time"
}

post_mission() {
  local desc="$1"
  local idem="${2:-}"
  if [[ -n "$idem" ]]; then
    curl -fsS -X POST "$COMMANDER_URL/missions" \
      -H "Content-Type: application/json" \
      -H "Idempotency-Key: $idem" \
      -d "{\"description\":\"$desc\"}"
  else
    curl -fsS -X POST "$COMMANDER_URL/missions" \
      -H "Content-Type: application/json" \
      -d "{\"description\":\"$desc\"}"
  fi
}

get_mission() {
  curl -fsS "$COMMANDER_URL/missions/$1"
}

mission_field() {
  # usage: mission_field JSON field
  local json="$1"
  local field="$2"
  if command -v jq >/dev/null 2>&1; then
    echo "$json" | jq -r ".$field"
  else
    python3 - "$json" "$field" <<'PY'
import json,sys
obj=json.loads(sys.argv[1])
val=obj.get(sys.argv[2])
print("" if val is None else val)
PY
  fi
}

wait_status() {
  local id="$1"
  local want="$2"   # comma-separated acceptable statuses
  local timeout="${3:-90}"
  local start
  start=$(date +%s)
  while true; do
    local body status
    body=$(get_mission "$id")
    status=$(mission_field "$body" status)
    if echo ",$want," | grep -q ",$status,"; then
      echo "$body"
      return 0
    fi
    if (( $(date +%s) - start > timeout )); then
      echo "last status=$status body=$body" >&2
      return 1
    fi
    sleep 1
  done
}

echo "=== Bringing up stack ==="
"${COMPOSE[@]}" up -d --build

wait_healthy

echo
echo "=== Test 1: Single mission flow ==="
RESP=$(post_mission "Single recon mission")
MID=$(mission_field "$RESP" mission_id)
[[ -n "$MID" && "$MID" != "null" ]] || fail "no mission_id"
echo "mission_id=$MID"

BODY=$(get_mission "$MID")
ST=$(mission_field "$BODY" status)
[[ "$ST" == "QUEUED" || "$ST" == "IN_PROGRESS" || "$ST" == "COMPLETED" || "$ST" == "FAILED" ]] || fail "unexpected initial status $ST"

FINAL=$(wait_status "$MID" "COMPLETED,FAILED" 90) || fail "mission did not reach terminal state"
FST=$(mission_field "$FINAL" status)
pass "single mission ended as $FST"

echo
echo "=== Test 2: Idempotency-Key replay ==="
KEY="idem-$(date +%s)-$$"
R1=$(post_mission "Idempotent mission" "$KEY")
ID1=$(mission_field "$R1" mission_id)
R2=$(post_mission "Idempotent mission CHANGED TEXT" "$KEY")
ID2=$(mission_field "$R2" mission_id)
[[ "$ID1" == "$ID2" ]] || fail "idempotency returned different ids: $ID1 vs $ID2"
pass "idempotency key returned same mission_id=$ID1"

echo
echo "=== Test 3: Concurrency (overlap via API) ==="
cat > /tmp/mc-test-soldier.yml <<'EOF'
services:
  soldier:
    environment:
      MISSION_MIN_DELAY_SECONDS: "5"
      MISSION_MAX_DELAY_SECONDS: "5"
      WORKER_POOL_SIZE: "4"
EOF
"${COMPOSE[@]}" -f docker-compose.yml -f /tmp/mc-test-soldier.yml up -d --no-deps --force-recreate soldier
sleep 3

IDS=()
for i in 1 2 3 4; do
  RESP=$(post_mission "Concurrent mission $i")
  IDS+=("$(mission_field "$RESP" mission_id)")
done
echo "submitted: ${IDS[*]}"

# Poll until at least 2 are IN_PROGRESS at the same snapshot, or all terminal with overlapping started_at
OVERLAP=0
DEADLINE=$(( $(date +%s) + 40 ))
while (( $(date +%s) < DEADLINE )); do
  INP=0
  for id in "${IDS[@]}"; do
    BODY=$(get_mission "$id")
    ST=$(mission_field "$BODY" status)
    if [[ "$ST" == "IN_PROGRESS" ]]; then
      INP=$((INP + 1))
    fi
  done
  if (( INP >= 2 )); then
    OVERLAP=1
    echo "observed $INP missions IN_PROGRESS simultaneously"
    break
  fi
  sleep 0.5
done

if (( OVERLAP != 1 )); then
  # Fallback: compare started_at / completed_at overlap from final states
  echo "No live IN_PROGRESS snapshot; checking timestamp overlap..."
  for id in "${IDS[@]}"; do
    wait_status "$id" "COMPLETED,FAILED" 60 >/dev/null || fail "mission $id did not finish"
  done
  python3 - "${IDS[@]}" <<'PY' || fail "no overlapping execution windows"
import json,sys,urllib.request
base="http://localhost:18080"
ids=sys.argv[1:]
rows=[]
for i in ids:
    with urllib.request.urlopen(base+"/missions/"+i) as r:
        rows.append(json.load(r))
# parse times
from datetime import datetime
def parse(t):
    if not t: return None
    return datetime.fromisoformat(t.replace("Z","+00:00"))
intervals=[]
for m in rows:
    s=parse(m.get("started_at"))
    e=parse(m.get("completed_at"))
    if s and e:
        intervals.append((s,e,m["mission_id"]))
overlap=False
for i in range(len(intervals)):
    for j in range(i+1,len(intervals)):
        a0,a1,_=intervals[i]
        b0,b1,_=intervals[j]
        if a0 < b1 and b0 < a1:
            overlap=True
            print("overlap", intervals[i][2], intervals[j][2])
if not overlap:
    raise SystemExit(1)
PY
fi

for id in "${IDS[@]}"; do
  wait_status "$id" "COMPLETED,FAILED" 90 >/dev/null || fail "mission $id did not finish"
done
pass "concurrency test completed"

echo
echo "=== Test 4: Auth token rotation ==="
# Wait past default TTL (30s) plus skew, then submit another mission
echo "sleeping 35s to allow token expiry/refresh..."
sleep 35
RESP=$(post_mission "Post-rotation mission")
MID=$(mission_field "$RESP" mission_id)
FINAL=$(wait_status "$MID" "COMPLETED,FAILED" 90) || fail "post-rotation mission failed"
pass "mission after token TTL completed: $(mission_field "$FINAL" status)"

echo "Checking rotation logs (token_id, no raw tokens)..."
LOGS=$("${COMPOSE[@]}" logs commander soldier 2>&1 || true)
echo "$LOGS" | grep -q "token issued\|token refreshed" || fail "missing token rotation log lines"
echo "$LOGS" | grep -qiE 'token=[0-9a-f]{32,}' && fail "raw token appears to be logged" || true
pass "token rotation logs present"

echo
echo "=============================="
echo " ALL TESTS PASSED"
echo "=============================="
