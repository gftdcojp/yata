#!/usr/bin/env bash
# yata CI: Load Test (WAL Projection architecture)
#
# Tests: write throughput, WAL propagation, read QPS, 2-hop traversal,
#        checkpoint/cold-start, mixed read-write, Arrow WAL format.
#
# Usage:
#   # Against docker-compose containers (default)
#   docker compose -f docker-compose.ci.yml up -d --build
#   ./scripts/ci-loadtest.sh
#
#   # Custom params
#   ./scripts/ci-loadtest.sh --writes 2000 --reads 500
#
#   # Inside docker-compose loadtest service
#   docker compose -f docker-compose.ci.yml --profile loadtest run --rm loadtest

set -euo pipefail

# ── Defaults (overridden by env or flags) ──
WRITE_URL="${WRITE_URL:-http://localhost:8083}"
READ0_URL="${READ0_URL:-http://localhost:8090}"
READ1_URL="${READ1_URL:-http://localhost:8091}"
NUM_WRITES="${NUM_WRITES:-1000}"
NUM_READS="${NUM_READS:-500}"

# Parse flags
while [[ $# -gt 0 ]]; do
  case "$1" in
    --writes) NUM_WRITES="$2"; shift 2 ;;
    --reads)  NUM_READS="$2"; shift 2 ;;
    --write-url) WRITE_URL="$2"; shift 2 ;;
    *) shift ;;
  esac
done

echo "============================================"
echo "  yata CI Load Test (WAL Projection)"
echo "============================================"
echo "Write:  $WRITE_URL"
echo "Read-0: $READ0_URL"
echo "Read-1: $READ1_URL"
echo "Writes: $NUM_WRITES | Reads: $NUM_READS"
echo ""

# ── Health check ──
for url in "$WRITE_URL" "$READ0_URL" "$READ1_URL"; do
  if ! curl -sf "$url/health" > /dev/null 2>&1; then
    echo "FAIL: $url not healthy"
    exit 1
  fi
done
echo "All containers healthy."
echo ""

# ── Helpers ──
calc_stats() {
  local name="$1"
  shift
  python3 -c "
import sys
vals = sorted([int(x) for x in '$*'.split() if x])
if not vals:
    print(f'  {\"$name\":>12}: no data')
    sys.exit()
n = len(vals)
p50 = vals[n//2]
p95 = vals[int(n*0.95)]
p99 = vals[int(n*0.99)]
mn  = vals[0]
mx  = vals[-1]
avg = sum(vals)/n
print(f'  {\"$name\":>12}: p50={p50}ms  p95={p95}ms  p99={p99}ms  min={mn}ms  max={mx}ms  avg={avg:.1f}ms  n={n}')
"
}

PASS=0
FAIL=0

check() {
  local name="$1" cond="$2"
  if python3 -c "import sys; sys.exit(0 if $cond else 1)" 2>/dev/null; then
    echo "  PASS: $name"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: $name ($cond)"
    FAIL=$((FAIL + 1))
  fi
}

# ══════════════════════════════════════════════
# Phase 1: Sequential Write + WAL Push
# ══════════════════════════════════════════════
echo "--- Phase 1: Write + WAL Push ($NUM_WRITES records) ---"
WRITE_START=$(python3 -c "import time; print(int(time.time()*1000))")
WRITE_LATENCIES=""
APPLY_LATENCIES=""

for i in $(seq 1 $NUM_WRITES); do
  T0=$(python3 -c "import time; print(int(time.time()*1000))")

  RESULT=$(curl -s -X POST "$WRITE_URL/xrpc/ai.gftd.yata.mergeRecordWal" \
    -H 'Content-Type: application/json' -H 'X-Magatama-Verified: true' \
    -d "{\"label\":\"LtNode\",\"pk_key\":\"rkey\",\"pk_value\":\"lt_$i\",\"props\":{\"idx\":$i,\"batch\":\"ci\",\"text\":\"record $i\",\"collection\":\"ai.gftd.apps.loadtest\",\"repo\":\"did:web:loadtest.gftd.ai\"}}")

  T1=$(python3 -c "import time; print(int(time.time()*1000))")
  WRITE_LATENCIES="$WRITE_LATENCIES $((T1 - T0))"

  # Push WAL entry to both read replicas
  ENTRY=$(echo "$RESULT" | python3 -c "import sys,json; e=json.load(sys.stdin).get('wal_entry'); print(json.dumps(e) if e else '')" 2>/dev/null || echo "")
  if [ -n "$ENTRY" ] && [ "$ENTRY" != "null" ]; then
    T2=$(python3 -c "import time; print(int(time.time()*1000))")
    curl -s -X POST "$READ0_URL/xrpc/ai.gftd.yata.walApply" \
      -H 'Content-Type: application/json' -d "{\"entries\":[$ENTRY]}" > /dev/null &
    curl -s -X POST "$READ1_URL/xrpc/ai.gftd.yata.walApply" \
      -H 'Content-Type: application/json' -d "{\"entries\":[$ENTRY]}" > /dev/null &
    wait
    T3=$(python3 -c "import time; print(int(time.time()*1000))")
    APPLY_LATENCIES="$APPLY_LATENCIES $((T3 - T2))"
  fi

  if [ $((i % 200)) -eq 0 ]; then
    echo "  $i/$NUM_WRITES written..."
  fi
done

WRITE_END=$(python3 -c "import time; print(int(time.time()*1000))")
WRITE_TOTAL_MS=$((WRITE_END - WRITE_START))
WRITE_OPS=$(python3 -c "print(f'{$NUM_WRITES / ($WRITE_TOTAL_MS / 1000.0):.0f}')")

echo ""
echo "Phase 1 results (total ${WRITE_TOTAL_MS}ms, ${WRITE_OPS} ops/s):"
calc_stats "Write" $WRITE_LATENCIES
calc_stats "WAL-Push" $APPLY_LATENCIES
check "write_ops >= 50/s" "$WRITE_OPS >= 50"

# ══════════════════════════════════════════════
# Phase 2: Count Query (read replicas)
# ══════════════════════════════════════════════
echo ""
echo "--- Phase 2: Count Query ($NUM_READS queries, round-robin replicas) ---"
COUNT_LATENCIES=""
COUNT_START=$(python3 -c "import time; print(int(time.time()*1000))")

for i in $(seq 1 $NUM_READS); do
  if [ $((i % 2)) -eq 0 ]; then URL="$READ0_URL"; else URL="$READ1_URL"; fi
  T0=$(python3 -c "import time; print(int(time.time()*1000))")
  curl -s -X POST "$URL/xrpc/ai.gftd.yata.cypher" \
    -H 'Content-Type: application/json' -H 'X-Magatama-Verified: true' \
    -d '{"statement":"MATCH (r:LtNode) WHERE r.batch = '"'"'ci'"'"' RETURN count(r) AS cnt LIMIT 1"}' > /dev/null
  T1=$(python3 -c "import time; print(int(time.time()*1000))")
  COUNT_LATENCIES="$COUNT_LATENCIES $((T1 - T0))"
done

COUNT_END=$(python3 -c "import time; print(int(time.time()*1000))")
COUNT_TOTAL_MS=$((COUNT_END - COUNT_START))
COUNT_QPS=$(python3 -c "print(f'{$NUM_READS / ($COUNT_TOTAL_MS / 1000.0):.0f}')")

echo "Phase 2 results (total ${COUNT_TOTAL_MS}ms, ${COUNT_QPS} QPS):"
calc_stats "Count" $COUNT_LATENCIES
check "count_qps >= 30" "$COUNT_QPS >= 30"

# ══════════════════════════════════════════════
# Phase 3: Point Read (random rkey lookup)
# ══════════════════════════════════════════════
echo ""
echo "--- Phase 3: Point Read ($NUM_READS queries) ---"
POINT_LATENCIES=""
POINT_START=$(python3 -c "import time; print(int(time.time()*1000))")

for i in $(seq 1 $NUM_READS); do
  RK=$((RANDOM % NUM_WRITES + 1))
  if [ $((i % 2)) -eq 0 ]; then URL="$READ0_URL"; else URL="$READ1_URL"; fi
  T0=$(python3 -c "import time; print(int(time.time()*1000))")
  curl -s -X POST "$URL/xrpc/ai.gftd.yata.cypher" \
    -H 'Content-Type: application/json' -H 'X-Magatama-Verified: true' \
    -d "{\"statement\":\"MATCH (r:LtNode) WHERE r.rkey = 'lt_$RK' RETURN r.text AS text LIMIT 1\"}" > /dev/null
  T1=$(python3 -c "import time; print(int(time.time()*1000))")
  POINT_LATENCIES="$POINT_LATENCIES $((T1 - T0))"
done

POINT_END=$(python3 -c "import time; print(int(time.time()*1000))")
POINT_TOTAL_MS=$((POINT_END - POINT_START))
POINT_QPS=$(python3 -c "print(f'{$NUM_READS / ($POINT_TOTAL_MS / 1000.0):.0f}')")

echo "Phase 3 results (total ${POINT_TOTAL_MS}ms, ${POINT_QPS} QPS):"
calc_stats "PointRead" $POINT_LATENCIES
check "point_qps >= 30" "$POINT_QPS >= 30"

# ══════════════════════════════════════════════
# Phase 4: Edge Create + 2-Hop Traversal
# ══════════════════════════════════════════════
echo ""
echo "--- Phase 4: Edge Create + 2-Hop Traversal ---"

# Create Company nodes
for i in $(seq 1 5); do
  curl -s -X POST "$WRITE_URL/xrpc/ai.gftd.yata.mergeRecordWal" \
    -H 'Content-Type: application/json' -H 'X-Magatama-Verified: true' \
    -d "{\"label\":\"LtCompany\",\"pk_key\":\"rkey\",\"pk_value\":\"co_$i\",\"props\":{\"name\":\"Company $i\"}}" > /dev/null
done

# Create edges: LtNode -[:WORKS_AT]-> LtCompany (first 100 nodes)
EDGE_COUNT=0
for i in $(seq 1 100); do
  CO=$((i % 5 + 1))
  RES=$(curl -s -X POST "$WRITE_URL/xrpc/ai.gftd.yata.cypher" \
    -H 'Content-Type: application/json' -H 'X-Magatama-Verified: true' \
    -d "{\"statement\":\"MATCH (p:LtNode),(c:LtCompany) WHERE p.rkey = 'lt_$i' AND c.rkey = 'co_$CO' CREATE (p)-[:WORKS_AT]->(c)\"}")
  EDGE_COUNT=$((EDGE_COUNT + 1))
done

# Create edges: LtNode -[:KNOWS]-> LtNode (first 50 nodes, next neighbor)
for i in $(seq 1 49); do
  NXT=$((i + 1))
  curl -s -X POST "$WRITE_URL/xrpc/ai.gftd.yata.cypher" \
    -H 'Content-Type: application/json' -H 'X-Magatama-Verified: true' \
    -d "{\"statement\":\"MATCH (a:LtNode),(b:LtNode) WHERE a.rkey = 'lt_$i' AND b.rkey = 'lt_$NXT' CREATE (a)-[:KNOWS]->(b)\"}" > /dev/null
done
EDGE_COUNT=$((EDGE_COUNT + 49))
echo "  Created $EDGE_COUNT edges."

# 2-hop traversal: Person -> KNOWS -> Person -> WORKS_AT -> Company
TWOHOP_READS=$((NUM_READS / 5))
TWOHOP_LATENCIES=""
TWOHOP_START=$(python3 -c "import time; print(int(time.time()*1000))")

for i in $(seq 1 $TWOHOP_READS); do
  RK=$((RANDOM % 49 + 1))
  T0=$(python3 -c "import time; print(int(time.time()*1000))")
  curl -s -X POST "$WRITE_URL/xrpc/ai.gftd.yata.cypher" \
    -H 'Content-Type: application/json' -H 'X-Magatama-Verified: true' \
    -d "{\"statement\":\"MATCH (a:LtNode)-[:KNOWS]->(b:LtNode)-[:WORKS_AT]->(c:LtCompany) WHERE a.rkey = 'lt_$RK' RETURN b.rkey AS person, c.name AS company LIMIT 5\"}" > /dev/null
  T1=$(python3 -c "import time; print(int(time.time()*1000))")
  TWOHOP_LATENCIES="$TWOHOP_LATENCIES $((T1 - T0))"
done

TWOHOP_END=$(python3 -c "import time; print(int(time.time()*1000))")
TWOHOP_TOTAL_MS=$((TWOHOP_END - TWOHOP_START))
TWOHOP_QPS=$(python3 -c "print(f'{$TWOHOP_READS / ($TWOHOP_TOTAL_MS / 1000.0):.0f}')" 2>/dev/null || echo "0")

echo "Phase 4 results (total ${TWOHOP_TOTAL_MS}ms, ${TWOHOP_QPS} QPS):"
calc_stats "2-Hop" $TWOHOP_LATENCIES
check "2hop_qps >= 10" "$TWOHOP_QPS >= 10"

# ══════════════════════════════════════════════
# Phase 5: WAL Operations (flush + checkpoint + cold start)
# ══════════════════════════════════════════════
echo ""
echo "--- Phase 5: WAL Operations ---"

# WAL tail
T0=$(python3 -c "import time; print(int(time.time()*1000))")
TAIL=$(curl -s -X POST "$WRITE_URL/xrpc/ai.gftd.yata.walTail" \
  -H 'Content-Type: application/json' -d '{"after_seq":0,"limit":10}')
T1=$(python3 -c "import time; print(int(time.time()*1000))")
HEAD_SEQ=$(echo "$TAIL" | python3 -c "import sys,json; print(json.load(sys.stdin).get('head_seq',0))" 2>/dev/null || echo "0")
echo "  walTail: $((T1 - T0))ms (head_seq=$HEAD_SEQ)"

# WAL flush segment
T0=$(python3 -c "import time; print(int(time.time()*1000))")
FLUSH=$(curl -s -X POST "$WRITE_URL/xrpc/ai.gftd.yata.walFlushSegment" \
  -H 'Content-Type: application/json' -d '{}')
T1=$(python3 -c "import time; print(int(time.time()*1000))")
FLUSH_BYTES=$(echo "$FLUSH" | python3 -c "import sys,json; print(json.load(sys.stdin).get('bytes',0))" 2>/dev/null || echo "0")
echo "  walFlush: $((T1 - T0))ms (bytes=$FLUSH_BYTES)"
check "flush_bytes > 0" "$FLUSH_BYTES > 0"

# WAL checkpoint (YataFragment)
T0=$(python3 -c "import time; print(int(time.time()*1000))")
CP=$(curl -s -X POST "$WRITE_URL/xrpc/ai.gftd.yata.walCheckpoint" \
  -H 'Content-Type: application/json' -d '{}')
T1=$(python3 -c "import time; print(int(time.time()*1000))")
CP_V=$(echo "$CP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('vertices',0))" 2>/dev/null || echo "0")
CP_E=$(echo "$CP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('edges',0))" 2>/dev/null || echo "0")
CP_MS=$((T1 - T0))
echo "  walCheckpoint: ${CP_MS}ms (${CP_V}v ${CP_E}e)"
check "checkpoint_vertices > 0" "$CP_V > 0"

# Cold start read-1 from checkpoint
T0=$(python3 -c "import time; print(int(time.time()*1000))")
COLD=$(curl -s -X POST "$READ1_URL/xrpc/ai.gftd.yata.walColdStart" \
  -H 'Content-Type: application/json' -d '{}')
T1=$(python3 -c "import time; print(int(time.time()*1000))")
COLD_SEQ=$(echo "$COLD" | python3 -c "import sys,json; print(json.load(sys.stdin).get('checkpoint_seq',0))" 2>/dev/null || echo "0")
COLD_MS=$((T1 - T0))
echo "  walColdStart (read-1): ${COLD_MS}ms (checkpoint_seq=$COLD_SEQ)"

# Catch up from WAL tail after checkpoint
CATCH_TAIL=$(curl -s -X POST "$WRITE_URL/xrpc/ai.gftd.yata.walTail" \
  -H 'Content-Type: application/json' -d "{\"after_seq\":$COLD_SEQ,\"limit\":100000}")
CATCH_ENTRIES=$(echo "$CATCH_TAIL" | python3 -c "import sys,json; d=json.load(sys.stdin); entries=d.get('entries',[]); print(len(entries))" 2>/dev/null || echo "0")
if [ "$CATCH_ENTRIES" -gt 0 ]; then
  ENTRIES_JSON=$(echo "$CATCH_TAIL" | python3 -c "import sys,json; d=json.load(sys.stdin); print(json.dumps(d.get('entries',[])))")
  curl -s -X POST "$READ1_URL/xrpc/ai.gftd.yata.walApply" \
    -H 'Content-Type: application/json' -d "{\"entries\":$ENTRIES_JSON}" > /dev/null
fi
echo "  WAL catch-up: $CATCH_ENTRIES entries applied to read-1"

# Verify data on read-1 after cold start
VERIFY=$(curl -s -X POST "$READ1_URL/xrpc/ai.gftd.yata.cypher" \
  -H 'Content-Type: application/json' -H 'X-Magatama-Verified: true' \
  -d '{"statement":"MATCH (r:LtNode) RETURN count(r) AS cnt LIMIT 1"}')
VERIFY_CNT=$(echo "$VERIFY" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['rows'][0][0] if d.get('rows') else 0)" 2>/dev/null || echo "0")
echo "  read-1 after cold start: $VERIFY_CNT LtNode vertices"
check "cold_start_recovery >= $((NUM_WRITES / 2))" "$VERIFY_CNT >= $((NUM_WRITES / 2))"

# ══════════════════════════════════════════════
# Phase 6: Mixed Read-Write (concurrent)
# ══════════════════════════════════════════════
echo ""
MIXED_N=$((NUM_READS / 5))
echo "--- Phase 6: Mixed Read-Write ($MIXED_N reads + $((MIXED_N / 3)) writes) ---"
MIXED_START=$(python3 -c "import time; print(int(time.time()*1000))")
MIXED_OPS=0

for i in $(seq 1 $MIXED_N); do
  # Read
  RK=$((RANDOM % NUM_WRITES + 1))
  curl -s -X POST "$READ0_URL/xrpc/ai.gftd.yata.cypher" \
    -H 'Content-Type: application/json' -H 'X-Magatama-Verified: true' \
    -d "{\"statement\":\"MATCH (r:LtNode) WHERE r.rkey = 'lt_$RK' RETURN r.text AS text LIMIT 1\"}" > /dev/null
  MIXED_OPS=$((MIXED_OPS + 1))

  # Write every 3rd iteration
  if [ $((i % 3)) -eq 0 ]; then
    WI=$((NUM_WRITES + i))
    curl -s -X POST "$WRITE_URL/xrpc/ai.gftd.yata.mergeRecordWal" \
      -H 'Content-Type: application/json' -H 'X-Magatama-Verified: true' \
      -d "{\"label\":\"LtNode\",\"pk_key\":\"rkey\",\"pk_value\":\"lt_$WI\",\"props\":{\"idx\":$WI,\"batch\":\"mixed\"}}" > /dev/null
    MIXED_OPS=$((MIXED_OPS + 1))
  fi
done

MIXED_END=$(python3 -c "import time; print(int(time.time()*1000))")
MIXED_TOTAL_MS=$((MIXED_END - MIXED_START))
MIXED_OPS_SEC=$(python3 -c "print(f'{$MIXED_OPS / ($MIXED_TOTAL_MS / 1000.0):.0f}')")
echo "Phase 6 results: ${MIXED_OPS} ops in ${MIXED_TOTAL_MS}ms (${MIXED_OPS_SEC} ops/s)"
check "mixed_ops >= 30/s" "$MIXED_OPS_SEC >= 30"

# ══════════════════════════════════════════════
# Phase 7: Prometheus Metrics
# ══════════════════════════════════════════════
echo ""
echo "--- Phase 7: Metrics ---"
METRICS=$(curl -sf "http://localhost:9090/metrics" 2>/dev/null || echo "")
if [ -n "$METRICS" ]; then
  echo "  Prometheus endpoint: OK"
  echo "$METRICS" | grep -c "^yata_" | xargs -I{} echo "  yata_* metrics: {} lines"
else
  echo "  Prometheus endpoint: not available (port 9090)"
fi

# ══════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════
echo ""
echo "============================================"
echo "  Load Test Summary"
echo "============================================"
echo "  Write throughput:    ${WRITE_OPS} ops/s"
echo "  Count QPS:           ${COUNT_QPS}"
echo "  Point read QPS:      ${POINT_QPS}"
echo "  2-hop QPS:           ${TWOHOP_QPS}"
echo "  Mixed ops/s:         ${MIXED_OPS_SEC}"
echo "  Checkpoint:          ${CP_MS}ms (${CP_V}v ${CP_E}e)"
echo "  Cold start:          ${COLD_MS}ms"
echo "  Flush bytes:         ${FLUSH_BYTES}"
echo "  ---"
echo "  PASS: $PASS  FAIL: $FAIL"
echo "============================================"

if [ "$FAIL" -gt 0 ]; then
  exit 1
fi
