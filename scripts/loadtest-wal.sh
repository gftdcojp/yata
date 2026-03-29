#!/usr/bin/env bash
# WAL Projection Load Test — Write + Read + Propagation
#
# Usage: ./scripts/loadtest-wal.sh [writes=500] [reads=100]
#
# Requires: docker compose -f docker-compose.wal.yml up -d --build

set -euo pipefail

WRITE_URL="http://localhost:8083"
READ0_URL="http://localhost:8090"
READ1_URL="http://localhost:8091"
NUM_WRITES=${1:-500}
NUM_READS=${2:-100}

echo "=== WAL Projection Load Test ==="
echo "Write: $WRITE_URL | Read: $READ0_URL, $READ1_URL"
echo "Writes: $NUM_WRITES | Reads: $NUM_READS"
echo ""

# Health check
for url in "$WRITE_URL" "$READ0_URL" "$READ1_URL"; do
  if ! curl -sf "$url/health" > /dev/null 2>&1; then
    echo "FAIL: $url not healthy"
    exit 1
  fi
done
echo "All containers healthy."
echo ""

# ── Phase 1: Sequential Write + WAL Push ──
echo "--- Phase 1: Write + WAL Push ($NUM_WRITES records) ---"
WRITE_START=$(date +%s%N)
WRITE_LATENCIES=""
APPLY_LATENCIES=""

for i in $(seq 1 $NUM_WRITES); do
  # Write to write container
  T0=$(date +%s%N)
  RESULT=$(curl -s -X POST "$WRITE_URL/xrpc/ai.gftd.yata.mergeRecordWal" \
    -H 'Content-Type: application/json' -H 'X-Magatama-Verified: true' \
    -d "{\"label\":\"LoadTest\",\"pk_key\":\"rkey\",\"pk_value\":\"lt_$i\",\"props\":{\"idx\":$i,\"batch\":\"load\",\"text\":\"record $i of $NUM_WRITES\"}}")
  T1=$(date +%s%N)
  WRITE_MS=$(( (T1 - T0) / 1000000 ))
  WRITE_LATENCIES="$WRITE_LATENCIES $WRITE_MS"

  # Push WAL entry to read replica
  ENTRY=$(echo "$RESULT" | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin).get('wal_entry',{})))" 2>/dev/null || echo "{}")
  if [ "$ENTRY" != "{}" ] && [ "$ENTRY" != "null" ]; then
    T2=$(date +%s%N)
    curl -s -X POST "$READ0_URL/xrpc/ai.gftd.yata.walApply" \
      -H 'Content-Type: application/json' \
      -d "{\"entries\":[$ENTRY]}" > /dev/null
    T3=$(date +%s%N)
    APPLY_MS=$(( (T3 - T2) / 1000000 ))
    APPLY_LATENCIES="$APPLY_LATENCIES $APPLY_MS"
  fi

  # Progress
  if [ $((i % 100)) -eq 0 ]; then
    echo "  $i/$NUM_WRITES written..."
  fi
done

WRITE_END=$(date +%s%N)
WRITE_TOTAL_MS=$(( (WRITE_END - WRITE_START) / 1000000 ))
WRITE_OPS=$(python3 -c "print(f'{$NUM_WRITES / ($WRITE_TOTAL_MS / 1000.0):.0f}')")

# Calculate percentiles
calc_stats() {
  local name="$1"
  shift
  python3 -c "
import sys
vals = sorted([int(x) for x in '$*'.split()])
if not vals:
    print(f'  {\"$name\":>8}: no data')
    sys.exit()
n = len(vals)
p50 = vals[n//2]
p95 = vals[int(n*0.95)]
p99 = vals[int(n*0.99)]
avg = sum(vals)/n
print(f'  {\"$name\":>8}: p50={p50}ms  p95={p95}ms  p99={p99}ms  avg={avg:.1f}ms  n={n}')
"
}

echo ""
echo "Write+Push results (total ${WRITE_TOTAL_MS}ms, ${WRITE_OPS} ops/s):"
calc_stats "Write" $WRITE_LATENCIES
calc_stats "Apply" $APPLY_LATENCIES

# ── Phase 2: Read from Read Replica ──
echo ""
echo "--- Phase 2: Read from Read Replica ($NUM_READS queries) ---"
READ_START=$(date +%s%N)
READ_LATENCIES=""

for i in $(seq 1 $NUM_READS); do
  T0=$(date +%s%N)
  curl -s -X POST "$READ0_URL/xrpc/ai.gftd.yata.cypher" \
    -H 'Content-Type: application/json' -H 'X-Magatama-Verified: true' \
    -d '{"statement":"MATCH (r:LoadTest) WHERE r.batch = '"'"'load'"'"' RETURN count(r) AS cnt"}' > /dev/null
  T1=$(date +%s%N)
  READ_MS=$(( (T1 - T0) / 1000000 ))
  READ_LATENCIES="$READ_LATENCIES $READ_MS"
done

READ_END=$(date +%s%N)
READ_TOTAL_MS=$(( (READ_END - READ_START) / 1000000 ))
READ_QPS=$(python3 -c "print(f'{$NUM_READS / ($READ_TOTAL_MS / 1000.0):.0f}')")

echo "Read results (total ${READ_TOTAL_MS}ms, ${READ_QPS} QPS):"
calc_stats "Read" $READ_LATENCIES

# ── Phase 3: Point Read (by rkey) ──
echo ""
echo "--- Phase 3: Point Read ($NUM_READS queries) ---"
POINT_LATENCIES=""
POINT_START=$(date +%s%N)

for i in $(seq 1 $NUM_READS); do
  RK=$((RANDOM % NUM_WRITES + 1))
  T0=$(date +%s%N)
  curl -s -X POST "$READ0_URL/xrpc/ai.gftd.yata.cypher" \
    -H 'Content-Type: application/json' -H 'X-Magatama-Verified: true' \
    -d "{\"statement\":\"MATCH (r:LoadTest) WHERE r.rkey = 'lt_$RK' RETURN r.text AS text LIMIT 1\"}" > /dev/null
  T1=$(date +%s%N)
  POINT_MS=$(( (T1 - T0) / 1000000 ))
  POINT_LATENCIES="$POINT_LATENCIES $POINT_MS"
done

POINT_END=$(date +%s%N)
POINT_TOTAL_MS=$(( (POINT_END - POINT_START) / 1000000 ))
POINT_QPS=$(python3 -c "print(f'{$NUM_READS / ($POINT_TOTAL_MS / 1000.0):.0f}')")

echo "Point read results (total ${POINT_TOTAL_MS}ms, ${POINT_QPS} QPS):"
calc_stats "PointRd" $POINT_LATENCIES

# ── Phase 4: WAL operations ──
echo ""
echo "--- Phase 4: WAL operations ---"

# WAL tail
T0=$(date +%s%N)
TAIL=$(curl -s -X POST "$WRITE_URL/xrpc/ai.gftd.yata.walTail" \
  -H 'Content-Type: application/json' -d '{"after_seq":0,"limit":10}')
T1=$(date +%s%N)
TAIL_MS=$(( (T1 - T0) / 1000000 ))
TAIL_COUNT=$(echo "$TAIL" | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))" 2>/dev/null || echo "?")
HEAD_SEQ=$(echo "$TAIL" | python3 -c "import sys,json; print(json.load(sys.stdin).get('head_seq',0))" 2>/dev/null || echo "?")
echo "  walTail: ${TAIL_MS}ms (count=$TAIL_COUNT, head_seq=$HEAD_SEQ)"

# WAL flush
T0=$(date +%s%N)
FLUSH=$(curl -s -X POST "$WRITE_URL/xrpc/ai.gftd.yata.walFlushSegment" \
  -H 'Content-Type: application/json' -d '{}')
T1=$(date +%s%N)
FLUSH_MS=$(( (T1 - T0) / 1000000 ))
FLUSH_BYTES=$(echo "$FLUSH" | python3 -c "import sys,json; print(json.load(sys.stdin).get('bytes',0))" 2>/dev/null || echo "?")
echo "  walFlush: ${FLUSH_MS}ms (bytes=$FLUSH_BYTES)"

# WAL checkpoint
T0=$(date +%s%N)
CP=$(curl -s -X POST "$WRITE_URL/xrpc/ai.gftd.yata.walCheckpoint" \
  -H 'Content-Type: application/json' -d '{}')
T1=$(date +%s%N)
CP_MS=$(( (T1 - T0) / 1000000 ))
CP_V=$(echo "$CP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('vertices',0))" 2>/dev/null || echo "?")
CP_E=$(echo "$CP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('edges',0))" 2>/dev/null || echo "?")
echo "  walCheckpoint: ${CP_MS}ms (${CP_V}v ${CP_E}e)"

# ── Summary ──
echo ""
echo "=== Summary ==="
echo "  Write throughput: ${WRITE_OPS} ops/s"
echo "  Read QPS (count): ${READ_QPS}"
echo "  Read QPS (point): ${POINT_QPS}"
echo "  Write→Read lag: ~p50(write) + p50(apply) ms"
