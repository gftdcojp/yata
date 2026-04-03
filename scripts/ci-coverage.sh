#!/usr/bin/env bash
# yata CI: Coverage (unit + e2e with MinIO)
#
# Usage:
#   ./scripts/ci-coverage.sh              # full coverage (docker)
#   ./scripts/ci-coverage.sh --local      # local coverage (cargo-llvm-cov required)
#   ./scripts/ci-coverage.sh --e2e-only   # e2e coverage against running containers
#
# Output:
#   coverage/lcov.info    — lcov format (CI upload)
#   coverage/html/        — HTML report (local browse)

set -euo pipefail
cd "$(dirname "$0")/.."

MODE="${1:-docker}"
COVERAGE_DIR="$(pwd)/coverage"
mkdir -p "$COVERAGE_DIR"

# ── Docker mode: build + run in container ──
if [ "$MODE" = "docker" ] || [ "$MODE" = "--docker" ]; then
  echo "=== yata Coverage (Docker) ==="
  echo ""

  # Build coverage image (cached deps)
  docker compose -f docker-compose.ci.yml build coverage

  # Run coverage container (unit tests + MinIO integration)
  docker compose -f docker-compose.ci.yml run --rm \
    -v "$COVERAGE_DIR:/coverage" \
    coverage

  echo ""
  echo "=== Coverage Complete ==="
  echo "  lcov: $COVERAGE_DIR/lcov.info"
  echo "  html: $COVERAGE_DIR/html/index.html"
  [ -f "$COVERAGE_DIR/lcov.info" ] && wc -l "$COVERAGE_DIR/lcov.info" | awk '{print "  lines:", $1}'
  exit 0
fi

# ── Local mode: cargo-llvm-cov on host ──
if [ "$MODE" = "--local" ]; then
  echo "=== yata Coverage (Local) ==="
  echo ""

  if ! command -v cargo-llvm-cov &>/dev/null; then
    echo "ERROR: cargo-llvm-cov not found. Install: cargo install cargo-llvm-cov"
    exit 1
  fi

  cargo llvm-cov clean --workspace

  echo "--- Unit tests ---"
  cargo llvm-cov test --workspace \
    --exclude yata-bench \
    --no-fail-fast \
    -- --test-threads=4 2>&1

  echo ""
  echo "--- Generating reports ---"
  cargo llvm-cov report --lcov --output-path "$COVERAGE_DIR/lcov.info"
  cargo llvm-cov report --html --output-dir "$COVERAGE_DIR/html"

  echo ""
  echo "--- Summary ---"
  cargo llvm-cov report --summary-only

  echo ""
  echo "  lcov: $COVERAGE_DIR/lcov.info"
  echo "  html: $COVERAGE_DIR/html/index.html"
  exit 0
fi

# ── E2E-only mode: run e2e tests against live containers ──
if [ "$MODE" = "--e2e-only" ]; then
  echo "=== yata Coverage (E2E against live containers) ==="
  echo ""

  # Verify containers are up
  for port in 8083 8090 8091; do
    if ! curl -sf "http://localhost:$port/health" > /dev/null 2>&1; then
      echo "ERROR: Container on port $port not healthy."
      echo "Start containers: docker compose -f docker-compose.ci.yml up -d --build"
      exit 1
    fi
  done
  echo "All containers healthy."

  if command -v cargo-llvm-cov &>/dev/null; then
    echo "Running e2e with coverage..."
    cargo llvm-cov clean --workspace
    cargo llvm-cov test -p yata-server \
      --test e2e_wal_coverage \
      --test e2e_wal_projection \
      --no-fail-fast \
      --lcov --output-path "$COVERAGE_DIR/lcov-e2e.info" \
      -- --nocapture --test-threads=1

    cargo llvm-cov report --html --output-dir "$COVERAGE_DIR/html-e2e"
    cargo llvm-cov report --summary-only
    echo ""
    echo "  lcov: $COVERAGE_DIR/lcov-e2e.info"
    echo "  html: $COVERAGE_DIR/html-e2e/index.html"
  else
    echo "Running e2e without coverage (install cargo-llvm-cov for instrumented run)..."
    cargo test -p yata-server \
      --test e2e_wal_coverage \
      --test e2e_wal_projection \
      -- --nocapture --test-threads=1
  fi
  exit 0
fi

echo "Usage: $0 [--docker|--local|--e2e-only]"
exit 1
