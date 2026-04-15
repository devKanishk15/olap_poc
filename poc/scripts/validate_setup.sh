#!/usr/bin/env bash
# =============================================================================
# validate_setup.sh — Pre-benchmark sanity checks
#
# Run this before starting any benchmarks to verify that:
#   1. Hardware meets minimum spec
#   2. Docker images are pulled
#   3. .env is configured with non-placeholder values
#   4. Data files exist (or GCS bucket is reachable)
#   5. Python venv + dependencies are installed
#   6. One dry-run query executes successfully on each engine
#
# Usage:
#   bash scripts/validate_setup.sh [--engine doris|duckdb|clickhouse|all]
# =============================================================================
set -euo pipefail

ENGINE="${1:-all}"
PASS=0; FAIL=0; WARN=0
ENV_FILE="/opt1/olap_poc/poc/.env"

# Colours
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}✓${NC} $*"; ((PASS++)); }
fail() { echo -e "  ${RED}✗${NC} $*"; ((FAIL++)); }
warn() { echo -e "  ${YELLOW}!${NC} $*"; ((WARN++)); }

echo "=========================================="
echo "  OLAP POC — Setup Validation"
echo "  $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "=========================================="

# ---------------------------------------------------------------------------
# 1. Hardware
# ---------------------------------------------------------------------------
echo ""
echo "--- Hardware ---"

CPUS=$(nproc)
[[ "$CPUS" -ge 4 ]] && ok "$CPUS vCPUs" || warn "$CPUS vCPUs (expected 4+)"

MEM_GB=$(awk '/MemTotal/{printf "%d", $2/1024/1024}' /proc/meminfo)
[[ "$MEM_GB" -ge 7 ]] && ok "${MEM_GB} GB RAM" || fail "${MEM_GB} GB RAM (minimum 8 GB required)"

DISK_GB=$(df -BG /opt1 2>/dev/null | awk 'NR==2{gsub("G",""); print $4}' || echo 0)
[[ "$DISK_GB" -ge 40 ]] && ok "${DISK_GB} GB free on /opt1" || warn "${DISK_GB} GB free on /opt1 (recommend 60+)"

# ---------------------------------------------------------------------------
# 2. Kernel tunables
# ---------------------------------------------------------------------------
echo ""
echo "--- Kernel Tunables ---"

MAP_COUNT=$(sysctl -n vm.max_map_count 2>/dev/null || echo 0)
[[ "$MAP_COUNT" -ge 2000000 ]] && ok "vm.max_map_count=$MAP_COUNT" || \
    warn "vm.max_map_count=$MAP_COUNT (Doris needs 2000000 — run 00_vm_prep.sh)"

SWAP=$(sysctl -n vm.swappiness 2>/dev/null || echo 60)
[[ "$SWAP" -le 20 ]] && ok "vm.swappiness=$SWAP" || \
    warn "vm.swappiness=$SWAP (recommend ≤10 for OLAP workloads)"

# ---------------------------------------------------------------------------
# 3. .env configuration
# ---------------------------------------------------------------------------
echo ""
echo "--- .env Configuration ---"

if [[ ! -f "$ENV_FILE" ]]; then
    fail ".env not found at $ENV_FILE (copy .env.example and fill it in)"
else
    ok ".env found"
    source "$ENV_FILE" 2>/dev/null || true

    [[ "${GCS_BUCKET:-}" != "your-gcs-bucket-name" && -n "${GCS_BUCKET:-}" ]] && \
        ok "GCS_BUCKET=${GCS_BUCKET}" || warn "GCS_BUCKET not set or still placeholder"

    [[ "${GCS_HMAC_ACCESS_KEY:-}" != "GOOGXXXXXXXXXXXXXXXX" && -n "${GCS_HMAC_ACCESS_KEY:-}" ]] && \
        ok "GCS_HMAC_ACCESS_KEY set" || warn "GCS_HMAC_ACCESS_KEY not configured (needed for GCS mode)"

    [[ -n "${DORIS_VERSION:-}" ]]      && ok "DORIS_VERSION=${DORIS_VERSION}"      || warn "DORIS_VERSION not set"
    [[ -n "${DUCKDB_VERSION:-}" ]]     && ok "DUCKDB_VERSION=${DUCKDB_VERSION}"     || warn "DUCKDB_VERSION not set"
    [[ -n "${CLICKHOUSE_VERSION:-}" ]] && ok "CLICKHOUSE_VERSION=${CLICKHOUSE_VERSION}" || warn "CLICKHOUSE_VERSION not set"
fi

# ---------------------------------------------------------------------------
# 4. Docker
# ---------------------------------------------------------------------------
echo ""
echo "--- Docker ---"

if command -v docker &>/dev/null; then
    ok "Docker: $(docker --version | head -1)"
    docker compose version &>/dev/null && ok "Docker Compose plugin available" || \
        fail "Docker Compose plugin not found (install docker-compose-plugin)"
else
    fail "Docker not installed"
fi

# ---------------------------------------------------------------------------
# 5. Python environment
# ---------------------------------------------------------------------------
echo ""
echo "--- Python Environment ---"

VENV="/opt1/olap_poc/poc/.venv/bin/python"
if [[ -f "$VENV" ]]; then
    ok "venv: $($VENV --version)"
    for pkg in numpy pandas pyarrow duckdb requests; do
        $VENV -c "import $pkg" 2>/dev/null && ok "  $pkg" || warn "  $pkg not installed (run: pip install -r harness/requirements.txt)"
    done
else
    warn "venv not found at /opt1/olap_poc/poc/.venv (run: python3 -m venv /opt1/olap_poc/poc/.venv)"
fi

# ---------------------------------------------------------------------------
# 6. Data files
# ---------------------------------------------------------------------------
echo ""
echo "--- Data Files ---"

DATA_DIR="${DATA_DIR:-/opt1/data}"
if [[ -d "$DATA_DIR/event_fact" ]]; then
    PARQUET_COUNT=$(find "$DATA_DIR/event_fact" -name "*.parquet" | wc -l)
    [[ "$PARQUET_COUNT" -ge 1 ]] && ok "$PARQUET_COUNT Parquet partition files found" || \
        warn "No Parquet files — run: python data/generate_data.py"
else
    warn "Data directory not found ($DATA_DIR/event_fact) — run: python data/generate_data.py"
fi

# ---------------------------------------------------------------------------
# 7. Engine connectivity (quick ping)
# ---------------------------------------------------------------------------
echo ""
echo "--- Engine Connectivity ---"

check_doris() {
    local STATUS
    STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
        "http://${DORIS_HOST:-127.0.0.1}:${DORIS_FE_HTTP_PORT:-8030}/api/bootstrap" \
        --connect-timeout 3 2>/dev/null || echo "000")
    [[ "$STATUS" == "200" ]] && ok "Doris FE HTTP: OK" || warn "Doris FE not reachable (start: make install-doris)"
}

check_duckdb() {
    local DB="${DUCKDB_DB_PATH:-/opt1/duckdb/benchmark.duckdb}"
    if command -v duckdb &>/dev/null; then
        RESULT=$(duckdb "$DB" -c "SELECT 42 AS n" 2>/dev/null | grep -c "42" || echo 0)
        [[ "$RESULT" -ge 1 ]] && ok "DuckDB CLI: OK ($DB)" || warn "DuckDB CLI query failed"
    else
        warn "DuckDB CLI not found (run: make install-duckdb)"
    fi
}

check_clickhouse() {
    local STATUS
    STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
        "http://${CLICKHOUSE_HOST:-127.0.0.1}:${CLICKHOUSE_HTTP_PORT:-8123}/ping" \
        --connect-timeout 3 2>/dev/null || echo "000")
    [[ "$STATUS" == "200" ]] && ok "ClickHouse HTTP: OK" || warn "ClickHouse not reachable (start: make install-clickhouse)"
}

case "${ENGINE}" in
    doris)      check_doris ;;
    duckdb)     check_duckdb ;;
    clickhouse) check_clickhouse ;;
    all)        check_doris; check_duckdb; check_clickhouse ;;
    *)          warn "Unknown engine: $ENGINE" ;;
esac

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "=========================================="
echo -e "  ${GREEN}PASS${NC}: $PASS   ${YELLOW}WARN${NC}: $WARN   ${RED}FAIL${NC}: $FAIL"
echo "=========================================="

if [[ "$FAIL" -gt 0 ]]; then
    echo -e "  ${RED}Fix failures before running benchmarks.${NC}"
    exit 1
elif [[ "$WARN" -gt 0 ]]; then
    echo -e "  ${YELLOW}Warnings detected — review before running.${NC}"
    exit 0
else
    echo -e "  ${GREEN}All checks passed — ready to benchmark!${NC}"
    echo "  Next: make bench-all"
    exit 0
fi
