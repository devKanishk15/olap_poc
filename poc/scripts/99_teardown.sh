#!/usr/bin/env bash
# =============================================================================
# 99_teardown.sh — Stop and optionally wipe a specific engine or all engines
#
# Usage:
#   bash 99_teardown.sh --engine doris       # stop + clean Doris
#   bash 99_teardown.sh --engine duckdb      # stop + clean DuckDB
#   bash 99_teardown.sh --engine clickhouse  # stop + clean ClickHouse
#   bash 99_teardown.sh --all                # stop + clean all engines
#   bash 99_teardown.sh --all --wipe         # stop, clean, AND delete data dirs
# =============================================================================
set -euo pipefail

ENGINE=""
ALL=false
WIPE=false

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --engine) ENGINE="$2"; shift 2 ;;
    --all)    ALL=true; shift ;;
    --wipe)   WIPE=true; shift ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ "$ALL" == "false" && -z "$ENGINE" ]]; then
  echo "Usage: $0 --engine {doris|duckdb|clickhouse} | --all [--wipe]" >&2
  exit 1
fi

teardown_doris() {
  echo "--- Tearing down Apache Doris ---"
  docker compose -f /opt1/olap_poc/poc/docker/doris-compose.yml \
    down --remove-orphans --volumes 2>/dev/null || true
  if [[ "$WIPE" == "true" ]]; then
    rm -rf /opt1/olap_poc/doris/*
    echo "  /opt1/olap_poc/doris wiped."
  fi
  echo "  Doris stopped."
}

teardown_duckdb() {
  echo "--- Tearing down DuckDB ---"
  # DuckDB is in-process — kill any stray duckdb processes
  pkill -f "duckdb" 2>/dev/null || true
  if [[ "$WIPE" == "true" ]]; then
    rm -rf /opt1/olap_poc/duckdb/spill/*
    rm -f /opt1/olap_poc/duckdb/benchmark.duckdb /opt1/olap_poc/duckdb/benchmark.duckdb.wal
    echo "  DuckDB DB and spill wiped."
  fi
  echo "  DuckDB stopped."
}

teardown_clickhouse() {
  echo "--- Tearing down ClickHouse ---"
  docker compose -f /opt1/olap_poc/poc/docker/clickhouse-compose.yml \
    down --remove-orphans --volumes 2>/dev/null || true
  if [[ "$WIPE" == "true" ]]; then
    rm -rf /opt1/olap_poc/clickhouse/data/*
    rm -rf /opt1/olap_poc/clickhouse/logs/*
    rm -rf /opt1/olap_poc/clickhouse/tmp/*
    echo "  /opt1/olap_poc/clickhouse data/logs/tmp wiped."
  fi
  echo "  ClickHouse stopped."
}

free_ram_check() {
  echo ""
  echo "--- RAM after teardown ---"
  free -h
  echo ""
}

echo "================================================"
echo "  OLAP POC Teardown  |  $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "  Wipe data: $WIPE"
echo "================================================"

if [[ "$ALL" == "true" ]]; then
  teardown_doris
  teardown_duckdb
  teardown_clickhouse
else
  case "$ENGINE" in
    doris)      teardown_doris ;;
    duckdb)     teardown_duckdb ;;
    clickhouse) teardown_clickhouse ;;
    *) echo "Unknown engine: $ENGINE" >&2; exit 1 ;;
  esac
fi

free_ram_check

echo "================================================"
echo "  Teardown complete."
echo "================================================"
