# =============================================================================
# OLAP POC — Makefile
# Convenience wrapper for all common POC operations.
# All targets assume the VM is set up (00_vm_prep.sh has been run)
# and /opt1/poc/.env is configured.
#
# Usage:  make <target>
# =============================================================================

SHELL       := /bin/bash
PYTHON      := /opt1/poc/.venv/bin/python
ENV_FILE    := /opt1/poc/.env
RESULTS_DIR := /opt1/poc/results
REPORT_DIR  := /opt1/poc/report

.PHONY: help prep data install-doris install-duckdb install-clickhouse \
        bench-doris-local bench-doris-gcs \
        bench-duckdb-local bench-duckdb-gcs \
        bench-clickhouse-local bench-clickhouse-gcs \
        bench-all analyse teardown-doris teardown-duckdb teardown-clickhouse \
        teardown-all clean-results

# ---------------------------------------------------------------------------
# Default target
# ---------------------------------------------------------------------------
help:
	@echo ""
	@echo "  OLAP POC — Available Targets"
	@echo "  =============================="
	@echo ""
	@echo "  Setup"
	@echo "    make prep               Run VM preparation (kernel tunables, Docker, dirs)"
	@echo "    make data               Generate 10M-row Parquet dataset + upload to GCS"
	@echo ""
	@echo "  Install (one engine at a time)"
	@echo "    make install-doris      Install and start Apache Doris"
	@echo "    make install-duckdb     Install DuckDB CLI + Python package"
	@echo "    make install-clickhouse Install and start ClickHouse"
	@echo ""
	@echo "  Benchmark (stop other engines first)"
	@echo "    make bench-doris-local        Read + write benchmarks — local mode"
	@echo "    make bench-doris-gcs          Read + write benchmarks — GCS mode"
	@echo "    make bench-duckdb-local"
	@echo "    make bench-duckdb-gcs"
	@echo "    make bench-clickhouse-local"
	@echo "    make bench-clickhouse-gcs"
	@echo "    make bench-all                Full sequential run (local only, all engines)"
	@echo ""
	@echo "  Analysis"
	@echo "    make analyse            Aggregate results → CSV + charts + summary table"
	@echo ""
	@echo "  Teardown"
	@echo "    make teardown-doris     Stop Doris containers"
	@echo "    make teardown-duckdb    Kill DuckDB processes"
	@echo "    make teardown-clickhouse Stop ClickHouse container"
	@echo "    make teardown-all       Stop all engines"
	@echo ""
	@echo "  Cleanup"
	@echo "    make clean-results      Delete all JSONL result files"
	@echo ""

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
prep:
	@echo "--- VM Preparation ---"
	sudo bash /opt1/poc/scripts/00_vm_prep.sh

data:
	@echo "--- Generating synthetic dataset (10M rows) ---"
	$(PYTHON) /opt1/poc/data/generate_data.py \
		--rows  $$(grep DATA_ROW_COUNT $(ENV_FILE) | cut -d= -f2 || echo 10000000) \
		--seed  $$(grep DATA_SEED      $(ENV_FILE) | cut -d= -f2 || echo 42) \
		--out   $$(grep DATA_DIR       $(ENV_FILE) | cut -d= -f2 || echo /opt1/data)
	@echo "--- Uploading to GCS ---"
	$(PYTHON) /opt1/poc/data/upload_to_gcs.py

# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------
install-doris:
	@echo "--- Installing Apache Doris ---"
	bash /opt1/poc/scripts/01_install_doris.sh

install-duckdb:
	@echo "--- Installing DuckDB ---"
	bash /opt1/poc/scripts/02_install_duckdb.sh
	$(PYTHON) /opt1/poc/schema/duckdb_ddl.sql 2>/dev/null || \
	    duckdb $$(grep DUCKDB_DB_PATH $(ENV_FILE) | cut -d= -f2) \
	        < /opt1/poc/schema/duckdb_ddl.sql

install-clickhouse:
	@echo "--- Installing ClickHouse ---"
	bash /opt1/poc/scripts/03_install_clickhouse.sh

# ---------------------------------------------------------------------------
# Schema creation (run after engine is up)
# ---------------------------------------------------------------------------
schema-doris:
	mysql -h $$(grep DORIS_HOST $(ENV_FILE) | cut -d= -f2 || echo 127.0.0.1) \
	      -P $$(grep DORIS_FE_QUERY_PORT $(ENV_FILE) | cut -d= -f2 || echo 9030) \
	      -u $$(grep DORIS_USER $(ENV_FILE) | cut -d= -f2 || echo root) \
	      < /opt1/poc/schema/doris_ddl.sql

schema-duckdb:
	duckdb $$(grep DUCKDB_DB_PATH $(ENV_FILE) | cut -d= -f2 || echo /opt1/duckdb/benchmark.duckdb) \
	    < /opt1/poc/schema/duckdb_ddl.sql

schema-clickhouse:
	curl -s "http://127.0.0.1:8123/" \
	    --data-binary @/opt1/poc/schema/clickhouse_ddl.sql

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------
bench-doris-local:
	@echo "--- Benchmarking Doris (local) ---"
	$(PYTHON) /opt1/poc/harness/run_benchmark.py --engine doris --mode local

bench-doris-gcs:
	@echo "--- Benchmarking Doris (GCS) ---"
	$(PYTHON) /opt1/poc/harness/run_benchmark.py --engine doris --mode gcs

bench-duckdb-local:
	@echo "--- Benchmarking DuckDB (local) ---"
	$(PYTHON) /opt1/poc/harness/run_benchmark.py --engine duckdb --mode local

bench-duckdb-gcs:
	@echo "--- Benchmarking DuckDB (GCS) ---"
	$(PYTHON) /opt1/poc/harness/run_benchmark.py --engine duckdb --mode gcs

bench-clickhouse-local:
	@echo "--- Benchmarking ClickHouse (local) ---"
	$(PYTHON) /opt1/poc/harness/run_benchmark.py --engine clickhouse --mode local

bench-clickhouse-gcs:
	@echo "--- Benchmarking ClickHouse (GCS) ---"
	$(PYTHON) /opt1/poc/harness/run_benchmark.py --engine clickhouse --mode gcs

# Run all three engines sequentially (local only) — stops each before starting next
bench-all:
	@echo "========================================"
	@echo "  Full Sequential Benchmark (local)"
	@echo "========================================"
	@echo ""
	@echo "=== Phase 1: Doris ==="
	$(MAKE) install-doris
	$(MAKE) schema-doris
	$(MAKE) bench-doris-local
	$(MAKE) teardown-doris
	sleep 10
	@echo ""
	@echo "=== Phase 2: DuckDB ==="
	$(MAKE) install-duckdb
	$(MAKE) schema-duckdb
	$(MAKE) bench-duckdb-local
	$(MAKE) teardown-duckdb
	sleep 5
	@echo ""
	@echo "=== Phase 3: ClickHouse ==="
	$(MAKE) install-clickhouse
	$(MAKE) schema-clickhouse
	$(MAKE) bench-clickhouse-local
	$(MAKE) teardown-clickhouse
	@echo ""
	@echo "=== All benchmarks complete — running analysis ==="
	$(MAKE) analyse

# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------
analyse:
	@echo "--- Analysing results ---"
	$(PYTHON) /opt1/poc/report/analyse_results.py \
		--results $(RESULTS_DIR) \
		--out     $(REPORT_DIR)
	@echo ""
	@echo "  Open: $(REPORT_DIR)/02_summary_table.md"
	@echo "  Open: $(REPORT_DIR)/04_final_report.md"

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
teardown-doris:
	bash /opt1/poc/scripts/99_teardown.sh --engine doris

teardown-duckdb:
	bash /opt1/poc/scripts/99_teardown.sh --engine duckdb

teardown-clickhouse:
	bash /opt1/poc/scripts/99_teardown.sh --engine clickhouse

teardown-all:
	bash /opt1/poc/scripts/99_teardown.sh --all

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
clean-results:
	@echo "Deleting all result files from $(RESULTS_DIR)..."
	rm -f $(RESULTS_DIR)/*.jsonl
	@echo "Done."

# ---------------------------------------------------------------------------
# Health checks
# ---------------------------------------------------------------------------
status:
	@echo "--- Docker containers ---"
	docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || true
	@echo ""
	@echo "--- Memory ---"
	free -h
	@echo ""
	@echo "--- Disk /opt1 ---"
	df -h /opt1 2>/dev/null || true
