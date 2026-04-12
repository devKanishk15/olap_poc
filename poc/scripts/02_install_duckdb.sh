#!/usr/bin/env bash
# =============================================================================
# 02_install_duckdb.sh  — Install and configure DuckDB (CLI + Python)
# Version: 1.2.1  |  RAM budget: 8 GB (configured via PRAGMA)
# DuckDB is an in-process engine — no daemon, no Docker needed.
# =============================================================================
set -euo pipefail
source /opt1/olap_poc/poc/.env 2>/dev/null || { echo "ERROR: /opt1/olap_poc/poc/.env not found."; exit 1; }

LOGFILE="/opt1/logs/duckdb_install.log"
exec > >(tee -a "$LOGFILE") 2>&1

echo "================================================"
echo "  Installing DuckDB ${DUCKDB_VERSION}"
echo "  $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "================================================"

DUCKDB_BIN="/usr/local/bin/duckdb"
DUCKDB_INSTALL_DIR="/opt1/duckdb"

# ---------------------------------------------------------------------------
# 1. Download DuckDB CLI binary
# ---------------------------------------------------------------------------
echo ""
echo "--- Downloading DuckDB CLI ---"

TMP_ZIP="/tmp/duckdb_cli.zip"
curl -fsSL "${DUCKDB_BINARY_URL}" -o "$TMP_ZIP"
unzip -o "$TMP_ZIP" -d /tmp/duckdb_extract/
install -m 755 /tmp/duckdb_extract/duckdb "$DUCKDB_BIN"
rm -rf "$TMP_ZIP" /tmp/duckdb_extract/

echo "  DuckDB CLI installed at $DUCKDB_BIN"
duckdb --version

# ---------------------------------------------------------------------------
# 2. Install DuckDB Python package
# ---------------------------------------------------------------------------
echo ""
echo "--- Installing DuckDB Python package ---"
/opt1/poc/.venv/bin/pip install "duckdb==${DUCKDB_VERSION}" -q
echo "  DuckDB Python package installed."

# ---------------------------------------------------------------------------
# 3. Create DuckDB database and apply memory/storage configuration
# ---------------------------------------------------------------------------
echo ""
echo "--- Configuring DuckDB (memory + spill) ---"

mkdir -p "${DUCKDB_INSTALL_DIR}/spill"
mkdir -p "${DUCKDB_INSTALL_DIR}/extensions"

# Create a startup SQL file applied before every benchmark session
cat > "${DUCKDB_INSTALL_DIR}/startup.sql" << SQL
-- Memory budget: 6 GB (leave 2 GB for OS + Python overhead)
SET memory_limit = '6GB';

-- Spill-to-disk directory on SSD
SET temp_directory = '/opt1/duckdb/spill';

-- Threads: match vCPU count
SET threads = 4;

-- Enable progress bar for long queries
SET enable_progress_bar = true;

-- Disable object cache cross-query (for fair cold-run measurements)
-- SET enable_object_cache = false;

-- HTTP extensions (for GCS/S3 access)
INSTALL httpfs;
LOAD httpfs;

-- Configure S3-compatible endpoint for GCS
SET s3_endpoint = 'storage.googleapis.com';
SQL

echo "  Startup SQL written to ${DUCKDB_INSTALL_DIR}/startup.sql"

# ---------------------------------------------------------------------------
# 4. Install DuckDB extensions
# ---------------------------------------------------------------------------
echo ""
echo "--- Installing DuckDB extensions ---"

duckdb "${DUCKDB_DB_PATH}" << 'DUCK'
INSTALL httpfs;
INSTALL parquet;
INSTALL json;
INSTALL icu;
LOAD httpfs;
LOAD parquet;
LOAD json;
LOAD icu;
SELECT extension_name, loaded, installed FROM duckdb_extensions() WHERE installed = TRUE;
DUCK

echo "  Extensions installed."

# ---------------------------------------------------------------------------
# 5. Health check
# ---------------------------------------------------------------------------
echo ""
echo "--- Health Check ---"
duckdb "${DUCKDB_DB_PATH}" \
  -c "SELECT 'DuckDB OK' AS status, version() AS version, current_setting('memory_limit') AS mem_limit;"

echo ""
echo "================================================"
echo "  DuckDB ${DUCKDB_VERSION} ready."
echo ""
echo "  CLI binary : $DUCKDB_BIN"
echo "  DB file    : ${DUCKDB_DB_PATH}"
echo "  Memory cap : 6 GB"
echo "  Spill dir  : /opt1/duckdb/spill"
echo ""
echo "  Teardown: bash scripts/99_teardown.sh --engine duckdb"
echo "================================================"
