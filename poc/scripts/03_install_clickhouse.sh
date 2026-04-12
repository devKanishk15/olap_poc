#!/usr/bin/env bash
# =============================================================================
# 03_install_clickhouse.sh  — Install and configure ClickHouse (single-node)
# Version: 24.12  |  RAM budget: 8 GB
# =============================================================================
set -euo pipefail
source /opt1/poc/.env 2>/dev/null || { echo "ERROR: /opt1/poc/.env not found."; exit 1; }

LOGFILE="/opt1/logs/clickhouse_install.log"
exec > >(tee -a "$LOGFILE") 2>&1

echo "================================================"
echo "  Installing ClickHouse ${CLICKHOUSE_VERSION}"
echo "  $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "================================================"

COMPOSE_FILE="/opt1/poc/docker/clickhouse-compose.yml"

# ---------------------------------------------------------------------------
# 1. Stop any existing ClickHouse containers
# ---------------------------------------------------------------------------
echo ""
echo "--- Stopping any existing ClickHouse containers ---"
docker compose -f "$COMPOSE_FILE" down --remove-orphans 2>/dev/null || true

# ---------------------------------------------------------------------------
# 2. Create ClickHouse data directories
# ---------------------------------------------------------------------------
echo ""
echo "--- Creating ClickHouse directories ---"
mkdir -p /opt1/clickhouse/data
mkdir -p /opt1/clickhouse/logs
mkdir -p /opt1/clickhouse/tmp
mkdir -p /opt1/clickhouse/user_files
echo "  Directories created."

# ---------------------------------------------------------------------------
# 3. Write ClickHouse custom config (memory limits)
# ---------------------------------------------------------------------------
echo ""
echo "--- Writing ClickHouse memory config ---"
mkdir -p /opt1/clickhouse/config.d

cat > /opt1/clickhouse/config.d/memory_limits.xml << 'XML'
<clickhouse>
    <!-- Hard cap: 6.5 GB — leaves ~1.5 GB for OS + Docker overhead -->
    <max_server_memory_usage>6979318784</max_server_memory_usage>

    <!-- Allow up to 90% of server memory per query before spilling -->
    <max_server_memory_usage_to_ram_ratio>0.9</max_server_memory_usage_to_ram_ratio>

    <!-- Spill directory on SSD -->
    <tmp_path>/opt1/clickhouse/tmp/</tmp_path>

    <!-- Log slow queries for analysis -->
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    </query_log>

    <!-- Mark cache: 512 MB (important for cold/warm distinction) -->
    <mark_cache_size>536870912</mark_cache_size>

    <!-- Uncompressed cache: 1 GB -->
    <uncompressed_cache_size>1073741824</uncompressed_cache_size>

    <!-- Background merge pool threads -->
    <background_pool_size>4</background_pool_size>
</clickhouse>
XML

cat > /opt1/clickhouse/config.d/storage_paths.xml << 'XML'
<clickhouse>
    <path>/opt1/clickhouse/data/</path>
    <tmp_path>/opt1/clickhouse/tmp/</tmp_path>
    <user_files_path>/opt1/clickhouse/user_files/</user_files_path>
</clickhouse>
XML

echo "  Config files written."

# ---------------------------------------------------------------------------
# 4. Pull image and start ClickHouse
# ---------------------------------------------------------------------------
echo ""
echo "--- Pulling and starting ClickHouse ---"
docker pull "${CLICKHOUSE_IMAGE}"
docker compose -f "$COMPOSE_FILE" up -d

# ---------------------------------------------------------------------------
# 5. Wait for ClickHouse to be ready
# ---------------------------------------------------------------------------
echo ""
echo "--- Waiting for ClickHouse HTTP interface (up to 60s) ---"
for i in $(seq 1 12); do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_HTTP_PORT}/ping" 2>/dev/null || echo "000")
  if [[ "$STATUS" == "200" ]]; then
    echo "  ClickHouse ready after $((i * 5))s"
    break
  fi
  echo "  Waiting... attempt $i/12 (status=$STATUS)"
  sleep 5
  if [[ "$i" -eq 12 ]]; then
    echo "ERROR: ClickHouse did not start in 60s." >&2
    docker compose -f "$COMPOSE_FILE" logs --tail=50 >&2
    exit 1
  fi
done

# ---------------------------------------------------------------------------
# 6. Create POC database
# ---------------------------------------------------------------------------
echo ""
echo "--- Creating POC database ---"
curl -s "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_HTTP_PORT}/" \
  --data "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DATABASE};" \
  -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}"

echo "  Database '${CLICKHOUSE_DATABASE}' created."

# ---------------------------------------------------------------------------
# 7. Set per-user memory limits
# ---------------------------------------------------------------------------
echo ""
echo "--- Configuring per-user query limits ---"
curl -s "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_HTTP_PORT}/" \
  --data "ALTER USER ${CLICKHOUSE_USER} SETTINGS max_memory_usage = 6000000000, max_bytes_before_external_group_by = 3000000000, max_bytes_before_external_sort = 3000000000;" \
  -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" || echo "  NOTE: User settings may require admin privileges."

# ---------------------------------------------------------------------------
# 8. Health check
# ---------------------------------------------------------------------------
echo ""
echo "--- Health Check ---"
curl -s "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_HTTP_PORT}/" \
  --data "SELECT 'ClickHouse OK' AS status, version() AS version, getSetting('max_memory_usage') AS mem_limit FORMAT Pretty;" \
  -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}"

echo ""
echo "================================================"
echo "  ClickHouse ${CLICKHOUSE_VERSION} ready."
echo ""
echo "  HTTP     : http://${CLICKHOUSE_HOST}:${CLICKHOUSE_HTTP_PORT}"
echo "  Native   : ${CLICKHOUSE_HOST}:${CLICKHOUSE_NATIVE_PORT}"
echo "  Database : ${CLICKHOUSE_DATABASE}"
echo "  Mem limit: 6.5 GB"
echo "  Tmp dir  : /opt1/clickhouse/tmp"
echo ""
echo "  Teardown: bash scripts/99_teardown.sh --engine clickhouse"
echo "================================================"
