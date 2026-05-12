#!/usr/bin/env bash
# =============================================================================
# 04_install_trino.sh — Install and configure Trino 481 (single-node)
# Version: 481  |  RAM budget: 8 GB VM  |  JVM heap: 6 GB
# =============================================================================
set -euo pipefail
source /opt1/olap_poc/poc/.env 2>/dev/null || { echo "ERROR: /opt1/olap_poc/poc/.env not found."; exit 1; }

LOGFILE="/opt1/olap_poc/logs/trino_install.log"
exec > >(tee -a "$LOGFILE") 2>&1

TRINO_VERSION="${TRINO_VERSION:-481}"
TRINO_IMAGE="${TRINO_IMAGE:-trinodb/trino:${TRINO_VERSION}}"
COMPOSE_FILE="/opt1/olap_poc/poc/docker/trino-compose.yml"
TEMPLATE_DIR="/opt1/olap_poc/poc/docker/trino-etc"
ETC_DIR="/opt1/olap_poc/trino/etc"

echo "================================================"
echo "  Installing Trino ${TRINO_VERSION}"
echo "  $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "================================================"

# ---------------------------------------------------------------------------
# 1. Stop any existing Trino containers
# ---------------------------------------------------------------------------
echo ""
echo "--- Stopping any existing Trino containers ---"
docker compose -f "$COMPOSE_FILE" down --remove-orphans 2>/dev/null || true

# ---------------------------------------------------------------------------
# 2. Create Trino directories
# ---------------------------------------------------------------------------
echo ""
echo "--- Creating Trino directories ---"
mkdir -p "${ETC_DIR}/catalog"

# Metastore dirs live inside /data/trino (mapped to /opt1/olap_poc/trino/data
# inside the container). A single volume mount covers everything so there are
# no separate bind-mount permission surprises.
# Always wipe metastore on a fresh install to clear stale .trinoSchema files.
rm -rf /opt1/olap_poc/trino/data/metastore
rm -rf /opt1/olap_poc/trino/data/gcs_metastore
mkdir -p /opt1/olap_poc/trino/data/spill
mkdir -p /opt1/olap_poc/trino/data/metastore
mkdir -p /opt1/olap_poc/trino/data/gcs_metastore

# Trino runs as uid 1000 inside the container — own everything under /data/trino
chown -R 1000:1000 /opt1/olap_poc/trino/data
chmod -R 755       /opt1/olap_poc/trino/data
echo "  Directories created."

# ---------------------------------------------------------------------------
# 3. Write Trino config files from templates
#    Templates live in docker/trino-etc/ and are copied verbatim; the
#    gcs_hive.properties file has ${ENV:...} tokens replaced with real values.
# ---------------------------------------------------------------------------
echo ""
echo "--- Writing Trino config files ---"

cp "${TEMPLATE_DIR}/config.properties" "${ETC_DIR}/config.properties"
cp "${TEMPLATE_DIR}/jvm.config"        "${ETC_DIR}/jvm.config"
cp "${TEMPLATE_DIR}/node.properties"   "${ETC_DIR}/node.properties"
cp "${TEMPLATE_DIR}/catalog/local_hive.properties" "${ETC_DIR}/catalog/local_hive.properties"

# Substitute real values into gcs_hive.properties (replaces ${ENV:VAR} tokens)
GCS_S3_ENDPOINT="${GCS_S3_ENDPOINT:-https://storage.googleapis.com}"
sed \
  -e "s|\${ENV:GCS_S3_ENDPOINT}|${GCS_S3_ENDPOINT}|g" \
  -e "s|\${ENV:GCS_HMAC_ACCESS_KEY}|${GCS_HMAC_ACCESS_KEY}|g" \
  -e "s|\${ENV:GCS_HMAC_SECRET}|${GCS_HMAC_SECRET}|g" \
  "${TEMPLATE_DIR}/catalog/gcs_hive.properties" \
  > "${ETC_DIR}/catalog/gcs_hive.properties"

# Protect the catalog file (contains HMAC secret); chown so uid 1000 (trino)
# can read it inside the container.
chmod 600 "${ETC_DIR}/catalog/gcs_hive.properties"
chown 1000:1000 "${ETC_DIR}/catalog/gcs_hive.properties"

echo "  Config files written to ${ETC_DIR}"

# ---------------------------------------------------------------------------
# 4. Pull image and start Trino
# ---------------------------------------------------------------------------
echo ""
echo "--- Pulling and starting Trino ---"
docker pull "${TRINO_IMAGE}"

# Export .env variables so docker compose can substitute them
set -a
# shellcheck source=/dev/null
source /opt1/olap_poc/poc/.env
set +a

docker compose -f "$COMPOSE_FILE" up -d

# ---------------------------------------------------------------------------
# 5. Wait for Trino to become ready (JVM startup takes ~20–30s)
# ---------------------------------------------------------------------------
echo ""
echo "--- Waiting for Trino to be fully ready (up to 180s) ---"
# Phase 1: wait for HTTP port to respond
for i in $(seq 1 36); do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    "http://${TRINO_HOST:-127.0.0.1}:${TRINO_PORT:-8080}/v1/info" 2>/dev/null || echo "000")
  if [[ "$STATUS" == "200" ]]; then
    echo "  HTTP interface up after $((i * 5))s"
    break
  fi
  echo "  Waiting for HTTP... attempt $i/36 (status=$STATUS)"
  sleep 5
  if [[ "$i" -eq 36 ]]; then
    echo "ERROR: Trino did not start in 180s." >&2
    docker compose -f "$COMPOSE_FILE" logs --tail=80 >&2
    exit 1
  fi
done

# Phase 2: wait for starting=false (engine fully initialised)
echo "  Waiting for Trino engine to finish initialising..."
for i in $(seq 1 24); do
  STARTING=$(curl -s \
    "http://${TRINO_HOST:-127.0.0.1}:${TRINO_PORT:-8080}/v1/info" 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('starting','true'))" 2>/dev/null \
    || echo "true")
  if [[ "$STARTING" == "False" || "$STARTING" == "false" ]]; then
    echo "  Trino fully ready."
    break
  fi
  echo "  Still initialising... attempt $i/24"
  sleep 5
  if [[ "$i" -eq 24 ]]; then
    echo "ERROR: Trino engine did not finish initialising in 120s." >&2
    docker compose -f "$COMPOSE_FILE" logs --tail=80 >&2
    exit 1
  fi
done

# ---------------------------------------------------------------------------
# 6. Create schemas + register external tables (local_hive catalog)
# ---------------------------------------------------------------------------
echo ""
echo "--- Creating schema and registering local Parquet table ---"
TRINO_HOST="${TRINO_HOST:-127.0.0.1}"
TRINO_PORT="${TRINO_PORT:-8080}"
TRINO_USER="${TRINO_USER:-trino}"

_trino_exec() {
  docker exec trino trino \
    --server "http://localhost:8080" \
    --user "${TRINO_USER}" \
    --catalog "$1" \
    --schema "$2" \
    --execute "$3"
}

# Create the poc schema in local_hive
_trino_exec "local_hive" "default" "CREATE SCHEMA IF NOT EXISTS local_hive.poc"

# Register the event_fact external Parquet table
docker exec trino trino \
  --server "http://localhost:8080" \
  --user "${TRINO_USER}" \
  --file "/etc/trino/../../../opt1/olap_poc/poc/schema/trino_local_ddl.sql" 2>/dev/null || \
docker exec -i trino trino \
  --server "http://localhost:8080" \
  --user "${TRINO_USER}" \
  --catalog "local_hive" \
  --schema "poc" < /opt1/olap_poc/poc/schema/trino_local_ddl.sql

echo "  Local schema registered."

# ---------------------------------------------------------------------------
# 7. Create schemas + register GCS tables (gcs_hive catalog)
# ---------------------------------------------------------------------------
echo ""
echo "--- Creating schema and registering GCS tables ---"

_trino_exec "gcs_hive" "default" "CREATE SCHEMA IF NOT EXISTS gcs_hive.poc"

sed \
  -e "s|<GCS_BUCKET>|${GCS_BUCKET}|g" \
  -e "s|<GCS_BUCKET_PREFIX>|${GCS_BUCKET_PREFIX}|g" \
  -e "s|<GCS_GLUSR_PREMIUM_LISTING_PREFIX>|${GCS_GLUSR_PREMIUM_LISTING_PREFIX:-}|g" \
  /opt1/olap_poc/poc/schema/trino_gcs_ddl.sql \
  | docker exec -i trino trino \
      --server "http://localhost:8080" \
      --user "${TRINO_USER}" \
      --catalog "gcs_hive" \
      --schema "poc"

echo "  GCS schema registered."

# ---------------------------------------------------------------------------
# 8. Health check — verify both catalogs
# ---------------------------------------------------------------------------
echo ""
echo "--- Health Check ---"
_trino_exec "local_hive" "poc" "SELECT 'Trino local OK' AS status, count(*) AS row_count FROM poc.event_fact"
_trino_exec "gcs_hive"   "poc" "SELECT 'Trino GCS OK'   AS status, count(*) AS row_count FROM poc.event_fact"

echo ""
echo "================================================"
echo "  Trino ${TRINO_VERSION} ready."
echo ""
echo "  HTTP / Web UI : http://${TRINO_HOST}:${TRINO_PORT}"
echo "  Catalogs      : local_hive (local Parquet), gcs_hive (GCS)"
echo "  Schema        : poc | Table: event_fact"
echo "  Mem limit     : 6 GB JVM heap, 5 GB per-query"
echo "  Spill dir     : /opt1/olap_poc/trino/data/spill"
echo ""
echo "  Next: make bench-trino-local"
echo "  Teardown: bash scripts/99_teardown.sh --engine trino"
echo "================================================"
