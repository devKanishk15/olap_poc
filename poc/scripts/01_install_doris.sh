#!/usr/bin/env bash
# =============================================================================
# 01_install_doris.sh  — Install and configure Apache Doris (single-node)
# Version: 2.1.7  |  RAM budget: 8 GB
# Must be run AFTER 00_vm_prep.sh
# =============================================================================
set -euo pipefail
source /opt1/olap_poc/poc/.env 2>/dev/null || { echo "ERROR: /opt1/olap_poc/poc/.env not found. Copy .env.example and fill it in."; exit 1; }

LOGFILE="/opt1/olap_poc/logs/doris_install.log"
exec > >(tee -a "$LOGFILE") 2>&1

echo "================================================"
echo "  Installing Apache Doris ${DORIS_VERSION}"
echo "  $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "================================================"

COMPOSE_FILE="/opt1/olap_poc/poc/docker/doris-compose.yml"

# ---------------------------------------------------------------------------
# 1. Stop any existing Doris containers
# ---------------------------------------------------------------------------
echo ""
echo "--- Stopping any existing Doris containers ---"
docker compose -f "$COMPOSE_FILE" down --remove-orphans 2>/dev/null || true

# ---------------------------------------------------------------------------
# 2. Create Doris data directories
# ---------------------------------------------------------------------------
echo ""
echo "--- Creating Doris data directories (purging stale BDBJE metadata) ---"
# Wipe FE meta so BDBJE always starts a fresh single-node election group.
# Without this, leftover journal files from a prior run cause FE to stay
# in UNKNOWN state indefinitely (it can never reach quorum alone).
rm -rf /opt1/olap_poc/doris/fe/meta
mkdir -p /opt1/olap_poc/doris/fe/meta
mkdir -p /opt1/olap_poc/doris/be/storage
mkdir -p /opt1/olap_poc/doris/be/log
mkdir -p /opt1/olap_poc/doris/fe/log
echo "  Directories created."

# ---------------------------------------------------------------------------
# 3. Pull images
# ---------------------------------------------------------------------------
echo ""
echo "--- Pulling Docker images ---"
docker pull "${DORIS_FE_IMAGE}"
docker pull "${DORIS_BE_IMAGE}"

# ---------------------------------------------------------------------------
# 4. Start Doris via Docker Compose
# ---------------------------------------------------------------------------
echo ""
echo "--- Starting Doris (FE + BE) ---"
docker compose -f "$COMPOSE_FILE" up -d

# ---------------------------------------------------------------------------
# 5. Wait for FE to be healthy
# ---------------------------------------------------------------------------
echo ""
echo "--- Waiting for Doris FE to become ready (up to 120s) ---"
for i in $(seq 1 24); do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    "http://${DORIS_HOST}:${DORIS_FE_HTTP_PORT}/api/bootstrap" 2>/dev/null || echo "000")
  if [[ "$STATUS" == "200" ]]; then
    echo "  FE is ready (HTTP 200) after $((i * 5))s"
    break
  fi
  echo "  Waiting... attempt $i/24 (status=$STATUS)"
  sleep 5
  if [[ "$i" -eq 24 ]]; then
    echo "ERROR: FE did not become ready in 120s. Check logs:" >&2
    docker compose -f "$COMPOSE_FILE" logs --tail=50 fe >&2
    exit 1
  fi
done

# ---------------------------------------------------------------------------
# 6. Register BE with FE
# ---------------------------------------------------------------------------
echo ""
echo "--- Registering BE with FE ---"
sleep 5   # give BE time to finish init

mysql -h "${DORIS_HOST}" -P "${DORIS_FE_QUERY_PORT}" \
  -u "${DORIS_USER}" --password="${DORIS_PASSWORD}" \
  --connect-timeout=30 \
  -e "ALTER SYSTEM ADD BACKEND '127.0.0.1:9050';" 2>/dev/null || \
  echo "  NOTE: BE may already be registered (safe to ignore duplicate error)."

# ---------------------------------------------------------------------------
# 7. Wait for BE to register
# ---------------------------------------------------------------------------
echo ""
echo "--- Waiting for BE to register (up to 60s) ---"
for i in $(seq 1 12); do
  if mysql -h "${DORIS_HOST}" -P "${DORIS_FE_QUERY_PORT}" \
      -u "${DORIS_USER}" --password="${DORIS_PASSWORD}" \
      --connect-timeout=10 -N -B \
      -e "SHOW BACKENDS\G" 2>/dev/null | grep -q "Alive: true"; then
    echo "  BE is alive after $((i * 5))s"
    break
  fi
  echo "  Waiting for BE... attempt $i/12"
  sleep 5
done

# ---------------------------------------------------------------------------
# 8. Create POC database
# ---------------------------------------------------------------------------
echo ""
echo "--- Creating POC database ---"
mysql -h "${DORIS_HOST}" -P "${DORIS_FE_QUERY_PORT}" \
  -u "${DORIS_USER}" --password="${DORIS_PASSWORD}" \
  -e "CREATE DATABASE IF NOT EXISTS poc;"

echo "  Database 'poc' created."

# ---------------------------------------------------------------------------
# 9. Health check summary
# ---------------------------------------------------------------------------
echo ""
echo "--- Health Check ---"
mysql -h "${DORIS_HOST}" -P "${DORIS_FE_QUERY_PORT}" \
  -u "${DORIS_USER}" --password="${DORIS_PASSWORD}" \
  -e "SHOW FRONTENDS\G" 2>/dev/null | grep -E "(Host|Alive|Version)"

echo ""
echo "================================================"
echo "  Doris ${DORIS_VERSION} installed and running."
echo ""
echo "  FE HTTP : http://${DORIS_HOST}:${DORIS_FE_HTTP_PORT}"
echo "  FE MySQL: ${DORIS_HOST}:${DORIS_FE_QUERY_PORT}"
echo ""
echo "  Teardown: bash scripts/99_teardown.sh --engine doris"
echo "================================================"
