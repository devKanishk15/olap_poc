#!/usr/bin/env bash
# =============================================================================
# 00_vm_prep.sh  — VM preparation for OLAP POC
# Run as root (or with sudo) on Rocky Linux 9.
# =============================================================================
set -euo pipefail
LOGFILE="/var/log/olap_poc_prep.log"
exec > >(tee -a "$LOGFILE") 2>&1

echo "=========================================="
echo "  OLAP POC — VM Preparation"
echo "  $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "=========================================="

# ---------------------------------------------------------------------------
# 1. Hardware verification
# ---------------------------------------------------------------------------
echo ""
echo "--- Hardware Verification ---"

CPUS=$(nproc)
echo "vCPUs detected   : $CPUS"
if [[ "$CPUS" -lt 4 ]]; then
  echo "WARNING: Expected 4+ vCPUs, found $CPUS. Results may differ from baseline." >&2
fi

TOTAL_RAM_KB=$(grep MemTotal /proc/meminfo | awk '{print $2}')
TOTAL_RAM_GB=$(( TOTAL_RAM_KB / 1024 / 1024 ))
echo "RAM detected     : ${TOTAL_RAM_GB} GB"
if [[ "$TOTAL_RAM_GB" -lt 7 ]]; then
  echo "ERROR: Minimum 8 GB RAM required, found ~${TOTAL_RAM_GB} GB. Aborting." >&2
  exit 1
fi

DISK_AVAIL_GB=$(df -BG /opt1 2>/dev/null | awk 'NR==2 {gsub("G",""); print $4}' || echo "0")
echo "Disk avail /opt1 : ${DISK_AVAIL_GB} GB"
if [[ "${DISK_AVAIL_GB}" -lt 60 ]]; then
  echo "WARNING: Less than 60 GB free on /opt1. POC requires ~80 GB." >&2
fi

OS_PRETTY=$(grep PRETTY_NAME /etc/os-release | cut -d= -f2 | tr -d '"')
echo "OS               : $OS_PRETTY"

# ---------------------------------------------------------------------------
# 2. Create directory structure under /opt1
# ---------------------------------------------------------------------------
echo ""
echo "--- Creating Directory Structure ---"

for DIR in \
  /opt1/poc/scripts \
  /opt1/poc/docker \
  /opt1/poc/schema \
  /opt1/poc/data \
  /opt1/poc/queries \
  /opt1/poc/workloads \
  /opt1/poc/harness \
  /opt1/poc/results \
  /opt1/poc/report/03_charts \
  /opt1/doris \
  /opt1/duckdb \
  /opt1/clickhouse \
  /opt1/data \
  /opt1/logs \
  /opt1/secrets; do
  mkdir -p "$DIR"
  echo "  Created: $DIR"
done

chmod 700 /opt1/secrets
echo "  Secured: /opt1/secrets (700)"

# ---------------------------------------------------------------------------
# 3. System package prerequisites
# ---------------------------------------------------------------------------
echo ""
echo "--- Installing System Packages ---"

# Enable EPEL (needed for jq, htop, sysstat on Rocky 9)
dnf install -y epel-release

dnf install -y \
  ca-certificates \
  curl \
  wget \
  gnupg2 \
  unzip \
  jq \
  sysstat \
  procps-ng \
  htop \
  net-tools \
  python3 \
  python3-pip \
  mariadb \
  2>/dev/null

echo "  System packages installed."

# ---------------------------------------------------------------------------
# 4. Install Docker Engine + Compose plugin
# ---------------------------------------------------------------------------
echo ""
echo "--- Installing Docker ---"

if command -v docker &>/dev/null; then
  echo "  Docker already installed: $(docker --version)"
else
  # Add Docker's official repo for RHEL/Rocky Linux
  dnf install -y dnf-plugins-core
  dnf config-manager --add-repo https://download.docker.com/linux/rhel/docker-ce.repo

  dnf install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  systemctl enable --now docker
  echo "  Docker installed and started."
fi

docker compose version

# Add current non-root user to docker group (if not root)
if [[ "$EUID" -ne 0 ]] && ! groups "$USER" | grep -q docker; then
  usermod -aG docker "$USER"
  echo "  Added $USER to docker group. Re-login required for group to take effect."
fi

# ---------------------------------------------------------------------------
# 5. Kernel / system tunables
# ---------------------------------------------------------------------------
echo ""
echo "--- Applying Kernel Tunables ---"

# Apache Doris requires vm.max_map_count >= 2000000
sysctl -w vm.max_map_count=2000000
# Reduce swappiness — we want to use RAM, not swap, for hot data
sysctl -w vm.swappiness=10
# Dirty page limits to avoid write stalls
sysctl -w vm.dirty_ratio=60
sysctl -w vm.dirty_background_ratio=5
# Increase TCP buffers (useful for GCS reads)
sysctl -w net.core.rmem_max=134217728
sysctl -w net.core.wmem_max=134217728
sysctl -w net.ipv4.tcp_rmem="4096 87380 134217728"
sysctl -w net.ipv4.tcp_wmem="4096 65536 134217728"

# Persist across reboots
cat > /etc/sysctl.d/99-olap-poc.conf << 'SYSCTL'
vm.max_map_count=2000000
vm.swappiness=10
vm.dirty_ratio=60
vm.dirty_background_ratio=5
net.core.rmem_max=134217728
net.core.wmem_max=134217728
net.ipv4.tcp_rmem=4096 87380 134217728
net.ipv4.tcp_wmem=4096 65536 134217728
SYSCTL

echo "  Kernel tunables applied and persisted to /etc/sysctl.d/99-olap-poc.conf"

# ---------------------------------------------------------------------------
# 6. File descriptor / ulimit tunables
# ---------------------------------------------------------------------------
echo ""
echo "--- Applying ulimit Settings ---"

cat > /etc/security/limits.d/99-olap-poc.conf << 'LIMITS'
# Raised limits for OLAP engines (Doris, ClickHouse require large nofile)
*    soft  nofile  655360
*    hard  nofile  655360
root soft  nofile  655360
root hard  nofile  655360
*    soft  nproc   65536
*    hard  nproc   65536
LIMITS

echo "  ulimits persisted to /etc/security/limits.d/99-olap-poc.conf"
echo "  NOTE: Limits take effect on next login. Docker containers override via compose."

# ---------------------------------------------------------------------------
# 7. Python virtual environment for harness
# ---------------------------------------------------------------------------
echo ""
echo "--- Setting Up Python Environment ---"

python3 -m venv /opt1/poc/.venv
/opt1/poc/.venv/bin/pip install --upgrade pip -q

# Install harness dependencies from requirements.txt if present
if [[ -f /opt1/olap_poc/poc/harness/requirements.txt ]]; then
  /opt1/poc/.venv/bin/pip install -r /opt1/olap_poc/poc/harness/requirements.txt -q
  echo "  Harness dependencies installed."
else
  echo "  harness/requirements.txt not found yet — run again after cloning the repo to /opt1/olap_poc."
fi

# ---------------------------------------------------------------------------
# 8. Verify swap (warn if absent)
# ---------------------------------------------------------------------------
echo ""
echo "--- Swap Check ---"
SWAP_KB=$(grep SwapTotal /proc/meminfo | awk '{print $2}')
if [[ "$SWAP_KB" -eq 0 ]]; then
  echo "  WARNING: No swap detected. For the 8 GB RAM budget, consider a 4 GB swap file."
  echo "  To add swap:"
  echo "    fallocate -l 4G /swapfile && chmod 600 /swapfile"
  echo "    mkswap /swapfile && swapon /swapfile"
  echo "    echo '/swapfile none swap sw 0 0' >> /etc/fstab"
else
  echo "  Swap detected: $(( SWAP_KB / 1024 )) MB"
fi

# ---------------------------------------------------------------------------
# 9. Final summary
# ---------------------------------------------------------------------------
echo ""
echo "=========================================="
echo "  VM Prep Complete"
echo "  CPUs  : $(nproc)"
echo "  RAM   : ${TOTAL_RAM_GB} GB"
echo "  Disk  : $(df -BG /opt1 | awk 'NR==2{print $4}') free on /opt1"
echo "  Docker: $(docker --version)"
echo "  Python: $(python3 --version)"
echo "=========================================="
echo ""
echo "Next step: copy poc/ files to /opt1/poc/, then run:"
echo "  bash scripts/01_install_doris.sh"
