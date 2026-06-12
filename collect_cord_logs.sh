#!/usr/bin/env bash
# Collect CORD logs from all cluster nodes to node0
# Run this AFTER the client test completes

set -euo pipefail

USER="root"
COORDINATOR_NODE="10.10.1.2"

COORDINATOR_LOG="/users/xue/xue/coordinator_update.log"
PROXY_LOG="/users/xue/xue/proxy_update.log"
REMOTE_COORDINATOR_TMP="/tmp/cord_coordinator_update.log"
REMOTE_PROXY_TMP="/tmp/cord_proxy_update.log"

HOSTS_FILE="proxy_hosts"

echo "=== Collecting CORD logs to node0 ==="

# --- Coordinator log ---
echo "Collecting coordinator log from ${COORDINATOR_NODE}..."
rm -f "${COORDINATOR_LOG}"
ssh ${USER}@${COORDINATOR_NODE} "cat ${REMOTE_COORDINATOR_TMP} 2>/dev/null" > "${COORDINATOR_LOG}" 2>/dev/null || true
if [ -s "${COORDINATOR_LOG}" ]; then
    echo "  coordinator_update.log: $(wc -l < ${COORDINATOR_LOG}) lines"
else
    echo "  WARNING: coordinator log empty or unreachable"
fi

# --- Proxy logs ---
echo "Collecting proxy logs from all proxy nodes..."
rm -f "${PROXY_LOG}"
while IFS= read -r host; do
    [[ -z "$host" || "$host" =~ ^[[:space:]]*# ]] && continue
    echo "--- Log from ${host} ---" >> "${PROXY_LOG}"
    ssh ${USER}@${host} "cat ${REMOTE_PROXY_TMP} 2>/dev/null" >> "${PROXY_LOG}" 2>/dev/null || true
done < "$HOSTS_FILE"
if [ -s "${PROXY_LOG}" ]; then
    echo "  proxy_update.log: $(wc -l < ${PROXY_LOG}) lines"
else
    echo "  WARNING: proxy log empty or unreachable"
fi

echo "=== Done ==="
echo "Coordinator log: ${COORDINATOR_LOG}"
echo "Proxy log:       ${PROXY_LOG}"
