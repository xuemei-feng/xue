#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_XML="${SCRIPT_DIR}/project/config/parameterConfiguration.xml"
USER="root"
REMOTE_COMMAND="cd /root/xue && sh run_coordinator.sh"
PARALLEL=5

COORDINATOR_IP="$(sed -n 's:.*<CoordinatorIP>\([^<]*\)</CoordinatorIP>.*:\1:p' "${CONFIG_XML}" | head -1 | tr -d '[:space:]')"
if [ -z "${COORDINATOR_IP}" ]; then
  # fallback: second line of hosts (client, then coordinator)
  HOSTS_FILE="${SCRIPT_DIR}/hosts"
  COORDINATOR_IP="$(grep -v '^[[:space:]]*#\|^[[:space:]]*$' "${HOSTS_FILE}" | sed -n '2p' | tr -d '[:space:]')"
fi
if [ -z "${COORDINATOR_IP}" ]; then
  echo "Error: CoordinatorIP not found in ${CONFIG_XML} or hosts" >&2
  exit 1
fi

echo "Starting coordinator on ${COORDINATOR_IP} (from ${CONFIG_XML})..."
pdsh -R ssh -w "${COORDINATOR_IP}" -l "$USER" -f "$PARALLEL" "$REMOTE_COMMAND"
rc=$?

if [ "$rc" -eq 0 ]; then
  echo "Command executed successfully on coordinator node."
  echo "Coordinator log file: /tmp/run_coordinator.log"
else
  echo "Failed to execute command on coordinator node (${COORDINATOR_IP})." >&2
  exit "$rc"
fi
