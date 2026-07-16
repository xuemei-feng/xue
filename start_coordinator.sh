#!/bin/bash
# 从 parameterConfiguration.xml 读取 CoordinatorIP（hosts 第 2 行亦可作后备）

set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
USER="root"
PARALLEL=5
REMOTE_COMMAND="cd /root/xue && sh run_coordinator.sh"

COORD_IP="$(sed -n 's:.*<CoordinatorIP>\([^<]*\)</CoordinatorIP>.*:\1:p' \
  "${ROOT_DIR}/project/config/parameterConfiguration.xml" | head -1 | tr -d '[:space:]')"
if [ -z "${COORD_IP}" ] && [ -f "${ROOT_DIR}/hosts" ]; then
  COORD_IP="$(sed -n '2p' "${ROOT_DIR}/hosts" | tr -d '[:space:]')"
fi
if [ -z "${COORD_IP}" ]; then
  echo "Error: cannot resolve CoordinatorIP" >&2
  exit 1
fi

echo "Starting coordinator on ${COORD_IP} ..."
pdsh -R ssh -w "${COORD_IP}" -l "$USER" -f "$PARALLEL" "$REMOTE_COMMAND"

if [ $? -eq 0 ]; then
  echo "Command executed successfully."
  echo "Coordinator log file: /tmp/run_coordinator.log"
else
  echo "Failed to start coordinator on ${COORD_IP}." >&2
  exit 1
fi