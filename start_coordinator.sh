#!/bin/bash
set -euo pipefail

USER="root"
REPO_ROOT="/root/xue"
PARAM_XML="${REPO_ROOT}/project/config/parameterConfiguration.xml"
REMOTE_COMMAND="cd ${REPO_ROOT} && sh run_coordinator.sh"
PARALLEL=5

COORDINATOR_IP="${COORDINATOR_IP:-}"
if [[ -z "${COORDINATOR_IP}" ]]; then
  COORDINATOR_IP="$(sed -n 's:.*<CoordinatorIP>\([^<]*\)</CoordinatorIP>.*:\1:p' "${PARAM_XML}" | head -1)"
fi

if [[ -z "${COORDINATOR_IP}" ]]; then
  echo "Error: CoordinatorIP not found in ${PARAM_XML}" >&2
  exit 1
fi

echo "Starting coordinator on ${COORDINATOR_IP} ..."
if pdsh -R ssh -w "${COORDINATOR_IP}" -l "${USER}" -f "${PARALLEL}" "${REMOTE_COMMAND}"; then
  echo "Command executed successfully on all nodes."
  echo "Coordinator log file: /tmp/run_coordinator.log"
else
  echo "Failed to execute command on some nodes." >&2
  exit 1
fi
