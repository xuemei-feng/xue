#!/bin/bash

USER="root"

REMOTE_COMMAND="cd /root/xue && sh run_coordinator.sh"

PARALLEL=5

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COORD_IP="$(sed -n 's:.*<CoordinatorIP>\([^<]*\)</CoordinatorIP>.*:\1:p' \
  "${SCRIPT_DIR}/project/config/parameterConfiguration.xml" | head -1)"
if [[ -z "$COORD_IP" ]]; then
  echo "Error: CoordinatorIP not found in project/config/parameterConfiguration.xml" >&2
  exit 1
fi

echo "Running command on coordinator ${COORD_IP}..."
pdsh -R ssh -w "$COORD_IP" -l $USER -f $PARALLEL "$REMOTE_COMMAND"

if [ $? -eq 0 ]; then
	echo "Command executed successfully on all nodes."
	echo "Coordinator log file: /tmp/run_coordinator.log"
else
	echo "Failed to execute command on some nodes."
fi
