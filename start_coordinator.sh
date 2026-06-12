#!/bin/bash

USER="root"
COORDINATOR_NODE="10.10.1.2"

REMOTE_COMMAND="cd /users/xue/xue && sh run_coordinator.sh"
PARALLEL=5

# Clean up stale remote log before starting
ssh ${USER}@${COORDINATOR_NODE} "rm -f /tmp/cord_coordinator_update.log" 2>/dev/null || true

echo "Running coordinator on ${COORDINATOR_NODE}..."
pdsh -R ssh -w ${COORDINATOR_NODE} -l $USER -f $PARALLEL "$REMOTE_COMMAND"

if [ $? -eq 0 ]; then
    echo "Coordinator exited successfully."
else
    echo "Coordinator exited with error."
fi
