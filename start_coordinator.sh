#!/bin/bash

USER="root"

REPO_ROOT="/users/xue/xue"
REMOTE_COMMAND="cd $REPO_ROOT && sh run_coordinator.sh"

PARALLEL=5

echo "Running command on all nodes..."
pdsh -R ssh -w 10.10.1.2 -l $USER -f $PARALLEL "$REMOTE_COMMAND"

if [ $? -eq 0 ]; then
	echo "Command executed successfully on all nodes."
	echo "Coordinator log file: /tmp/run_coordinator.log"
else
	echo "Failed to execute command on some nodes."
fi
