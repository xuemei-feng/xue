#!/bin/bash

USER="root"

REMOTE_COMMAND="cd /users/xue/xue && sh run_coordinator.sh"

PARALLEL=5

echo "Running command on all nodes..."
# 把 start_coordinator.sh 第 10 行改为：
pdsh -R ssh -w 10.10.1.2 -l $USER -f $PARALLEL "$REMOTE_COMMAND"

if [ $? -eq 0 ]; then
	echo "Command executed successfully on all nodes."
	echo "Coordinator log file: /tmp/run_coordinator.log"
else
	echo "Failed to execute command on some nodes."
fi