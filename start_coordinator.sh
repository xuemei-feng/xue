#!/bin/bash

USER="root"

REMOTE_COMMAND="cd /root/xue && sh run_coordinator.sh"

PARALLEL=5

echo "Running command on all nodes..."
# 把 start_coordinator.sh 第 10 行改为：
pdsh -R ssh -w 172.16.0.114 -l $USER -f $PARALLEL "$REMOTE_COMMAND"

if [ $? -eq 0 ]; then
	echo "Command executed successfully on all nodes."
	echo "Coordinator log file: /tmp/run_coordinator.log"
else
	echo "Failed to execute command on some nodes."
fi