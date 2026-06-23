#!/bin/bash

HOSTS_FILE="proxy_hosts"

USER="root"

REMOTE_COMMAND="cd /root/xue && sh limit_0.5Gb.sh"

PARALLEL=5

echo "Running command on all nodes..."
sudo pdsh -R ssh -w ^$HOSTS_FILE -l $USER -f $PARALLEL "$REMOTE_COMMAND"

if [ $? -eq 0 ]; then
	echo "Command executed successfully on all nodes."
else
	echo "Failed to execute command on some nodes."
fi
