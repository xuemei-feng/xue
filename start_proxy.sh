#!/bin/bash

RUN_ENV=${UNILRC_ENV:-half-sim}

if [ "$RUN_ENV" = "local" ]; then
  echo "Local mode detected, running run_proxy_datanode.sh on localhost..."
  bash run_proxy_datanode.sh
  exit $?
fi

HOSTS_FILE="proxy_hosts"

USER="root"

REMOTE_COMMAND="cd /users/xue/xue && sh run_proxy_datanode.sh"

PARALLEL=50

echo "Running command on all nodes..."
sudo pdsh -R ssh -w ^$HOSTS_FILE -l $USER -f $PARALLEL "$REMOTE_COMMAND"

if [ $? -eq 0 ]; then
	echo "Command executed successfully on all nodes."
else
	echo "Failed to execute command on some nodes."
fi
