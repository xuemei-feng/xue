#!/bin/bash

RUN_ENV=${UNILRC_ENV:-half-sim}

if [ "$RUN_ENV" = "local" ]; then
  echo "Local mode detected, running run_client_node.sh on localhost..."
  bash run_client_node.sh
  exit $?
fi

USER="root"

REMOTE_COMMAND="cd /users/xue/xue && sh run_client_node.sh"

PARALLEL=5

echo "Running command on all nodes..."
sudo pdsh -R ssh -w 10.10.1.1 -l $USER -f $PARALLEL "$REMOTE_COMMAND"

if [ $? -eq 0 ]; then
	echo "Command executed successfully on all nodes."
else
	echo "Failed to execute command on some nodes."
fi
