#!/bin/bash

REPO_ROOT="/users/xue/xue"
RUN_ENV=${UNILRC_ENV:-local}

if [ "$RUN_ENV" = "local" ]; then
  echo "Local mode detected, running run_proxy_datanode.sh on localhost..."
  cd "$REPO_ROOT" && bash run_proxy_datanode.sh
  exit $?
fi

HOSTS_FILE="$REPO_ROOT/proxy_hosts"

USER="root"

REMOTE_COMMAND="cd $REPO_ROOT && sh run_proxy_datanode.sh"

PARALLEL=50

echo "Running command on all nodes..."
sudo pdsh -R ssh -w ^$HOSTS_FILE -l $USER -f $PARALLEL "$REMOTE_COMMAND"

if [ $? -eq 0 ]; then
	echo "Command executed successfully on all nodes."
else
	echo "Failed to execute command on some nodes."
fi
