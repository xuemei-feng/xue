#!/bin/bash

REPO_ROOT="/users/xue/xue"
HOSTS_FILE="$REPO_ROOT/hosts"

USER="root"

REMOTE_COMMAND="cd $REPO_ROOT && sh kill_all.sh"

PARALLEL=5

echo "Running command on all nodes..."
sudo pdsh -R ssh -w ^$HOSTS_FILE -l $USER -f $PARALLEL "$REMOTE_COMMAND"

if [ $? -eq 0 ]; then
	echo "Command executed successfully on all nodes."
else
	echo "Failed to execute command on some nodes."
fi

cd "$REPO_ROOT" && sh kill_all.sh
