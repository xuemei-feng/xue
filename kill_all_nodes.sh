#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOSTS_FILE="$SCRIPT_DIR/hosts"

USER="root"
REMOTE_COMMAND="cd $SCRIPT_DIR && sh kill_all.sh"

PARALLEL=5

if [[ ! -f "$HOSTS_FILE" ]]; then
    echo "Error: hosts file not found: $HOSTS_FILE"
    exit 1
fi

echo "Running command on all nodes..."
sudo pdsh -R ssh -w ^"$HOSTS_FILE" -l "$USER" -f "$PARALLEL" "$REMOTE_COMMAND"

if [[ $? -eq 0 ]]; then
    echo "Command executed successfully on all nodes."
else
    echo "Failed to execute command on some nodes."
fi

cd "$SCRIPT_DIR" || exit 1
sh kill_all.sh
