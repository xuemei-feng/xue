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

# Clean up stale remote logs before starting
while IFS= read -r host; do
    [[ -z "$host" || "$host" =~ ^[[:space:]]*# ]] && continue
    ssh ${USER}@${host} "rm -f /tmp/cord_proxy_update.log" 2>/dev/null || true
done < "$HOSTS_FILE"

echo "Running proxy+datanode on all proxy nodes..."
pdsh -R ssh -w ^$HOSTS_FILE -l $USER -f $PARALLEL "$REMOTE_COMMAND"

if [ $? -eq 0 ]; then
    echo "Proxies exited successfully."
else
    echo "Proxies exited with error."
fi
