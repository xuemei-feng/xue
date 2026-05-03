#!/bin/bash

# 本机与远程统一写死的路径（与 update_all.sh 中 REPO_ROOT / REMOTE_DIR 一致）
REPO_ROOT="/users/xue/xue"

SOURCE_FILE="$REPO_ROOT/small_tools/generator_sh.py"
HOSTS_FILE="$REPO_ROOT/hosts"

if [ ! -f "$HOSTS_FILE" ]; then
  echo "Error: hosts file not found!"
  exit 1
fi

HOSTS=$(cat "$HOSTS_FILE")

echo "Copying $SOURCE_FILE to all hosts..."
for HOST in $HOSTS; do
  echo "Copying to $HOST..."
  scp "$SOURCE_FILE" "$HOST:$REPO_ROOT/small_tools/"
  if [ $? -eq 0 ]; then
    echo "Successfully copied to $HOST!"
  else
    echo "Failed to copy to $HOST!"
    exit 1
  fi
done

REMOTE_COMMAND="cd $REPO_ROOT/small_tools/ && python generator_sh.py"
PARALLEL=50
USER="root"

echo "Running generator_sh.py on all hosts..."
pdsh -R ssh -w ^$HOSTS_FILE -l $USER -f $PARALLEL "$REMOTE_COMMAND"

if [ $? -eq 0 ]; then
  echo "Successfully ran generator_sh.py on all hosts!"
else
  echo "Failed to run generator_sh.py on some hosts!"
  exit 1
fi

echo "All done!"
