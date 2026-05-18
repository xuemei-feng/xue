#!/bin/bash
# 在 hosts 中第一台机器（10.10.1.1）上跑客户端；与 main_client 中 client_ip 一致。
USER="root"
CLIENT_HOST="10.10.1.1"
REMOTE_COMMAND="cd /users/xue/xue && ./project/cmake/build/main_client"
PARALLEL=5

echo "Running main_client on ${CLIENT_HOST}..."
pdsh -R ssh -w "${CLIENT_HOST}" -l "${USER}" -f "${PARALLEL}" "${REMOTE_COMMAND}"

if [ $? -eq 0 ]; then
  echo "Command executed successfully."
else
  echo "Failed to execute on ${CLIENT_HOST}."
fi
