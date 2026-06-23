#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_FILE="${SCRIPT_DIR}/small_tools/generator_sh.py"
HOSTS_FILE="${SCRIPT_DIR}/hosts"

if [[ ! -f "$HOSTS_FILE" ]]; then
  echo "Error: hosts file not found: $HOSTS_FILE" >&2
  exit 1
fi

USER="root"
REMOTE_DIR="/root/xue/small_tools"

# update_all.sh 已用 sudo rsync 同步全仓库时，可设 SKIP_COPY=1 跳过重复 scp
if [[ "${SKIP_COPY:-0}" != "1" ]]; then
  echo "Copying $SOURCE_FILE to all hosts (as ${USER})..."
  while read -r HOST; do
    [[ -z "${HOST// }" || "$HOST" =~ ^# ]] && continue
    echo "Copying to $HOST..."
    scp -o ConnectTimeout=15 -o StrictHostKeyChecking=no \
      "$SOURCE_FILE" "${USER}@${HOST}:${REMOTE_DIR}/"
    echo "Successfully copied to $HOST!"
  done < "$HOSTS_FILE"
else
  echo "SKIP_COPY=1, assuming generator_sh.py already synced."
fi

PARALLEL=10

# Auto-detect cluster IP prefix from hosts (e.g. 172.16.2.31 -> 172.16.2).
if [[ -z "${UNILRC_IP_PREFIX:-}" ]]; then
  first_ip="$(grep -v '^[[:space:]]*#\|^[[:space:]]*$' "$HOSTS_FILE" | head -1 | tr -d '[:space:]')"
  if [[ -n "$first_ip" ]]; then
    UNILRC_IP_PREFIX="${first_ip%.*}"
  fi
fi
: "${UNILRC_IP_PREFIX:=10.10.1}"

# 每台机器必须逐台显式传 LOCAL_IP=<hosts 中该行 IP>；UNILRC_IP_PREFIX 一并传给远程。
echo "Running generator_sh.py on all hosts (parallel=${PARALLEL}, prefix=${UNILRC_IP_PREFIX}, LOCAL_IP per host)..."
if xargs -P "$PARALLEL" -I{} ssh -o ConnectTimeout=15 -o StrictHostKeyChecking=no \
    "${USER}@{}" "cd ${REMOTE_DIR} && UNILRC_IP_PREFIX=${UNILRC_IP_PREFIX} LOCAL_IP={} python3 generator_sh.py" \
    < "$HOSTS_FILE"; then
  echo "Successfully ran generator_sh.py on all hosts!"
else
  echo "Failed to run generator_sh.py on some hosts!" >&2
  exit 1
fi

echo "All done!"