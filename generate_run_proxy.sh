#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${UNILRC_REPO_ROOT:-$SCRIPT_DIR}"
SOURCE_FILE="${REPO_ROOT}/small_tools/generator_sh.py"
HOSTS_FILE="${REPO_ROOT}/hosts"
SMALL_TOOLS_DIR="${REPO_ROOT}/small_tools"

if [[ ! -f "$HOSTS_FILE" ]]; then
  echo "Error: hosts file not found: $HOSTS_FILE" >&2
  exit 1
fi

USER="root"

# update_all.sh 已用 rsync 同步全仓库时，可设 SKIP_COPY=1 跳过重复 scp
if [[ "${SKIP_COPY:-0}" != "1" ]]; then
  echo "Copying $SOURCE_FILE to all hosts (as ${USER})..."
  while read -r HOST; do
    [[ -z "${HOST// }" || "$HOST" =~ ^# ]] && continue
    echo "Copying to $HOST..."
    scp -o ConnectTimeout=15 -o StrictHostKeyChecking=no \
      "$SOURCE_FILE" "${USER}@${HOST}:${SMALL_TOOLS_DIR}/"
    echo "Successfully copied to $HOST!"
  done < "$HOSTS_FILE"
else
  echo "SKIP_COPY=1, assuming generator_sh.py already synced."
fi

PARALLEL=10

# 每台机器网卡上挂了整段 10.10.1.* 别名，必须逐台显式传 LOCAL_IP=<hosts 中该行 IP>。
# LOCAL_IP=... 必须紧贴 python，不能写在 cd 前。
echo "Running generator_sh.py on all hosts (repo=${REPO_ROOT}, parallel=${PARALLEL}, LOCAL_IP per host)..."
if grep -v '^#' "$HOSTS_FILE" | grep -v '^[[:space:]]*$' | \
    xargs -P "$PARALLEL" -I{} ssh -o ConnectTimeout=15 -o StrictHostKeyChecking=no \
    "${USER}@{}" "cd ${SMALL_TOOLS_DIR} && LOCAL_IP={} python3 generator_sh.py"; then
  echo "Successfully ran generator_sh.py on all hosts!"
else
  echo "Failed to run generator_sh.py on some hosts!" >&2
  exit 1
fi

echo "All done!"
