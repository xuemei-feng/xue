#!/bin/bash
if [ -z "${BASH_VERSION:-}" ]; then
  exec /bin/bash "$0" "$@"
fi
set -euo pipefail

HOSTS_FILE="proxy_hosts"
USER="root"
PARALLEL=5

REMOTE_COMMAND="cd /users/xue/xue && /bin/bash limit_bw_matrix.sh"

echo "Applying matrix bandwidth limit on all nodes..."
# 10.10.1.1 / 10.10.1.2 不做限速：勿加入 proxy_hosts；单机上跑 limit_bw_matrix.sh 也会自动跳过。
# 假定各节点已通过 update_all.sh（rsync）与本机目录 /users/xue/xue 对齐，无需 pdcp 分发。
if sudo pdsh -R ssh -w ^"${HOSTS_FILE}" -l "${USER}" -f "${PARALLEL}" "${REMOTE_COMMAND}"; then
  echo "Bandwidth matrix applied on all nodes."
else
  echo "Failed to apply bandwidth matrix on some nodes." >&2
  exit 1
fi

