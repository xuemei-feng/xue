#!/bin/bash
if [ -z "${BASH_VERSION:-}" ]; then
  exec /bin/bash "$0" "$@"
fi
set -euo pipefail

# 与 limit_all_bw_matrix.sh 相同：在 proxy_hosts 列出的节点上解除矩阵 tc 限速。

HOSTS_FILE="proxy_hosts"
USER="root"
PARALLEL=5

V="${BW_MATRIX_VERBOSE:-0}"
REMOTE_COMMAND="cd /users/xue/xue && BW_MATRIX_VERBOSE=${V} /bin/bash unlimit_bw_matrix.sh"

echo "Removing matrix bandwidth limit on all nodes in ${HOSTS_FILE} (BW_MATRIX_VERBOSE=${V})..."
if sudo pdsh -R ssh -w ^"${HOSTS_FILE}" -l "${USER}" -f "${PARALLEL}" "${REMOTE_COMMAND}"; then
  echo "Bandwidth matrix cleared on all nodes."
else
  echo "Failed to unlimit on some nodes." >&2
  exit 1
fi
