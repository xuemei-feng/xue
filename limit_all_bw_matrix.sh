#!/bin/bash
if [ -z "${BASH_VERSION:-}" ]; then
  exec /bin/bash "$0" "$@"
fi
set -euo pipefail

HOSTS_FILE="proxy_hosts"
USER="root"
PARALLEL=5

# 默认各节点只打一行 OK；需要旧版详细输出：BW_MATRIX_VERBOSE=1 每条 dst；=2 再 dump tc
V="${BW_MATRIX_VERBOSE:-0}"
REMOTE_COMMAND="cd /root/xue && BW_MATRIX_VERBOSE=${V} /bin/bash limit_bw_matrix.sh"

echo "Applying matrix bandwidth limit (egress + ingress) on all nodes (BW_MATRIX_VERBOSE=${V})..."
echo "Matrix: project/config/BW_limitsame (pairwise MB/s → tc mbit; cross peers share parent ceil=max pairwise)."
echo "Also shapes proxy↔client on proxy hosts only (PROXY_CLIENT_BW_MB_PER_SEC, default 125MB/s); client hosts skipped."
# client / coordinator 不做限速：在 SKIP_BW_LIMIT_IPS；limit_bw_matrix.sh 会自动跳过。
# proxy_hosts 应为 6 个 cluster proxy（.3/.12/.21/.30/.39/.48），与 CLUSTER_IPS 一致。
# 假定各节点已通过 update_all.sh（rsync）与本机目录 /root/xue 对齐，无需 pdcp 分发。
if sudo pdsh -R ssh -w ^"${HOSTS_FILE}" -l "${USER}" -f "${PARALLEL}" "${REMOTE_COMMAND}"; then
  echo "Bandwidth matrix applied on all nodes."
else
  echo "Failed to apply bandwidth matrix on some nodes." >&2
  exit 1
fi