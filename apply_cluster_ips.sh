#!/usr/bin/env bash
# Sort a column of IPs (numeric order) and rewrite cluster deployment configs.
#
# Usage:
#   ./apply_cluster_ips.sh ip_list.txt
#   ./apply_cluster_ips.sh -              # stdin
#   ./apply_cluster_ips.sh --dry-run ip_list.txt
#
# Expected: 92 IPs = 1 client + 1 coordinator + 10×(1 proxy + 8 datanodes).
# Override: CLUSTER_NUM=10 DATANODES_PER_CLUSTER=8

if [ -z "${BASH_VERSION:-}" ]; then
  exec /bin/bash "$0" "$@"
fi
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PY="${SCRIPT_DIR}/small_tools/apply_sorted_ips.py"

if [[ ! -f "$PY" ]]; then
  echo "Error: missing $PY" >&2
  exit 1
fi

exec python3 "$PY" "$@"