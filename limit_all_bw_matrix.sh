#!/bin/bash
if [ -z "${BASH_VERSION:-}" ]; then
  exec /bin/bash "$0" "$@"
fi
set -euo pipefail

HOSTS_FILE="proxy_hosts"
USER="root"
PARALLEL=5

LOCAL_SCRIPT="/users/xue/xue/limit_bw_matrix.sh"
REMOTE_SCRIPT="/users/xue/xue/limit_bw_matrix.sh"
REMOTE_COMMAND="cd /users/xue/xue && /bin/bash limit_bw_matrix.sh"

echo "Applying matrix bandwidth limit on all nodes..."
# 1) Distribute latest script to all nodes first.
sudo pdcp -R ssh -w ^"${HOSTS_FILE}" -l "${USER}" -f "${PARALLEL}" "${LOCAL_SCRIPT}" "${REMOTE_SCRIPT}"

# 2) Run with bash explicitly on all nodes.
if sudo pdsh -R ssh -w ^"${HOSTS_FILE}" -l "${USER}" -f "${PARALLEL}" "${REMOTE_COMMAND}"; then
  echo "Bandwidth matrix applied on all nodes."
else
  echo "Failed to apply bandwidth matrix on some nodes." >&2
  exit 1
fi

