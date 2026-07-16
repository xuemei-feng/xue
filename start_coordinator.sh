#!/bin/bash
[ -n "${BASH_VERSION:-}" ] || exec bash "$0" "$@"
set -e
set -o pipefail

USER="root"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${UNILRC_REPO_ROOT:-$SCRIPT_DIR}"
HOSTS_FILE="${REPO_ROOT}/hosts"

# Coordinator = 2nd IP in sorted hosts (after apply_cluster_ips.sh).
COORD_HOST="${COORDINATOR_HOST:-$(sed -n '2p' "$HOSTS_FILE" | tr -d '[:space:]')}"
if [ -z "$COORD_HOST" ]; then
  echo "Error: could not determine coordinator host (set COORDINATOR_HOST or fix hosts)" >&2
  exit 1
fi

REMOTE_COMMAND="cd ${REPO_ROOT} && bash run_coordinator.sh"

PARALLEL=5

echo "Starting coordinator on ${COORD_HOST}..."
PDSH_OUT="$(mktemp)"
trap 'rm -f "$PDSH_OUT"' EXIT
sudo pdsh -R ssh -w "$COORD_HOST" -l "$USER" -f "$PARALLEL" "$REMOTE_COMMAND" 2>&1 | tee "$PDSH_OUT"
PDSH_RC=${PIPESTATUS[0]}
FAIL_COUNT=$(grep -cE 'ssh exited with exit code [1-9]|Connection timed out|Permission denied' "$PDSH_OUT" || true)

if [ "$PDSH_RC" -eq 0 ] && [ "${FAIL_COUNT:-0}" -eq 0 ]; then
  echo "Coordinator started on ${COORD_HOST}."
else
  echo "Failed to start coordinator on ${COORD_HOST} (pdsh_rc=${PDSH_RC}, failures=${FAIL_COUNT:-?})." >&2
  exit 1
fi
