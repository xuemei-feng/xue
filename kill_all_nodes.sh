#!/bin/bash
[ -n "${BASH_VERSION:-}" ] || exec bash "$0" "$@"
set -euo pipefail

ROOT_DIR="/root/xue"
USER="root"
REMOTE_COMMAND="cd $ROOT_DIR && bash kill_all.sh"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o BatchMode=yes -o ConnectTimeout=10"
: "${KILL_PARALLEL:=8}"
: "${KILL_RETRY:=1}"

for hf in hosts datanode_hosts; do
  if [ ! -f "$ROOT_DIR/$hf" ]; then
    echo "Error: missing host file $ROOT_DIR/$hf"
    exit 1
  fi
done

TMP_HOSTS="$(mktemp)"
FAIL_HOSTS="$(mktemp)"
PDSH_LOG="$(mktemp)"
trap 'rm -f "$TMP_HOSTS" "$FAIL_HOSTS" "$PDSH_LOG"' EXIT

awk 'NF > 0 {print $1}' "$ROOT_DIR/hosts" "$ROOT_DIR/datanode_hosts" | sort -u > "$TMP_HOSTS"
TOTAL="$(wc -l < "$TMP_HOSTS" | tr -d ' ')"

parse_pdsh_ssh_failures() {
  awk '
    /ssh exited with exit code/ {
      host = $2
      sub(/:$/, "", host)
      if (host != "") print host
    }
  ' "$1" | sort -u
}

run_remote_kill() {
  local hosts_file="$1"
  local count
  count="$(wc -l < "$hosts_file" | tr -d ' ')"
  : >"$PDSH_LOG"
  set +e
  PDSH_SSH_ARGS_APPEND="$SSH_OPTS" sudo pdsh -R ssh -w ^"$hosts_file" -l "$USER" -f "$KILL_PARALLEL" \
    "$REMOTE_COMMAND" 2>&1 | tee "$PDSH_LOG"
  local rc=${PIPESTATUS[0]}
  set -e
  parse_pdsh_ssh_failures "$PDSH_LOG" > "$FAIL_HOSTS"
  local fail_count
  fail_count="$(wc -l < "$FAIL_HOSTS" | tr -d ' ')"
  local ok_count=$((count - fail_count))
  echo "Remote kill result: ok=${ok_count} fail=${fail_count} (pdsh_rc=${rc})"
  [ "$fail_count" -eq 0 ]
}

echo "Pass 1/2: kill_all.sh on ${TOTAL} nodes (parallel=${KILL_PARALLEL})..."
REMAINING="$TMP_HOSTS"
attempt=0
while true; do
  attempt=$((attempt + 1))
  if run_remote_kill "$REMAINING"; then
    break
  fi
  if [ "$attempt" -gt "$KILL_RETRY" ]; then
    echo "[ERROR] Remote kill still failed on these hosts after ${attempt} attempt(s):"
    cat "$FAIL_HOSTS"
    exit 1
  fi
  echo "Retrying $(wc -l < "$FAIL_HOSTS" | tr -d ' ') failed host(s) in 2s..."
  cp "$FAIL_HOSTS" "$REMAINING"
  sleep 2
done

echo "Pass 2/2: local kill on node0..."
cd "$ROOT_DIR"
set +e
bash kill_all.sh
# node0 上若进程过多，pkill 可能被 OOM kill；killall 更直接
killall -9 run_datanode run_proxy run_coordinator main_client 2>/dev/null || true
set -e

echo "Done. All ${TOTAL} remote nodes killed successfully; node0 cleaned."
