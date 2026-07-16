#!/bin/bash
[ -n "${BASH_VERSION:-}" ] || exec bash "$0" "$@"
set -e
set -o pipefail

RUN_ENV=${UNILRC_ENV:-half-sim}

if [ "$RUN_ENV" = "local" ]; then
  echo "Local mode detected, running run_proxy_datanode.sh on localhost..."
  bash run_proxy_datanode.sh
  exit $?
fi

# 真实节点：只对 proxy_hosts 启动（datanode 用 start_datanode.sh）。
# 旧行为：HOSTS_FILE=hosts bash start_proxy.sh
HOSTS_FILE="${HOSTS_FILE:-proxy_hosts}"

USER="root"
: "${PROXY_PARALLEL:=10}"
# 启动后在本终端跟随各 proxy 的 /tmp/unilrc-proxy.log；Ctrl-C 只停 tail，不杀 proxy
: "${PROXY_FOLLOW:=1}"
# CoRD 跨机架 plan 详细日志（proxy.cpp cord_plan_log_*）
: "${CORD_XFER_VERBOSE:=1}"

REMOTE_COMMAND="cd /root/xue && CORD_XFER_VERBOSE=${CORD_XFER_VERBOSE} bash run_proxy_datanode.sh"

if [ ! -f "$HOSTS_FILE" ]; then
  echo "Error: missing $HOSTS_FILE" >&2
  exit 1
fi

echo "Starting proxies via $HOSTS_FILE (parallel=${PROXY_PARALLEL}, CORD_XFER_VERBOSE=${CORD_XFER_VERBOSE})..."
PDSH_OUT="$(mktemp)"
trap 'rm -f "$PDSH_OUT"' EXIT
sudo pdsh -R ssh -w "^${HOSTS_FILE}" -l "$USER" -f "$PROXY_PARALLEL" "$REMOTE_COMMAND" 2>&1 | tee "$PDSH_OUT"
PDSH_RC=${PIPESTATUS[0]}
FAIL_COUNT=$(grep -cE 'ssh exited with exit code [1-9]|Connection timed out|Permission denied' "$PDSH_OUT" || true)

if [ "$PDSH_RC" -ne 0 ] || [ "${FAIL_COUNT:-0}" -ne 0 ]; then
  echo "Failed on ${FAIL_COUNT:-?} node(s) (pdsh_rc=${PDSH_RC}). Regenerate: SKIP_COPY=1 bash generate_run_proxy.sh" >&2
  exit 1
fi

echo "Proxies started. Logs: /tmp/unilrc-proxy.log on each proxy host."

if [ "${PROXY_FOLLOW}" = "1" ]; then
  echo "Following proxy logs (Ctrl-C stops follow only; proxies keep running)..."
  echo "Tip: PROXY_FOLLOW=0 bash start_proxy.sh  # start without tail"
  # -F: 文件被 truncate/重创后继续跟；前缀带主机 IP
  sudo pdsh -R ssh -w "^${HOSTS_FILE}" -l "$USER" -f "$PROXY_PARALLEL" \
    'tail -n +1 -F /tmp/unilrc-proxy.log'
fi
