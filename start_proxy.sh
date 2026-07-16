#!/bin/bash
if [ -z "${BASH_VERSION:-}" ]; then
  exec /bin/bash "$0" "$@"
fi
set -euo pipefail

RUN_ENV=${UNILRC_ENV:-half-sim}

if [ "$RUN_ENV" = "local" ]; then
  echo "Local mode detected, running run_proxy_datanode.sh on localhost..."
  bash run_proxy_datanode.sh
  exit $?
fi

# 只在 10 个 proxy 节点启动（勿用全量 hosts：PARALLEL 槽位会被 datanode 上的 sleep/脚本占满，
# 且旧版 run_proxy 前台挂后台会卡住 SSH，导致最后几个 proxy 永远起不来）。
HOSTS_FILE="${PROXY_HOSTS_FILE:-proxy_hosts}"
USER="root"
PROXY_PARALLEL="${PROXY_PARALLEL:-10}"
PROXY_FOLLOW="${PROXY_FOLLOW:-0}"

if [[ ! -f "$HOSTS_FILE" ]]; then
  echo "Error: hosts file not found: $HOSTS_FILE" >&2
  exit 1
fi

# 变更集群规模后请先：SKIP_COPY=1 bash generate_run_proxy.sh
REMOTE_COMMAND='cd /root/xue && export CORD_XFER_VERBOSE="${CORD_XFER_VERBOSE:-1}" && bash run_proxy_datanode.sh'

echo "Starting proxies via ${HOSTS_FILE} (parallel=${PROXY_PARALLEL}, CORD_XFER_VERBOSE=${CORD_XFER_VERBOSE:-1})..."
PDSH_OUT="$(mktemp)"
sudo pdsh -R ssh -w "^${HOSTS_FILE}" -l "$USER" -f "$PROXY_PARALLEL" "$REMOTE_COMMAND" 2>&1 | tee "$PDSH_OUT"
PDSH_RC=${PIPESTATUS[0]}
FAIL_COUNT=$(grep -c 'ssh exited with exit code [1-9]' "$PDSH_OUT" || true)
rm -f "$PDSH_OUT"

if [ "$PDSH_RC" -ne 0 ] || [ "${FAIL_COUNT:-0}" -ne 0 ]; then
  echo "Failed on ${FAIL_COUNT:-?} node(s) (pdsh_rc=${PDSH_RC}). Regenerate: SKIP_COPY=1 bash generate_run_proxy.sh" >&2
  exit 1
fi

echo "Proxies started. Logs: /tmp/unilrc-proxy.log on each proxy host."

if [[ "$PROXY_FOLLOW" == "1" ]]; then
  echo "Following proxy logs (PROXY_FOLLOW=1). Ctrl-C stops follow only."
  exec sudo pdsh -R ssh -w "^${HOSTS_FILE}" -l "$USER" -f "$PROXY_PARALLEL" \
    'tail -n +1 -F /tmp/unilrc-proxy.log'
fi
