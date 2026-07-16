#!/bin/bash
# 单独跟随已启动的 proxy 日志（不重启进程）
[ -n "${BASH_VERSION:-}" ] || exec bash "$0" "$@"
set -e
HOSTS_FILE="${HOSTS_FILE:-proxy_hosts}"
: "${PROXY_PARALLEL:=10}"
USER="root"
if [ ! -f "$HOSTS_FILE" ]; then
  echo "Error: missing $HOSTS_FILE" >&2
  exit 1
fi
echo "Following /tmp/unilrc-proxy.log on $HOSTS_FILE (Ctrl-C to stop)..."
sudo pdsh -R ssh -w "^${HOSTS_FILE}" -l "$USER" -f "$PROXY_PARALLEL" \
  'tail -n 50 -F /tmp/unilrc-proxy.log'
