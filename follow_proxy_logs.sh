#!/bin/bash
if [ -z "${BASH_VERSION:-}" ]; then
  exec /bin/bash "$0" "$@"
fi
HOSTS_FILE="${PROXY_HOSTS_FILE:-proxy_hosts}"
USER="${USER:-root}"
PARALLEL="${PROXY_PARALLEL:-10}"
exec sudo pdsh -R ssh -w "^${HOSTS_FILE}" -l "$USER" -f "$PARALLEL" \
  'tail -n +1 -F /tmp/unilrc-proxy.log'
