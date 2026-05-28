#!/bin/bash
# 在 node0 上检查 coordinator + 各 proxy 是否在监听

COORD_HOST="10.10.1.2"
COORD_PORT="55555"
PROXY_GRPC_PORT="50405"
PROXY_TCP_PORT="50505"

echo "=== Coordinator ${COORD_HOST}:${COORD_PORT} ==="
if nc -z -w 2 "${COORD_HOST}" "${COORD_PORT}" 2>/dev/null; then
  echo "OK: TCP connect to coordinator"
else
  echo "FAIL: cannot connect to coordinator"
fi
pdsh -R ssh -w "${COORD_HOST}" -l root "ss -tlnp | grep -E ':${COORD_PORT}' || echo 'not listening'" 2>/dev/null || true

echo ""
echo "=== Proxies (gRPC ${PROXY_GRPC_PORT}, TCP append ${PROXY_TCP_PORT}) ==="
if [ -f proxy_hosts ]; then
  pdsh -R ssh -w ^proxy_hosts -l root \
    "H=\$(hostname -I | tr ' ' '\n' | grep '^10.10.1' | head -1); echo -n \"\$H: \"; ss -tlnp 2>/dev/null | grep -E ':${PROXY_GRPC_PORT}|:${PROXY_TCP_PORT}' || echo NOT_LISTENING; pgrep -a run_proxy | head -1" \
    2>/dev/null || echo "pdsh failed"
else
  echo "proxy_hosts not found"
fi
