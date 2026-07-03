#!/bin/bash

RUN_ENV=${UNILRC_ENV:-half-sim}

if [ "$RUN_ENV" = "local" ]; then
  echo "Local mode detected, running run_proxy_datanode.sh on localhost..."
  bash run_proxy_datanode.sh
  exit $?
fi

# 真实节点部署：proxy 与 datanode 分布在不同机器上，需在所有 cluster 节点上各自
# 启动“本机角色”（每台机器的 run_proxy_datanode.sh 由 generator_sh.py 按本机 IP 生成）。
# 因此这里用 hosts（全部节点），而非只含 6 个 proxy 的 proxy_hosts。
# client(.1)/coordinator(.2) 上的脚本只有 pkill，无害。
HOSTS_FILE="hosts"

USER="root"

REMOTE_COMMAND="cd /root/xue && bash run_proxy_datanode.sh"

PARALLEL=50

echo "Running command on all nodes..."
PDSH_OUT="$(mktemp)"
sudo pdsh -R ssh -w ^$HOSTS_FILE -l $USER -f $PARALLEL "$REMOTE_COMMAND" 2>&1 | tee "$PDSH_OUT"
PDSH_RC=${PIPESTATUS[0]}
FAIL_COUNT=$(grep -c 'ssh exited with exit code [1-9]' "$PDSH_OUT" || true)
rm -f "$PDSH_OUT"

if [ "$PDSH_RC" -eq 0 ] && [ "${FAIL_COUNT:-0}" -eq 0 ]; then
	echo "Command executed successfully on all nodes."
else
	echo "Failed on ${FAIL_COUNT:-?} node(s) (pdsh_rc=${PDSH_RC}). Regenerate scripts: SKIP_COPY=1 bash generate_run_proxy.sh"
	exit 1
fi
