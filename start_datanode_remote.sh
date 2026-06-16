#!/bin/bash
# pdsh 调用：bash start_datanode_remote.sh %h detach
# 一台机器一个角色：%h = 该节点的 role IP，进程只绑定该 URI。
set -e

ROOT_DIR="/users/xue/xue"
LOCAL_IP="${1:-${LOCAL_IP:-}}"
MODE="${2:-detach}"
CLUSTER_XML="${ROOT_DIR}/project/config/clusterInformation.xml"
BULK_SHIFT=500
LOG_FILE="/tmp/unilrc-datanode-start-${LOCAL_IP:-unknown}.log"

if [ -z "$LOCAL_IP" ]; then
  echo "__DATANODE_ERROR__ missing LOCAL_IP"
  exit 1
fi

cd "$ROOT_DIR"

node="$(hostname)"
ts="$(date +%H:%M:%S)"
echo "__DATANODE_STARTING__ node=$node host=$LOCAL_IP time=$ts"

# 真实节点：本机网卡上必须有 role IP（不应依赖整段 alias 猜测角色）
if ! ip -4 addr show 2>/dev/null | grep -qE "inet ${LOCAL_IP}/"; then
  echo "__DATANODE_ERROR__ role IP ${LOCAL_IP} not configured on $(hostname); fix IP alias/routing (one machine one role)"
  exit 1
fi

uri="$(
  grep -oE "datanode uri=\"${LOCAL_IP}:[0-9]+\"" "$CLUSTER_XML" 2>/dev/null \
    | head -1 \
    | sed -n 's/.*uri="\([^"]*\)".*/\1/p'
)"

if [ -z "$uri" ]; then
  echo "__DATANODE_ERROR__ no datanode uri for host=$LOCAL_IP in clusterInformation.xml"
  exit 1
fi

port="${uri##*:}"
bulk=$((port + BULK_SHIFT))

# 只释放本角色端口，避免误杀同机其它进程（half-sim 遗留环境）
if command -v fuser >/dev/null 2>&1; then
  fuser -k "${port}/tcp" "${bulk}/tcp" 2>/dev/null || true
else
  pkill -9 -f "./project/cmake/build/run_datanode ${uri}" 2>/dev/null || true
fi

if [ "$MODE" = "attach" ]; then
  echo "__DATANODE_ATTACH__ host=$LOCAL_IP uri=$uri"
  exec ./project/cmake/build/run_datanode "$uri"
fi

nohup ./project/cmake/build/run_datanode "$uri" >"$LOG_FILE" 2>&1 < /dev/null &
echo "__DATANODE_START_SENT__ host=$LOCAL_IP uri=$uri log=$LOG_FILE"

if ! command -v ss >/dev/null 2>&1; then
  echo "__DATANODE_LISTENING_SKIP__ host=$LOCAL_IP port=$port time=$(date +%H:%M:%S) no_ss"
  exit 0
fi

for _i in 1 2 3 4 5 6 7 8 9 10 12 15; do
  if ss -lnt 2>/dev/null | grep -qE ":${port}([^0-9]|$)" \
     && ss -lnt 2>/dev/null | grep -qE ":${bulk}([^0-9]|$)"; then
    echo "__DATANODE_LISTENING__ host=$LOCAL_IP port=$port bulk=$bulk time=$(date +%H:%M:%S)"
    exit 0
  fi
  sleep 1
done

echo "__DATANODE_LISTENING_FAIL__ host=$LOCAL_IP port=$port bulk=$bulk time=$(date +%H:%M:%S)"
echo "__DATANODE_LOG_TAIL__ host=$LOCAL_IP log=$LOG_FILE"
tail -n 8 "$LOG_FILE" 2>/dev/null || true
exit 0
