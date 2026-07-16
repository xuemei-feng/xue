#!/bin/bash
# 本机清理：进程不存在时 pkill 返回 1，不能因此让脚本失败（否则 pdsh 会当成杀失败）。
set +e

pkill -9 -x run_datanode 2>/dev/null || true
pkill -9 -x run_proxy 2>/dev/null || true
pkill -9 -x run_coordinator 2>/dev/null || true
pkill -9 -x main_client 2>/dev/null || true
# 兼容偶发进程名匹配不到的情况
pkill -9 -f '/project/cmake/build/run_datanode' 2>/dev/null || true
pkill -9 -f '/project/cmake/build/run_proxy' 2>/dev/null || true
pkill -9 -f '/project/cmake/build/run_coordinator' 2>/dev/null || true
pkill -9 -f '/project/cmake/build/main_client' 2>/dev/null || true
killall -9 run_datanode run_proxy run_coordinator main_client 2>/dev/null || true

# start_proxy.sh PROXY_FOLLOW 留下的远程 tail，不是 run_proxy，但容易误判“没杀掉”
pkill -9 -f 'tail -.* /tmp/unilrc-proxy' 2>/dev/null || true

rm -rf ./storage/* 2>/dev/null || true
exit 0
