#!/bin/bash
pkill -9 run_datanode 2>/dev/null || true
pkill -9 run_proxy 2>/dev/null || true

: > /tmp/unilrc-datanode-start-172.16.2.108.log
nohup env stdbuf -oL -eL ./project/cmake/build/run_datanode 172.16.2.108:17606 >/tmp/unilrc-datanode-start-172.16.2.108.log 2>&1 < /dev/null &
echo "started 172.16.2.108:17606 log=/tmp/unilrc-datanode-start-172.16.2.108.log pid=$!"

