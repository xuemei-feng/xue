#!/bin/bash
pkill -9 run_datanode 2>/dev/null || true
pkill -9 run_proxy 2>/dev/null || true

: > /tmp/unilrc-datanode-start-172.16.2.113.log
nohup env stdbuf -oL -eL ./project/cmake/build/run_datanode 172.16.2.113:17621 >/tmp/unilrc-datanode-start-172.16.2.113.log 2>&1 < /dev/null &
echo "started 172.16.2.113:17621 log=/tmp/unilrc-datanode-start-172.16.2.113.log pid=$!"

