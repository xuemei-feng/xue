#!/bin/bash
set -e

pkill -9 run_datanode || true

echo "Starting datanode 172.16.2.108:17606 (bulk :17656)"
: > /tmp/unilrc-datanode-start-172.16.2.108.log
nohup env stdbuf -oL -eL ./project/cmake/build/run_datanode 172.16.2.108:17606 >/tmp/unilrc-datanode-start-172.16.2.108.log 2>&1 < /dev/null &
echo "started 172.16.2.108:17606 log=/tmp/unilrc-datanode-start-172.16.2.108.log pid=$!"

