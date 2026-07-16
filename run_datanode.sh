#!/bin/bash
set -e

pkill -9 run_datanode || true

echo "Starting datanode 172.16.2.113:17621 (bulk :17671)"
: > /tmp/unilrc-datanode-start-172.16.2.113.log
nohup env stdbuf -oL -eL ./project/cmake/build/run_datanode 172.16.2.113:17621 >/tmp/unilrc-datanode-start-172.16.2.113.log 2>&1 < /dev/null &
echo "started 172.16.2.113:17621 log=/tmp/unilrc-datanode-start-172.16.2.113.log pid=$!"

