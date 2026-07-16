#!/bin/bash
set -e

pkill -9 run_datanode || true

echo "Starting datanode 172.16.2.113:17621 (bulk :17671)"
nohup ./project/cmake/build/run_datanode 172.16.2.113:17621 >>/tmp/unilrc-datanode.log 2>&1 &

