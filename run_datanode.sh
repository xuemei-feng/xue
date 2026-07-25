#!/bin/bash
set -e

pkill -9 run_datanode || true

echo "Starting datanode 172.16.2.137:17621 (bulk :17671)"
./project/cmake/build/run_datanode 172.16.2.137:17621 & 

