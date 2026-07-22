#!/bin/bash
set -e

pkill -9 run_datanode || true

echo "Starting datanode 172.16.2.108:17606 (bulk :17656)"
./project/cmake/build/run_datanode 172.16.2.108:17606 & 

