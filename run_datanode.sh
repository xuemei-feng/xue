#!/bin/bash
set -e

pkill -9 run_datanode || true

echo "Starting datanode 172.16.2.46:17611 (bulk :17661)"
./project/cmake/build/run_datanode 172.16.2.46:17611 & 

