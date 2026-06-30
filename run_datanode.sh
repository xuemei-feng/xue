#!/bin/bash
set -e

pkill -9 run_datanode || true

echo "Starting datanode 172.16.3.3:17619 (bulk :17669)"
./project/cmake/build/run_datanode 172.16.3.3:17619 & 

