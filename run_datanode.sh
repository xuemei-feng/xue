#!/bin/bash
set -e

pkill -9 run_datanode || true

echo "No datanode role for local_ip=172.16.2.31; skip."
exit 0
