#!/bin/bash
set -e

pkill -9 run_datanode || true

echo "No datanode role for local_ip=10.10.1.1; skip."
exit 0
