#!/usr/bin/env bash
# Long-run iperf3 client: one sample every INTERVAL sec, for DURATION_HOURS.
#
# BEFORE YOU START — on SERVER machine (the iperf "receiver"):
#   iperf3 -s -p 5201
# Leave that terminal/process running for the whole experiment.
#
# Usage:
#   chmod +x iperf3_longrun_client.sh
#   SERVER_IP=10.10.1.1 ./iperf3_longrun_client.sh
#   # optional env: PORT DURATION_HOURS DURATION_PER_RUN INTERVAL LOGDIR
set -euo pipefail

SERVER_IP="${SERVER_IP:-10.10.1.1}"
PORT="${PORT:-5201}"
DURATION_HOURS="${DURATION_HOURS:-2}"
DURATION_PER_RUN="${DURATION_PER_RUN:-30}"
INTERVAL="${INTERVAL:-600}"
LOGDIR="${LOGDIR:-$HOME/iperf_logs_json}"

if ! command -v iperf3 >/dev/null; then
  echo "install: sudo apt-get install -y iperf3" >&2
  exit 1
fi

END_TIME=$(($(date +%s) + DURATION_HOURS * 3600))
mkdir -p "$LOGDIR"
cd "$LOGDIR"

echo "=== Server (must be running on $SERVER_IP) ==="
echo "    iperf3 -s -p $PORT"
echo "=== Client (this machine) ==="
echo "    samples every ${INTERVAL}s, ${DURATION_PER_RUN}s each -> $LOGDIR"
echo ""

echo "=== 5s connectivity check ==="
if iperf3 -c "$SERVER_IP" -p "$PORT" -t 5; then
  echo "OK."
else
  echo "FAIL: start server on $SERVER_IP or fix firewall/port $PORT" >&2
  exit 1
fi
echo ""

while [[ $(date +%s) -lt $END_TIME ]]; do
  stamp=$(date +%Y%m%d_%H%M%S)
  echo "=== $(date '+%Y-%m-%d %H:%M:%S')  run -> result_${stamp}.json"
  jsonf="result_${stamp}.json"
  errf="result_${stamp}.err"
  set +e
  iperf3 -c "$SERVER_IP" -p "$PORT" -t "$DURATION_PER_RUN" -i 1 -J >"$jsonf" 2>"$errf"
  rc=$?
  set -e
  if [[ $rc -ne 0 ]]; then
    echo "iperf3 failed rc=$rc (see $errf); json may be empty or partial."
  fi
  echo "sleep ${INTERVAL}s ..."
  sleep "$INTERVAL"
done

echo "Done. Plot/summarize JSON in $LOGDIR"
