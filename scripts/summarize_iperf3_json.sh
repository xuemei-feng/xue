#!/usr/bin/env bash
# Summarize iperf3 -J logs: end sender throughput -> Mbps.
# Usage: bash summarize_iperf3_json.sh [directory]
# Needs: jq   (sudo apt-get install -y jq)
set -euo pipefail
dir="${1:-.}"
shopt -s nullglob
mapfile -t files < <(ls "$dir"/result_*.json 2>/dev/null || true)
if [[ ${#files[@]} -eq 0 ]]; then
  echo "no $dir/result_*.json"
  exit 1
fi
echo "file,end_sender_Mbps"
tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
for f in "${files[@]}"; do
  if ! bps=$(jq -r '.end.sum_sent.bits_per_second // empty' "$f" 2>/dev/null) || [[ -z "$bps" || "$bps" == "null" ]]; then
    echo "$(basename "$f"),FAIL"
    continue
  fi
  mbps=$(awk -v b="$bps" 'BEGIN { printf "%.6f", b/1e6 }')
  echo "$(basename "$f"),$mbps"
  echo "$mbps" >>"$tmp"
done
if [[ -s "$tmp" ]]; then
  awk '{ s+=$1; if(NR==1||$1<min)min=$1; if(NR==1||$1>max)max=$1 } END { printf "--- n=%d avg=%.4f min=%.4f max=%.4f Mbps ---\n", NR, s/NR, min, max }' "$tmp"
fi
