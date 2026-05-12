#!/usr/bin/env bash
# Summarize iperf3 --logfile outputs in current directory (result_*.log).
# Converts sender bitrate to Mbps for comparison.
# Usage: cd ~/iperf_logs && bash /path/to/summarize_iperf3_logs.sh
set -euo pipefail

first_line_or_empty() {
  head -1 "$1" 2>/dev/null | tr ',' ';' | tr -d '\r' | head -c 200
}

parse_sender_mbps() {
  awk '
    /sender$/ && /bits\/sec/ {
      for (i = 1; i <= NF; i++) {
        if ($(i+1) ~ /bits\/sec$/) {
          val = $i
          unit = $(i+1)
          if (unit == "Kbits/sec") { printf "%.4f\n", val/1000; next }
          if (unit == "Mbits/sec") { printf "%.4f\n", val; next }
          if (unit == "Gbits/sec") { printf "%.4f\n", val*1000; next }
        }
      }
    }' "$1" | tail -1
}

echo "file,size_bytes,status,throughput_Mbps,note"
shopt -nullglob
files=(result_*.log)
if [[ ${#files[@]} -eq 0 ]]; then
  echo "(no result_*.log in $(pwd))"
  exit 1
fi

values=()
for f in "${files[@]}"; do
  sz=$(wc -c <"$f" | tr -d ' ')
  if [[ "$sz" -lt 200 ]]; then
    echo "$f,$sz,FAIL,,$(first_line_or_empty "$f")"
    continue
  fi
  mbps=$(parse_sender_mbps "$f" || true)
  if [[ -z "${mbps:-}" ]]; then
    echo "$f,$sz,UNKNOWN,,could not parse sender line"
    continue
  fi
  echo "$f,$sz,OK,$mbps,"
  values+=("$mbps")
done

echo ""
if [[ ${#values[@]} -eq 0 ]]; then
  echo "No OK runs — fix server (-s on target), firewall, or SERVER_IP in test script."
  exit 0
fi

# min / max / avg in awk
printf '%s\n' "${values[@]}" | awk '
  { v[++n]=$1; sum+=$1 }
  END {
    if (n<1) exit
    min=max=v[1]
    for (i=2;i<=n;i++) { if (v[i]<min) min=v[i]; if (v[i]>max) max=v[i] }
    printf "OK_runs=%d avg_Mbps=%.4f min_Mbps=%.4f max_Mbps=%.4f\n", n, sum/n, min, max
  }'
