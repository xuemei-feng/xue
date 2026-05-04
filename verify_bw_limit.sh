#!/bin/bash
if [ -z "${BASH_VERSION:-}" ]; then
  exec /bin/bash "$0" "$@"
fi
set -euo pipefail

# 与 limit_bw_matrix.sh 一致：用于解析「发往集群 IP」的真实 egress（避免默认路由落在另一块网卡上）
CLUSTER_IPS=(
  "10.10.1.3"
  "10.10.1.4"
  "10.10.1.5"
  "10.10.1.6"
  "10.10.1.7"
  "10.10.1.8"
)

SKIP_BW_LIMIT_IPS=(
  "10.10.1.1"
  "10.10.1.2"
)

skip_bw_limit_this_host() {
  local ips lip s
  ips="$(hostname -I 2>/dev/null || true)"
  for lip in $ips; do
    for s in "${SKIP_BW_LIMIT_IPS[@]}"; do
      [[ "$lip" == "$s" ]] && return 0
    done
  done
  return 1
}

detect_iface() {
  if [[ $# -ge 1 && -n "${1:-}" ]]; then
    echo "$1"
    return 0
  fi
  local ip dev my_ips
  my_ips="$(hostname -I 2>/dev/null || true)"
  for ip in "${CLUSTER_IPS[@]}"; do
    for lip in $my_ips; do
      [[ "$lip" == "$ip" ]] && continue 2
    done
    dev="$(ip route get "$ip" 2>/dev/null | awk '{ for (i = 1; i <= NF; i++) if ($i == "dev") { print $(i + 1); exit } }')"
    if [[ -n "$dev" && "$dev" != "lo" ]]; then
      echo "$dev"
      return 0
    fi
  done
  dev="$(ip route | awk '/^default/ {print $5; exit}')"
  if [[ -n "$dev" ]]; then
    echo "$dev"
    return 0
  fi
  for cand in enp6s0f0 enp6s0f1 enp1s0f0 enp1s0f1 eth0; do
    if ip link show "$cand" &>/dev/null && ip link show "$cand" | grep -q "state UP"; then
      echo "$cand"
      return 0
    fi
  done
  return 1
}

main() {
  local iface
  iface="$(detect_iface "${1:-}")" || {
    echo "Error: cannot detect active interface" >&2
    exit 1
  }

  echo "=== verify_bw_limit ==="
  echo "host: $(hostname)"
  echo "iface: $iface"
  if skip_bw_limit_this_host; then
    echo "note: this host is SKIP_BW_LIMIT_IPS (${SKIP_BW_LIMIT_IPS[*]}) — matrix shaping is not applied here."
  fi
  echo

  echo "[1] qdisc:"
  tc qdisc show dev "$iface" || true
  echo

  echo "[2] class:"
  tc class show dev "$iface" || true
  echo

  echo "[3] filter:"
  tc filter show dev "$iface" || true
  echo

  echo "[4] class stats (-s):"
  tc -s class show dev "$iface" || true
  echo

  if tc qdisc show dev "$iface" | grep -q "htb"; then
    echo "Result: HTB qdisc detected. Bandwidth limit rules are likely loaded."
  else
    echo "Result: No HTB qdisc detected. Limit may NOT be applied."
  fi
}

main "$@"

