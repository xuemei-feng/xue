#!/bin/bash
if [ -z "${BASH_VERSION:-}" ]; then
  exec /bin/bash "$0" "$@"
fi
set -euo pipefail

detect_iface() {
  if [[ $# -ge 1 && -n "${1:-}" ]]; then
    echo "$1"
    return 0
  fi
  local dev
  dev="$(ip route | awk '/^default/ {print $5; exit}')"
  if [[ -n "$dev" ]]; then
    echo "$dev"
    return 0
  fi
  for cand in enp6s0f0 enp6s0f1 enp1s0f0 enp1s0f1 eth0; do
    if ip link show "$cand" &>/dev/null && ip link show "$cand" | rg -q "state UP"; then
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

  if tc qdisc show dev "$iface" | rg -q "htb"; then
    echo "Result: HTB qdisc detected. Bandwidth limit rules are likely loaded."
  else
    echo "Result: No HTB qdisc detected. Limit may NOT be applied."
  fi
}

main "$@"

