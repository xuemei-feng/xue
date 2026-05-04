#!/bin/bash
if [ -z "${BASH_VERSION:-}" ]; then
  exec /bin/bash "$0" "$@"
fi
set -euo pipefail

# Remove tc/htb root from limit_bw_matrix.sh. BW_MATRIX_VERBOSE=1 prints tc qdisc after.

SKIP_BW_LIMIT_IPS=(
  "10.10.1.1"
  "10.10.1.2"
)

CLUSTER_IPS=(
  "10.10.1.3"
  "10.10.1.4"
  "10.10.1.5"
  "10.10.1.6"
  "10.10.1.7"
  "10.10.1.8"
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
  for cand in enp129s0f0 enp6s0f0 enp6s0f1 enp1s0f0 enp1s0f1 eth0; do
    if ip link show "$cand" &>/dev/null && ip link show "$cand" | grep -q "state UP"; then
      echo "$cand"
      return 0
    fi
  done
  return 1
}

main() {
  if skip_bw_limit_this_host; then
    echo "Skip unlimit (SKIP_BW_LIMIT_IPS: ${SKIP_BW_LIMIT_IPS[*]})."
    exit 0
  fi

  local iface
  iface="$(detect_iface "${1:-}")" || {
    echo "Error: cannot detect interface (optional arg1: iface name)" >&2
    exit 1
  }

  tc qdisc del dev "$iface" root 2>/dev/null || true
  echo "OK bw-matrix removed dev=${iface}"

  if [[ "${BW_MATRIX_VERBOSE:-0}" == "1" ]] || [[ "${BW_MATRIX_VERBOSE:-0}" == "2" ]]; then
    tc qdisc show dev "$iface" 2>/dev/null || true
  fi
}

main "$@"
