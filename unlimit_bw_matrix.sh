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

# All IPs per cluster: proxy + 8 datanodes (space-separated, index matches CLUSTER_IPS)
ALL_CLUSTER_IPS=(
  "10.10.1.3 10.10.1.4 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8 10.10.1.9 10.10.1.10 10.10.1.11"
  "10.10.1.12 10.10.1.13 10.10.1.14 10.10.1.15 10.10.1.16 10.10.1.17 10.10.1.18 10.10.1.19 10.10.1.20"
  "10.10.1.21 10.10.1.22 10.10.1.23 10.10.1.24 10.10.1.25 10.10.1.26 10.10.1.27 10.10.1.28 10.10.1.29"
  "10.10.1.30 10.10.1.31 10.10.1.32 10.10.1.33 10.10.1.34 10.10.1.35 10.10.1.36 10.10.1.37 10.10.1.38"
  "10.10.1.39 10.10.1.40 10.10.1.41 10.10.1.42 10.10.1.43 10.10.1.44 10.10.1.45 10.10.1.46 10.10.1.47"
  "10.10.1.48 10.10.1.49 10.10.1.50 10.10.1.51 10.10.1.52 10.10.1.53 10.10.1.54 10.10.1.55 10.10.1.56"
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
  for cluster_ips in "${ALL_CLUSTER_IPS[@]}"; do
    for ip in $cluster_ips; do
      for lip in $my_ips; do
        [[ "$lip" == "$ip" ]] && continue 2
      done
      dev="$(ip route get "$ip" 2>/dev/null | awk '{ for (i = 1; i <= NF; i++) if ($i == "dev") { print $(i + 1); exit } }')"
      if [[ -n "$dev" && "$dev" != "lo" ]]; then
        echo "$dev"
        return 0
      fi
    done
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
