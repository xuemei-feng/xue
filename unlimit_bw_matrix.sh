#!/bin/bash
if [ -z "${BASH_VERSION:-}" ]; then
  exec /bin/bash "$0" "$@"
fi
set -euo pipefail

# Remove tc/htb (egress root + ingress/IFB) from limit_bw_matrix.sh. BW_MATRIX_VERBOSE=1 prints tc qdisc after.

IFB_DEV="ifb0"

SKIP_BW_LIMIT_IPS=(
  "172.16.2.31"
  "172.16.2.32"
)

# 与 proxy_hosts / clusterInformation.xml 中 6 个 proxy 一致
CLUSTER_IPS=(
  "172.16.2.33"   # 0: TYO  cluster 0 proxy
  "172.16.2.42"   # 1: MEL  cluster 1 proxy
  "172.16.2.51"   # 2: SG  cluster 2 proxy
  "172.16.2.60"   # 3: SEO  cluster 3 proxy
  "172.16.2.69"   # 4: JAK  cluster 4 proxy
  "172.16.2.78"   # 5: HK  cluster 5 proxy
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
  tc qdisc del dev "$iface" ingress 2>/dev/null || true
  tc qdisc del dev "$IFB_DEV" root 2>/dev/null || true
  ip link del "$IFB_DEV" 2>/dev/null || true
  echo "OK bw-matrix removed dev=${iface} ifb=${IFB_DEV}"

  if [[ "${BW_MATRIX_VERBOSE:-0}" == "1" ]] || [[ "${BW_MATRIX_VERBOSE:-0}" == "2" ]]; then
    tc qdisc show dev "$iface" 2>/dev/null || true
    tc qdisc show dev "$IFB_DEV" 2>/dev/null || true
  fi
}

main "$@"