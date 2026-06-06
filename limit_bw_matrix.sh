#!/bin/bash
if [ -z "${BASH_VERSION:-}" ]; then
  exec /bin/bash "$0" "$@"
fi
set -euo pipefail

# Real bandwidth shaping by tc/htb (egress).
# Matrix: project/config/BW_limit — BW_MATRIX_MB_PER_SEC in MB/s; tc uses Mbit/s via get_bw_tc_mbit_rate.
# 输出：默认一行摘要；BW_MATRIX_VERBOSE=1 打印 matrix=…MB/s 与 tc=…mbit；=2 再 dump tc。

_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BW_FILE="${_SCRIPT_DIR}/project/config/BW_limit_same"
if [[ ! -f "$BW_FILE" ]]; then
  echo "Error: bandwidth file not found: $BW_FILE" >&2
  exit 1
fi
source "$BW_FILE"

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

CLUSTER_IPS=(
  "10.10.1.3"  # 0: TYO
  "10.10.1.4"  # 1: MEL
  "10.10.1.5"  # 2: SG
  "10.10.1.6"  # 3: SEO
  "10.10.1.7"  # 4: JAK
  "10.10.1.8"  # 5: HK
)

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

detect_local_cluster() {
  local ips local_ip
  ips="$(hostname -I 2>/dev/null || true)"
  for local_ip in $ips; do
    for i in "${!CLUSTER_IPS[@]}"; do
      if [[ "${CLUSTER_IPS[$i]}" == "$local_ip" ]]; then
        echo "$i"
        return 0
      fi
    done
  done
  return 1
}

float3() {
  awk -v m="$1" 'BEGIN { printf "%.3f", m + 0.0 }'
}

main() {
  if skip_bw_limit_this_host; then
    echo "Skip bandwidth matrix (SKIP_BW_LIMIT_IPS: ${SKIP_BW_LIMIT_IPS[*]})."
    exit 0
  fi

  local iface
  iface="$(detect_iface "${1:-}")" || {
    echo "Error: cannot detect active interface" >&2
    exit 1
  }

  local src_cluster
  src_cluster="$(detect_local_cluster)" || {
    echo "Error: local node IP not in cluster IP list" >&2
    exit 1
  }

  local v="${BW_MATRIX_VERBOSE:-0}"
  [[ "$v" =~ ^[0-9]+$ ]] || v=0

  if ((v >= 1)); then
    echo "Applying bandwidth matrix on iface=$iface, local_cluster=$src_cluster (${CLUSTER_IPS[$src_cluster]})"
  fi

  tc qdisc del dev "$iface" root 2>/dev/null || true
  tc qdisc add dev "$iface" root handle 1: htb default 999
  tc class add dev "$iface" parent 1: classid 1:1 htb rate 10000mbit ceil 10000mbit
  tc class add dev "$iface" parent 1: classid 1:999 htb rate 10000mbit ceil 10000mbit

  local idx dst_ip bw_mb_per_sec bw_mbit_rate class_minor classid rules=0
  for idx in "${!CLUSTER_IPS[@]}"; do
    if [[ "$idx" == "$src_cluster" ]]; then
      continue
    fi
    dst_ip="${CLUSTER_IPS[$idx]}"
    bw_mb_per_sec="$(get_bw_matrix_mb_per_sec "$src_cluster" "$idx")"
    bw_mbit_rate="$(get_bw_tc_mbit_rate "$src_cluster" "$idx")"
    if [[ -z "$bw_mbit_rate" || "$bw_mbit_rate" == "0" ]]; then
      ((v >= 1)) && echo "Skip $src_cluster->$idx (no bandwidth entry)"
      continue
    fi
    bw_mbit="$(float3 "$bw_mbit_rate")"
    class_minor=$((100 + idx))
    classid="1:${class_minor}"
    tc class add dev "$iface" parent 1: classid "$classid" htb rate "${bw_mbit}mbit" ceil "${bw_mbit}mbit"
    tc filter add dev "$iface" protocol ip parent 1:0 prio 1 u32 match ip dst "${dst_ip}/32" flowid "$classid"
    ((rules++)) || true
    ((v >= 1)) && echo "Limit dst=${dst_ip} cluster=${idx} matrix=${bw_mb_per_sec}MB/s tc=${bw_mbit}mbit"
  done

  echo "OK bw-matrix dev=${iface} host=${CLUSTER_IPS[$src_cluster]} cluster_id=${src_cluster} dst_rules=${rules}"

  if ((v >= 2)); then
    echo "--- tc qdisc show dev $iface ---"
    tc qdisc show dev "$iface"
    echo "--- tc class show dev $iface ---"
    tc class show dev "$iface"
    echo "--- tc filter show dev $iface ---"
    tc filter show dev "$iface"
  fi
}

main "$@"
