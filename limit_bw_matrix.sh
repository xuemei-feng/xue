#!/bin/bash
if [ -z "${BASH_VERSION:-}" ]; then
  exec /bin/bash "$0" "$@"
fi
set -euo pipefail

# Real bandwidth shaping by tc/htb (egress).
# Matrix source: /users/xue/xue/project/config/BW_limit (MB/s, symmetric).

BW_FILE="/users/xue/xue/project/config/BW_limit"
if [[ ! -f "$BW_FILE" ]]; then
  echo "Error: bandwidth file not found: $BW_FILE" >&2
  exit 1
fi
source "$BW_FILE"

# 不参与矩阵限速的节点（管理/协调等；不挂 HTB）
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

# cluster id -> host ip (from clusterInformation.xml)
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
  # 优先：发往集群对端的出口（与 ip route get <peer> 一致），避免默认路由块网卡与业务路由不一致
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

mbps_to_mbit() {
  # MB/s -> Mbit/s
  awk -v mb="$1" 'BEGIN { printf "%.3f", mb * 8.0 }'
}

main() {
  if skip_bw_limit_this_host; then
    echo "Skip bandwidth matrix: this host is in SKIP_BW_LIMIT_IPS (${SKIP_BW_LIMIT_IPS[*]})."
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

  echo "Applying bandwidth matrix on iface=$iface, local_cluster=$src_cluster (${CLUSTER_IPS[$src_cluster]})"

  tc qdisc del dev "$iface" root 2>/dev/null || true
  tc qdisc add dev "$iface" root handle 1: htb default 999
  tc class add dev "$iface" parent 1: classid 1:1 htb rate 10000mbit ceil 10000mbit
  tc class add dev "$iface" parent 1:1 classid 1:999 htb rate 10000mbit ceil 10000mbit

  local idx dst_ip bw_mb bw_mbit class_minor classid
  for idx in "${!CLUSTER_IPS[@]}"; do
    if [[ "$idx" == "$src_cluster" ]]; then
      continue
    fi
    dst_ip="${CLUSTER_IPS[$idx]}"
    bw_mb="$(get_bw_mb "$src_cluster" "$idx")"
    if [[ -z "$bw_mb" || "$bw_mb" == "0" ]]; then
      echo "Skip $src_cluster->$idx (no bandwidth entry)"
      continue
    fi
    bw_mbit="$(mbps_to_mbit "$bw_mb")"
    class_minor=$((100 + idx))
    classid="1:${class_minor}"
    tc class add dev "$iface" parent 1:1 classid "$classid" htb rate "${bw_mbit}mbit" ceil "${bw_mbit}mbit"
    tc filter add dev "$iface" protocol ip parent 1:0 prio 1 u32 match ip dst "${dst_ip}/32" flowid "$classid"
    echo "Limit dst=${dst_ip} cluster=${idx} bw=${bw_mb}MB/s (${bw_mbit}mbit)"
  done

  echo "Done. Current qdisc/class/filter:"
  tc qdisc show dev "$iface"
  tc class show dev "$iface"
  tc filter show dev "$iface"
}

main "$@"

