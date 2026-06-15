#!/bin/bash
if [ -z "${BASH_VERSION:-}" ]; then
  exec /bin/bash "$0" "$@"
fi
set -euo pipefail

# Real bandwidth shaping by tc/htb (egress).
# Matrix: /users/xue/xue/project/config/BW_limit — TABLE II MB/s (symmetric from upper triangle + diagonal),
#   converted to Mbit/s for tc via BW_MATRIX_MB_PER_SEC_TO_TC_MBIT (default ×8).
# get_bw_mbps(src,dst) is a legacy name: it returns tc rate in Mbit/s (see get_bw_tc_mbit_rate in BW_limit).
# 输出：默认一行摘要；BW_MATRIX_VERBOSE=1 打印每条 dst；=2 再 dump tc。

BW_FILE="/users/xue/xue/project/config/BW_limit"
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

# Proxy IPs — one per cluster (6 clusters, id 0-5)
CLUSTER_IPS=(
  "10.10.1.3"   # 0
  "10.10.1.12"  # 1
  "10.10.1.21"  # 2
  "10.10.1.30"  # 3
  "10.10.1.39"  # 4
  "10.10.1.48"  # 5
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
    # Also check datanode IPs
    for i in "${!ALL_CLUSTER_IPS[@]}"; do
      for cip in ${ALL_CLUSTER_IPS[$i]}; do
        if [[ "$cip" == "$local_ip" ]]; then
          echo "$i"
          return 0
        fi
      done
    done
  done
  return 1
}

mbps_to_tc_mbit() {
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

  local idx dst_ips bw_mbps bw_mbit class_minor classid rules=0
  for idx in "${!CLUSTER_IPS[@]}"; do
    dst_ips="${ALL_CLUSTER_IPS[$idx]}"
    if [[ "$idx" == "$src_cluster" ]]; then
      # Same cluster: use diagonal bandwidth for intra-cluster traffic
      bw_mbps="$(get_bw_mbps "$src_cluster" "$idx")"
      if [[ -z "$bw_mbps" || "$bw_mbps" == "0" ]]; then
        ((v >= 1)) && echo "Skip intra-cluster $src_cluster (no diagonal entry)"
        continue
      fi
      bw_mbit="$(mbps_to_tc_mbit "$bw_mbps")"
      class_minor=$((200 + idx))
      classid="1:${class_minor}"
      tc class add dev "$iface" parent 1: classid "$classid" htb rate "${bw_mbit}mbit" ceil "${bw_mbit}mbit"
      for dst_ip in $dst_ips; do
        if [[ "$dst_ip" == "${CLUSTER_IPS[$src_cluster]}" ]]; then
          continue  # skip own proxy IP (local machine)
        fi
        # Also skip if this IP is on the local machine
        local skip_self=0
        for lip in $(hostname -I 2>/dev/null); do
          [[ "$lip" == "$dst_ip" ]] && skip_self=1 && break
        done
        [[ $skip_self -eq 1 ]] && continue
        tc filter add dev "$iface" protocol ip parent 1:0 prio 1 u32 match ip dst "${dst_ip}/32" flowid "$classid"
        ((rules++)) || true
        ((v >= 1)) && echo "Intra-cluster dst=${dst_ip} cluster=${idx} tc_rate=${bw_mbit}mbit (diagonal ${bw_mbps} MB/s)"
      done
    else
      # Cross cluster: use off-diagonal bandwidth
      bw_mbps="$(get_bw_mbps "$src_cluster" "$idx")"
      if [[ -z "$bw_mbps" || "$bw_mbps" == "0" ]]; then
        ((v >= 1)) && echo "Skip $src_cluster->$idx (no bandwidth entry)"
        continue
      fi
      bw_mbit="$(mbps_to_tc_mbit "$bw_mbps")"
      class_minor=$((100 + idx))
      classid="1:${class_minor}"
      tc class add dev "$iface" parent 1: classid "$classid" htb rate "${bw_mbit}mbit" ceil "${bw_mbit}mbit"
      for dst_ip in $dst_ips; do
        tc filter add dev "$iface" protocol ip parent 1:0 prio 1 u32 match ip dst "${dst_ip}/32" flowid "$classid"
        ((rules++)) || true
        ((v >= 1)) && echo "Cross-cluster dst=${dst_ip} cluster=${idx} tc_rate=${bw_mbit}mbit (matrix[${src_cluster}][${idx}]=${bw_mbps} MB/s)"
      done
    fi
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
