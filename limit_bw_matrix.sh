#!/bin/bash
if [ -z "${BASH_VERSION:-}" ]; then
  exec /bin/bash "$0" "$@"
fi
set -euo pipefail

# Real bandwidth shaping by tc/htb (egress + ingress via IFB redirect).
# Matrix: /users/xue/xue/project/config/BW_limit same — TABLE II MB/s (symmetric from upper triangle + diagonal),
#   converted to Mbit/s for tc via BW_MATRIX_MB_PER_SEC_TO_TC_MBIT (default ×8).
# get_bw_mbps(src,dst) is a legacy name: it returns tc rate in Mbit/s (see get_bw_tc_mbit_rate in BW_limit same).
# Egress: HTB on iface root, match ip dst per remote cluster.
# Ingress: ingress qdisc mirrors to IFB; HTB on IFB root, match ip src per remote cluster.
# 输出：默认一行摘要；BW_MATRIX_VERBOSE=1 打印每条 peer；=2 再 dump tc。

BW_FILE="/users/xue/xue/project/config/BW_limitsame"
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

# 与 proxy_hosts / clusterInformation.xml 中 6 个 proxy 一致（一机一角色）
CLUSTER_IPS=(
  "10.10.1.3"   # 0: TYO  cluster 0 proxy
  "10.10.1.12"   # 1: MEL  cluster 1 proxy
  "10.10.1.21"   # 2: SG  cluster 2 proxy
  "10.10.1.30"   # 3: SEO  cluster 3 proxy
  "10.10.1.39"   # 4: JAK  cluster 4 proxy
  "10.10.1.48"   # 5: HK  cluster 5 proxy
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

mbps_to_tc_mbit() {
  awk -v m="$1" 'BEGIN { printf "%.3f", m + 0.0 }'
}

IFB_DEV="ifb0"
# HTB token bucket burst (helps short gRPC bursts reach rate cap). Override: BW_MATRIX_HTB_BURST=512k
HTB_BURST="${BW_MATRIX_HTB_BURST:-256k}"

cleanup_tc() {
  local iface=$1
  tc qdisc del dev "$iface" root 2>/dev/null || true
  tc qdisc del dev "$iface" ingress 2>/dev/null || true
  tc qdisc del dev "$IFB_DEV" root 2>/dev/null || true
  ip link del "$IFB_DEV" 2>/dev/null || true
}

ensure_ifb() {
  modprobe ifb numifbs=1 2>/dev/null || true
  if ! ip link show "$IFB_DEV" &>/dev/null; then
    ip link add "$IFB_DEV" type ifb
  fi
  ip link set dev "$IFB_DEV" up
}

# Apply per-peer HTB on dev. direction: dst (egress) or src (ingress).
apply_peer_htb() {
  local dev=$1 handle=$2 default_minor=$3 src_cluster=$4 direction=$5 v=$6
  local rules=0 idx peer_ip bw_mbps bw_mbit class_minor classid match_kw

  match_kw="$direction"
  tc qdisc add dev "$dev" root handle "${handle}:" htb default "$default_minor"
  tc class add dev "$dev" parent "${handle}:" classid "${handle}:1" htb rate 10000mbit ceil 10000mbit
  tc class add dev "$dev" parent "${handle}:" classid "${handle}:${default_minor}" htb rate 10000mbit ceil 10000mbit

  for idx in "${!CLUSTER_IPS[@]}"; do
    if [[ "$idx" == "$src_cluster" ]]; then
      continue
    fi
    peer_ip="${CLUSTER_IPS[$idx]}"
    bw_mbps="$(get_bw_mbps "$src_cluster" "$idx")"
    if [[ -z "$bw_mbps" || "$bw_mbps" == "0" ]]; then
      ((v >= 1)) && echo "Skip ${direction} peer cluster=${idx} (no bandwidth entry)" >&2
      continue
    fi
    bw_mbit="$(mbps_to_tc_mbit "$bw_mbps")"
    class_minor=$((100 + idx))
    classid="${handle}:${class_minor}"
    tc class add dev "$dev" parent "${handle}:" classid "$classid" \
      htb rate "${bw_mbit}mbit" ceil "${bw_mbit}mbit" burst "$HTB_BURST" cburst "$HTB_BURST"
    tc filter add dev "$dev" protocol ip parent "${handle}:0" prio 1 u32 match ip "$match_kw" "${peer_ip}/32" flowid "$classid"
    ((rules++)) || true
    if ((v >= 1)); then
      if [[ "$direction" == "dst" ]]; then
        echo "Egress limit dst=${peer_ip} cluster=${idx} tc_rate=${bw_mbit}mbit (matrix MB/s×${BW_MATRIX_MB_PER_SEC_TO_TC_MBIT:-8})" >&2
      else
        echo "Ingress limit src=${peer_ip} cluster=${idx} tc_rate=${bw_mbit}mbit (matrix MB/s×${BW_MATRIX_MB_PER_SEC_TO_TC_MBIT:-8})" >&2
      fi
    fi
  done
  echo "$rules"
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

  cleanup_tc "$iface"

  local egress_rules ingress_rules
  egress_rules="$(apply_peer_htb "$iface" 1 999 "$src_cluster" dst "$v")"
  ensure_ifb
  tc qdisc add dev "$iface" handle ffff: ingress
  tc filter add dev "$iface" parent ffff: protocol ip u32 match u32 0 0 action mirred egress redirect dev "$IFB_DEV"
  ingress_rules="$(apply_peer_htb "$IFB_DEV" 2 998 "$src_cluster" src "$v")"

  echo "OK bw-matrix dev=${iface} ifb=${IFB_DEV} host=${CLUSTER_IPS[$src_cluster]} cluster_id=${src_cluster} egress_rules=${egress_rules} ingress_rules=${ingress_rules}"

  if ((v >= 2)); then
    echo "--- tc qdisc show dev $iface ---"
    tc qdisc show dev "$iface"
    echo "--- tc class show dev $iface ---"
    tc class show dev "$iface"
    echo "--- tc filter show dev $iface ---"
    tc filter show dev "$iface"
    echo "--- tc qdisc show dev $IFB_DEV ---"
    tc qdisc show dev "$IFB_DEV"
    echo "--- tc class show dev $IFB_DEV ---"
    tc class show dev "$IFB_DEV"
    echo "--- tc filter show dev $IFB_DEV ---"
    tc filter show dev "$IFB_DEV"
  fi
}

main "$@"