#!/bin/bash
# 验证「一台机器一个角色」：SSH 目标应对应不同物理机，且本机配置了 role IP。
[ -n "${BASH_VERSION:-}" ] || exec bash "$0" "$@"
set -euo pipefail

ROOT_DIR="/users/xue/xue"
HOSTS_FILE="${HOSTS_FILE:-${ROOT_DIR}/hosts}"
CLUSTER_XML="${ROOT_DIR}/project/config/clusterInformation.xml"
USER="${VERIFY_USER:-root}"
PARALLEL="${VERIFY_PARALLEL:-8}"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o BatchMode=yes -o ConnectTimeout=8"

usage() {
  echo "Usage: bash verify_role_hosts.sh [hosts_file]"
  echo "Checks: (1) SSH lands on distinct hostnames (2) role IP exists on that machine"
  exit 1
}

[[ $# -le 1 ]] || usage
[[ $# -eq 1 ]] && HOSTS_FILE="$1"
[[ -f "$HOSTS_FILE" ]] || { echo "Error: missing $HOSTS_FILE"; exit 1; }

read_role_ip() {
  local line="$1"
  line="${line%%#*}"
  line="${line//[[:space:]]/}"
  [[ -n "$line" ]] || return 1
  # 支持两列：ssh_host role_ip；单列时 ssh 与 role 相同
  if [[ "$line" =~ ^[^[:space:]]+[[:space:]]+[^[:space:]]+$ ]]; then
    echo "$line" | awk '{print $2}'
  else
    echo "$line" | awk '{print $1}'
  fi
}

read_ssh_host() {
  local line="$1"
  line="${line%%#*}"
  line=$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
  [[ -n "$line" ]] || return 1
  echo "$line" | awk '{print $1}'
}

mapfile -t HOST_LINES < <(grep -v '^[[:space:]]*#' "$HOSTS_FILE" | grep -v '^[[:space:]]*$' || true)
if [[ ${#HOST_LINES[@]} -eq 0 ]]; then
  echo "Error: no hosts in $HOSTS_FILE"
  exit 1
fi

echo "Verifying ${#HOST_LINES[@]} entries from $HOSTS_FILE (parallel=$PARALLEL)..."

ok_file="$(mktemp)"
fail_file="$(mktemp)"
hostname_file="$(mktemp)"
trap 'rm -f "$ok_file" "$fail_file" "$hostname_file"' EXIT

check_one() {
  local line="$1"
  local ssh_host role_ip
  ssh_host="$(read_ssh_host "$line")" || return 0
  role_ip="$(read_role_ip "$line")" || return 0

  local out rc=0 has_ip
  out="$(ssh -n $SSH_OPTS -l "$USER" "$ssh_host" \
    "hostname; if ip -4 addr show 2>/dev/null | grep -qE 'inet ${role_ip}/'; then echo HAS_IP; else echo NO_IP; fi" \
    2>&1)" || rc=$?

  if [[ "$rc" -ne 0 ]]; then
    echo "FAIL ssh $ssh_host (role=$role_ip): $out" >>"$fail_file"
    return 0
  fi

  local hname has_ip_flag
  hname="$(echo "$out" | sed -n '1p')"
  has_ip_flag="$(echo "$out" | sed -n '2p')"

  if [[ "$has_ip_flag" != "HAS_IP" ]]; then
    echo "FAIL $ssh_host role=$role_ip hostname=$hname: role IP not configured on this machine" >>"$fail_file"
    return 0
  fi

  echo "$role_ip $ssh_host $hname" >>"$ok_file"
  echo "$hname" >>"$hostname_file"
}

export -f check_one read_role_ip read_ssh_host
export USER SSH_OPTS ok_file fail_file

set +e
printf '%s\n' "${HOST_LINES[@]}" | xargs -P "$PARALLEL" -I{} bash -c 'check_one "$1"' _ {}
set -e

ok_count="$(wc -l < "$ok_file" | tr -d ' ')"
fail_count="$(wc -l < "$fail_file" | tr -d ' ')"
dup_count="$(sort "$hostname_file" | uniq -d | wc -l | tr -d ' ')"

echo ""
echo "Result: ok=$ok_count fail=$fail_count duplicate_hostnames=$dup_count"

if [[ "$dup_count" -gt 0 ]]; then
  echo ""
  echo "[ERROR] Multiple role IPs SSH to the SAME physical hostname (not one-machine-one-role):"
  sort "$hostname_file" | uniq -d | while read -r hn; do
    echo "  hostname=$hn <= $(grep " $hn\$" "$ok_file" | awk '{print $1}' | paste -sd, -)"
  done
fi

if [[ "$fail_count" -gt 0 ]]; then
  echo ""
  echo "Failed entries:"
  cat "$fail_file"
fi

if [[ "$fail_count" -eq 0 && "$dup_count" -eq 0 ]]; then
  echo ""
  echo "OK: each role IP reaches a distinct machine with that IP configured locally."
  exit 0
fi

echo ""
echo "Fix network/hosts before start_datanode/start_proxy:"
echo "  - Each physical node should have ONLY its role 10.10.1.x (not the full alias range on every node)."
echo "  - ssh <target> must land on the machine that owns that role IP."
exit 1
