#!/usr/bin/env bash
# Run N main_client processes in parallel, each on disjoint stripe_id subsets.
#
# Usage:
#   ./run_multi_client_parix.sh [BATCH_FILE] [NUM_CLIENTS] [CLIENT_IP] [BASE_PORT]
#
# Example:
#   ./run_multi_client_parix.sh /root/xue/T00-1MB-1000-10log 4 172.16.2.31 77777
#
# Notes:
# - Each client MUST use a unique --port (BASE_PORT + worker_id).
# - Phase 1 worker 0 runs SET (--set-only); Phase 2 all workers use --skip-set.
# - Client + ALL proxies must be rebuilt with Parix TCP header (PRX1) support.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
BATCH_FILE="${1:-${ROOT_DIR}/T00-1MB-1000-10log}"
NUM_CLIENTS="${2:-4}"
CLIENT_IP="${3:-172.16.2.31}"
BASE_PORT="${4:-77777}"
BUILD="${ROOT_DIR}/project/build/main_client"
CONFIG="${ROOT_DIR}/project/config/parameterConfiguration.xml"
SPLIT_PREFIX="${ROOT_DIR}/tmp/parix_batch_split_"
LOG_DIR="${ROOT_DIR}/tmp/parix_multi_client_logs"
COMMON_ARGS=(--config "${CONFIG}" --delay-ms 50)

if [[ ! -x "${BUILD}" ]]; then
  echo "missing executable: ${BUILD}" >&2
  echo "run: cd ${ROOT_DIR}/project/build && cmake .. && make -j main_client run_proxy" >&2
  exit 1
fi
if [[ ! -f "${CONFIG}" ]]; then
  echo "missing config: ${CONFIG}" >&2
  exit 1
fi

mkdir -p "${ROOT_DIR}/tmp" "${LOG_DIR}"
python3 "${ROOT_DIR}/split_parix_batch.py" "${BATCH_FILE}" "${NUM_CLIENTS}" "${SPLIT_PREFIX}"

echo "=== Phase 1: initialize stripes (worker 0 only) ==="
"${BUILD}" "${SPLIT_PREFIX}0.log" \
  "${COMMON_ARGS[@]}" \
  --ip "${CLIENT_IP}" --port "${BASE_PORT}" \
  --tag "client0" --set-only --auto-y \
  | tee "${LOG_DIR}/client0_setonly.log"

echo "=== Phase 2: parallel batch updates ==="
pids=()
PHASE2_START=$(date +%s.%N)
for ((i = 0; i < NUM_CLIENTS; i++)); do
  PORT=$((BASE_PORT + i))
  TAG="client${i}"
  OUT_LOG="${LOG_DIR}/${TAG}.log"
  EXTRA=( "${COMMON_ARGS[@]}" --ip "${CLIENT_IP}" --port "${PORT}" --auto-y --tag "${TAG}" --skip-set )
  echo "starting ${TAG} port=${PORT} batch=${SPLIT_PREFIX}${i}.log log=${OUT_LOG}"
  echo "  tail -f ${OUT_LOG}   # live progress"
  stdbuf -oL -eL "${BUILD}" "${SPLIT_PREFIX}${i}.log" "${EXTRA[@]}" > "${OUT_LOG}" 2>&1 &
  pids+=("$!")
done

fail=0
for pid in "${pids[@]}"; do
  if ! wait "${pid}"; then
    fail=1
  fi
done
PHASE2_END=$(date +%s.%N)
PHASE2_WALL=$(echo "scale=3; ($PHASE2_END - $PHASE2_START) / 1" | bc 2>/dev/null || awk "BEGIN {printf \"%.3f\", $PHASE2_END - $PHASE2_START}")

# ── Aggregate total across all clients ──────────────────────────────────
SUMMARY_FILE="${LOG_DIR}/summary.log"
# Build summary into a variable to avoid subshell issues with | tee
total_success=0
total_fail=0
total_reqs=0
total_sum_latency=0
max_wall=0
min_wall=999999
SUMMARY=""
summary_line() { SUMMARY="${SUMMARY}$1"$'\n'; }
summary_line "$(printf "╔══════════════════════════════════════════════════════════╗")"
summary_line "$(printf "║           Multi-Client Parix Batch Summary              ║")"
summary_line "$(printf "╠══════════════════════════════════════════════════════════╣")"
summary_line "$(printf "║  Total clients            : %-27d ║" "${NUM_CLIENTS}")"
summary_line "$(printf "║  Overall end-to-end time  : %-23.3f s ║" "${PHASE2_WALL}")"
summary_line "$(printf "╠══════════════════════════════════════════════════════════╣")"
for ((i = 0; i < NUM_CLIENTS; i++)); do
  CLIENT_LOG="${LOG_DIR}/client${i}.log"
  if [[ -f "${CLIENT_LOG}" ]]; then
    # extract stats from client summary lines
    # Example: "total_requests=125 success=125 failed=0"
    #          "total_time=8.31581 s"  "wall_clock_time=14.5617 s"
    c_reqs=$(sed -n 's/.*total_requests=\([0-9]\+\).*/\1/p' "${CLIENT_LOG}" | tail -1)
    c_succ=$(sed -n 's/.*success=\([0-9]\+\).*/\1/p' "${CLIENT_LOG}" | tail -1)
    c_fail=$(sed -n 's/.*failed=\([0-9]\+\).*/\1/p' "${CLIENT_LOG}" | tail -1)
    c_wall=$(sed -n 's/.*wall_clock_time=\([0-9.]\+\).*/\1/p' "${CLIENT_LOG}" | tail -1)
    c_total=$(sed -n 's/.*total_time=\([0-9.]\+\).*/\1/p' "${CLIENT_LOG}" | tail -1)
    c_reqs=${c_reqs:-0}; c_succ=${c_succ:-0}; c_fail=${c_fail:-0}
    c_wall=${c_wall:-0}; c_total=${c_total:-0}
    total_reqs=$((total_reqs + c_reqs))
    total_success=$((total_success + c_succ))
    total_fail=$((total_fail + c_fail))
    total_sum_latency=$(awk "BEGIN {printf \"%.3f\", ${total_sum_latency} + ${c_total}}")
    if awk "BEGIN {exit !(${c_wall} > ${max_wall})}"; then max_wall="${c_wall}"; fi
    if awk "BEGIN {exit !(${c_wall} > 0 && ${c_wall} < ${min_wall})}"; then min_wall="${c_wall}"; fi
    summary_line "$(printf "║  client%-2d  success=%-5s fail=%-3s wall=%-10s s ║" "${i}" "${c_succ}" "${c_fail}" "${c_wall}")"
  fi
done
summary_line "$(printf "╠══════════════════════════════════════════════════════════╣")"
summary_line "$(printf "║  Total requests (all clients) : %-21d ║" "${total_reqs}")"
summary_line "$(printf "║  Total success (all clients)  : %-21d ║" "${total_success}")"
summary_line "$(printf "║  Total failed  (all clients)  : %-21d ║" "${total_fail}")"
summary_line "$(printf "║  Sum of per-request latencies : %-18.3f s ║" "${total_sum_latency}")"
if [[ "${total_success}" -gt 0 ]]; then
  avg_all=$(awk "BEGIN {printf \"%.3f\", ${total_sum_latency} / ${total_success}}")
  summary_line "$(printf "║  Avg per-request latency      : %-18.3f s ║" "${avg_all}")"
fi
summary_line "$(printf "║  Slowest client wall-clock    : %-18s s ║" "${max_wall}")"
summary_line "$(printf "║  Fastest client wall-clock    : %-18s s ║" "${min_wall}")"
summary_line "$(printf "╚══════════════════════════════════════════════════════════╝")"
echo "${SUMMARY}" | tee "${SUMMARY_FILE}"

echo ""
echo "=== Done (fail=${fail}) logs in ${LOG_DIR} ==="
echo "=== Overall end-to-end (Phase 2): ${PHASE2_WALL} s (all ${NUM_CLIENTS} clients) ==="
echo "=== Summary file: ${SUMMARY_FILE} ==="
exit "${fail}"
