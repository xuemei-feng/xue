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
COMMON_ARGS=(--config "${CONFIG}")

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

echo "=== Done (fail=${fail}) logs in ${LOG_DIR} ==="
exit "${fail}"
