#!/usr/bin/env bash
# CoRD 批量更新 trace：改下面路径即可，每行格式见 main_client.cpp 用法说明
CORD_TRACE_FILE="/root/xue/log/log/A00-1MB-100-12-20000log"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_XML="${SCRIPT_DIR}/project/config/parameterConfiguration.xml"
HOSTS_FILE="${SCRIPT_DIR}/hosts"

resolve_client_ip() {
  if [ -n "${CORD_CLIENT_IP:-}" ]; then
    echo "${CORD_CLIENT_IP}"
    return 0
  fi
  local hosts_ip=""
  if [ -f "${HOSTS_FILE}" ]; then
    hosts_ip="$(head -1 "${HOSTS_FILE}" | tr -d '[:space:]')"
  fi
  if [ -n "${hosts_ip}" ] && ip -4 addr show 2>/dev/null | grep -qE "inet ${hosts_ip}/"; then
    echo "${hosts_ip}"
    return 0
  fi
  local prefix=""
  if [ -n "${hosts_ip}" ]; then
    prefix="$(echo "${hosts_ip}" | cut -d. -f1-3)"
  else
    prefix="$(sed -n 's:.*<CoordinatorIP>\([0-9]*\.[0-9]*\.[0-9]*\)\.[0-9]*</CoordinatorIP>.*:\1:p' "${CONFIG_XML}" | head -1)"
  fi
  if [ -n "${prefix}" ]; then
    local detected=""
    detected="$(ip -4 -o addr show scope global 2>/dev/null | awk -v p="${prefix}" '$4 ~ "^"p"." {print $4; exit}' | cut -d/ -f1)"
    if [ -n "${detected}" ]; then
      echo "${detected}"
      return 0
    fi
  fi
  if [ -n "${hosts_ip}" ]; then
    echo "${hosts_ip}"
  fi
}

CLIENT_IP="$(resolve_client_ip)"
CLIENT_PORT="${CORD_CLIENT_PORT:-77777}"
# 条带放置数量：修改 parameterConfiguration.xml 中的 ClientStripeNum
CLIENT_STRIPE_NUM="$(sed -n 's:.*<ClientStripeNum>\([0-9][0-9]*\)</ClientStripeNum>.*:\1:p' "${CONFIG_XML}" | head -1)"
if [ -z "${CLIENT_STRIPE_NUM}" ]; then
  CLIENT_STRIPE_NUM=100
fi
CORD_REQUEST_TIMEOUT_SEC="$(sed -n 's:.*<CordRequestTimeoutSec>\([0-9][0-9]*\)</CordRequestTimeoutSec>.*:\1:p' "${CONFIG_XML}" | head -1)"
if [ -z "${CORD_REQUEST_TIMEOUT_SEC}" ]; then
  CORD_REQUEST_TIMEOUT_SEC=2
fi

# 必须用相对路径启动 main_client，否则 config 路径拼接会出错（见 main_client.cpp）
MAIN_CLIENT="./project/cmake/build/main_client"

pkill -9 main_client 2>/dev/null || true

cd "${SCRIPT_DIR}" || exit 1

if [ ! -x "${MAIN_CLIENT}" ]; then
  echo "main_client not found or not executable: ${SCRIPT_DIR}/${MAIN_CLIENT#./}" >&2
  echo "Build first: cd ${SCRIPT_DIR}/project/cmake/build && make main_client -j4" >&2
  exit 1
fi

if [ ! -f "${CORD_TRACE_FILE}" ]; then
  echo "Trace file not found: ${CORD_TRACE_FILE}" >&2
  exit 1
fi

# 并行 batch：CORD_BATCH_THREADS（默认 1）；CORD_PIPELINE_XFER=0（默认）每条 stripe 等 xfer 完成后再跑下一条
export CORD_BATCH_THREADS="${CORD_BATCH_THREADS:-1}"
export CORD_PIPELINE_XFER="${CORD_PIPELINE_XFER:-0}"
export CORD_UPDATE_SLICE_PARALLEL="${CORD_UPDATE_SLICE_PARALLEL:-1}"
echo "CoRD batch trace: ${CORD_TRACE_FILE}"
echo "ClientStripeNum=${CLIENT_STRIPE_NUM} (from ${CONFIG_XML})"
echo "CordRequestTimeoutSec=${CORD_REQUEST_TIMEOUT_SEC} (from ${CONFIG_XML})"
echo "CORD_BATCH_THREADS=${CORD_BATCH_THREADS}"
echo "CORD_PIPELINE_XFER=${CORD_PIPELINE_XFER}"
echo "CORD_UPDATE_SLICE_PARALLEL=${CORD_UPDATE_SLICE_PARALLEL}"
echo "Client IP=${CLIENT_IP} (override with CORD_CLIENT_IP)"
echo "Client port=${CLIENT_PORT} (override with CORD_CLIENT_PORT)"
echo y | "${MAIN_CLIENT}" --config "${CONFIG_XML}" --ip "${CLIENT_IP}" --port "${CLIENT_PORT}" "${CORD_TRACE_FILE}"
