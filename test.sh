#!/usr/bin/env bash
# CoRD 批量更新 trace：改下面路径即可，每行格式见 main_client.cpp 用法说明
CORD_TRACE_FILE="/root/xue/T00-1MB-1000-10log"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_XML="${SCRIPT_DIR}/project/config/parameterConfiguration.xml"
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

# CoRD batch：逐 stripe 串行（plan + upload + xfer 全部完成后再处理下一条）
export CORD_BATCH_THREADS="${CORD_BATCH_THREADS:-1}"
echo "CoRD batch trace: ${CORD_TRACE_FILE}"
echo "ClientStripeNum=${CLIENT_STRIPE_NUM} (from ${CONFIG_XML})"
echo "CordRequestTimeoutSec=${CORD_REQUEST_TIMEOUT_SEC} (from ${CONFIG_XML})"
echo "CoRD batch mode: serial one-stripe-at-a-time"
echo y | "${MAIN_CLIENT}" "${CORD_TRACE_FILE}"
