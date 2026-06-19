#!/usr/bin/env bash
# CoRD 批量更新 trace：改下面路径即可，每行格式见 main_client.cpp 用法说明
CORD_TRACE_FILE="/users/xue/xue/T00-64KB-100-10"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_XML="${SCRIPT_DIR}/project/config/parameterConfiguration.xml"
# 与 parameterConfiguration.xml 中 ClientStripeNum 保持一致；改条带数量请编辑该 XML
CLIENT_STRIPE_NUM="$(sed -n 's:.*<ClientStripeNum>\([0-9][0-9]*\)</ClientStripeNum>.*:\1:p' "${CONFIG_XML}" | head -1)"
if [ -z "${CLIENT_STRIPE_NUM}" ]; then
  CLIENT_STRIPE_NUM=100
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

echo "CoRD batch trace: ${CORD_TRACE_FILE}"
echo "ClientStripeNum=${CLIENT_STRIPE_NUM} (from ${CONFIG_XML})"
echo y | "${MAIN_CLIENT}" "${CORD_TRACE_FILE}"
