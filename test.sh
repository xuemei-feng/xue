#!/bin/bash

# xue 批量更新请求文件路径（实验时只需改这一行）
XUE_UPDATE_REQUEST_FILE="/users/xue/xue/T00-1MB-1000-10log"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_XML="${SCRIPT_DIR}/project/config/parameterConfiguration.xml"
# 条带放置数量：修改 parameterConfiguration.xml 中的 ClientStripeNum
CLIENT_STRIPE_NUM="$(sed -n 's:.*<ClientStripeNum>\([0-9][0-9]*\)</ClientStripeNum>.*:\1:p' "${CONFIG_XML}" | head -1)"
if [ -z "${CLIENT_STRIPE_NUM}" ]; then
  CLIENT_STRIPE_NUM=100
fi

pkill -9 main_client 2>/dev/null || true

cd "${SCRIPT_DIR}" || exit 1

echo "ClientStripeNum=${CLIENT_STRIPE_NUM} (from ${CONFIG_XML})"
echo "XUE update file: ${XUE_UPDATE_REQUEST_FILE}"
echo y | ./project/cmake/build/main_client "${XUE_UPDATE_REQUEST_FILE}"
