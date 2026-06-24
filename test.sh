#!/bin/sh
# 批量 Parix 更新请求文件（改 UPDATE_BATCH_FILE）
UPDATE_BATCH_FILE="/root/xue/T00-1MB-1000-10log"

# 预写条带数（main_client 启动时 SET 的条带数量）：改 project/config/parameterConfiguration.xml 里的 ClientStripeNum
# 有效 stripe_id 范围为 [0, ClientStripeNum)；batch 中 stripe_id >= ClientStripeNum 的更新会失败
CONFIG_FILE="project/config/parameterConfiguration.xml"

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
cd "$SCRIPT_DIR" || exit 1

case $UPDATE_BATCH_FILE in
  /*) ;;
  *) UPDATE_BATCH_FILE="$SCRIPT_DIR/$UPDATE_BATCH_FILE" ;;
esac

if [ ! -f "$UPDATE_BATCH_FILE" ]; then
  echo "ERROR: batch file not found: $UPDATE_BATCH_FILE" >&2
  exit 1
fi

if [ ! -f "$CONFIG_FILE" ]; then
  echo "ERROR: config not found: $SCRIPT_DIR/$CONFIG_FILE" >&2
  exit 1
fi

CLIENT_STRIPE_NUM=$(sed -n 's/.*<ClientStripeNum>\([0-9][0-9]*\)<\/ClientStripeNum>.*/\1/p' "$CONFIG_FILE" | head -1)
if [ -z "$CLIENT_STRIPE_NUM" ]; then
  echo "WARN: ClientStripeNum not set in $CONFIG_FILE (main_client uses default from config.h)" >&2
else
  echo "ClientStripeNum=$CLIENT_STRIPE_NUM  (edit $CONFIG_FILE to change pre-SET stripe count)"
fi

MAIN_CLIENT=./project/cmake/build/main_client
if [ ! -x "$MAIN_CLIENT" ]; then
  echo "ERROR: main_client not found: $SCRIPT_DIR/$MAIN_CLIENT" >&2
  exit 1
fi

pkill -9 main_client 2>/dev/null || true

echo "batch_file=$UPDATE_BATCH_FILE"
printf 'y\n' | "$MAIN_CLIENT" "$UPDATE_BATCH_FILE"
