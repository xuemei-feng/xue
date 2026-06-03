#!/bin/sh
# 批量 Parix 更新请求文件（改这一行；相对 test.sh 目录或绝对路径）
UPDATE_BATCH_FILE="/users/xue/xue/stripe Ali/A-64KB-8/A00-result10000.txt"
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

MAIN_CLIENT=./project/cmake/build/main_client
if [ ! -x "$MAIN_CLIENT" ]; then
  echo "ERROR: main_client not found: $SCRIPT_DIR/$MAIN_CLIENT" >&2
  exit 1
fi

pkill -9 main_client 2>/dev/null || true

echo "batch_file=$UPDATE_BATCH_FILE"
printf 'y\n' | "$MAIN_CLIENT" "$UPDATE_BATCH_FILE"
