#!/bin/bash
set -euo pipefail

# 必须用 bash 执行（不要用 `sh`）：脚本用了 [[ 等 bash 语法。
#   bash /users/xue/xue/update_all.sh
#   或: chmod +x update_all.sh && ./update_all.sh

SOURCE_DIR="/users/xue/xue"
HOSTS_FILE="${SOURCE_DIR}/hosts"

# 旧环境曾用 /users/xue/UniLRC；当前仓库在 SOURCE_DIR，仅当目录存在时才处理
if [[ -d /users/xue/UniLRC ]]; then
  cd /users/xue
  sudo chmod 777 -R UniLRC
  cd UniLRC
fi

if [[ ! -f "$HOSTS_FILE" ]]; then
  echo "Error: hosts file not found: $HOSTS_FILE" >&2
  exit 1
fi

REMOTE_DIR="/users/xue/xue"

while read -r ip; do
  [[ -z "${ip// }" ]] && continue
  [[ "$ip" =~ ^# ]] && continue

  echo "Copying to host: $ip..."

  if sudo rsync -avz --delete \
    --exclude='project/cmake/build/CMakeFiles' \
    --exclude='project/cmake/build/run_client' \
    --exclude='project/cmake/build/main_test' \
    --exclude='project/cmake/build/main_client' \
    --exclude='storage/*' \
    -e ssh "${SOURCE_DIR}/" "${ip}:${REMOTE_DIR}/"; then
    echo "Successfully copied to $ip!"
  else
    echo "Failed to copy to $ip!" >&2
  fi
done <"$HOSTS_FILE"

cd "$SOURCE_DIR"
sh generate_run_proxy.sh

echo "All done!"
