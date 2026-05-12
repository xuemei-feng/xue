#!/usr/bin/env bash
# 在仓库根目录执行，或从任意目录: bash /path/to/update_all.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

SOURCE_DIR="$SCRIPT_DIR"
HOSTS_FILE="$SCRIPT_DIR/hosts"
REMOTE_DIR="$SCRIPT_DIR"

if [[ ! -f "$HOSTS_FILE" ]]; then
    echo "Error: hosts file not found: $HOSTS_FILE"
    exit 1
fi

# 遍历 hosts 文件中的每个 IP 地址
while read -r ip; do
    # 跳过空行与注释
    [[ -z "${ip// }" || "$ip" =~ ^# ]] && continue

    echo "Copying to host: $ip..."

    sudo rsync -avz \
        --exclude='project/build/CMakeFiles' \
        --exclude='project/build/run_client' \
        --exclude='project/build/main_test' \
        --exclude='project/build/main_client' \
        --exclude='project/cmake/build' \
        --exclude='storage/*' \
        -e ssh "$SOURCE_DIR/" "$ip:$REMOTE_DIR/"

    if [[ $? -eq 0 ]]; then
        echo "Successfully copied to $ip!"
    else
        echo "Failed to copy to $ip!"
    fi

done < "$HOSTS_FILE"

if [[ -f "$SCRIPT_DIR/generate_run_proxy.sh" ]]; then
    cd "$SCRIPT_DIR"
    sh generate_run_proxy.sh
else
    echo "Warning: generate_run_proxy.sh not found under $SCRIPT_DIR"
fi

echo "All done!"
