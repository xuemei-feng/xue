#!/bin/bash
# Sync repo to remote hosts. Run: bash update_all.sh
# (Do not use `sh`; use bash — see shebang.)

# 本机与远程统一写死的路径（与 REMOTE_DIR 一致）
REPO_ROOT="/users/xue/xue"

cd "$REPO_ROOT" || exit 1

SOURCE_DIR="$REPO_ROOT"
HOSTS_FILE="$REPO_ROOT/hosts"
REMOTE_DIR="$REPO_ROOT"

if [ ! -f "$HOSTS_FILE" ]; then
    echo "Error: hosts file not found at $HOSTS_FILE"
    exit 1
fi

while read -r ip; do
    [ -z "$ip" ] && continue
    case "$ip" in \#*) continue ;; esac

    echo "Copying to host: $ip..."

    sudo rsync -avz \
        --exclude='project/cmake/build/CMakeFiles' \
        --exclude='project/cmake/build/run_client' \
        --exclude='project/cmake/build/main_test' \
        --exclude='project/cmake/build/main_client' \
        --exclude='storage/*' \
        -e ssh "$SOURCE_DIR/" "$ip:$REMOTE_DIR/"

    if [ $? -eq 0 ]; then
        echo "Successfully copied to $ip!"
    else
        echo "Failed to copy to $ip!"
    fi

done < "$HOSTS_FILE"

bash "$REPO_ROOT/generate_run_proxy.sh"

echo "All done!"
