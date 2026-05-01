#!/bin/bash

cd /users/xue
sudo chmod 777 -R UniLRC
cd UniLRC


# 定义源文件夹路径
SOURCE_DIR="/users/xue/xue"

# 定义 hosts 文件路径
HOSTS_FILE="hosts"

# 定义远程目标文件夹路径
REMOTE_DIR="/users/xue/xue"

# 检查 hosts 文件是否存在
if [[ ! -f "$HOSTS_FILE" ]]; then
    echo "Error: hosts file not found!"
    exit 1
fi

# 遍历 hosts 文件中的每个 IP 地址
while read -r ip; do

    echo "Copying to host: $ip..."

    # 使用 rsync 复制文件夹
    sudo rsync -avz  --exclude='project/cmake/build/CMakeFiles' --exclude='project/cmake/build/run_client' --exclude='project/cmake/build/main_test' --exclude='project/cmake/build/main_client' --exclude='storage/*' -e ssh "$SOURCE_DIR/" "$ip:$REMOTE_DIR/"
    #rsync -avz -e ssh "$SOURCE_DIR/" "$ip:$REMOTE_DIR/"

    # 检查 rsync 是否成功
    if [ $? -eq 0 ]; then
        echo "Successfully copied to $ip!"
    else
        echo "Failed to copy to $ip!"
    fi

done < "$HOSTS_FILE"

cd /users/xue/xue
sh generate_run_proxy.sh

echo "All done!"