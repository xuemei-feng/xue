#!/bin/bash

# 定义源文件夹路径
SOURCE_DIR="/users/xue/xue/project/config"

# 定义 hosts 文件路径
HOSTS_FILE="hosts"

# 获取本机的主机名
LOCAL_HOST=$(hostname)

# 检查 hosts 文件是否存在
if [ ! -f "$HOSTS_FILE" ]; then
  echo "Error: hosts file not found!"
  exit 1
fi

# 遍历 hosts 文件中的每一行
while read -r REMOTE_HOST; do
  # 跳过空行和本机
  if [ -z "$REMOTE_HOST" ] || [ "$REMOTE_HOST" = "$LOCAL_HOST" ]; then
    continue
  fi

  echo "Copying contents of $SOURCE_DIR to $REMOTE_HOST..."

  # 使用 scp 递归复制文件夹内容
  sudo scp -r "$SOURCE_DIR"/* "$REMOTE_HOST:/users/xue/xue/project/config/"

  # 检查 scp 是否成功
  if [ $? -eq 0 ]; then
    echo "Successfully copied to $REMOTE_HOST!"
  else
    echo "Failed to copy to $REMOTE_HOST!"
  fi

done < "$HOSTS_FILE"

echo "All done!"