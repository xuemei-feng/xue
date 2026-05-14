#!/bin/bash

# 定义配置文件目录
CONFIG_DIR="/users/xue/xue/project/config"

# 定义 hosts 文件路径
HOSTS_FILE="hosts"

# 检查 hosts 文件是否存在
if [ ! -f "$HOSTS_FILE" ]; then
  echo "Error: hosts file not found!"
  exit 1
fi

# 读取 hosts 文件中的主机列表
HOSTS=$(cat "$HOSTS_FILE")

# 提示用户输入字符串 XXX（POSIX：不用 read -p，避免 dash 报错）
printf "Enter the string XXX: "
read -r XXX

# 检查输入的字符串是否为空
if [ -z "$XXX" ]; then
  echo "Error: Input string cannot be empty!"
  exit 1
fi

# 定义源文件和目标文件路径
SOURCE_FILE="$CONFIG_DIR/$XXX.xml"
TARGET_FILE="$CONFIG_DIR/parameterConfiguration.xml"

# 检查源文件是否存在
if [ ! -f "$SOURCE_FILE" ]; then
  echo "Error: Source file $SOURCE_FILE does not exist!"
  exit 1
fi

# 在本地主机上复制并覆盖目标文件
echo "Copying $SOURCE_FILE to $TARGET_FILE on the local host..."
cp "$SOURCE_FILE" "$TARGET_FILE"
if [ $? -ne 0 ]; then
  echo "Failed to copy $SOURCE_FILE to $TARGET_FILE on the local host!"
  exit 1
fi

# 使用 rsync 将修改后的文件同步到所有远程主机
echo "Syncing $TARGET_FILE to all remote hosts..."
for host in $HOSTS; do
  echo "Syncing to $host..."
  rsync -avz --progress "$TARGET_FILE" "$host:$TARGET_FILE"
  if [ $? -ne 0 ]; then
    echo "Failed to sync $TARGET_FILE to $host!"
    exit 1
  fi
done

echo "Successfully synced $TARGET_FILE to all remote hosts!"
echo "All done!"