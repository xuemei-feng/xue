#!/bin/bash

# 定义源文件路径
SOURCE_FILE="/users/xue/xue/small_tools/generator_sh.py"

# 定义 hosts 文件路径
HOSTS_FILE="hosts"

# 检查 hosts 文件是否存在
if [ ! -f "$HOSTS_FILE" ]; then
  echo "Error: hosts file not found!"
  exit 1
fi

# 读取 hosts 文件中的主机列表
HOSTS=$(cat "$HOSTS_FILE")

# 使用 scp 复制文件到所有主机
echo "Copying $SOURCE_FILE to all hosts..."
for HOST in $HOSTS; do
  echo "Copying to $HOST..."
  scp "$SOURCE_FILE" "$HOST:/users/xue/xue/small_tools/"
  if [ $? -eq 0 ]; then
    echo "Successfully copied to $HOST!"
  else
    echo "Failed to copy to $HOST!"
    exit 1
  fi
done

# 使用 pdsh 在所有主机上运行 Python 脚本
REMOTE_COMMAND="cd /users/xue/xue/small_tools/ && python generator_sh.py"
PARALLEL=50
USER="root"

echo "Running generator_sh.py on all hosts..."
pdsh -R ssh -w ^$HOSTS_FILE -l $USER -f $PARALLEL "$REMOTE_COMMAND"

# 检查脚本是否成功运行
if [ $? -eq 0 ]; then
  echo "Successfully ran generator_sh.py on all hosts!"
else
  echo "Failed to run generator_sh.py on some hosts!"
  exit 1
fi

echo "All done!"