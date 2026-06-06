#!/bin/bash

# xue 批量更新请求文件路径（实验时只需改这一行）
XUE_UPDATE_REQUEST_FILE="/users/xue/xue/stripe-Ten/T-64KB-12/T00-result10000.txt"

pkill -9 main_client

# gdb ./project/cmake/build/main_client
echo y | ./project/cmake/build/main_client "${XUE_UPDATE_REQUEST_FILE}"
