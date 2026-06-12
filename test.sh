#!/bin/bash

# xue 批量更新请求文件路径（实验时只需改这一行）/users/
XUE_UPDATE_REQUEST_FILE="/users/xue/xue/stripe-Ten/T-64KB-10/T00-100.txt"

pkill -9 main_client

# gdb ./project/cmake/build/main_client
echo y | ./project/cmake/build/main_client "${XUE_UPDATE_REQUEST_FILE}"
