#!/bin/bash

# xue 批量更新请求文件路径（实验时只需改这一行）/users/
XUE_UPDATE_REQUEST_FILE="/users/xue/xue/1"

# 并行 stripe 数（不同 stripe 同时跑；同一 stripe 在文件里重复会出现 warning）
# 串行：export XUE_UPDATE_PARALLEL=1
: "${XUE_UPDATE_PARALLEL:=4}"
# 单请求墙钟超时（秒）；并行默认 120
: "${XUE_UPDATE_REQUEST_TIMEOUT_SEC:=120}"
# Client gRPC 等待 ingress/commit 默认 120s；Proxy schedule 默认 30s
: "${XUE_GRPC_WAIT_DEADLINE_MS:=120000}"
: "${XUE_GRPC_UPLOAD_DEADLINE_MS:=30000}"
: "${XUE_GRPC_PROXY_SCHEDULE_DEADLINE_MS:=30000}"

pkill -9 main_client

export XUE_UPDATE_PARALLEL
export XUE_UPDATE_REQUEST_TIMEOUT_SEC
export XUE_GRPC_WAIT_DEADLINE_MS
export XUE_GRPC_UPLOAD_DEADLINE_MS
export XUE_GRPC_PROXY_SCHEDULE_DEADLINE_MS
echo y | ./project/cmake/build/main_client "${XUE_UPDATE_REQUEST_FILE}"
