#!/bin/bash

# 检查enp6s0f0是否存在且处于UP状态
if ip link show enp6s0f0 >/dev/null 2>&1 && \
   ip link show enp6s0f0 | grep -q 'state UP'
then
    wondershaper -a enp6s0f0 -d 512000 -u 512000
    exit 0
fi

# 检查enp6s0f1是否存在且处于UP状态
if ip link show enp6s0f1 >/dev/null 2>&1 && \
   ip link show enp6s0f1 | grep -q 'state UP'
then
    wondershaper -a enp6s0f1 -d 512000 -u 512000
    exit 0
fi

# 如果都不满足则报错
echo "Error: No active interface found!" >&2
exit 1