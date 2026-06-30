pkill -9 run_datanode 2>/dev/null || true
pkill -9 run_proxy 2>/dev/null || true

./project/cmake/build/run_datanode 172.16.3.3:17619 & 

