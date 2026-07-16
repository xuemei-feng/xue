pkill -9 run_datanode 2>/dev/null || true
pkill -9 run_proxy 2>/dev/null || true

nohup ./project/cmake/build/run_datanode 172.16.2.113:17621 >>/tmp/unilrc-datanode.log 2>&1 &

