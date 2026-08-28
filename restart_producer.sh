#!/bin/bash
# 阶段四：生产者迁移到 venv（/opt/module/mall_rt_env）重启
pids=$(pgrep -f "mock_producer.py" || true)
for p in $pids; do
  kill "$p" 2>/dev/null && echo "killed old producer: $p"
done
sleep 2
cd /opt/script/mall_rt
nohup /opt/module/mall_rt_env/bin/python mock_producer.py > producer.log 2>&1 &
sleep 6
echo "--- 生产者状态 ---"
pgrep -af "mock_producer.py" || echo "PRODUCER NOT RUNNING"
tail -2 producer.log
