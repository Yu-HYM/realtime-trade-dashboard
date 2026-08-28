#!/bin/bash
cd /opt/script/mall_rt

# 停止旧进程
for pid in $(pgrep -f "uvicorn api:app"); do
  kill -9 $pid 2>/dev/null
done
for pid in $(pgrep -f "streamlit run dashboard"); do
  kill -9 $pid 2>/dev/null
done
sleep 2

# 启动 API
nohup /opt/module/mall_rt_env/bin/uvicorn api:app --host 0.0.0.0 --port 8000 > api.log 2>&1 &
echo "API started, pid=$!"

# 启动 Dashboard
nohup /opt/module/mall_rt_env/bin/python -m streamlit run dashboard.py --server.port 8502 --server.headless true > dashboard.log 2>&1 &
echo "Dashboard started, pid=$!"

# 等待启动
sleep 5

# 验证
echo "=== 进程验证 ==="
pgrep -af "uvicorn api:app" | head -1
pgrep -af "streamlit run dashboard.py" | head -1

echo "=== API 验证 ==="
curl -s http://localhost:8000/api/realtime/trend | head -c 300
echo ""

echo "=== Dashboard 验证 ==="
curl -s -o /dev/null -w "%{http_code}" http://localhost:8502
echo " (200=OK)"
