#!/bin/bash
# 阶段五：启动 FastAPI 查询接口（端口 8000）
cd /opt/script/mall_rt

# 已有实例则拒绝重复启动
if pgrep -f "uvicorn api:app" > /dev/null; then
  echo "api already running:"; pgrep -af "uvicorn api:app"; exit 1
fi

nohup /opt/module/mall_rt_env/bin/python -m uvicorn api:app --host 0.0.0.0 --port 8000 > api.log 2>&1 &
echo "api started, pid=$!"
sleep 5
echo "--- 进程状态 ---"
pgrep -af "uvicorn api:app" || { echo "PROCESS DEAD, 日志尾部："; tail -15 api.log; exit 1; }
echo "--- 接口自检 ---"
curl -s http://localhost:8000/api/realtime/overview && echo
