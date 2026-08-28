#!/bin/bash
# 阶段六：启动深色科技风大屏（8502 端口，静态托管 web/ 目录）
# 架构：前后端分离——8502 = 大屏前端（ECharts），8000 = FastAPI 数据接口
cd /opt/script/mall_rt

# 已有实例则拒绝重复启动
if pgrep -f "http[.]server 8502" > /dev/null; then
  echo "dashboard already running:"; pgrep -af "http[.]server 8502"; exit 1
fi

nohup /opt/module/mall_rt_env/bin/python -m http.server 8502 --bind 0.0.0.0 --directory /opt/script/mall_rt/web > dashboard.log 2>&1 &
echo "dashboard started, pid=$!"
sleep 3
echo "--- 进程状态 ---"
pgrep -af "http[.]server" || { echo "PROCESS DEAD, 日志尾部："; tail -15 dashboard.log; exit 1; }
echo "--- 页面可访问性自检（HTTP 状态码）---"
curl -s -o /dev/null -w "%{http_code}" http://localhost:8502 && echo " (200=OK)"