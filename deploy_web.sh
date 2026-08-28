#!/bin/bash
# 部署深色大屏到 8502（前后端分离：8502 静态大屏 + 8000 API）
set -e
cd /opt/script/mall_rt

# 1 同步文件（Windows -> WSL）
sudo mkdir -p /opt/script/mall_rt/web/vendor
sudo cp /mnt/e/简历项目/02-实时计算项目/web/index.html /opt/script/mall_rt/web/
sudo cp /mnt/e/简历项目/02-实时计算项目/web/styles.css /opt/script/mall_rt/web/
sudo cp /mnt/e/简历项目/02-实时计算项目/web/app.js /opt/script/mall_rt/web/
sudo cp /mnt/e/简历项目/02-实时计算项目/web/README.md /opt/script/mall_rt/web/
sudo cp /mnt/e/简历项目/02-实时计算项目/web/vendor/echarts.min.js /opt/script/mall_rt/web/vendor/
sudo cp /mnt/e/简历项目/02-实时计算项目/api.py /opt/script/mall_rt/
sudo cp /mnt/e/简历项目/02-实时计算项目/start_dashboard.sh /opt/script/mall_rt/
sudo chown -R hadoop:hadoop /opt/script/mall_rt/web /opt/script/mall_rt/api.py /opt/script/mall_rt/start_dashboard.sh
echo "=== SYNC OK ==="

# 2 停掉旧 Streamlit 大屏（8502 让位给深色大屏）
pkill -9 -f "[s]treamlit run dashboard" 2>/dev/null || true
echo "=== Streamlit 已停止 ==="

# 3 重启 API（8000，回归纯 API 服务）
pkill -9 -f "uvicorn [a]pi:app" 2>/dev/null || true
sleep 2
nohup /opt/module/mall_rt_env/bin/uvicorn api:app --host 0.0.0.0 --port 8000 > api.log 2>&1 &
echo "API(8000) restarted, pid=$!"
sleep 5

# 4 启动深色大屏（8502，静态托管）
pkill -9 -f "http[.]server 8502" 2>/dev/null || true
sleep 1
nohup /opt/module/mall_rt_env/bin/python -m http.server 8502 --bind 0.0.0.0 --directory /opt/script/mall_rt/web > dashboard.log 2>&1 &
echo "Dashboard(8502) started, pid=$!"
sleep 3

# 5 验证
echo "=== 进程清单 ==="
pgrep -af "uvicorn" | head -2
pgrep -af "http[.]server" | head -2
echo "=== 8502 大屏静态资源 ==="
curl -s -o /dev/null -w "index:   %{http_code}\n" http://localhost:8502/
curl -s -o /dev/null -w "app.js:  %{http_code}\n" http://localhost:8502/app.js
curl -s -o /dev/null -w "echarts: %{http_code}\n" http://localhost:8502/vendor/echarts.min.js
curl -s -o /dev/null -w "styles:  %{http_code}\n" http://localhost:8502/styles.css
echo "=== 8502 页面标题 ==="
curl -s http://localhost:8502/ | grep -o "<title>[^<]*</title>"
echo "=== 8000 API（CORS 头 + 数据）==="
curl -s -D - -o /dev/null http://localhost:8000/api/realtime/overview | grep -i -E "^HTTP|access-control-allow-origin"
curl -s http://localhost:8000/api/realtime/overview
echo ""