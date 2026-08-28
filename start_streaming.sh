#!/bin/bash
# 阶段四：启动 Structured Streaming 流作业（加固版：等待初始化 + 状态验证）
# 非交互 shell 不加载 ~/.bashrc，必须显式设置 JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
cd /opt/script/mall_rt

# 已有实例则拒绝重复启动
if pgrep -f "realtime_gmv.py" > /dev/null; then
  echo "streaming already running:"; pgrep -af "realtime_gmv.py"; exit 1
fi

nohup /opt/module/mall_rt_env/bin/python realtime_gmv.py > streaming.log 2>&1 &
echo "streaming started, pid=$!"
# 等待 Spark 初始化完成（本地 jar 加载 + 三个流查询注册约 20~30 秒）
sleep 25
echo "--- 进程状态 ---"
pgrep -af "realtime_gmv.py" || { echo "PROCESS DEAD, 日志尾部："; tail -15 streaming.log; exit 1; }
echo "--- 日志尾部 ---"
tail -8 streaming.log
