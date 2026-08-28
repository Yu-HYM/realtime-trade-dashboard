#!/usr/bin/env python3
# /opt/script/mall_rt/api.py
# 阶段五：FastAPI 查询接口（Redis 读侧，服务大屏）
# 架构：前后端分离——本服务跑在 8000 只提供 API；
#       深色大屏（web/ 目录）由 http.server 托管在 8502，跨域轮询本接口
import time
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import redis

app = FastAPI(title="realtime-dashboard-api")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"])
rdb = redis.Redis(host="localhost", port=6379, decode_responses=True)

@app.get("/api/realtime/overview")
def overview():
    return {
        "ts": int(time.time() * 1000),
        "gmv_total": float(rdb.get("rt:gmv:total") or 0),
        "order_total": int(rdb.get("rt:order:total") or 0),
        "dau": rdb.scard("rt:dau"),
    }

@app.get("/api/realtime/category")
def category_gmv():
    result = []
    for key in rdb.keys("rt:gmv:cat:*"):
        windows = rdb.hgetall(key)
        latest = max(windows) if windows else "-"
        result.append({"category": key.split(":")[-1],
                       "window": latest,
                       "gmv": float(windows.get(latest, 0))})
    return sorted(result, key=lambda x: -x["gmv"])

@app.get("/api/realtime/trend")
def category_trend():
    """各品类 GMV 趋势（按时间窗口）"""
    result = {}
    for key in rdb.keys("rt:gmv:cat:*"):
        category = key.split(":")[-1]
        windows = rdb.hgetall(key)
        # 按时间排序，返回时间和 GMV 列表
        sorted_times = sorted(windows.keys())
        result[category] = {
            "times": sorted_times,
            "values": [float(windows[t]) for t in sorted_times]
        }
    return result
