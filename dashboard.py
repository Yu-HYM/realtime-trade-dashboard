#!/usr/bin/env python3
# /opt/script/mall_rt/dashboard.py
# 阶段六：Streamlit 实时交易大屏（5 秒自动刷新）
# 注意：端口用 8502（8501 已被项目三 Streamlit 占用）
import pandas as pd
import requests
import streamlit as st
import plotly.express as px
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="电商实时大屏", layout="wide")
st.title("电商实时交易大屏")
st_autorefresh(interval=5000, key="refresh")   # 5 秒自动刷新

try:
    ov = requests.get("http://localhost:8000/api/realtime/overview", timeout=3).json()
    cat = requests.get("http://localhost:8000/api/realtime/category", timeout=3).json()
    trend = requests.get("http://localhost:8000/api/realtime/trend", timeout=3).json()
except Exception:
    st.error("后端不可用，请确认 api.py 已启动")
    st.stop()

# === 模块1：核心经营指标 ===
c1, c2, c3 = st.columns(3)
c1.metric("实时 GMV（当日累计）", f"{ov['gmv_total']:,.2f} 元")
c2.metric("实时订单数", f"{ov['order_total']:,}")
c3.metric("实时 DAU", f"{ov['dau']:,}")

# === 模块2：各品类实时 GMV 对比（柱状图）===
st.subheader("各品类 5 分钟窗口 GMV")
col1, col2 = st.columns([2, 1])  # 左柱状图占2，右表格占1
with col1:
    if cat:
        df_bar = pd.DataFrame(cat)
        st.bar_chart(df_bar.set_index("category")["gmv"])
    else:
        st.info("等待窗口数据产出（约 1 分钟）")
with col2:
    if cat:
        st.dataframe(df_bar, use_container_width=True, hide_index=True)
    else:
        st.info("等待数据...")

# === 模块3&4：实时 GMV 趋势（折线图）+ 明细表 ===
st.subheader("各品类 GMV 趋势（按时间窗口）")
if trend:
    # 构造趋势数据 DataFrame
    trend_df = pd.DataFrame()
    for cat_name, data in trend.items():
        temp_df = pd.DataFrame({
            "时间窗口": data["times"],
            "GMV": data["values"],
            "品类": cat_name
        })
        trend_df = pd.concat([trend_df, temp_df], ignore_index=True)
    
    if not trend_df.empty:
        # 绘制折线图
        fig = px.line(
            trend_df, 
            x="时间窗口", 
            y="GMV", 
            color="品类",
            markers=True,
            title="各品类实时 GMV 趋势"
        )
        fig.update_layout(
            xaxis_title="时间窗口",
            yaxis_title="GMV (元)",
            hovermode="x unified",
            legend_title="商品品类"
        )
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("等待趋势数据产出...")
