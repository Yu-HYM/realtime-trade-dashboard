# 电商实时交易监控大屏

> 基于 Kafka + Spark Structured Streaming + Redis 的电商实时交易监控大屏，支持秒级 GMV 更新、滑动窗口品类聚合、DAU 实时统计、故障恢复验证。

---

## 📌 项目简介

本项目构建了一条从订单生产 → 消息队列 → 流计算 → 缓存 → API → 前端大屏的完整实时数据链路。模拟电商订单实时产生，通过 Spark Structured Streaming 进行实时去重、窗口聚合、维表关联，结果写入 Redis，最终在 ECharts 深色大屏上秒级刷新展示。

项目与离线数仓项目形成互补：离线项目覆盖 T-1 批处理全链路，本项目覆盖秒级实时流处理链路。

---

## 🏗️ 系统架构

```
mock_producer.py ──> Kafka(order_topic) ──> Structured Streaming(realtime_gmv.py)
                                              ├─ watermark + dropDuplicates 订单去重
                                              ├─ 5 分钟滑动窗口聚合实时 GMV
                                              ├─ broadcast join MySQL 商品维表补品类
                                              └─ foreachBatch upsert Redis

FastAPI(api.py :8000) <── Redis ──> ECharts 深色大屏(web/ :8502，跨域轮询)
```

### Redis 数据结构设计

| Key | 类型 | 说明 |
|-----|------|------|
| `rt:gmv:total` | string | 当日累计 GMV |
| `rt:order:total` | string | 当日累计订单数 |
| `rt:gmv:cat:{category}` | hash | field=窗口起始时间, value=窗口 GMV |
| `rt:dau` | set | 当日活跃 user_id 集合 |

---

## 🛠️ 技术栈

| 类别 | 技术 | 版本 |
|------|------|------|
| 消息队列 | Kafka (KRaft 单节点) | 3.7 |
| 流计算 | Spark Structured Streaming (PySpark) | 3.5.1 |
| 缓存 | Redis | 7.2 |
| 后端 API | FastAPI | 0.115 |
| 前端 | ECharts 5 + HTML/CSS/JS | 5.x |
| 数据库 | MySQL（维表库 mall_rt） | 8.0 |
| 容器化 | Docker + docker-compose | - |

---

## ✨ 核心设计

### 1. Watermark 去重
```python
dedup = (orders
         .withWatermark("event_time", "10 minutes")
         .dropDuplicates(["order_id"]))
```
- 10 分钟 watermark 容忍延迟到达的重复订单
- 基于 order_id 去重，保证幂等

### 2. 滑动窗口聚合
```python
win = (enriched
       .groupBy(F.window("event_time", "5 minutes", "1 minutes"), "category")
       .agg(F.sum("amount").alias("gmv"), F.count("order_id").alias("order_cnt")))
```
- 5 分钟窗口，1 分钟滑动步长
- 按品类聚合实时 GMV 和订单数

### 3. 维表广播关联
```python
enriched = dedup.join(F.broadcast(sku_dim), dedup.sku_id == sku_dim.id, "left")
```
- Stream-Static Join，广播小维表避免 Shuffle
- 实时补充商品品类信息

### 4. foreachBatch 幂等写入
- 窗口聚合 → `hset` 覆盖（同窗口多次触发天然幂等）
- 累计指标 → `incrbyfloat` 增量累加
- DAU → `sadd` 集合去重（幂等）

### 5. Checkpoint 容灾
- 三个流查询各自独立 checkpoint 目录
- 故障重启后自动恢复进度，不丢数据
- 删除 checkpoint 即重跑全量

---

## 📂 项目结构

```
02-实时计算项目/
├── mock_producer.py          # 模拟订单生产者（写入 Kafka）
├── realtime_gmv.py           # Structured Streaming 核心作业
├── api.py                    # FastAPI 后端（读 Redis 返回数据）
├── dashboard.py              # Streamlit 备选大屏
├── docker-compose.yml        # Kafka + Redis 容器编排
├── download_jars.sh          # 下载 Spark 依赖 jar（华为云镜像）
├── init_sku_dim_rt.sql       # MySQL 维表建表 + 初始化
├── web/                      # ECharts 前端大屏
│   ├── index.html            # 大屏页面
│   ├── app.js                # 轮询 + 图表渲染
│   ├── styles.css            # 深色主题样式
│   └── vendor/               # echarts.min.js
├── start_api.sh              # 启动 API 服务
├── start_streaming.sh        # 启动流计算作业
├── start_dashboard.sh        # 启动 Web 大屏
├── restart_services.sh       # 一键重启全部服务
├── 02-实时计算项目操作文档.md
└── 实时计算项目面试问答手册.md
```

---

## 🚀 快速开始

### 环境要求

- WSL2 Ubuntu 22.04（或 Linux 环境）
- Docker + docker-compose
- Python 3.10+ + PySpark 3.5.1
- Java 8/11（Spark 依赖）

### 部署步骤

```bash
# 1. 启动 Kafka + Redis（Docker）
docker compose up -d

# 2. 创建 Kafka topic
docker exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic order_topic --partitions 1 --replication-factor 1

# 3. 初始化 MySQL 维表
mysql -uroot -p123456 < init_sku_dim_rt.sql

# 4. 下载 Spark 依赖 jar（华为云镜像）
bash download_jars.sh

# 5. 启动流计算作业
bash start_streaming.sh

# 6. 启动订单生产者（模拟实时数据）
python3 mock_producer.py

# 7. 启动 API 服务
bash start_api.sh

# 8. 启动 Web 大屏
bash start_dashboard.sh

# 访问大屏：http://localhost:8502
# 访问 API：http://localhost:8000/metrics
```

### 一键启停

```bash
# 启动全部服务
bash restart_services.sh

# 停止全部服务
bash x_stop_all.sh
```

---

## 📡 API 接口

| 接口 | 方法 | 说明 |
|------|------|------|
| `/metrics` | GET | 返回实时 GMV、订单数、DAU、品类窗口数据 |
| `/health` | GET | 健康检查 |

### 示例响应

```json
{
  "gmv_total": 128560.50,
  "order_total": 342,
  "dau": 156,
  "category_windows": {
    "手机": {"09:05": 15200.00, "09:06": 8900.00},
    "电脑": {"09:05": 22800.00}
  }
}
```

---

## 🧠 设计亮点

1. **Watermark + dropDuplicates**：解决实时数据乱序和重复消费问题，10分钟延迟容忍窗口。

2. **广播维表关联**：Stream-Static Join 使用 broadcast hint，避免 Shuffle 开销，维表秒级生效。

3. **三路分流设计**：一个 Streaming 作业同时输出三类结果（窗口聚合、累计指标、DAU），各自独立 checkpoint，互不影响。

4. **幂等保障全链路**：Redis `hset` 覆盖、`incrbyfloat` 累加、`sadd` 去重，故障恢复后不产生重复数据。

5. **项目隔离约束**：独立数据库 `mall_rt`，不复用离线项目 `mall` 库，避免跨项目数据污染。

6. **深色大屏设计**：ECharts 深色主题，5s/15s 跨域轮询，实时数据可视化。

---

## 📝 License

MIT License
