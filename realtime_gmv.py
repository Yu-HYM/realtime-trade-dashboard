#!/usr/bin/env python3
# /opt/script/mall_rt/realtime_gmv.py
# 阶段四：Structured Streaming 实时计算作业（核心）
# 覆盖硬核点：watermark 去重 / 5min滑动窗口 / broadcast 维表关联 / foreachBatch 幂等 upsert / checkpoint 容灾
# 相对操作文档的调整：
#   1. JDBC 指向独立库 mall_rt（项目隔离约束，不动项目一 mall 库）
#   2. spark.jars.packages（境外 Maven）改为本地 jar（华为云镜像下载，见 download_jars.sh）
#   3. 去掉 spark.executor.memory（local 模式该配置无意义）
#   4. 修复 write_total 的 bug：原代码在 foreachBatch 内对流 DataFrame dedup 聚合，
#      会抛 AnalysisException（流式源不能在批上下文查询），改为对 batch_df 聚合
import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import redis

MYSQL_URL = "jdbc:mysql://localhost:3306/mall_rt?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai"
# 本地 jar 目录（download_jars.sh 已下载）
JARS = (
    "/opt/script/mall_rt/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,"
    "/opt/script/mall_rt/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar,"
    "/opt/script/mall_rt/jars/kafka-clients-3.4.1.jar,"
    "/opt/script/mall_rt/jars/commons-pool2-2.11.1.jar,"
    "/opt/script/mall_rt/jars/mysql-connector-j-8.0.33.jar"
)

spark = (SparkSession.builder
         .appName("mall_realtime_gmv")
         .master("local[2]")
         .config("spark.jars", JARS)
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("order_id", StringType()),
    StructField("user_id", LongType()),
    StructField("sku_id", LongType()),
    StructField("amount", DoubleType()),
    StructField("ts", StringType()),
])

# 1 读 Kafka
raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "order_topic")
       .option("startingOffsets", "latest")
       .option("maxOffsetsPerTrigger", 1000)   # 限流，防止积压打爆本地环境
       .load())

orders = (raw.selectExpr("CAST(value AS STRING) AS json_str")
          .select(F.from_json("json_str", schema).alias("d")).select("d.*")
          .withColumn("event_time", F.to_timestamp("ts", "yyyy-MM-dd HH:mm:ss")))

# 2 watermark + 按订单号去重
dedup = (orders
         .withWatermark("event_time", "10 minutes")
         .dropDuplicates(["order_id"]))

# 3 维表广播关联（stream-static join）
sku_dim = (spark.read.format("jdbc")
           .option("url", MYSQL_URL)
           .option("dbtable", "sku_dim_rt")
           .option("user", "root").option("password", "123456")
           .option("driver", "com.mysql.cj.jdbc.Driver")
           .load())
enriched = dedup.join(F.broadcast(sku_dim), dedup.sku_id == sku_dim.id, "left")

# 4 5 分钟滑动窗口（滑动 1 分钟）按品类聚合 GMV
#    修复5：流式 DataFrame 不支持 countDistinct（AnalysisException），
#    但上游已 dropDuplicates(order_id) 去重，count(order_id) 语义等价于 countDistinct
win = (enriched
       .groupBy(F.window("event_time", "5 minutes", "1 minutes"),
                F.coalesce("category", F.lit("未知")).alias("category"))
       .agg(F.sum("amount").alias("gmv"),
            F.count("order_id").alias("order_cnt")))

rdb = redis.Redis(host="localhost", port=6379, decode_responses=True)

def write_redis(batch_df, batch_id):
    """窗口聚合结果 upsert Redis hash，同窗口多次触发覆盖，天然幂等"""
    for row in batch_df.collect():
        ws = row["window"]["start"].strftime("%H:%M")
        rdb.hset(f"rt:gmv:cat:{row['category']}", ws, round(row["gmv"], 2))

def write_total(batch_df, batch_id):
    """当日累计指标：batch_df 已是去重后的本批新增明细，增量累加"""
    row = batch_df.agg(F.sum("amount").alias("g"),
                       F.count("order_id").alias("c")).first()
    if row and row["g"] is not None:
        rdb.incrbyfloat("rt:gmv:total", row["g"])
        rdb.incrby("rt:order:total", int(row["c"]))

def write_dau(batch_df, batch_id):
    """当日 DAU：set 去重，sadd 天然幂等"""
    pipe = rdb.pipeline()
    for uid in batch_df.select("user_id").distinct().collect():
        pipe.sadd("rt:dau", uid["user_id"])
    pipe.execute()

queries = [
    (win.writeStream.outputMode("update").foreachBatch(write_redis)
        .option("checkpointLocation", "/opt/script/mall_rt/checkpoint/win")
        .trigger(processingTime="30 seconds").start()),
    (dedup.writeStream.foreachBatch(write_total)
        .option("checkpointLocation", "/opt/script/mall_rt/checkpoint/total")
        .trigger(processingTime="30 seconds").start()),
    (dedup.writeStream.foreachBatch(write_dau)
        .option("checkpointLocation", "/opt/script/mall_rt/checkpoint/dau")
        .trigger(processingTime="30 seconds").start()),
]
print("=== 三个流查询已启动：win / total / dau ===")
spark.streams.awaitAnyTermination()
