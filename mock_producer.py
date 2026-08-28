#!/usr/bin/env python3
# /opt/script/mall_rt/mock_producer.py
# 模拟订单流生产者：每 0.5 秒一条订单 json，2% 概率重复发送同一 order_id（验证去重）
import json, random, time, datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"))

oid = 1000000
try:
    while True:
        oid += 1
        ev = {
            "order_id": f"O{oid}",
            "user_id": random.randint(1, 2000),
            "sku_id": random.randint(1, 200),
            "amount": round(random.uniform(20, 3000), 2),
            "ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        producer.send("order_topic", ev)
        if random.random() < 0.02:        # 2% 重复事件，验证去重
            producer.send("order_topic", ev)
        if oid % 100 == 0:
            print("sent:", oid)
        time.sleep(0.5)
except KeyboardInterrupt:
    producer.flush()
