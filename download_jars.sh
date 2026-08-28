#!/bin/bash
# 阶段四：下载 Spark Kafka connector + MySQL JDBC 驱动（华为云 Maven 镜像）
set -e
mkdir -p /opt/script/mall_rt/jars
cd /opt/script/mall_rt/jars

BASE="https://mirrors.huaweicloud.com/repository/maven"
JARS=(
  "org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar"
  "org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar"
  "org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar"
  "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
  "com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar"
)

for u in "${JARS[@]}"; do
  f=$(basename "$u")
  if [ -s "$f" ]; then
    echo "SKIP(exists): $f"
    continue
  fi
  echo "Downloading: $f"
  curl -fsSL -o "$f" "$BASE/$u" && echo "OK: $f" || { echo "FAIL: $f"; exit 1; }
done

echo "--- 结果清单 ---"
ls -la /opt/script/mall_rt/jars/
