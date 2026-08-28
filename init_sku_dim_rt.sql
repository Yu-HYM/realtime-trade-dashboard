-- 阶段三：实时计算项目维表 sku_dim_rt（最小替代表）
-- 说明：项目一 mall.sku_info 的品类是 category3_id 外键，无品类名，
--       实时作业需要 category 字段，故建独立维表。
-- 修复1：文档原"翻倍 INSERT id+10"只能执行一次（第二次起主键冲突），
--        改用 CROSS JOIN 数字序列一次生成 id 1~200，精确覆盖 mock sku_id。
-- 修复2：按项目隔离约束，维表迁移至独立库 mall_rt，与项目一 mall 库彻底解耦；
--        并清理阶段三误建在 mall 库的同名表（项目二自建表，非项目一资产）。
CREATE DATABASE IF NOT EXISTS mall_rt DEFAULT CHARACTER SET utf8mb4;
USE mall_rt;

CREATE TABLE IF NOT EXISTS sku_dim_rt(
  id BIGINT PRIMARY KEY,
  sku_name VARCHAR(100),
  category VARCHAR(20),
  price DECIMAL(10,2)
);

-- 清空旧数据，保证脚本可重复执行
TRUNCATE TABLE sku_dim_rt;

INSERT INTO sku_dim_rt VALUES
 (1,'安卓手机','手机数码',2999),(2,'笔记本电脑','手机数码',5999),
 (3,'冰箱','家用电器',2199),(4,'洗衣机','家用电器',1799),
 (5,'运动鞋','服饰鞋包',399),(6,'羽绒服','服饰鞋包',899),
 (7,'坚果零食','食品生鲜',59),(8,'进口牛排','食品生鲜',129),
 (9,'口红','美妆个护',199),(10,'香水','美妆个护',459);

-- 基础 10 行 × 序列 1~19 = 新增 190 行，合计 200 行，id 1~200 与 mock 的 sku_id 完全对齐
INSERT INTO sku_dim_rt(id, sku_name, category, price)
SELECT b.id + 10 * seq.n, b.sku_name, b.category, b.price
FROM sku_dim_rt b
CROSS JOIN (
  SELECT 1 AS n UNION ALL SELECT 2  UNION ALL SELECT 3  UNION ALL SELECT 4
  UNION ALL SELECT 5  UNION ALL SELECT 6  UNION ALL SELECT 7  UNION ALL SELECT 8
  UNION ALL SELECT 9  UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
  UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16
  UNION ALL SELECT 17 UNION ALL SELECT 18 UNION ALL SELECT 19
) seq
WHERE b.id <= 10;

-- 验证
SELECT COUNT(*) AS total_rows FROM sku_dim_rt;
SELECT category, COUNT(*) AS cnt FROM sku_dim_rt GROUP BY category;
SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM sku_dim_rt;

-- 清理阶段三误建在项目一 mall 库的同名表（仅此一张，为项目二自建表）
DROP TABLE IF EXISTS mall.sku_dim_rt;

-- 确认 mall 库已恢复项目一原始状态（应只含项目一的 8 张表）
SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'mall' ORDER BY TABLE_NAME;
