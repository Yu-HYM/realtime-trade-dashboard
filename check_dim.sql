-- 探查项目一 mall 库的 sku_info 维表结构
USE mall;
SHOW TABLES;
DESCRIBE sku_info;
SELECT COUNT(*) AS sku_cnt FROM sku_info;
SELECT * FROM sku_info LIMIT 3;
