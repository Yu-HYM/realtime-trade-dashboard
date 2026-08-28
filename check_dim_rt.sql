-- 用阶段二实际消费到的 sku_id 抽样验证关联
SELECT id, sku_name, category, price FROM mall.sku_dim_rt WHERE id IN (91, 46, 45, 200, 137);
