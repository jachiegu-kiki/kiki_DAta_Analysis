-- ============================================================
-- 迁移：已收款未归档 → 已收款未盖章
-- 执行顺序：先于代码部署运行
-- ============================================================

BEGIN;

-- 1) 动态查找并删除 metric_type 上的所有 CHECK 约束
--    （不依赖约束名，兼容 PG 自动命名）
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT con.conname
          FROM pg_constraint con
          JOIN pg_class rel ON rel.oid = con.conrelid
          JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
         WHERE rel.relname = 'fact_fund_snapshot'
           AND con.contype = 'c'                         -- CHECK 类型
           AND pg_get_constraintdef(con.oid) LIKE '%metric_type%'
    LOOP
        EXECUTE format('ALTER TABLE fact_fund_snapshot DROP CONSTRAINT %I', r.conname);
        RAISE NOTICE '已删除旧约束: %', r.conname;
    END LOOP;
END $$;

-- 2) 更新已有数据
UPDATE fact_fund_snapshot
   SET metric_type = '已收款未盖章'
 WHERE metric_type = '已收款未归档';

-- 3) 建立新 CHECK 约束
ALTER TABLE fact_fund_snapshot
  ADD CONSTRAINT fact_fund_snapshot_metric_type_check
  CHECK (metric_type IN ('已收款未盖章','潜在签约','未认款'));

COMMIT;
