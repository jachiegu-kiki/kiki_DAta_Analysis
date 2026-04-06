-- ============================================================
-- 迁移脚本：为 fact_receipt 添加 updated_at 列
-- 执行方式：
--   docker compose exec postgres psql -U qiantu -d qiantu_finance
--   然后粘贴以下 SQL
-- ============================================================

ALTER TABLE fact_receipt
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

UPDATE fact_receipt SET updated_at = created_at WHERE updated_at IS NULL;
