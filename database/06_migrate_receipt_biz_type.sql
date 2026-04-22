-- ============================================================
-- Migration 06: fact_receipt 新增 sign_biz_type 业务类型字段
-- ============================================================
-- 目的: 与 fact_signing.sign_biz_type / fact_refund.refund_biz_type 对称，
--      使收款表具备按业务类型（留学/多语）切片分析的能力。
-- 幂等: 全部使用 IF NOT EXISTS，可重复执行。
-- 回滚: 见文末回滚脚本。
-- ============================================================

-- 1. 加欄位（默认值 '留学' 自动回填所有存量行）
ALTER TABLE fact_receipt
    ADD COLUMN IF NOT EXISTS sign_biz_type VARCHAR(16) DEFAULT '留学';

-- 2. 加 CHECK 约束（若已存在则跳过）
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.constraint_column_usage
        WHERE table_name = 'fact_receipt'
          AND column_name = 'sign_biz_type'
          AND constraint_name = 'fact_receipt_sign_biz_type_check'
    ) THEN
        ALTER TABLE fact_receipt
            ADD CONSTRAINT fact_receipt_sign_biz_type_check
            CHECK (sign_biz_type IN ('留学','多语'));
    END IF;
END$$;

-- 3. 加索引
CREATE INDEX IF NOT EXISTS idx_receipt_biz_type ON fact_receipt(sign_biz_type);

-- ============================================================
-- 回滚脚本（如需）：
-- ALTER TABLE fact_receipt DROP CONSTRAINT IF EXISTS fact_receipt_sign_biz_type_check;
-- DROP INDEX IF EXISTS idx_receipt_biz_type;
-- ALTER TABLE fact_receipt DROP COLUMN IF EXISTS sign_biz_type;
-- ============================================================
