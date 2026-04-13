-- ============================================================
-- Migration 05: 新增 actual_advisor + 业务类型索引
-- ============================================================

-- fact_signing 新增实际签约顾问
ALTER TABLE fact_signing
    ADD COLUMN IF NOT EXISTS actual_advisor VARCHAR(64) DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_signing_actual_adv ON fact_signing(actual_advisor);

-- fact_refund 新增实际签约顾问
ALTER TABLE fact_refund
    ADD COLUMN IF NOT EXISTS actual_advisor VARCHAR(64) DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_refund_actual_adv ON fact_refund(actual_advisor);
