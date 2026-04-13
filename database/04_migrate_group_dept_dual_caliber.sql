-- ============================================================
-- Migration 04: 分组部门维度表 + 双口径支持
-- 1. 新建 dim_group_dept（参数-分组部门 层级映射）
-- 2. 扩展 dim_contract_group（增加 实际签约顾问 + 顾问口径分组部门）
-- 3. fact_signing / fact_refund 新增 secondary_group_advisor 字段
-- ============================================================

-- ── 1. 新建 dim_group_dept ──
CREATE TABLE IF NOT EXISTS dim_group_dept (
    secondary_group     VARCHAR(64) PRIMARY KEY,   -- 二级分组部门（原始值，作为关联键）
    secondary_group_tidy VARCHAR(64) DEFAULT '',    -- 二级分组部门（整理）
    primary_group       VARCHAR(64) DEFAULT '',     -- 一级分组部门
    biz_block           VARCHAR(32) DEFAULT ''      -- 业务板块
);
CREATE INDEX IF NOT EXISTS idx_gdept_primary ON dim_group_dept(primary_group);
CREATE INDEX IF NOT EXISTS idx_gdept_block   ON dim_group_dept(biz_block);

-- ── 2. 扩展 dim_contract_group ──
ALTER TABLE dim_contract_group
    ADD COLUMN IF NOT EXISTS actual_advisor      VARCHAR(64) DEFAULT '',
    ADD COLUMN IF NOT EXISTS group_dept_advisor   VARCHAR(64) DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_cg_advisor ON dim_contract_group(group_dept_advisor);

-- ── 3. fact_signing 新增顾问口径分组 ──
ALTER TABLE fact_signing
    ADD COLUMN IF NOT EXISTS secondary_group_advisor VARCHAR(64) DEFAULT '未知部门';

CREATE INDEX IF NOT EXISTS idx_signing_group_adv ON fact_signing(secondary_group_advisor);

-- ── 4. fact_refund 新增顾问口径分组 ──
ALTER TABLE fact_refund
    ADD COLUMN IF NOT EXISTS secondary_group_advisor VARCHAR(64) DEFAULT '未知部门';

CREATE INDEX IF NOT EXISTS idx_refund_group_adv ON fact_refund(secondary_group_advisor);
