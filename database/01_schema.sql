-- ============================================================
-- 广州前途财务日报系统 · PostgreSQL 完整 Schema
-- Version: 2.0  (2026-04  · 整合 migration 02~06)
-- ============================================================
-- 历史迁移已全部合并入本文件，新部署只需跑此一份。
-- 已有生产数据库若已套用 migration 02~06，可忽略本文件
-- （docker-entrypoint-initdb.d 只在 pg_data volume 为空时執行）。
--
-- 已整合的历史 migration：
--   02_migrate_receipt_updated_at.sql        → fact_receipt.updated_at
--   03_migrate_rename_unarchived.sql         → metric_type 值集合
--   04_migrate_group_dept_dual_caliber.sql   → dim_group_dept 表 + 顾问口径字段
--   05_migrate_actual_advisor.sql            → actual_advisor 字段
--   06_migrate_receipt_biz_type.sql          → fact_receipt.sign_biz_type
--
-- 已移除：
--   dim_users 表  — 认证交给 AccountSystem（users.yaml），此表不再使用
-- ============================================================

-- 扩展
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- 顾问姓名模糊搜索

-- ============================================================
-- 维度表
-- ============================================================

-- dim_advisor: 顾问字典表（来源：钉钉多维表格 Webhook 同步）
CREATE TABLE IF NOT EXISTS dim_advisor (
    advisor_id      VARCHAR(32)  PRIMARY KEY,           -- 员工编号
    name            VARCHAR(64)  NOT NULL,
    email           VARCHAR(128) UNIQUE,                 -- 迅程邮箱映射
    primary_dept    VARCHAR(64),                         -- 部门
    secondary_group VARCHAR(64),                         -- 二级分组部门
    entry_date      DATE,                                -- 入职日期
    exit_date       DATE,                                -- 离职日期（有值=已离职）
    updated_at      TIMESTAMPTZ  DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_advisor_name  ON dim_advisor(name);
CREATE INDEX IF NOT EXISTS idx_advisor_email ON dim_advisor(email);
CREATE INDEX IF NOT EXISTS idx_advisor_dept  ON dim_advisor(primary_dept);
CREATE INDEX IF NOT EXISTS idx_advisor_group ON dim_advisor(secondary_group);

-- 顾问状态视图：根据入职/离职日期动态计算，不再存储静态 status 列
CREATE OR REPLACE VIEW v_advisor_with_status AS
SELECT *,
    CASE
        WHEN exit_date IS NOT NULL THEN '离职'
        WHEN entry_date IS NOT NULL
             AND entry_date + INTERVAL '6 months' <= CURRENT_DATE THEN '老顾问'
        ELSE '新顾问'
    END AS status
FROM dim_advisor;

-- dim_monthly_target: 每月目标配置（来源：钉钉多维表格 Webhook 同步）
CREATE TABLE IF NOT EXISTS dim_monthly_target (
    id              SERIAL       PRIMARY KEY,
    year_month      VARCHAR(7)   NOT NULL,               -- 格式: 'YYYY-MM'
    department      VARCHAR(64)  NOT NULL,
    secondary_group VARCHAR(64)  NOT NULL DEFAULT '全部',
    target_amount   DECIMAL(12,2) NOT NULL,              -- 超额目标（万元）
    updated_at      TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (year_month, secondary_group)
);
CREATE INDEX IF NOT EXISTS idx_target_month ON dim_monthly_target(year_month);

-- dim_contract_group: 合同号→分组部门映射（含 migration 04 扩展字段）
CREATE TABLE IF NOT EXISTS dim_contract_group (
    contract_no        VARCHAR(64)  PRIMARY KEY,
    group_dept         VARCHAR(64)  NOT NULL,
    actual_advisor     VARCHAR(64)  DEFAULT '',
    group_dept_advisor VARCHAR(64)  DEFAULT '',
    updated_at         TIMESTAMPTZ  DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cg_advisor ON dim_contract_group(group_dept_advisor);

-- dim_group_dept: 参数-分组部门 层级映射（migration 04 新建）
CREATE TABLE IF NOT EXISTS dim_group_dept (
    secondary_group      VARCHAR(64) PRIMARY KEY,
    secondary_group_tidy VARCHAR(64) DEFAULT '',
    primary_group        VARCHAR(64) DEFAULT '',
    biz_block            VARCHAR(32) DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_gdept_primary ON dim_group_dept(primary_group);
CREATE INDEX IF NOT EXISTS idx_gdept_block   ON dim_group_dept(biz_block);

-- ============================================================
-- 事实表
-- ============================================================

-- fact_signing: 签约事实表
-- contract_no 不做主键：学校签约的班级编码可对应多名学生，允许重复
-- 采用按 source_system 全量刷新策略保证幂等
CREATE TABLE IF NOT EXISTS fact_signing (
    id                      BIGSERIAL    PRIMARY KEY,
    contract_no             VARCHAR(64)  NOT NULL,
    sign_date               DATE         NOT NULL,
    advisor_name            VARCHAR(64)  DEFAULT '',
    actual_advisor          VARCHAR(64)  DEFAULT '',             -- migration 05
    original_dept           VARCHAR(64)  DEFAULT '',
    line                    VARCHAR(32)  DEFAULT '',
    sub_line                VARCHAR(64)  DEFAULT '',
    secondary_group         VARCHAR(64)  DEFAULT '未知部门',
    secondary_group_advisor VARCHAR(64)  DEFAULT '未知部门',     -- migration 04
    sign_biz_type           VARCHAR(16)  DEFAULT '留学'
                            CHECK (sign_biz_type IN ('留学','多语')),
    school                  VARCHAR(32)  DEFAULT 'ERP'
                            CHECK (school IN ('ERP','广州前途','前途出国','迅程')),
    gross_sign              DECIMAL(14,2) NOT NULL DEFAULT 0,
    source_system           VARCHAR(16)  DEFAULT '日更'
                            CHECK (source_system IN ('日更','月更','历史')),
    created_at              TIMESTAMPTZ  DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_signing_contract   ON fact_signing(contract_no);
CREATE INDEX IF NOT EXISTS idx_signing_date       ON fact_signing(sign_date);
CREATE INDEX IF NOT EXISTS idx_signing_advisor    ON fact_signing(advisor_name);
CREATE INDEX IF NOT EXISTS idx_signing_actual_adv ON fact_signing(actual_advisor);
CREATE INDEX IF NOT EXISTS idx_signing_group      ON fact_signing(secondary_group);
CREATE INDEX IF NOT EXISTS idx_signing_group_adv  ON fact_signing(secondary_group_advisor);
CREATE INDEX IF NOT EXISTS idx_signing_biz_type   ON fact_signing(sign_biz_type);
CREATE INDEX IF NOT EXISTS idx_signing_date_adv   ON fact_signing(sign_date, advisor_name);
CREATE INDEX IF NOT EXISTS idx_signing_orig_dept  ON fact_signing(original_dept);
CREATE INDEX IF NOT EXISTS idx_signing_source     ON fact_signing(source_system);

-- fact_refund: 退费事实表
CREATE TABLE IF NOT EXISTS fact_refund (
    id                      BIGSERIAL    PRIMARY KEY,
    refund_id               VARCHAR(64)  NOT NULL DEFAULT '',
    refund_date             DATE         NOT NULL,
    contract_no             VARCHAR(64)  DEFAULT '',
    advisor_name            VARCHAR(64)  DEFAULT '',
    actual_advisor          VARCHAR(64)  DEFAULT '',             -- migration 05
    original_dept           VARCHAR(64)  DEFAULT '',
    line                    VARCHAR(32)  DEFAULT '',
    sub_line                VARCHAR(64)  DEFAULT '',
    secondary_group         VARCHAR(64)  DEFAULT '未知部门',
    secondary_group_advisor VARCHAR(64)  DEFAULT '未知部门',     -- migration 04
    refund_biz_type         VARCHAR(16)  DEFAULT '留学'
                            CHECK (refund_biz_type IN ('留学','多语')),
    gross_refund            DECIMAL(14,2) NOT NULL DEFAULT 0,
    source_system           VARCHAR(16)  DEFAULT '日更'
                            CHECK (source_system IN ('日更','月更','历史')),
    created_at              TIMESTAMPTZ  DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_refund_rid        ON fact_refund(refund_id);
CREATE INDEX IF NOT EXISTS idx_refund_date       ON fact_refund(refund_date);
CREATE INDEX IF NOT EXISTS idx_refund_advisor    ON fact_refund(advisor_name);
CREATE INDEX IF NOT EXISTS idx_refund_actual_adv ON fact_refund(actual_advisor);
CREATE INDEX IF NOT EXISTS idx_refund_group      ON fact_refund(secondary_group);
CREATE INDEX IF NOT EXISTS idx_refund_group_adv  ON fact_refund(secondary_group_advisor);
CREATE INDEX IF NOT EXISTS idx_refund_contract   ON fact_refund(contract_no);
CREATE INDEX IF NOT EXISTS idx_refund_orig_dept  ON fact_refund(original_dept);
CREATE INDEX IF NOT EXISTS idx_refund_source     ON fact_refund(source_system);

-- fact_receipt: 收款统计事实表
-- 已整合 migration 02 (updated_at) + migration 06 (sign_biz_type)
CREATE TABLE IF NOT EXISTS fact_receipt (
    receipt_no      VARCHAR(64)  PRIMARY KEY,
    receipt_date    DATE         NOT NULL,
    arrived_date    DATE,
    contract_no     VARCHAR(64)  DEFAULT '',
    advisor_name    VARCHAR(64)  DEFAULT '',
    dept            VARCHAR(64)  DEFAULT '',
    pay_method      VARCHAR(32)  DEFAULT '',
    status          VARCHAR(32)  DEFAULT '',
    sign_biz_type   VARCHAR(16)  DEFAULT '留学'                  -- migration 06
                    CHECK (sign_biz_type IN ('留学','多语')),
    amount          DECIMAL(14,2) NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ  DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  DEFAULT NOW()                    -- migration 02
);
CREATE INDEX IF NOT EXISTS idx_receipt_arrived  ON fact_receipt(arrived_date);
CREATE INDEX IF NOT EXISTS idx_receipt_date     ON fact_receipt(receipt_date);
CREATE INDEX IF NOT EXISTS idx_receipt_advisor  ON fact_receipt(advisor_name);
CREATE INDEX IF NOT EXISTS idx_receipt_biz_type ON fact_receipt(sign_biz_type);

-- 创建一个计算列视图，统一"有效日期"（receipt_date 优先）
CREATE OR REPLACE VIEW v_receipt_effective AS
SELECT *,
    COALESCE(receipt_date, arrived_date) AS effective_date
FROM fact_receipt;

-- fact_fund_snapshot: 资金快照表（每日覆写）
-- CHECK 约束已是 migration 03 的最终值域：'已收款未盖章','潜在签约','未认款'
CREATE TABLE IF NOT EXISTS fact_fund_snapshot (
    id              BIGSERIAL    PRIMARY KEY,
    snapshot_date   DATE         NOT NULL,
    contract_no     VARCHAR(64)  DEFAULT '',
    advisor_name    VARCHAR(64)  DEFAULT '',
    dept            VARCHAR(64)  DEFAULT '',
    secondary_group VARCHAR(64)  DEFAULT '未知部门',
    metric_type     VARCHAR(16)  NOT NULL
                    CHECK (metric_type IN ('已收款未盖章','潜在签约','未认款')),
    amount          DECIMAL(14,2) NOT NULL DEFAULT 0,
    contract_status VARCHAR(32)  DEFAULT '',
    created_at      TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (snapshot_date, metric_type, contract_no)
);
CREATE INDEX IF NOT EXISTS idx_snapshot_date    ON fact_fund_snapshot(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_snapshot_type    ON fact_fund_snapshot(metric_type);
CREATE INDEX IF NOT EXISTS idx_snapshot_dept    ON fact_fund_snapshot(dept);
CREATE INDEX IF NOT EXISTS idx_snapshot_advisor ON fact_fund_snapshot(advisor_name);

-- ============================================================
-- 数据摄入日志（用于防重与回滚）
-- ============================================================
CREATE TABLE IF NOT EXISTS ingest_log (
    id               BIGSERIAL    PRIMARY KEY,
    source_tag       VARCHAR(128) NOT NULL,
    table_name       VARCHAR(64)  NOT NULL,
    records_total    INTEGER      DEFAULT 0,
    records_inserted INTEGER      DEFAULT 0,
    records_skipped  INTEGER      DEFAULT 0,
    records_failed   INTEGER      DEFAULT 0,
    status           VARCHAR(16)  DEFAULT 'running'
                     CHECK (status IN ('running','success','partial','failed')),
    error_detail     TEXT,
    started_at       TIMESTAMPTZ  DEFAULT NOW(),
    finished_at      TIMESTAMPTZ
);
