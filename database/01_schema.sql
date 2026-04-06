-- ============================================================
-- 广州前途财务日报系统 · PostgreSQL 完整 Schema
-- Version: 1.0  |  2026-03-17
-- ============================================================

-- 扩展
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- 用于顾问姓名模糊搜索

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
-- 离职：exit_date 有值
-- 老顾问：入职超过6个月（以查询执行日为基准）
-- 新顾问：入职不足6个月或入职日期缺失
CREATE OR REPLACE VIEW v_advisor_with_status AS
SELECT *,
    CASE
        WHEN exit_date IS NOT NULL THEN '离职'
        WHEN entry_date IS NOT NULL
             AND entry_date + INTERVAL '6 months' <= CURRENT_DATE THEN '老顾问'
        ELSE '新顾问'
    END AS status
FROM dim_advisor;

-- dim_users: 系统使用者与 RBAC 权限表
CREATE TABLE IF NOT EXISTS dim_users (
    id               UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
    employee_id      VARCHAR(32)  REFERENCES dim_advisor(advisor_id) ON DELETE SET NULL,
    username         VARCHAR(64)  NOT NULL UNIQUE,
    hashed_password  VARCHAR(256) NOT NULL,
    role             VARCHAR(16)  NOT NULL CHECK (role IN ('ADMIN','MANAGER','ADVISOR')),
    -- ADMIN=总盘全量  MANAGER=按department_scope过滤  ADVISOR=按advisor_name过滤
    department_scope VARCHAR(64),                         -- MANAGER 所管部门
    is_active        BOOLEAN      DEFAULT TRUE,
    last_login       TIMESTAMPTZ,
    created_at       TIMESTAMPTZ  DEFAULT NOW()
);

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

-- dim_contract_group: 合同号→分组部门映射
CREATE TABLE IF NOT EXISTS dim_contract_group (
    contract_no     VARCHAR(64)  PRIMARY KEY,
    group_dept      VARCHAR(64)  NOT NULL,
    updated_at      TIMESTAMPTZ  DEFAULT NOW()
);

-- ============================================================
-- 事实表
-- ============================================================

-- fact_signing: 签约事实表
-- contract_no 不做主键：学校签约的班级编码可对应多名学生，允许重复
-- 采用按 source_system 全量刷新策略保证幂等，定版数据(月更/历史)不做去重过滤
CREATE TABLE IF NOT EXISTS fact_signing (
    id              BIGSERIAL    PRIMARY KEY,
    contract_no     VARCHAR(64)  NOT NULL,               -- 合同号/班级编码/订单号
    sign_date       DATE         NOT NULL,
    advisor_name    VARCHAR(64)  DEFAULT '',             -- 广州前途/前途出国允许为空
    original_dept   VARCHAR(64)  DEFAULT '',
    line            VARCHAR(32)  DEFAULT '',             -- 条线：亚洲/欧洲/英国等
    sub_line        VARCHAR(64)  DEFAULT '',             -- 二级条线名称
    secondary_group VARCHAR(64)  DEFAULT '未知部门',
    sign_biz_type   VARCHAR(16)  DEFAULT '留学'         -- 留学 / 多语
                    CHECK (sign_biz_type IN ('留学','多语')),
    school          VARCHAR(32)  DEFAULT 'ERP'           -- ERP/广州前途/前途出国/迅程
                    CHECK (school IN ('ERP','广州前途','前途出国','迅程')),
    gross_sign      DECIMAL(14,2) NOT NULL DEFAULT 0,    -- 毛签金额（元）
    source_system   VARCHAR(16)  DEFAULT '日更'         -- 日更/月更/历史
                    CHECK (source_system IN ('日更','月更','历史')),
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_signing_contract  ON fact_signing(contract_no);
CREATE INDEX IF NOT EXISTS idx_signing_date      ON fact_signing(sign_date);
CREATE INDEX IF NOT EXISTS idx_signing_advisor   ON fact_signing(advisor_name);
CREATE INDEX IF NOT EXISTS idx_signing_group     ON fact_signing(secondary_group);
CREATE INDEX IF NOT EXISTS idx_signing_biz_type  ON fact_signing(sign_biz_type);
CREATE INDEX IF NOT EXISTS idx_signing_date_adv  ON fact_signing(sign_date, advisor_name);
CREATE INDEX IF NOT EXISTS idx_signing_orig_dept ON fact_signing(original_dept);
CREATE INDEX IF NOT EXISTS idx_signing_source    ON fact_signing(source_system);

-- fact_refund: 退费事实表
-- refund_id 不做主键：同一退费协议编号可能对应多条记录（同合同多次退费）
-- 采用按 source_system 全量刷新策略，与 fact_signing 一致
CREATE TABLE IF NOT EXISTS fact_refund (
    id              BIGSERIAL    PRIMARY KEY,
    refund_id       VARCHAR(64)  NOT NULL DEFAULT '',    -- 退费协议编号，允许重复
    refund_date     DATE         NOT NULL,
    contract_no     VARCHAR(64)  DEFAULT '',
    advisor_name    VARCHAR(64)  DEFAULT '',
    original_dept   VARCHAR(64)  DEFAULT '',
    line            VARCHAR(32)  DEFAULT '',
    sub_line        VARCHAR(64)  DEFAULT '',
    secondary_group VARCHAR(64)  DEFAULT '未知部门',
    refund_biz_type VARCHAR(16)  DEFAULT '留学'
                    CHECK (refund_biz_type IN ('留学','多语')),
    gross_refund    DECIMAL(14,2) NOT NULL DEFAULT 0,    -- 退费总金额（元）
    source_system   VARCHAR(16)  DEFAULT '日更'
                    CHECK (source_system IN ('日更','月更','历史')),
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_refund_rid      ON fact_refund(refund_id);
CREATE INDEX IF NOT EXISTS idx_refund_date     ON fact_refund(refund_date);
CREATE INDEX IF NOT EXISTS idx_refund_advisor  ON fact_refund(advisor_name);
CREATE INDEX IF NOT EXISTS idx_refund_group    ON fact_refund(secondary_group);
CREATE INDEX IF NOT EXISTS idx_refund_contract ON fact_refund(contract_no);
CREATE INDEX IF NOT EXISTS idx_refund_orig_dept ON fact_refund(original_dept);
CREATE INDEX IF NOT EXISTS idx_refund_source   ON fact_refund(source_system);

-- fact_receipt: 收款统计事实表（仅留学服务费）
-- 对应 Fact_Signing.py build_fact_receipt()
CREATE TABLE IF NOT EXISTS fact_receipt (
    receipt_no      VARCHAR(64)  PRIMARY KEY,            -- 收据号
    receipt_date    DATE         NOT NULL,               -- 收款日期
    arrived_date    DATE,                                -- 到账日期（优先用此字段）
    contract_no     VARCHAR(64)  DEFAULT '',
    advisor_name    VARCHAR(64)  DEFAULT '',
    dept            VARCHAR(64)  DEFAULT '',
    pay_method      VARCHAR(32)  DEFAULT '',
    status          VARCHAR(32)  DEFAULT '',
    amount          DECIMAL(14,2) NOT NULL DEFAULT 0,    -- 收款金额（元）
    created_at      TIMESTAMPTZ  DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  DEFAULT NOW()
);
-- 收款统计用 arrived_date 优先，缺失用 receipt_date（与原脚本逻辑对齐）
CREATE INDEX IF NOT EXISTS idx_receipt_arrived ON fact_receipt(arrived_date);
CREATE INDEX IF NOT EXISTS idx_receipt_date    ON fact_receipt(receipt_date);
CREATE INDEX IF NOT EXISTS idx_receipt_advisor ON fact_receipt(advisor_name);

-- 创建一个计算列视图，统一"有效日期"（receipt_date优先）
CREATE OR REPLACE VIEW v_receipt_effective AS
SELECT *,
    COALESCE(receipt_date, arrived_date) AS effective_date
FROM fact_receipt;

-- fact_fund_snapshot: 资金快照表（每日覆写）
-- 对应 build_fact_archived_unpaid() + build_fact_unrecognized()
CREATE TABLE IF NOT EXISTS fact_fund_snapshot (
    id              BIGSERIAL    PRIMARY KEY,
    snapshot_date   DATE         NOT NULL,
    contract_no     VARCHAR(64)  DEFAULT '',
    advisor_name    VARCHAR(64)  DEFAULT '',
    dept            VARCHAR(64)  DEFAULT '',
    secondary_group VARCHAR(64)  DEFAULT '未知部门',
    metric_type     VARCHAR(16)  NOT NULL
                    CHECK (metric_type IN ('已收款未盖章','潜在签约','未认款')),
    amount          DECIMAL(14,2) NOT NULL DEFAULT 0,   -- 余额（元）
    contract_status VARCHAR(32)  DEFAULT '',
    created_at      TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (snapshot_date, metric_type, contract_no)    -- 防重复快照
);
CREATE INDEX IF NOT EXISTS idx_snapshot_date   ON fact_fund_snapshot(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_snapshot_type   ON fact_fund_snapshot(metric_type);
CREATE INDEX IF NOT EXISTS idx_snapshot_dept   ON fact_fund_snapshot(dept);
CREATE INDEX IF NOT EXISTS idx_snapshot_advisor ON fact_fund_snapshot(advisor_name);

-- ============================================================
-- 数据摄入日志（用于防重与回滚）
-- ============================================================
CREATE TABLE IF NOT EXISTS ingest_log (
    id              BIGSERIAL    PRIMARY KEY,
    source_tag      VARCHAR(128) NOT NULL,              -- n8n批次标记，如"日更_2026-03-17"
    table_name      VARCHAR(64)  NOT NULL,
    records_total   INTEGER      DEFAULT 0,
    records_inserted INTEGER     DEFAULT 0,
    records_skipped  INTEGER     DEFAULT 0,
    records_failed   INTEGER     DEFAULT 0,
    status          VARCHAR(16)  DEFAULT 'running'
                    CHECK (status IN ('running','success','partial','failed')),
    error_detail    TEXT,
    started_at      TIMESTAMPTZ  DEFAULT NOW(),
    finished_at     TIMESTAMPTZ
);

-- ============================================================
-- 初始管理员账户（密码 admin@qiantu2026 的 bcrypt hash，上线后立即修改）
-- ============================================================
-- 注意：实际密码 hash 由后端 Python 生成后再插入
-- INSERT INTO dim_users (username, hashed_password, role)
-- VALUES ('admin', '$2b$12$...', 'ADMIN');
