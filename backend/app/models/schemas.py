# backend/app/models/schemas.py
"""
Pydantic v2 数据校验模型（防呆核心）
严格对应 daily_report.json 结构 + 写入接口 Payload
"""
from pydantic import BaseModel, field_validator, model_validator, constr
from datetime import date
from typing import Optional, List
import math


# ─── 写入接口：签约数据 ─────────────────────────────────────────────────────
class SigningRecord(BaseModel):
    contract_no:        constr(min_length=1)    # 绝对不可为空
    sign_date:          date                    # 强制 YYYY-MM-DD
    gross_sign_amount:  float                   # 强制数字，拒绝 "N/A" 或 "-"
    advisor_name:       str = ""
    original_dept:      str = ""
    line:               str = ""
    sub_line:           str = ""
    secondary_group:    str = "未知部门"
    sign_biz_type:      str = "留学"
    school:             str = "ERP"
    source_system:      str = "日更"

    @field_validator("gross_sign_amount")
    @classmethod
    def reject_nan_inf(cls, v):
        if math.isnan(v) or math.isinf(v):
            raise ValueError("gross_sign_amount 不可为 NaN 或 Inf，请检查原始数据")
        return round(v, 2)

    @field_validator("sign_biz_type")
    @classmethod
    def validate_biz_type(cls, v):
        if v not in ("留学", "多语"):
            raise ValueError(f"sign_biz_type 必须是 '留学' 或 '多语'，收到: {v}")
        return v

    @field_validator("school")
    @classmethod
    def validate_school(cls, v):
        if v not in ("ERP", "广州前途", "前途出国", "迅程"):
            raise ValueError(f"school 值不合法: {v}")
        return v


class IngestSigningPayload(BaseModel):
    records:    List[SigningRecord]
    source_tag: str     # 批次标记，如 "日更_2026-03-17_Sign_Archiving"


# ─── 写入接口：退费数据 ─────────────────────────────────────────────────────
class RefundRecord(BaseModel):
    refund_id:          constr(min_length=1)
    refund_date:        date
    gross_refund:       float
    contract_no:        str = ""
    advisor_name:       str = ""
    original_dept:      str = ""
    line:               str = ""
    sub_line:           str = ""
    secondary_group:    str = "未知部门"
    refund_biz_type:    str = "留学"
    source_system:      str = "日更"

    @field_validator("gross_refund")
    @classmethod
    def reject_nan_inf(cls, v):
        if math.isnan(v) or math.isinf(v):
            raise ValueError("gross_refund 不可为 NaN 或 Inf")
        return round(v, 2)


class IngestRefundPayload(BaseModel):
    records:    List[RefundRecord]
    source_tag: str


# ─── 写入接口：收款数据 ─────────────────────────────────────────────────────
class ReceiptRecord(BaseModel):
    receipt_no:     constr(min_length=1)
    receipt_date:   date
    arrived_date:   Optional[date] = None
    amount:         float
    contract_no:    str = ""
    advisor_name:   str = ""
    dept:           str = ""
    pay_method:     str = ""
    status:         str = ""
    sign_biz_type:  str = "留学"     # 业务类型（留学/多语），与 fact_signing 对齐

    @field_validator("amount")
    @classmethod
    def reject_nan_inf(cls, v):
        if math.isnan(v) or math.isinf(v):
            raise ValueError("amount 不可为 NaN 或 Inf")
        return round(v, 2)

    @field_validator("sign_biz_type")
    @classmethod
    def validate_biz_type(cls, v):
        if v not in ("留学", "多语"):
            raise ValueError("sign_biz_type 必须为 '留学' 或 '多语'")
        return v


class IngestReceiptPayload(BaseModel):
    records:    List[ReceiptRecord]
    source_tag: str


# ─── 写入接口：资金快照 ─────────────────────────────────────────────────────
class FundSnapshotRecord(BaseModel):
    snapshot_date:  date
    contract_no:    str = ""
    advisor_name:   str = ""
    dept:           str = ""
    secondary_group: str = "未知部门"
    metric_type:    str
    amount:         float
    contract_status: str = ""

    @field_validator("metric_type")
    @classmethod
    def validate_metric(cls, v):
        if v not in ("已收款未盖章", "潜在签约", "未认款"):
            raise ValueError(f"metric_type 不合法: {v}")
        return v

    @field_validator("amount")
    @classmethod
    def reject_nan_inf(cls, v):
        if math.isnan(v) or math.isinf(v):
            raise ValueError("amount 不可为 NaN 或 Inf")
        return round(v, 2)


class IngestFundSnapshotPayload(BaseModel):
    records:    List[FundSnapshotRecord]
    source_tag: str
    replace_date: Optional[date] = None  # 若指定，先删除该日期的同类型快照再插入


# ─── 维度同步：顾问表（status 不再存储，由数据库视图动态计算）──────────────
class AdvisorSyncRecord(BaseModel):
    advisor_id:      constr(min_length=1)
    name:            constr(min_length=1)
    email:           Optional[str] = None
    primary_dept:    Optional[str] = None
    secondary_group: Optional[str] = None
    entry_date:      Optional[date] = None
    exit_date:       Optional[date] = None


# ─── 维度同步：月度目标 ─────────────────────────────────────────────────────
class TargetSyncRecord(BaseModel):
    year_month:      constr(pattern=r"^\d{4}-\d{2}$")  # 强制 YYYY-MM 格式
    department:      constr(min_length=1)
    secondary_group: str = "全部"
    sign_biz_type:   str = "留学"      # v5: 新增，与 fact_signing.sign_biz_type 对齐
    target_amount:   float

    @field_validator("sign_biz_type")
    @classmethod
    def validate_biz_type(cls, v):
        if v not in ("留学", "多语"):
            raise ValueError("sign_biz_type 必须为 '留学' 或 '多语'")
        return v

    @field_validator("target_amount")
    @classmethod
    def validate_amount(cls, v):
        if v < 0:
            raise ValueError("target_amount 不可为负数")
        return round(v, 3)


# ─── 日报响应结构（严格对齐 daily_report.json）─────────────────────────────
class KpiPeriod(BaseModel):
    value:          Optional[float]
    wow_pct:        Optional[float] = None
    yoy_pct:        Optional[float] = None
    yoy_abs:        Optional[float] = None
    mom_pct:        Optional[float] = None
    target:         Optional[float] = None
    completion_rate: Optional[float] = None
    gap:            Optional[float] = None
    gross_sign:     Optional[float] = None
    refund:         Optional[float] = None


class KpiBlock(BaseModel):
    daily:       KpiPeriod
    weekly:      KpiPeriod
    monthly:     KpiPeriod
    fiscal_year: KpiPeriod


class FundDept(BaseModel):
    name:        str
    unarchived:  float
    unconfirmed: float


class FundWarning(BaseModel):
    total_unarchived:  float
    total_unconfirmed: float
    departments:       List[FundDept]


class AdvisorNetSign(BaseModel):
    rank:       int
    name:       str
    net_sign:   float
    gross_sign: float
    refund:     float
    multilang:  float


class AdvisorMillion(BaseModel):
    rank:                   int
    name:                   str
    total_payment:          float
    gross_sign:             float
    multilang:              float
    unarchived_unconfirmed: float


class DailyReportHeader(BaseModel):
    company_name:            str
    monthly_time_progress:   float
    fiscal_time_progress:    float
    update_time:             str
    execution_date:          str
    fiscal_week_start:       str


class DailyReportResponse(BaseModel):
    header:           DailyReportHeader
    kpi_payment:      KpiBlock
    kpi_signing:      KpiBlock
    fund_warning:     FundWarning
    advisor_net_sign: List[AdvisorNetSign]
    advisor_million:  List[AdvisorMillion]
