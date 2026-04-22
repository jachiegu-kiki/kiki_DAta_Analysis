# backend/app/services/aggregation.py
"""
聚合服务 v5.0
────────────────────────────────────────────────────────────────
本版变更（相对 v4.2 / v4.3）:
  [Fix P1] rbac_receipt MANAGER 分支误引用 fact_receipt.secondary_group_advisor
           （此列不存在）→ 改走 contract_no → fact_signing 子查询。
  [Fix P2] rbac_fund MANAGER 分支同上 → 改走 contract_no 子查询。
  [Fix P3] ufilter_receipt 的 adv_group_all 分支同上 → 改走 contract_no
           子查询。P1/P2/P3 合并用同一个 helper 产生。
  [v4.3]   receipt 永久剔除 status='作废'；receipt/fund 支援 line / sub_line
           / group_sys / biz_type filter 经 contract_no 回溯 fact_signing。
  [新] 目标表 dim_monthly_target 支援 sign_biz_type 维度（migration 07）。
           filter_biz_type 时 target 自动按 biz_type 求和。
  [新] SCOPED 角色: 多维度多值白名单（line / biz_block 等）作为永久
           WHERE 子句，套在所有 fact 表 + metadata 下拉 + target 查询上。
           用户 UI filter 仍可在 scope 内进一步收敛（AND 交集）。

第一性原理:
  RBAC 不是魔法标签，是「能看哪些 row」的谓词。任何维度缺列，
  就用 contract_no 回溯 fact_signing 推导，避免 schema 硬扩展。
────────────────────────────────────────────────────────────────
"""
from datetime import date, timedelta
from typing import Optional, List, Dict
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import math


def get_fy_start(d):
    return date(d.year, 6, 1) if d.month >= 6 else date(d.year - 1, 6, 1)

def get_fiscal_week_start(d):
    fy_start = get_fy_start(d)
    fy_end = date(fy_start.year + 1, 6, 1) - timedelta(days=1)
    days_to_sun = (6 - fy_start.weekday()) % 7
    first_week_end = fy_start + timedelta(days=days_to_sun)
    if d <= first_week_end: return fy_start
    last_week_start = fy_end - timedelta(days=fy_end.weekday())
    if d >= last_week_start: return last_week_start
    return d - timedelta(days=d.weekday())

def get_fiscal_week_number(d):
    fy_start = get_fy_start(d)
    days_to_sun = (6 - fy_start.weekday()) % 7
    first_week_end = fy_start + timedelta(days=days_to_sun)
    if d <= first_week_end: return 1
    return 2 + (d - first_week_end - timedelta(days=1)).days // 7

def get_prev_year_date(d):
    try: return d.replace(year=d.year - 1)
    except ValueError: return d.replace(year=d.year - 1, day=28)

def safe_pct(new, old):
    if old is None or old == 0 or (isinstance(old, float) and math.isnan(old)): return None
    return round((new - old) / abs(old) * 100, 2)

def safe_round(v, digits=2):
    if v is None: return None
    try:
        f = float(v)
        return None if math.isnan(f) else round(f, digits)
    except (TypeError, ValueError): return None


async def build_daily_report(
    db: AsyncSession, today: date, role: str = "ADMIN",
    dept_scope: Optional[str] = None, advisor_name: Optional[str] = None,
    scope: Optional[Dict[str, List[str]]] = None,   # v5 新增: SCOPED 角色用
    filter_depts: Optional[List[str]] = None,
    filter_advisors: Optional[List[str]] = None,
    filter_line: Optional[List[str]] = None,
    filter_sub_line: Optional[List[str]] = None,
    filter_group_sys: Optional[List[str]] = None,
    filter_biz_block: Optional[List[str]] = None,
    filter_group_l1: Optional[List[str]] = None,
    filter_group_advisor: Optional[List[str]] = None,
    filter_biz_type: Optional[List[str]] = None,
) -> dict:

    fy_start    = get_fy_start(today)
    week_start  = get_fiscal_week_start(today)
    month_start = today.replace(day=1)
    yesterday   = today - timedelta(days=1)

    yoy_today       = get_prev_year_date(today)
    yoy_yesterday   = get_prev_year_date(yesterday)
    yoy_week_start  = get_prev_year_date(week_start)
    yoy_month_start = get_prev_year_date(month_start)
    yoy_fy_start    = get_prev_year_date(fy_start)

    prev_week_end   = week_start - timedelta(days=1)
    prev_week_start = get_fiscal_week_start(prev_week_end)

    if month_start.month == 1:
        prev_month_start = month_start.replace(year=month_start.year - 1, month=12)
    else:
        prev_month_start = month_start.replace(month=month_start.month - 1)
    try:
        prev_month_today = today.replace(year=prev_month_start.year, month=prev_month_start.month)
    except ValueError:
        import calendar as cal
        last_day = cal.monthrange(prev_month_start.year, prev_month_start.month)[1]
        prev_month_today = today.replace(year=prev_month_start.year, month=prev_month_start.month, day=last_day)

    # ── SCOPED 角色的 scope 处理（v5 新增）──
    # scope 结构: {"line": [...], "biz_block": [...], "sub_line": [...], ...}
    # 作为永久 WHERE 附加到所有查询，UI filter 在 scope 内进一步收敛。
    scope = scope or {}
    s_line     = scope.get("line")       or None
    s_sub_line = scope.get("sub_line")   or None
    s_bblock   = scope.get("biz_block")  or None
    s_gl1      = scope.get("group_l1")   or None
    s_gadv     = scope.get("group_advisor") or None
    s_biztype  = scope.get("biz_type")   or None

    # ── RBAC (v5) ──
    # 统一口径 + 修复 P1/P2/P3:
    #   ADMIN    → 1=1
    #   MANAGER  → fact_signing/refund 直接 secondary_group_advisor 列；
    #              fact_receipt/fund 无该列 → contract_no IN (SELECT ...)
    #   ADVISOR  → actual_advisor（四张表都有此列）
    #   SCOPED   → scope 谓词（line / biz_block 等），经 contract_no 回溯
    def _scope_sign(alias):
        """SCOPED: fact_signing / fact_refund 谓词（两张表同构）"""
        parts = []
        if s_line:     parts.append(f"{alias}.line = ANY(:scope_line)")
        if s_sub_line: parts.append(f"{alias}.sub_line = ANY(:scope_sub_line)")
        if s_bblock:
            parts.append(f"{alias}.secondary_group_advisor IN "
                         f"(SELECT secondary_group FROM dim_group_dept WHERE biz_block = ANY(:scope_bblock))")
        if s_gl1:
            parts.append(f"{alias}.secondary_group_advisor IN "
                         f"(SELECT secondary_group FROM dim_group_dept WHERE primary_group = ANY(:scope_gl1))")
        if s_gadv:
            parts.append(f"{alias}.secondary_group_advisor = ANY(:scope_gadv)")
        if s_biztype:
            col = "sign_biz_type" if alias == "fs" else "refund_biz_type"
            parts.append(f"{alias}.{col} = ANY(:scope_biztype)")
        return "(" + " AND ".join(parts) + ")" if parts else "1=1"

    def _scope_via_contract(alias):
        """SCOPED: fact_receipt / fact_fund_snapshot 谓词（经 contract_no 回溯）"""
        sub = []
        if s_line:     sub.append("line = ANY(:scope_line)")
        if s_sub_line: sub.append("sub_line = ANY(:scope_sub_line)")
        if s_bblock:
            sub.append("secondary_group_advisor IN "
                       "(SELECT secondary_group FROM dim_group_dept WHERE biz_block = ANY(:scope_bblock))")
        if s_gl1:
            sub.append("secondary_group_advisor IN "
                       "(SELECT secondary_group FROM dim_group_dept WHERE primary_group = ANY(:scope_gl1))")
        if s_gadv:
            sub.append("secondary_group_advisor = ANY(:scope_gadv)")
        if s_biztype:
            sub.append("sign_biz_type = ANY(:scope_biztype)")
        if not sub:
            return "1=1"
        return f"{alias}.contract_no IN (SELECT contract_no FROM fact_signing WHERE {' AND '.join(sub)})"

    def rbac_sign(alias="fs"):
        if role == "ADMIN": return "1=1"
        if role == "SCOPED": return _scope_sign(alias)
        if role == "MANAGER" and dept_scope: return f"{alias}.secondary_group_advisor = :dept_scope"
        if role == "ADVISOR" and advisor_name: return f"{alias}.actual_advisor = :advisor_name"
        return "1=1"

    def rbac_receipt(alias="fr"):
        if role == "ADMIN": return "1=1"
        if role == "SCOPED": return _scope_via_contract(alias)
        if role == "MANAGER" and dept_scope:
            # [Fix P1] fact_receipt 无 secondary_group_advisor 列 → 回溯 fact_signing
            return (f"{alias}.contract_no IN "
                    f"(SELECT contract_no FROM fact_signing WHERE secondary_group_advisor = :dept_scope)")
        if role == "ADVISOR" and advisor_name: return f"{alias}.actual_advisor = :advisor_name"
        return "1=1"

    def rbac_fund(alias="fs"):
        if role == "ADMIN": return "1=1"
        if role == "SCOPED": return _scope_via_contract(alias)
        if role == "MANAGER" and dept_scope:
            # [Fix P2] fact_fund_snapshot 无 secondary_group_advisor 列 → 回溯 fact_signing
            return (f"{alias}.contract_no IN "
                    f"(SELECT contract_no FROM fact_signing WHERE secondary_group_advisor = :dept_scope)")
        if role == "ADVISOR" and advisor_name: return f"{alias}.actual_advisor = :advisor_name"
        return "1=1"

    # ── 预计算：展开业务板块/一级分组部门 → 二级分组部门列表 ──
    resolved_group_adv_from_dim = []
    if filter_biz_block or filter_group_l1:
        dim_clauses = []
        if filter_biz_block: dim_clauses.append("biz_block = ANY(:filter_biz_block)")
        if filter_group_l1:  dim_clauses.append("primary_group = ANY(:filter_group_l1)")
        dim_where = " OR ".join(dim_clauses)
        dim_rows = (await db.execute(text(f"""
            SELECT secondary_group FROM dim_group_dept WHERE {dim_where}
        """), {"filter_biz_block": filter_biz_block or [], "filter_group_l1": filter_group_l1 or []})).scalars().all()
        resolved_group_adv_from_dim = list(dim_rows)

    adv_group_all = list(filter_group_advisor or []) + resolved_group_adv_from_dim

    # ── 目标联动：解析筛选条件 → secondary_group 列表 ──
    target_groups = []
    if filter_group_sys:
        target_groups.extend(filter_group_sys)
    if adv_group_all:
        target_groups.extend(adv_group_all)
    if filter_line or filter_sub_line:
        line_clauses = []
        if filter_line: line_clauses.append("line = ANY(:filter_line)")
        if filter_sub_line: line_clauses.append("sub_line = ANY(:filter_sub_line)")
        line_where = " AND ".join(line_clauses)
        line_groups = (await db.execute(text(f"""
            SELECT DISTINCT secondary_group FROM fact_signing
            WHERE sign_date BETWEEN :fy_start AND :today AND {line_where}
        """), {"fy_start": fy_start, "today": today,
               "filter_line": filter_line or [], "filter_sub_line": filter_sub_line or []})).scalars().all()
        target_groups.extend(line_groups)
    target_groups = list(set(target_groups)) if target_groups else []

    # ── SCOPED: scope 也要约束 target 求和（v5 新增）──
    scope_target_groups = []
    if role == "SCOPED":
        if s_bblock:
            rows = (await db.execute(text(
                "SELECT secondary_group FROM dim_group_dept WHERE biz_block = ANY(:bb)"
            ), {"bb": s_bblock})).scalars().all()
            scope_target_groups.extend(rows)
        if s_gl1:
            rows = (await db.execute(text(
                "SELECT secondary_group FROM dim_group_dept WHERE primary_group = ANY(:gl1)"
            ), {"gl1": s_gl1})).scalars().all()
            scope_target_groups.extend(rows)
        if s_gadv:
            scope_target_groups.extend(s_gadv)
        if s_line or s_sub_line:
            line_clauses = []
            if s_line:     line_clauses.append("line = ANY(:sl)")
            if s_sub_line: line_clauses.append("sub_line = ANY(:ssl)")
            rows = (await db.execute(text(f"""
                SELECT DISTINCT secondary_group FROM fact_signing
                WHERE sign_date BETWEEN :fy_start AND :today AND {' AND '.join(line_clauses)}
            """), {"fy_start": fy_start, "today": today,
                   "sl": s_line or [], "ssl": s_sub_line or []})).scalars().all()
            scope_target_groups.extend(rows)
        scope_target_groups = list(set(scope_target_groups))

    # ── 用户筛选函数 ──
    def ufilter_sign(alias="fs"):
        clauses = []
        if filter_depts:    clauses.append(f"{alias}.original_dept = ANY(:filter_depts)")
        if filter_advisors: clauses.append(f"{alias}.actual_advisor = ANY(:filter_advisors)")
        if filter_line:      clauses.append(f"{alias}.line = ANY(:filter_line)")
        if filter_sub_line:  clauses.append(f"{alias}.sub_line = ANY(:filter_sub_line)")
        if filter_group_sys: clauses.append(f"{alias}.secondary_group = ANY(:filter_group_sys)")
        if adv_group_all:    clauses.append(f"{alias}.secondary_group_advisor = ANY(:filter_group_adv_all)")
        if filter_biz_type:  clauses.append(f"{alias}.sign_biz_type = ANY(:filter_biz_type)")
        return " AND ".join(clauses) if clauses else "1=1"

    def ufilter_refund(alias="rf"):
        clauses = []
        if filter_depts:    clauses.append(f"{alias}.original_dept = ANY(:filter_depts)")
        if filter_advisors: clauses.append(f"{alias}.actual_advisor = ANY(:filter_advisors)")
        if filter_line:      clauses.append(f"{alias}.line = ANY(:filter_line)")
        if filter_sub_line:  clauses.append(f"{alias}.sub_line = ANY(:filter_sub_line)")
        if filter_group_sys: clauses.append(f"{alias}.secondary_group = ANY(:filter_group_sys)")
        if adv_group_all:    clauses.append(f"{alias}.secondary_group_advisor = ANY(:filter_group_adv_all)")
        if filter_biz_type:  clauses.append(f"{alias}.refund_biz_type = ANY(:filter_biz_type)")
        return " AND ".join(clauses) if clauses else "1=1"

    def ufilter_receipt(alias="fr"):
        # v4.3: 作废收据永久剔除
        clauses = [f"{alias}.status IS DISTINCT FROM '作废'"]
        if filter_depts:    clauses.append(f"{alias}.dept = ANY(:filter_depts)")
        if filter_advisors: clauses.append(f"{alias}.actual_advisor = ANY(:filter_advisors)")
        # [Fix P3] fact_receipt 无 secondary_group_advisor → contract_no 回溯
        if adv_group_all:
            clauses.append(f"{alias}.contract_no IN "
                           f"(SELECT contract_no FROM fact_signing WHERE secondary_group_advisor = ANY(:filter_group_adv_all))")
        if filter_line:
            clauses.append(f"{alias}.contract_no IN (SELECT contract_no FROM fact_signing WHERE line = ANY(:filter_line))")
        if filter_sub_line:
            clauses.append(f"{alias}.contract_no IN (SELECT contract_no FROM fact_signing WHERE sub_line = ANY(:filter_sub_line))")
        if filter_group_sys:
            clauses.append(f"{alias}.contract_no IN (SELECT contract_no FROM fact_signing WHERE secondary_group = ANY(:filter_group_sys))")
        if filter_biz_type: clauses.append(f"{alias}.sign_biz_type = ANY(:filter_biz_type)")
        return " AND ".join(clauses)

    def ufilter_fund(alias="fs"):
        clauses = []
        if filter_depts:    clauses.append(f"{alias}.dept = ANY(:filter_depts)")
        if filter_advisors: clauses.append(f"{alias}.actual_advisor = ANY(:filter_advisors)")
        if filter_group_sys: clauses.append(f"{alias}.secondary_group = ANY(:filter_group_sys)")
        if adv_group_all:    clauses.append(f"{alias}.secondary_group = ANY(:filter_group_adv_all)")
        if filter_line:
            clauses.append(f"{alias}.contract_no IN (SELECT contract_no FROM fact_signing WHERE line = ANY(:filter_line))")
        if filter_sub_line:
            clauses.append(f"{alias}.contract_no IN (SELECT contract_no FROM fact_signing WHERE sub_line = ANY(:filter_sub_line))")
        if filter_biz_type:
            clauses.append(f"{alias}.contract_no IN (SELECT contract_no FROM fact_signing WHERE sign_biz_type = ANY(:filter_biz_type))")
        return " AND ".join(clauses) if clauses else "1=1"

    params = {
        "today": today, "yesterday": yesterday, "week_start": week_start,
        "month_start": month_start, "fy_start": fy_start,
        "yoy_today": yoy_today, "yoy_yesterday": yoy_yesterday,
        "yoy_week_start": yoy_week_start, "yoy_month_start": yoy_month_start,
        "yoy_fy_start": yoy_fy_start,
        "prev_week_start": prev_week_start, "prev_week_end": prev_week_end,
        "prev_month_start": prev_month_start, "prev_month_today": prev_month_today,
        "dept_scope": dept_scope or "", "dept_like": f"%{dept_scope}%" if dept_scope else "%",
        "advisor_name": advisor_name or "",
        "filter_depts": filter_depts or [],
        "filter_advisors": filter_advisors or [],
        "filter_line": filter_line or [],
        "filter_sub_line": filter_sub_line or [],
        "filter_group_sys": filter_group_sys or [],
        "filter_group_adv_all": adv_group_all or [],
        "filter_biz_type": filter_biz_type or [],
        "target_groups": target_groups or [],
        # SCOPED 参数绑定
        "scope_line":     s_line or [],
        "scope_sub_line": s_sub_line or [],
        "scope_bblock":   s_bblock or [],
        "scope_gl1":      s_gl1 or [],
        "scope_gadv":     s_gadv or [],
        "scope_biztype":  s_biztype or [],
    }

    r_rbac = rbac_receipt()
    s_rbac = rbac_sign("fs")
    rf_rbac = rbac_sign("rf")
    fund_rbac = rbac_fund("fs")
    r_uf = ufilter_receipt()
    s_uf = ufilter_sign("fs")
    rf_uf = ufilter_refund("rf")
    fund_uf = ufilter_fund("fs")

    # ── 收款 KPI ──
    pr = (await db.execute(text(f"""
        SELECT
            SUM(CASE WHEN receipt_date=:today AND {r_rbac} AND {r_uf} THEN amount ELSE 0 END)/10000.0 AS daily_cur,
            SUM(CASE WHEN receipt_date=:yesterday AND {r_rbac} AND {r_uf} THEN amount ELSE 0 END)/10000.0 AS daily_wow,
            SUM(CASE WHEN receipt_date=:yoy_today AND {r_rbac} AND {r_uf} THEN amount ELSE 0 END)/10000.0 AS daily_yoy,
            SUM(CASE WHEN receipt_date BETWEEN :week_start AND :today AND {r_rbac} AND {r_uf} THEN amount ELSE 0 END)/10000.0 AS weekly_cur,
            SUM(CASE WHEN receipt_date BETWEEN :prev_week_start AND :prev_week_end AND {r_rbac} AND {r_uf} THEN amount ELSE 0 END)/10000.0 AS weekly_wow,
            SUM(CASE WHEN receipt_date BETWEEN :yoy_week_start AND :yoy_today AND {r_rbac} AND {r_uf} THEN amount ELSE 0 END)/10000.0 AS weekly_yoy,
            SUM(CASE WHEN receipt_date BETWEEN :month_start AND :today AND {r_rbac} AND {r_uf} THEN amount ELSE 0 END)/10000.0 AS monthly_cur,
            SUM(CASE WHEN receipt_date BETWEEN :yoy_month_start AND :yoy_today AND {r_rbac} AND {r_uf} THEN amount ELSE 0 END)/10000.0 AS monthly_yoy,
            SUM(CASE WHEN receipt_date BETWEEN :prev_month_start AND :prev_month_today AND {r_rbac} AND {r_uf} THEN amount ELSE 0 END)/10000.0 AS monthly_mom,
            SUM(CASE WHEN receipt_date BETWEEN :fy_start AND :today AND {r_rbac} AND {r_uf} THEN amount ELSE 0 END)/10000.0 AS fy_cur,
            SUM(CASE WHEN receipt_date BETWEEN :yoy_fy_start AND :yoy_today AND {r_rbac} AND {r_uf} THEN amount ELSE 0 END)/10000.0 AS fy_yoy
        FROM fact_receipt fr
    """), params)).mappings().one()

    # ── 净签 KPI ──
    sr = (await db.execute(text(f"""
        WITH sign_agg AS (
            SELECT
                SUM(CASE WHEN sign_date=:today THEN gross_sign ELSE 0 END) AS d_gs,
                SUM(CASE WHEN sign_date=:yesterday THEN gross_sign ELSE 0 END) AS d_wow_gs,
                SUM(CASE WHEN sign_date=:yoy_today THEN gross_sign ELSE 0 END) AS d_yoy_gs,
                SUM(CASE WHEN sign_date BETWEEN :week_start AND :today THEN gross_sign ELSE 0 END) AS w_gs,
                SUM(CASE WHEN sign_date BETWEEN :prev_week_start AND :prev_week_end THEN gross_sign ELSE 0 END) AS w_wow_gs,
                SUM(CASE WHEN sign_date BETWEEN :yoy_week_start AND :yoy_today THEN gross_sign ELSE 0 END) AS w_yoy_gs,
                SUM(CASE WHEN sign_date BETWEEN :month_start AND :today THEN gross_sign ELSE 0 END) AS m_gs,
                SUM(CASE WHEN sign_date BETWEEN :yoy_month_start AND :yoy_today THEN gross_sign ELSE 0 END) AS m_yoy_gs,
                SUM(CASE WHEN sign_date BETWEEN :prev_month_start AND :prev_month_today THEN gross_sign ELSE 0 END) AS m_mom_gs,
                SUM(CASE WHEN sign_date BETWEEN :fy_start AND :today THEN gross_sign ELSE 0 END) AS fy_gs,
                SUM(CASE WHEN sign_date BETWEEN :yoy_fy_start AND :yoy_today THEN gross_sign ELSE 0 END) AS fy_yoy_gs
            FROM fact_signing fs WHERE {s_rbac} AND {s_uf}
        ),
        refund_agg AS (
            SELECT
                SUM(CASE WHEN refund_date=:today THEN gross_refund ELSE 0 END) AS d_rf,
                SUM(CASE WHEN refund_date=:yesterday THEN gross_refund ELSE 0 END) AS d_wow_rf,
                SUM(CASE WHEN refund_date=:yoy_today THEN gross_refund ELSE 0 END) AS d_yoy_rf,
                SUM(CASE WHEN refund_date BETWEEN :week_start AND :today THEN gross_refund ELSE 0 END) AS w_rf,
                SUM(CASE WHEN refund_date BETWEEN :prev_week_start AND :prev_week_end THEN gross_refund ELSE 0 END) AS w_wow_rf,
                SUM(CASE WHEN refund_date BETWEEN :yoy_week_start AND :yoy_today THEN gross_refund ELSE 0 END) AS w_yoy_rf,
                SUM(CASE WHEN refund_date BETWEEN :month_start AND :today THEN gross_refund ELSE 0 END) AS m_rf,
                SUM(CASE WHEN refund_date BETWEEN :yoy_month_start AND :yoy_today THEN gross_refund ELSE 0 END) AS m_yoy_rf,
                SUM(CASE WHEN refund_date BETWEEN :prev_month_start AND :prev_month_today THEN gross_refund ELSE 0 END) AS m_mom_rf,
                SUM(CASE WHEN refund_date BETWEEN :fy_start AND :today THEN gross_refund ELSE 0 END) AS fy_rf,
                SUM(CASE WHEN refund_date BETWEEN :yoy_fy_start AND :yoy_today THEN gross_refund ELSE 0 END) AS fy_yoy_rf
            FROM fact_refund rf WHERE {rf_rbac} AND {rf_uf}
        )
        SELECT s.*, r.* FROM sign_agg s, refund_agg r
    """), params)).mappings().one()

    # ── 目标查询（v5: biz_type 联动 + SCOPED 约束）──
    # 1. 组装 target_groups: SCOPED 下自动套 scope 限制
    if role == "SCOPED":
        if target_groups and scope_target_groups:
            effective_tg = [g for g in target_groups if g in scope_target_groups]
        elif target_groups:
            effective_tg = target_groups
        else:
            effective_tg = scope_target_groups
    else:
        effective_tg = target_groups
    if effective_tg:
        params["target_groups"] = effective_tg
        target_where = " AND secondary_group = ANY(:target_groups)"
    elif filter_depts and role != "SCOPED":
        target_where = " AND secondary_group = ANY(:filter_depts)"
    else:
        target_where = ""

    # 2. biz_type 联动（migration 07 新增 sign_biz_type 列）
    if filter_biz_type:
        target_where += " AND sign_biz_type = ANY(:filter_biz_type)"
    elif role == "SCOPED" and s_biztype:
        params["scope_biztype"] = s_biztype
        target_where += " AND sign_biz_type = ANY(:scope_biztype)"

    cur_ym = today.strftime("%Y-%m")
    monthly_target = float((await db.execute(
        text(f"SELECT COALESCE(SUM(target_amount),0) FROM dim_monthly_target WHERE year_month=:ym{target_where}"),
        params | {"ym": cur_ym})).scalar() or 0) or None

    fy_months = []
    d = fy_start
    while d <= today:
        fy_months.append(d.strftime("%Y-%m"))
        d = d.replace(year=d.year+1, month=1) if d.month == 12 else d.replace(month=d.month+1)
    fy_target = float((await db.execute(
        text(f"SELECT COALESCE(SUM(target_amount),0) FROM dim_monthly_target WHERE year_month = ANY(:months){target_where}"),
        params | {"months": fy_months})).scalar() or 0) or None

    # ══════════════════════════════════════════════════════════
    #  潛簽 / 顧問排行 / 百萬榜
    # ══════════════════════════════════════════════════════════

    # ── 资金快照：用 secondary_group 替代 dept ──
    fund_detail_rows = (await db.execute(text(f"""
        WITH unarchived AS (
            SELECT fs.secondary_group AS grp, fs.contract_no, fs.advisor_name, fs.metric_type,
                   fs.amount / 10000.0 AS amount_wan
            FROM fact_fund_snapshot fs
            WHERE fs.snapshot_date = (SELECT MAX(snapshot_date) FROM fact_fund_snapshot WHERE metric_type='已收款未盖章')
              AND fs.metric_type = '已收款未盖章' AND {fund_rbac} AND {fund_uf}
        ),
        unconfirmed AS (
            SELECT fs.secondary_group AS grp, fs.contract_no, fs.advisor_name, fs.metric_type,
                   fs.amount / 10000.0 AS amount_wan
            FROM fact_fund_snapshot fs
            WHERE fs.snapshot_date = (SELECT MAX(snapshot_date) FROM fact_fund_snapshot WHERE metric_type='未认款')
              AND fs.metric_type = '未认款' AND {fund_rbac} AND {fund_uf}
        )
        SELECT * FROM unarchived UNION ALL SELECT * FROM unconfirmed
        ORDER BY grp, amount_wan DESC
    """), params)).mappings().all()

    dept_map = {}
    for row in fund_detail_rows:
        dn = row["grp"] or "未知部门"
        if dn not in dept_map:
            dept_map[dn] = {"name": dn, "unarchived": 0.0, "unconfirmed": 0.0, "contracts": []}
        de = dept_map[dn]
        amt = float(row["amount_wan"] or 0)
        if row["metric_type"] == "已收款未盖章": de["unarchived"] += amt
        else: de["unconfirmed"] += amt
        cn = row["contract_no"] or ""
        ex = next((c for c in de["contracts"] if c["contract_no"] == cn), None)
        if ex:
            if row["metric_type"] == "已收款未盖章": ex["unarchived"] += amt
            else: ex["unconfirmed"] += amt
        else:
            de["contracts"].append({
                "student": "", "contract_no": cn, "advisor": row["advisor_name"] or "",
                "unarchived": amt if row["metric_type"] == "已收款未盖章" else 0.0,
                "unconfirmed": amt if row["metric_type"] == "未认款" else 0.0,
            })

    fund_depts = sorted(dept_map.values(), key=lambda x: x["unarchived"], reverse=True)
    for dept in fund_depts:
        dept["unarchived"]  = safe_round(dept["unarchived"], 2)
        dept["unconfirmed"] = safe_round(dept["unconfirmed"], 2)
        for c in dept["contracts"]:
            c["unarchived"]  = safe_round(c["unarchived"], 2)
            c["unconfirmed"] = safe_round(c["unconfirmed"], 2)
    t_ua = safe_round(sum(d["unarchived"] or 0 for d in fund_depts), 2)
    t_uc = safe_round(sum(d["unconfirmed"] or 0 for d in fund_depts), 2)

    # ── 顾问排行：使用 actual_advisor ──
    advisor_sign_rows = (await db.execute(text(f"""
        WITH gs AS (
            SELECT actual_advisor AS adv, SUM(gross_sign) AS gross_sign,
                   SUM(CASE WHEN sign_biz_type='多语' THEN gross_sign ELSE 0 END) AS multilang
            FROM fact_signing fs WHERE sign_date BETWEEN :month_start AND :today AND actual_advisor!='' AND {s_rbac} AND {s_uf} GROUP BY actual_advisor
        ), rf AS (
            SELECT actual_advisor AS adv, SUM(gross_refund) AS refund
            FROM fact_refund rf WHERE refund_date BETWEEN :month_start AND :today AND actual_advisor!='' AND {rf_rbac} AND {rf_uf} GROUP BY actual_advisor
        )
        SELECT COALESCE(gs.adv, rf.adv) AS name,
               ROUND(COALESCE(gs.gross_sign,0)/10000.0,4) AS gross_sign,
               ROUND(COALESCE(rf.refund,0)/10000.0,4) AS refund,
               ROUND((COALESCE(gs.gross_sign,0)-COALESCE(rf.refund,0))/10000.0,4) AS net_sign,
               ROUND(COALESCE(gs.multilang,0)/10000.0,4) AS multilang
        FROM gs FULL OUTER JOIN rf ON gs.adv = rf.adv ORDER BY net_sign DESC
    """), params)).mappings().all()

    # ── 百萬顧問榜 ──
    million_rows = (await db.execute(text(f"""
        WITH pay AS (
            SELECT actual_advisor AS adv, SUM(amount)/10000.0 AS total_payment
            FROM fact_receipt fr WHERE receipt_date BETWEEN :month_start AND :today AND actual_advisor!='' AND {r_rbac} AND {r_uf} GROUP BY actual_advisor
        ), gs AS (
            SELECT actual_advisor AS adv, SUM(gross_sign)/10000.0 AS gross_sign,
                   SUM(CASE WHEN sign_biz_type='多语' THEN gross_sign ELSE 0 END)/10000.0 AS multilang
            FROM fact_signing fs WHERE sign_date BETWEEN :month_start AND :today AND actual_advisor!='' AND {s_rbac} AND {s_uf} GROUP BY actual_advisor
        ), fund AS (
            SELECT fs.actual_advisor AS adv, SUM(fs.amount)/10000.0 AS unarchived_unconfirmed
            FROM fact_fund_snapshot fs
            WHERE fs.snapshot_date=(SELECT MAX(snapshot_date) FROM fact_fund_snapshot WHERE metric_type IN ('已收款未盖章','未认款'))
              AND fs.actual_advisor!='' AND {fund_rbac}
            GROUP BY fs.actual_advisor
        )
        SELECT pay.adv AS name, ROUND(pay.total_payment,4) AS total_payment,
               ROUND(COALESCE(gs.gross_sign,0),4) AS gross_sign, ROUND(COALESCE(gs.multilang,0),4) AS multilang,
               ROUND(COALESCE(fund.unarchived_unconfirmed,0),4) AS unarchived_unconfirmed
        FROM pay LEFT JOIN gs USING(adv) LEFT JOIN fund USING(adv)
        WHERE pay.total_payment > 0 ORDER BY total_payment DESC
    """), params)).mappings().all()

    # ── 级联筛选器元数据 ──
    all_lines_rows = (await db.execute(text(f"""
        SELECT DISTINCT line FROM fact_signing fs
        WHERE sign_date BETWEEN :fy_start AND :today AND line != '' AND {s_rbac}
        ORDER BY line
    """), params)).mappings().all()
    all_lines = [r["line"] for r in all_lines_rows]

    all_sub_lines_rows = (await db.execute(text(f"""
        SELECT DISTINCT sub_line, line FROM fact_signing fs
        WHERE sign_date BETWEEN :fy_start AND :today AND sub_line != '' AND {s_rbac}
        ORDER BY line, sub_line
    """), params)).mappings().all()
    all_sub_lines = [{"value": r["sub_line"], "parent": r["line"]} for r in all_sub_lines_rows]

    all_group_sys_rows = (await db.execute(text(f"""
        SELECT DISTINCT fs.secondary_group, fs.sub_line
        FROM fact_signing fs
        WHERE sign_date BETWEEN :fy_start AND :today AND fs.secondary_group != '' AND fs.secondary_group != '未知部门' AND {s_rbac}
        ORDER BY fs.sub_line, fs.secondary_group
    """), params)).mappings().all()
    all_group_sys = [{"value": r["secondary_group"], "parent": r["sub_line"]} for r in all_group_sys_rows]

    dim_gd_rows = (await db.execute(text(f"""
        SELECT DISTINCT dgd.biz_block, dgd.primary_group, dgd.secondary_group
        FROM dim_group_dept dgd
        INNER JOIN (
            SELECT DISTINCT fs.secondary_group_advisor AS grp
            FROM fact_signing fs
            WHERE fs.sign_date BETWEEN :fy_start AND :today
              AND fs.secondary_group_advisor != ''
              AND fs.secondary_group_advisor != '未知部门'
              AND {s_rbac}
        ) v ON dgd.secondary_group = v.grp
        WHERE dgd.biz_block != '' AND dgd.primary_group != ''
        ORDER BY dgd.biz_block, dgd.primary_group, dgd.secondary_group
    """), params)).mappings().all()

    all_biz_blocks = sorted(set(r["biz_block"] for r in dim_gd_rows))
    seen_l1 = set()
    dedup_l1 = []
    for r in dim_gd_rows:
        key = (r["primary_group"], r["biz_block"])
        if key not in seen_l1:
            seen_l1.add(key)
            dedup_l1.append({"value": r["primary_group"], "parent": r["biz_block"]})
    all_group_l1 = dedup_l1
    all_group_advisor = [{"value": r["secondary_group"], "parent": r["primary_group"]} for r in dim_gd_rows]

    all_advisors_rows = (await db.execute(text(f"""
        SELECT DISTINCT actual_advisor FROM fact_signing fs
        WHERE sign_date BETWEEN :fy_start AND :today AND actual_advisor != '' AND {s_rbac}
        ORDER BY actual_advisor
    """), params)).mappings().all()
    all_advisors = [r["actual_advisor"] for r in all_advisors_rows]

    is_advisor = (role == "ADVISOR")
    if is_advisor and advisor_name:
        all_advisors = [advisor_name]

    # ── 财周 ──
    current_fw = get_fiscal_week_number(today)

    import calendar
    month_days = calendar.monthrange(today.year, today.month)[1]
    monthly_progress = round((today.day - 1) / month_days * 100, 2)
    fy_end_date = date(fy_start.year + 1, 6, 1) - timedelta(days=1)
    fy_total_days = (fy_end_date - fy_start).days
    fiscal_progress = round((today - fy_start).days / fy_total_days * 100, 2) if fy_total_days > 0 else 0

    # ── 组装 ──
    def _cr(val, tgt): return safe_round(val / tgt * 100) if tgt else None
    def _gap(val, tgt): return safe_round(tgt - val) if tgt else None

    dc=safe_round(pr["daily_cur"],4); dw_=safe_round(pr["daily_wow"],4); dy=safe_round(pr["daily_yoy"],4)
    wc=safe_round(pr["weekly_cur"],4); ww=safe_round(pr["weekly_wow"],4); wy=safe_round(pr["weekly_yoy"],4)
    mc=safe_round(pr["monthly_cur"],4); my_=safe_round(pr["monthly_yoy"],4); mm=safe_round(pr["monthly_mom"],4)
    fyc=safe_round(pr["fy_cur"],4); fyy=safe_round(pr["fy_yoy"],4)

    def ns(a,b): return safe_round((float(sr[a] or 0)-float(sr[b] or 0))/10000, 4)
    def _gs(k): return safe_round(float(sr[k] or 0)/10000, 4)
    def _rf(k): return safe_round(float(sr[k] or 0)/10000, 4)

    d_net=ns("d_gs","d_rf"); d_wow_n=ns("d_wow_gs","d_wow_rf"); d_yoy_n=ns("d_yoy_gs","d_yoy_rf")
    w_net=ns("w_gs","w_rf"); w_wow_n=ns("w_wow_gs","w_wow_rf"); w_yoy_n=ns("w_yoy_gs","w_yoy_rf")
    m_net=ns("m_gs","m_rf"); m_yoy_n=ns("m_yoy_gs","m_yoy_rf"); m_mom_n=ns("m_mom_gs","m_mom_rf")
    fy_net=ns("fy_gs","fy_rf"); fy_yoy_n=ns("fy_yoy_gs","fy_yoy_rf")

    fund_obj = {"total_unarchived": t_ua, "total_unconfirmed": t_uc, "departments": fund_depts}

    return {
        "header": {
            "company_name": "广州前途财务日报", "monthly_time_progress": monthly_progress,
            "fiscal_time_progress": fiscal_progress,
            "update_time": today.strftime("%Y/%m/%d") + " 18:00",
            "execution_date": today.isoformat(), "fiscal_week_start": week_start.isoformat(),
            "fiscal_week_number": current_fw,
        },
        "viewer": {
            "role": role,
            "advisor_name": advisor_name,
            "is_advisor": is_advisor,
        },
        "kpi_payment": {
            "daily":       {"value": safe_round(dc,2), "wow_pct": safe_pct(dc,dw_), "yoy_pct": safe_pct(dc,dy)},
            "weekly":      {"value": safe_round(wc,2), "wow_pct": safe_pct(wc,ww),  "yoy_pct": safe_pct(wc,wy)},
            "monthly":     {"value": safe_round(mc,2), "yoy_pct": safe_pct(mc,my_), "mom_pct": safe_pct(mc,mm)},
            "fiscal_year": {"value": safe_round(fyc,2), "yoy_pct": safe_pct(fyc,fyy)},
        },
        "kpi_signing": {
            "daily":       {"value": safe_round(d_net,2), "gross_sign": _gs("d_gs"), "refund": _rf("d_rf"), "wow_pct": safe_pct(d_net,d_wow_n), "yoy_pct": safe_pct(d_net,d_yoy_n)},
            "weekly":      {"value": safe_round(w_net,2), "gross_sign": _gs("w_gs"), "refund": _rf("w_rf"), "wow_pct": safe_pct(w_net,w_wow_n), "yoy_abs": safe_round(w_net-w_yoy_n,2) if w_yoy_n is not None else None},
            "monthly":     {"value": safe_round(m_net,2), "gross_sign": _gs("m_gs"), "refund": _rf("m_rf"), "yoy_pct": safe_pct(m_net,m_yoy_n), "mom_pct": safe_pct(m_net,m_mom_n),
                            "target": safe_round(monthly_target,2), "completion_rate": _cr(m_net,monthly_target), "gap": _gap(m_net,monthly_target)},
            "fiscal_year": {"value": safe_round(fy_net,2), "gross_sign": _gs("fy_gs"), "refund": _rf("fy_rf"), "yoy_pct": safe_pct(fy_net,fy_yoy_n),
                            "target": safe_round(fy_target,2), "completion_rate": _cr(fy_net,fy_target), "gap": _gap(fy_net,fy_target)},
        },
        "fund_warning": fund_obj, "potential": fund_obj,
        "advisor_net_sign": [
            {"rank":i+1,"name":r["name"],"net_sign":safe_round(float(r["net_sign"] or 0),2),
             "gross_sign":safe_round(float(r["gross_sign"] or 0),2),"refund":safe_round(float(r["refund"] or 0),2),
             "multilang":safe_round(float(r["multilang"] or 0),2)} for i,r in enumerate(advisor_sign_rows)],
        "advisor_million": [
            {"rank":i+1,"name":r["name"],"total_payment":safe_round(float(r["total_payment"] or 0),2),
             "gross_sign":safe_round(float(r["gross_sign"] or 0),2),"multilang":safe_round(float(r["multilang"] or 0),2),
             "unarchived_unconfirmed":safe_round(float(r["unarchived_unconfirmed"] or 0),2)} for i,r in enumerate(million_rows)],
        "advisor_dept_links": {},
        "all_depts": [],
        "all_advisors": all_advisors,
        "filter_options": {
            "lines": all_lines,
            "sub_lines": all_sub_lines,
            "group_sys": all_group_sys,
            "biz_blocks": all_biz_blocks,
            "group_l1": all_group_l1,
            "group_advisor": all_group_advisor,
            "biz_types": ["留学", "多语"],
        },
    }
