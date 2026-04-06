# backend/app/services/aggregation.py
"""
聚合服务 v3.3:
  Fix1: 部门筛选用 original_dept（实际部门）而非 secondary_group（分组）
  Fix2: 潜在签约表返回 advisor_name 字段给前端
  Fix3: 已收款未盖章只统计签约日期在当财年内的记录
  Fix4: 收款KPI不返回目标
"""
from datetime import date, timedelta
from typing import Optional, List
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
    filter_depts: Optional[List[str]] = None,
    filter_advisors: Optional[List[str]] = None,
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

    # ── RBAC ──
    def rbac_sign(alias="fs"):
        if role == "ADMIN": return "1=1"
        if role == "MANAGER" and dept_scope: return f"{alias}.secondary_group = :dept_scope"
        if role == "ADVISOR" and advisor_name: return f"{alias}.advisor_name = :advisor_name"
        return "1=1"
    def rbac_receipt(alias="fr"):
        if role == "ADMIN": return "1=1"
        if role == "MANAGER" and dept_scope: return f"{alias}.dept LIKE :dept_like"
        if role == "ADVISOR" and advisor_name: return f"{alias}.advisor_name = :advisor_name"
        return "1=1"

    # ── [Fix1] 用户筛选: 签约/退费用 original_dept，收款/快照用 dept ──
    def ufilter_sign(alias="fs"):
        clauses = []
        if filter_depts:    clauses.append(f"{alias}.original_dept = ANY(:filter_depts)")
        if filter_advisors: clauses.append(f"{alias}.advisor_name = ANY(:filter_advisors)")
        return " AND ".join(clauses) if clauses else "1=1"
    def ufilter_receipt(alias="fr"):
        clauses = []
        if filter_depts:    clauses.append(f"{alias}.dept = ANY(:filter_depts)")
        if filter_advisors: clauses.append(f"{alias}.advisor_name = ANY(:filter_advisors)")
        return " AND ".join(clauses) if clauses else "1=1"
    def ufilter_fund(alias="fs"):
        clauses = []
        if filter_depts:    clauses.append(f"{alias}.dept = ANY(:filter_depts)")
        if filter_advisors: clauses.append(f"{alias}.advisor_name = ANY(:filter_advisors)")
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
    }

    r_rbac = rbac_receipt()
    s_rbac = rbac_sign("fs")
    rf_rbac = rbac_sign("rf")
    r_uf = ufilter_receipt()
    s_uf = ufilter_sign("fs")
    rf_uf = ufilter_sign("rf")
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

    # ── 目标（仅用于净签，不用于收款）──
    target_where = ""
    if filter_depts:
        target_where = " AND secondary_group = ANY(:filter_depts)"
    cur_ym = today.strftime("%Y-%m")
    monthly_target = float((await db.execute(text(f"SELECT COALESCE(SUM(target_amount),0) FROM dim_monthly_target WHERE year_month=:ym{target_where}"), params | {"ym": cur_ym})).scalar() or 0) or None
    fy_months = []
    d = fy_start
    while d <= today:
        fy_months.append(d.strftime("%Y-%m"))
        d = d.replace(year=d.year+1, month=1) if d.month == 12 else d.replace(month=d.month+1)
    fy_target = float((await db.execute(text(f"SELECT COALESCE(SUM(target_amount),0) FROM dim_monthly_target WHERE year_month = ANY(:months){target_where}"), params | {"months": fy_months})).scalar() or 0) or None

    # ── 资金快照：已收款未盖章 + 未认款 ──
    # 财年过滤已在 ETL 层完成（create_dt < FY_START 的记录不入库），此处无需二次过滤
    fund_detail_rows = (await db.execute(text(f"""
        WITH unarchived AS (
            SELECT fs.dept, fs.contract_no, fs.advisor_name, fs.metric_type,
                   fs.amount / 10000.0 AS amount_wan
            FROM fact_fund_snapshot fs
            WHERE fs.snapshot_date = (SELECT MAX(snapshot_date) FROM fact_fund_snapshot WHERE metric_type='已收款未盖章')
              AND fs.metric_type = '已收款未盖章'
              AND {fund_uf}
        ),
        unconfirmed AS (
            SELECT fs.dept, fs.contract_no, fs.advisor_name, fs.metric_type,
                   fs.amount / 10000.0 AS amount_wan
            FROM fact_fund_snapshot fs
            WHERE fs.snapshot_date = (SELECT MAX(snapshot_date) FROM fact_fund_snapshot WHERE metric_type='未认款')
              AND fs.metric_type = '未认款'
              AND {fund_uf}
        )
        SELECT * FROM unarchived
        UNION ALL
        SELECT * FROM unconfirmed
        ORDER BY dept, amount_wan DESC
    """), params)).mappings().all()

    dept_map = {}
    for row in fund_detail_rows:
        dn = row["dept"] or "未知部门"
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

    # ── 顾问排行 ──
    advisor_sign_rows = (await db.execute(text(f"""
        WITH gs AS (
            SELECT advisor_name, SUM(gross_sign) AS gross_sign,
                   SUM(CASE WHEN sign_biz_type='多语' THEN gross_sign ELSE 0 END) AS multilang
            FROM fact_signing fs WHERE sign_date BETWEEN :month_start AND :today AND advisor_name!='' AND {s_rbac} AND {s_uf} GROUP BY advisor_name
        ), rf AS (
            SELECT advisor_name, SUM(gross_refund) AS refund
            FROM fact_refund rf WHERE refund_date BETWEEN :month_start AND :today AND advisor_name!='' AND {rf_rbac} AND {rf_uf} GROUP BY advisor_name
        )
        SELECT COALESCE(gs.advisor_name, rf.advisor_name) AS name,
               ROUND(COALESCE(gs.gross_sign,0)/10000.0,4) AS gross_sign,
               ROUND(COALESCE(rf.refund,0)/10000.0,4) AS refund,
               ROUND((COALESCE(gs.gross_sign,0)-COALESCE(rf.refund,0))/10000.0,4) AS net_sign,
               ROUND(COALESCE(gs.multilang,0)/10000.0,4) AS multilang
        FROM gs FULL OUTER JOIN rf USING (advisor_name) ORDER BY net_sign DESC
    """), params)).mappings().all()

    million_rows = (await db.execute(text(f"""
        WITH pay AS (
            SELECT advisor_name, SUM(amount)/10000.0 AS total_payment
            FROM fact_receipt fr WHERE receipt_date BETWEEN :month_start AND :today AND advisor_name!='' AND {r_rbac} AND {r_uf} GROUP BY advisor_name
        ), gs AS (
            SELECT advisor_name, SUM(gross_sign)/10000.0 AS gross_sign,
                   SUM(CASE WHEN sign_biz_type='多语' THEN gross_sign ELSE 0 END)/10000.0 AS multilang
            FROM fact_signing fs WHERE sign_date BETWEEN :month_start AND :today AND advisor_name!='' AND {s_rbac} AND {s_uf} GROUP BY advisor_name
        ), fund AS (
            SELECT advisor_name, SUM(amount)/10000.0 AS unarchived_unconfirmed
            FROM fact_fund_snapshot WHERE snapshot_date=(SELECT MAX(snapshot_date) FROM fact_fund_snapshot WHERE metric_type IN ('已收款未盖章','未认款')) AND advisor_name!='' GROUP BY advisor_name
        )
        SELECT pay.advisor_name AS name, ROUND(pay.total_payment,4) AS total_payment,
               ROUND(COALESCE(gs.gross_sign,0),4) AS gross_sign, ROUND(COALESCE(gs.multilang,0),4) AS multilang,
               ROUND(COALESCE(fund.unarchived_unconfirmed,0),4) AS unarchived_unconfirmed
        FROM pay LEFT JOIN gs USING(advisor_name) LEFT JOIN fund USING(advisor_name)
        WHERE pay.total_payment > 0 ORDER BY total_payment DESC
    """), params)).mappings().all()

    # ── [Fix1] advisor_dept_links: 用 original_dept（实际部门）──
    adl_rows = (await db.execute(text(f"""
        SELECT DISTINCT advisor_name, original_dept AS dept
        FROM fact_signing fs
        WHERE sign_date BETWEEN :fy_start AND :today
          AND advisor_name != '' AND original_dept != ''
          AND {s_rbac}
        ORDER BY advisor_name, dept
    """), params)).mappings().all()
    advisor_dept_links = {}
    for row in adl_rows:
        advisor_dept_links.setdefault(row["advisor_name"], [])
        if row["dept"] not in advisor_dept_links[row["advisor_name"]]:
            advisor_dept_links[row["advisor_name"]].append(row["dept"])

    # ── [Fix1] all_depts: 用 original_dept ──
    all_depts_rows = (await db.execute(text(f"""
        SELECT DISTINCT original_dept AS dept FROM fact_signing fs
        WHERE sign_date BETWEEN :fy_start AND :today AND original_dept != '' AND {s_rbac}
        ORDER BY dept
    """), params)).mappings().all()
    all_depts = [r["dept"] for r in all_depts_rows]

    all_advisors_rows = (await db.execute(text(f"""
        SELECT DISTINCT advisor_name FROM fact_signing fs
        WHERE sign_date BETWEEN :fy_start AND :today AND advisor_name != '' AND {s_rbac}
        ORDER BY advisor_name
    """), params)).mappings().all()
    all_advisors = [r["advisor_name"] for r in all_advisors_rows]

    # ── 财周 ──
    current_fw = get_fiscal_week_number(today)

    # ── 时间进度 ──
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
        # [Fix4] 收款KPI不设目标
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
        "advisor_dept_links": advisor_dept_links,
        "all_depts": all_depts,
        "all_advisors": all_advisors,
    }
