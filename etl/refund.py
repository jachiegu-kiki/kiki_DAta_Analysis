"""
refund.py — 退费域（第四章 + 第八章）
========================================================
extract:
  · R1 — 日更退费 (RefundAndTrans_Detail)
  · R2 — 月更退费 (业绩表附件4)
  · R3 — 历史退费 (历史数据退费 sheet)
load:
  · write_refund — 按 source_system 全量刷新（与 write_signing 一致）
"""
from sqlalchemy import text

from config import get_engine, stats
from utils import cs, cf, safe_date, read_excel, normalize_biz_type
from time_boundary import FY_START, DAILY_START
from dimensions import (
    get_group, get_group_advisor,
    get_actual_advisor, get_subline,
)


def _refund_rec(refund_id, refund_date, contract_no="", advisor="",
                dept="", line="", biz_type="留学", gross_refund=0, source="日更"):
    # v6: biz_type 统一走归一化，保证 '语培'/'培训' 也能正确识别
    biz_type = normalize_biz_type(biz_type)
    sg = "语培" if biz_type == "多语" else get_group(contract_no, advisor)
    sga = "语培" if biz_type == "多语" else get_group_advisor(contract_no, advisor)
    return {
        "refund_id": cs(refund_id), "refund_date": refund_date,
        "contract_no": cs(contract_no),
        "advisor_name": cs(advisor), "original_dept": cs(dept),
        "actual_advisor": get_actual_advisor(contract_no, advisor),
        "line": cs(line), "sub_line": get_subline(contract_no),
        "secondary_group": sg,
        "secondary_group_advisor": sga,
        "refund_biz_type": biz_type,
        "gross_refund": cf(gross_refund), "source_system": source,
    }


def mod_R1():
    """R1 日更退费 (RefundAndTrans_Detail, date >= DAILY_START)"""
    df = read_excel("refund_daily")
    if df is None: return []
    recs = []
    for i, (_, r) in enumerate(df.iterrows()):
        dt = safe_date(r.get("日期"))
        if not dt or dt < DAILY_START: continue
        cn = cs(r.get("合同号"))
        adv = cs(r.get("签约顾问")) or cs(r.get("退费顾问"))
        rid = cs(r.get("退费协议编号")) or f"{cn}_R1_{dt}_{i}"
        is_lang = cs(r.get("语言培训")) == "是"
        recs.append(_refund_rec(rid, dt, cn, adv, r.get("部门"),
                                r.get("业务条线"), "多语" if is_lang else "留学",
                                r.get("退费总金额", 0), "日更"))
    print(f"  R1 日更退费: {len(recs)} 条")
    return recs


def mod_R2():
    """R2 月更退费 (业绩表附件4, FY_START <= date < DAILY_START)"""
    df = read_excel("perf_refund")
    if df is None: return []
    recs = []
    for i, (_, r) in enumerate(df.iterrows()):
        dt = safe_date(r.get("日期"))
        if not dt: continue
        if not (FY_START <= dt < DAILY_START): continue
        cn = cs(r.get("合同号"))
        adv = cs(r.get("签约顾问")) or cs(r.get("退费顾问"))
        rid = cs(r.get("退费协议编号")) or f"{cn}_R2_{dt}_{i}"
        is_lang = cs(r.get("语言培训")) == "是"
        recs.append(_refund_rec(rid, dt, cn, adv, r.get("部门"),
                                r.get("业务条线"), "多语" if is_lang else "留学",
                                r.get("退费总金额", 0), "月更"))
    print(f"  R2 月更退费: {len(recs)} 条")
    return recs


def mod_R3():
    """R3 历史退费 (历史数据退费sheet, date < FY_START)"""
    df = read_excel("history_refund")
    if df is None: return []
    recs = []
    for i, (_, r) in enumerate(df.iterrows()):
        dt = safe_date(r.get("日期"))
        if not dt or dt >= FY_START: continue
        cn = cs(r.get("合同号"))
        adv = cs(r.get("签约顾问")) or cs(r.get("退费顾问"))
        rid = cs(r.get("退费协议编号")) or f"{cn}_R3_{dt}_{i}"
        is_lang = cs(r.get("语言培训")) == "是"
        recs.append(_refund_rec(rid, dt, cn, adv, r.get("部门"),
                                r.get("业务条线"), "多语" if is_lang else "留学",
                                r.get("退费总金额", 0), "历史"))
    print(f"  R3 历史退费: {len(recs)} 条")
    return recs


def write_refund(records):
    """写入退费事实表（按 source_system 全量刷新，与 write_signing 一致）"""
    if not records: return
    by_source = {}
    for rec in records:
        by_source.setdefault(rec["source_system"], []).append(rec)
    total_ins = 0
    with get_engine().begin() as conn:
        for source, recs in by_source.items():
            conn.execute(text(
                "DELETE FROM fact_refund WHERE source_system = :src"
            ), {"src": source})
            for rec in recs:
                conn.execute(text("""
                    INSERT INTO fact_refund
                      (refund_id,refund_date,contract_no,advisor_name,original_dept,actual_advisor,
                       line,sub_line,secondary_group,secondary_group_advisor,refund_biz_type,gross_refund,source_system)
                    VALUES (:refund_id,:refund_date,:contract_no,:advisor_name,:original_dept,:actual_advisor,
                            :line,:sub_line,:secondary_group,:secondary_group_advisor,:refund_biz_type,:gross_refund,:source_system)
                """), rec)
            total_ins += len(recs)
            print(f"    {source}: 清除旧数据 → 写入 {len(recs)} 条")
    stats["fact_refund"] = total_ins
    print(f"  ✓ 退费合计写入 {total_ins} 条")
