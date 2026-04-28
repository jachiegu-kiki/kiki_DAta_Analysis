"""
signing.py — 签约域（第四章）
========================================================
extract:
  · A1/A2/A3 — ERP 签约（日更/月更/历史）
  · B1/B2/B3 — 学校签约（日更/月更/历史）
  · B4       — 周更补充签约（GroupWeekAmount-OY，财务反馈追加）
  · C1       — 迅程签约（日更）
  · D        — 在线签约（月更）
load:
  · write_signing — 按 source_system 全量刷新
"""
from sqlalchemy import text

from config import get_engine, stats, ASIA
from utils import cs, cf, safe_date, combine_ymd, read_excel, normalize_biz_type
from time_boundary import FY_START, DAILY_START
from dimensions import (
    load_staff_map,
    get_group, get_group_advisor,
    get_actual_advisor, get_subline,
)


def _sign_rec(contract_no, sign_date, advisor="", dept="", line="",
              biz_type="留学", school="ERP", gross_sign=0, source="日更"):
    """统一构建签约记录"""
    cn = cs(contract_no)
    # v6: biz_type 统一走归一化，保证 '语培'/'培训' 也能正确识别
    biz_type = normalize_biz_type(biz_type)
    sg = "语培" if biz_type == "多语" else get_group(cn, advisor)
    sga = "语培" if biz_type == "多语" else get_group_advisor(cn, advisor)
    return {
        "contract_no": cn, "sign_date": sign_date,
        "advisor_name": cs(advisor), "original_dept": cs(dept),
        "actual_advisor": get_actual_advisor(cn, advisor),
        "line": cs(line), "sub_line": get_subline(cn),
        "secondary_group": sg,
        "secondary_group_advisor": sga,
        "sign_biz_type": biz_type, "school": school,
        "gross_sign": cf(gross_sign), "source_system": source,
    }


def mod_A1():
    """A1 日更签约 ERP (Sign_Archiving, date >= DAILY_START)"""
    df = read_excel("sign_archiving")
    if df is None: return []
    recs = []
    for i, (_, r) in enumerate(df.iterrows()):
        dt = safe_date(r.get("日期"))
        cn = cs(r.get("合同号"))
        if not dt or not cn or dt < DAILY_START: continue
        is_lang = cs(r.get("语言培训")) == "是"
        recs.append(_sign_rec(cn, dt, r.get("签约顾问"), r.get("部门"),
                              r.get("条线"), "多语" if is_lang else "留学",
                              "ERP", r.get("签约金额", 0), "日更"))
    print(f"  A1 日更ERP签约: {len(recs)} 条")
    return recs


def mod_A2():
    """A2 月更签约 ERP (业绩表附件1, FY_START <= date < DAILY_START)"""
    df = read_excel("performance")
    if df is None: return []
    recs = []
    for i, (_, r) in enumerate(df.iterrows()):
        dt = safe_date(r.get("日期"))
        cn = cs(r.get("合同号"))
        if not dt or not cn: continue
        if not (FY_START <= dt < DAILY_START): continue
        is_lang = cs(r.get("语言培训")) == "是"
        recs.append(_sign_rec(cn, dt, r.get("签约顾问"), r.get("部门"),
                              r.get("条线"), "多语" if is_lang else "留学",
                              "ERP", r.get("签约金额", 0), "月更"))
    print(f"  A2 月更ERP签约: {len(recs)} 条")
    return recs


def mod_A3():
    """A3 历史签约 ERP (历史数据签约明细, date < FY_START)"""
    df = read_excel("history_sign")
    if df is None: return []
    recs = []
    for i, (_, r) in enumerate(df.iterrows()):
        dt = safe_date(r.get("日期"))
        cn = cs(r.get("合同号"))
        if not dt or not cn or dt >= FY_START: continue
        is_lang = cs(r.get("语言培训")) == "是"
        recs.append(_sign_rec(cn, dt, r.get("签约顾问"), r.get("部门"),
                              r.get("条线"), "多语" if is_lang else "留学",
                              "ERP", r.get("签约金额", 0), "历史"))
    print(f"  A3 历史ERP签约: {len(recs)} 条")
    return recs


def _school_from_mgmt(mgmt_name: str) -> str:
    """学校归属（§5.4）"""
    s = cs(mgmt_name)
    if "前途出国" in s: return "前途出国"
    if "出国考试" in s: return "迅程"
    return "广州前途"


def _line_from_country(country: str) -> str:
    """条线映射（§5.2）：国家 → 亚洲/欧洲"""
    return "亚洲" if cs(country) in ASIA else "欧洲"


def mod_B1():
    """B1 日更签约 OY_Income (广州前途/前途出国, date >= DAILY_START)"""
    df = read_excel("oy_income")
    if df is None: return []
    recs = []
    for i, (_, r) in enumerate(df.iterrows()):
        dt = safe_date(r.get("业务日期"))  # 文档：使用业务日期
        if not dt or dt < DAILY_START: continue
        school_val = cs(r.get("学校"))
        if school_val not in ("广州前途", "前途出国"): continue  # 文档§4.2：学校字段过滤
        cn = cs(r.get("班级编码"))  # 注意：是班级编码，不是班级编号（§5.1）
        if not cn: continue
        # 条线映射（§5.2）：合同二级条线分类名称
        raw_line = cs(r.get("合同二级条线分类名称"))
        if "欧洲" in raw_line: line = "欧洲"
        elif any(k in raw_line for k in ("亚英","日韩","亚洲")): line = "亚洲"
        else: line = raw_line
        is_lang = cs(r.get("是否语培")) == "是"
        recs.append(_sign_rec(cn, dt, "", "", line,
                              "多语" if is_lang else "留学", school_val,
                              r.get("现金收入_人民币", 0), "日更"))
    print(f"  B1 日更学校签约: {len(recs)} 条")
    return recs


def mod_B2():
    """B2 月更签约 学校YJ (全量归广州前途, FY_START <= date < DAILY_START)"""
    df = read_excel("school_yj")
    if df is None: return []
    recs = []
    for i, (_, r) in enumerate(df.iterrows()):
        dt = combine_ymd(r.get("年份"), r.get("月份"), r.get("日"))
        if not dt or not (FY_START <= dt < DAILY_START): continue
        cn = cs(r.get("班级编号")) or f"SCHYJ_{i}"  # §5.1：月更用班级编号
        country = cs(r.get("国家"))
        recs.append(_sign_rec(cn, dt, "", "", _line_from_country(country),
                              "多语", "广州前途",  # 文档§4.2：全量归广州前途
                              r.get("当日预收款总计", 0), "月更"))
    print(f"  B2 月更学校签约: {len(recs)} 条")
    return recs


def mod_B3():
    """B3 历史签约 学校 (date < FY_START)"""
    df = read_excel("history_school")
    if df is None: return []
    recs = []
    for i, (_, r) in enumerate(df.iterrows()):
        dt = combine_ymd(r.get("年份"), r.get("月份"), r.get("日"))
        if not dt or dt >= FY_START: continue
        cn = cs(r.get("班级编号")) or f"SCH_HIST_{i}"
        mgmt = cs(r.get("管理部门名称"))
        school = _school_from_mgmt(mgmt)
        country = cs(r.get("国家"))
        recs.append(_sign_rec(cn, dt, "", "", _line_from_country(country),
                              "多语", school,
                              r.get("当日预收款总计", 0), "历史"))
    print(f"  B3 历史学校签约: {len(recs)} 条")
    return recs


def mod_C1():
    """C1 日更签约 迅程 (date >= DAILY_START)"""
    df = read_excel("xuncheng")
    if df is None: return []
    _, email_map = load_staff_map()
    recs = []
    for i, (_, r) in enumerate(df.iterrows()):
        dt = safe_date(r.get("订单支付时间"))
        if not dt or dt < DAILY_START: continue
        cn = cs(r.get("订单号"))  # §5.1：迅程用订单号
        if not cn: continue
        # 顾问：销售邮箱→查职员表（§5.5）
        email = cs(r.get("销售邮箱"))
        advisor = email_map.get(email, "")
        line = cs(r.get("条线"))
        is_lang = cs(r.get("是否语培")) == "是"
        recs.append(_sign_rec(cn, dt, advisor, "", line,
                              "多语" if is_lang else "留学", "迅程",
                              r.get("现金收入", 0), "日更"))
    print(f"  C1 迅程签约: {len(recs)} 条")
    return recs


def mod_D():
    """D 月更在线YJ (前途出国/迅程, FY_START <= date < DAILY_START)"""
    df = read_excel("online_yj")
    if df is None: return []
    recs = []
    for i, (_, r) in enumerate(df.iterrows()):
        dt = combine_ymd(r.get("年份"), r.get("月份"), r.get("日"))
        if not dt or not (FY_START <= dt < DAILY_START): continue
        mgmt = cs(r.get("管理部门名称"))
        # §5.4：含前途出国→前途出国；含出国考试→迅程；其余跳过
        if "前途出国" in mgmt:    school = "前途出国"
        elif "出国考试" in mgmt:  school = "迅程"
        else: continue  # 文档明确：其余跳过不处理
        cn = cs(r.get("班级编号")) or f"ONLINE_{i}"
        country = cs(r.get("国家"))
        recs.append(_sign_rec(cn, dt, "", "", _line_from_country(country),
                              "多语", school,
                              r.get("当日预收款总计", 0), "月更"))
    print(f"  D  月更在线签约: {len(recs)} 条")
    return recs


# ───────────────────────────────────────────────────────────────
# B4 — 周更补充档 GroupWeekAmount-OY（财务反馈追加 / 日更层补充）
# ───────────────────────────────────────────────────────────────
# 财务反馈原文（2026-04）：
#   周更档作为日更档之外的"系统外调整"补充。抓数逻辑改为：
#     时间维度 在 日更范围内（dt >= DAILY_START）
#       且
#     （ 管理部门名称 = '出国考试'   或   项目备注 包含 '申诉调整' ）
#
# 设计要点：
#   1. 第一性原理：只有"系统外调整"才需要从周更档补回，判断信号即上述两条。
#      其他记录都已在 B1（日更 OY_Income）/ C1（日更 xuncheng）/ B2,D（月更）覆盖，
#      不应在此重复抓取，否则会造成跨档重复。
#   2. 奥卡姆剃刀：完全复用既有辅助函数（_school_from_mgmt / _line_from_country /
#      normalize_biz_type via _sign_rec），不引入任何新映射。
#   3. source_system='日更'：周更档补充的是日更层数据缺口，写入策略与日更一致，
#      由 write_signing 在每次同步时自动按 source_system 全量刷新。
# ───────────────────────────────────────────────────────────────
def mod_B4():
    """B4 周更补充签约 GroupWeekAmount-OY
    抓数规则：dt >= DAILY_START
              且（管理部门名称 = '出国考试'  OR  项目备注 包含 '申诉调整'）
    """
    df = read_excel("oy_weekly_group")
    if df is None: return []
    recs = []
    for i, (_, r) in enumerate(df.iterrows()):
        # 时间维度：dt = 年+月+日，必须在日更范围内
        dt = combine_ymd(r.get("年"), r.get("月"), r.get("日"))
        if not dt or dt < DAILY_START: continue

        # 业务过滤：管理部门=出国考试  OR  项目备注 包含 申诉调整
        mgmt = cs(r.get("管理部门名称"))
        memo = cs(r.get("项目备注"))
        if not (mgmt == "出国考试" or "申诉调整" in memo):
            continue

        # 标识符：优先班级编码（在周更档从不为空），降级听课证号/合同编号/订单号
        cn = (cs(r.get("班级编码"))
              or cs(r.get("听课证号/合同编号/订单号"))
              or f"WKOY_{i}")

        # 学校归属：复用既有映射（含'前途出国'→前途出国 / 含'出国考试'→迅程 / 其余→广州前途）
        school = _school_from_mgmt(mgmt)

        # 条线：优先取 '条线' 列；为空时按 '国家' 推导
        line = cs(r.get("条线")) or _line_from_country(r.get("国家"))

        # 业务类型：'留学/培训' 列原值交给 normalize_biz_type 归一化
        #   '培训' → 多语   '留学' → 留学   空 → 留学
        biz_type_raw = cs(r.get("留学/培训")) or "留学"

        # 金额：当日预收款总计
        amount = r.get("当日预收款总计", 0)

        recs.append(_sign_rec(cn, dt, "", "", line,
                              biz_type_raw, school,
                              amount, "日更"))
    print(f"  B4 周更补充签约: {len(recs)} 条")
    return recs


def write_signing(records):
    """写入签约事实表（按 source_system 全量刷新）
    策略：先清除该数据层的旧记录，再全量写入。
    - 定版数据(月更/历史)：从源文件完整重载，不做任何过滤或去重
    - 日更数据：每日清除旧日更后重新写入
    """
    if not records: return
    # 按数据层分组
    by_source = {}
    for rec in records:
        by_source.setdefault(rec["source_system"], []).append(rec)
    total_ins = 0
    with get_engine().begin() as conn:
        for source, recs in by_source.items():
            # 先清除该数据层的旧数据
            conn.execute(text(
                "DELETE FROM fact_signing WHERE source_system = :src"
            ), {"src": source})
            # 全量写入，不做去重
            for rec in recs:
                conn.execute(text("""
                    INSERT INTO fact_signing
                      (contract_no,sign_date,advisor_name,original_dept,actual_advisor,line,sub_line,
                       secondary_group,secondary_group_advisor,sign_biz_type,school,gross_sign,source_system)
                    VALUES (:contract_no,:sign_date,:advisor_name,:original_dept,:actual_advisor,:line,:sub_line,
                            :secondary_group,:secondary_group_advisor,:sign_biz_type,:school,:gross_sign,:source_system)
                """), rec)
            total_ins += len(recs)
            print(f"    {source}: 清除旧数据 → 写入 {len(recs)} 条")
    stats["fact_signing"] = total_ins
    print(f"  ✓ 签约合计写入 {total_ins} 条")
