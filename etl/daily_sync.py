"""
daily_sync.py v2 — 完整 ETL，严格遵循《数据逻辑说明 v3》
========================================================
架构：
  1. 时间边界计算（第一/二章）
  2. 维度表加载（职员表/签约分组/历史分组/Sign_Details二级条线）
  3. 签约模块 A1~A3 B1~B3 C1 D（第四章）
  4. 退费模块 R1~R3（第四章+第八章）
  5. 快照模块：收款/已收款未盖章/潜在签约/未认款（第二章 §2.2）
  6. 维度同步：顾问/目标/合同分组

每个模块一个函数，返回 list[dict]，互不依赖。
修改任何模块只需改对应函数，不影响其他模块。

环境变量：
  DATABASE_URL_SYNC   PostgreSQL 连接串
  PREDATA_DIR         PreData 目录
  PULLDATA_DIR        PullData 目录
"""
import os, sys, math, warnings
from datetime import datetime, date, timedelta
from typing import Optional
import pandas as pd
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ═══════════════════════════════════════════════════════════════
# §0  配置
# ═══════════════════════════════════════════════════════════════
DATABASE_URL = os.getenv("DATABASE_URL_SYNC",
    "postgresql://qiantu:qiantu2026@postgres:5432/qiantu_finance")
PREDATA_DIR  = os.getenv("PREDATA_DIR",  "/data/PreData")
PULLDATA_DIR = os.getenv("PULLDATA_DIR", "/data/PullData")
TODAY = date.today()

# 文件注册表：(目录, 文件名, sheet名, 表头行号)
# 修改文件名/sheet/表头行只需改这里
FILES = {
    # ─── PullData（n8n 自动下载）───
    "sign_archiving": (PULLDATA_DIR, "【日更】Sign_Archiving_Details.xlsx", "签约明细",       0),  # A1
    "oy_income":      (PULLDATA_DIR, "【日更】OY_Income_Talend_Hign_End_NEW.xlsx", "sheet1",  0),  # B1
    "xuncheng":       (PULLDATA_DIR, "【日更】xuncheng_sales_order_product_detail.xlsx", "sheet1", 0),  # C1
    "refund_daily":   (PULLDATA_DIR, "【日更】RefundAndTrans_Detail.xlsx",   "sheet1",        0),  # R1
    "sign_details":   (PULLDATA_DIR, "【日更】Sign_Details.xlsx",            "签约明细",       0),  # 二级条线查询表
    "contract":       (PULLDATA_DIR, "【日更】contractDetail.xlsx",          "sheet1",        0),  # 快照
    "receipt":        (PULLDATA_DIR, "【日更】receipt_statistics_d.xlsx",     "sheet1",        0),  # 收款
    "performance":    (PULLDATA_DIR, "【月更】业绩表.xlsx",                  "附件1签约明细",  1),  # A2
    "perf_refund":    (PULLDATA_DIR, "【月更】业绩表.xlsx",                  "附件4退费及转国家明细", 1),  # R2
    "school_yj":      (PULLDATA_DIR, "【月更】学校YJ.xlsx",                 "签约金额按天",   0),  # B2
    "online_yj":      (PULLDATA_DIR, "【月更】在线YJ.xlsx",                 "签约金额按天",   0),  # D
    "sign_target":    (PULLDATA_DIR, "【月更】净签目标.xlsx",               "Sheet1",         0),
    # ─── PreData（手动/维度数据）───
    "staff":          (PREDATA_DIR, "职员表.xlsx",               "顾问部门",     0),
    "sign_group":     (PREDATA_DIR, "签约分组.xlsx",             "Sheet1",       0),
    "history_sign":   (PREDATA_DIR, "历史数据.xlsx",             "签约明细",     0),  # A3
    "history_refund": (PREDATA_DIR, "历史数据.xlsx",             "退费",         0),  # R3
    "history_school": (PREDATA_DIR, "历史数据-学校.xlsx",        "签约金额按天", 0),  # B3
    "unrecognized":   (PREDATA_DIR, "【人手更】未认款明细.xlsx", "未认款明细",   1),  # 表头第2行=header=1
}

ASIA = {"日本","韩国","泰国","亚洲其他","中国香港","中国澳门","香港","澳门","新加坡","马来西亚"}

engine = None
stats = {}

# ═══════════════════════════════════════════════════════════════
# §1  通用工具
# ═══════════════════════════════════════════════════════════════
def get_engine():
    global engine
    if engine is None:
        engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
    return engine

def get_file(key):
    """返回文件路径，不存在返回 None"""
    if key not in FILES: return None
    d, fn, _, _ = FILES[key]
    p = os.path.join(d, fn)
    return p if os.path.exists(p) else None

def read_excel(key, **kwargs):
    """按注册表配置读取 Excel，返回 DataFrame 或 None"""
    p = get_file(key)
    if not p: return None
    _, _, sheet, header = FILES[key]
    try:
        return pd.read_excel(p, sheet_name=sheet, header=header, **kwargs)
    except Exception as e:
        print(f"  [警告] 读取 {key} 失败: {e}")
        return None

def cs(val, default=""):
    """Clean String: pandas NaN/None/nan → default"""
    if val is None: return default
    if isinstance(val, float) and math.isnan(val): return default
    try:
        if pd.isna(val): return default
    except: pass
    s = str(val).strip()
    return default if s.lower() == "nan" or s == "" else s

def cs_or_none(val):
    r = cs(val)
    return r if r else None

def safe_date(val):
    if val is None: return None
    try:
        if pd.isna(val): return None
    except: pass
    try: return pd.to_datetime(val).date()
    except: return None

def combine_ymd(y, m, d):
    try: return date(int(y), int(m), int(d))
    except: return None

def cf(v):
    """Clean Float"""
    try: f = float(v)
    except: return 0.0
    return 0.0 if (math.isnan(f) or math.isinf(f)) else round(f, 2)

def sep(t): print(f"\n{'='*60}\n  {t}\n{'='*60}")

# ═══════════════════════════════════════════════════════════════
# §2  时间边界（第一/二章）
# ═══════════════════════════════════════════════════════════════
def get_fy_start(d: date) -> date:
    """财年起始：月>=6 用当年6/1，否则用上年6/1"""
    return date(d.year, 6, 1) if d.month >= 6 else date(d.year - 1, 6, 1)

def _nth_business_day(year: int, month: int, n: int) -> date:
    """计算指定月份的第 n 个工作日（周一~周五）"""
    count = 0
    d = date(year, month, 1)
    while True:
        if d.weekday() < 5:  # 0=周一 … 4=周五
            count += 1
            if count == n:
                return d
        d += timedelta(days=1)

def get_daily_start(d: date) -> date:
    """日更起始：当前日期 <= 当月第7个工作日 → 上月1日；否则 → 当月1日
    （文档§2.1：月初过渡期月更数据尚未刷新，日更需覆盖上月）"""
    seventh_biz = _nth_business_day(d.year, d.month, 7)
    if d <= seventh_biz:
        # 仍在过渡期，日更需覆盖上月
        if d.month == 1:
            return date(d.year - 1, 12, 1)
        return date(d.year, d.month - 1, 1)
    else:
        # 过渡期已过，日更从当月1日起
        return date(d.year, d.month, 1)

FY_START     = get_fy_start(TODAY)
DAILY_START  = get_daily_start(TODAY)

def layer_of(dt: date) -> str:
    """判断日期属于哪个数据层"""
    if dt >= DAILY_START: return "日更"
    if dt >= FY_START:    return "月更"
    return "历史"

# ═══════════════════════════════════════════════════════════════
# §3  维度表加载（第六章）
# ═══════════════════════════════════════════════════════════════
_dim_cache = {}

def load_staff_map():
    """职员表：顾问姓名 → 二级分组部门 + 邮箱 → 顾问姓名"""
    if "staff" in _dim_cache: return _dim_cache["staff"]
    name_to_group, email_to_name = {}, {}
    df = read_excel("staff")
    if df is not None:
        for i, (_, r) in enumerate(df.iterrows()):
            name = cs(r.get("顾问"))
            if not name: continue
            name_to_group[name] = cs(r.get("二级分组部门"), "未知部门")
            email = cs(r.get("顾问邮箱"))
            if email: email_to_name[email] = name
    _dim_cache["staff"] = (name_to_group, email_to_name)
    return name_to_group, email_to_name

def load_sign_group():
    """签约分组：合同号 → 分组部门（优先级1，本财年）"""
    if "sign_group" in _dim_cache: return _dim_cache["sign_group"]
    m = {}
    df = read_excel("sign_group")
    if df is not None:
        for _, r in df.dropna(subset=["合同号"]).iterrows():
            m[cs(r["合同号"])] = cs(r["分组部门"])
    _dim_cache["sign_group"] = m
    return m

def load_history_group():
    """历史数据签约明细：合同号 → 团队分组（优先级2，往年）"""
    if "hist_group" in _dim_cache: return _dim_cache["hist_group"]
    m = {}
    df = read_excel("history_sign")
    if df is not None:
        for i, (_, r) in enumerate(df.iterrows()):
            cn = cs(r.get("合同号"))
            g = cs(r.get("团队分组"))
            if cn and g: m[cn] = g
    _dim_cache["hist_group"] = m
    return m

def load_subline_map():
    """Sign_Details：合同号 → 二级条线名称（第五章 §5.3）"""
    if "subline" in _dim_cache: return _dim_cache["subline"]
    m = {}
    df = read_excel("sign_details")
    if df is not None:
        # 尝试多种可能的列名
        cn_col = next((c for c in df.columns if "合同" in str(c) and "编号" in str(c)), None)
        sl_col = next((c for c in df.columns if "二级条线" in str(c)), None)
        if cn_col and sl_col:
            for i, (_, r) in enumerate(df.iterrows()):
                cn = cs(r.get(cn_col))
                sl = cs(r.get(sl_col))
                if cn and sl: m[cn] = sl
    _dim_cache["subline"] = m
    return m

def get_group(contract_no: str, advisor: str) -> str:
    """三级优先查找分组部门（第六章）"""
    cn = cs(contract_no)
    # 优先级1：签约分组
    sg = load_sign_group()
    if cn in sg: return sg[cn]
    # 优先级2：历史数据团队分组
    hg = load_history_group()
    if cn in hg: return hg[cn]
    # 优先级3：职员表（顾问姓名 → 部门）
    adv = cs(advisor)
    if adv:
        nm, _ = load_staff_map()
        if adv in nm: return nm[adv]
    return "未知部门"

def get_subline(contract_no: str) -> str:
    """查 Sign_Details 获取二级条线名称（第五章 §5.3）"""
    return load_subline_map().get(cs(contract_no), "")

# ═══════════════════════════════════════════════════════════════
# §4  签约模块（第四章）
# ═══════════════════════════════════════════════════════════════
def _sign_rec(contract_no, sign_date, advisor="", dept="", line="",
              biz_type="留学", school="ERP", gross_sign=0, source="日更"):
    """统一构建签约记录"""
    cn = cs(contract_no)
    return {
        "contract_no": cn, "sign_date": sign_date,
        "advisor_name": cs(advisor), "original_dept": cs(dept),
        "line": cs(line), "sub_line": get_subline(cn),
        "secondary_group": get_group(cn, advisor),
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

# ═══════════════════════════════════════════════════════════════
# §5  退费模块（第四章 + 第八章）
# ═══════════════════════════════════════════════════════════════
def _refund_rec(refund_id, refund_date, contract_no="", advisor="",
                dept="", line="", biz_type="留学", gross_refund=0, source="日更"):
    return {
        "refund_id": cs(refund_id), "refund_date": refund_date,
        "contract_no": cs(contract_no),
        "advisor_name": cs(advisor), "original_dept": cs(dept),
        "line": cs(line), "sub_line": get_subline(contract_no),
        "secondary_group": get_group(contract_no, advisor),
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

# ═══════════════════════════════════════════════════════════════
# §6  快照模块（第二章 §2.2 - 不做时间分层）
# ═══════════════════════════════════════════════════════════════
def snap_receipt():
    """收款统计：款项类别=留学服务费"""
    df = read_excel("receipt")
    if df is None: return []
    if "款项类别" in df.columns:
        df = df[df["款项类别"] == "留学服务费"].copy()
    recs = []
    for i, (_, r) in enumerate(df.iterrows()):
        dt = safe_date(r.get("收款日期"))
        rno = cs(r.get("收据号"))
        if not dt or not rno: continue
        recs.append({
            "receipt_no": rno, "receipt_date": dt,
            "arrived_date": safe_date(r.get("到账日期")),
            "contract_no": cs(r.get("合同号")),
            "advisor_name": cs(r.get("签约顾问")),
            "dept": cs(r.get("部门")),
            "pay_method": cs(r.get("收款方式")),
            "status": cs(r.get("状态")),
            "amount": cf(r.get("收款金额", 0)),
        })
    print(f"  收款统计: {len(recs)} 条")
    return recs

def snap_fund():
    """已收款未盖章 + 潜在签约 (contractDetail, §2.2)"""
    df = read_excel("contract")
    if df is None: return []
    recs = []
    for i, (_, r) in enumerate(df.iterrows()):
        # 创建日期不在当财年 → 跳过（ETL层清洗，不统计非当财年数据）
        create_dt = safe_date(r.get("创建时间"))
        if not create_dt or create_dt < FY_START: continue
        status = cs(r.get("合同状态"))
        balance = cf(r.get("服务费余额", 0))
        if balance == 0: continue
        cn = cs(r.get("合同编号"))  # §5.1：contractDetail用合同编号
        advisor = cs(r.get("签约顾问"))
        dept = cs(r.get("签约部门"))
        student = cs(r.get("客户名称"))

        if "已收款" in status:
            metric = "已收款未盖章"
        elif status in ("审批通过", "已盖章未收款") or "已盖章" in status:
            metric = "潜在签约"
        else:
            continue

        recs.append({
            "snapshot_date": TODAY, "contract_no": cn,
            "advisor_name": advisor, "dept": dept,
            "secondary_group": get_group(cn, advisor),
            "metric_type": metric, "amount": balance,
            "contract_status": status,
            "student_name": student,  # 额外字段，写入时忽略（v3前端展开用）
        })
    print(f"  资金快照(已收款未盖章+潜在): {len(recs)} 条")
    return recs

def snap_unrecognized():
    """未认款 (未认款明细, §2.2: 是否参与未认款统计=是)"""
    df = read_excel("unrecognized")
    if df is None: return []
    recs = []
    for i, (_, r) in enumerate(df.iterrows()):
        participate = cs(r.get("是否参与未认款统计"))
        if participate != "是": continue
        amount = cf(r.get("剩余金额", 0))
        if amount == 0: continue
        advisor = cs(r.get("顾问"))
        group = cs(r.get("组别"))
        # 未认款按顾问找部门（§7.4）
        nm, _ = load_staff_map()
        dept = nm.get(advisor, group) if advisor else group
        recs.append({
            "snapshot_date": TODAY, "contract_no": f"URK_{i}_{cs(r.get('汇款附言',''))[:20]}",
            "advisor_name": advisor, "dept": dept or "未知部门",
            "secondary_group": dept or "未知部门",
            "metric_type": "未认款", "amount": amount,
            "contract_status": cs(r.get("认款状态")),
        })
    print(f"  未认款快照: {len(recs)} 条")
    return recs

# ═══════════════════════════════════════════════════════════════
# §7  维度同步
# ═══════════════════════════════════════════════════════════════
def sync_dim_advisor():
    """同步顾问维度表（status 不再存储，由视图 v_advisor_with_status 动态计算）"""
    df = read_excel("staff")
    if df is None: return
    recs, seen = [], set()
    for i, (_, r) in enumerate(df.iterrows()):
        name = cs(r.get("顾问"))
        eid = cs(r.get("员工编号"))
        if not name or not eid: continue
        if eid in seen: continue
        seen.add(eid)
        recs.append({
            "advisor_id": eid, "name": name,
            "email": cs_or_none(r.get("顾问邮箱")),
            "primary_dept": cs_or_none(r.get("部门")),
            "secondary_group": cs_or_none(r.get("二级分组部门")),
            "entry_date": safe_date(r.get("入职时间")),
            "exit_date": safe_date(r.get("离职时间")),
        })
    with get_engine().begin() as conn:
        for rec in recs:
            conn.execute(text("""
                INSERT INTO dim_advisor (advisor_id,name,email,primary_dept,secondary_group,entry_date,exit_date)
                VALUES (:advisor_id,:name,:email,:primary_dept,:secondary_group,:entry_date,:exit_date)
                ON CONFLICT (advisor_id) DO UPDATE SET
                  name=EXCLUDED.name, email=COALESCE(EXCLUDED.email,dim_advisor.email),
                  primary_dept=EXCLUDED.primary_dept, secondary_group=EXCLUDED.secondary_group,
                  entry_date=EXCLUDED.entry_date, exit_date=EXCLUDED.exit_date, updated_at=NOW()
            """), rec)
    stats["dim_advisor"] = len(recs)
    print(f"  ✓ {len(recs)} 名顾问")

def _fix_excel_serial(val):
    """修复 Excel 序列号：openpyxl 读取混合格式列时，部分单元格
    返回原始 Excel 序列号(int/float) 而非 datetime，需手动转换。
    Excel 序列号以 1899-12-30 为第 0 天；25569 = 1970-01-01。"""
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        try:
            if pd.isna(val): return val
        except: pass
        if val > 25569:  # 排除不合理的小数字
            return datetime(1899, 12, 30) + timedelta(days=int(val))
    return val

def sync_dim_target():
    """同步月度目标（§7.3）"""
    df = read_excel("sign_target")
    if df is None: return
    # ★ 先将 Excel 序列号(int)转为真实日期，再统一 to_datetime
    df["所属月份"] = df["所属月份"].apply(_fix_excel_serial)
    df["所属月份"] = pd.to_datetime(df["所属月份"], errors="coerce")
    df["超额目标（万）"] = pd.to_numeric(df["超额目标（万）"], errors="coerce").fillna(0)
    recs = []
    for _, r in df.dropna(subset=["所属月份"]).iterrows():
        dept = cs(r.get("部门"))
        if not dept: continue
        recs.append({
            "year_month": r["所属月份"].strftime("%Y-%m"),
            "department": dept,
            "secondary_group": cs(r.get("二级分组部门"), "全部") or "全部",
            "target_amount": float(r["超额目标（万）"]),
        })
    with get_engine().begin() as conn:
        for rec in recs:
            conn.execute(text("""
                INSERT INTO dim_monthly_target (year_month,department,secondary_group,target_amount)
                VALUES (:year_month,:department,:secondary_group,:target_amount)
                ON CONFLICT (year_month,secondary_group) DO UPDATE SET
                  target_amount=EXCLUDED.target_amount, department=EXCLUDED.department, updated_at=NOW()
            """), rec)
    stats["dim_monthly_target"] = len(recs)
    print(f"  ✓ {len(recs)} 条目标")

def sync_dim_contract_group():
    """同步合同分组"""
    df = read_excel("sign_group")
    if df is None: return
    df = df.dropna(subset=["合同号","分组部门"]).drop_duplicates(subset=["合同号"])
    with get_engine().begin() as conn:
        for i, (_, r) in enumerate(df.iterrows()):
            conn.execute(text("""
                INSERT INTO dim_contract_group (contract_no,group_dept)
                VALUES (:cn,:gd) ON CONFLICT (contract_no) DO UPDATE SET group_dept=EXCLUDED.group_dept, updated_at=NOW()
            """), {"cn": cs(r["合同号"]), "gd": cs(r["分组部门"])})
    stats["dim_contract_group"] = len(df)
    print(f"  ✓ {len(df)} 条映射")

# ═══════════════════════════════════════════════════════════════
# §8  写入数据库
# ═══════════════════════════════════════════════════════════════
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
                      (contract_no,sign_date,advisor_name,original_dept,line,sub_line,
                       secondary_group,sign_biz_type,school,gross_sign,source_system)
                    VALUES (:contract_no,:sign_date,:advisor_name,:original_dept,:line,:sub_line,
                            :secondary_group,:sign_biz_type,:school,:gross_sign,:source_system)
                """), rec)
            total_ins += len(recs)
            print(f"    {source}: 清除旧数据 → 写入 {len(recs)} 条")
    stats["fact_signing"] = total_ins
    print(f"  ✓ 签约合计写入 {total_ins} 条")

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
                      (refund_id,refund_date,contract_no,advisor_name,original_dept,
                       line,sub_line,secondary_group,refund_biz_type,gross_refund,source_system)
                    VALUES (:refund_id,:refund_date,:contract_no,:advisor_name,:original_dept,
                            :line,:sub_line,:secondary_group,:refund_biz_type,:gross_refund,:source_system)
                """), rec)
            total_ins += len(recs)
            print(f"    {source}: 清除旧数据 → 写入 {len(recs)} 条")
    stats["fact_refund"] = total_ins
    print(f"  ✓ 退费合计写入 {total_ins} 条")

def write_receipt(records):
    """写入收款事实表（ON CONFLICT 时更新为最新数据，不再跳过）"""
    if not records: return
    ins = 0
    with get_engine().begin() as conn:
        for rec in records:
            conn.execute(text("""
                INSERT INTO fact_receipt
                  (receipt_no,receipt_date,arrived_date,contract_no,
                   advisor_name,dept,pay_method,status,amount)
                VALUES (:receipt_no,:receipt_date,:arrived_date,:contract_no,
                        :advisor_name,:dept,:pay_method,:status,:amount)
                ON CONFLICT (receipt_no) DO UPDATE SET
                  receipt_date  = EXCLUDED.receipt_date,
                  arrived_date  = EXCLUDED.arrived_date,
                  contract_no   = EXCLUDED.contract_no,
                  advisor_name  = EXCLUDED.advisor_name,
                  dept          = EXCLUDED.dept,
                  pay_method    = EXCLUDED.pay_method,
                  status        = EXCLUDED.status,
                  amount        = EXCLUDED.amount,
                  updated_at    = NOW()
            """), rec)
            ins += 1
    stats["fact_receipt"] = ins
    print(f"  ✓ 收款写入/更新 {ins} 条（全量覆盖，无跳过）")

def write_fund_snapshot(records):
    """写入资金快照（当日全量替换）"""
    if not records: return
    with get_engine().begin() as conn:
        # 先删除当日快照
        conn.execute(text("DELETE FROM fact_fund_snapshot WHERE snapshot_date = :d"), {"d": TODAY})
        for rec in records:
            clean = {k: v for k, v in rec.items() if k != "student_name"}
            conn.execute(text("""
                INSERT INTO fact_fund_snapshot
                  (snapshot_date,contract_no,advisor_name,dept,secondary_group,
                   metric_type,amount,contract_status)
                VALUES (:snapshot_date,:contract_no,:advisor_name,:dept,:secondary_group,
                        :metric_type,:amount,:contract_status)
            """), clean)
    stats["fact_fund_snapshot"] = len(records)
    print(f"  ✓ 资金快照写入 {len(records)} 条")

# ═══════════════════════════════════════════════════════════════
# §9  验证
# ═══════════════════════════════════════════════════════════════
def verify():
    sep("同步结果")
    tables = ["dim_advisor","dim_monthly_target","dim_contract_group",
              "fact_signing","fact_refund","fact_receipt","fact_fund_snapshot"]
    with get_engine().connect() as conn:
        for t in tables:
            try:
                n = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
                print(f"  {t:<30} {n:>10,} 行")
            except: pass
    print("\n  本次写入：")
    for k, v in stats.items():
        print(f"    {k:<30} +{v:>8,}")

# ═══════════════════════════════════════════════════════════════
# §10  主入口
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    start = datetime.now()
    print(f"广州前途财务日报 · 每日数据同步 v2")
    print(f"执行时间: {start:%Y-%m-%d %H:%M:%S}")
    print(f"执行日:   {TODAY}")
    print(f"财年起始: {FY_START}")
    print(f"日更起始: {DAILY_START}")
    print(f"PreData:  {PREDATA_DIR}")
    print(f"PullData: {PULLDATA_DIR}")
    print()

    # 检查文件
    for key, (d, fn, sheet, hdr) in FILES.items():
        p = os.path.join(d, fn)
        status = "✓" if os.path.exists(p) else "✗"
        src = "Pre" if d == PREDATA_DIR else "Pull"
        print(f"  {status} [{src}] {fn} → {sheet} (header={hdr})")

    try:
        # 维度同步
        sep("维度同步")
        sync_dim_advisor()
        sync_dim_target()
        sync_dim_contract_group()

        # 签约（所有模块合并后写入）
        sep("签约数据")
        all_sign = mod_A1() + mod_A2() + mod_A3() + mod_B1() + mod_B2() + mod_B3() + mod_C1() + mod_D()
        print(f"  合计签约记录: {len(all_sign)} 条")
        write_signing(all_sign)

        # 退费
        sep("退费数据")
        all_refund = mod_R1() + mod_R2() + mod_R3()
        print(f"  合计退费记录: {len(all_refund)} 条")
        write_refund(all_refund)

        # 快照
        sep("快照数据")
        write_receipt(snap_receipt())
        write_fund_snapshot(snap_fund() + snap_unrecognized())

        # 验证
        verify()
        elapsed = (datetime.now() - start).total_seconds()
        print(f"\n  ✅ 同步完成！耗时 {elapsed:.1f} 秒")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"\n  ❌ 同步失败: {e}")
        sys.exit(1)
