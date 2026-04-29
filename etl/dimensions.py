"""
dimensions.py — 维度域（加载 + 同步）
========================================================
本文件包含两类操作（同一域、不同方向）：
  · 加载（read 侧）：把维度数据读到内存供 extract 阶段做 lookup
        load_staff_map / load_sign_group / load_history_group /
        load_subline_map / load_group_to_primary
        get_group / get_group_advisor / get_actual_advisor / get_subline
  · 同步（write 侧）：把维度数据写入 dim_* 表
        sync_dim_advisor / sync_dim_target /
        sync_dim_contract_group / sync_dim_group_dept

注意：sync_dim_target 依赖 dim_group_dept 已写入，调用顺序由主入口保证。
"""
from sqlalchemy import text

from config import get_engine, stats
from utils import (
    cs, cs_or_none, safe_date, read_excel,
    normalize_biz_type, _fix_excel_serial,
)
import pandas as pd


# ═══════════════════════════════════════════════════════════════
#  维度加载（第六章）
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
    """签约分组：合同号 → 分组部门（系统口径）+ 分组部门（顾问口径）+ 实际签约顾问"""
    if "sign_group" in _dim_cache: return _dim_cache["sign_group"]
    m_sys, m_adv, m_actual = {}, {}, {}
    df = read_excel("sign_group")
    if df is not None:
        for _, r in df.dropna(subset=["合同号"]).iterrows():
            cn = cs(r["合同号"])
            m_sys[cn] = cs(r["分组部门"])
            adv_dept = cs(r.get("分组部门（顾问口径）")) or cs(r.get("分组部门(顾问口径)"))
            if adv_dept:
                m_adv[cn] = adv_dept
            actual = cs(r.get("实际签约顾问"))
            if actual:
                m_actual[cn] = actual
    _dim_cache["sign_group"] = (m_sys, m_adv, m_actual)
    return m_sys, m_adv, m_actual


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


def load_group_to_primary():
    """[v6 新增] 从 dim_group_dept 加载 secondary_group → primary_group 映射

    用于 dim_monthly_target 同步时反查 department 字段
    （源 Excel 的"部门"列已废弃，改为从此表自动推导）

    Returns:
        dict: {secondary_group_name: primary_group_name}
    """
    if "group_to_primary" in _dim_cache:
        return _dim_cache["group_to_primary"]
    m = {}
    try:
        with get_engine().connect() as conn:
            rows = conn.execute(text(
                "SELECT secondary_group, primary_group FROM dim_group_dept"
            )).all()
            m = {r[0]: (r[1] or "") for r in rows if r[0]}
    except Exception as e:
        print(f"  [警告] 读取 dim_group_dept 失败，department 字段将留空: {e}")
    _dim_cache["group_to_primary"] = m
    return m


def get_group(contract_no: str, advisor: str) -> str:
    """三级优先查找分组部门-系统口径（第六章）"""
    cn = cs(contract_no)
    sg_sys, _, _ = load_sign_group()
    if cn in sg_sys: return sg_sys[cn]
    hg = load_history_group()
    if cn in hg: return hg[cn]
    adv = cs(advisor)
    if adv:
        nm, _ = load_staff_map()
        if adv in nm: return nm[adv]
    return "未知部门"


def get_group_advisor(contract_no: str, advisor: str) -> str:
    """查找分组部门-顾问口径：签约分组优先，回退到系统口径"""
    cn = cs(contract_no)
    _, sg_adv, _ = load_sign_group()
    if cn in sg_adv: return sg_adv[cn]
    return get_group(contract_no, advisor)


def get_actual_advisor(contract_no: str, fallback_advisor: str) -> str:
    """查找实际签约顾问：签约分组优先，回退到系统顾问"""
    cn = cs(contract_no)
    _, _, m_actual = load_sign_group()
    if cn in m_actual: return m_actual[cn]
    return cs(fallback_advisor)


def get_subline(contract_no: str) -> str:
    """查 Sign_Details 获取二级条线名称（第五章 §5.3）"""
    return load_subline_map().get(cs(contract_no), "")


# ═══════════════════════════════════════════════════════════════
#  [v7 新增] 欧亚事业部签约表 → 多语签约 advisor / secondary_group 维度
#  用户反馈 2026-04-29：
#    多语签约记录(B1/B2/B3/B4/D) 缺签约顾问列, 须从
#    《26财年欧亚事业部签约表.xlsx 收入人次 sheet》登记，规则：
#      签约顾问 = 学管 if 学管!=空 else 顾问
#      secondary_group = 分组列 ('#N/A' 视为空)
#      索引键 = 班级编码 / 听课证号 / 听课证号.1 / 合同编号
#               学校='出国考试' 时额外索引 班级编码+学员编码
#      校验：分组='#N/A' 全部记录 现金收入_人民币 合计应为 0（一进一出）
# ═══════════════════════════════════════════════════════════════
def load_eurasia_signing_map():
    """加载欧亚事业部签约表 → (多键索引 dict, '#N/A' 异常记录列表)

    索引值：(签约顾问, secondary_group)
    pandas 默认会把 Excel 错误值 '#N/A' 转为 NaN，故此处用 keep_default_na=False
    + na_values=[] 重读以保留原始文本。
    """
    if "eurasia_sign" in _dim_cache:
        return _dim_cache["eurasia_sign"]

    key_to_av = {}      # multikey → (advisor, secondary_group)
    na_records = []     # 分组='#N/A' 的所有记录（用于一进一出校验）

    from utils import get_file, cf
    from config import FILES
    p = get_file("eurasia_signing")
    if not p:
        print("  [警告] 26财年欧亚事业部签约表.xlsx 未找到，跳过欧亚维度加载")
        _dim_cache["eurasia_sign"] = (key_to_av, na_records)
        return key_to_av, na_records

    _, _, sheet, header = FILES["eurasia_signing"]
    try:
        df = pd.read_excel(p, sheet_name=sheet, header=header,
                           keep_default_na=False, na_values=[])
    except Exception as e:
        print(f"  [警告] 读取 eurasia_signing 失败: {e}")
        _dim_cache["eurasia_sign"] = (key_to_av, na_records)
        return key_to_av, na_records

    na_amount_sum = 0.0
    for i, (_, r) in enumerate(df.iterrows()):
        # 1) 签约顾问：学管优先，否则顾问
        adv = cs(r.get("学管")) or cs(r.get("顾问"))
        # 2) 二级分组：分组列；'#N/A' 文本视为空（同时记录用于校验）
        grp_raw = cs(r.get("分组"))
        is_na = (grp_raw == "#N/A")
        grp = "" if is_na else grp_raw

        # #N/A 一进一出校验：累加现金收入_人民币
        if is_na:
            cash_rmb = cf(r.get("现金收入_人民币"))
            na_amount_sum += cash_rmb
            na_records.append({
                "row": i + 2,  # +2: header=1 + 0-based offset
                "学校": cs(r.get("学校")),
                "班级编码": cs(r.get("班级编码")),
                "听课证号": cs(r.get("听课证号")),
                "合同编号": cs(r.get("合同编号")),
                "现金收入_人民币": cash_rmb,
            })

        if not (adv or grp):
            continue
        value = (adv, grp)

        # 3) 多键索引：班级编码 / 听课证号 / 听课证号.1 / 合同编号
        for col in ("班级编码", "听课证号", "听课证号.1", "合同编号"):
            k = cs(r.get(col))
            if k and k not in key_to_av:
                key_to_av[k] = value

        # 4) school='出国考试'：班级编码+学员编码 复合键
        school = cs(r.get("学校"))
        if school == "出国考试":
            bj = cs(r.get("班级编码"))
            stu = cs(r.get("学员编码"))
            if bj and stu:
                k = f"{bj}|{stu}"
                if k not in key_to_av:
                    key_to_av[k] = value

    # 一进一出校验告警
    if na_records:
        eps = 0.01
        if abs(na_amount_sum) > eps:
            print(f"  [告警] 收入人次 sheet 中 分组='#N/A' 共 {len(na_records)} 条，"
                  f"现金收入_人民币合计 = {na_amount_sum:.2f} ≠ 0（应一进一出, 请人工核对）")
            for rec in na_records[:5]:
                print(f"    第 {rec['row']:>4} 行 学校={rec['学校']:<6} "
                      f"班级编码={rec['班级编码']:<24} 听课证号={rec['听课证号']:<24} "
                      f"合同编号={rec['合同编号']:<20} 金额={rec['现金收入_人民币']:>12.2f}")
            if len(na_records) > 5:
                print(f"    ... 余 {len(na_records)-5} 条略")
        else:
            print(f"  [信息] 收入人次 sheet 中 分组='#N/A' 共 {len(na_records)} 条，"
                  f"合计=0（一进一出 OK）")

    _dim_cache["eurasia_sign"] = (key_to_av, na_records)
    print(f"  ✓ 欧亚事业部签约表索引：{len(key_to_av)} 个键 / #N/A 行 {len(na_records)} 条")
    return key_to_av, na_records


def get_eurasia_advisor_group(*candidate_keys):
    """[v7 新增] 按候选键序贯查欧亚事业部签约映射 → (advisor, secondary_group)

    任一候选键命中即返回；全部未命中返回 ('','')。空串/None 自动跳过。
    候选键传入顺序即优先级（建议：复合键 > 单键）。
    """
    m, _ = load_eurasia_signing_map()
    for k in candidate_keys:
        k = cs(k)
        if k and k in m:
            return m[k]
    return ("", "")


# ═══════════════════════════════════════════════════════════════
#  维度同步（写入 dim_* 表）
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


def sync_dim_target():
    """[v6 重写] 同步月度目标

    数据流:
        【月更】净签目标.xlsx（列: 所属月份/二级分组部门/留学/培训/超额目标）
         → normalize_biz_type 归一化业务类型
         → dim_group_dept 反查 primary_group 填充 department
         → TRUNCATE dim_monthly_target → 全量 INSERT

    v6 重要变更:
      1. 不再读 Excel '部门' 列（该列已废弃）。department 字段由 ETL 从
         dim_group_dept 用 secondary_group 反查 primary_group 自动填充。
      2. biz_type 映射走全局 normalize_biz_type，支持 '语培'/'培训'/'多语'
         → '多语'，未知值打警告。
      3. 改为 TRUNCATE + 全量 INSERT，彻底清除可能存在的历史错分类脏数据。
         （原 ON CONFLICT UPDATE 策略无法清除已存在但分类错误的旧记录）
    """
    df = read_excel("sign_target")
    if df is None:
        print("  ✗ 【月更】净签目标.xlsx 未找到，跳过")
        return

    # 预处理：统一日期格式 + 数值转换
    df["所属月份"] = df["所属月份"].apply(_fix_excel_serial)
    df["所属月份"] = pd.to_datetime(df["所属月份"], errors="coerce")
    # 兼容中英括号写法
    df["超额目标（万）"] = pd.to_numeric(
        df.get("超额目标（万）", df.get("超额目标(万)")),
        errors="coerce"
    ).fillna(0)

    # 加载 secondary_group → primary_group 映射，用于填充 department
    group_to_primary = load_group_to_primary()

    recs = []
    missing_group_warned = set()
    for _, r in df.dropna(subset=["所属月份"]).iterrows():
        sec_group = cs(r.get("二级分组部门"), "全部") or "全部"
        # 兼容欄位名（根據原始表頭）：'留学/培训' 為首選，兜底 '业务类型'
        raw_bt = r.get("留学/培训")
        if raw_bt is None or (isinstance(raw_bt, float) and pd.isna(raw_bt)):
            raw_bt = r.get("业务类型")

        # department 字段：从 dim_group_dept 反查，反查不到给空字符串
        # （schema 的 NOT NULL 约束允许空串，只不允许 NULL）
        department = group_to_primary.get(sec_group, "")
        if not department and sec_group not in missing_group_warned and sec_group != "全部":
            print(f"  [提示] secondary_group='{sec_group}' 在 dim_group_dept 中无对应 primary_group，department 留空")
            missing_group_warned.add(sec_group)

        recs.append({
            "year_month":      r["所属月份"].strftime("%Y-%m"),
            "department":      department,
            "secondary_group": sec_group,
            "sign_biz_type":   normalize_biz_type(raw_bt),
            "target_amount":   float(r["超额目标（万）"]),
        })

    # v6: 整表 TRUNCATE 后全量重灌，彻底清除历史脏数据
    # 比 ON CONFLICT UPDATE 更安全，避免"错分类的旧记录不会被自动清除"的问题
    with get_engine().begin() as conn:
        conn.execute(text("TRUNCATE TABLE dim_monthly_target RESTART IDENTITY"))
        for rec in recs:
            conn.execute(text("""
                INSERT INTO dim_monthly_target
                  (year_month, department, secondary_group, sign_biz_type, target_amount)
                VALUES
                  (:year_month, :department, :secondary_group, :sign_biz_type, :target_amount)
            """), rec)

    stats["dim_monthly_target"] = len(recs)
    # 打印分类统计便于验证
    by_type = {}
    for r in recs:
        by_type[r["sign_biz_type"]] = by_type.get(r["sign_biz_type"], 0) + 1
    type_summary = ", ".join(f"{k}={v}" for k, v in sorted(by_type.items()))
    print(f"  ✓ {len(recs)} 条目标（全量重灌 · 含 sign_biz_type 维度 · {type_summary}）")


def sync_dim_contract_group():
    """同步合同分组（双口径：系统口径 + 顾问口径）"""
    df = read_excel("sign_group")
    if df is None: return
    df = df.dropna(subset=["合同号"]).drop_duplicates(subset=["合同号"])
    with get_engine().begin() as conn:
        for i, (_, r) in enumerate(df.iterrows()):
            # 兼容列名写法（中英括号）
            gda = cs(r.get("分组部门（顾问口径）", "")) or cs(r.get("分组部门(顾问口径)", ""))
            conn.execute(text("""
                INSERT INTO dim_contract_group (contract_no,group_dept,actual_advisor,group_dept_advisor)
                VALUES (:cn,:gd,:aa,:gda)
                ON CONFLICT (contract_no) DO UPDATE SET
                  group_dept=EXCLUDED.group_dept,
                  actual_advisor=EXCLUDED.actual_advisor,
                  group_dept_advisor=EXCLUDED.group_dept_advisor,
                  updated_at=NOW()
            """), {
                "cn": cs(r["合同号"]),
                "gd": cs(r.get("分组部门", "")),
                "aa": cs(r.get("实际签约顾问", "")),
                "gda": gda,
            })
    stats["dim_contract_group"] = len(df)
    print(f"  ✓ {len(df)} 条映射（含顾问口径）")


def sync_dim_group_dept():
    """同步分组部门维度表（参数-分组部门.xlsx）"""
    df = read_excel("group_dept_param")
    if df is None:
        print("  ✗ 参数-分组部门.xlsx 未找到")
        return
    with get_engine().begin() as conn:
        conn.execute(text("DELETE FROM dim_group_dept"))
        for _, r in df.iterrows():
            sg = cs(r.get("二级分组部门"))
            if not sg: continue
            conn.execute(text("""
                INSERT INTO dim_group_dept (secondary_group,secondary_group_tidy,primary_group,biz_block)
                VALUES (:sg,:sgt,:pg,:bb)
                ON CONFLICT (secondary_group) DO UPDATE SET
                  secondary_group_tidy=EXCLUDED.secondary_group_tidy,
                  primary_group=EXCLUDED.primary_group,
                  biz_block=EXCLUDED.biz_block
            """), {
                "sg": sg,
                "sgt": cs(r.get("二级分组部门（整理）", "")) or cs(r.get("二级分组部门(整理)", "")),
                "pg": cs(r.get("一级分组部门", "")),
                "bb": cs(r.get("业务板块", "")),
            })
    stats["dim_group_dept"] = len(df)
    print(f"  ✓ {len(df)} 条分组部门层级")
