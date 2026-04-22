"""
migration_etl.py  ——  一次性历史数据迁移脚本
将所有旧版 Excel 文件批量清洗写入 PostgreSQL
"""
import os, sys, math, warnings
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

DATABASE_URL = os.getenv("DATABASE_URL_SYNC",
    "postgresql://qiantu:qiantu2026@localhost:5432/qiantu_finance")
BASE_DIR = os.getenv("DATA_DIR",
    r"D:\PythonCode\KiKiAutoPullDataScript\DailyReport\data")

FILES = {
    "staff":               os.path.join(BASE_DIR, "职员表.xlsx"),
    "sign_group":          os.path.join(BASE_DIR, "签约分组.xlsx"),
    "history_sign":        os.path.join(BASE_DIR, "历史数据.xlsx"),
    "history_school":      os.path.join(BASE_DIR, "历史数据-学校.xlsx"),
    "performance_monthly": os.path.join(BASE_DIR, "【月更】业绩表.xlsx"),
    "refund_daily":        os.path.join(BASE_DIR, "【日更】RefundAndTrans_Detail.xlsx"),
    "receipt":             os.path.join(BASE_DIR, "【日更】receipt_statistics_d.xlsx"),
    "sign_target":         os.path.join(BASE_DIR, "【月更】净签目标.xlsx"),
}

engine = create_engine(DATABASE_URL, echo=False)
stats = {}

SEP = "=" * 60

def sep(t): print(f"\n{SEP}\n  {t}\n{SEP}")

def safe_date(val):
    if pd.isna(val): return None
    try: return pd.to_datetime(val).date()
    except: return None

def combine_ymd(y, m, d):
    try: return pd.Timestamp(int(y), int(m), int(d)).date()
    except: return None

def clean_float(v):
    f = float(v or 0)
    return 0.0 if (math.isnan(f) or math.isinf(f)) else round(f, 2)

# ── 加载维度映射 ────────────────────────────────────────────────────────────
def load_dim_maps():
    group_map, staff_map = {}, {}
    gp = FILES["sign_group"]
    if os.path.exists(gp):
        dg = pd.read_excel(gp, sheet_name="Sheet1", header=0).dropna(subset=["合同号"])
        group_map = dict(zip(dg["合同号"].astype(str).str.strip(),
                             dg["分组部门"].astype(str).str.strip()))
    sp = FILES["staff"]
    if os.path.exists(sp):
        ds = pd.read_excel(sp, sheet_name="顾问部门", header=0)
        ds = ds.dropna(subset=["顾问"])
        ds["二级分组部门"] = ds["二级分组部门"].fillna("未知部门")
        staff_map = dict(zip(ds["顾问"].astype(str).str.strip(),
                             ds["二级分组部门"].astype(str).str.strip()))
    def get_group(cn, adv):
        cn = str(cn).strip()
        if cn in group_map: return group_map[cn]
        adv = str(adv).strip()
        if adv and adv != "nan": return staff_map.get(adv, "未知部门")
        return "未知部门"
    return get_group, staff_map

ASIA = {"日本","韩国","泰国","亚洲其他","中国香港","中国澳门","香港","澳门","新加坡","马来西亚"}

# ── 1. dim_advisor ──────────────────────────────────────────────────────────
def migrate_dim_advisor():
    sep("迁移 dim_advisor（职员表）")
    p = FILES["staff"]
    if not os.path.exists(p): print(f"  [跳过] {p}"); return
    df = pd.read_excel(p, sheet_name="顾问部门", header=0)
    recs = []
    for _, r in df.iterrows():
        name = str(r.get("顾问","") or "").strip()
        eid  = str(r.get("员工编号","") or "").strip()
        if not name or not eid: continue
        recs.append({
            "advisor_id": eid, "name": name,
            "email":           str(r.get("顾问邮箱","") or "").strip() or None,
            "primary_dept":    str(r.get("部门","") or "").strip() or None,
            "secondary_group": str(r.get("二级分组部门","") or "").strip() or None,
            "entry_date":      safe_date(r.get("入职时间")),
            "exit_date":       safe_date(r.get("离职时间")),
        })
    df_out = pd.DataFrame(recs).drop_duplicates(subset=["advisor_id"], keep="first")
    with engine.begin() as conn:
        for _, row in df_out.iterrows():
            conn.execute(text("""
                INSERT INTO dim_advisor (advisor_id,name,email,primary_dept,secondary_group,entry_date,exit_date)
                VALUES (:advisor_id,:name,:email,:primary_dept,:secondary_group,:entry_date,:exit_date)
                ON CONFLICT (advisor_id) DO UPDATE SET
                  name=EXCLUDED.name, email=COALESCE(EXCLUDED.email,dim_advisor.email),
                  primary_dept=EXCLUDED.primary_dept, secondary_group=EXCLUDED.secondary_group,
                  entry_date=EXCLUDED.entry_date, exit_date=EXCLUDED.exit_date
            """), row.to_dict())
    stats["dim_advisor"] = len(df_out)
    print(f"  ✓ {len(df_out)} 名顾问")

# ── 2. dim_monthly_target ───────────────────────────────────────────────────
def migrate_dim_monthly_target():
    sep("迁移 dim_monthly_target（净签目标）")
    p = FILES["sign_target"]
    if not os.path.exists(p): print(f"  [跳过] {p}"); return
    df = pd.read_excel(p, sheet_name="Sheet1", header=0)
    df["所属月份"] = pd.to_datetime(df["所属月份"], errors="coerce")
    df["超额目标（万）"] = pd.to_numeric(df["超额目标（万）"], errors="coerce").fillna(0)
    df = df.dropna(subset=["所属月份"])
    recs = []
    for _, r in df.iterrows():
        dept = str(r.get("部门","") or "").strip()
        if not dept: continue
        recs.append({
            "year_month":      r["所属月份"].strftime("%Y-%m"),
            "department":      dept,
            "secondary_group": str(r.get("二级分组部门","全部") or "全部").strip() or "全部",
            "target_amount":   float(r["超额目标（万）"]),
        })
    with engine.begin() as conn:
        for rec in recs:
            conn.execute(text("""
                INSERT INTO dim_monthly_target (year_month,department,secondary_group,target_amount)
                VALUES (:year_month,:department,:secondary_group,:target_amount)
                ON CONFLICT (year_month,secondary_group) DO UPDATE SET
                  target_amount=EXCLUDED.target_amount, department=EXCLUDED.department
            """), rec)
    stats["dim_monthly_target"] = len(recs)
    print(f"  ✓ {len(recs)} 条目标")

# ── 3. dim_contract_group ───────────────────────────────────────────────────
def migrate_dim_contract_group():
    sep("迁移 dim_contract_group（签约分组映射）")
    p = FILES["sign_group"]
    if not os.path.exists(p): print(f"  [跳过] {p}"); return
    df = pd.read_excel(p, sheet_name="Sheet1", header=0).dropna(subset=["合同号","分组部门"])
    df["合同号"] = df["合同号"].astype(str).str.strip()
    df = df.drop_duplicates(subset=["合同号"], keep="first")
    with engine.begin() as conn:
        for _, r in df.iterrows():
            conn.execute(text("""
                INSERT INTO dim_contract_group (contract_no,group_dept)
                VALUES (:cn,:gd) ON CONFLICT (contract_no) DO UPDATE SET group_dept=EXCLUDED.group_dept
            """), {"cn": r["合同号"], "gd": str(r["分组部门"]).strip()})
    stats["dim_contract_group"] = len(df)
    print(f"  ✓ {len(df)} 条映射")

# ── 4. fact_signing ─────────────────────────────────────────────────────────
def migrate_fact_signing():
    sep("迁移 fact_signing（全量签约数据）")
    get_group, _ = load_dim_maps()
    records = []

    # A3: 历史数据签约明细
    p = FILES["history_sign"]
    if os.path.exists(p):
        df = pd.read_excel(p, sheet_name="签约明细", header=0)
        cnt = 0
        for _, r in df.iterrows():
            dt = safe_date(r.get("日期"))
            cn = str(r.get("合同号","") or "").strip()
            if not dt or not cn: continue
            adv = str(r.get("签约顾问","") or "").strip()
            is_lang = str(r.get("语言培训","否")).strip() == "是"
            records.append({"contract_no":cn,"sign_date":dt,"advisor_name":adv if adv!="nan" else "",
                "original_dept":str(r.get("部门","") or "").strip(),"line":str(r.get("条线","") or "").strip(),
                "sub_line":"","secondary_group":get_group(cn,adv),
                "sign_biz_type":"多语" if is_lang else "留学","school":"ERP",
                "gross_sign":clean_float(r.get("签约金额",0)),"source_system":"历史"})
            cnt += 1
        print(f"  A3 历史签约明细: {cnt} 条")

    # B3: 历史学校按天
    p = FILES["history_school"]
    if os.path.exists(p):
        df = pd.read_excel(p, sheet_name="签约金额按天", header=0)
        cnt = 0
        for i,(_, r) in enumerate(df.iterrows()):
            dt = combine_ymd(r.get("年份"), r.get("月份"), r.get("日"))
            if not dt: continue
            cn = str(r.get("班级编号","") or "").strip() or f"SCH_HIST_{i}"
            mgmt = str(r.get("管理部门名称","") or "")
            school = "前途出国" if "前途出国" in mgmt else ("迅程" if "出国考试" in mgmt else "广州前途")
            country = str(r.get("国家","") or "").strip()
            records.append({"contract_no":cn,"sign_date":dt,"advisor_name":"",
                "original_dept":"广州前途欧亚事业部","line":"亚洲" if country in ASIA else "欧洲",
                "sub_line":"","secondary_group":get_group(cn,""),
                "sign_biz_type":"多语","school":school,
                "gross_sign":clean_float(r.get("当日预收款总计",0)),"source_system":"历史"})
            cnt += 1
        print(f"  B3 历史学校: {cnt} 条")

    # A2: 月更业绩表附件1
    p = FILES["performance_monthly"]
    if os.path.exists(p):
        try:
            df = pd.read_excel(p, sheet_name="附件1签约明细", header=1)
            cnt = 0
            for _, r in df.iterrows():
                dt = safe_date(r.get("日期"))
                cn = str(r.get("合同号","") or "").strip()
                if not dt or not cn: continue
                adv = str(r.get("签约顾问","") or "").strip()
                is_lang = str(r.get("语言培训","否")).strip() == "是"
                records.append({"contract_no":cn,"sign_date":dt,"advisor_name":adv if adv not in ("nan","") else "",
                    "original_dept":str(r.get("部门","") or "").strip(),"line":str(r.get("条线","") or "").strip(),
                    "sub_line":"","secondary_group":get_group(cn,adv),
                    "sign_biz_type":"多语" if is_lang else "留学","school":"ERP",
                    "gross_sign":clean_float(r.get("签约金额",0)),"source_system":"月更"})
                cnt += 1
            print(f"  A2 月更附件1: {cnt} 条")
        except Exception as e:
            print(f"  [警告] 月更附件1: {e}")

    if not records: return
    inserted = 0
    with engine.begin() as conn:
        # 迁移脚本：先清空再全量写入，定版数据全量保留
        conn.execute(text("DELETE FROM fact_signing"))
        for rec in records:
            conn.execute(text("""
                INSERT INTO fact_signing
                  (contract_no,sign_date,advisor_name,original_dept,line,sub_line,
                   secondary_group,sign_biz_type,school,gross_sign,source_system)
                VALUES (:contract_no,:sign_date,:advisor_name,:original_dept,:line,:sub_line,
                        :secondary_group,:sign_biz_type,:school,:gross_sign,:source_system)
            """), rec)
            inserted += 1
    stats["fact_signing"] = inserted
    print(f"  ✓ 写入 {inserted} 条")

# ── 5. fact_refund ──────────────────────────────────────────────────────────
def migrate_fact_refund():
    sep("迁移 fact_refund（退费数据）")
    get_group, _ = load_dim_maps()
    records = []

    def parse_row(r, date_field, source, idx=0):
        dt = safe_date(r.get(date_field))
        if not dt: return None
        cn = str(r.get("合同号","") or "").strip()
        adv = (str(r.get("签约顾问","") or "").strip()
               or str(r.get("退费顾问","") or "").strip())
        rid = str(r.get("退费协议编号","") or "").strip()
        if not rid: rid = f"{cn}_TF_{dt}_{idx}"
        is_lang = str(r.get("语言培训","否")).strip() == "是"
        return {"refund_id":rid,"refund_date":dt,"contract_no":cn,
                "advisor_name":adv if adv not in ("nan","") else "",
                "original_dept":str(r.get("部门","") or "").strip(),
                "line":str(r.get("业务条线","") or r.get("条线","") or "").strip(),
                "sub_line":"","secondary_group":get_group(cn,adv),
                "refund_biz_type":"多语" if is_lang else "留学",
                "gross_refund":clean_float(r.get("退费总金额",0)),"source_system":source}

    p = FILES["history_sign"]
    if os.path.exists(p):
        try:
            df = pd.read_excel(p, sheet_name="退费", header=0)
            for i,(_, r) in enumerate(df.iterrows()):
                rec = parse_row(r, "日期", "历史", i)
                if rec: records.append(rec)
            print(f"  R3 历史退费: {len(records)} 条")
        except Exception as e: print(f"  [警告] {e}")

    p = FILES["refund_daily"]
    if os.path.exists(p):
        try:
            df = pd.read_excel(p, sheet_name="sheet1", header=0)
            before = len(records)
            for i,(_, r) in enumerate(df.iterrows()):
                rec = parse_row(r, "日期", "日更", i)
                if rec: records.append(rec)
            print(f"  R1 日更退费: {len(records)-before} 条")
        except Exception as e: print(f"  [警告] {e}")

    if not records: return
    inserted = 0
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM fact_refund"))
        for rec in records:
            conn.execute(text("""
                INSERT INTO fact_refund
                  (refund_id,refund_date,contract_no,advisor_name,original_dept,
                   line,sub_line,secondary_group,refund_biz_type,gross_refund,source_system)
                VALUES (:refund_id,:refund_date,:contract_no,:advisor_name,:original_dept,
                        :line,:sub_line,:secondary_group,:refund_biz_type,:gross_refund,:source_system)
            """), rec)
            inserted += 1
    stats["fact_refund"] = inserted
    print(f"  ✓ 写入 {inserted} 条")

# ── 6. fact_receipt ─────────────────────────────────────────────────────────
def migrate_fact_receipt():
    sep("迁移 fact_receipt（收款统计）")
    p = FILES["receipt"]
    if not os.path.exists(p): print(f"  [跳过] {p}"); return
    df = pd.read_excel(p, sheet_name="sheet1", header=0)
    df = df[df["款项类别"] == "留学服务费"].copy()
    inserted = skipped = 0
    with engine.begin() as conn:
        for _, r in df.iterrows():
            dt = safe_date(r.get("收款日期"))
            rno = str(r.get("收据号","") or "").strip()
            if not dt or not rno: continue
            rec = {"receipt_no":rno,"receipt_date":dt,"arrived_date":safe_date(r.get("到账日期")),
                   "contract_no":str(r.get("合同号","") or "").strip(),
                   "advisor_name":str(r.get("签约顾问","") or "").strip(),
                   "dept":str(r.get("部门","") or "").strip(),
                   "pay_method":str(r.get("收款方式","") or "").strip(),
                   "status":str(r.get("状态","") or "").strip(),
                   "sign_biz_type":"留学",
                   "amount":clean_float(r.get("收款金额",0))}
            result = conn.execute(text("""
                INSERT INTO fact_receipt (receipt_no,receipt_date,arrived_date,contract_no,
                  advisor_name,dept,pay_method,status,sign_biz_type,amount)
                VALUES (:receipt_no,:receipt_date,:arrived_date,:contract_no,
                        :advisor_name,:dept,:pay_method,:status,:sign_biz_type,:amount)
                ON CONFLICT (receipt_no) DO NOTHING
            """), rec)
            if result.rowcount == 0: skipped += 1
            else: inserted += 1
    stats["fact_receipt"] = inserted
    print(f"  ✓ 写入 {inserted} 条，跳过 {skipped} 条")

# ── 7. 验证报告 ─────────────────────────────────────────────────────────────
def verification_report():
    sep("迁移验证报告")
    tables = ["dim_advisor","dim_monthly_target","dim_contract_group",
              "fact_signing","fact_refund","fact_receipt"]
    with engine.connect() as conn:
        for t in tables:
            try:
                n = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
                print(f"  {t:<35} {n:>10,} 行")
            except Exception as e:
                print(f"  {t:<35} 查询失败: {e}")
    print("\n  脚本写入统计：")
    for k,v in stats.items():
        print(f"    {k:<35} {v:>10,} 条")
    print(f"\n  ✅ 迁移完成！请将旧版 Excel 归档至 /archive/pre-migration/{datetime.now():%Y%m%d}/")

if __name__ == "__main__":
    print(f"广州前途财务日报 · ETL 迁移脚本")
    print(f"执行时间: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"数据目录: {BASE_DIR}\n")
    for name, path in FILES.items():
        status = "✓" if os.path.exists(path) else "✗ 缺失"
        print(f"  {status}  {name}: {os.path.basename(path)}")
    try:
        migrate_dim_advisor()
        migrate_dim_monthly_target()
        migrate_dim_contract_group()
        migrate_fact_signing()
        migrate_fact_refund()
        migrate_fact_receipt()
        verification_report()
    except Exception as e:
        import traceback; traceback.print_exc(); sys.exit(1)
