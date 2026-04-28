"""
snapshot.py — 快照域（第二章 §2.2，不做时间分层）
========================================================
extract:
  · snap_receipt           — 收款（款项类别=留学服务费），同时收集作废黑名单
  · snap_fund              — 已收款未盖章 + 潜在签约（contractDetail）
  · snap_unrecognized      — 未认款（未认款明细）
  · delete_voided_receipts — 物理删除作废收据
load:
  · write_receipt          — 收款 UPSERT（已过滤作废）
  · write_fund_snapshot    — 资金快照（当日全量替换）
"""
from sqlalchemy import text

from config import get_engine, stats, TODAY
from utils import cs, cf, safe_date, read_excel
from time_boundary import FY_START
from dimensions import load_staff_map, get_group, get_actual_advisor


def snap_receipt():
    """收款统计：款项类别=留学服务费

    清洗规则:
      1. 跳过 contract_no 含 '-MHT-' 的记录（业务废弃/测试合同）
      2. [v6 新增] 跳过 status='作废' 的记录，不写入库
         同时收集作废记录的 receipt_no，返回供主流程从 DB 物理删除
         （处理"源表里原本正常的收据被改为作废"的场景 —
          只靠写入过滤不够，必须主动 DELETE）
      3. actual_advisor 优先用 dim_contract_group.actual_advisor，
         回退 advisor_name — 与 fact_signing / fact_refund 口径保持一致

    Returns:
        tuple[list[dict], list[str]]: (可写入的收据记录列表, 作废的收据号列表)
    """
    df = read_excel("receipt")
    if df is None: return [], []
    if "款项类别" in df.columns:
        df = df[df["款项类别"] == "留学服务费"].copy()
    recs = []
    voided_nos = []       # v6: 作废记录的 receipt_no 黑名单，供后续 DELETE
    skip_mht = 0
    skip_voided = 0
    for i, (_, r) in enumerate(df.iterrows()):
        dt = safe_date(r.get("收款日期"))
        rno = cs(r.get("收据号"))
        if not dt or not rno: continue
        cn = cs(r.get("合同号"))
        if "-MHT-" in cn:
            skip_mht += 1
            continue
        status = cs(r.get("状态"))
        # v6: 作废记录不写入，同时加入删除黑名单
        if status == "作废":
            voided_nos.append(rno)
            skip_voided += 1
            continue
        advisor = cs(r.get("签约顾问"))
        recs.append({
            "receipt_no": rno, "receipt_date": dt,
            "arrived_date": safe_date(r.get("到账日期")),
            "contract_no": cn,
            "advisor_name": advisor,
            "actual_advisor": get_actual_advisor(cn, advisor),
            "dept": cs(r.get("部门")),
            "pay_method": cs(r.get("收款方式")),
            "status": status,
            "sign_biz_type": "留学",  # filter 已保證 款项类别=='留学服务费'
            "amount": cf(r.get("收款金额", 0)),
        })
    print(f"  收款统计: {len(recs)} 条（跳过 -MHT- {skip_mht} 条, 作废 {skip_voided} 条）")
    return recs, voided_nos


def delete_voided_receipts(voided_nos):
    """[v6 新增] 物理删除 Excel 中标记为作废的收据记录

    处理场景：一条收据之前入库时状态是"已认款"等正常值，后来在源系统
    被改为"作废"。如果只在写入时过滤，DB 里的老记录会残留。
    此函数每次 ETL 运行都把当前 Excel 中所有作废号从 DB 删掉。

    Args:
        voided_nos: 作废的 receipt_no 列表（来自 snap_receipt 返回值）
    """
    if not voided_nos:
        print("  ✓ 作废收据: 源表无作废记录，无需删除")
        return
    BATCH = 500
    total_deleted = 0
    with get_engine().begin() as conn:
        for i in range(0, len(voided_nos), BATCH):
            batch = voided_nos[i:i+BATCH]
            r = conn.execute(text(
                "DELETE FROM fact_receipt WHERE receipt_no = ANY(:nos)"
            ), {"nos": batch})
            total_deleted += r.rowcount or 0
    stats["fact_receipt_voided_deleted"] = total_deleted
    print(f"  ✓ 作废收据物理删除: 源表标记 {len(voided_nos)} 条, DB 实删 {total_deleted} 条")


def snap_fund():
    """已收款未盖章 + 潜在签约 (contractDetail, §2.2)
    新增 actual_advisor: 与 fact_receipt / fact_signing 口径统一，
    供前端「百万顾问榜」跨表 JOIN 使用。
    """
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
            "advisor_name": advisor,
            "actual_advisor": get_actual_advisor(cn, advisor),
            "dept": dept,
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
        # 未认款场景下没有真实 contract_no（是生成的 URK_ 前缀），
        # 所以 actual_advisor 直接用原顾问字段
        recs.append({
            "snapshot_date": TODAY, "contract_no": f"URK_{i}_{cs(r.get('汇款附言',''))[:20]}",
            "advisor_name": advisor,
            "actual_advisor": advisor,
            "dept": dept or "未知部门",
            "secondary_group": dept or "未知部门",
            "metric_type": "未认款", "amount": amount,
            "contract_status": cs(r.get("认款状态")),
        })
    print(f"  未认款快照: {len(recs)} 条")
    return recs


def write_receipt(records):
    """写入收款事实表（ON CONFLICT 时更新为最新数据，不再跳过）

    注意: 作废记录在 snap_receipt 阶段已被过滤掉，这里只处理正常记录。
    配合 delete_voided_receipts() 保证 DB 不残留作废数据。
    """
    if not records: return
    ins = 0
    with get_engine().begin() as conn:
        for rec in records:
            conn.execute(text("""
                INSERT INTO fact_receipt
                  (receipt_no,receipt_date,arrived_date,contract_no,
                   advisor_name,actual_advisor,dept,pay_method,status,sign_biz_type,amount)
                VALUES (:receipt_no,:receipt_date,:arrived_date,:contract_no,
                        :advisor_name,:actual_advisor,:dept,:pay_method,:status,:sign_biz_type,:amount)
                ON CONFLICT (receipt_no) DO UPDATE SET
                  receipt_date    = EXCLUDED.receipt_date,
                  arrived_date    = EXCLUDED.arrived_date,
                  contract_no     = EXCLUDED.contract_no,
                  advisor_name    = EXCLUDED.advisor_name,
                  actual_advisor  = EXCLUDED.actual_advisor,
                  dept            = EXCLUDED.dept,
                  pay_method      = EXCLUDED.pay_method,
                  status          = EXCLUDED.status,
                  sign_biz_type   = EXCLUDED.sign_biz_type,
                  amount          = EXCLUDED.amount,
                  updated_at      = NOW()
            """), rec)
            ins += 1
    stats["fact_receipt"] = ins
    print(f"  ✓ 收款写入/更新 {ins} 条（已过滤作废）")


def write_fund_snapshot(records):
    """写入资金快照（当日全量替换）"""
    if not records: return
    with get_engine().begin() as conn:
        # 先删除当日快照
        conn.execute(text("DELETE FROM fact_fund_snapshot WHERE snapshot_date = :d"), {"d": TODAY})
        for rec in records:
            clean = {k: v for k, v in rec.items() if k != "student_name"}
            # 兼容性：老 snap_* 可能没送 actual_advisor，此处兜底
            clean.setdefault("actual_advisor", clean.get("advisor_name", ""))
            conn.execute(text("""
                INSERT INTO fact_fund_snapshot
                  (snapshot_date,contract_no,advisor_name,actual_advisor,dept,secondary_group,
                   metric_type,amount,contract_status)
                VALUES (:snapshot_date,:contract_no,:advisor_name,:actual_advisor,:dept,:secondary_group,
                        :metric_type,:amount,:contract_status)
            """), clean)
    stats["fact_fund_snapshot"] = len(records)
    print(f"  ✓ 资金快照写入 {len(records)} 条")
