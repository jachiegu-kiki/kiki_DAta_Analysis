"""
daily_sync.py v6 — 完整 ETL（重构入口）
========================================================
本版修复（v6, 2026-04-23）:
  [Fix-1] snap_receipt 跳过 status='作废' 记录 + 收集黑名单
          新增 delete_voided_receipts() 从 DB 物理删除作废收据
          （处理源表把正常收据改为作废的场景）
  [Fix-2] sync_dim_target 全面重写：
          - normalize_biz_type 增加 '语培' 识别（源表实际取值）
          - 去掉对 Excel '部门' 列的强依赖（该列已废弃）
          - department 字段改由 dim_group_dept 反查 secondary_group → primary_group 填充
          - 未知 biz_type 值不静默归'留学'，打警告日志
          - 整表 TRUNCATE 后全量重灌，避免历史错分类脏数据残留

架构（拆分为多模块，逻辑不变）:
  config.py          配置 / FILES 注册表 / engine / stats
  utils.py           通用工具（清洗、Excel 读取、biz_type 归一化）
  time_boundary.py   时间边界（FY_START / DAILY_START / layer_of）
  dimensions.py      维度域（加载 + 同步）
  signing.py         签约域（A1~A3 / B1~B3 / C1 / D + 写入）
  refund.py          退费域（R1~R3 + 写入）
  snapshot.py        快照域（收款 / 已收款未盖章 / 潜在签约 / 未认款 + 写入）
  daily_sync.py      主入口（编排 + 验证）

环境变量：
  DATABASE_URL_SYNC   PostgreSQL 连接串
  PREDATA_DIR         PreData 目录
  PULLDATA_DIR        PullData 目录
"""
import os
import sys
from datetime import datetime
from sqlalchemy import text

from config import (
    PREDATA_DIR, PULLDATA_DIR, TODAY, FILES,
    get_engine, stats,
)
from utils import sep
from time_boundary import FY_START, DAILY_START
from dimensions import (
    sync_dim_advisor, sync_dim_target,
    sync_dim_contract_group, sync_dim_group_dept,
)
from signing import (
    mod_A1, mod_A2, mod_A3,
    mod_B1, mod_B2, mod_B3,
    mod_C1, mod_D,
    write_signing,
)
from refund import mod_R1, mod_R2, mod_R3, write_refund
from snapshot import (
    snap_receipt, delete_voided_receipts,
    snap_fund, snap_unrecognized,
    write_receipt, write_fund_snapshot,
)


# ═══════════════════════════════════════════════════════════════
# §9  验证
# ═══════════════════════════════════════════════════════════════
def verify():
    sep("同步结果")
    tables = ["dim_advisor","dim_monthly_target","dim_contract_group","dim_group_dept",
              "fact_signing","fact_refund","fact_receipt","fact_fund_snapshot"]
    with get_engine().connect() as conn:
        for t in tables:
            try:
                n = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
                print(f"  {t:<30} {n:>10,} 行")
            except: pass

        # v6: 额外检验 dim_monthly_target 的 sign_biz_type 分布
        try:
            rows = conn.execute(text(
                "SELECT sign_biz_type, COUNT(*) FROM dim_monthly_target GROUP BY sign_biz_type ORDER BY sign_biz_type"
            )).all()
            if rows:
                print(f"\n  dim_monthly_target 业务类型分布:")
                for bt, cnt in rows:
                    print(f"    {bt:<10} {cnt:>6} 条")
        except: pass

    print("\n  本次写入：")
    for k, v in stats.items():
        print(f"    {k:<30} +{v:>8,}")


# ═══════════════════════════════════════════════════════════════
# §10  主入口
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    start = datetime.now()
    print(f"广州前途财务日报 · 每日数据同步 v6")
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
        # 维度同步（注意: sync_dim_target 依赖 dim_group_dept, 所以顺序不能变）
        sep("维度同步")
        sync_dim_advisor()
        sync_dim_group_dept()       # v6: 必须在 sync_dim_target 之前，因后者反查依赖此表
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
        # v6: snap_receipt 现在返回 (records, voided_nos)，先删除作废再写入正常记录
        receipt_recs, voided_nos = snap_receipt()
        delete_voided_receipts(voided_nos)
        write_receipt(receipt_recs)
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
