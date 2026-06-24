"""
fact_writer.py — 事实表统一写入器（签约/退费共用）
========================================================
背景（double bug 根治 · 2026-06）:
  旧版 write_signing / write_refund 各自维护一份「按 source_system 分组、
  只删除本次 records 里出现过的 source」的写入逻辑。当某次同步缺少某源档
  （如【月更】业绩表.xlsx 未下载 → mod_A2/R2 回 []）时：
      by_source 没有 '月更' 这个 key
      → DELETE WHERE source_system='月更' 整条不执行
      → 历史灌入的旧月更分区永久残留，与重灌的历史(全FY)并存 → 金额翻倍。

第一性原理（最小错误单位）:
  清理动作（DELETE）被错误地绑定在「本次是否含该源数据」这个【易失条件】上，
  而非绑定在「表结构受管的固定源集合 MANAGED_SOURCES」这个【恒定条件】上。

修复（奥卡姆剃刀 · 最小依赖正确模型）:
  把 DELETE 绑定到 config.MANAGED_SOURCES —— 无条件清空全部受管分层后再重灌。
  与「本次是否含某源」彻底解耦 → 任何孤儿分层每次同步都会被自愈清除。

逆向思维（什么会让本修复失效 / 造成新伤害，已逐条处理）:
  1. 字面值简繁不一致 → DELETE 匹配 0 行、bug 静默存活
       → MANAGED_SOURCES 统一在 config.py，与 DB CHECK 简体值对齐。
  2. records 为空（全部源档缺失）时清空全表 → 灾难性数据丢失
       → 空则直接 return，绝不开启事务、绝不 DELETE。
  3. 写入中途失败留下半张表 → 单事务（begin），整体提交或整体回滚。
  4. 某受管源「本应有却本次为 0 行」（如源档临时缺失）被静默清空而无人察觉
       → 每次打印各 source 计数；受管源命中 0 行时打印醒目告警，
         把「沉默的失败」变成「可观测的信号」（见 _log_source_breakdown）。
"""
from collections import Counter
from sqlalchemy import text

from config import get_engine, stats, MANAGED_SOURCES


def _log_source_breakdown(table: str, records: list) -> None:
    """打印本次各 source 的记录数；受管源为 0 行时醒目告警。

    『月更 = 0 行』正是当初引发 double 的那个信号——现在它会在每次同步时
    第一时间显现，而不是几个月后才在报表里发现金额翻倍。
    """
    counts = Counter(r["source_system"] for r in records)
    for src in MANAGED_SOURCES:
        n = counts.get(src, 0)
        flag = "  ⚠ 本次该受管源 0 行（源档缺失？将被清空，请确认是否预期）" if n == 0 else ""
        print(f"    [{table}] {src}: {n} 行{flag}")
    # 出现未登记在 MANAGED_SOURCES 里的 source —— 不会被清理，提示登记
    for src, n in counts.items():
        if src not in MANAGED_SOURCES:
            print(f"    [{table}] ⚠ 非受管 source '{src}': {n} 行 "
                  f"（不会被全量刷新清理，如需托管请加入 config.MANAGED_SOURCES）")


def refresh_fact_table(table: str, columns: tuple, records: list, stats_key: str) -> int:
    """无条件清空受管分层后，全量重灌一张事实表。

    不变量：同步后，表中所有 MANAGED_SOURCES 分层 == 本次 records 全集。

    Args:
        table:     事实表名（'fact_signing' / 'fact_refund'）。
        columns:   写入列名元组；既用于拼 INSERT 列清单，也用于拼命名占位符，
                   两者同源，列顺序天然一致，不会错位。每个列名须是 records 字典的键。
        records:   待写入记录（dict 列表，键含 'source_system'）。
        stats_key: 写入 config.stats 的计数键。

    Returns:
        写入行数（records 为空时为 0）。
    """
    # 保护：本次完全无记录（所有源档缺失）→ 不动表，避免清空全表
    if not records:
        print(f"  ⚠ {table}: 本次无任何记录，跳过写入（保护：不清空全表）")
        stats[stats_key] = 0
        return 0

    _log_source_breakdown(table, records)

    col_list = ", ".join(columns)
    placeholders = ", ".join(f":{c}" for c in columns)
    delete_sql = text(f"DELETE FROM {table} WHERE source_system = ANY(:srcs)")
    insert_sql = text(f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})")

    with get_engine().begin() as conn:
        # 1) 无条件清空全部受管分层（与本次是否含该源无关 —— 根治点）
        deleted = conn.execute(delete_sql, {"srcs": list(MANAGED_SOURCES)}).rowcount
        # 2) 全量批量写入（executemany：一次传入整列表，无逐行 round-trip）
        conn.execute(insert_sql, records)

    stats[stats_key] = len(records)
    print(f"  ✓ {table}: 清除受管分层 {deleted} 行 → 写入 {len(records)} 行 "
          f"（受管 source = {MANAGED_SOURCES}）")
    return len(records)
