"""
time_boundary.py — 时间边界（第一/二章）
========================================================
财年起始 / 日更起始 / 数据层判定。
模块导入时即固定 FY_START 与 DAILY_START，与原版语义完全一致。
"""
from datetime import date, timedelta

from config import TODAY


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
