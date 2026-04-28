"""
utils.py — 通用工具（无业务逻辑、无 DB 状态）
========================================================
· 字符串/数值清洗：cs / cs_or_none / cf
· 日期处理：safe_date / combine_ymd / _fix_excel_serial
· Excel 读取：get_file / read_excel
· 业务类型归一化：normalize_biz_type（项目全局唯一入口）
· 打印分隔：sep
"""
import os
import math
import warnings
from datetime import date, datetime, timedelta
import pandas as pd

from config import FILES

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


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


def normalize_biz_type(v) -> str:
    """[v6 新增] 统一业务类型归一化（项目全局唯一入口）

    源表/上游 Excel/API 可能填 '培训' / '语培' / '多语' / '留学' 等，
    数据库 schema 只允许 ('留学', '多语') 两值。
    此函数负责把所有变体归一化到这两个值。

    映射规则：
      '多语' / '培训' / '语培'  →  '多语'
      '留学' / 空                →  '留学'
      其他未知值                 →  '留学' 并打警告日志

    与 backend/app/models/schemas.py::normalize_biz_type 保持逻辑一致，
    修改时请同步更新两处。

    Returns:
        str: 归一化后的业务类型，必为 '留学' 或 '多语'
    """
    s = cs(v)
    if s in ("多语", "培训", "语培"):
        return "多语"
    if s in ("留学", ""):
        return "留学"
    print(f"  [警告] biz_type 列出现未知值: '{s}'，默认归为'留学'，请检查源数据")
    return "留学"


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
