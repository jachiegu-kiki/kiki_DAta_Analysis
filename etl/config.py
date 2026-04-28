"""
config.py — 全局配置 & 共享状态
========================================================
集中管理：
  · 数据库连接串 / 数据目录
  · 文件注册表 FILES（修改文件名/sheet/表头行只需改这里）
  · 业务常量（亚洲国家集合）
  · 数据库引擎（懒初始化）
  · 全局写入计数器 stats（跨模块共享，dict 可变所以 import 后直接写即可）
"""
import os
from datetime import date
from sqlalchemy import create_engine

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
    "group_dept_param": (PREDATA_DIR, "参数-分组部门.xlsx",       "Sheet1",       0),
    "history_sign":   (PREDATA_DIR, "历史数据.xlsx",             "签约明细",     0),  # A3
    "history_refund": (PREDATA_DIR, "历史数据.xlsx",             "退费",         0),  # R3
    "history_school": (PREDATA_DIR, "历史数据-学校.xlsx",        "签约金额按天", 0),  # B3
    "unrecognized":   (PREDATA_DIR, "【人手更】未认款明细.xlsx", "未认款明细",   1),  # 表头第2行=header=1
}

ASIA = {"日本","韩国","泰国","亚洲其他","中国香港","中国澳门","香港","澳门","新加坡","马来西亚"}

_engine = None
stats = {}

def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
    return _engine
