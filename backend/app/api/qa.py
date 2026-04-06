# backend/app/api/qa.py
"""数据质检 API（替代 run_qa_checks() 函数，提供 HTTP 接口）"""
from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.core.database import get_db
from app.core.config import settings

router = APIRouter()


def _verify_key(x_api_key: str = Header(...)):
    if x_api_key != settings.INTERNAL_API_KEY:
        raise HTTPException(status_code=403, detail="无效的内网 API Key")


@router.get("/summary", summary="数据质检摘要")
async def qa_summary(
    db: AsyncSession = Depends(get_db),
    _: None = Depends(_verify_key),
):
    """
    对应原脚本 run_qa_checks() 的全部检查项，以 JSON 形式返回。
    前端数据监控面板直接调用此接口。
    """
    results = {}

    # [1] 各数据层行数分布（签约）
    layer_dist = await db.execute(text("""
        SELECT source_system, COUNT(*) as cnt, SUM(gross_sign)/10000 as amount_wan
        FROM fact_signing GROUP BY source_system
    """))
    results["signing_layer_distribution"] = [
        {"layer": r[0], "count": r[1], "amount_wan": round(float(r[2] or 0), 2)}
        for r in layer_dist
    ]

    # [2] 各阵地毛签金额
    school_dist = await db.execute(text("""
        SELECT school, SUM(gross_sign)/10000 as amount_wan FROM fact_signing GROUP BY school
    """))
    results["signing_by_school"] = [
        {"school": r[0], "amount_wan": round(float(r[1] or 0), 2)} for r in school_dist
    ]

    # [3] 留学 vs 多语
    biz_dist = await db.execute(text("""
        SELECT sign_biz_type, SUM(gross_sign)/10000 as amount_wan FROM fact_signing GROUP BY sign_biz_type
    """))
    results["signing_by_biz_type"] = [
        {"biz_type": r[0], "amount_wan": round(float(r[1] or 0), 2)} for r in biz_dist
    ]

    # [4] 二级分组映射失败率
    unknown_dept = await db.execute(text("""
        SELECT COUNT(*) FROM fact_signing WHERE secondary_group = '未知部门'
    """))
    total_sign = await db.execute(text("SELECT COUNT(*) FROM fact_signing"))
    u = unknown_dept.scalar() or 0
    t = total_sign.scalar() or 1
    results["unknown_dept_count"] = int(u)
    results["unknown_dept_pct"] = round(u / t * 100, 2)

    # [5] 迅程顾问邮箱映射失败数
    xc_no_adv = await db.execute(text("""
        SELECT COUNT(*) FROM fact_signing WHERE school='迅程' AND advisor_name=''
    """))
    results["xuncheng_no_advisor"] = int(xc_no_adv.scalar() or 0)

    # [6] 二级条线为空比例
    no_subline = await db.execute(text("""
        SELECT COUNT(*) FROM fact_signing WHERE sub_line=''
    """))
    results["no_subline_count"] = int(no_subline.scalar() or 0)
    results["no_subline_pct"] = round(int(no_subline.scalar() or 0) / t * 100, 2)

    # [7] 日期范围
    date_range = await db.execute(text("""
        SELECT MIN(sign_date), MAX(sign_date) FROM fact_signing
    """))
    dr = date_range.one()
    results["signing_date_range"] = {
        "min": str(dr[0]) if dr[0] else None,
        "max": str(dr[1]) if dr[1] else None,
    }

    # [8] 退费表摘要
    refund_summary = await db.execute(text("""
        SELECT COUNT(*), SUM(gross_refund)/10000 FROM fact_refund
    """))
    rs = refund_summary.one()
    results["refund_summary"] = {"count": int(rs[0] or 0), "amount_wan": round(float(rs[1] or 0), 2)}

    # [9] 收款表摘要
    receipt_summary = await db.execute(text("""
        SELECT COUNT(*), SUM(amount)/10000 FROM fact_receipt
    """))
    rcs = receipt_summary.one()
    results["receipt_summary"] = {"count": int(rcs[0] or 0), "amount_wan": round(float(rcs[1] or 0), 2)}

    # [10] 最近摄入日志
    recent_logs = await db.execute(text("""
        SELECT source_tag, table_name, records_total, records_inserted, records_failed, status, started_at
        FROM ingest_log ORDER BY started_at DESC LIMIT 10
    """))
    results["recent_ingest_logs"] = [
        {
            "source_tag": r[0], "table": r[1], "total": r[2],
            "inserted": r[3], "failed": r[4], "status": r[5],
            "started_at": str(r[6]),
        }
        for r in recent_logs
    ]

    return results
