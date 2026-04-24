# backend/app/api/ingest.py
"""
数据摄入 API（供 n8n 内网调用）
实现 Pydantic 防呆 + 幂等写入 + 钉钉告警

v6 变更（2026-04-23）:
  - ingest_receipt: 跳过 status='作废' 的记录（不写入 DB），
    与 ETL daily_sync.py::snap_receipt 保持一致的过滤规则
    （通过 HTTP 接口过来的作废数据也不会污染 fact_receipt）
"""
from fastapi import APIRouter, Depends, HTTPException, Header
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from datetime import datetime

from app.core.database import get_db
from app.core.config import settings
from app.models.schemas import (
    IngestSigningPayload, IngestRefundPayload,
    IngestReceiptPayload, IngestFundSnapshotPayload,
)
from app.services.alerting import alert_ingest_error

router = APIRouter()


def _verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != settings.INTERNAL_API_KEY:
        raise HTTPException(status_code=403, detail="无效的内网 API Key")


# ─── 签约数据写入 ─────────────────────────────────────────────────────────
@router.post("/signing-data", summary="写入签约事实表（n8n 日更）")
async def ingest_signing(
    payload: IngestSigningPayload,
    db: AsyncSession = Depends(get_db),
    _: None = Depends(_verify_api_key),
):
    """
    写入签约事实表。contract_no 不再做主键去重（学校签约的班级编码可对应多名学生）。
    全部通过 Pydantic 校验后才开始批量写入，一条失败告警并跳过该条。
    幂等性由 daily_sync 全量刷新保证。
    """
    inserted, skipped, failed = 0, 0, 0
    errors = []

    # 记录日志
    log_res = await db.execute(text("""
        INSERT INTO ingest_log (source_tag, table_name, records_total, status)
        VALUES (:tag, 'fact_signing', :total, 'running')
        RETURNING id
    """), {"tag": payload.source_tag, "total": len(payload.records)})
    log_id = log_res.scalar()
    await db.commit()

    for rec in payload.records:
        try:
            await db.execute(text("""
                INSERT INTO fact_signing
                    (contract_no, sign_date, advisor_name, original_dept, line, sub_line,
                     secondary_group, sign_biz_type, school, gross_sign, source_system)
                VALUES
                    (:contract_no, :sign_date, :advisor_name, :original_dept, :line, :sub_line,
                     :secondary_group, :sign_biz_type, :school, :gross_sign, :source_system)
            """), rec.model_dump(exclude={"gross_sign_amount"}) | {
                "gross_sign": rec.gross_sign_amount
            })
            inserted += 1
        except Exception as e:
            failed += 1
            errors.append(f"合同号 {rec.contract_no}: {str(e)[:100]}")

    await db.commit()

    # 更新日志
    await db.execute(text("""
        UPDATE ingest_log SET
            records_inserted=:ins, records_skipped=:skip, records_failed=:fail,
            status=:status, finished_at=NOW(),
            error_detail=:err
        WHERE id=:id
    """), {
        "ins": inserted, "skip": skipped, "fail": failed,
        "status": "success" if failed == 0 else "partial",
        "err": "\n".join(errors) if errors else None,
        "id": log_id,
    })
    await db.commit()

    if failed > 0:
        await alert_ingest_error(
            payload.source_tag,
            f"{failed} 条记录写入失败",
            "\n".join(errors[:5])
        )

    return {
        "source_tag": payload.source_tag,
        "total":    len(payload.records),
        "inserted": inserted,
        "skipped":  skipped,
        "failed":   failed,
        "errors":   errors[:10],
    }


# ─── 退费数据写入 ─────────────────────────────────────────────────────────
@router.post("/refund-data", summary="写入退费事实表（n8n 日更）")
async def ingest_refund(
    payload: IngestRefundPayload,
    db: AsyncSession = Depends(get_db),
    _: None = Depends(_verify_api_key),
):
    inserted, failed = 0, 0
    errors = []

    for rec in payload.records:
        try:
            await db.execute(text("""
                INSERT INTO fact_refund
                    (refund_id, refund_date, contract_no, advisor_name, original_dept,
                     line, sub_line, secondary_group, refund_biz_type, gross_refund, source_system)
                VALUES
                    (:refund_id, :refund_date, :contract_no, :advisor_name, :original_dept,
                     :line, :sub_line, :secondary_group, :refund_biz_type, :gross_refund, :source_system)
            """), rec.model_dump())
            inserted += 1
        except Exception as e:
            failed += 1
            errors.append(f"退费ID {rec.refund_id}: {str(e)[:100]}")

    await db.commit()

    if failed > 0:
        await alert_ingest_error(payload.source_tag, f"{failed} 条退费记录失败", "\n".join(errors[:5]))

    return {"inserted": inserted, "failed": failed}


# ─── 收款数据写入 ─────────────────────────────────────────────────────────
@router.post("/receipt-data", summary="写入收款事实表（n8n 日更）")
async def ingest_receipt(
    payload: IngestReceiptPayload,
    db: AsyncSession = Depends(get_db),
    _: None = Depends(_verify_api_key),
):
    """
    v6: 收款数据写入时跳过 status='作废' 记录，与 ETL 保持一致。
    若需删除已入库但当前变为作废的记录，请依赖 ETL daily_sync.py
    的 delete_voided_receipts() 每日定时清理。
    """
    inserted, skipped, failed, voided = 0, 0, 0, 0
    errors = []

    for rec in payload.records:
        # v6: 作废记录不写入
        if rec.status == "作废":
            voided += 1
            continue
        try:
            result = await db.execute(text("""
                INSERT INTO fact_receipt
                    (receipt_no, receipt_date, arrived_date, contract_no,
                     advisor_name, dept, pay_method, status, sign_biz_type, amount)
                VALUES
                    (:receipt_no, :receipt_date, :arrived_date, :contract_no,
                     :advisor_name, :dept, :pay_method, :status, :sign_biz_type, :amount)
                ON CONFLICT (receipt_no) DO NOTHING
            """), rec.model_dump())
            if result.rowcount == 0:
                skipped += 1
            else:
                inserted += 1
        except Exception as e:
            failed += 1
            errors.append(f"收据号 {rec.receipt_no}: {str(e)[:100]}")

    await db.commit()

    if failed > 0:
        await alert_ingest_error(payload.source_tag, f"{failed} 条收款记录失败", "\n".join(errors[:5]))

    return {
        "inserted":       inserted,
        "skipped":        skipped,
        "voided_skipped": voided,   # v6 新增返回字段：作废记录被跳过的数量
        "failed":         failed,
    }


# ─── 资金快照写入 ─────────────────────────────────────────────────────────
@router.post("/fund-snapshot", summary="写入资金快照（每日全量替换）")
async def ingest_fund_snapshot(
    payload: IngestFundSnapshotPayload,
    db: AsyncSession = Depends(get_db),
    _: None = Depends(_verify_api_key),
):
    """
    资金快照采用全量替换策略：
    先删除当日同类型快照，再批量插入。
    """
    if payload.replace_date and payload.records:
        metric_types = list({r.metric_type for r in payload.records})
        await db.execute(text("""
            DELETE FROM fact_fund_snapshot
            WHERE snapshot_date = :d AND metric_type = ANY(:types)
        """), {"d": payload.replace_date, "types": metric_types})

    inserted = 0
    for rec in payload.records:
        await db.execute(text("""
            INSERT INTO fact_fund_snapshot
                (snapshot_date, contract_no, advisor_name, dept,
                 secondary_group, metric_type, amount, contract_status)
            VALUES
                (:snapshot_date, :contract_no, :advisor_name, :dept,
                 :secondary_group, :metric_type, :amount, :contract_status)
            ON CONFLICT (snapshot_date, metric_type, contract_no) DO UPDATE
                SET amount = EXCLUDED.amount,
                    dept = EXCLUDED.dept,
                    advisor_name = EXCLUDED.advisor_name
        """), rec.model_dump())
        inserted += 1

    await db.commit()
    return {"inserted": inserted, "replace_date": str(payload.replace_date)}
