# backend/app/api/sync.py
"""维度表同步 API（供钉钉多维表格 Webhook 调用）

v6 变更（2026-04-23）:
  - sync_monthly_target: department 字段缺失时，从 dim_group_dept 用
    secondary_group 反查 primary_group 自动填充。
    （配合 Excel '部门' 列废弃后的新口径）
"""
from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List
from pydantic import BaseModel
from datetime import date
from typing import Optional

from app.core.database import get_db
from app.core.config import settings
from app.models.schemas import AdvisorSyncRecord, TargetSyncRecord

router = APIRouter()


def _verify_key(x_api_key: str = Header(...)):
    if x_api_key != settings.INTERNAL_API_KEY:
        raise HTTPException(status_code=403, detail="无效的内网 API Key")


@router.post("/dim-advisor", summary="同步顾问字典（来自钉钉 Webhook）")
async def sync_advisor(
    records: List[AdvisorSyncRecord],
    db: AsyncSession = Depends(get_db),
    _: None = Depends(_verify_key),
):
    upserted = 0
    for rec in records:
        await db.execute(text("""
            INSERT INTO dim_advisor
                (advisor_id, name, email, primary_dept, secondary_group,
                 entry_date, exit_date, updated_at)
            VALUES
                (:advisor_id, :name, :email, :primary_dept, :secondary_group,
                 :entry_date, :exit_date, NOW())
            ON CONFLICT (advisor_id) DO UPDATE SET
                name            = EXCLUDED.name,
                email           = EXCLUDED.email,
                primary_dept    = EXCLUDED.primary_dept,
                secondary_group = EXCLUDED.secondary_group,
                entry_date      = EXCLUDED.entry_date,
                exit_date       = EXCLUDED.exit_date,
                updated_at      = NOW()
        """), rec.model_dump())
        upserted += 1
    await db.commit()
    return {"upserted": upserted}


@router.post("/dim-monthly-target", summary="同步月度目标（来自钉钉 Webhook）")
async def sync_monthly_target(
    records: List[TargetSyncRecord],
    db: AsyncSession = Depends(get_db),
    _: None = Depends(_verify_key),
):
    """
    v5: UPSERT 冲突键从 (year_month, secondary_group) 改为
        (year_month, secondary_group, sign_biz_type)，对齐 migration 07。
    v6: department 缺失时自动从 dim_group_dept 反查 primary_group。
        （Excel '部门' 列已废弃，但 schema 仍保留该字段 NOT NULL 约束，
         故此处做兜底填充）
    """
    # v6: 预加载 secondary_group → primary_group 映射
    rows = (await db.execute(text(
        "SELECT secondary_group, primary_group FROM dim_group_dept"
    ))).all()
    group_to_primary = {r[0]: (r[1] or "") for r in rows if r[0]}

    upserted = 0
    for rec in records:
        d = rec.model_dump()
        # v6: department 缺失时用 secondary_group 反查兜底
        if not d.get("department"):
            d["department"] = group_to_primary.get(d.get("secondary_group", ""), "")

        await db.execute(text("""
            INSERT INTO dim_monthly_target
                (year_month, department, secondary_group, sign_biz_type, target_amount, updated_at)
            VALUES
                (:year_month, :department, :secondary_group, :sign_biz_type, :target_amount, NOW())
            ON CONFLICT (year_month, secondary_group, sign_biz_type) DO UPDATE SET
                target_amount = EXCLUDED.target_amount,
                department    = EXCLUDED.department,
                updated_at    = NOW()
        """), d)
        upserted += 1
    await db.commit()
    return {"upserted": upserted}
