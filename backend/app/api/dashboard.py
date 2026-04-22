# backend/app/api/dashboard.py
from datetime import date
from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user, AuthUser
from app.services.aggregation import build_daily_report

router = APIRouter()


@router.get("/daily-report", summary="獲取財務日報（完整 JSON）")
async def daily_report(
    execution_date: date = None,
    depts: Optional[str] = Query(None, description="[兼容舊版]部門篩選，逗號分隔"),
    advisors: Optional[str] = Query(None, description="顧問篩選，逗號分隔"),
    filter_line: Optional[str] = Query(None, description="按條線篩選，逗號分隔"),
    filter_sub_line: Optional[str] = Query(None, description="按二級條線篩選，逗號分隔"),
    filter_group_sys: Optional[str] = Query(None, description="按二級分組部門（系統口徑）篩選"),
    filter_biz_block: Optional[str] = Query(None, description="按業務板塊篩選"),
    filter_group_l1: Optional[str] = Query(None, description="按一級分組部門篩選"),
    filter_group_advisor: Optional[str] = Query(None, description="按二級分組部門（顧問口徑）篩選"),
    filter_biz_type: Optional[str] = Query(None, description="按業務類型篩選：留學/多語"),
    db: AsyncSession = Depends(get_db),
    current_user: AuthUser = Depends(get_current_user),
):
    today = execution_date or date.today()
    _split = lambda s: [x.strip() for x in s.split(",") if x.strip()] if s else None

    report = await build_daily_report(
        db=db, today=today,
        role=current_user.role,
        dept_scope=current_user.department_scope,
        advisor_name=current_user.advisor_name if current_user.role == "ADVISOR" else None,
        scope=current_user.scope if current_user.role == "SCOPED" else None,  # v3 新增
        filter_depts=_split(depts),
        filter_advisors=_split(advisors),
        filter_line=_split(filter_line),
        filter_sub_line=_split(filter_sub_line),
        filter_group_sys=_split(filter_group_sys),
        filter_biz_block=_split(filter_biz_block),
        filter_group_l1=_split(filter_group_l1),
        filter_group_advisor=_split(filter_group_advisor),
        filter_biz_type=_split(filter_biz_type),
    )
    return report
