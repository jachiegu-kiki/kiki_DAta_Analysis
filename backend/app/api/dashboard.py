# backend/app/api/dashboard.py
from datetime import date
from typing import Optional, List
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.services.aggregation import build_daily_report
from app.models.user import UserModel

router = APIRouter()


@router.get("/daily-report", summary="获取财务日报（完整 JSON）")
async def daily_report(
    execution_date: date = None,
    depts: Optional[str] = Query(None, description="部门筛选，逗号分隔"),
    advisors: Optional[str] = Query(None, description="顾问筛选，逗号分隔"),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    today = execution_date or date.today()

    # 解析逗号分隔的筛选参数
    filter_depts = [d.strip() for d in depts.split(",") if d.strip()] if depts else None
    filter_advisors = [a.strip() for a in advisors.split(",") if a.strip()] if advisors else None

    report = await build_daily_report(
        db=db, today=today,
        role=current_user.role,
        dept_scope=current_user.department_scope,
        advisor_name=current_user.username if current_user.role == "ADVISOR" else None,
        filter_depts=filter_depts,
        filter_advisors=filter_advisors,
    )
    return report
