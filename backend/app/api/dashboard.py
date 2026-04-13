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
    depts: Optional[str] = Query(None, description="[兼容旧版]部门筛选，逗号分隔"),
    advisors: Optional[str] = Query(None, description="顾问筛选，逗号分隔"),
    filter_line: Optional[str] = Query(None, description="按条线筛选，逗号分隔"),
    filter_sub_line: Optional[str] = Query(None, description="按二级条线筛选，逗号分隔"),
    filter_group_sys: Optional[str] = Query(None, description="按二级分组部门（系统口径）筛选，逗号分隔"),
    filter_biz_block: Optional[str] = Query(None, description="按业务板块筛选，逗号分隔"),
    filter_group_l1: Optional[str] = Query(None, description="按一级分组部门筛选，逗号分隔"),
    filter_group_advisor: Optional[str] = Query(None, description="按二级分组部门（顾问口径）筛选，逗号分隔"),
    filter_biz_type: Optional[str] = Query(None, description="按业务类型筛选：留学/多语，逗号分隔"),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    today = execution_date or date.today()

    _split = lambda s: [x.strip() for x in s.split(",") if x.strip()] if s else None
    filter_depts = _split(depts)
    filter_advisors = _split(advisors)

    report = await build_daily_report(
        db=db, today=today,
        role=current_user.role,
        dept_scope=current_user.department_scope,
        advisor_name=current_user.username if current_user.role == "ADVISOR" else None,
        filter_depts=filter_depts,
        filter_advisors=filter_advisors,
        filter_line=_split(filter_line),
        filter_sub_line=_split(filter_sub_line),
        filter_group_sys=_split(filter_group_sys),
        filter_biz_block=_split(filter_biz_block),
        filter_group_l1=_split(filter_group_l1),
        filter_group_advisor=_split(filter_group_advisor),
        filter_biz_type=_split(filter_biz_type),
    )
    return report
