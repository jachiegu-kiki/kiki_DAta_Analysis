# backend/app/api/auth.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from pydantic import BaseModel
from datetime import datetime, timezone

from app.core.database import get_db
from app.core.security import verify_password, create_access_token
from app.models.user import UserModel

router = APIRouter()


class LoginRequest(BaseModel):
    username: str
    password: str


@router.post("/login", summary="登录获取 JWT Token")
async def login(req: LoginRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(UserModel).where(UserModel.username == req.username)
    )
    user = result.scalar_one_or_none()

    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="用户名不存在或已停用")
    if not verify_password(req.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="密码错误")

    # 更新最后登录时间
    await db.execute(
        update(UserModel)
        .where(UserModel.id == user.id)
        .values(last_login=datetime.now(timezone.utc))
    )
    await db.commit()

    token = create_access_token({
        "sub":        str(user.id),
        "role":       user.role,
        "dept_scope": user.department_scope or "",
        "username":   user.username,
    })

    return {
        "access_token": token,
        "token_type":   "bearer",
        "role":         user.role,
        "username":     user.username,
        "dept_scope":   user.department_scope,
    }
