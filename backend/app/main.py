# backend/app/main.py
"""
廣州前途財務日報系統 · FastAPI 後端入口（整合版 v2）
────────────────────────────────────────────────────────────────
變更：
  ✗ 移除 auth router（/api/v1/auth/login）— 認證交給 Gateway
  ✓ 保留內網 ingest/sync/qa router（n8n 用 X-API-Key，不走 SSO）
  ✓ 新增 /api/v1/me — 回傳當前用戶（從 X-Auth-* header 解析）
"""
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.core.database import engine, Base
from app.core.security import get_current_user, AuthUser
from app.api import dashboard, ingest, sync, qa, etl_trigger


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()


app = FastAPI(
    title="廣州前途財務日報 API",
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

# CORS：本服務現在只從 Gateway 同源進來，理論上不需要 CORS；保留以防直連除錯
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 路由註冊（/api/v1/auth 已刪除）
app.include_router(dashboard.router,   prefix="/api/v1/dashboard",    tags=["日報看板"])
app.include_router(ingest.router,      prefix="/api/internal/ingest", tags=["數據攝入"])
app.include_router(sync.router,        prefix="/api/internal/sync",   tags=["維度同步"])
app.include_router(qa.router,          prefix="/api/internal/qa",     tags=["數據質檢"])
app.include_router(etl_trigger.router, prefix="/api/v1/etl",          tags=["ETL 觸發"])


@app.get("/api/v1/me", tags=["用戶"], summary="取當前登入用戶（由 Gateway 注入）")
async def me(current_user: AuthUser = Depends(get_current_user)):
    return {
        "username":         current_user.username,
        "role":             current_user.role,
        "display_name":     current_user.display_name,
        "department_scope": current_user.department_scope,
        "advisor_name":     current_user.advisor_name,
    }


@app.get("/health")
async def health():
    return {"status": "ok", "service": "qiantu-finance-api", "auth": "gateway-header"}
