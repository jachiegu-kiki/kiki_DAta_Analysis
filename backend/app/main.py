# backend/app/main.py
"""
廣州前途財務日報系統 · FastAPI 後端入口（v4）
────────────────────────────────────────────────────────────────
v4 變更 (2026-04):
  ✓ /api/v1/me 返回 gw_role — 供前端判斷「系統管理員」類按鈕
    （例如 ETL 觸發按鈕）是否顯示
  ✓ CORS 收緊：預設不再 allow_origins="*"，改由環境變量白名單
    （同源 Gateway 反代時根本不需要 CORS；這條只是降風險）

v3 保留:
  ✗ 刪除 Base.metadata.create_all() — 全部用 raw SQL
  ✗ 刪除 engine.dispose()
  ✓ 保留 lifespan hook

v2 保留:
  ✗ 移除 auth router — 認證交給 Gateway
  ✓ 內網 ingest/sync/qa router（n8n 用 X-API-Key）
"""
import os

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.core.database import engine
from app.core.security import get_current_user, AuthUser
from app.api import dashboard, ingest, sync, qa, etl_trigger


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await engine.dispose()


app = FastAPI(
    title="廣州前途財務日報 API",
    version="4.0.0",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

# ── CORS 白名單（同源反代時為空即可；空 = 不允許跨域）────────
# 本服務在 Gateway 反代下為同源，根本不需要 CORS。保留此設定僅
# 為除錯直連。非必要不要開 "*"。
_cors_origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "").split(",") if o.strip()]
if _cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_cors_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )

# 路由註冊
app.include_router(dashboard.router,   prefix="/api/v1/dashboard",    tags=["日報看板"])
app.include_router(ingest.router,      prefix="/api/internal/ingest", tags=["數據攝入"])
app.include_router(sync.router,        prefix="/api/internal/sync",   tags=["維度同步"])
app.include_router(qa.router,          prefix="/api/internal/qa",     tags=["數據質檢"])
app.include_router(etl_trigger.router, prefix="/api/v1/etl",          tags=["ETL 觸發"])


@app.get("/api/v1/me", tags=["用戶"], summary="取當前登入用戶（由 Gateway 注入）")
async def me(current_user: AuthUser = Depends(get_current_user)):
    return {
        "username":         current_user.username,
        "role":             current_user.role,        # Finance 層 role
        "gw_role":          current_user.gw_role,     # v4: Gateway 層 role
        "is_system_admin":  current_user.is_system_admin(),  # v4: 前端直接用
        "display_name":     current_user.display_name,
        "department_scope": current_user.department_scope,
        "advisor_name":     current_user.advisor_name,
    }


@app.get("/health")
async def health():
    return {"status": "ok", "service": "qiantu-finance-api", "auth": "gateway-header"}
