# backend/app/main.py
"""
广州前途财务日报系统 · FastAPI 后端入口
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.core.database import engine, Base
from app.api import dashboard, ingest, sync, auth, qa, etl_trigger


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时：确认数据库连接
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    # 关闭时：释放连接池
    await engine.dispose()


app = FastAPI(
    title="广州前途财务日报 API",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # 内网系统，允许所有来源
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 路由注册
app.include_router(auth.router,      prefix="/api/v1/auth",      tags=["认证"])
app.include_router(dashboard.router, prefix="/api/v1/dashboard", tags=["日报看板"])
app.include_router(ingest.router,    prefix="/api/internal/ingest", tags=["数据摄入"])
app.include_router(sync.router,      prefix="/api/internal/sync",   tags=["维度同步"])
app.include_router(qa.router,        prefix="/api/internal/qa",     tags=["数据质检"])
app.include_router(etl_trigger.router, prefix="/api/v1/etl",        tags=["ETL 触发"])


@app.get("/health")
async def health():
    return {"status": "ok", "service": "qiantu-finance-api"}
