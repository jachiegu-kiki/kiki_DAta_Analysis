# backend/app/core/config.py
"""
应用配置（v4 精简版）

v4 變更:
  ✗ 刪除 SECRET_KEY / ALGORITHM / ACCESS_TOKEN_EXPIRE_HOURS
    — 認證完全交給 Gateway（AccountSystem），本服務不自發 JWT

保留欄位:
  DATABASE_URL          PostgreSQL 連線串
  INTERNAL_API_KEY      供 n8n 呼叫 /api/internal/* 寫入端點的 shared secret
  DINGTALK_WEBHOOK_URL  告警 Webhook（可空）
  DINGTALK_SECRET       钉钉簽名密鑰（可空）
"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # 数据库
    DATABASE_URL: str = "postgresql+asyncpg://qiantu:qiantu2026@localhost:5432/qiantu_finance"

    # 内网 API Key（供 n8n 调用写入接口）
    INTERNAL_API_KEY: str = "CHANGE_ME_INTERNAL_KEY"

    # 告警 Webhook（钉钉）
    DINGTALK_WEBHOOK_URL: str = ""
    DINGTALK_SECRET: str = ""

    class Config:
        env_file = ".env"


settings = Settings()
