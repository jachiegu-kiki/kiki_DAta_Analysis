# backend/app/core/config.py
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # 数据库
    DATABASE_URL: str = "postgresql+asyncpg://qiantu:qiantu2026@localhost:5432/qiantu_finance"

    # JWT
    SECRET_KEY: str = "CHANGE_ME_IN_PRODUCTION_32CHAR_MIN"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_HOURS: int = 8

    # 内网 API Key（供 n8n 调用写入接口）
    INTERNAL_API_KEY: str = "CHANGE_ME_INTERNAL_KEY"

    # 告警 Webhook（钉钉）
    DINGTALK_WEBHOOK_URL: str = ""
    DINGTALK_SECRET: str = ""

    class Config:
        env_file = ".env"


settings = Settings()
