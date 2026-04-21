# backend/app/core/security.py
"""
Header-based Auth (整合版 v2)
────────────────────────────────────────────────────────────────
第一性原理：這個服務不需要自己做「你是誰」的判定，只需要消費
Gateway 已驗證的身份。因此:
  ✗ 刪除 JWT 編/解碼
  ✗ 刪除 bcrypt 密碼驗證
  ✗ 刪除 dim_users 查庫
  ✓ 只讀 Gateway 注入的 X-Auth-* header

合約（由 Gateway nginx 保證）:
  X-Auth-User           ASCII 用戶名
  X-Auth-Role           ADMIN / MANAGER / ADVISOR
  X-Auth-Dept-Scope     URL-encoded 中文部門（可空）
  X-Auth-Advisor-Name   URL-encoded 中文顧問名（可空）
  X-Auth-Display-Name   URL-encoded 顯示名稱（可空）

安全性：
  外部網路無法直達此服務（雲安全組關閉 8771 公網入站 + 僅 Gateway
  容器能經 host.docker.internal 進來）。因此信任 header 是安全的。
────────────────────────────────────────────────────────────────
"""
from dataclasses import dataclass
from typing import Optional
from urllib.parse import unquote

from fastapi import Header, HTTPException


VALID_ROLES = {"ADMIN", "MANAGER", "ADVISOR"}


@dataclass
class AuthUser:
    """取代原本的 UserModel 做 auth 用。對外欄位名保留兼容。"""
    username: str
    role: str                               # ADMIN / MANAGER / ADVISOR
    department_scope: Optional[str] = None
    advisor_name: Optional[str] = None
    display_name: Optional[str] = None

    # 兼容舊代碼取 .is_active 等字段的地方（一律認定為 active）
    is_active: bool = True


def _decode(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    try:
        return unquote(v) or None
    except Exception:
        return None


def get_current_user(
    x_auth_user:    Optional[str] = Header(None, alias="X-Auth-User"),
    x_auth_role:    Optional[str] = Header(None, alias="X-Auth-Role"),
    x_auth_dept:    Optional[str] = Header(None, alias="X-Auth-Dept-Scope"),
    x_auth_advisor: Optional[str] = Header(None, alias="X-Auth-Advisor-Name"),
    x_auth_display: Optional[str] = Header(None, alias="X-Auth-Display-Name"),
) -> AuthUser:
    """
    從 Gateway 注入的 header 構造當前用戶。
    缺 header → 401（大概率是繞過 Gateway 直連，或 Gateway 配置錯誤）
    role 非法 → 403
    """
    if not x_auth_user or not x_auth_role:
        raise HTTPException(status_code=401, detail="未認證（缺少 Gateway 注入的身份標頭）")

    role = x_auth_role.strip().upper()
    if role not in VALID_ROLES:
        raise HTTPException(status_code=403, detail=f"未知角色: {x_auth_role}")

    return AuthUser(
        username=x_auth_user.strip(),
        role=role,
        department_scope=_decode(x_auth_dept),
        advisor_name=_decode(x_auth_advisor),
        display_name=_decode(x_auth_display) or x_auth_user.strip(),
    )
