# backend/app/core/security.py
"""
Header-based Auth (整合版 v4)
────────────────────────────────────────────────────────────────
v4 變更 (2026-04):
  ✓ 新增 gw_role 字段 — 讀取 Gateway 注入的 X-Auth-Gw-Role
    用途：分離「看全公司數據」(finance ADMIN) 與「能觸發系統操作」
    (gw_role 'admin')。ETL/管理類 API 必須用 gw_role 判定。

v3 保留:
  ✓ 支援 SCOPED 角色 + X-Auth-Scope JSON 白名單

合約（由 Gateway nginx 保證）:
  X-Auth-User           ASCII 用戶名
  X-Auth-Gw-Role        Gateway 層角色 admin/manager/consultant/advisor/viewer
  X-Auth-Role           Finance 數據層角色 ADMIN/MANAGER/ADVISOR/SCOPED
  X-Auth-Dept-Scope     URL-encoded 中文部門（MANAGER 用）
  X-Auth-Advisor-Name   URL-encoded 中文顧問名（ADVISOR 用）
  X-Auth-Scope          URL-encoded JSON（SCOPED 用）
  X-Auth-Display-Name   URL-encoded 顯示名稱（可空）

安全性：
  外部網路無法直達此服務（雲安全組關閉 8771 公網入站 + 僅 Gateway
  容器能經 host.docker.internal 進來）。因此信任 header 是安全的。
────────────────────────────────────────────────────────────────
"""
import json
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, List
from urllib.parse import unquote

from fastapi import Header, HTTPException

log = logging.getLogger("security")

VALID_FIN_ROLES = {"ADMIN", "MANAGER", "ADVISOR", "SCOPED"}
VALID_GW_ROLES = {"admin", "manager", "consultant", "advisor", "viewer"}


@dataclass
class AuthUser:
    """Gateway 注入的當前用戶身份。"""
    username: str
    role: str                               # Finance 層：ADMIN/MANAGER/ADVISOR/SCOPED
    gw_role: str = ""                       # v4 新增：Gateway 層 admin/manager/...
    department_scope: Optional[str] = None
    advisor_name: Optional[str] = None
    display_name: Optional[str] = None
    # SCOPED 角色的多維白名單，例: {"line": ["欧洲","亚洲"]}
    scope: Dict[str, List[str]] = field(default_factory=dict)

    # 兼容舊代碼
    is_active: bool = True

    # ── 權限判定輔助 ───────────────────────────────────────
    def is_system_admin(self) -> bool:
        """
        判定是否為『系統管理員』— 僅 gw_role == 'admin'。

        第一性原理：系統操作（ETL、用戶管理、服務重啟）與業務數據看全
        公司（finance ADMIN）是兩個不同維度。不可混用。
        """
        return self.gw_role == "admin"


def _decode(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    try:
        return unquote(v) or None
    except Exception:
        return None


def _parse_scope(v: Optional[str]) -> Dict[str, List[str]]:
    """X-Auth-Scope 解析，防禦性處理見 v3 註釋。"""
    if not v:
        return {}
    try:
        raw = json.loads(unquote(v))
    except (json.JSONDecodeError, ValueError) as e:
        log.warning("X-Auth-Scope parse failed: %s", e)
        return {}
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, List[str]] = {}
    for k, vals in raw.items():
        if not isinstance(k, str):
            continue
        if not isinstance(vals, (list, tuple)):
            continue
        clean = [str(x).strip() for x in vals if x is not None and str(x).strip()]
        if clean:
            out[k] = clean
    return out


def get_current_user(
    x_auth_user:    Optional[str] = Header(None, alias="X-Auth-User"),
    x_auth_gw_role: Optional[str] = Header(None, alias="X-Auth-Gw-Role"),  # v4
    x_auth_role:    Optional[str] = Header(None, alias="X-Auth-Role"),
    x_auth_dept:    Optional[str] = Header(None, alias="X-Auth-Dept-Scope"),
    x_auth_advisor: Optional[str] = Header(None, alias="X-Auth-Advisor-Name"),
    x_auth_scope:   Optional[str] = Header(None, alias="X-Auth-Scope"),
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
    if role not in VALID_FIN_ROLES:
        raise HTTPException(status_code=403, detail=f"未知 finance 角色: {x_auth_role}")

    # gw_role 可選（舊 Gateway 可能不注入此 header）；注入則驗證合法
    gw_role = (x_auth_gw_role or "").strip().lower()
    if gw_role and gw_role not in VALID_GW_ROLES:
        log.warning("未知 gw_role '%s'，當作空值處理", gw_role)
        gw_role = ""

    scope = _parse_scope(x_auth_scope)
    if role == "SCOPED" and not scope:
        raise HTTPException(status_code=403, detail="SCOPED 角色缺少 X-Auth-Scope")

    return AuthUser(
        username=x_auth_user.strip(),
        role=role,
        gw_role=gw_role,
        department_scope=_decode(x_auth_dept),
        advisor_name=_decode(x_auth_advisor),
        display_name=_decode(x_auth_display) or x_auth_user.strip(),
        scope=scope,
    )


def require_system_admin(
    current_user: AuthUser,
) -> AuthUser:
    """
    依賴注入輔助：要求系統管理員。用於 ETL、用戶管理等敏感端點。

    用法（在 router 裡）:
        @router.post("/trigger")
        async def trigger(u: AuthUser = Depends(get_current_user)):
            require_system_admin(u)
            ...
    """
    if not current_user.is_system_admin():
        raise HTTPException(
            status_code=403,
            detail="此操作僅限系統管理員（gw_role=admin）",
        )
    return current_user
