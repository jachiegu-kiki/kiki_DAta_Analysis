# backend/app/core/security.py
"""
Header-based Auth (整合版 v3)
────────────────────────────────────────────────────────────────
第一性原理：這個服務不需要自己做「你是誰」的判定，只需要消費
Gateway 已驗證的身份。因此:
  ✗ 刪除 JWT 編/解碼
  ✗ 刪除 bcrypt 密碼驗證
  ✗ 刪除 dim_users 查庫
  ✓ 只讀 Gateway 注入的 X-Auth-* header

v3 變更:
  ✓ 支援新角色 SCOPED: 由 X-Auth-Scope header 攜帶 URL-encoded JSON
    形式的多維度白名單，如 {"line":["欧洲","亚洲"],"biz_block":["欧亚"]}
    下游據此把 scope 當作永久 WHERE 子句。

合約（由 Gateway nginx 保證）:
  X-Auth-User           ASCII 用戶名
  X-Auth-Role           ADMIN / MANAGER / ADVISOR / SCOPED
  X-Auth-Dept-Scope     URL-encoded 中文部門（MANAGER 用）
  X-Auth-Advisor-Name   URL-encoded 中文顧問名（ADVISOR 用）
  X-Auth-Scope          URL-encoded JSON（SCOPED 用，v3 新增）
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

VALID_ROLES = {"ADMIN", "MANAGER", "ADVISOR", "SCOPED"}


@dataclass
class AuthUser:
    """取代原本的 UserModel 做 auth 用。對外欄位名保留兼容。"""
    username: str
    role: str                               # ADMIN / MANAGER / ADVISOR / SCOPED
    department_scope: Optional[str] = None
    advisor_name: Optional[str] = None
    display_name: Optional[str] = None
    # v3 新增: SCOPED 角色的多維白名單
    # 例: {"line": ["欧洲","亚洲"], "biz_block": ["欧亚","外包"]}
    scope: Dict[str, List[str]] = field(default_factory=dict)

    # 兼容舊代碼取 .is_active 等字段的地方（一律認定為 active）
    is_active: bool = True


def _decode(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    try:
        return unquote(v) or None
    except Exception:
        return None


def _parse_scope(v: Optional[str]) -> Dict[str, List[str]]:
    """
    X-Auth-Scope 解析。防禦性處理:
      - 空 → {}
      - 非法 JSON → {} 並記 warn
      - 非 dict 或 value 非 list → {}
      - value 會被強制轉為 list[str] 並過濾空值
    """
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
    if role not in VALID_ROLES:
        raise HTTPException(status_code=403, detail=f"未知角色: {x_auth_role}")

    scope = _parse_scope(x_auth_scope)
    if role == "SCOPED" and not scope:
        # SCOPED 必須帶 scope，否則拒絕進入（避免降級成 ADMIN）
        raise HTTPException(status_code=403, detail="SCOPED 角色缺少 X-Auth-Scope")

    return AuthUser(
        username=x_auth_user.strip(),
        role=role,
        department_scope=_decode(x_auth_dept),
        advisor_name=_decode(x_auth_advisor),
        display_name=_decode(x_auth_display) or x_auth_user.strip(),
        scope=scope,
    )
