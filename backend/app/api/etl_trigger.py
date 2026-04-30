# backend/app/api/etl_trigger.py
"""ETL 觸發 API — 從前端一鍵執行數據同步（整合版 v4）

v4 變更 (2026-04-30):
  ✓ 新增 GET /status 端點 — 返回當前 ETL 是否運行中、最近一次完成時間、
    上次狀態、耗時等元數據。供前端「同步數據」按鈕點擊前的預檢與
    二次確認使用，避免重複觸發。
  ✓ trigger_etl 在進入/退出鎖時同步維護 _etl_history 字典；返回體內附
    completed_at 便於前端立即更新展示。
  ✓ 衝突響應 detail 改為結構化 dict（含 started_at），前端可直接展示
    「正在同步中（開始於 X）」而無需二次請求。
  ✓ 時區策略: 後端一律返回 UTC ISO 8601；上海時間轉換交由前端
    Intl.DateTimeFormat({timeZone:'Asia/Shanghai'}) 處理，零依賴。

  第一性原理:
    「是否運行中」的真相源是 asyncio.Lock 本身（_etl_lock.locked()），
    不需要在 _etl_history 中冗余 running 字段，避免雙真相源不一致。
    _etl_history 只記錄「歷史快照」（started_at / completed_at / status）。
  奧卡姆剃刀:
    單進程內存字典已足夠當前單 worker 部署。如未來改 multi-worker，
    再升級到 Redis / DB；現在不引依賴。

v3 保留 (2026-04):
  ✓ 權限判定從 finance.role == 'ADMIN' 改為 gw_role == 'admin'
    原因：finance.ADMIN 的語義是「看全公司數據」，包含 manager；
    而 ETL 觸發屬於系統級操作，應該只允許系統管理員。
    見 security.py AuthUser.is_system_admin()
"""
import asyncio
import os
import logging
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException

from app.core.security import get_current_user, AuthUser, require_system_admin

router = APIRouter()
logger = logging.getLogger("etl_trigger")

# 真相源: 鎖狀態 = 是否運行中
_etl_lock = asyncio.Lock()

# 歷史快照: 進程內單例（單 worker 部署足夠；多 worker 需移到 Redis/DB）
# 不存 running 字段 — 避免與 _etl_lock.locked() 雙真相源不一致
_etl_history = {
    "started_at":      None,  # str  — UTC ISO 8601, 最近一次啟動時刻
    "completed_at":    None,  # str  — UTC ISO 8601, 最近一次「成功完成」時刻
    "last_status":     None,  # str  — 'success' | 'failed' | 'timeout' | None
    "last_error":      None,  # str  — 失敗時的尾部錯誤訊息（截斷）
    "elapsed_seconds": None,  # float — 最近一次耗時（成功或失敗均記錄）
    "triggered_by":    None,  # str  — 最近一次觸發人 username
}

PROJECT_HOST_DIR = os.getenv("PROJECT_HOST_DIR", "/opt/qiantu-finance-v4")


@router.get("/status", summary="查詢 ETL 執行狀態（時間戳一律 UTC ISO，前端轉換上海時區展示）")
async def etl_status(current_user: AuthUser = Depends(get_current_user)):
    """
    返回:
      - running:          當前是否正在執行（從 _etl_lock 直接讀，唯一真相源）
      - started_at:       最近一次啟動時刻 (UTC ISO)
      - completed_at:     最近一次「成功完成」時刻 (UTC ISO)
      - last_status:      最近一次結果 'success' | 'failed' | 'timeout' | None
      - last_error:       失敗訊息尾部（成功為 None）
      - elapsed_seconds:  最近一次耗時
      - triggered_by:     最近一次觸發人 username

    時區策略: 後端只返回 UTC ISO，前端負責用 Intl.DateTimeFormat
    轉換為 Asia/Shanghai 顯示，避免後端硬編碼時區耦合表現層。
    """
    require_system_admin(current_user)
    return {
        "running":         _etl_lock.locked(),
        "started_at":      _etl_history["started_at"],
        "completed_at":    _etl_history["completed_at"],
        "last_status":     _etl_history["last_status"],
        "last_error":      _etl_history["last_error"],
        "elapsed_seconds": _etl_history["elapsed_seconds"],
        "triggered_by":    _etl_history["triggered_by"],
    }


@router.post("/trigger", summary="觸發 ETL 數據同步（僅系統管理員 gw_role=admin）")
async def trigger_etl(current_user: AuthUser = Depends(get_current_user)):
    # v3: 用 gw_role 判定，而非 finance role
    require_system_admin(current_user)

    # v4: 結構化 detail，前端可直接展示「正在同步中（開始於 X）」
    if _etl_lock.locked():
        raise HTTPException(
            status_code=409,
            detail={
                "code": "ETL_RUNNING",
                "message": "ETL 正在執行中，請勿重複觸發",
                "started_at": _etl_history["started_at"],
                "triggered_by": _etl_history["triggered_by"],
            },
        )

    compose_file = f"{PROJECT_HOST_DIR}/docker-compose.yml"

    async with _etl_lock:
        started = datetime.now(timezone.utc)
        # v4: 進入鎖即更新「最近一次啟動」歷史
        _etl_history["started_at"]   = started.isoformat()
        _etl_history["last_error"]   = None
        _etl_history["triggered_by"] = current_user.username

        logger.info(
            "[ETL] 用戶 %s (gw=%s fin=%s) 觸發同步 @ %s",
            current_user.username, current_user.gw_role, current_user.role, started,
        )
        logger.info("[ETL] compose: %s, project_dir: %s", compose_file, PROJECT_HOST_DIR)

        try:
            proc = await asyncio.create_subprocess_exec(
                "docker", "compose",
                "-f", compose_file,
                "--project-directory", PROJECT_HOST_DIR,
                "run", "--rm", "etl",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
            elapsed = (datetime.now(timezone.utc) - started).total_seconds()
            stdout_text = stdout.decode("utf-8", errors="replace")[-3000:]
            stderr_text = stderr.decode("utf-8", errors="replace")[-1500:]

            if proc.returncode != 0:
                logger.error("[ETL] 失敗 (exit=%s): %s", proc.returncode, stderr_text)
                # v4: 失敗也更新歷史，但不更新 completed_at（completed_at 語義 = 上次成功完成）
                _etl_history["last_status"]     = "failed"
                _etl_history["last_error"]      = f"exit {proc.returncode}: {stderr_text[-500:]}"
                _etl_history["elapsed_seconds"] = round(elapsed, 1)
                raise HTTPException(
                    status_code=500,
                    detail=f"ETL 執行失敗 (exit {proc.returncode})\n{stderr_text}",
                )

            # 成功
            now_iso = datetime.now(timezone.utc).isoformat()
            _etl_history["completed_at"]    = now_iso
            _etl_history["last_status"]     = "success"
            _etl_history["last_error"]      = None
            _etl_history["elapsed_seconds"] = round(elapsed, 1)

            logger.info("[ETL] 完成, 耗時 %.1fs", elapsed)
            print(f"\n{'='*60}\n[ETL 同步輸出] 耗時 {elapsed:.1f}s\n{'='*60}")
            print(stdout_text)
            if stderr_text.strip():
                print(f"[ETL stderr] {stderr_text}")
            print(f"{'='*60}\n")
            return {
                "status": "success",
                "elapsed_seconds": round(elapsed, 1),
                "output_tail": stdout_text,
                "triggered_by": current_user.username,
                "completed_at": now_iso,  # v4: 前端可直接更新展示
            }

        except asyncio.TimeoutError:
            logger.error("[ETL] 超時（300s）")
            _etl_history["last_status"]     = "timeout"
            _etl_history["last_error"]      = "ETL 執行超時（>300秒）"
            _etl_history["elapsed_seconds"] = 300.0
            raise HTTPException(status_code=504, detail="ETL 執行超時（>300秒）")
        except HTTPException:
            # 已在上方分支記錄歷史，直接重拋
            raise
        except Exception as e:
            logger.exception("[ETL] 未知錯誤")
            _etl_history["last_status"] = "failed"
            _etl_history["last_error"]  = str(e)[:500]
            raise HTTPException(status_code=500, detail=f"ETL 觸發異常: {str(e)}")
