# backend/app/api/etl_trigger.py
"""ETL 觸發 API — 從前端一鍵執行數據同步（整合版 v2）"""
import asyncio
import os
import logging
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException

from app.core.security import get_current_user, AuthUser

router = APIRouter()
logger = logging.getLogger("etl_trigger")

_etl_lock = asyncio.Lock()
PROJECT_HOST_DIR = os.getenv("PROJECT_HOST_DIR", "/opt/qiantu-finance-v4")


@router.post("/trigger", summary="觸發 ETL 數據同步（僅管理員）")
async def trigger_etl(current_user: AuthUser = Depends(get_current_user)):
    if current_user.role != "ADMIN":
        raise HTTPException(status_code=403, detail="僅管理員可執行數據同步")

    if _etl_lock.locked():
        raise HTTPException(status_code=409, detail="ETL 正在執行中，請稍後重試")

    compose_file = f"{PROJECT_HOST_DIR}/docker-compose.yml"

    async with _etl_lock:
        started = datetime.now(timezone.utc)
        logger.info(f"[ETL] 用戶 {current_user.username} 觸發同步 @ {started}")
        logger.info(f"[ETL] compose: {compose_file}, project_dir: {PROJECT_HOST_DIR}")

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
                logger.error(f"[ETL] 失敗 (exit={proc.returncode}): {stderr_text}")
                raise HTTPException(
                    status_code=500,
                    detail=f"ETL 執行失敗 (exit {proc.returncode})\n{stderr_text}",
                )

            logger.info(f"[ETL] 完成, 耗時 {elapsed:.1f}s")
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
            }

        except asyncio.TimeoutError:
            logger.error("[ETL] 超時（300s）")
            raise HTTPException(status_code=504, detail="ETL 執行超時（>300秒）")
        except HTTPException:
            raise
        except Exception as e:
            logger.exception("[ETL] 未知錯誤")
            raise HTTPException(status_code=500, detail=f"ETL 觸發異常: {str(e)}")
