# backend/app/api/etl_trigger.py
"""ETL 触发 API — 从前端一键执行数据同步

原理：
  API 容器通过挂载宿主机 docker.sock 调用 docker compose run --rm etl
  关键：compose file 路径和 --project-directory 必须使用宿主机路径
  容器内挂载项目到与宿主机相同的路径，确保 compose 的相对路径能被 daemon 正确 resolve
"""
import asyncio
import os
import logging
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException

from app.core.security import get_current_user
from app.models.user import UserModel

router = APIRouter()
logger = logging.getLogger("etl_trigger")

# 全局锁：防止并发触发
_etl_lock = asyncio.Lock()

# 宿主机上项目的绝对路径（通过 docker-compose.yml 的 environment 注入）
PROJECT_HOST_DIR = os.getenv("PROJECT_HOST_DIR", "/opt/qiantu-finance-v4")


@router.post("/trigger", summary="触发 ETL 数据同步（仅管理员）")
async def trigger_etl(
    current_user: UserModel = Depends(get_current_user),
):
    # 仅 ADMIN 角色允许触发
    if current_user.role != "ADMIN":
        raise HTTPException(status_code=403, detail="仅管理员可执行数据同步")

    # 防止并发执行
    if _etl_lock.locked():
        raise HTTPException(status_code=409, detail="ETL 正在执行中，请稍后重试")

    compose_file = f"{PROJECT_HOST_DIR}/docker-compose.yml"

    async with _etl_lock:
        started = datetime.now(timezone.utc)
        logger.info(f"[ETL] 用户 {current_user.username} 触发同步 @ {started}")
        logger.info(f"[ETL] compose file: {compose_file}, project dir: {PROJECT_HOST_DIR}")

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

            stdout_text = stdout.decode("utf-8", errors="replace")[-3000:]  # 截取尾部
            stderr_text = stderr.decode("utf-8", errors="replace")[-1500:]

            if proc.returncode != 0:
                logger.error(f"[ETL] 失败 (exit={proc.returncode}): {stderr_text}")
                raise HTTPException(
                    status_code=500,
                    detail=f"ETL 执行失败 (exit code {proc.returncode})\n{stderr_text}",
                )

            logger.info(f"[ETL] 完成, 耗时 {elapsed:.1f}s")
            # 把 ETL 完整输出打印到 API 容器日志，方便 docker logs 查看
            print(f"\n{'='*60}\n[ETL 同步输出] 耗时 {elapsed:.1f}s\n{'='*60}")
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
            logger.error("[ETL] 超时（300s）")
            raise HTTPException(status_code=504, detail="ETL 执行超时（>300秒），请检查服务器日志")
        except HTTPException:
            raise
        except Exception as e:
            logger.exception("[ETL] 未知错误")
            raise HTTPException(status_code=500, detail=f"ETL 触发异常: {str(e)}")
