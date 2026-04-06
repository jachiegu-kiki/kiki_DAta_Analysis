# backend/app/services/alerting.py
"""
告警推送服务：支持钉钉 / 企业微信 Webhook
"""
import hmac, hashlib, base64, time, urllib.parse
from datetime import datetime
import httpx
from app.core.config import settings


async def send_dingtalk_alert(title: str, content: str, level: str = "warning") -> bool:
    """
    发送钉钉告警（加签模式）
    level: warning | error | info
    """
    if not settings.DINGTALK_WEBHOOK_URL:
        print(f"[告警未配置] {title}: {content}")
        return False

    # 生成加签
    timestamp = str(round(time.time() * 1000))
    secret_enc = settings.DINGTALK_SECRET.encode("utf-8")
    string_to_sign = f"{timestamp}\n{settings.DINGTALK_SECRET}"
    sign = base64.b64encode(
        hmac.new(secret_enc, string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
    ).decode("utf-8")
    sign_encoded = urllib.parse.quote_plus(sign)

    url = f"{settings.DINGTALK_WEBHOOK_URL}&timestamp={timestamp}&sign={sign_encoded}"

    emoji = {"error": "🔴", "warning": "⚠️", "info": "ℹ️"}.get(level, "⚠️")
    body = {
        "msgtype": "markdown",
        "markdown": {
            "title": f"{emoji} {title}",
            "text": (
                f"## {emoji} {title}\n\n"
                f"{content}\n\n"
                f"> 时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                f"> 系统：广州前途财务日报"
            ),
        },
    }

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(url, json=body)
            return resp.status_code == 200
    except Exception as e:
        print(f"[告警推送失败] {e}")
        return False


async def alert_ingest_error(source: str, error: str, record_detail: str = "") -> None:
    """数据写入异常专用告警"""
    content = (
        f"**来源系统**：{source}  \n"
        f"**异常原因**：{error}  \n"
    )
    if record_detail:
        content += f"**数据详情**：{record_detail}  \n"
    content += "**操作建议**：请核查原始 Excel 文件并重新触发 n8n 工作流"
    await send_dingtalk_alert(f"数据写入异常 · {source}", content, level="error")


async def alert_workflow_success(workflow: str, inserted: int, skipped: int) -> None:
    """工作流成功完成通知"""
    content = (
        f"**工作流**：{workflow}  \n"
        f"**写入成功**：{inserted} 条  \n"
        f"**幂等跳过**：{skipped} 条  \n"
    )
    await send_dingtalk_alert(f"✅ {workflow} 完成", content, level="info")
