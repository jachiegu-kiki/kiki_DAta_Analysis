#!/usr/bin/env bash
set -euo pipefail
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║     广州前途财务日报系统 · 一键部署 v3              ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# ─── 1. Docker ───
info "检查 Docker..."
command -v docker >/dev/null 2>&1 || err "未检测到 Docker"
docker info >/dev/null 2>&1 || err "Docker 未运行"
if docker compose version >/dev/null 2>&1; then DC="docker compose"; else DC="docker-compose"; fi
ok "Docker 正常"

# ─── 2. .env ───
if [ ! -f .env ]; then
    cp .env.example .env
    SECRET=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))" 2>/dev/null || openssl rand -base64 32)
    API_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(24))" 2>/dev/null || openssl rand -base64 24)
    sed -i "s|CHANGE_ME_TO_RANDOM_32_CHARS_STRING|${SECRET}|g" .env
    sed -i "s|CHANGE_ME_INTERNAL_KEY|${API_KEY}|g" .env
    ok ".env 已创建（密钥自动生成）"
    warn "请确认数据路径："
    grep "PATH=" .env | while read line; do echo "    $line"; done
    echo ""
    read -p "$(echo -e "${YELLOW}按 Enter 继续，或 Ctrl+C 退出编辑 .env...${NC}")" _
else
    ok ".env 已存在"
fi

source .env 2>/dev/null || true
HTTP_PORT="${HTTP_PORT:-8771}"
PREDATA_PATH="${PREDATA_PATH:-/kiki/KiKiAutoPullDataScript/PreData}"
PULLDATA_PATH="${PULLDATA_PATH:-/kiki/n8n_data/PullData}"

# ─── 3. 检查数据目录 ───
[ -d "$PREDATA_PATH" ]  && ok "PreData:  $PREDATA_PATH"  || warn "PreData 不存在: $PREDATA_PATH"
[ -d "$PULLDATA_PATH" ] && ok "PullData: $PULLDATA_PATH" || warn "PullData 不存在: $PULLDATA_PATH"

# ─── 4. 构建启动 ───
info "构建并启动容器..."
$DC down --remove-orphans 2>/dev/null || true
$DC build --quiet
$DC up -d

# ─── 5. 等待就绪 ───
info "等待服务启动..."
for i in $(seq 1 30); do
    curl -sf http://localhost:${HTTP_PORT}/health >/dev/null 2>&1 && break
    sleep 2
done
curl -sf http://localhost:${HTTP_PORT}/health >/dev/null 2>&1 && ok "服务就绪" || warn "健康检查超时"

# ─── 6. 管理员账号 ───
info "检查管理员账号..."
sleep 3
ADMIN_EXISTS=$($DC exec -T postgres psql -U qiantu -d qiantu_finance -tAc \
    "SELECT COUNT(*) FROM dim_users WHERE username='admin'" 2>/dev/null || echo "0")
if [ "$ADMIN_EXISTS" = "0" ] || [ -z "$ADMIN_EXISTS" ]; then
    $DC exec -T api python3 -c "
import bcrypt, asyncio
from sqlalchemy import text
from app.core.database import engine
async def f():
    pwd = bcrypt.hashpw(b'admin@qiantu2026', bcrypt.gensalt(12)).decode()
    async with engine.begin() as c:
        r = await c.execute(text(\"SELECT COUNT(*) FROM dim_users WHERE username='admin'\"))
        if r.scalar() == 0:
            await c.execute(text(\"INSERT INTO dim_users (username,hashed_password,role,is_active) VALUES ('admin',:p,'ADMIN',true)\"),{'p':pwd})
asyncio.run(f())
" 2>/dev/null && ok "管理员已创建 (admin / admin@qiantu2026)" || warn "管理员创建跳过"
else
    ok "管理员已存在"
fi

# ─── 7. 连接 n8n 到同一网络 ───
N8N_CONTAINER=$(docker ps --format '{{.Names}}' | grep -i n8n | head -1)
NETWORK=$(docker network ls --format '{{.Name}}' | grep qiantu_net | head -1)
if [ -n "$N8N_CONTAINER" ] && [ -n "$NETWORK" ]; then
    docker network connect "$NETWORK" "$N8N_CONTAINER" 2>/dev/null && \
        ok "n8n ($N8N_CONTAINER) 已连接" || ok "n8n 已在网络中"
fi

# ─── 8. 首次 ETL ───
info "构建 ETL 并执行首次数据同步..."
$DC build etl --quiet 2>/dev/null
$DC run --rm etl 2>&1 | tail -30
ok "ETL 同步完成"

# ─── 9. Cron ───
ETL_HOUR="${ETL_CRON_HOUR:-18}"
ETL_MIN="${ETL_CRON_MINUTE:-05}"
CRON_CMD="${ETL_MIN} ${ETL_HOUR} * * * cd ${SCRIPT_DIR} && ${DC} run --rm etl >> ${SCRIPT_DIR}/etl/sync.log 2>&1"
(crontab -l 2>/dev/null | grep -v "qiantu.*etl" || true; echo "$CRON_CMD") | crontab -
ok "Cron: 每天 ${ETL_HOUR}:$(printf '%02d' ${ETL_MIN})"

# ─── 10. 结果 ───
LOCAL_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo "localhost")
PUBLIC_IP=$(curl -sf --connect-timeout 2 http://100.100.100.200/latest/meta-data/eipv4 2>/dev/null || \
            curl -sf --connect-timeout 2 ifconfig.me 2>/dev/null || echo "")
INT_KEY=$(grep -oP '^INTERNAL_API_KEY=\K.*' .env 2>/dev/null || echo "见.env")

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║                  部署完成！                         ║"
echo "╠══════════════════════════════════════════════════════╣"
echo -e "║  ${GREEN}看板${NC}: http://${LOCAL_IP}:${HTTP_PORT}"
echo -e "║  ${GREEN}API${NC}:  http://${LOCAL_IP}:${HTTP_PORT}/api/docs"
[ -n "$PUBLIC_IP" ] && echo -e "║  ${GREEN}公网${NC}: http://${PUBLIC_IP}:${HTTP_PORT}"
echo -e "║  ${YELLOW}账号${NC}: admin / admin@qiantu2026"
echo "╠══════════════════════════════════════════════════════╣"
echo "║  n8n 配置：                                          ║"
echo -e "║  FASTAPI_BASE_URL = http://${LOCAL_IP}:${HTTP_PORT}"
echo -e "║  INTERNAL_API_KEY = ${INT_KEY}"
echo "╠══════════════════════════════════════════════════════╣"
echo -e "║  ${CYAN}阿里云安全组${NC}: 只放行 ${GREEN}${HTTP_PORT}/tcp${NC}"
echo "║  ETL: 每天 ${ETL_HOUR}:$(printf '%02d' ${ETL_MIN}) | 手动: docker compose run --rm etl"
echo "║  更新前端: 编辑 frontend/index.html 刷新浏览器"
echo "║  更新后端: 编辑 backend/app/*.py 自动热重载"
echo "║  更新ETL:  编辑 etl/daily_sync.py 下次cron生效"
echo "╚══════════════════════════════════════════════════════╝"
