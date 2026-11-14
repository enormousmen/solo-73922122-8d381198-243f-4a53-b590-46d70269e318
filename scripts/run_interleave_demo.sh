#!/usr/bin/env bash
# 慢读交错验证：在可控慢读下验证
#   1) 慢读期间补传上传/新游标创建不被阻塞；
#   2) 同一游标并发翻页 FIFO 串行、不重读不跳行、行序不变；
#   3) 慢读期间合并切换，在途页完整返回、旧文件读完前不回收；
#   4) 慢读期间关闭游标，在途页明确失败且不推进、读者释放后才回收。
set -euo pipefail

cd "$(dirname "$0")/.."

COMPOSE="docker compose"
if ! docker compose version >/dev/null 2>&1; then
  COMPOSE="docker-compose"
fi
EXEC="$COMPOSE exec -T lakehouse"

wait_health() {
  for _ in $(seq 1 60); do
    if $EXEC python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8000/health')" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "服务未在预期时间内就绪" >&2
  exit 1
}

echo "########## 构建并启动服务 ##########"
$COMPOSE up -d --build
wait_health

echo "########## 慢读交错验证（INTERLEAVE_SLOW_SECONDS 控制慢读秒数）##########"
$EXEC env INTERLEAVE_SLOW_SECONDS="${INTERLEAVE_SLOW_SECONDS:-0.8}" \
  python scripts/interleave_demo.py

echo "########## 交错验证完成 ✔  Swagger: http://localhost:8000/docs ##########"
