#!/usr/bin/env bash
# 事务隔离可控样例（需要 docker compose 环境已启动：docker compose up -d）。
#
#   样例 1  未提交时查询    —— 采集“写到一半”暂停，查询只返回已提交数据
#   样例 2  分页中途提交    —— 同一快照内 total / 明细 / next_offset 保持一致
#   样例 3  提交前退出      —— 未提交事务随进程退出回滚，记录消失
#
# 所有样例也可以在 http://localhost:8000/docs 的 demo 分组里手动逐步执行。
set -euo pipefail
cd "$(dirname "$0")/.."
API="${API:-http://localhost:8000}"

# 从 stdin 的 JSON 里取一个字段：jget key
jget() { python3 -c "import json,sys; print(json.load(sys.stdin)['$1'])"; }

echo "==================================================================="
echo "样例 1：采集写到一半暂停（未提交）时，查询仍只返回已提交数据"
echo "==================================================================="
echo "-- POST /demo/uncommitted/start?rows=5 （BEGIN + 写入 5 行，不提交）"
curl -fsS -X POST "$API/demo/uncommitted/start?rows=5" | python3 -m json.tool
echo "-- GET /events?path=/demo/uncommitted  →  应为 total=0"
curl -fsS "$API/events?path=/demo/uncommitted" | python3 -m json.tool
echo "-- GET /demo/uncommitted/status  →  写事务内 5 行 vs 查询可见 0 行"
curl -fsS "$API/demo/uncommitted/status" | python3 -m json.tool
echo "-- 普通查询不受暂停写入影响（此前已提交数据照常返回，读写未串行化）"
curl -fsS "$API/events?limit=3" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print('total=%d items=%d' % (d['total'], len(d['items'])))"
echo "-- POST /demo/uncommitted/commit  →  提交后立即可见"
curl -fsS -X POST "$API/demo/uncommitted/commit" | python3 -m json.tool
curl -fsS "$API/events?path=/demo/uncommitted" \
  | python3 -c "import json,sys; print('total=%d' % json.load(sys.stdin)['total'])"

echo
echo "==================================================================="
echo "样例 2：分页中途有提交，同一快照内 total/明细/next_offset 保持一致"
echo "==================================================================="
SID=$(curl -fsS -X POST "$API/demo/snapshot/start" | jget snapshot_id)
T0=$(curl -fsS "$API/demo/snapshot/$SID?limit=2" \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['page']['total'])")
echo "-- 快照 $SID 已按住，开始时 total=$T0"
echo "-- 快照保持期间，另一连接提交 3 行新数据（模拟分页中途的采集提交）"
curl -fsS -X POST "$API/demo/uncommitted/start?rows=3" >/dev/null
curl -fsS -X POST "$API/demo/uncommitted/commit" >/dev/null
T1=$(curl -fsS "$API/demo/snapshot/$SID?limit=2" | python3 -c "import json,sys; print(json.load(sys.stdin)['page']['total'])")
echo "-- 提交后同一快照内再查：total=$T1（应等于 $T0，快照未被污染）"
curl -fsS "$API/demo/snapshot/$SID?limit=2&offset=0" | python3 -m json.tool
curl -fsS -X POST "$API/demo/snapshot/$SID/close" | python3 -m json.tool
echo "-- 新发起的查询能看到提交后的总数"
curl -fsS "$API/events?limit=1" \
  | python3 -c "import json,sys; print('total=%d' % json.load(sys.stdin)['total'])"
[ "$T0" = "$T1" ] && echo "样例 2 通过：快照内计数未受中途提交影响" \
                  || { echo "样例 2 失败"; exit 1; }

echo
echo "==================================================================="
echo "样例 3：提交前进程退出，未提交记录消失"
echo "==================================================================="
if ! command -v docker >/dev/null 2>&1; then
  echo "（当前环境无 docker，跳过需重启容器的样例 3；"
  echo " 在 compose 主机上运行本脚本即可完整演示）"
  echo
  echo "样例 1/2 已完成。demo 行 source_code=demo-hold、"
  echo "path=/demo/uncommitted，不影响正常采集与筛选。"
  exit 0
fi
BEFORE=$(curl -fsS "$API/events?path=/demo/uncommitted" | python3 -c "import json,sys; print(json.load(sys.stdin)['total'])")
echo "-- 当前已提交 /demo/uncommitted 行数：$BEFORE"
curl -fsS -X POST "$API/demo/uncommitted/start?rows=4" >/dev/null
echo "-- 已写入 4 条未提交行；现在重启 collector（进程退出 = 事务回滚）"
docker compose restart collector >/dev/null
for _ in $(seq 1 60); do
  curl -fsS "$API/health" >/dev/null 2>&1 && break
  sleep 1
done
AFTER=$(curl -fsS "$API/events?path=/demo/uncommitted" | python3 -c "import json,sys; print(json.load(sys.stdin)['total'])")
echo "-- 重启后 /demo/uncommitted 行数：$AFTER（应等于 $BEFORE，未提交的 4 行已回滚）"
[ "$BEFORE" = "$AFTER" ] && echo "样例 3 通过：未提交记录随进程退出消失" \
                         || { echo "样例 3 失败"; exit 1; }

echo
echo "全部样例完成。demo 行 source_code=demo-hold、path=/demo/uncommitted，"
echo "不影响正常采集与筛选；docker compose down -v 可清空全部演示数据。"
