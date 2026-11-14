# Nginx 访问日志持续入仓（FastAPI + SQLite）

把 Nginx 的 JSON 行访问日志从共享只读目录**持续、精确一次（exactly-once）**
地采集进 SQLite，并提供按路径、状态码分页检索故障请求的 HTTP API。

## 为什么不会漏行 / 重记 / 错用偏移

| 风险 | 设计对策 |
| --- | --- |
| 轮转后把旧偏移用到新文件 | 文件身份用 `(st_dev, st_ino)`，**不是文件名**。rename 后旧 inode 与新 inode 各自独立追踪 |
| 旧文件轮转后仍被短暂写入 | 旧 inode 继续按已提交偏移采集到 EOF；归档永不删除、不截断 |
| 读到写了一半的行 | 只入仓以 `\n` 结尾的完整行；未换行尾部保持原偏移，下轮重读 |
| 进程在写入中途/前后退出 | 行数据与“下一字节偏移”在**同一个 SQLite 事务**提交，重启从最后提交点续采 |
| 查询读到采集未提交的记录 | 写连接专属于采集；每次查询响应用**独立连接的读事务**，只能看到已提交数据 |
| 分页 total 与明细对不上 | 同一响应的 COUNT、明细、`next_offset` 在**同一个读快照**内完成 |
| 内容相同的两行 | 唯一性只约束物理位置 `(dev,inode,source_code,byte_offset)`，不含内容，重复内容各自成行 |
| JSON 坏行 | 隔离进 `malformed_lines` 表，不阻断后续行 |

## 事务隔离设计（查询 vs 采集）

| 要求 | 实现 |
| --- | --- |
| 只读已提交数据 | 采集独占**唯一写连接**；`/events`、`/events/{id}`、`/malformed`、`/ingest/progress`、`/health` 的每次响应都另开独立只读连接（`PRAGMA query_only=ON`），全部 SELECT 在单个读事务内完成 |
| 同一响应同一快照 | 读事务在 WAL 下钉住一致快照：分页 `total`、明细、`next_offset` 必然一致；分页中途的采集提交不影响本次响应 |
| 读写不串行化 | WAL 读写并发：采集写到一半暂停，查询照常返回已提交数据；查询读到一半暂停，采集照常提交（无全局读写锁） |
| 提交/回滚不污染其他请求 | 连接不共享：任何连接的 COMMIT/ROLLBACK 只影响自己；请求之间互不可见 |
| 读取资源释放 | `read_snapshot` 上下文管理器保证响应结束或异常都结束读事务并关闭连接；demo 快照另有 TTL 与显式 close |

## 目录结构

```
.
├── docker-compose.yml      # nginx + collector + traffic 一键启动
├── collector/              # FastAPI + SQLite 采集/查询服务
│   ├── app/storage.py      # 表结构、写事务、读快照、查询
│   ├── app/ingest.py       # 目录扫描、inode 识别、完整行切分、续采
│   ├── app/demo.py         # 事务隔离可控演示钩子（/demo/*）
│   └── app/main.py         # API（/docs 可用）
├── nginx/                  # JSON 行日志格式 + 可控轮转脚本
├── traffic/                # 持续请求生成器（含 4xx/5xx）
├── scripts/                # verify.sh 校验、rotate.sh 轮转、demo_tx_isolation.sh 隔离演示
└── tests/                  # 67 项单元/集成断言 + 端到端模拟
```

## 快速开始

```bash
docker compose build
docker compose up -d
# 实时看日志
docker compose logs -f collector
```

| 服务 | 地址 | 说明 |
| --- | --- | --- |
| API 文档 Swagger | http://localhost:8000/docs | 直接在页面上试所有接口 |
| 示例站点 | http://localhost:8080/ | Nginx，访问即产生 JSON 日志 |
| OpenAPI JSON | http://localhost:8000/openapi.json | |

### 可控轮转（手动）

```bash
# 随时手动轮转：rename 成唯一归档名 + nginx 重开同名新日志
docker compose exec nginx /usr/local/bin/rotate-nginx-log.sh

# 自定义归档后缀
docker compose exec nginx ROTATE_NAME=incident-1 \
    /usr/local/bin/rotate-nginx-log.sh
```

### 自动周期轮转（演示轮转与采集交错）

```bash
# 每 15 秒自动轮转一次
AUTO_ROTATE_INTERVAL=15 docker compose up -d
# 恢复纯手动
docker compose up -d --force-recreate nginx
```

轮转后共享目录里同时存在 `access.log` 与所有
`access.log.<时间戳>.log` 归档，采集器分别追踪、全部保留。

## 常用查询

```bash
# 采集进度（每个文件的 offset / size、是否采完）
curl -s localhost:8000/ingest/progress | python3 -m json.tool

# 所有 5xx 故障请求，分页
curl -s "localhost:8000/events?status_min=500&status_max=599&limit=50"

# 某路径的 404
curl -s "localhost:8000/events?path=/missing/page&status=404"

# 只看某个归档来源
curl -s "localhost:8000/events?source_code=access.log.20261113T100000Z.log"

# 翻页
curl -s "localhost:8000/events?limit=100&offset=200"

# 立即触发一次采集（默认每 0.5s 自动轮询）
curl -s -X POST localhost:8000/ingest/poll-now

# 无法解析的坏行
curl -s localhost:8000/malformed
```

## 事务隔离可控样例

三个样例都可以在 http://localhost:8000/docs 的 **demo** 分组里逐步执行，
也可以一键运行（需要 compose 环境已启动）：

```bash
./scripts/demo_tx_isolation.sh
```

### 样例 1：未提交时查询（采集写到一半暂停）

1. `POST /demo/uncommitted/start?rows=5` —— 在独立连接上 `BEGIN` 并写入 5 行，
   **保持不提交**。（SQLite 单写者：此时后台采集等锁暂停，正是“写到一半”，
   提交/回滚后立即恢复。）
2. `GET /events?path=/demo/uncommitted` → `total=0`；
   `GET /events` 照常返回此前已提交数据。
3. `GET /demo/uncommitted/status` → 写事务内 5 行 vs 查询可见 0 行。
4. `POST /demo/uncommitted/commit` → 再查 `total=5`，原子可见。

### 样例 2：分页中途提交（查询读到一半暂停）

1. `POST /demo/snapshot/start` → 记下 `snapshot_id` 与 `total_at_start`。
2. 提交新行（用样例 1 的 commit，或等正常流量入仓）——
   **采集在查询暂停期间照常提交**。
3. `GET /demo/snapshot/{snapshot_id}?limit=10` → `total` 仍等于
   `total_at_start`，明细数量与 `next_offset` 自洽：同一快照不受中途提交影响。
4. `POST /demo/snapshot/{snapshot_id}/close` 释放读取资源
   （5 分钟未关闭也会自动回收）。

### 样例 3：提交前退出

1. `POST /demo/uncommitted/start?rows=4`。
2. `docker compose restart collector` —— 进程退出，未提交事务随连接消亡回滚。
3. 再查 `GET /events?path=/demo/uncommitted` —— 没有新增行，与退出前一致。

demo 行 `source_code=demo-hold`、`path=/demo/uncommitted`，不影响正常采集与筛选；
`docker compose down -v` 可清空全部演示数据。demo 钩子仅用于演示/测试，
生产部署在 `app/main.py` 移除 `app.include_router(demo_router)` 一行即可关闭。

## API 一览

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/health` | 存活、计数、采集线程状态 |
| GET | `/ingest/progress` | 各来源文件 offset/size/是否完成 |
| POST | `/ingest/poll-now` | 立即执行一轮采集 |
| GET | `/events` | 分页取数，支持 `path`、`status`、`status_min/max`、`source_code`、`limit`、`offset` |
| GET | `/events/{id}` | 单条事件（含原始 JSON 行 `raw_line`） |
| GET | `/malformed` | 坏行隔离区 |
| POST | `/demo/uncommitted/start` | （演示）按住一个未提交写事务（`rows` 行写入后不提交） |
| GET | `/demo/uncommitted/status` | （演示）写事务内计数 vs 查询可见计数 |
| POST | `/demo/uncommitted/commit` | （演示）提交被按住的事务 |
| POST | `/demo/uncommitted/rollback` | （演示）回滚被按住的事务（等价于提交前退出） |
| POST | `/demo/snapshot/start` | （演示）按住一个读快照，返回 `snapshot_id` |
| GET | `/demo/snapshot/{id}` | （演示）在按住的快照内分页查询 |
| POST | `/demo/snapshot/{id}/close` | （演示）释放快照 |

## 数据模型

```
log_events(id, ts, path, status, method, source_code,
           dev, inode, byte_offset, raw_line, ingested_at)
ingest_state(dev, inode, source_code, path, offset, last_seen)  -- 续采位置
malformed_lines(..., raw_line, error)                            -- 坏行隔离
```

- `source_code`：来源文件代号（当前文件名/归档名）。
- `byte_offset`：该行在其来源文件中的起始字节位置。
- 唯一索引 `(dev, inode, source_code, byte_offset)` 是防重的最后防线；
  内容不在键中，所以两行完全相同也分别保留。
- SQLite 运行在 `WAL + synchronous=FULL`：提交即落盘，读不阻塞写、写不阻塞读
  （查询侧的快照隔离见上文「事务隔离设计」）。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `LOG_DIR` | `/logs` | 共享日志目录（compose 中以 `:ro` 只读挂载） |
| `DB_PATH` | `/data/warehouse.db` | SQLite 数据库（独立可写卷） |
| `POLL_INTERVAL` | `0.5` | 轮询间隔秒 |
| `LOG_LEVEL` | `INFO` | 日志级别 |
| `AUTO_ROTATE_INTERVAL` | 空 | 仅 nginx 容器；非空时按秒自动轮转 |

## 本地测试（无需 Docker）

```bash
python3 -m venv --without-pip .venv && . .venv/bin/activate
# 若 venv 内无 pip：python get-pip.py
pip install -r collector/requirements.txt httpx pyyaml
python -I tests/test_collector.py   # 67 项：部分行/轮转/崩溃恢复/重复行/API/事务隔离
python -I tests/e2e_sim.py          # 完整服务启停 + 轮转 + 重启续采 + 隔离样例 E2E
```

覆盖的关键场景：未换行尾部等待补齐、rename 轮转双 inode、旧 inode 延迟写入、
20 轮随机“进程退出”后续采不漏不重、相同内容多行保留、坏行隔离、分页与过滤、
未提交数据对查询不可见、单响应快照一致（total/明细/next_offset）、
读写并发互不阻塞、提交前退出回滚不留痕。

## 故障排查

```bash
docker compose ps
docker compose logs --tail=100 collector
docker compose exec nginx ls -li /var/log/nginx/json   # 看 inode 与归档
docker compose exec collector sh -c 'ls -li /logs'      # 只读视角
# 入库数与文件总字节核对
curl -s localhost:8000/ingest/progress
```
