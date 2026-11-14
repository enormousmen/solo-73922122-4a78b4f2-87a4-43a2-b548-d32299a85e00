"""FastAPI service: continuous ingestion + query API.

Transaction isolation
---------------------
The collector loop owns the single *writer* connection. Every query
endpoint serves its whole response from one short-lived read snapshot
(see :func:`app.storage.read_snapshot`): committed data only, one
consistent snapshot per response (``total``/items/``next_offset`` always
agree), readers and writer never serialised against each other, and all
read resources released when the request ends or fails. The ``/demo/*``
router provides controllable hooks that demonstrate this contract.

Endpoints
---------
GET  /health                 liveness + counters
GET  /ingest/progress        per-file collection progress (offset/size)
POST /ingest/poll-now        force an immediate collection pass
GET  /events                 paginated events, filter by path / status /
                             status range / source code
GET  /events/{event_id}      single event with the raw JSON line
GET  /malformed              quarantined lines that failed JSON parsing
/demo/*                      controllable isolation demos (held
                             uncommitted writes, pinned read snapshots)
"""

from __future__ import annotations

import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from . import storage
from .demo import cleanup as demo_cleanup
from .demo import router as demo_router
from .ingest import CollectorLoop, poll_once

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("collector.api")

DB_PATH = os.getenv("DB_PATH", "/data/warehouse.db")
LOG_DIR = os.getenv("LOG_DIR", "/logs")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "0.5"))


class Health(BaseModel):
    status: str
    log_dir: str
    db_path: str
    poll_interval_seconds: float
    collector_running: bool
    total_polls: int
    total_ingested: int
    counts: dict


class FileProgress(BaseModel):
    source_code: str
    dev: int
    inode: int
    path: str
    offset: int
    size: Optional[int] = None
    complete: bool
    last_seen: str


class ProgressResponse(BaseModel):
    collector_running: bool
    total_polls: int
    total_ingested: int
    last_poll: Optional[dict] = None
    counts: dict
    files: list[FileProgress]


class Event(BaseModel):
    id: int
    ts: Optional[str]
    path: Optional[str]
    status: Optional[int]
    method: Optional[str]
    source_code: str
    dev: int
    inode: int
    byte_offset: int
    raw_line: str
    ingested_at: str


class EventPage(BaseModel):
    total: int
    limit: int
    offset: int
    next_offset: Optional[int]
    items: list[Event]


state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    # The one and only writer connection. It is used exclusively by the
    # collector loop and POST /ingest/poll-now — never by query handlers,
    # so an open write transaction can never leak uncommitted rows (or a
    # commit/rollback) into a response.
    conn = storage.connect(DB_PATH)
    storage.init_db(conn)

    loop = CollectorLoop(conn, LOG_DIR, interval=POLL_INTERVAL)
    thread = threading.Thread(target=loop.run_forever, name="collector",
                              daemon=True)
    thread.start()
    state.update(conn=conn, loop=loop, thread=thread)
    log.info("API ready; watching %s, db %s", LOG_DIR, DB_PATH)
    try:
        yield
    finally:
        log.info("shutdown requested, stopping collector loop...")
        loop.request_stop()
        thread.join(timeout=10)
        # Release any demo transaction/snapshot still being held.
        demo_cleanup()
        # A checkpoint persists WAL frames into the main db file on exit.
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            log.exception("checkpoint on shutdown failed")
        conn.close()
        log.info("shutdown complete")


app = FastAPI(
    title="Nginx 访问日志入仓 API",
    description=(
        "持续采集共享只读目录中的 JSON 行 Nginx 日志，按 (设备, inode) "
        "识别文件、行与续采偏移同事务提交，支持按路径与状态码分页查询故障请求。"
        "查询只读已提交数据：每个响应来自同一读快照，分页 total/明细/next_offset "
        "一致；读写互不阻塞。demo 分组提供事务隔离的可控演示样例。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)
app.include_router(demo_router)


def _conn():
    """The single writer connection — ingest paths only, never queries."""
    return state["conn"]


@app.get("/health", response_model=Health, tags=["meta"])
def health():
    loop: CollectorLoop = state["loop"]
    with storage.read_snapshot(DB_PATH) as conn:
        db_counts = storage.counts(conn)
    return Health(
        status="ok",
        log_dir=LOG_DIR,
        db_path=DB_PATH,
        poll_interval_seconds=POLL_INTERVAL,
        collector_running=threading.main_thread().is_alive() and not loop._stop,
        total_polls=loop.total_polls,
        total_ingested=loop.total_ingested,
        counts=db_counts,
    )


@app.get("/ingest/progress", response_model=ProgressResponse, tags=["ingest"])
def progress():
    loop: CollectorLoop = state["loop"]
    # Every piece of database content in this response (committed resume
    # offsets and table counts) comes from one read snapshot.
    with storage.read_snapshot(DB_PATH) as conn:
        rows = storage.all_states(conn)
        db_counts = storage.counts(conn)
    # Current sizes from the live directory, read-only (filesystem state,
    # not database content, so intentionally read outside the snapshot).
    current: dict[tuple[int, int], int] = {}
    try:
        for name in os.listdir(LOG_DIR):
            try:
                st = os.stat(os.path.join(LOG_DIR, name))
            except FileNotFoundError:
                continue
            current[(st.st_dev, st.st_ino)] = st.st_size
    except FileNotFoundError:
        pass

    files = []
    for row in rows:
        size = current.get((row["dev"], row["inode"]))
        files.append(FileProgress(
            source_code=row["source_code"],
            dev=row["dev"],
            inode=row["inode"],
            path=row["path"],
            offset=row["offset"],
            size=size,
            complete=(size is not None and row["offset"] >= size),
            last_seen=row["last_seen"],
        ))
    return ProgressResponse(
        collector_running=not loop._stop,
        total_polls=loop.total_polls,
        total_ingested=loop.total_ingested,
        last_poll=loop.last_result,
        counts=db_counts,
        files=files,
    )


@app.post("/ingest/poll-now", tags=["ingest"])
def poll_now():
    """Force an immediate scan/ingest pass (useful for tests/demos).

    Runs on the writer connection, serialised against the background loop
    by ``write_lock``; concurrent queries are unaffected (WAL readers)."""
    loop: CollectorLoop = state["loop"]
    with loop.write_lock:
        return poll_once(_conn(), LOG_DIR)


@app.get("/events", response_model=EventPage, tags=["query"])
def list_events(
    path: Optional[str] = Query(None, description="精确路径，例如 /api/orders"),
    status: Optional[int] = Query(None, description="精确状态码，例如 500"),
    status_min: Optional[int] = Query(None, description="状态码下界，例如 400"),
    status_max: Optional[int] = Query(None, description="状态码上界，例如 599"),
    source_code: Optional[str] = Query(None, description="来源文件代号，例如 access.log.20261113T100000.log"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """按路径、状态码分页取数。查故障请求示例：
    ``/events?status_min=500&status_max=599&limit=50``。

    整个响应（total、items、next_offset）来自同一个只读已提交数据的
    快照；采集中途提交不会改变本次响应。"""
    with storage.read_snapshot(DB_PATH) as conn:
        return storage.query_events(
            conn,
            path=path,
            status=status,
            status_min=status_min,
            status_max=status_max,
            source_code=source_code,
            limit=limit,
            offset=offset,
        )


@app.get("/events/{event_id}", response_model=Event, tags=["query"])
def get_event(event_id: int):
    with storage.read_snapshot(DB_PATH) as conn:
        row = conn.execute(
            "SELECT * FROM log_events WHERE id = ?", (event_id,)
        ).fetchone()
        if row is None:
            # The snapshot is rolled back and its connection closed by the
            # context manager even on this error path.
            raise HTTPException(status_code=404, detail="event not found")
        return dict(row)


@app.get("/malformed", tags=["query"])
def list_malformed(limit: int = Query(100, ge=1, le=1000),
                   offset: int = Query(0, ge=0)):
    with storage.read_snapshot(DB_PATH) as conn:
        return storage.query_malformed(conn, limit=limit, offset=offset)
