"""Controllable demo hooks for the transaction-isolation contract.

These endpoints exist so the isolation behaviour can be demonstrated from
/docs and verified by tests. They never touch the collector's writer
connection and never interfere with per-request read snapshots:

* ``/demo/uncommitted/*`` holds a real, uncommitted write transaction on
  its own dedicated connection — the "collection paused mid-write" sample.
  While it is held, every query endpoint keeps serving previously
  committed data and never sees the held rows. ``commit`` makes the rows
  visible atomically; ``rollback`` makes them vanish, exactly like a
  process exit before commit.
* ``/demo/snapshot/*`` pins a read snapshot across requests — the "query
  paused mid-read" sample. Commits from other connections land while the
  snapshot is held, yet every read through the pinned snapshot still sees
  the same totals, proving that one response comes from one snapshot and
  that readers never block the writer.

Demo rows use ``source_code="demo-hold"`` and negative dev/inode, so they
can never collide with real collected files. Remove the router include in
:mod:`app.main` to disable the hooks entirely.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from . import storage

DB_PATH = os.getenv("DB_PATH", "/data/warehouse.db")
DEMO_SOURCE = "demo-hold"
_SNAPSHOT_TTL_SECONDS = 300.0

router = APIRouter(prefix="/demo", tags=["demo"])

# All demo state is guarded by this lock. It never serialises normal
# queries or ingestion — only the demo hooks themselves.
_lock = threading.Lock()
_write_conn: Optional[sqlite3.Connection] = None
_write_rows = 0
_snapshots: dict[str, tuple[sqlite3.Connection, float]] = {}


def _insert_demo_rows(conn: sqlite3.Connection, rows: int) -> None:
    """Insert ``rows`` demo events inside the caller's open transaction."""
    start = conn.execute(
        "SELECT COALESCE(MAX(byte_offset), -1) + 1 AS nxt FROM log_events "
        "WHERE dev = -1 AND inode = -1 AND source_code = ?",
        (DEMO_SOURCE,),
    ).fetchone()["nxt"]
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    for i in range(rows):
        raw = json.dumps({
            "time": now,
            "path": "/demo/uncommitted",
            "status": 599,
            "method": "DEMO",
            "note": "held by /demo/uncommitted; invisible until commit",
        })
        conn.execute(
            """INSERT INTO log_events
                   (ts, path, status, method, source_code,
                    dev, inode, byte_offset, raw_line)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (now, "/demo/uncommitted", 599, "DEMO", DEMO_SOURCE,
             -1, -1, start + i, raw),
        )


def _close_quietly(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass
    conn.close()


def _gc_snapshots() -> None:
    """Release held snapshots whose owner forgot to close them (TTL)."""
    now = time.monotonic()
    stale = [sid for sid, (_, ts) in _snapshots.items()
             if now - ts > _SNAPSHOT_TTL_SECONDS]
    for sid in stale:
        conn, _ = _snapshots.pop(sid)
        _close_quietly(conn)


@router.post("/uncommitted/start",
             summary="按住一个未提交写事务（采集写到一半暂停）")
def start_uncommitted(rows: int = Query(3, ge=1, le=100)):
    """BEGIN a write transaction on a dedicated connection, insert ``rows``
    demo events and keep the transaction **open**. Until it is committed,
    no query endpoint can see these rows — but queries keep working."""
    global _write_conn, _write_rows
    with _lock:
        if _write_conn is not None:
            raise HTTPException(
                status_code=409,
                detail="already holding an uncommitted transaction; "
                       "commit or roll it back first",
            )
        conn = storage.connect(DB_PATH)
        try:
            conn.execute("BEGIN IMMEDIATE")
            _insert_demo_rows(conn, rows)
        except Exception:
            _close_quietly(conn)
            raise
        _write_conn = conn
        _write_rows = rows
    return {
        "holding": True,
        "held_uncommitted_rows": rows,
        "hint": "GET /events?path=/demo/uncommitted still sees none of these "
                "rows; finish with /demo/uncommitted/commit or "
                "/demo/uncommitted/rollback",
    }


@router.get("/uncommitted/status",
            summary="对比写事务内视角与查询（已提交）视角")
def uncommitted_status():
    """Show what the open write transaction sees versus what any query
    (committed-only snapshot) sees, at the same instant."""
    with _lock:
        conn = _write_conn
        held = _write_rows
        in_txn: Optional[int] = None
        if conn is not None:
            in_txn = conn.execute(
                "SELECT COUNT(*) AS c FROM log_events WHERE source_code = ?",
                (DEMO_SOURCE,),
            ).fetchone()["c"]
    with storage.read_snapshot(DB_PATH) as snap:
        committed = snap.execute(
            "SELECT COUNT(*) AS c FROM log_events WHERE source_code = ?",
            (DEMO_SOURCE,),
        ).fetchone()["c"]
    return {
        "holding": conn is not None,
        "held_uncommitted_rows": held if conn is not None else 0,
        "writer_transaction_rows": in_txn,
        "committed_visible_to_queries": committed,
    }


@router.post("/uncommitted/commit",
             summary="提交被按住的事务（行原子可见）")
def commit_uncommitted():
    global _write_conn, _write_rows
    with _lock:
        if _write_conn is None:
            raise HTTPException(status_code=409,
                                detail="no held transaction")
        _write_conn.execute("COMMIT")
        _write_conn.close()
        _write_conn = None
        rows = _write_rows
        _write_rows = 0
    return {"holding": False, "committed_rows": rows}


@router.post("/uncommitted/rollback",
             summary="回滚被按住的事务（等价于提交前进程退出）")
def rollback_uncommitted():
    global _write_conn, _write_rows
    with _lock:
        if _write_conn is None:
            raise HTTPException(status_code=409,
                                detail="no held transaction")
        _write_conn.execute("ROLLBACK")
        _write_conn.close()
        _write_conn = None
        rows = _write_rows
        _write_rows = 0
    return {
        "holding": False,
        "rolled_back_rows": rows,
        "note": "restarting the collector container while holding has the "
                "same effect: the uncommitted rows never existed",
    }


@router.post("/snapshot/start",
             summary="按住一个读快照（查询读到一半暂停）")
def start_snapshot():
    """Open a read snapshot and pin it across requests. Commits made after
    this point stay invisible to every read through the snapshot."""
    with _lock:
        _gc_snapshots()
        sid = uuid.uuid4().hex[:12]
        conn = storage.connect_reader(DB_PATH)
        conn.execute("BEGIN")
        total = conn.execute(
            "SELECT COUNT(*) AS c FROM log_events"
        ).fetchone()["c"]
        _snapshots[sid] = (conn, time.monotonic())
    return {
        "snapshot_id": sid,
        "total_at_start": total,
        "hint": "commit more rows (via /demo/uncommitted/* or normal "
                "traffic), then GET /demo/snapshot/{id} — totals stay "
                "frozen; close it with POST /demo/snapshot/{id}/close",
    }


@router.get("/snapshot/{snapshot_id}",
            summary="在按住的快照内分页查询（total/明细/next_offset 一致）")
def read_held_snapshot(snapshot_id: str,
                       limit: int = Query(5, ge=1, le=1000),
                       offset: int = Query(0, ge=0)):
    with _lock:
        _gc_snapshots()
        entry = _snapshots.get(snapshot_id)
        if entry is None:
            raise HTTPException(status_code=404,
                                detail="snapshot not found or expired")
        conn, _ = entry
        _snapshots[snapshot_id] = (conn, time.monotonic())
        page = storage.query_events(conn, limit=limit, offset=offset)
    return {
        "snapshot_id": snapshot_id,
        "page": page,
        "note": "total/items/next_offset all come from the pinned snapshot; "
                "commits made after /demo/snapshot/start are invisible here",
    }


@router.post("/snapshot/{snapshot_id}/close",
             summary="释放快照（读取资源）")
def close_snapshot(snapshot_id: str):
    with _lock:
        _gc_snapshots()
        entry = _snapshots.pop(snapshot_id, None)
        if entry is None:
            raise HTTPException(status_code=404,
                                detail="snapshot not found or expired")
        _close_quietly(entry[0])
    return {"closed": snapshot_id}


def cleanup() -> None:
    """Release every demo resource; called from the app lifespan shutdown."""
    global _write_conn, _write_rows
    with _lock:
        if _write_conn is not None:
            _close_quietly(_write_conn)
            _write_conn = None
            _write_rows = 0
        for conn, _ in _snapshots.values():
            _close_quietly(conn)
        _snapshots.clear()
