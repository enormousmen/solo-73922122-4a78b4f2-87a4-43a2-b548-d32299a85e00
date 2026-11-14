"""SQLite persistence layer.

Durability contract
-------------------
Every ingested line is committed together with the *next* byte offset of its
source file in a single SQLite transaction. A crash can therefore only leave
the database in one of two states:

* the line and the advanced offset were both committed, or
* neither was committed.

On restart the collector resumes exactly at the last committed offset, which
guarantees no line is recorded twice and no line is skipped.

Isolation contract
------------------
Exactly **one writer connection** exists (owned by the collector loop and
``POST /ingest/poll-now``); HTTP queries never touch it. Every query
response is served from a short-lived **read snapshot** — a dedicated
connection that runs all of the response's SELECTs inside a single
deferred read transaction. Under WAL that transaction pins a consistent
snapshot of committed data, so:

* queries only ever see committed rows, never a half-written transaction
  (a row whose writer later rolls back is never observed);
* ``total``, the page items and ``next_offset`` of one response always
  come from the same snapshot, even if the collector commits mid-request;
* readers and the writer proceed concurrently — a writer paused
  mid-transaction does not block queries, and a query paused mid-read
  does not block commits (reads and writes are *not* serialised);
* a commit or rollback on one connection can never leak into another
  request, and every snapshot is ended and its connection closed when the
  request finishes or fails.

WAL + ``synchronous=FULL`` make committed data survive process/container
crashes and power loss while still allowing the API to read while the
ingestor is writing.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Any, Iterator, Optional

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS ingest_state (
    dev          INTEGER NOT NULL,
    inode        INTEGER NOT NULL,
    source_code  TEXT    NOT NULL,
    path         TEXT    NOT NULL,
    offset       INTEGER NOT NULL,
    last_seen    TEXT    NOT NULL,
    PRIMARY KEY (dev, inode)
);

CREATE TABLE IF NOT EXISTS log_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT,
    path        TEXT,
    status      INTEGER,
    method      TEXT,
    source_code TEXT    NOT NULL,
    dev         INTEGER NOT NULL,
    inode       INTEGER NOT NULL,
    byte_offset INTEGER NOT NULL,
    raw_line    TEXT    NOT NULL,
    ingested_at TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- Defence in depth: the same physical byte position of one source file can
-- be recorded once only. source_code (the file's archive name) is part of the
-- key so an inode number recycled after deletion can never collide with the
-- previous file's rows. Content is intentionally *not* part of this key, so
-- two lines with identical content are stored as separate rows.
CREATE UNIQUE INDEX IF NOT EXISTS ux_log_events_position
    ON log_events (dev, inode, source_code, byte_offset);
CREATE INDEX IF NOT EXISTS ix_log_events_path   ON log_events (path);
CREATE INDEX IF NOT EXISTS ix_log_events_status ON log_events (status);
CREATE INDEX IF NOT EXISTS ix_log_events_ts     ON log_events (ts);

CREATE TABLE IF NOT EXISTS malformed_lines (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_code TEXT    NOT NULL,
    dev         INTEGER NOT NULL,
    inode       INTEGER NOT NULL,
    byte_offset INTEGER NOT NULL,
    raw_line    TEXT    NOT NULL,
    error       TEXT    NOT NULL,
    ingested_at TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    """The single *writer* connection (collector loop / POST /ingest/poll-now).

    check_same_thread=False: the ingestor runs in a background thread while
    the poll-now handler runs in the threadpool. Writes are serialised by
    CollectorLoop.write_lock; WAL permits those writes alongside readers.
    """
    conn = sqlite3.connect(
        db_path, timeout=10, isolation_level=None, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def connect_reader(db_path: str) -> sqlite3.Connection:
    """A fresh read-only connection for a single query response.

    ``query_only`` makes it impossible for a request to write by accident.
    WAL mode (already persisted in the database file by :func:`connect`)
    lets it read committed data while the writer is mid-transaction, and
    never blocks — nor is blocked by — the writer.
    """
    conn = sqlite3.connect(
        db_path, timeout=10, isolation_level=None, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA query_only=ON")
    return conn


@contextmanager
def read_snapshot(db_path: str) -> Iterator[sqlite3.Connection]:
    """Yield a connection whose reads all share one committed-only snapshot.

    All SELECTs of one response run inside a single deferred transaction on
    a dedicated connection, so a concurrent commit neither becomes visible
    halfway through the response nor blocks this reader. The transaction is
    always ended and the connection closed — even when the handler raises —
    so no read resource outlives the request.
    """
    conn = connect_reader(db_path)
    try:
        conn.execute("BEGIN")
        try:
            yield conn
        finally:
            try:
                conn.execute("ROLLBACK")  # end the read transaction
            except sqlite3.Error:
                pass
    finally:
        conn.close()


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Explicit BEGIN/COMMIT; on error everything rolls back together."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def get_state(conn: sqlite3.Connection, dev: int, inode: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM ingest_state WHERE dev = ? AND inode = ?",
        (dev, inode),
    ).fetchone()


def all_states(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM ingest_state ORDER BY source_code"
    ).fetchall()


def query_events(
    conn: sqlite3.Connection,
    *,
    path: Optional[str] = None,
    status: Optional[int] = None,
    status_min: Optional[int] = None,
    status_max: Optional[int] = None,
    source_code: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    """Paginated event query.

    ``conn`` must be inside a read transaction (see :func:`read_snapshot`)
    so the COUNT and the page SELECT observe the same snapshot — ``total``,
    ``items`` and ``next_offset`` are always mutually consistent, even if
    the collector commits between them.
    """
    where: list[str] = []
    params: list[Any] = []
    if path is not None:
        where.append("path = ?")
        params.append(path)
    if status is not None:
        where.append("status = ?")
        params.append(status)
    if status_min is not None:
        where.append("status >= ?")
        params.append(status_min)
    if status_max is not None:
        where.append("status <= ?")
        params.append(status_max)
    if source_code is not None:
        where.append("source_code = ?")
        params.append(source_code)
    clause = (" WHERE " + " AND ".join(where)) if where else ""

    total = conn.execute(
        f"SELECT COUNT(*) AS c FROM log_events{clause}", params
    ).fetchone()["c"]

    rows = conn.execute(
        f"""SELECT id, ts, path, status, method, source_code,
                   dev, inode, byte_offset, raw_line, ingested_at
              FROM log_events{clause}
             ORDER BY id
             LIMIT ? OFFSET ?""",
        [*params, limit, offset],
    ).fetchall()

    next_offset = offset + limit if offset + limit < total else None
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "next_offset": next_offset,
        "items": [dict(r) for r in rows],
    }


def query_malformed(
    conn: sqlite3.Connection, *, limit: int = 100, offset: int = 0
) -> dict[str, Any]:
    """Paginated view of the quarantine table.

    Same snapshot rules as :func:`query_events`: COUNT and the page SELECT
    run in the caller's read transaction, so the page metadata can never
    disagree with the rows returned.
    """
    total = conn.execute(
        "SELECT COUNT(*) AS c FROM malformed_lines"
    ).fetchone()["c"]
    rows = conn.execute(
        "SELECT * FROM malformed_lines ORDER BY id LIMIT ? OFFSET ?",
        (limit, offset),
    ).fetchall()
    next_offset = offset + limit if offset + limit < total else None
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "next_offset": next_offset,
        "items": [dict(r) for r in rows],
    }


def counts(conn: sqlite3.Connection) -> dict[str, int]:
    row = conn.execute(
        """SELECT
                 (SELECT COUNT(*) FROM log_events)      AS events,
                 (SELECT COUNT(*) FROM malformed_lines) AS malformed,
                 (SELECT COUNT(*) FROM ingest_state)    AS tracked_files"""
    ).fetchone()
    return dict(row)
