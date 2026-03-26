"""
db.py — Database abstraction layer.

Uses Postgres when DATABASE_URL is set, falls back to local SQLite.

Usage:
    from db import get_connection, init_db, ph, is_postgres

    conn = get_connection()
    init_db(conn)
    conn.execute(f"INSERT INTO t (a, b) VALUES ({ph()}, {ph()})", (1, 2))
"""

import json
import logging
import os
import sqlite3
from pathlib import Path

log = logging.getLogger(__name__)

_DB_DIR = Path(__file__).parent / "data"
_SQLITE_PATH = _DB_DIR / "kalshi.db"


def _has_postgres():
    return bool(os.environ.get("DATABASE_URL"))


def is_postgres() -> bool:
    return _has_postgres()


def ph() -> str:
    """Return the parameter placeholder for the current backend."""
    return "%s" if _has_postgres() else "?"


def get_connection():
    """Get a database connection (Postgres or SQLite)."""
    if _has_postgres():
        import psycopg2
        import psycopg2.extras
        url = os.environ["DATABASE_URL"]
        conn = psycopg2.connect(url)
        conn.autocommit = False
        log.info("Connected to Postgres")
        return conn
    else:
        _DB_DIR.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(_SQLITE_PATH), check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        log.info("Connected to SQLite (%s)", _SQLITE_PATH)
        return conn


def init_db(conn):
    """Create all tables. Auto-detects Postgres vs SQLite."""
    if _has_postgres():
        _init_postgres(conn)
    else:
        _init_sqlite(conn)


def _init_postgres(conn):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS market_snapshots (
        id               SERIAL PRIMARY KEY,
        snapshot_time    TIMESTAMP NOT NULL,
        ticker           TEXT NOT NULL,
        title            TEXT,
        series           TEXT,
        yes_bid          REAL,
        yes_ask          REAL,
        mid              REAL,
        volume           INTEGER,
        close_time       TIMESTAMP,
        minutes_to_close REAL,
        tags             JSONB,
        raw              JSONB
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_snap_ticker_time ON market_snapshots(ticker, snapshot_time)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_snap_time ON market_snapshots(snapshot_time)")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS market_outcomes (
        ticker          TEXT PRIMARY KEY,
        title           TEXT,
        series          TEXT,
        close_time      TIMESTAMP,
        result          TEXT,
        yes_price_final REAL,
        volume_total    INTEGER,
        scraped_at      TIMESTAMP,
        raw             JSONB
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_outcome_series ON market_outcomes(series)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_outcome_close ON market_outcomes(close_time)")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS backtest_runs (
        run_id       TEXT PRIMARY KEY,
        started_at   TIMESTAMP,
        params       JSONB,
        strategy     TEXT,
        total_trades INTEGER,
        win_rate     REAL,
        total_pnl    REAL,
        sharpe_ratio REAL,
        max_drawdown REAL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS backtest_trades (
        id            SERIAL PRIMARY KEY,
        run_id        TEXT NOT NULL,
        strategy      TEXT NOT NULL,
        ticker        TEXT NOT NULL,
        side          TEXT NOT NULL,
        action        TEXT NOT NULL,
        count         INTEGER,
        limit_price   INTEGER,
        reason        TEXT,
        snapshot_time TIMESTAMP,
        outcome       TEXT,
        pnl_cents     REAL
    )
    """)
    conn.commit()
    log.info("Postgres tables initialized")


def _init_sqlite(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS market_snapshots (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_time    TEXT NOT NULL,
        ticker           TEXT NOT NULL,
        title            TEXT,
        series           TEXT,
        yes_bid          REAL,
        yes_ask          REAL,
        mid              REAL,
        volume           INTEGER,
        close_time       TEXT,
        minutes_to_close REAL,
        tags             TEXT,
        raw              TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_snap_ticker_time ON market_snapshots(ticker, snapshot_time);
    CREATE INDEX IF NOT EXISTS idx_snap_time ON market_snapshots(snapshot_time);

    CREATE TABLE IF NOT EXISTS market_outcomes (
        ticker          TEXT PRIMARY KEY,
        title           TEXT,
        series          TEXT,
        close_time      TEXT,
        result          TEXT,
        yes_price_final REAL,
        volume_total    INTEGER,
        scraped_at      TEXT,
        raw             TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_outcome_series ON market_outcomes(series);
    CREATE INDEX IF NOT EXISTS idx_outcome_close ON market_outcomes(close_time);

    CREATE TABLE IF NOT EXISTS backtest_runs (
        run_id      TEXT PRIMARY KEY,
        started_at  TEXT,
        params      TEXT,
        strategy    TEXT,
        total_trades INTEGER,
        win_rate    REAL,
        total_pnl   REAL,
        sharpe_ratio REAL,
        max_drawdown REAL
    );

    CREATE TABLE IF NOT EXISTS backtest_trades (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id        TEXT NOT NULL,
        strategy      TEXT NOT NULL,
        ticker        TEXT NOT NULL,
        side          TEXT NOT NULL,
        action        TEXT NOT NULL,
        count         INTEGER,
        limit_price   INTEGER,
        reason        TEXT,
        snapshot_time TEXT,
        outcome       TEXT,
        pnl_cents     REAL
    );
    """)
    conn.commit()
    log.info("SQLite tables initialized")


def fetchall_dicts(conn, query: str, params: tuple = ()) -> list[dict]:
    """Execute a query and return results as list of dicts (works for both backends)."""
    if _has_postgres():
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(query, params)
        rows = cur.fetchall()
        cur.close()
        return [dict(r) for r in rows]
    else:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(query, params)
        rows = cur.fetchall()
        conn.row_factory = None
        return [dict(r) for r in rows]


def execute(conn, query: str, params: tuple = ()):
    """Execute a query (works for both backends)."""
    if _has_postgres():
        cur = conn.cursor()
        cur.execute(query, params)
        cur.close()
    else:
        conn.execute(query, params)


def executemany(conn, query: str, rows: list[tuple]):
    """Execute many rows (works for both backends)."""
    if _has_postgres():
        cur = conn.cursor()
        cur.executemany(query, rows)
        cur.close()
    else:
        conn.executemany(query, rows)
