"""
OPTIONEX — Database Layer
SQLite setup and table initialisation for options trading.
All tables use IF NOT EXISTS — safe to call init() on every startup.
"""

import sqlite3
import logging
from config import DB_PATH

logger = logging.getLogger(__name__)


def get_connection() -> sqlite3.Connection:
    """Return a SQLite connection with row_factory for dict-style access."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init():
    """
    Create all tables if they don't exist.
    Safe to call on every application startup.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # ── Options Signals Log ────────────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS options_signals_log (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            index_name          TEXT NOT NULL,
            timeframe           TEXT NOT NULL,
            trading_style       TEXT NOT NULL,
            mode                TEXT NOT NULL,
            llm_provider        TEXT NOT NULL,
            llm_model           TEXT NOT NULL,
            prompt_version      TEXT,

            strategy_name       TEXT NOT NULL,
            direction           TEXT NOT NULL,
            legs_json           TEXT NOT NULL,
            expiry              TEXT,

            action              TEXT NOT NULL,
            confidence          INTEGER,
            signal_quality      TEXT,
            primary_reason      TEXT,

            max_loss_per_lot    REAL,
            max_profit_per_lot  REAL,
            breakeven_points    TEXT,
            rr_ratio            REAL,
            net_premium         REAL,
            net_delta           REAL,
            net_theta           REAL,
            position_lots       INTEGER,
            capital_risk_pct    REAL,
            capital_risk_inr    REAL,

            spot_price          REAL,
            atm_iv              REAL,
            iv_rank             REAL,
            pcr_oi              REAL,
            india_vix           REAL,
            market_regime       TEXT,
            sentiment           TEXT,

            analyst_output      TEXT,
            signal_output       TEXT,
            risk_output         TEXT,
            guardrail_flags     TEXT,

            approved            INTEGER DEFAULT 0,
            block_reason        TEXT,
            news_available      INTEGER DEFAULT 1,
            followed            INTEGER DEFAULT NULL,
            created_at          DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ── Options Trades Log ─────────────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS options_trades_log (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id           INTEGER REFERENCES options_signals_log(id),
            index_name          TEXT NOT NULL,
            strategy_name       TEXT NOT NULL,
            legs_json           TEXT NOT NULL,
            mode                TEXT NOT NULL,
            lots                INTEGER NOT NULL,

            entry_time          DATETIME,
            entry_premium_net   REAL,
            entry_spot          REAL,

            exit_time           DATETIME,
            exit_premium_net    REAL,
            exit_spot           REAL,
            exit_reason         TEXT,

            pnl_per_lot         REAL,
            pnl_total           REAL,
            pnl_pct             REAL,

            dte_at_entry        INTEGER,
            dte_at_exit         INTEGER,
            iv_at_entry         REAL,
            iv_at_exit          REAL,

            notes               TEXT,
            order_id            TEXT,
            order_status        TEXT DEFAULT 'MANUAL',
            created_at          DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ── IV History (for IV rank / percentile) ──────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS iv_history (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            index_name          TEXT NOT NULL,
            date                DATE NOT NULL,
            atm_iv              REAL NOT NULL,
            atm_strike          REAL,
            spot_close          REAL,
            pcr_oi              REAL,
            max_pain            REAL,
            india_vix           REAL,
            created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(index_name, date)
        )
    """)

    # ── Chain Snapshots (for OI shift detection) ───────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS chain_snapshots (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            index_name          TEXT NOT NULL,
            expiry              TEXT NOT NULL,
            snapshot_time       DATETIME NOT NULL,
            chain_json          TEXT NOT NULL,
            created_at          DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ── News Cache ─────────────────────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS news_cache (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            commodity           TEXT NOT NULL,
            headline            TEXT NOT NULL,
            snippet             TEXT,
            source              TEXT,
            url                 TEXT,
            published_at        DATETIME,
            fetched_at          DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ── Market Cache (spot candles) ────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_cache (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            index_name          TEXT NOT NULL,
            timeframe           TEXT NOT NULL,
            candle_time         DATETIME NOT NULL,
            open                REAL,
            high                REAL,
            low                 REAL,
            close               REAL,
            volume              INTEGER,
            cached_at           DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(index_name, timeframe, candle_time)
        )
    """)

    # ── Daily Summary ──────────────────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_summary (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            date                    DATE NOT NULL,
            index_name              TEXT NOT NULL,
            signals_generated       INTEGER DEFAULT 0,
            signals_followed        INTEGER DEFAULT 0,
            paper_pnl_inr           REAL DEFAULT 0,
            real_pnl_inr            REAL DEFAULT 0,
            win_count               INTEGER DEFAULT 0,
            loss_count              INTEGER DEFAULT 0,
            daily_loss_limit_hit    INTEGER DEFAULT 0,
            UNIQUE(date, index_name)
        )
    """)

    # ── Prompt Versions ────────────────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS prompt_versions (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_name          TEXT NOT NULL,
            version             TEXT NOT NULL,
            prompt_text         TEXT NOT NULL,
            notes               TEXT,
            active              INTEGER DEFAULT 1,
            created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(agent_name, version)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS swing_signals_log (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            symbol              TEXT NOT NULL,
            exchange            TEXT NOT NULL DEFAULT 'NSE',
            mode                TEXT NOT NULL,
            llm_provider        TEXT NOT NULL,
            llm_model           TEXT NOT NULL,
     
            setup_type          TEXT,
            direction           TEXT,
            signal_quality      TEXT,
     
            entry_price         REAL,
            stop_loss           REAL,
            target_1            REAL,
            target_2            REAL,
            hold_days           INTEGER,
            risk_reward         REAL,
            sector              TEXT,
     
            action              TEXT NOT NULL,
            confidence          INTEGER,
            approved            INTEGER DEFAULT 0,
            block_reason        TEXT,
     
            shares              INTEGER,
            position_value      REAL,
            actual_risk_inr     REAL,
            actual_risk_pct     REAL,
     
            spot_price          REAL,
            india_vix           REAL,
            market_regime       TEXT,
            data_quality        TEXT,
            sanity_passed       INTEGER DEFAULT 1,
            primary_reason      TEXT,
     
            followed            INTEGER DEFAULT NULL,
            created_at          DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
 
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS swing_trades_log (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id           INTEGER REFERENCES swing_signals_log(id),
            symbol              TEXT NOT NULL,
            exchange            TEXT NOT NULL DEFAULT 'NSE',
            setup_type          TEXT,
            mode                TEXT NOT NULL,
     
            shares              INTEGER NOT NULL,
            entry_price         REAL,
            entry_time          DATETIME,
            entry_order_id      TEXT,
     
            stop_loss           REAL,
            target_1            REAL,
            target_2            REAL,
     
            exit_price          REAL,
            exit_time           DATETIME,
            exit_reason         TEXT,
            exit_order_id       TEXT,
     
            pnl_per_share       REAL,
            pnl_total           REAL,
            pnl_pct             REAL,
            hold_days_actual    INTEGER,
     
            sector              TEXT,
            india_vix_at_entry  REAL,
            order_status        TEXT DEFAULT 'MANUAL',
            notes               TEXT,
            created_at          DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()
    logger.info(f"Database initialised at {DB_PATH}")

    


def health_check() -> dict:
    """Quick check that DB is accessible and all tables exist."""
    expected_tables = {
        "options_signals_log", "options_trades_log",
        "iv_history", "chain_snapshots",
        "news_cache", "market_cache",
        "daily_summary", "prompt_versions",
        "swing_signals_log",    # ← ADD
        "swing_trades_log",     # ← ADD
    }
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing = {row["name"] for row in cursor.fetchall()}
        conn.close()
        missing = expected_tables - existing
        return {
            "status":  "ok" if not missing else "degraded",
            "tables":  list(existing),
            "missing": list(missing),
            "db_path": str(DB_PATH),
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}



