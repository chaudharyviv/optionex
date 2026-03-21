"""
OPTIONEX — IV History Archival Job
Records daily ATM IV, spot close, PCR, max pain for each index.
This data is essential for IV Rank and IV Percentile computation.

Run daily after market close (3:30 PM IST) via cron or scheduler:
  python -m core.iv_archiver

Or call archive_today() from the Streamlit app's Settings page.
"""

import logging
from datetime import datetime, date
from zoneinfo import ZoneInfo

from core.db import get_connection
from config import ACTIVE_INDICES

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")


def archive_today(
    index:      str,
    atm_iv:     float,
    spot_close: float,
    pcr_oi:     float = None,
    max_pain:   float = None,
    india_vix:  float = None,
    atm_strike: float = None,
) -> dict:
    """
    Archive today's IV data point for an index.
    Uses INSERT OR REPLACE so re-runs are safe.
    """
    today = date.today().isoformat()
    try:
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO iv_history
            (index_name, date, atm_iv, atm_strike, spot_close,
             pcr_oi, max_pain, india_vix)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            index, today, atm_iv, atm_strike, spot_close,
            pcr_oi, max_pain, india_vix,
        ))
        conn.commit()
        conn.close()
        logger.info(
            f"IV archived: {index} {today} | "
            f"IV={atm_iv:.1f}% spot={spot_close:,.0f}"
        )
        return {"status": "ok", "index": index, "date": today}
    except Exception as e:
        logger.error(f"IV archival failed: {e}")
        return {"status": "error", "error": str(e)}


def archive_from_bundle(bundle) -> dict:
    """
    Archive IV data from an OptionsDataBundle after a signal run.
    Called automatically by the orchestrator or manually.
    """
    if not bundle.options_ok or not bundle.options:
        return {"status": "skipped", "reason": "No options data"}

    return archive_today(
        index      = bundle.index,
        atm_iv     = bundle.options.atm_iv,
        spot_close = bundle.spot_price or 0,
        pcr_oi     = bundle.options.pcr_oi,
        max_pain   = bundle.options.max_pain_strike,
        india_vix  = bundle.india_vix,
    )


def get_iv_history(index: str, days: int = 252) -> list[dict]:
    """Fetch IV history for display or analysis."""
    try:
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT date, atm_iv, spot_close, pcr_oi, max_pain, india_vix
            FROM iv_history
            WHERE index_name = ?
            ORDER BY date DESC
            LIMIT ?
        """, (index, days))
        rows = [dict(r) for r in cursor.fetchall()]
        conn.close()
        return list(reversed(rows))
    except Exception:
        return []


def get_history_stats(index: str) -> dict:
    """Summary stats for IV history — useful for Settings page."""
    try:
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) as days,
                   MIN(date) as first_date,
                   MAX(date) as last_date,
                   MIN(atm_iv) as iv_low,
                   MAX(atm_iv) as iv_high,
                   AVG(atm_iv) as iv_avg
            FROM iv_history
            WHERE index_name = ?
        """, (index,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return dict(row)
        return {}
    except Exception:
        return {}


def seed_iv_history(
    index: str,
    iv_data: list[dict],
) -> int:
    """
    Bulk-insert historical IV data for bootstrapping.
    iv_data: list of {date: "2025-01-15", atm_iv: 14.5, spot_close: 23200}

    Use this to seed from external sources (NSE historical data, etc.)
    before the daily archiver has enough data for IV rank computation.
    """
    conn   = get_connection()
    cursor = conn.cursor()
    inserted = 0
    for row in iv_data:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO iv_history
                (index_name, date, atm_iv, spot_close)
                VALUES (?, ?, ?, ?)
            """, (
                index,
                row["date"],
                float(row["atm_iv"]),
                float(row.get("spot_close", 0)),
            ))
            inserted += cursor.rowcount
        except Exception:
            pass
    conn.commit()
    conn.close()
    logger.info(f"Seeded {inserted} IV history records for {index}")
    return inserted


# ── CLI Entry Point ──────────────────────────────────────────────

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()

    logging.basicConfig(level=logging.INFO)

    print("=" * 55)
    print("OPTIONEX — IV History Archiver")
    print("=" * 55)

    # This requires Groww client to be functional
    try:
        from generate_token import generate_totp_token, save_token_to_env
        token = generate_totp_token()
        save_token_to_env(token)
        os.environ["GROWW_ACCESS_TOKEN"] = token

        from core.groww_client import GrowwClient
        from core.options_engine import OptionsEngine

        client = GrowwClient(access_token=token)
        engine = OptionsEngine()

        for index in ACTIVE_INDICES:
            print(f"\nArchiving {index}...")

            spot = client.get_nse_spot(index)
            if not spot:
                print(f"  ⚠ Could not get spot for {index}")
                continue

            chain_result = client.get_option_chain(index)
            if not chain_result:
                print(f"  ⚠ Could not get chain for {index}")
                continue

            lot_size = 75 if index == "NIFTY" else 30
            opts = engine.compute(
                chain_data=chain_result["chain"],
                spot_price=spot,
                futures_price=spot,
                index=index,
                nearest_expiry=chain_result["nearest_expiry"],
                lot_size=lot_size,
            )

            vix_data = client.get_india_vix()

            result = archive_today(
                index=index,
                atm_iv=opts.atm_iv,
                spot_close=spot,
                pcr_oi=opts.pcr_oi,
                max_pain=opts.max_pain_strike,
                india_vix=vix_data.get("vix"),
            )
            print(f"  {result}")

        print("\nDone.")

    except Exception as e:
        print(f"Error: {e}")
        print("Run manually after setting up Groww credentials.")
