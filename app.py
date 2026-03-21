"""
OPTIONEX — Streamlit Application
AI-Assisted NSE Index Options Signal Platform
Phase 1+2: Nifty & BankNifty, defined-risk strategies only.
"""

import streamlit as st
import logging
from config import TRADING_MODE, ACTIVE_LLM, validate_config
from core.db import init as db_init, health_check

st.set_page_config(
    page_title="OPTIONEX",
    page_icon="⬡",
    layout="wide",
    initial_sidebar_state="expanded",
)

if "app_initialised" not in st.session_state:
    db_init()
    st.session_state["app_initialised"] = True

import os
import json
import pandas as pd
from datetime import datetime, date
from dotenv import load_dotenv, set_key
from pathlib import Path


# ─────────────────────────────────────────────────────────────────
# DASHBOARD
# ─────────────────────────────────────────────────────────────────

def render_dashboard():
    from core.ui_helpers import render_signal_badge
    from core.db import get_connection
    from config import CAPITAL_INR

    st.title("📊 Dashboard")
    st.caption("Live NSE index prices and latest options signals")

    if st.button("🔄 Refresh", type="secondary"):
        st.cache_data.clear()
        st.rerun()

    # ── Live Prices ────────────────────────────────────
    st.subheader("Live Market Data")

    @st.cache_data(ttl=30)
    def fetch_live_data():
        try:
            from generate_token import generate_totp_token, save_token_to_env
            token = generate_totp_token()
            save_token_to_env(token)
            os.environ["GROWW_ACCESS_TOKEN"] = token

            from core.groww_client import GrowwClient
            client = GrowwClient(access_token=token)

            nifty_spot  = client.get_nse_spot("NIFTY")
            bnifty_spot = client.get_nse_spot("BANKNIFTY")
            vix_data    = client.get_india_vix()

            return {
                "nifty":  nifty_spot,
                "bnifty": bnifty_spot,
                "vix":    vix_data,
            }
        except Exception as e:
            return {"error": str(e)}

    with st.spinner("Fetching live data..."):
        live = fetch_live_data()

    if "error" in live:
        st.warning(f"Live data unavailable: {live['error']}")
    else:
        lc1, lc2, lc3 = st.columns(3)
        with lc1:
            st.markdown("### 📈 Nifty 50")
            if live.get("nifty"):
                st.metric("Spot", f"₹{live['nifty']:,.2f}")
            else:
                st.warning("Spot unavailable")
        with lc2:
            st.markdown("### 🏦 Bank Nifty")
            if live.get("bnifty"):
                st.metric("Spot", f"₹{live['bnifty']:,.2f}")
            else:
                st.warning("Spot unavailable")
        with lc3:
            st.markdown("### 🌡 India VIX")
            vix = live.get("vix", {})
            if vix.get("available"):
                vix_val = vix["vix"]
                if vix_val < 14:
                    st.success(f"VIX: {vix_val:.2f} — Low volatility")
                elif vix_val < 20:
                    st.info(f"VIX: {vix_val:.2f} — Normal")
                else:
                    st.error(f"VIX: {vix_val:.2f} — Elevated")
            else:
                st.warning("VIX unavailable")

    st.divider()

    # ── Latest Signals ─────────────────────────────────
    st.subheader("Latest Signals")

    def get_latest_signals(limit=6):
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT index_name, strategy_name, direction, action,
                       confidence, signal_quality, market_regime, timestamp,
                       primary_reason, followed, mode, spot_price
                FROM options_signals_log
                ORDER BY timestamp DESC LIMIT ?
            """, (limit,))
            rows = [dict(r) for r in cursor.fetchall()]
            conn.close()
            return rows
        except Exception:
            return []

    signals = get_latest_signals()
    if not signals:
        st.info("No signals yet. Go to Signal Engine to run your first analysis.")
    else:
        for sig in signals:
            with st.expander(
                f"{sig['index_name']}  |  {sig['strategy_name']}  |  "
                f"{sig['action']}  |  {sig['confidence']}%  |  {sig['timestamp']}",
                expanded=False,
            ):
                render_signal_badge(
                    sig["action"], sig["confidence"],
                    sig["signal_quality"] or "N/A",
                )
                sc1, sc2, sc3 = st.columns(3)
                with sc1:
                    st.caption(f"Direction: {sig.get('direction', 'N/A')}")
                with sc2:
                    st.caption(f"Regime: {sig.get('market_regime', 'N/A')}")
                with sc3:
                    st.caption(f"Spot: ₹{sig.get('spot_price', 0):,.0f}")
                if sig.get("primary_reason"):
                    st.caption(f"Reason: {sig['primary_reason']}")

    # ── Today's Summary ────────────────────────────────
    st.divider()
    st.subheader("Today's Summary")
    try:
        conn = get_connection()
        cursor = conn.cursor()
        today = date.today().isoformat()
        cursor.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN action != 'HOLD' THEN 1 ELSE 0 END) as actionable,
                   SUM(CASE WHEN followed=1 THEN 1 ELSE 0 END) as followed
            FROM options_signals_log
            WHERE DATE(timestamp) = ? AND mode = ?
        """, (today, TRADING_MODE))
        summary = dict(cursor.fetchone())
        conn.close()
    except Exception:
        summary = {}

    ts1, ts2, ts3 = st.columns(3)
    with ts1:
        st.metric("Signals Today", summary.get("total", 0))
    with ts2:
        st.metric("Actionable", summary.get("actionable", 0))
    with ts3:
        st.metric("Followed", summary.get("followed", 0))


# ─────────────────────────────────────────────────────────────────
# SIGNAL ENGINE
# ─────────────────────────────────────────────────────────────────

def render_signal_engine():
    from core.ui_helpers import (
        render_signal_badge, render_options_analytics,
        render_strategy_legs, render_options_risk,
        render_guardrails, render_chain_table,
    )
    from core.db import get_connection

    st.title("⚡ Signal Engine")
    st.caption(
        f"AI-powered NSE index options signal generation  |  "
        f"Provider: {ACTIVE_LLM['provider'].upper()}  |  "
        f"Model: {ACTIVE_LLM['model']}"
    )

    # ── Controls ───────────────────────────────────────
    st.subheader("Analysis Parameters")
    ctrl1, ctrl2, ctrl3 = st.columns([2, 2, 1])

    with ctrl1:
        index = st.selectbox(
            "Index",
            options=["NIFTY", "BANKNIFTY"],
            format_func=lambda x: {
                "NIFTY": "📈 Nifty 50",
                "BANKNIFTY": "🏦 Bank Nifty",
            }.get(x, x)
        )

    with ctrl2:
        trading_style = st.selectbox(
            "Trading Style",
            options=["system", "intraday", "expiry_day", "positional"],
            format_func=lambda x: {
                "system":     "🤖 System Decides",
                "intraday":   "⚡ Intraday Only",
                "expiry_day": "📅 Expiry Day",
                "positional": "📈 Positional (2-7 days)",
            }.get(x, x)
        )

    with ctrl3:
        st.write("")
        st.write("")
        run_button = st.button("▶ Run Analysis", type="primary", use_container_width=True)

    # ── Pipeline Execution ─────────────────────────────
    if run_button:
        st.divider()
        st.subheader("Pipeline Status")

        progress_bar = st.progress(0)
        status_text  = st.empty()

        try:
            status_text.info("⏳ Initialising...")

            from generate_token import generate_totp_token, save_token_to_env
            token = generate_totp_token()
            save_token_to_env(token)
            os.environ["GROWW_ACCESS_TOKEN"] = token

            from core.groww_client import GrowwClient
            from core.technical_engine import TechnicalEngine
            from core.news_client import NewsClient
            from core.orchestrator import OptionsSignalOrchestrator
            from core.risk_engine import OptionsRiskEngine

            groww = GrowwClient(access_token=token)
            tech  = TechnicalEngine()
            news  = NewsClient()
            orch  = OptionsSignalOrchestrator(groww, tech, news)
            risk_engine = OptionsRiskEngine()

            progress_bar.progress(10)
            status_text.info("📡 Assembling data bundle...")

            progress_bar.progress(20)
            status_text.info("🔬 Running full pipeline...")

            with st.spinner("Running 3-agent pipeline..."):
                result = orch.generate(
                    index=index,
                    timeframe="15minute",
                    trading_style=trading_style,
                )

            progress_bar.progress(80)
            status_text.info("🛡 Running guardrails...")

            # Run guardrails
            is_buying = False
            if result.signal:
                is_buying = any(
                    leg.action == "BUY" for leg in result.signal.legs
                ) if result.signal.legs else False

            guardrail_check = risk_engine.check_all(
                index             = index,
                action            = result.final_action,
                confidence        = result.final_confidence,
                rr_ratio          = result.risk.risk_reward_ratio if result.risk else None,
                trading_style     = trading_style,
                strategy_name     = result.strategy_name,
                iv_percentile     = result.iv_percentile,
                dte               = result.dte_nearest,
                premium_total     = result.position_sizing.get("premium_total") if result.position_sizing else None,
                vix_change_pct    = result.india_vix_change,
                expiry_date       = result.expiry_date,
                high_impact_event = result.analysis.high_impact_events_next_24h if result.analysis else None,
                open_positions    = risk_engine.get_open_positions_count(),
                daily_pnl_pct     = risk_engine.get_daily_pnl_pct(),
                is_buying_premium = is_buying,
            )

            progress_bar.progress(100)
            status_text.success("✅ Analysis complete")

            # ── Persist to DB ──────────────────────────
            try:
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO options_signals_log (
                        index_name, timeframe, trading_style, mode,
                        llm_provider, llm_model, prompt_version,
                        strategy_name, direction, legs_json, expiry,
                        action, confidence, signal_quality, primary_reason,
                        max_loss_per_lot, rr_ratio, net_premium, net_delta, net_theta,
                        position_lots, capital_risk_pct, capital_risk_inr,
                        spot_price, atm_iv, iv_rank, pcr_oi, india_vix,
                        market_regime, sentiment,
                        analyst_output, signal_output, risk_output,
                        guardrail_flags, approved, block_reason
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    index, "15minute", trading_style, TRADING_MODE,
                    ACTIVE_LLM["provider"], ACTIVE_LLM["model"], "1.0",
                    result.strategy_name, result.direction, result.legs_json,
                    result.signal.legs[0].expiry if result.signal and result.signal.legs else None,
                    result.final_action, result.final_confidence,
                    result.signal.signal_quality if result.signal else None,
                    result.signal.primary_reason if result.signal else None,
                    result.risk.max_loss_per_lot if result.risk else None,
                    result.risk.risk_reward_ratio if result.risk else None,
                    result.risk.net_premium if result.risk else None,
                    result.risk.net_delta if result.risk else None,
                    result.risk.net_theta_per_day if result.risk else None,
                    result.position_sizing.get("position_lots") if result.position_sizing else None,
                    result.position_sizing.get("actual_risk_pct") if result.position_sizing else None,
                    result.position_sizing.get("actual_risk_inr") if result.position_sizing else None,
                    result.spot_price,
                    result.atm_iv,
                    result.iv_rank,
                    result.pcr_oi,
                    result.india_vix,
                    result.analysis.market_regime if result.analysis else None,
                    result.analysis.overall_sentiment if result.analysis else None,
                    json.dumps(result.analysis.model_dump()) if result.analysis else None,
                    json.dumps(result.signal.model_dump()) if result.signal else None,
                    json.dumps(result.risk.model_dump()) if result.risk else None,
                    json.dumps(guardrail_check["block_reasons"]),
                    1 if result.approved else 0,
                    result.block_reason,
                ))
                conn.commit()
                conn.close()
            except Exception as e:
                st.warning(f"Signal not saved: {e}")

            # ── Display Results ────────────────────────
            st.divider()
            st.subheader("Signal Result")

            render_signal_badge(
                result.final_action, result.final_confidence,
                result.signal.signal_quality if result.signal else "N/A",
            )

            if result.strategy_name != "none":
                st.markdown(
                    f"**Strategy**: {result.strategy_name} | "
                    f"**Direction**: {result.direction}"
                )

            if guardrail_check["block_reasons"]:
                for reason in guardrail_check["block_reasons"]:
                    st.warning(f"🚫 {reason}")

            with st.expander("Guardrail Status", expanded=False):
                render_guardrails(guardrail_check["guardrail_results"])
                for gr in guardrail_check["guardrail_results"]:
                    icon = "✅" if gr.passed else "🚫"
                    st.caption(f"{icon} {gr.name}: {gr.reason}")

            st.divider()

            # Two-column layout
            left, right = st.columns([1, 1])

            with left:
                st.subheader("Market Analysis")
                if result.analysis:
                    a = result.analysis
                    st.markdown(f"**Regime**: {a.market_regime} ({a.trend_strength})")
                    st.markdown(f"**Sentiment**: {a.overall_sentiment} ({a.sentiment_confidence}%)")
                    st.markdown(f"**IV Regime**: {a.iv_regime}")
                    st.markdown(f"**IV Assessment**: {a.iv_assessment}")
                    st.markdown(f"**Expected Move**: ±{a.expected_move:.0f} pts ({a.expected_move_pct:.2f}%)")
                    st.markdown(f"**PCR**: {a.pcr_interpretation}")
                    st.markdown(f"**Max Pain**: {a.max_pain_interpretation}")
                    st.markdown(f"**OI Walls**: {a.oi_wall_interpretation}")
                    st.markdown(f"**Bias**: {a.recommended_bias}")
                    if a.high_impact_events_next_24h:
                        st.warning(f"⚠ Event: {a.high_impact_events_next_24h}")
                    st.caption(f"Notes: {a.analyst_notes}")

                if result.signal and result.final_action != "HOLD":
                    st.divider()
                    st.subheader("Strategy Details")
                    render_strategy_legs(
                        result.signal.legs,
                        NSE_LOT_CONFIG.get(index, {}).get("lot_size", 75),
                    )
                    st.markdown(f"**IV Edge**: {result.signal.iv_edge}")
                    st.markdown(f"**Theta**: {result.signal.theta_impact}")
                    st.markdown(f"**Greeks**: {result.signal.greeks_summary}")

                elif result.signal and result.final_action == "HOLD":
                    st.divider()
                    st.info(
                        f"**HOLD Reason**: "
                        f"{result.signal.hold_reasoning or result.signal.primary_reason}"
                    )

            with right:
                st.subheader("Risk Parameters")
                if result.risk:
                    render_options_risk(result.risk, result.position_sizing)

                if result.sanity_warnings:
                    st.divider()
                    st.subheader("⚠ Sanity Warnings")
                    for w in result.sanity_warnings:
                        st.warning(w)

        except Exception as e:
            progress_bar.progress(100)
            status_text.error(f"Pipeline failed: {e}")
            st.error(f"Error: {e}")
            import traceback
            st.code(traceback.format_exc())


# ─────────────────────────────────────────────────────────────────
# TRADE LOG
# ─────────────────────────────────────────────────────────────────

def render_trade_log():
    from core.db import get_connection
    from config import CAPITAL_INR

    st.title("📒 Trade Log")
    st.caption("Track paper and live options trades")

    tab1, tab2 = st.tabs(["Signals History", "Trades"])

    with tab1:
        try:
            conn = get_connection()
            df = pd.read_sql_query("""
                SELECT timestamp, index_name, strategy_name, direction,
                       action, confidence, signal_quality,
                       max_loss_per_lot, rr_ratio, net_premium,
                       position_lots, spot_price, market_regime,
                       approved, block_reason, primary_reason
                FROM options_signals_log
                ORDER BY timestamp DESC LIMIT 50
            """, conn)
            conn.close()

            if df.empty:
                st.info("No signals generated yet.")
            else:
                st.dataframe(df, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"Failed to load signals: {e}")

    with tab2:
        try:
            conn = get_connection()
            df = pd.read_sql_query("""
                SELECT * FROM options_trades_log
                ORDER BY entry_time DESC LIMIT 50
            """, conn)
            conn.close()
            if df.empty:
                st.info("No trades logged yet.")
            else:
                st.dataframe(df, use_container_width=True, hide_index=True)
                total_pnl = df["pnl_total"].sum() if "pnl_total" in df.columns else 0
                st.metric("Total P&L", f"₹{total_pnl:,.0f}")
        except Exception:
            st.info("No trades logged yet.")


# ─────────────────────────────────────────────────────────────────
# SETTINGS
# ─────────────────────────────────────────────────────────────────

def render_settings():
    from core.backup import run_backup, list_backups
    from config import (
        CAPITAL_INR, RISK_PCT_PER_TRADE,
        MAX_OPEN_POSITIONS, DAILY_LOSS_LIMIT_PCT,
    )

    load_dotenv()
    st.title("⚙ Settings")
    ENV_PATH = Path(".env")

    warnings = validate_config()
    if warnings:
        for w in warnings:
            st.warning(f"⚠ {w}")
    else:
        st.success("✓ All configuration valid")

    # API Keys
    st.subheader("API Key Status")
    for env_var, label in [
        ("GROWW_API_KEY", "Groww API Key"),
        ("GROWW_TOTP_SECRET", "Groww TOTP Secret"),
        ("OPENAI_API_KEY", "OpenAI (demo)"),
        ("ANTHROPIC_API_KEY", "Anthropic (paper/prod)"),
        ("TAVILY_API_KEY", "Tavily News"),
    ]:
        val = os.getenv(env_var, "")
        if val and not val.startswith("your_"):
            st.success(f"✓ {label}")
        else:
            st.error(f"✗ {label} not set")

    # Risk Parameters
    st.divider()
    st.subheader("Risk Parameters")
    rc = st.columns(4)
    with rc[0]:
        st.metric("Capital", f"₹{CAPITAL_INR:,.0f}")
    with rc[1]:
        st.metric("Risk/Trade", f"{RISK_PCT_PER_TRADE}%")
    with rc[2]:
        st.metric("Max Positions", MAX_OPEN_POSITIONS)
    with rc[3]:
        st.metric("Daily Loss Limit", f"{DAILY_LOSS_LIMIT_PCT}%")

    # Trading Mode
    st.divider()
    st.subheader("Trading Mode")
    mode_options = {
        "demo": "🟡 Demo (GPT-4o)",
        "paper": "🔵 Paper (Claude Sonnet 4.6)",
        "production": "🔴 Production — REAL MONEY",
    }
    st.info(f"Current: **{mode_options.get(TRADING_MODE)}**")

    new_mode = st.selectbox("Switch to:", list(mode_options.keys()),
                            format_func=lambda x: mode_options[x],
                            index=list(mode_options.keys()).index(TRADING_MODE))
    if new_mode != TRADING_MODE:
        if new_mode == "production":
            st.error("⚠ PRODUCTION — real money. Complete paper trading first.")
            if st.text_input("Type CONFIRM REAL MONEY:") == "CONFIRM REAL MONEY":
                if st.button("🔴 Switch to Production"):
                    set_key(str(ENV_PATH), "TRADING_MODE", "production")
                    set_key(str(ENV_PATH), "PRODUCTION_CONFIRMED", "true")
                    st.success("Restart app to apply.")
        else:
            if st.button(f"Switch to {mode_options[new_mode]}"):
                set_key(str(ENV_PATH), "TRADING_MODE", new_mode)
                st.success("Restart app to apply.")

    # Database
    st.divider()
    st.subheader("Database")
    db_status = health_check()
    if db_status["status"] == "ok":
        st.success(f"✓ Healthy — {len(db_status['tables'])} tables")
    else:
        st.error(f"Issue: {db_status}")

    if st.button("💾 Backup Now"):
        r = run_backup()
        if r["status"] == "ok":
            st.success(f"Backup: {r['backup_path']}")
        else:
            st.error(str(r))

    # ── IV History ─────────────────────────────────────
    st.divider()
    st.subheader("IV History (for IV Rank)")

    from core.iv_archiver import get_history_stats

    for idx in ["NIFTY", "BANKNIFTY"]:
        stats = get_history_stats(idx)
        days = stats.get("days", 0)
        if days and days > 0:
            st.markdown(
                f"**{idx}**: {days} days | "
                f"IV range: {stats.get('iv_low',0):.1f}% – {stats.get('iv_high',0):.1f}% | "
                f"Avg: {stats.get('iv_avg',0):.1f}% | "
                f"First: {stats.get('first_date','')} | Last: {stats.get('last_date','')}"
            )
            if days < 20:
                st.warning(
                    f"⚠ {idx}: Only {days} days of IV history. "
                    f"Need ≥20 for IV rank. Run daily archiver or seed from external data."
                )
        else:
            st.warning(
                f"⚠ {idx}: No IV history. IV rank/percentile will be unavailable. "
                f"Run `python -m core.iv_archiver` daily after market close, "
                f"or seed historical data via `seed_iv_history()`."
            )

    st.caption(
        "IV history is archived automatically with each signal run. "
        "For bootstrapping, use `core.iv_archiver.seed_iv_history()` "
        "with historical ATM IV data from NSE."
    )


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────

def main():
    from core.ui_helpers import render_sidebar
    selection = render_sidebar()

    if selection == "Home":
        st.title("⬡ OPTIONEX")
        st.caption("AI-Assisted NSE Index Options Signal Platform")
        st.divider()
        mode_info = {
            "demo": ("🟡", "Demo Mode", "OpenAI GPT-4o", "info"),
            "paper": ("🔵", "Paper Trading", "Claude Sonnet 4.6", "info"),
            "production": ("🔴", "PRODUCTION", "Claude Sonnet 4.6", "error"),
        }
        icon, label, model, kind = mode_info.get(TRADING_MODE, ("⚪", TRADING_MODE, "?", "info"))
        getattr(st, kind)(f"{icon} **{label}** — {model}")
        st.divider()

        st.markdown("""
        ### Phase 1 + 2 — Defined Risk Strategies

        **Supported Indices**: Nifty 50, Bank Nifty

        **Available Strategies**:
        - Long Call / Long Put (directional)
        - Bull Call Spread / Bear Put Spread (directional spreads)
        - Long Straddle / Long Strangle (volatility plays)
        - Iron Condor (range-bound / premium selling)

        **Pipeline**: Data Bundle → Market Analyst → Sanity Check → Strategy Selector → Risk Assessor

        **Guardrails**: 15 hardcoded safety checks including IV gate, DTE gate, premium cap

        Select **Signal Engine** from the sidebar to run your first analysis.
        """)

    elif selection == "Dashboard":
        render_dashboard()
    elif selection == "Signal Engine":
        render_signal_engine()
    elif selection == "Trade Log":
        render_trade_log()
    elif selection == "Settings":
        render_settings()


if __name__ == "__main__":
    main()
