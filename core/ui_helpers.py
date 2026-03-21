"""
OPTIONEX — Shared UI Helpers v1.0
Reusable Streamlit components for options display.
"""

import streamlit as st
from datetime import datetime
from zoneinfo import ZoneInfo
from config import TRADING_MODE, NSE_LOT_CONFIG

IST = ZoneInfo("Asia/Kolkata")


def render_mode_badge():
    badges = {
        "demo":       ("🟡", "DEMO MODE",        "warning"),
        "paper":      ("🔵", "PAPER TRADING",     "info"),
        "production": ("🔴", "PRODUCTION — LIVE", "error"),
    }
    icon, label, kind = badges.get(TRADING_MODE, ("⚪", TRADING_MODE.upper(), "info"))
    getattr(st, kind)(f"{icon} {label}")


def get_market_status() -> dict:
    now      = datetime.now(IST)
    now_time = now.strftime("%H:%M")
    is_open  = "09:15" <= now_time <= "15:30"
    return {
        "is_open":  is_open,
        "time_ist": now.strftime("%H:%M:%S IST"),
        "date":     now.strftime("%d %b %Y"),
        "label":    "🟢 OPEN" if is_open else "🔴 CLOSED",
    }


def render_market_status():
    status = get_market_status()
    if status["is_open"]:
        st.success(f"NSE {status['label']}  |  {status['time_ist']}")
    else:
        st.error(f"NSE {status['label']}  |  {status['time_ist']}")


def render_signal_badge(action: str, confidence: int, quality: str):
    if action in ("BUY_PREMIUM", "DIRECTIONAL"):
        st.success(f"▲ {action}  |  {confidence}% confidence  |  Grade {quality}")
    elif action == "SELL_PREMIUM":
        st.error(f"▼ {action}  |  {confidence}% confidence  |  Grade {quality}")
    elif action == "HOLD":
        st.warning(f"◆ HOLD  |  {confidence}% confidence  |  Grade {quality}")
    else:
        st.info(f"● {action}  |  {confidence}% confidence  |  Grade {quality}")


def render_options_analytics(options_data):
    """Render options analytics — IV, PCR, max pain, OI walls."""
    if not options_data:
        st.info("No options data available")
        return

    def fmt(val, suffix=""):
        return f"{val}{suffix}" if val is not None else "N/A"

    # ── IV Section ─────────────────────────────────
    st.markdown("#### IV Analytics")
    iv = st.columns(4)
    with iv[0]:
        st.markdown("**ATM IV**")
        st.markdown(f"### {options_data.atm_iv:.1f}%")
        st.caption(f"IV-HV: {options_data.iv_hv_spread:+.1f}%")
    with iv[1]:
        st.markdown("**IV Rank**")
        st.markdown(f"### {options_data.iv_rank:.0f}")
        regime_colors = {"low": "🟢", "normal": "🔵", "high": "🟡", "extreme": "🔴"}
        st.caption(f"{regime_colors.get(options_data.iv_regime,'⚪')} {options_data.iv_regime}")
    with iv[2]:
        st.markdown("**IV Percentile**")
        st.markdown(f"### {options_data.iv_percentile:.0f}")
        st.caption(f"HV(20): {options_data.historical_vol_20:.1f}%")
    with iv[3]:
        st.markdown("**IV Skew**")
        st.markdown(f"### {options_data.iv_skew:+.2f}%")
        st.caption(f"Term: {options_data.iv_term_structure}")
    st.divider()

    # ── Sentiment Section ──────────────────────────
    st.markdown("#### Options Sentiment")
    se = st.columns(4)
    with se[0]:
        st.markdown("**PCR (OI)**")
        st.markdown(f"### {options_data.pcr_oi:.2f}")
        pcr_colors = {"bullish": "🟢", "bearish": "🔴", "neutral": "⚪"}
        st.caption(f"{pcr_colors.get(options_data.pcr_signal,'⚪')} {options_data.pcr_signal}")
    with se[1]:
        st.markdown("**Max Pain**")
        st.markdown(f"### {options_data.max_pain_strike:,.0f}")
        st.caption(f"Distance: {options_data.max_pain_distance:+,.0f}")
    with se[2]:
        st.markdown("**Call OI Wall**")
        st.markdown(f"### {options_data.highest_call_oi_strike:,.0f}")
        st.caption(f"OI: {options_data.call_oi_wall:,}")
    with se[3]:
        st.markdown("**Put OI Wall**")
        st.markdown(f"### {options_data.highest_put_oi_strike:,.0f}")
        st.caption(f"OI: {options_data.put_oi_wall:,}")
    st.divider()

    # ── ATM Greeks ─────────────────────────────────
    st.markdown("#### ATM Greeks (per lot)")
    gr = st.columns(4)
    with gr[0]:
        st.markdown("**Call Delta / Put Delta**")
        st.markdown(f"{options_data.atm_call_delta:+.2f} / {options_data.atm_put_delta:+.2f}")
    with gr[1]:
        st.markdown("**Call Theta / Put Theta**")
        st.markdown(f"₹{options_data.atm_call_theta:.0f} / ₹{options_data.atm_put_theta:.0f}")
        st.caption("per day per lot")
    with gr[2]:
        st.markdown("**Gamma**")
        st.markdown(f"{options_data.atm_call_gamma:.4f}")
    with gr[3]:
        st.markdown("**Vega**")
        st.markdown(f"₹{options_data.atm_call_vega:.1f}")
        st.caption("per 1% IV move per lot")
    st.divider()


def render_strategy_legs(legs, lot_size: int = 75):
    """Render strategy legs in a clean table."""
    if not legs:
        st.info("No strategy legs")
        return

    st.markdown("#### Strategy Legs")
    for i, leg in enumerate(legs, 1):
        action_icon = "🟢 BUY" if leg.action == "BUY" else "🔴 SELL"
        st.markdown(
            f"**Leg {i}**: {action_icon} {leg.option_type} "
            f"**{leg.strike:,.0f}** @ ₹{leg.approx_premium:.1f} "
            f"(Δ={leg.delta:+.2f}) — {leg.expiry}"
        )


def render_options_risk(risk, position_sizing):
    """Render options risk parameters."""
    if not risk:
        st.info("No risk parameters — HOLD signal")
        return

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Max Loss / Lot", f"₹{risk.max_loss_per_lot:,.0f}")
        st.metric("Net Premium", f"₹{risk.net_premium:,.0f}")
    with col2:
        st.metric("Max Profit / Lot", f"₹{risk.max_profit_per_lot:,.0f}")
        st.metric("R:R Ratio", f"{risk.risk_reward_ratio:.1f}:1")
    with col3:
        st.metric("Net Theta", f"₹{risk.net_theta_per_day:,.0f}/day")
        st.metric("Net Delta", f"{risk.net_delta:+.2f}")
    with col4:
        st.metric("Max Hold", risk.max_hold_duration)
        st.metric("Exit DTE", f"{risk.optimal_exit_dte}")

    if risk.breakeven_points:
        st.caption(f"Breakeven: {', '.join(f'{b:,.0f}' for b in risk.breakeven_points)}")

    if position_sizing:
        st.divider()
        pc = st.columns(4)
        with pc[0]:
            st.metric("Position", f"{position_sizing['position_lots']} lot(s)")
        with pc[1]:
            st.metric("Capital at Risk", f"₹{position_sizing['actual_risk_inr']:,.0f}")
        with pc[2]:
            st.metric("Risk %", f"{position_sizing['actual_risk_pct']}%")
        with pc[3]:
            st.metric("Premium Total", f"₹{position_sizing.get('premium_total',0):,.0f}")

    if risk.adjustment_plan:
        st.caption(f"📌 Adjustment: {risk.adjustment_plan}")
    if risk.execution_notes:
        st.caption(f"📌 Execution: {risk.execution_notes}")


def render_guardrails(guardrail_results: list):
    if not guardrail_results:
        return
    cols = st.columns(5)
    for i, gr in enumerate(guardrail_results):
        with cols[i % 5]:
            icon = "✅" if gr.passed else "🚫"
            name = gr.name.replace("G", "").replace("_", " ", 1)
            st.caption(f"{icon} {name}")


def render_chain_table(chain_snapshot: list):
    """Render option chain as a table."""
    if not chain_snapshot:
        st.info("No chain data")
        return

    import pandas as pd
    df = pd.DataFrame(chain_snapshot)
    display_cols = {
        "strike": "Strike",
        "call_ltp": "CE LTP", "call_iv": "CE IV%", "call_oi": "CE OI", "call_delta": "CE Δ",
        "put_ltp": "PE LTP", "put_iv": "PE IV%", "put_oi": "PE OI", "put_delta": "PE Δ",
    }
    df = df[[c for c in display_cols if c in df.columns]]
    df = df.rename(columns=display_cols)
    st.dataframe(df, use_container_width=True, hide_index=True)


def render_sidebar():
    with st.sidebar:
        st.title("⬡ OPTIONEX")
        render_mode_badge()
        st.divider()
        render_market_status()
        st.divider()
        st.caption("Navigate:")
        selection = st.radio(
            "Navigation",
            options=["Home", "Dashboard", "Signal Engine", "Trade Log", "Settings"],
            label_visibility="collapsed"
        )
        st.divider()
        st.caption(
            f"v1.0 | Phase 1+2 | "
            f"{datetime.now(IST).strftime('%d %b %Y')}"
        )
        return selection
