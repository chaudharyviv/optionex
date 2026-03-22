"""
SWINGTRADE — Agent 3: Swing Risk Assessor
Takes SwingSetupDecision + SwingDataBundle → validates risk and computes position size.
Mirrors options_risk_agent.py pattern.

LLM validates the trade setup parameters.
Position sizing is always done deterministically via get_swing_position_size().
All guardrails are hardcoded — cannot be overridden by LLM output.
"""

import logging
from pydantic import BaseModel
from typing import Optional

from core.llm_client import LLMClient, SwingMarketAnalysis, load_prompt
from config import (
    SWING_MIN_RR,
    SWING_MAX_OPEN_TRADES,
    SWING_VIX_MAX_LONGS,
    get_swing_position_size,
)

logger = logging.getLogger(__name__)


class SwingRiskParameters(BaseModel):
    """Agent 3 LLM output — Swing Risk Assessor."""

    # ── Validated trade levels ────────────────────────────────────
    entry_price:        float
    stop_loss:          float
    target_1:           float
    target_2:           float
    risk_per_share:     float    # entry - stop_loss
    risk_reward_ratio:  float    # to target_1

    # ── Trade context ─────────────────────────────────────────────
    max_hold_days:      int
    exit_strategy:      str      # e.g. "Trail SL to entry after target_1 hit"
    adjustment_plan:    str      # e.g. "Exit half if stock closes below EMA20"
    execution_notes:    str      # slippage, liquidity, order type notes

    # ── Risk flags ────────────────────────────────────────────────
    liquidity_concern:  bool     # True if avg volume < 5L
    sector_risk:        str      # "LOW" | "MEDIUM" | "HIGH"
    event_risk:         str      # e.g. "Budget week" or "None"

    # ── LLM approval ─────────────────────────────────────────────
    risk_approved:      bool
    risk_block_reason:  Optional[str] = None


class SwingRiskAgent:
    """
    Agent 3 — Swing Risk & Execution Assessor.
    Input:  SwingDataBundle + SwingMarketAnalysis + SwingSetupDecision
    Output: {risk_params, position_sizing, final_approved, block_reason}
    """

    SYSTEM_PROMPT = """You are a risk assessor for Indian NSE cash segment swing trades.
You validate trade setup parameters and flag execution concerns.
You DO NOT compute position sizing — that is handled deterministically in code.
You evaluate: price level validity, SL placement quality, R:R accuracy, liquidity.
Respond only in valid JSON. No markdown. No preamble."""

    def __init__(self, llm_client: LLMClient):
        self._llm = llm_client

    def assess(self, bundle, analysis, signal) -> dict:
        """
        Assess risk for the swing signal.
        Returns:
        {
            "risk_params":     SwingRiskParameters,
            "position_sizing": dict from get_swing_position_size(),
            "final_approved":  bool,
            "block_reason":    str or None,
        }
        """
        # AVOID signals skip the LLM call
        if signal.action in ("AVOID", "WATCH"):
            logger.info(f"Signal is {signal.action} — skipping risk assessment")
            return {
                "risk_params":     None,
                "position_sizing": None,
                "final_approved":  False,
                "block_reason":    f"Signal is {signal.action} — no position to size",
            }

        user_prompt = self._build_user_prompt(bundle, analysis, signal)

        logger.info(
            f"SwingRiskAgent running — {bundle.symbol} "
            f"entry={signal.entry_price} SL={signal.stop_loss}"
        )

        risk = self._llm.call(
            system_prompt = self.SYSTEM_PROMPT,
            user_prompt   = user_prompt,
            output_model  = SwingRiskParameters,
            max_tokens    = 800,
            temperature   = 0.1,
        )

        # ── Hardcoded guardrails ──────────────────────────────────
        block_reason = self._run_guardrails(bundle, signal, risk)
        if block_reason:
            risk.risk_approved    = False
            risk.risk_block_reason = block_reason
            logger.warning(f"Guardrail blocked {bundle.symbol}: {block_reason}")

        # ── Deterministic position sizing ─────────────────────────
        position_sizing = None
        if risk.risk_approved:
            position_sizing = get_swing_position_size(
                symbol         = bundle.symbol,
                entry_price    = risk.entry_price,
                stop_loss      = risk.stop_loss,
                signal_quality = signal.signal_quality,
            )
            if position_sizing.get("error"):
                risk.risk_approved    = False
                risk.risk_block_reason = position_sizing["error"]
            else:
                logger.info(
                    f"Position sizing: {position_sizing['shares']} shares | "
                    f"₹{position_sizing['position_value']:,.0f} value | "
                    f"₹{position_sizing['actual_risk_inr']:,.0f} risk "
                    f"({position_sizing['actual_risk_pct']}%)"
                )

        return {
            "risk_params":     risk,
            "position_sizing": position_sizing,
            "final_approved":  risk.risk_approved,
            "block_reason":    risk.risk_block_reason,
        }

    def _run_guardrails(self, bundle, signal, risk) -> Optional[str]:
        """
        Hardcoded guardrails — same philosophy as OptionsRiskEngine.
        Returns block reason string if blocked, None if all pass.
        """
        # G1: R:R floor
        if risk.risk_reward_ratio < SWING_MIN_RR:
            return (
                f"R:R {risk.risk_reward_ratio:.1f}:1 < minimum {SWING_MIN_RR}:1"
            )

        # G2: VIX gate for longs
        if (
            bundle.india_vix
            and bundle.india_vix > SWING_VIX_MAX_LONGS
        ):
            return (
                f"India VIX {bundle.india_vix:.1f} > {SWING_VIX_MAX_LONGS} "
                f"— no new longs in elevated volatility"
            )

        # G3: SL must be below entry for BUY
        if risk.stop_loss >= risk.entry_price:
            return f"Stop loss ₹{risk.stop_loss} must be below entry ₹{risk.entry_price}"

        # G4: Hard filter must have passed
        if not bundle.passes_hard_filters:
            return f"Hard filter: {bundle.filter_fail_reason}"

        # G5: Price floor
        from config import SWING_HARD_FILTERS as HF
        if bundle.spot_price and bundle.spot_price < HF["min_price_inr"]:
            return f"Price ₹{bundle.spot_price:.2f} below minimum ₹{HF['min_price_inr']}"

        # G6: Promoter pledge
        max_pledge = HF.get("max_promoter_pledge_pct", 30)
        if bundle.promoter_pledge and bundle.promoter_pledge > max_pledge:
            return (
                f"Promoter pledge {bundle.promoter_pledge:.1f}% > {max_pledge}% limit"
            )

        # G7: Results blackout
        blackout_days = HF.get("results_blackout_days", 10)
        if (
            bundle.days_to_results is not None
            and bundle.days_to_results <= blackout_days
        ):
            return (
                f"Results in {bundle.days_to_results} days — "
                f"within {blackout_days}-day blackout window"
            )

        return None  # all guardrails passed

    def _build_user_prompt(self, bundle, analysis, signal) -> str:
        tech = bundle.technicals_daily
        atr  = tech.atr_14 if (tech and tech.atr_14) else None

        return f"""Validate this swing trade setup and return risk parameters as JSON.

STOCK:      {bundle.symbol} ({bundle.exchange})
SECTOR:     {bundle.sector or 'Unknown'}
PRICE:      ₹{bundle.spot_price:,.2f}
AVG VOLUME: {(tech.volume_avg_20 or 0):,.0f} shares/day
ATR(14):    ₹{atr:.2f} if {atr} else N/A

PROPOSED TRADE:
  Entry:     ₹{signal.entry_price:,.2f} ({signal.entry_type})
  Stop Loss: ₹{signal.stop_loss:,.2f} ({signal.stop_loss_basis})
  Target 1:  ₹{signal.target_1:,.2f}
  Target 2:  ₹{signal.target_2:,.2f}
  Hold:      {signal.hold_days} trading days
  R:R:       {signal.risk_reward_ratio:.1f}:1
  Quality:   {signal.signal_quality}

ANALYST:
  Setup:     {analysis.setup_type}
  Trend:     {analysis.trend_direction} ({analysis.trend_strength})
  Supports:  {[f'₹{l:,.0f}' for l in analysis.key_support_levels]}
  Resistance:{[f'₹{l:,.0f}' for l in analysis.key_resistance_levels]}

VALIDATE:
1. Is the SL placement logical? (below swing low / EMA / support?)
2. Are targets near actual resistance levels or Fib extensions?
3. Is the R:R calculation correct? (entry-SL vs entry-T1)
4. Any liquidity concerns with avg volume?
5. Sector or event risk to flag?

Return JSON:
{{
    "entry_price": float,
    "stop_loss": float,
    "target_1": float,
    "target_2": float,
    "risk_per_share": float,
    "risk_reward_ratio": float,
    "max_hold_days": integer,
    "exit_strategy": "how to trail SL and when to exit",
    "adjustment_plan": "what to do if trade goes against",
    "execution_notes": "order type, slippage, timing notes",
    "liquidity_concern": true | false,
    "sector_risk": "LOW | MEDIUM | HIGH",
    "event_risk": "description or None",
    "risk_approved": true | false,
    "risk_block_reason": "string if blocked else null"
}}"""
