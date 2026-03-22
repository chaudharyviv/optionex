"""
SWINGTRADE — Agent 2: Setup Selector
Takes SwingMarketAnalysis + SwingDataBundle → produces SwingSetupDecision.
Mirrors options_signal_agent.py pattern.

Responsible for: entry type, entry price, stop loss, targets, hold duration.
All price levels must come from the actual TA data — not LLM imagination.
"""

import logging
from pydantic import BaseModel
from typing import Optional

from core.llm_client import LLMClient, SwingMarketAnalysis, load_prompt

logger = logging.getLogger(__name__)


class SwingSetupDecision(BaseModel):
    """Agent 2 output — Swing Setup Selector."""

    # ── Decision ──────────────────────────────────────────────────
    action:             str       # BUY | WATCH | AVOID
    # Note: SHORT is always AVOID for cash segment delivery

    # ── Entry ─────────────────────────────────────────────────────
    entry_price:        float     # suggested entry (₹)
    entry_type:         str       # "at_market" | "limit" | "breakout_trigger"
    entry_trigger:      Optional[str] = None  # e.g. "buy above ₹2950 on volume"

    # ── Risk levels ───────────────────────────────────────────────
    stop_loss:          float     # ₹ — below last swing low or ATR-based
    stop_loss_basis:    str       # "swing_low" | "atr_2x" | "support_level"
    target_1:           float     # ₹ — R:R 1.5:1 minimum
    target_2:           float     # ₹ — R:R 2.5:1 extended
    target_basis:       str       # "resistance_level" | "fib_extension" | "rr_ratio"

    # ── Trade parameters ──────────────────────────────────────────
    hold_days:          int       # expected hold (trading days) — 5 to 15
    risk_reward_ratio:  float     # to target_1

    # ── Quality and confidence ────────────────────────────────────
    confidence:         int       # 0-100
    signal_quality:     str       # "A" | "B" | "C"
    confluence_score:   float     # 0.0 to 1.0

    # ── Rationale ─────────────────────────────────────────────────
    primary_reason:     str       # single strongest reason
    supporting_factors: list[str]
    contradicting_factors: list[str]
    invalidation_condition: str   # what would make this signal wrong

    # ── Hold logic ────────────────────────────────────────────────
    exit_plan:          str       # "trail SL after target_1" | "book half at target_1"
    watch_reasoning:    Optional[str] = None   # if WATCH — what to wait for
    avoid_reasoning:    Optional[str] = None   # if AVOID — why


class SwingSetupAgent:
    """
    Agent 2 — Swing Setup Selector.
    Input:  SwingDataBundle + SwingMarketAnalysis + sanity result
    Output: SwingSetupDecision (validated Pydantic model)
    """

    SYSTEM_PROMPT = """You are an expert swing trade setup selector for Indian NSE/BSE cash equities.
You receive market analysis and technical data, then decide whether to BUY, WATCH, or AVOID.
CRITICAL RULES:
  1. SHORT (selling short) is NOT ALLOWED in cash segment delivery. Use AVOID if bearish.
  2. All price levels (entry, SL, targets) must be derived from the technical data provided.
  3. Minimum R:R to target_1 must be 2.0. Below 2.0 = AVOID.
  4. Respect the sanity checker warnings — they are hard constraints.
  5. WATCH means "setup not ready yet, specific trigger needed before entry".
Respond only in valid JSON. No markdown."""

    def __init__(self, llm_client: LLMClient):
        self._llm = llm_client

    def generate(
        self,
        bundle,
        analysis,
        sanity_result: dict,
        trading_style: str = "swing",
    ) -> SwingSetupDecision:
        """Generate setup decision from analysis and data."""
        from config import ALLOWED_STRATEGIES   # not used but kept for symmetry
        user_prompt = self._build_user_prompt(bundle, analysis, sanity_result)

        logger.info(
            f"SwingSetupAgent running — {bundle.symbol} "
            f"bias={analysis.overall_bias} setup={analysis.setup_type}"
        )

        signal = self._llm.call(
            system_prompt = self.SYSTEM_PROMPT,
            user_prompt   = user_prompt,
            output_model  = SwingSetupDecision,
            max_tokens    = 1200,
            temperature   = 0.2,
        )

        # Apply confidence caps from sanity checker
        cap = sanity_result.get("confidence_cap")
        if cap and signal.confidence > cap:
            logger.warning(f"Confidence capped: {signal.confidence} → {cap}")
            signal.confidence = cap

        # Bundle-level cap
        if bundle.confidence_cap < 100 and signal.confidence > bundle.confidence_cap:
            signal.confidence = bundle.confidence_cap

        # Enforce R:R floor
        if (
            signal.action == "BUY"
            and signal.risk_reward_ratio < 2.0
        ):
            logger.warning(
                f"R:R {signal.risk_reward_ratio:.1f} < 2.0 — converting to AVOID"
            )
            signal.action          = "AVOID"
            signal.avoid_reasoning = (
                f"R:R {signal.risk_reward_ratio:.1f}:1 is below minimum 2.0:1. "
                f"Setup doesn't offer adequate reward for the risk."
            )

        logger.info(
            f"Setup: {bundle.symbol} | {signal.action} | "
            f"entry={signal.entry_price} SL={signal.stop_loss} "
            f"T1={signal.target_1} | "
            f"confidence={signal.confidence}% | quality={signal.signal_quality}"
        )
        return signal

    def _build_user_prompt(self, bundle, analysis, sanity_result: dict) -> str:
        tech = bundle.technicals_daily

        # Sanity warning block
        sanity_block = ""
        if sanity_result.get("warnings"):
            sanity_block = "\n⚠ SANITY CHECKER WARNINGS (MANDATORY CONSTRAINTS):\n"
            for w in sanity_result["warnings"]:
                sanity_block += f"  - {w}\n"
            if sanity_result.get("confidence_cap"):
                sanity_block += (
                    f"  → Maximum confidence: {sanity_result['confidence_cap']}%\n"
                )

        atr = f"₹{tech.atr_14:.2f}" if (tech and tech.atr_14) else "N/A"
        spot = bundle.spot_price or 0

        return f"""Generate a swing trade setup decision for {bundle.symbol}.

STOCK: {bundle.symbol} ({bundle.exchange}) | Sector: {bundle.sector or 'Unknown'}
PRICE: ₹{spot:,.2f}
ATR(14): {atr}   ← Use this for SL calculation: SL = entry - 2×ATR

--- ANALYST ASSESSMENT ---
Market Regime:     {analysis.market_regime}
Nifty Context:     {analysis.nifty_context}
Trend Direction:   {analysis.trend_direction} ({analysis.trend_strength})
Above EMA200:      {analysis.above_ema200}
EMA Alignment:     {analysis.ema_alignment}
Setup Type:        {analysis.setup_type} (quality: {analysis.setup_quality})
Key Level:         ₹{analysis.key_level:,.2f}
Entry Rationale:   {analysis.entry_rationale}
Support Levels:    {[f'₹{l:,.0f}' for l in analysis.key_support_levels]}
Resistance Levels: {[f'₹{l:,.0f}' for l in analysis.key_resistance_levels]}
Volume:            {analysis.volume_verdict}
OI:                {analysis.oi_verdict}
Momentum:          {analysis.momentum_verdict}
RSI:               {analysis.rsi_assessment}
MACD:              {analysis.macd_assessment}
Fib:               {analysis.fib_context or 'N/A'}
Overall Bias:      {analysis.overall_bias} ({analysis.bias_confidence}%)
Primary Thesis:    {analysis.primary_thesis}
Risk Factors:      {analysis.risk_factors}
{sanity_block}

STOP LOSS RULES (use in order of preference):
  1. Below last swing low (from Fib data or key support levels)
  2. Below EMA20 if pullback setup
  3. Entry price - 2×ATR as fallback

TARGET RULES:
  target_1 = entry + (entry - stop_loss) × 2.0   (minimum R:R 2.0)
  target_2 = entry + (entry - stop_loss) × 3.0   (or nearest key resistance)

HOLD DURATION:
  5-15 trading days typical. Shorter for breakouts, longer for positional setups.

Return JSON with these EXACT fields:
{{
    "action": "BUY | WATCH | AVOID",
    "entry_price": float,
    "entry_type": "at_market | limit | breakout_trigger",
    "entry_trigger": "string describing exact trigger or null",
    "stop_loss": float,
    "stop_loss_basis": "swing_low | atr_2x | support_level",
    "target_1": float,
    "target_2": float,
    "target_basis": "resistance_level | fib_extension | rr_ratio",
    "hold_days": integer (5-15),
    "risk_reward_ratio": float (to target_1),
    "confidence": integer 0-100,
    "signal_quality": "A | B | C",
    "confluence_score": float 0.0-1.0,
    "primary_reason": "single strongest reason",
    "supporting_factors": ["factor1", "factor2"],
    "contradicting_factors": ["factor1"],
    "invalidation_condition": "what makes this signal wrong",
    "exit_plan": "how to manage the trade after entry",
    "watch_reasoning": "string if WATCH else null",
    "avoid_reasoning": "string if AVOID else null"
}}"""
