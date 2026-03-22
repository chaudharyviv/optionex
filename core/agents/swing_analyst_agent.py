"""
SWINGTRADE — Agent 1: Swing Market Analyst
Reads SwingDataBundle → produces SwingMarketAnalysis.
No BUY/SELL decisions — pure market description and setup classification.

Mirrors options_analyst_agent.py pattern exactly:
  - Same LLMClient usage (.call() with Pydantic output model)
  - Same sanity checker pattern between Agent 1 and Agent 2
  - Same logging conventions
"""

import logging
from pydantic import BaseModel
from typing import Optional

from core.llm_client import LLMClient, SwingMarketAnalysis, load_prompt

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# PYDANTIC OUTPUT MODEL — Add to llm_client.py alongside options models
# ─────────────────────────────────────────────────────────────────

class SwingMarketAnalysis(BaseModel):
    """Agent 1 output — Swing Market Analyst."""

    # ── Market regime ─────────────────────────────────────────────
    market_regime:      str     # trending_bull | trending_bear | ranging | volatile
    nifty_context:      str     # above_ema20 | below_ema20 | at_support | at_resistance
    vix_regime:         str     # low | normal | elevated | extreme

    # ── Stock-level trend ─────────────────────────────────────────
    trend_direction:    str     # bullish | bearish | neutral
    trend_strength:     str     # strong | moderate | weak
    above_ema200:       bool    # positional anchor — weekly trend
    ema_alignment:      str     # "20>50>200" | "20>50" | "mixed" | "bearish"

    # ── Setup classification ──────────────────────────────────────
    setup_type:         str     # breakout | pullback_to_ema | reversal | continuation | none
    setup_quality:      str     # "A" | "B" | "C"   (A = textbook, C = marginal)
    key_level:          float   # price level the setup is keyed off (breakout / support)
    entry_rationale:    str     # 1-2 sentences explaining the setup

    # ── Key price levels ─────────────────────────────────────────
    key_support_levels:     list[float]
    key_resistance_levels:  list[float]

    # ── Volume + OI verdict ───────────────────────────────────────
    volume_verdict:     str     # confirmed | weak | absent | divergent
    oi_verdict:         str     # fresh_longs | short_covering | fresh_shorts | neutral | N/A

    # ── Momentum ─────────────────────────────────────────────────
    momentum_verdict:   str     # "bullish" | "bearish" | "mixed" | "neutral"
    rsi_assessment:     str     # 1 sentence on RSI state
    macd_assessment:    str     # 1 sentence on MACD state

    # ── Fibonacci context ─────────────────────────────────────────
    fib_context:        Optional[str] = None   # "near 61.8% support" or None

    # ── Risk factors ─────────────────────────────────────────────
    risk_factors:       list[str]

    # ── Overall bias ─────────────────────────────────────────────
    overall_bias:       str     # bullish | bearish | neutral
    bias_confidence:    int     # 0-100
    primary_thesis:     str     # 2-3 sentence rationale
    analyst_notes:      str     # anything unusual


class SwingAnalystAgent:
    """
    Agent 1 — Swing Market Analyst.
    Input:  SwingDataBundle
    Output: SwingMarketAnalysis (validated Pydantic model)
    """

    SYSTEM_PROMPT = """You are an expert swing trading analyst specialising in Indian cash segment equities (NSE/BSE).
Your role is to analyse market and stock data and produce a structured market description.
You do NOT make BUY or SELL recommendations — that is Agent 2's job.
You focus on: trend direction and strength, setup classification, key price levels, and risk factors.
Respond only in valid JSON matching the requested schema. No markdown, no explanation."""

    def __init__(self, llm_client: LLMClient):
        self._llm = llm_client

    def analyse(self, bundle) -> SwingMarketAnalysis:
        """Run market analysis on a SwingDataBundle."""
        user_prompt = self._build_user_prompt(bundle)

        logger.info(
            f"SwingAnalystAgent running — {bundle.symbol} "
            f"[quality={bundle.data_quality}]"
        )

        analysis = self._llm.call(
            system_prompt = self.SYSTEM_PROMPT,
            user_prompt   = user_prompt,
            output_model  = SwingMarketAnalysis,
            max_tokens    = 1500,
            temperature   = 0.2,
        )

        logger.info(
            f"Swing analysis: {bundle.symbol} | "
            f"regime={analysis.market_regime} | "
            f"setup={analysis.setup_type} | "
            f"quality={analysis.setup_quality} | "
            f"bias={analysis.overall_bias}"
        )
        return analysis

    def _build_user_prompt(self, bundle) -> str:
        return f"""Analyse the following cash segment stock data and return your assessment as JSON.

{bundle.to_prompt_string()}

Return a JSON object with these EXACT fields:
{{
    "market_regime": "trending_bull | trending_bear | ranging | volatile",
    "nifty_context": "above_ema20 | below_ema20 | at_support | at_resistance",
    "vix_regime": "low | normal | elevated | extreme",

    "trend_direction": "bullish | bearish | neutral",
    "trend_strength": "strong | moderate | weak",
    "above_ema200": true | false,
    "ema_alignment": "20>50>200 | 20>50 | mixed | bearish",

    "setup_type": "breakout | pullback_to_ema | reversal | continuation | none",
    "setup_quality": "A | B | C",
    "key_level": float (₹ price),
    "entry_rationale": "1-2 sentences describing WHY this is a valid setup",

    "key_support_levels": [float, float],
    "key_resistance_levels": [float, float],

    "volume_verdict": "confirmed | weak | absent | divergent",
    "oi_verdict": "fresh_longs | short_covering | fresh_shorts | neutral | N/A",

    "momentum_verdict": "bullish | bearish | mixed | neutral",
    "rsi_assessment": "1 sentence on RSI state and what it means",
    "macd_assessment": "1 sentence on MACD state",

    "fib_context": "string describing nearest Fib level or null",

    "risk_factors": ["risk1", "risk2"],

    "overall_bias": "bullish | bearish | neutral",
    "bias_confidence": integer 0-100,
    "primary_thesis": "2-3 sentences explaining the dominant signal",
    "analyst_notes": "anything unusual about this stock's setup"
}}

SETUP CLASSIFICATION GUIDE:
  breakout          — price breaking above significant resistance on volume. Key level = breakout price.
  pullback_to_ema   — price pulling back to EMA20/50 in an uptrend. Key level = EMA being tested.
  reversal          — potential trend change at key support/resistance. Needs RSI divergence or pin bar.
  continuation      — flag/pennant/inside bar after strong move. Trend resumption expected.
  none              — no clear setup. Return overall_bias="neutral" and setup_quality="C".

SETUP QUALITY:
  A — textbook: volume confirmed, EMA aligned, RSI in sweet spot (50-70), Supertrend bullish
  B — mostly there: 2-3 of the above, minor caveats
  C — marginal: 1 criterion, or conflicting signals"""


# ─────────────────────────────────────────────────────────────────
# SWING SANITY CHECKER
# ─────────────────────────────────────────────────────────────────

class SwingSanityChecker:
    """
    Programmatic contradiction check between Agent 1 output and raw data.
    Mirrors OptionsSanityChecker — same pattern, cash-specific rules.
    No LLM call — purely deterministic.
    """

    def check(self, analysis: SwingMarketAnalysis, bundle) -> dict:
        """
        Returns {passed, warnings, confidence_cap}.
        Warnings are injected into Agent 2 prompt.
        """
        warnings       = []
        confidence_cap = None
        tech           = bundle.technicals_daily

        # ── Check 1: Bullish bias in bearish Nifty ─────────────
        if (
            analysis.overall_bias == "bullish"
            and bundle.nifty_trend == "bearish"
        ):
            warnings.append(
                f"MARKET HEADWIND: Bullish bias for {bundle.symbol} but "
                f"Nifty 50 is bearish ({bundle.nifty_ema_signal}). "
                f"Swing longs against broad market have lower success rate."
            )
            if confidence_cap is None or confidence_cap > 65:
                confidence_cap = 65

        # ── Check 2: VIX spike + bullish ──────────────────────
        if (
            bundle.india_vix and bundle.india_vix > 22
            and analysis.overall_bias == "bullish"
        ):
            warnings.append(
                f"VIX ELEVATED ({bundle.india_vix:.1f}): Avoid new swing longs. "
                f"High VIX = high volatility = wider SLs needed = worse R:R."
            )
            if confidence_cap is None or confidence_cap > 60:
                confidence_cap = 60

        # ── Check 3: Breakout without volume ──────────────────
        if (
            analysis.setup_type == "breakout"
            and analysis.volume_verdict in ("weak", "absent")
        ):
            warnings.append(
                f"VOLUME WARNING: Breakout setup but volume is {analysis.volume_verdict}. "
                f"Breakouts on low volume often fail. Wait for volume confirmation."
            )
            if confidence_cap is None or confidence_cap > 65:
                confidence_cap = 65

        # ── Check 4: RSI overbought + bullish entry ────────────
        if (
            tech and tech.rsi_14
            and tech.rsi_14 > 75
            and analysis.overall_bias == "bullish"
        ):
            warnings.append(
                f"OVERBOUGHT: RSI={tech.rsi_14:.1f} > 75 with bullish signal. "
                f"Chasing extended moves increases entry risk. "
                f"Prefer waiting for a pullback to EMA or lower RSI."
            )
            if confidence_cap is None or confidence_cap > 65:
                confidence_cap = 65

        # ── Check 5: Below EMA200 + bullish signal ─────────────
        if (
            not analysis.above_ema200
            and analysis.overall_bias == "bullish"
            and analysis.trend_strength == "strong"
        ):
            warnings.append(
                f"EMA200 RESISTANCE: {bundle.symbol} is below EMA200 on weekly. "
                f"Strong weekly resistance. Reduce confidence and size for longs."
            )
            if confidence_cap is None or confidence_cap > 65:
                confidence_cap = 65

        # ── Check 6: Results blackout ──────────────────────────
        if (
            bundle.days_to_results is not None
            and bundle.days_to_results <= 10
        ):
            warnings.append(
                f"RESULTS WINDOW: Results in {bundle.days_to_results} days "
                f"({bundle.results_date}). "
                f"Binary outcome risk — reduce size or avoid."
            )
            if confidence_cap is None or confidence_cap > 55:
                confidence_cap = 55

        # ── Check 7: ADX ranging + trend signal ───────────────
        if (
            tech and tech.adx_14
            and tech.adx_14 < 20
            and analysis.trend_strength == "strong"
        ):
            warnings.append(
                f"ADX CONTRADICTION: ADX={tech.adx_14:.1f} (ranging) "
                f"but trend_strength=strong. ADX below 20 means no real trend."
            )
            if confidence_cap is None or confidence_cap > 60:
                confidence_cap = 60

        # ── Check 8: Promoter pledge high ─────────────────────
        if bundle.promoter_pledge and bundle.promoter_pledge > 20:
            warnings.append(
                f"PLEDGE RISK: Promoter pledge at {bundle.promoter_pledge:.1f}%. "
                f"High pledge = forced selling risk if stock falls."
            )

        passed = len(warnings) == 0

        if warnings:
            logger.warning(
                f"SwingSanityChecker: {len(warnings)} issue(s) for {bundle.symbol}"
            )

        return {
            "passed":         passed,
            "warnings":       warnings,
            "confidence_cap": confidence_cap,
        }
