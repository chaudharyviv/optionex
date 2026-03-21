"""
OPTIONEX — Agent 1: Market & IV Analyst v1.0
Reads the full OptionsDataBundle and produces OptionsMarketAnalysis.
No buy/sell decisions — pure market description.

Includes programmatic sanity checker between Agent 1 and Agent 2
for options-specific contradictions.
"""

import logging
from core.llm_client import LLMClient, OptionsMarketAnalysis, load_prompt
from core.options_data_bundle import OptionsDataBundle

logger = logging.getLogger(__name__)

PROMPT_VERSION = "1.0"


class OptionsAnalystAgent:
    """
    Agent 1 — Market & IV Analyst.
    Input:  OptionsDataBundle
    Output: OptionsMarketAnalysis (validated Pydantic model)
    """

    def __init__(self, llm_client: LLMClient):
        self._llm    = llm_client
        self._prompt = load_prompt("options_analyst", PROMPT_VERSION)

    def analyse(self, bundle: OptionsDataBundle) -> OptionsMarketAnalysis:
        """Run market and IV analysis on the data bundle."""
        user_prompt = self._build_user_prompt(bundle)

        logger.info(
            f"OptionsAnalystAgent running — {bundle.index} "
            f"[{bundle.timeframe}] prompt_v{PROMPT_VERSION}"
        )

        analysis = self._llm.call(
            system_prompt = self._prompt,
            user_prompt   = user_prompt,
            output_model  = OptionsMarketAnalysis,
            max_tokens    = 1500,
            temperature   = 0.2,
        )

        logger.info(
            f"Analysis complete — regime={analysis.market_regime} "
            f"sentiment={analysis.overall_sentiment} "
            f"bias={analysis.recommended_bias}"
        )
        return analysis

    def _build_user_prompt(self, bundle: OptionsDataBundle) -> str:
        return f"""Analyse the following NSE index options market data and return your assessment as JSON.

{bundle.to_prompt_string()}

Return a JSON object with these exact fields:
{{
    "market_regime": "trending_up | trending_down | ranging | volatile",
    "trend_strength": "strong | moderate | weak",
    "key_support_levels": [float, float],
    "key_resistance_levels": [float, float],
    "technical_summary": "3 sentences max describing spot price action",
    "iv_assessment": "2-3 sentences on IV environment — rank, percentile, whether IV is cheap/expensive",
    "iv_regime": "low | normal | high | extreme",
    "iv_skew_interpretation": "1-2 sentences on put-call skew meaning",
    "expected_move": float (± points by nearest expiry from ATM straddle),
    "expected_move_pct": float (as % of spot),
    "pcr_interpretation": "1-2 sentences on what PCR tells us",
    "max_pain_interpretation": "1-2 sentences on max pain vs spot",
    "oi_wall_interpretation": "1-2 sentences on OI concentration walls",
    "fii_interpretation": "1-2 sentences on FII positioning or 'data unavailable'",
    "india_specific_factors": "2 sentences on RBI, events, seasonal",
    "global_risk_factors": "2 sentences on international drivers",
    "high_impact_events_next_24h": "string or null",
    "overall_sentiment": "bullish | bearish | neutral | mixed",
    "sentiment_confidence": integer 0-100,
    "recommended_bias": "buy_premium | sell_premium | directional | hedge",
    "analyst_notes": "anything unusual worth flagging"
}}"""


# ─────────────────────────────────────────────────────────────────
# SANITY CHECKER — OPTIONS v1.0
# ─────────────────────────────────────────────────────────────────

class OptionsSanityChecker:
    """
    Programmatic contradiction check between Agent 1 output and raw data.
    Flags contradictions and injects warnings into Agent 2 prompt.
    No LLM call — pure deterministic code.
    """

    def check(
        self,
        analysis: OptionsMarketAnalysis,
        bundle:   OptionsDataBundle,
    ) -> dict:
        warnings       = []
        confidence_cap = None
        options = bundle.options
        tech    = bundle.technicals

        if not options:
            return {"passed": True, "warnings": [], "confidence_cap": None}

        # ── Check 1: IV regime vs recommended bias ─────
        if (
            analysis.iv_regime in ("high", "extreme")
            and analysis.recommended_bias == "buy_premium"
        ):
            warnings.append(
                f"CONTRADICTION: IV regime is {analysis.iv_regime.upper()} "
                f"(rank={options.iv_rank:.0f}) but recommending buy_premium. "
                f"Buying expensive IV means theta and vega work against you. "
                f"Consider premium selling strategies instead."
            )
            confidence_cap = 55

        # ── Check 2: Low IV vs sell premium ────────────
        if (
            analysis.iv_regime == "low"
            and analysis.recommended_bias == "sell_premium"
        ):
            warnings.append(
                f"CONTRADICTION: IV regime is LOW (rank={options.iv_rank:.0f}) "
                f"but recommending sell_premium. Low IV means limited "
                f"premium to collect and high risk of IV expansion."
            )
            confidence_cap = 55

        # ── Check 3: PCR vs sentiment ──────────────────
        if (
            analysis.overall_sentiment == "bullish"
            and options.pcr_oi < 0.7
        ):
            warnings.append(
                f"CAUTION: Bullish sentiment but PCR={options.pcr_oi:.2f} "
                f"(very low — heavy call buying/put selling). "
                f"Market may be over-positioned on the call side."
            )
            if confidence_cap is None or confidence_cap > 65:
                confidence_cap = 65

        if (
            analysis.overall_sentiment == "bearish"
            and options.pcr_oi > 1.5
        ):
            warnings.append(
                f"CAUTION: Bearish sentiment but PCR={options.pcr_oi:.2f} "
                f"(very high — excessive hedging). "
                f"Heavy put buying often precedes reversals."
            )
            if confidence_cap is None or confidence_cap > 65:
                confidence_cap = 65

        # ── Check 4: Max pain on expiry day ────────────
        if (
            options.dte_nearest <= 1
            and abs(options.max_pain_distance) > options.spot_price * 0.01
        ):
            warnings.append(
                f"EXPIRY DAY: Spot is {options.max_pain_distance:+,.0f} "
                f"away from max pain ({options.max_pain_strike:,.0f}). "
                f"On expiry day, spot often gravitates to max pain."
            )
            if confidence_cap is None or confidence_cap > 60:
                confidence_cap = 60

        # ── Check 5: VIX spike ─────────────────────────
        if (
            bundle.india_vix_change
            and bundle.india_vix_change > 10
        ):
            warnings.append(
                f"VIX SPIKE: India VIX up {bundle.india_vix_change:.1f}% today. "
                f"Avoid buying premium — IV may normalize quickly."
            )
            if confidence_cap is None or confidence_cap > 55:
                confidence_cap = 55

        # ── Check 6: OI wall breach ────────────────────
        if (
            analysis.overall_sentiment == "bullish"
            and options.spot_price > options.highest_call_oi_strike
        ):
            warnings.append(
                f"OI WALL BREACH: Spot ({options.spot_price:,.0f}) above "
                f"call OI wall ({options.highest_call_oi_strike:,.0f}). "
                f"Call writers may be covering — potential gamma squeeze or reversal."
            )

        if (
            analysis.overall_sentiment == "bearish"
            and options.spot_price < options.highest_put_oi_strike
        ):
            warnings.append(
                f"OI WALL BREACH: Spot ({options.spot_price:,.0f}) below "
                f"put OI wall ({options.highest_put_oi_strike:,.0f}). "
                f"Put writers may be covering — potential bounce."
            )

        # ── Check 7: RSI overbought + bullish buy ─────
        if (
            tech and tech.rsi_14
            and tech.rsi_14 > 75
            and analysis.overall_sentiment == "bullish"
            and analysis.recommended_bias == "buy_premium"
        ):
            warnings.append(
                f"EXHAUSTION RISK: RSI={tech.rsi_14} (overbought) with "
                f"bullish buy_premium recommendation. "
                f"Buying calls at RSI extremes is high-risk."
            )
            if confidence_cap is None or confidence_cap > 60:
                confidence_cap = 60

        # ── Check 8: RSI oversold + bearish buy ───────
        if (
            tech and tech.rsi_14
            and tech.rsi_14 < 25
            and analysis.overall_sentiment == "bearish"
            and analysis.recommended_bias == "buy_premium"
        ):
            warnings.append(
                f"EXHAUSTION RISK: RSI={tech.rsi_14} (oversold) with "
                f"bearish buy_premium recommendation. "
                f"Buying puts at RSI extremes is high-risk."
            )
            if confidence_cap is None or confidence_cap > 60:
                confidence_cap = 60

        # ── Check 9: Term structure inversion ──────────
        if options.iv_term_structure == "backwardation":
            warnings.append(
                f"IV TERM STRUCTURE INVERTED (backwardation): "
                f"Near-term IV > far-term IV. Acute event risk. "
                f"Calendar spreads will NOT work. Intraday only."
            )

        # ── Check 10: High impact event ────────────────
        if analysis.high_impact_events_next_24h:
            warnings.append(
                f"HIGH IMPACT EVENT: {analysis.high_impact_events_next_24h} — "
                f"confidence capped at 60%"
            )
            if confidence_cap is None or confidence_cap > 60:
                confidence_cap = 60

        # ── Check 11: ADX contradiction ────────────────
        if (
            tech and tech.adx_14 is not None
            and analysis.market_regime in ("trending_up", "trending_down")
            and hasattr(tech, "adx_signal")
            and tech.adx_signal == "ranging"
        ):
            warnings.append(
                f"CONTRADICTION: regime={analysis.market_regime} but "
                f"ADX={tech.adx_14} [{tech.adx_signal}]. "
                f"ADX below 20 = no meaningful trend."
            )
            if confidence_cap is None or confidence_cap > 60:
                confidence_cap = 60

        # ── Check 12: BB Squeeze ───────────────────────
        if tech and hasattr(tech, "bb_squeeze") and tech.bb_squeeze:
            warnings.append(
                f"BB SQUEEZE ACTIVE: Bollinger Band width at period low. "
                f"Breakout imminent — direction unknown. "
                f"Wait for confirmation before committing."
            )

        passed = len(warnings) == 0

        if warnings:
            logger.warning(
                f"SanityChecker flagged {len(warnings)} issue(s) "
                f"for {bundle.index}"
            )
        else:
            logger.info(f"SanityChecker passed for {bundle.index}")

        return {
            "passed":         passed,
            "warnings":       warnings,
            "confidence_cap": confidence_cap,
        }
