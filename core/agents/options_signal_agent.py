"""
OPTIONEX — Agent 2: Options Strategy Selector v1.0
Takes OptionsMarketAnalysis + OptionsDataBundle and produces a strategy.
Receives sanity checker warnings as additional constraints.

Phase 1 + 2: Defined risk strategies only.
"""

import logging
from core.llm_client import (
    LLMClient, OptionsSignalDecision, OptionsMarketAnalysis, load_prompt,
)
from core.options_data_bundle import OptionsDataBundle

logger = logging.getLogger(__name__)

PROMPT_VERSION = "1.0"


class OptionsSignalAgent:
    """
    Agent 2 — Strategy Selector.
    Input:  OptionsDataBundle + OptionsMarketAnalysis + sanity warnings
    Output: OptionsSignalDecision (validated Pydantic model)
    """

    def __init__(self, llm_client: LLMClient):
        self._llm    = llm_client
        self._prompt = load_prompt("options_signal", PROMPT_VERSION)

    def generate(
        self,
        bundle:         OptionsDataBundle,
        analysis:       OptionsMarketAnalysis,
        sanity_result:  dict,
        trading_style:  str = "system",
    ) -> OptionsSignalDecision:
        """Generate options strategy signal from analysis and data."""
        user_prompt = self._build_user_prompt(
            bundle, analysis, sanity_result, trading_style
        )

        logger.info(
            f"OptionsSignalAgent running — {bundle.index} "
            f"style={trading_style} prompt_v{PROMPT_VERSION}"
        )

        signal = self._llm.call(
            system_prompt = self._prompt,
            user_prompt   = user_prompt,
            output_model  = OptionsSignalDecision,
            max_tokens    = 1500,
            temperature   = 0.2,
        )

        # Apply confidence caps from sanity checker
        cap = sanity_result.get("confidence_cap")
        if cap and signal.confidence > cap:
            logger.warning(
                f"Confidence capped: {signal.confidence} → {cap} (sanity)"
            )
            signal.confidence = cap

        # Bundle-level confidence cap
        if bundle.confidence_cap < 100 and signal.confidence > bundle.confidence_cap:
            signal.confidence = bundle.confidence_cap

        # Validate strategy is in allowlist
        from config import ALLOWED_STRATEGIES
        if signal.strategy_name not in ALLOWED_STRATEGIES and signal.action != "HOLD":
            logger.warning(
                f"Strategy '{signal.strategy_name}' not in allowlist — forcing HOLD"
            )
            signal.action         = "HOLD"
            signal.hold_reasoning = (
                f"Strategy '{signal.strategy_name}' is not in the Phase 1+2 "
                f"allowlist. Only defined-risk strategies are permitted."
            )

        logger.info(
            f"Signal: {signal.action} | {signal.strategy_name} | "
            f"confidence={signal.confidence}% | "
            f"quality={signal.signal_quality}"
        )
        return signal

    def _build_user_prompt(
        self,
        bundle:        OptionsDataBundle,
        analysis:      OptionsMarketAnalysis,
        sanity_result: dict,
        trading_style: str,
    ) -> str:
        options = bundle.options

        # Sanity warning block
        sanity_block = ""
        if sanity_result.get("warnings"):
            sanity_block = "\n⚠ SANITY CHECKER WARNINGS (you MUST consider these):\n"
            for w in sanity_result["warnings"]:
                sanity_block += f"  - {w}\n"
            if sanity_result.get("confidence_cap"):
                sanity_block += (
                    f"  → Maximum confidence allowed: "
                    f"{sanity_result['confidence_cap']}%\n"
                )

        # Style constraint
        style_constraint = {
            "intraday":   "INTRADAY ONLY — only consider setups closeable within today's session (before 3:15 PM IST). Use current week expiry ONLY. Output HOLD if best setup is positional.",
            "expiry_day": "EXPIRY DAY — theta is massive. ONLY consider selling premium or very short-term plays. Do NOT buy premium on expiry day.",
            "positional": "POSITIONAL — 2-7 day holds. Can use next weekly or monthly expiry. Prefer spreads over naked options to control theta.",
            "system":     "MIXED — you decide the best timeframe based on IV regime, DTE, and market conditions.",
        }.get(trading_style, "MIXED — you decide.")

        # Available strikes context
        chain_str = ""
        if options and options.chain_snapshot:
            chain_str = "\n--- AVAILABLE STRIKES (around ATM) ---\n"
            chain_str += (
                f"{'Strike':>8} | {'CE LTP':>8} {'CE IV':>6} {'CE Δ':>6} | "
                f"{'PE LTP':>8} {'PE IV':>6} {'PE Δ':>6}\n"
            )
            for s in options.chain_snapshot:
                chain_str += (
                    f"{s['strike']:>8.0f} | "
                    f"{s.get('call_ltp',0):>8.1f} "
                    f"{s.get('call_iv',0):>5.1f}% "
                    f"{s.get('call_delta',0):>+6.2f} | "
                    f"{s.get('put_ltp',0):>8.1f} "
                    f"{s.get('put_iv',0):>5.1f}% "
                    f"{s.get('put_delta',0):>+6.2f}\n"
                )

        # Expiry info
        expiry_str = ""
        if options:
            expiry_str = (
                f"Nearest expiry: {options.nearest_expiry} ({options.dte_nearest} DTE)\n"
                f"Available expiries: {', '.join(options.available_expiries[:4])}"
            )

        lot_size = bundle.lot_config.get("lot_size", 75) if bundle.lot_config else 75

        return f"""Generate an options trading strategy based on the following data.

TRADING STYLE CONSTRAINT: {style_constraint}

LOT SIZE: {lot_size} units per lot
INDEX: {bundle.index}
SPOT: {bundle.spot_price or 'N/A'}

{expiry_str}

--- ANALYST ASSESSMENT ---
Market Regime:    {analysis.market_regime} ({analysis.trend_strength})
Sentiment:        {analysis.overall_sentiment} ({analysis.sentiment_confidence}%)
IV Assessment:    {analysis.iv_assessment}
IV Regime:        {analysis.iv_regime}
Recommended Bias: {analysis.recommended_bias}
Expected Move:    ±{analysis.expected_move:.0f} pts ({analysis.expected_move_pct:.2f}%)
PCR:              {analysis.pcr_interpretation}
Max Pain:         {analysis.max_pain_interpretation}
OI Walls:         {analysis.oi_wall_interpretation}
Support:          {analysis.key_support_levels}
Resistance:       {analysis.key_resistance_levels}
Events:           {analysis.high_impact_events_next_24h or 'None'}
Notes:            {analysis.analyst_notes}

--- OPTIONS DATA ---
ATM IV:           {options.atm_iv if options else 'N/A'}%
IV Rank:          {options.iv_rank if options else 'N/A'}
IV Percentile:    {options.iv_percentile if options else 'N/A'}
PCR (OI):         {options.pcr_oi if options else 'N/A'}
Max Pain:         {options.max_pain_strike if options else 'N/A'}
Term Structure:   {options.iv_term_structure if options else 'N/A'}
{chain_str}
{sanity_block}

ALLOWED STRATEGIES (Phase 1+2 — defined risk only):
  - long_call, long_put
  - bull_call_spread, bear_put_spread
  - long_straddle, long_strangle
  - iron_condor

STRATEGY SELECTION GUIDE:
  Strong Bullish + IV Low:   long_call
  Strong Bullish + IV High:  bull_call_spread (or bear_put_spread credit)
  Strong Bearish + IV Low:   long_put
  Strong Bearish + IV High:  bear_put_spread (or bull_call_spread credit)
  Neutral + IV Low:          long_straddle or long_strangle
  Neutral + IV High:         iron_condor
  Mixed/Uncertain:           HOLD

For EACH leg, select a SPECIFIC strike from the chain data above.
Use realistic premiums from the LTP column.

Return a JSON object with these exact fields:
{{
    "action": "BUY_PREMIUM | SELL_PREMIUM | DIRECTIONAL | HEDGE | HOLD",
    "direction": "bullish | bearish | neutral | neutral_bullish | neutral_bearish",
    "strategy_name": "long_call | long_put | bull_call_spread | bear_put_spread | long_straddle | long_strangle | iron_condor",
    "strategy_type": "defined_risk",
    "legs": [
        {{
            "option_type": "CE | PE",
            "strike": float,
            "action": "BUY | SELL",
            "expiry": "YYYY-MM-DD",
            "approx_premium": float (per share),
            "delta": float,
            "lots": 1
        }}
    ],
    "confidence": integer 0-100,
    "primary_reason": "single strongest reason",
    "supporting_factors": ["factor1", "factor2"],
    "contradicting_factors": ["factor1"],
    "invalidation_condition": "what would make this signal wrong",
    "recommended_timeframe": "intraday | expiry_day | positional_weekly | positional_monthly",
    "signal_quality": "A | B | C",
    "iv_edge": "describe IV advantage or 'no IV edge'",
    "theta_impact": "describe if theta works for or against",
    "greeks_summary": "net delta, theta, vega for the strategy",
    "hold_reasoning": "string if HOLD, else null"
}}"""
