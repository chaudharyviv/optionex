"""
OPTIONEX — LLM Client Abstraction
Supports OpenAI (demo mode) and Anthropic Claude (paper/production).
Switched via config.TRADING_MODE — zero code changes needed.

Same retry-and-repair pattern as COMMODEX.
Extended Pydantic models for options-specific agent outputs.
"""

import os
import json
import logging
import re
from typing import Optional, Type
from pydantic import BaseModel, ValidationError
from openai import OpenAI
from anthropic import Anthropic
from config import TRADING_MODE, ACTIVE_LLM, PROMPTS_DIR

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# PYDANTIC OUTPUT MODELS — OPTIONS
# ─────────────────────────────────────────────────────────────────

class OptionsMarketAnalysis(BaseModel):
    """Agent 1 output — Market & IV Analyst."""
    # Underlying regime
    market_regime:              str       # trending_up | trending_down | ranging | volatile
    trend_strength:             str       # strong | moderate | weak
    key_support_levels:         list[float]
    key_resistance_levels:      list[float]
    technical_summary:          str

    # Options-specific
    iv_assessment:              str
    iv_regime:                  str       # low | normal | high | extreme
    iv_skew_interpretation:     str
    expected_move:              float     # ± points by nearest expiry
    expected_move_pct:          float

    # Sentiment
    pcr_interpretation:         str
    max_pain_interpretation:    str
    oi_wall_interpretation:     str
    fii_interpretation:         str

    # Events
    india_specific_factors:     str
    global_risk_factors:        str
    high_impact_events_next_24h: Optional[str] = None

    # Overall
    overall_sentiment:          str       # bullish | bearish | neutral | mixed
    sentiment_confidence:       int       # 0-100
    recommended_bias:           str       # buy_premium | sell_premium | directional | hedge
    analyst_notes:              str


class StrategyLeg(BaseModel):
    """Single leg of an options strategy."""
    option_type:    str       # CE | PE
    strike:         float
    action:         str       # BUY | SELL
    expiry:         str
    approx_premium: float
    delta:          float
    lots:           int = 1


class OptionsSignalDecision(BaseModel):
    """Agent 2 output — Strategy Selector."""
    action:                     str       # BUY_PREMIUM | SELL_PREMIUM | DIRECTIONAL | HEDGE | HOLD
    direction:                  str       # bullish | bearish | neutral | neutral_bullish | neutral_bearish
    strategy_name:              str       # long_call | bull_call_spread | iron_condor | etc.
    strategy_type:              str       # defined_risk | undefined_risk
    legs:                       list[StrategyLeg]

    confidence:                 int       # 0-100
    primary_reason:             str
    supporting_factors:         list[str]
    contradicting_factors:      list[str]
    invalidation_condition:     str
    recommended_timeframe:      str       # intraday | expiry_day | positional_weekly | positional_monthly
    signal_quality:             str       # A | B | C

    iv_edge:                    str
    theta_impact:               str
    greeks_summary:             str
    hold_reasoning:             Optional[str] = None


class OptionsRiskParameters(BaseModel):
    """Agent 3 output — Options Risk Assessor."""
    strategy_name:              str
    legs:                       list[StrategyLeg]

    max_loss_per_lot:           float
    max_profit_per_lot:         float
    breakeven_points:           list[float]
    risk_reward_ratio:          float

    total_premium_paid:         float
    total_premium_received:     float
    net_premium:                float     # positive = credit, negative = debit

    net_delta:                  float
    net_theta_per_day:          float
    net_vega:                   float
    net_gamma:                  float

    max_hold_duration:          str
    optimal_exit_dte:           int
    theta_decay_curve:          str

    entry_type:                 str       # market | limit
    margin_required_approx:     float
    exit_conditions:            list[str]
    adjustment_plan:            str
    execution_notes:            str

    risk_approved:              bool
    risk_block_reason:          Optional[str] = None


# ─────────────────────────────────────────────────────────────────
# PYDANTIC OUTPUT MODELS — SWING TRADING (CASH SEGMENT)
# ─────────────────────────────────────────────────────────────────

class SwingMarketAnalysis(BaseModel):
    """Agent 1 output — Swing Market Analyst."""
    market_regime:          str
    nifty_context:          str
    vix_regime:             str
    trend_direction:        str
    trend_strength:         str
    above_ema200:           bool
    ema_alignment:          str
    setup_type:             str
    setup_quality:          str
    key_level:              float
    entry_rationale:        str
    key_support_levels:     list[float]
    key_resistance_levels:  list[float]
    volume_verdict:         str
    oi_verdict:             str
    momentum_verdict:       str
    rsi_assessment:         str
    macd_assessment:        str
    fib_context:            Optional[str] = None
    risk_factors:           list[str]
    overall_bias:           str
    bias_confidence:        int
    primary_thesis:         str
    analyst_notes:          str


class SwingSetupDecision(BaseModel):
    """Agent 2 output — Swing Setup Selector."""
    action:                 str
    entry_price:            float
    entry_type:             str
    entry_trigger:          Optional[str] = None
    stop_loss:              float
    stop_loss_basis:        str
    target_1:               float
    target_2:               float
    target_basis:           str
    hold_days:              int
    risk_reward_ratio:      float
    confidence:             int
    signal_quality:         str
    confluence_score:       float
    primary_reason:         str
    supporting_factors:     list[str]
    contradicting_factors:  list[str]
    invalidation_condition: str
    exit_plan:              str
    watch_reasoning:        Optional[str] = None
    avoid_reasoning:        Optional[str] = None


class SwingRiskParameters(BaseModel):
    """Agent 3 output — Swing Risk Assessor."""
    entry_price:            float
    stop_loss:              float
    target_1:               float
    target_2:               float
    risk_per_share:         float
    risk_reward_ratio:      float
    max_hold_days:          int
    exit_strategy:          str
    adjustment_plan:        str
    execution_notes:        str
    liquidity_concern:      bool
    sector_risk:            str
    event_risk:             str
    risk_approved:          bool
    risk_block_reason:      Optional[str] = None


# ─────────────────────────────────────────────────────────────────
# PROMPT LOADER
# ─────────────────────────────────────────────────────────────────

def load_prompt(agent_name: str, version: str = "1.0") -> str:
    """Load a versioned prompt from the prompts/ directory."""
    prompt_file = PROMPTS_DIR / f"{agent_name}_v{version}.txt"
    try:
        return prompt_file.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        logger.warning(f"Prompt file not found: {prompt_file}")
        return f"You are a {agent_name} for NSE index options trading. Respond only in JSON."


# ─────────────────────────────────────────────────────────────────
# LLM CLIENT
# ─────────────────────────────────────────────────────────────────

class LLMClient:
    """
    Unified LLM interface for OpenAI and Anthropic.
    Same pattern as COMMODEX — works for options without changes.
    """

    def __init__(self):
        self.provider = ACTIVE_LLM["provider"]
        self.model    = ACTIVE_LLM["model"]
        self.api_key  = ACTIVE_LLM["api_key"]

        if self.provider == "openai":
            self._client = OpenAI(api_key=self.api_key)
        elif self.provider == "anthropic":
            self._client = Anthropic(api_key=self.api_key)
        else:
            raise ValueError(f"Unknown LLM provider: {self.provider}")

        logger.info(
            f"LLMClient ready — provider={self.provider} "
            f"model={self.model} mode={TRADING_MODE}"
        )

    def call(
        self,
        system_prompt: str,
        user_prompt:   str,
        output_model:  Type[BaseModel],
        max_tokens:    int = 1500,
        temperature:   float = 0.2,
    ) -> BaseModel:
        """
        Call LLM and return validated Pydantic model.
        Retry-with-repair on parse/validation failure.
        """
        raw_response = self._call_llm(
            system_prompt, user_prompt, max_tokens, temperature
        )

        # First parse attempt
        try:
            return self._parse_and_validate(raw_response, output_model)
        except (json.JSONDecodeError, ValidationError) as e:
            logger.warning(
                f"First parse failed ({type(e).__name__}): {e}\n"
                f"Attempting repair..."
            )

        # Repair attempt
        repair_prompt = f"""The following JSON is malformed or missing required fields.
Fix it to exactly match this Pydantic schema:

{output_model.model_json_schema()}

Malformed JSON:
{raw_response}

Return ONLY the corrected JSON. No explanation, no markdown."""

        repaired = self._call_llm(
            "You are a JSON repair assistant. Fix JSON to match the given schema exactly.",
            repair_prompt, max_tokens, 0.0,
        )

        try:
            return self._parse_and_validate(repaired, output_model)
        except (json.JSONDecodeError, ValidationError) as e:
            logger.error(f"Repair attempt also failed: {e}")
            raise ValueError(
                f"LLM output could not be parsed after repair attempt.\n"
                f"Provider: {self.provider}\n"
                f"Error: {e}\n"
                f"Raw: {repaired[:500]}"
            )

    def _call_llm(
        self,
        system_prompt: str,
        user_prompt:   str,
        max_tokens:    int,
        temperature:   float,
    ) -> str:
        """Raw LLM call — returns string response."""
        try:
            if self.provider == "openai":
                response = self._client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system",  "content": system_prompt},
                        {"role": "user",    "content": user_prompt},
                    ],
                    max_tokens=max_tokens,
                    temperature=temperature,
                    response_format={"type": "json_object"},
                )
                return response.choices[0].message.content

            elif self.provider == "anthropic":
                response = self._client.messages.create(
                    model=self.model,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    system=system_prompt,
                    messages=[
                        {"role": "user", "content": user_prompt}
                    ],
                )
                return response.content[0].text

        except Exception as e:
            logger.error(f"LLM call failed ({self.provider}): {e}")
            raise

    def _parse_and_validate(
        self,
        raw: str,
        output_model: Type[BaseModel],
    ) -> BaseModel:
        """Extract JSON from response and validate."""
        clean = re.sub(r"```(?:json)?", "", raw).strip()
        clean = clean.strip("`").strip()

        start = clean.find("{")
        end   = clean.rfind("}") + 1
        if start == -1 or end == 0:
            raise json.JSONDecodeError("No JSON object found", clean, 0)

        json_str = clean[start:end]
        data     = json.loads(json_str)
        return output_model.model_validate(data)
