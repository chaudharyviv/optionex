"""
OPTIONEX — Agent 3: Options Risk & Execution Assessor v1.0
Takes signal + analysis + data and produces precise risk parameters.
Position sizing is handled by config.get_options_position_size() — not LLM.
"""

import logging
from core.llm_client import (
    LLMClient, OptionsRiskParameters, OptionsMarketAnalysis,
    OptionsSignalDecision, load_prompt,
)
from core.options_data_bundle import OptionsDataBundle
from config import get_options_position_size, NSE_LOT_CONFIG, MIN_RR_RATIO

logger = logging.getLogger(__name__)

PROMPT_VERSION = "1.0"


class OptionsRiskAgent:
    """
    Agent 3 — Options Risk Assessor.
    Input:  OptionsDataBundle + OptionsMarketAnalysis + OptionsSignalDecision
    Output: OptionsRiskParameters + position sizing from config formula
    """

    def __init__(self, llm_client: LLMClient):
        self._llm    = llm_client
        self._prompt = load_prompt("options_risk", PROMPT_VERSION)

    def assess(
        self,
        bundle:   OptionsDataBundle,
        analysis: OptionsMarketAnalysis,
        signal:   OptionsSignalDecision,
    ) -> dict:
        """
        Assess risk for the given options signal.
        Returns combined dict:
        {
            "risk_params":     OptionsRiskParameters,
            "position_sizing": dict from get_options_position_size(),
            "final_approved":  bool,
            "block_reason":    str or None,
        }
        """
        # If HOLD — skip LLM call entirely
        if signal.action == "HOLD":
            logger.info("Signal is HOLD — skipping risk assessment")
            return {
                "risk_params":     None,
                "position_sizing": None,
                "final_approved":  False,
                "block_reason":    "Signal is HOLD — no position to assess",
            }

        user_prompt = self._build_user_prompt(bundle, analysis, signal)

        logger.info(
            f"OptionsRiskAgent running — {bundle.index} "
            f"{signal.strategy_name} prompt_v{PROMPT_VERSION}"
        )

        risk = self._llm.call(
            system_prompt = self._prompt,
            user_prompt   = user_prompt,
            output_model  = OptionsRiskParameters,
            max_tokens    = 1200,
            temperature   = 0.1,
        )

        # ── Deterministic position sizing ──────────────
        position_sizing = None
        if risk.risk_approved and risk.max_loss_per_lot > 0:
            lot_cfg = NSE_LOT_CONFIG.get(bundle.index, {})
            lot_size = lot_cfg.get("lot_size", 75)

            # For buying strategies: premium_per_lot = total_premium_paid
            premium_per_lot = None
            if risk.total_premium_paid > 0:
                premium_per_lot = risk.total_premium_paid

            position_sizing = get_options_position_size(
                index             = bundle.index,
                strategy          = signal.strategy_name,
                max_loss_per_lot  = risk.max_loss_per_lot,
                premium_per_lot   = premium_per_lot,
                signal_quality    = signal.signal_quality,
            )

            if position_sizing.get("risk_blocked"):
                risk.risk_approved    = False
                risk.risk_block_reason = position_sizing["risk_block_reason"]
                logger.warning(f"Position sizing blocked: {risk.risk_block_reason}")
            else:
                logger.info(
                    f"Position sizing: {position_sizing.get('position_lots')} lots | "
                    f"risk=₹{position_sizing.get('actual_risk_inr'):,.0f} "
                    f"({position_sizing.get('actual_risk_pct')}%)"
                )

        # ── Validate R:R ratio ─────────────────────────
        if risk.risk_reward_ratio < MIN_RR_RATIO and risk.risk_approved:
            risk.risk_approved     = False
            risk.risk_block_reason = (
                f"R:R ratio {risk.risk_reward_ratio:.1f} below "
                f"minimum {MIN_RR_RATIO}"
            )
            logger.warning(f"Risk blocked: {risk.risk_block_reason}")

        # ── DTE check for buying ───────────────────────
        from config import MIN_DTE_FOR_BUYING
        if (
            risk.risk_approved
            and signal.action in ("BUY_PREMIUM", "DIRECTIONAL")
            and bundle.options
            and bundle.options.dte_nearest < MIN_DTE_FOR_BUYING
        ):
            # Check if strategy involves buying options
            has_buys = any(
                leg.action == "BUY" for leg in signal.legs
            )
            if has_buys:
                risk.risk_approved     = False
                risk.risk_block_reason = (
                    f"DTE={bundle.options.dte_nearest} < minimum "
                    f"{MIN_DTE_FOR_BUYING} for buying premium. "
                    f"Theta crush risk too high."
                )

        final_approved = risk.risk_approved
        block_reason   = risk.risk_block_reason

        logger.info(
            f"Risk assessment: approved={final_approved} | "
            f"max_loss=₹{risk.max_loss_per_lot:,.0f}/lot | "
            f"RR={risk.risk_reward_ratio:.1f}"
        )

        return {
            "risk_params":     risk,
            "position_sizing": position_sizing,
            "final_approved":  final_approved,
            "block_reason":    block_reason,
        }

    def _build_user_prompt(
        self,
        bundle:   OptionsDataBundle,
        analysis: OptionsMarketAnalysis,
        signal:   OptionsSignalDecision,
    ) -> str:
        options  = bundle.options
        lot_cfg  = NSE_LOT_CONFIG.get(bundle.index, {})
        lot_size = lot_cfg.get("lot_size", 75)
        spot     = bundle.spot_price or 0

        # Format legs for prompt
        legs_str = ""
        for i, leg in enumerate(signal.legs, 1):
            legs_str += (
                f"  Leg {i}: {leg.action} {leg.option_type} {leg.strike} "
                f"@ ₹{leg.approx_premium:.1f} (Δ={leg.delta:+.2f}) "
                f"expiry={leg.expiry}\n"
            )

        return f"""Define risk parameters for this NSE options trade.

INDEX: {bundle.index}
SPOT:  ₹{spot:,.2f}
LOT SIZE: {lot_size}
ATM IV: {options.atm_iv if options else 'N/A'}%
DTE: {options.dte_nearest if options else 'N/A'}

STRATEGY: {signal.strategy_name} ({signal.direction})
{legs_str}

ANALYST SUPPORT: {analysis.key_support_levels}
ANALYST RESISTANCE: {analysis.key_resistance_levels}
EXPECTED MOVE: ±{analysis.expected_move:.0f} pts

SIGNAL:
  Action:     {signal.action}
  Confidence: {signal.confidence}%
  Quality:    {signal.signal_quality}
  Reason:     {signal.primary_reason}
  Timeframe:  {signal.recommended_timeframe}
  IV Edge:    {signal.iv_edge}
  Theta:      {signal.theta_impact}

RISK CALCULATION RULES:
  For long_call / long_put:
    max_loss_per_lot = premium × lot_size
    max_profit_per_lot = theoretical (use expected move × lot_size)
    breakeven = strike ± premium

  For bull_call_spread / bear_put_spread:
    max_loss_per_lot = (spread_width - net_credit) × lot_size  (if debit)
    max_loss_per_lot = (spread_width - net_credit) × lot_size  (if credit)
    max_profit_per_lot = net_credit × lot_size (credit) or (spread_width - net_debit) × lot_size (debit)

  For iron_condor:
    max_loss_per_lot = (wider_spread_width - net_credit) × lot_size
    max_profit_per_lot = net_credit × lot_size

  For long_straddle / long_strangle:
    max_loss_per_lot = total_premium × lot_size
    breakeven_upper = higher_strike + total_premium
    breakeven_lower = lower_strike - total_premium

THETA per day: use the sum of individual leg thetas.

Return a JSON object with these exact fields:
{{
    "strategy_name": "{signal.strategy_name}",
    "legs": [same legs as input with any corrections],
    "max_loss_per_lot": float (₹ per lot),
    "max_profit_per_lot": float (₹ per lot),
    "breakeven_points": [float, ...],
    "risk_reward_ratio": float,
    "total_premium_paid": float (₹ per lot, total buying cost),
    "total_premium_received": float (₹ per lot, total selling credit),
    "net_premium": float (positive=credit, negative=debit, ₹ per lot),
    "net_delta": float (aggregate delta),
    "net_theta_per_day": float (₹/day/lot, negative=costs money),
    "net_vega": float,
    "net_gamma": float,
    "max_hold_duration": "string e.g. until expiry | 2 days | same session",
    "optimal_exit_dte": integer (DTE at which to close),
    "theta_decay_curve": "string e.g. accelerating | linear | favourable",
    "entry_type": "market | limit",
    "margin_required_approx": float (₹),
    "exit_conditions": ["condition1", "condition2"],
    "adjustment_plan": "what to do if trade goes wrong",
    "execution_notes": "slippage, timing, liquidity notes",
    "risk_approved": true | false,
    "risk_block_reason": "string or null"
}}"""
