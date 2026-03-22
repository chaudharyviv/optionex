"""
OPTIONEX — Options Signal Orchestrator
Wires together the full 3-agent pipeline.
Single entry point for signal generation.

Flow:
  OptionsDataBundle → Agent1 (Analyst) → SanityChecker →
  Agent2 (Strategy) → Agent3 (Risk) → OptionsSignalResult
"""

import json
import logging
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional

from core.llm_client import (
    LLMClient, OptionsMarketAnalysis,
    OptionsSignalDecision, OptionsRiskParameters,
)
from core.agents.options_analyst_agent import OptionsAnalystAgent, OptionsSanityChecker
from core.agents.options_signal_agent import OptionsSignalAgent
from core.agents.options_risk_agent import OptionsRiskAgent
from core.options_data_bundle import OptionsDataBundle, OptionsDataBundleAssembler
from core.options_engine import OptionsEngine
from config import (
    TRADING_MODE, ACTIVE_LLM,
    MIN_CONFIDENCE_THRESHOLD,
)

logger = logging.getLogger(__name__)


@dataclass
class OptionsSignalResult:
    """Complete output from one options signal generation run."""
    # Request
    index:            str
    timeframe:        str
    trading_style:    str
    mode:             str
    llm_provider:     str
    llm_model:        str
    timestamp:        str = field(
        default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

    # Agent outputs
    analysis:         Optional[OptionsMarketAnalysis]  = None
    signal:           Optional[OptionsSignalDecision]  = None
    risk:             Optional[OptionsRiskParameters]   = None
    position_sizing:  Optional[dict]                   = None

    # Final decision
    final_action:     str  = "HOLD"
    final_confidence: int  = 0
    approved:         bool = False
    block_reason:     Optional[str] = None

    # Strategy details
    strategy_name:    str = "none"
    direction:        str = "neutral"
    legs_json:        str = "[]"

    # Sanity check
    sanity_passed:    bool = True
    sanity_warnings:  list = field(default_factory=list)

    # Data quality
    data_quality:     str  = "unknown"
    confidence_cap:   int  = 100
    spot_price:       Optional[float] = None

    # Options context (for guardrail pass-through)
    atm_iv:           Optional[float] = None
    iv_rank:          Optional[float] = None
    iv_percentile:    Optional[float] = None
    pcr_oi:           Optional[float] = None
    dte_nearest:      Optional[int]   = None
    india_vix:        Optional[float] = None
    india_vix_change: Optional[float] = None
    expiry_date:      Optional[str]   = None

    # Error tracking
    error:            Optional[str] = None
    pipeline_stage:   str  = "complete"

    def to_display_dict(self) -> dict:
        """Clean dict for Streamlit display."""
        return {
            "index":          self.index,
            "timestamp":      self.timestamp,
            "action":         self.final_action,
            "strategy":       self.strategy_name,
            "direction":      self.direction,
            "confidence":     self.final_confidence,
            "quality":        self.signal.signal_quality if self.signal else "N/A",
            "approved":       self.approved,
            "regime":         self.analysis.market_regime if self.analysis else "N/A",
            "sentiment":      self.analysis.overall_sentiment if self.analysis else "N/A",
            "iv_regime":      self.analysis.iv_regime if self.analysis else "N/A",
            "primary_reason": self.signal.primary_reason if self.signal else "N/A",
            "max_loss":       self.risk.max_loss_per_lot if self.risk else None,
            "max_profit":     self.risk.max_profit_per_lot if self.risk else None,
            "rr_ratio":       self.risk.risk_reward_ratio if self.risk else None,
            "lots":           self.position_sizing.get("position_lots") if self.position_sizing else None,
            "risk_inr":       self.position_sizing.get("actual_risk_inr") if self.position_sizing else None,
            "block_reason":   self.block_reason,
            "data_quality":   self.data_quality,
            "spot":           self.spot_price,
        }


class OptionsSignalOrchestrator:
    """
    Orchestrates the full options signal generation pipeline.
    Handles partial failures gracefully.
    Default output is always HOLD on any failure.
    """

    def __init__(self, groww_client, tech_engine, news_client):
        self._llm       = LLMClient()
        self._analyst   = OptionsAnalystAgent(self._llm)
        self._sanity    = OptionsSanityChecker()
        self._signal    = OptionsSignalAgent(self._llm)
        self._risk      = OptionsRiskAgent(self._llm)
        self._options   = OptionsEngine()
        self._assembler = OptionsDataBundleAssembler(
            groww_client, tech_engine, self._options, news_client,
        )
        logger.info(
            f"OptionsSignalOrchestrator ready — "
            f"mode={TRADING_MODE} provider={ACTIVE_LLM['provider']}"
        )

    def generate(
        self,
        index:         str,
        timeframe:     str = "15minute",
        trading_style: str = "system",
    ) -> OptionsSignalResult:
        """
        Run the full 3-agent pipeline for one index.
        Returns OptionsSignalResult — never raises.
        """
        result = OptionsSignalResult(
            index         = index,
            timeframe     = timeframe,
            trading_style = trading_style,
            mode          = TRADING_MODE,
            llm_provider  = ACTIVE_LLM["provider"],
            llm_model     = ACTIVE_LLM["model"],
        )

        # ── Stage 1: Data Bundle ───────────────────────
        try:
            logger.info(f"Stage 1: Assembling data bundle for {index}")
            bundle = self._assembler.assemble(
                index=index,
                timeframe=timeframe,
                trading_style=trading_style,
            )
            result.data_quality   = bundle.data_quality
            result.confidence_cap = bundle.confidence_cap
            result.spot_price     = bundle.spot_price

            # Options context for guardrails
            if bundle.options_ok and bundle.options:
                result.atm_iv         = bundle.options.atm_iv
                result.iv_rank        = bundle.options.iv_rank
                result.iv_percentile  = bundle.options.iv_percentile
                result.pcr_oi         = bundle.options.pcr_oi
                result.dte_nearest    = bundle.options.dte_nearest
                result.expiry_date    = bundle.options.nearest_expiry
            result.india_vix        = bundle.india_vix
            result.india_vix_change = bundle.india_vix_change

            result.pipeline_stage = "data_complete"
        except Exception as e:
            result.error          = f"Data assembly failed: {e}"
            result.pipeline_stage = "data_failed"
            result.block_reason   = "Data unavailable — defaulting to HOLD"
            logger.error(result.error)
            return result

        # ── Stage 2: Agent 1 — Market & IV Analyst ─────
        try:
            logger.info("Stage 2: Running Market & IV Analyst")
            analysis        = self._analyst.analyse(bundle)
            result.analysis = analysis
            result.pipeline_stage = "analyst_complete"
        except Exception as e:
            result.error          = f"Analyst agent failed: {e}"
            result.pipeline_stage = "analyst_failed"
            result.block_reason   = "Analysis failed — defaulting to HOLD"
            logger.error(result.error)
            return result

        # ── Stage 2b: Sanity Checker ───────────────────
        sanity = self._sanity.check(analysis, bundle)
        result.sanity_passed   = sanity["passed"]
        result.sanity_warnings = sanity["warnings"]

        # ── Stage 3: Agent 2 — Strategy Selector ──────
        try:
            logger.info("Stage 3: Running Strategy Selector")
            signal        = self._signal.generate(
                bundle, analysis, sanity, trading_style
            )
            result.signal        = signal
            result.strategy_name = signal.strategy_name
            result.direction     = signal.direction
            result.legs_json     = json.dumps(
                [leg.model_dump() for leg in signal.legs]
            ) if signal.legs else "[]"
            result.pipeline_stage = "signal_complete"
        except Exception as e:
            result.error          = f"Signal agent failed: {e}"
            result.pipeline_stage = "signal_failed"
            result.block_reason   = "Signal generation failed — defaulting to HOLD"
            logger.error(result.error)
            return result

        # ── Confidence threshold check ─────────────────
        if signal.confidence < MIN_CONFIDENCE_THRESHOLD:
            result.final_action     = "HOLD"
            result.final_confidence = signal.confidence
            result.block_reason     = (
                f"Confidence {signal.confidence}% below "
                f"minimum threshold {MIN_CONFIDENCE_THRESHOLD}%"
            )
            logger.info(result.block_reason)
            return result

        # ── Stage 4: Agent 3 — Risk Assessor ──────────
        try:
            logger.info("Stage 4: Running Risk Assessor")
            risk_result            = self._risk.assess(bundle, analysis, signal)
            result.risk            = risk_result["risk_params"]
            result.position_sizing = risk_result["position_sizing"]
            result.approved        = risk_result["final_approved"]
            result.block_reason    = risk_result["block_reason"]
            result.pipeline_stage  = "risk_complete"
        except Exception as e:
            result.error          = f"Risk agent failed: {e}"
            result.pipeline_stage = "risk_failed"
            result.block_reason   = "Risk assessment failed — defaulting to HOLD"
            logger.error(result.error)
            return result

        # ── Final signal ───────────────────────────────
        result.final_action     = signal.action if result.approved else "HOLD"
        result.final_confidence = signal.confidence
        result.pipeline_stage   = "complete"

        logger.info(
            f"Pipeline complete: {index} | "
            f"{result.final_action} | {result.strategy_name} | "
            f"confidence={result.final_confidence}% | "
            f"approved={result.approved}"
        )

        # ── Archive IV data ────────────────────────────
        # Records daily ATM IV for IV rank computation.
        # Non-critical — failure doesn't affect signal output.
        try:
            from core.iv_archiver import archive_from_bundle
            archive_from_bundle(bundle)
        except Exception as e:
            logger.warning(f"IV archival failed (non-critical): {e}")

        return result



# ─────────────────────────────────────────────────────────────────
# SWING ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────

from core.llm_client import (
    SwingMarketAnalysis,
    SwingSetupDecision,
    SwingRiskParameters,
)
from core.agents.swing_analyst_agent import SwingAnalystAgent, SwingSanityChecker
from core.agents.swing_setup_agent import SwingSetupAgent
from core.agents.swing_risk_agent import SwingRiskAgent
from core.swing_data_bundle import SwingDataBundleAssembler
from config import SWING_MIN_CONFIDENCE


@dataclass
class SwingSignalResult:
    """
    Complete output from one swing signal generation run.
    Mirrors OptionsSignalResult field-for-field.
    """
    # ── Request ───────────────────────────────────────────────────
    symbol:           str
    exchange:         str
    mode:             str
    llm_provider:     str
    llm_model:        str
    timestamp:        str = field(
        default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

    # ── Agent outputs ─────────────────────────────────────────────
    analysis:         Optional[SwingMarketAnalysis]  = None
    signal:           Optional[SwingSetupDecision]   = None
    risk:             Optional[SwingRiskParameters]  = None
    position_sizing:  Optional[dict]                 = None

    # ── Final decision ────────────────────────────────────────────
    final_action:     str  = "AVOID"
    final_confidence: int  = 0
    approved:         bool = False
    block_reason:     Optional[str] = None

    # ── Setup details ─────────────────────────────────────────────
    setup_type:       str  = "none"
    direction:        str  = "neutral"
    signal_quality:   str  = "C"

    # ── Price levels ──────────────────────────────────────────────
    entry_price:      Optional[float] = None
    stop_loss:        Optional[float] = None
    target_1:         Optional[float] = None
    target_2:         Optional[float] = None
    hold_days:        Optional[int]   = None
    risk_reward:      Optional[float] = None
    sector:           Optional[str]   = None

    # ── Sanity check ──────────────────────────────────────────────
    sanity_passed:    bool = True
    sanity_warnings:  list = field(default_factory=list)

    # ── Data quality ──────────────────────────────────────────────
    data_quality:     str  = "unknown"
    confidence_cap:   int  = 100
    spot_price:       Optional[float] = None

    # ── Market context (pass-through for Streamlit display) ───────
    india_vix:        Optional[float] = None
    nifty_trend:      Optional[str]   = None
    market_regime:    Optional[str]   = None

    # ── Error tracking ────────────────────────────────────────────
    error:            Optional[str] = None
    pipeline_stage:   str  = "complete"

    def to_display_dict(self) -> dict:
        """Clean dict for Streamlit display — mirrors OptionsSignalResult.to_display_dict()."""
        return {
            "symbol":         self.symbol,
            "exchange":       self.exchange,
            "timestamp":      self.timestamp,
            "action":         self.final_action,
            "setup":          self.setup_type,
            "quality":        self.signal_quality,
            "confidence":     self.final_confidence,
            "approved":       self.approved,
            "entry":          self.entry_price,
            "sl":             self.stop_loss,
            "target_1":       self.target_1,
            "target_2":       self.target_2,
            "rr":             self.risk_reward,
            "hold_days":      self.hold_days,
            "shares":         self.position_sizing.get("shares") if self.position_sizing else None,
            "position_value": self.position_sizing.get("position_value") if self.position_sizing else None,
            "risk_inr":       self.position_sizing.get("actual_risk_inr") if self.position_sizing else None,
            "risk_pct":       self.position_sizing.get("actual_risk_pct") if self.position_sizing else None,
            "sector":         self.sector,
            "regime":         self.market_regime,
            "vix":            self.india_vix,
            "block_reason":   self.block_reason,
            "data_quality":   self.data_quality,
            "spot":           self.spot_price,
            "primary_reason": self.signal.primary_reason if self.signal else "N/A",
        }


class SwingSignalOrchestrator:
    """
    Orchestrates the full swing signal generation pipeline.
    Handles partial failures gracefully.
    Default output is always AVOID on any failure.

    Shared instances with OPTIONEX:
      groww_client — same GrowwClient instance (token already refreshed)
      tech_engine  — same TechnicalEngine instance
      news_client  — same NewsClient instance (optional)
    """

    def __init__(self, groww_client, tech_engine, news_client=None):
        self._llm       = LLMClient()
        self._analyst   = SwingAnalystAgent(self._llm)
        self._sanity    = SwingSanityChecker()
        self._setup     = SwingSetupAgent(self._llm)
        self._risk      = SwingRiskAgent(self._llm)
        self._assembler = SwingDataBundleAssembler(
            groww_client, tech_engine, news_client
        )
        logger.info(
            f"SwingSignalOrchestrator ready — "
            f"mode={TRADING_MODE} provider={ACTIVE_LLM['provider']}"
        )

    def generate(
        self,
        symbol:   str,
        exchange: str = "NSE",
    ) -> SwingSignalResult:
        """
        Run the full 3-agent swing pipeline for one cash stock.
        Returns SwingSignalResult — never raises.
        """
        result = SwingSignalResult(
            symbol       = symbol,
            exchange     = exchange,
            mode         = TRADING_MODE,
            llm_provider = ACTIVE_LLM["provider"],
            llm_model    = ACTIVE_LLM["model"],
        )

        # ── Stage 1: Data Bundle ─────────────────────────────────
        try:
            logger.info(f"Stage 1: Assembling swing data bundle for {symbol}")
            bundle = self._assembler.assemble(symbol=symbol, exchange=exchange)

            result.data_quality  = bundle.data_quality
            result.confidence_cap = bundle.confidence_cap
            result.spot_price    = bundle.spot_price
            result.india_vix     = bundle.india_vix
            result.nifty_trend   = bundle.nifty_trend
            result.sector        = bundle.sector
            result.pipeline_stage = "data_complete"

            # Hard filter fail → return immediately
            if not bundle.passes_hard_filters:
                result.final_action = "AVOID"
                result.block_reason = f"Hard filter: {bundle.filter_fail_reason}"
                result.pipeline_stage = "hard_filter_failed"
                logger.info(f"Hard filter blocked {symbol}: {bundle.filter_fail_reason}")
                return result

        except Exception as e:
            result.error          = f"Data assembly failed: {e}"
            result.pipeline_stage = "data_failed"
            result.block_reason   = "Data unavailable — defaulting to AVOID"
            logger.error(result.error)
            return result

        # ── Stage 2: Agent 1 — Swing Analyst ────────────────────
        try:
            logger.info(f"Stage 2: Running Swing Analyst for {symbol}")
            analysis               = self._analyst.analyse(bundle)
            result.analysis        = analysis
            result.market_regime   = analysis.market_regime
            result.pipeline_stage  = "analyst_complete"
        except Exception as e:
            result.error          = f"Analyst agent failed: {e}"
            result.pipeline_stage = "analyst_failed"
            result.block_reason   = "Analysis failed — defaulting to AVOID"
            logger.error(result.error)
            return result

        # ── Stage 2b: Sanity Checker ─────────────────────────────
        sanity = self._sanity.check(analysis, bundle)
        result.sanity_passed   = sanity["passed"]
        result.sanity_warnings = sanity["warnings"]

        # ── Stage 3: Agent 2 — Setup Selector ───────────────────
        try:
            logger.info(f"Stage 3: Running Setup Selector for {symbol}")
            signal               = self._setup.generate(bundle, analysis, sanity)
            result.signal        = signal
            result.setup_type    = analysis.setup_type
            result.direction     = analysis.trend_direction
            result.signal_quality = signal.signal_quality
            result.entry_price   = signal.entry_price
            result.stop_loss     = signal.stop_loss
            result.target_1      = signal.target_1
            result.target_2      = signal.target_2
            result.hold_days     = signal.hold_days
            result.risk_reward   = signal.risk_reward_ratio
            result.pipeline_stage = "signal_complete"
        except Exception as e:
            result.error          = f"Setup agent failed: {e}"
            result.pipeline_stage = "signal_failed"
            result.block_reason   = "Setup generation failed — defaulting to AVOID"
            logger.error(result.error)
            return result

        # ── Confidence threshold check ────────────────────────────
        if signal.confidence < SWING_MIN_CONFIDENCE:
            result.final_action     = "AVOID"
            result.final_confidence = signal.confidence
            result.block_reason     = (
                f"Confidence {signal.confidence}% below "
                f"minimum {SWING_MIN_CONFIDENCE}%"
            )
            logger.info(result.block_reason)
            return result

        # ── WATCH / AVOID pass-through ────────────────────────────
        if signal.action in ("WATCH", "AVOID"):
            result.final_action     = signal.action
            result.final_confidence = signal.confidence
            result.block_reason     = (
                signal.watch_reasoning or signal.avoid_reasoning
            )
            result.pipeline_stage   = "complete"
            logger.info(
                f"{symbol}: {signal.action} — "
                f"{result.block_reason or 'no specific reason'}"
            )
            return result

        # ── Stage 4: Agent 3 — Risk Assessor ────────────────────
        try:
            logger.info(f"Stage 4: Running Risk Assessor for {symbol}")
            risk_result            = self._risk.assess(bundle, analysis, signal)
            result.risk            = risk_result["risk_params"]
            result.position_sizing = risk_result["position_sizing"]
            result.approved        = risk_result["final_approved"]
            result.block_reason    = risk_result["block_reason"]
            result.pipeline_stage  = "risk_complete"
        except Exception as e:
            result.error          = f"Risk agent failed: {e}"
            result.pipeline_stage = "risk_failed"
            result.block_reason   = "Risk assessment failed — defaulting to AVOID"
            logger.error(result.error)
            return result

        # ── Final signal ──────────────────────────────────────────
        result.final_action     = signal.action if result.approved else "AVOID"
        result.final_confidence = signal.confidence
        result.pipeline_stage   = "complete"

        logger.info(
            f"Swing pipeline complete: {symbol} | "
            f"{result.final_action} | {result.setup_type} | "
            f"confidence={result.final_confidence}% | "
            f"approved={result.approved}"
        )

        # ── Archive to DB (non-critical) ──────────────────────────
        try:
            self._archive_signal(result)
        except Exception as e:
            logger.warning(f"Swing signal archive failed (non-critical): {e}")

        return result

    def generate_batch(
        self,
        symbols:  list[str],
        exchange: str = "NSE",
    ) -> list[SwingSignalResult]:
        """
        Run the pipeline for a list of symbols (screener shortlist).
        Returns list of results sorted by confidence descending.
        Only approved BUY signals are sorted first.
        """
        logger.info(f"Batch swing run: {len(symbols)} symbols")
        results = []
        for symbol in symbols:
            r = self.generate(symbol=symbol, exchange=exchange)
            results.append(r)

        # Sort: approved BUY first, then WATCH, then AVOID
        def sort_key(r):
            if r.final_action == "BUY" and r.approved:
                return (0, -r.final_confidence)
            elif r.final_action == "WATCH":
                return (1, -r.final_confidence)
            else:
                return (2, -r.final_confidence)

        results.sort(key=sort_key)
        approved = sum(1 for r in results if r.approved)
        logger.info(
            f"Batch complete: {approved}/{len(symbols)} approved | "
            f"top: {[r.symbol for r in results[:3] if r.approved]}"
        )
        return results

    def _archive_signal(self, result: SwingSignalResult):
        """Persist signal to swing_signals_log table."""
        from core.db import get_connection
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO swing_signals_log (
                timestamp, symbol, exchange, mode, llm_provider, llm_model,
                setup_type, direction, signal_quality,
                entry_price, stop_loss, target_1, target_2,
                hold_days, risk_reward, sector,
                action, confidence, approved, block_reason,
                shares, position_value, actual_risk_inr, actual_risk_pct,
                spot_price, india_vix, market_regime, data_quality,
                sanity_passed, primary_reason
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
        """, (
            result.timestamp, result.symbol, result.exchange,
            result.mode, result.llm_provider, result.llm_model,
            result.setup_type, result.direction, result.signal_quality,
            result.entry_price, result.stop_loss, result.target_1, result.target_2,
            result.hold_days, result.risk_reward, result.sector,
            result.final_action, result.final_confidence,
            int(result.approved), result.block_reason,
            result.position_sizing.get("shares") if result.position_sizing else None,
            result.position_sizing.get("position_value") if result.position_sizing else None,
            result.position_sizing.get("actual_risk_inr") if result.position_sizing else None,
            result.position_sizing.get("actual_risk_pct") if result.position_sizing else None,
            result.spot_price, result.india_vix, result.market_regime,
            result.data_quality, int(result.sanity_passed),
            result.signal.primary_reason if result.signal else None,
        ))
        conn.commit()
        conn.close()
