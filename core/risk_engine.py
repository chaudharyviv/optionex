"""
OPTIONEX — Risk Engine
Enforces all 15 guardrails before any signal is approved.
All guardrails are hardcoded — cannot be overridden by LLM output.

Extended from COMMODEX with 5 options-specific guardrails (G11-G15).
"""

import os
import logging
from datetime import datetime, date
from zoneinfo import ZoneInfo
from typing import Optional

from config import (
    TRADING_MODE,
    CAPITAL_INR,
    DAILY_LOSS_LIMIT_PCT,
    MAX_OPEN_POSITIONS,
    MIN_CONFIDENCE_THRESHOLD,
    MIN_RR_RATIO,
    NSE_OPEN_TIME,
    NSE_CLOSE_TIME,
    INTRADAY_OPTIONS_CUTOFF_TIME,
    EXPIRY_DAY_CUTOFF_TIME,
    CONFIDENCE_CAP_HIGH_IMPACT,
    CONFIDENCE_CAP_VIX_SPIKE,
    MAX_IV_PERCENTILE_FOR_BUYING,
    MIN_DTE_FOR_BUYING,
    MAX_PREMIUM_RISK_INR,
    ALLOWED_STRATEGIES,
    VIX_SPIKE_CHANGE_PCT,
)
from core.db import get_connection

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")


class GuardrailResult:
    """Result from a single guardrail check."""
    def __init__(self, name: str, passed: bool, reason: str = "", cap: Optional[int] = None):
        self.name   = name
        self.passed = passed
        self.reason = reason
        self.cap    = cap

    def __repr__(self):
        status = "PASS" if self.passed else "BLOCK"
        return f"[{status}] {self.name}: {self.reason}"


class OptionsRiskEngine:
    """Enforces all 15 guardrails for options signals."""

    def check_all(
        self,
        index:              str,
        action:             str,
        confidence:         int,
        rr_ratio:           Optional[float],
        trading_style:      str,
        strategy_name:      str = "",
        iv_percentile:      Optional[float] = None,
        dte:                Optional[int] = None,
        premium_total:      Optional[float] = None,
        vix_change_pct:     Optional[float] = None,
        expiry_date:        Optional[str] = None,
        high_impact_event:  Optional[str] = None,
        open_positions:     int = 0,
        daily_pnl_pct:      float = 0.0,
        is_buying_premium:  bool = False,
    ) -> dict:
        results       = []
        block_reasons = []
        cap           = 100
        cap_reasons   = []

        # ── G1: Daily Loss Limit ────────────────────
        g = self._g1_daily_loss(daily_pnl_pct)
        results.append(g)
        if not g.passed: block_reasons.append(g.reason)

        # ── G2: Max Open Positions ──────────────────
        g = self._g2_max_positions(open_positions, action)
        results.append(g)
        if not g.passed: block_reasons.append(g.reason)

        # ── G3: Min Confidence ──────────────────────
        g = self._g3_min_confidence(confidence)
        results.append(g)
        if not g.passed: block_reasons.append(g.reason)

        # ── G4: Market Hours ────────────────────────
        g = self._g4_market_hours()
        results.append(g)
        if not g.passed: block_reasons.append(g.reason)

        # ── G5: High Impact Event ───────────────────
        g = self._g5_high_impact(high_impact_event)
        results.append(g)
        if not g.passed: block_reasons.append(g.reason)
        if g.cap and g.cap < cap:
            cap = g.cap
            cap_reasons.append(g.reason)

        # ── G6: Min R:R ────────────────────────────
        g = self._g6_min_rr(rr_ratio)
        results.append(g)
        if not g.passed: block_reasons.append(g.reason)

        # ── G7: Production Confirmation ────────────
        g = self._g7_production_check()
        results.append(g)
        if not g.passed: block_reasons.append(g.reason)

        # ── G8: Expiry Proximity ───────────────────
        g = self._g8_expiry_proximity(dte, trading_style)
        results.append(g)
        if not g.passed: block_reasons.append(g.reason)

        # ── G9: VIX Gate ───────────────────────────
        g = self._g9_vix_gate(vix_change_pct)
        results.append(g)
        if g.cap and g.cap < cap:
            cap = g.cap
            cap_reasons.append(g.reason)

        # ── G10: Session Boundary ──────────────────
        g = self._g10_session_boundary(trading_style)
        results.append(g)
        if not g.passed: block_reasons.append(g.reason)

        # ── G11: IV Gate (buying) ──────────────────
        g = self._g11_iv_gate(iv_percentile, is_buying_premium)
        results.append(g)
        if not g.passed: block_reasons.append(g.reason)

        # ── G12: DTE Gate (buying) ─────────────────
        g = self._g12_dte_gate(dte, is_buying_premium)
        results.append(g)
        if not g.passed: block_reasons.append(g.reason)

        # ── G13: Premium Cap ───────────────────────
        g = self._g13_premium_cap(premium_total)
        results.append(g)
        if not g.passed: block_reasons.append(g.reason)

        # ── G14: Strategy Allowlist ────────────────
        g = self._g14_strategy_allowlist(strategy_name, action)
        results.append(g)
        if not g.passed: block_reasons.append(g.reason)

        # ── G15: Expiry Day Gate ───────────────────
        g = self._g15_expiry_day_gate(dte)
        results.append(g)
        if not g.passed: block_reasons.append(g.reason)

        approved = len(block_reasons) == 0 and action != "HOLD"

        if block_reasons:
            logger.warning(f"Guardrails BLOCKED {index} {action}: {block_reasons}")
        else:
            logger.info(f"All guardrails passed for {index} {action}")

        return {
            "approved":          approved,
            "block_reasons":     block_reasons,
            "confidence_cap":    cap,
            "cap_reasons":       cap_reasons,
            "guardrail_results": results,
        }

    # ── Guardrail Implementations ──────────────────────

    def _g1_daily_loss(self, daily_pnl_pct: float) -> GuardrailResult:
        limit = DAILY_LOSS_LIMIT_PCT
        if daily_pnl_pct <= -limit:
            return GuardrailResult("G1_DailyLoss", False,
                f"Daily loss {daily_pnl_pct:.1f}% ≥ limit -{limit}%")
        return GuardrailResult("G1_DailyLoss", True,
            f"Daily P&L: {daily_pnl_pct:.1f}%")

    def _g2_max_positions(self, open_pos: int, action: str) -> GuardrailResult:
        if action != "HOLD" and open_pos >= MAX_OPEN_POSITIONS:
            return GuardrailResult("G2_MaxPositions", False,
                f"Open positions {open_pos} ≥ limit {MAX_OPEN_POSITIONS}")
        return GuardrailResult("G2_MaxPositions", True,
            f"Open: {open_pos}/{MAX_OPEN_POSITIONS}")

    def _g3_min_confidence(self, confidence: int) -> GuardrailResult:
        if confidence < MIN_CONFIDENCE_THRESHOLD:
            return GuardrailResult("G3_MinConfidence", False,
                f"Confidence {confidence}% < min {MIN_CONFIDENCE_THRESHOLD}%")
        return GuardrailResult("G3_MinConfidence", True,
            f"Confidence {confidence}% OK")

    def _g4_market_hours(self) -> GuardrailResult:
        now_time = datetime.now(IST).strftime("%H:%M")
        if NSE_OPEN_TIME <= now_time <= NSE_CLOSE_TIME:
            return GuardrailResult("G4_MarketHours", True, f"NSE open: {now_time} IST")
        return GuardrailResult("G4_MarketHours", False,
            f"NSE closed at {now_time} IST ({NSE_OPEN_TIME}–{NSE_CLOSE_TIME})")

    def _g5_high_impact(self, event: Optional[str]) -> GuardrailResult:
        if event:
            return GuardrailResult("G5_HighImpact", True,
                f"Event: {event} — capped at {CONFIDENCE_CAP_HIGH_IMPACT}%",
                cap=CONFIDENCE_CAP_HIGH_IMPACT)
        return GuardrailResult("G5_HighImpact", True, "No high impact events")

    def _g6_min_rr(self, rr: Optional[float]) -> GuardrailResult:
        if rr is None:
            return GuardrailResult("G6_MinRR", True, "R:R not computed (HOLD)")
        if rr < MIN_RR_RATIO:
            return GuardrailResult("G6_MinRR", False,
                f"R:R {rr:.1f} < min {MIN_RR_RATIO}")
        return GuardrailResult("G6_MinRR", True, f"R:R {rr:.1f} OK")

    def _g7_production_check(self) -> GuardrailResult:
        if TRADING_MODE != "production":
            return GuardrailResult("G7_Production", True, f"Mode: {TRADING_MODE}")
        confirmed = os.getenv("PRODUCTION_CONFIRMED", "false").lower()
        if confirmed != "true":
            return GuardrailResult("G7_Production", False,
                "Production needs explicit confirmation")
        return GuardrailResult("G7_Production", True, "Production confirmed")

    def _g8_expiry_proximity(self, dte: Optional[int], style: str) -> GuardrailResult:
        if dte is None:
            return GuardrailResult("G8_Expiry", True, "DTE not provided")
        if dte == 0 and style not in ("expiry_day", "system"):
            return GuardrailResult("G8_Expiry", False,
                f"DTE=0 (expiry day) but style={style}. Switch to expiry_day style.")
        return GuardrailResult("G8_Expiry", True, f"DTE={dte} OK")

    def _g9_vix_gate(self, vix_change: Optional[float]) -> GuardrailResult:
        if vix_change is None:
            return GuardrailResult("G9_VIX", True, "VIX data unavailable")
        if abs(vix_change) >= VIX_SPIKE_CHANGE_PCT:
            return GuardrailResult("G9_VIX", True,
                f"VIX spike {vix_change:+.1f}% — capped at {CONFIDENCE_CAP_VIX_SPIKE}%",
                cap=CONFIDENCE_CAP_VIX_SPIKE)
        return GuardrailResult("G9_VIX", True, f"VIX change {vix_change:+.1f}% — stable")

    def _g10_session_boundary(self, style: str) -> GuardrailResult:
        if style not in ("intraday", "system"):
            return GuardrailResult("G10_Session", True, f"Style={style}")
        now_time = datetime.now(IST).strftime("%H:%M")
        if now_time > INTRADAY_OPTIONS_CUTOFF_TIME:
            return GuardrailResult("G10_Session", False,
                f"Intraday cutoff passed: {now_time} > {INTRADAY_OPTIONS_CUTOFF_TIME}")
        return GuardrailResult("G10_Session", True, f"Within session: {now_time}")

    def _g11_iv_gate(self, iv_pctile: Optional[float], is_buying: bool) -> GuardrailResult:
        if not is_buying or iv_pctile is None:
            return GuardrailResult("G11_IVGate", True, "Not buying premium or no IV data")
        if iv_pctile > MAX_IV_PERCENTILE_FOR_BUYING:
            return GuardrailResult("G11_IVGate", False,
                f"IV percentile {iv_pctile:.0f} > {MAX_IV_PERCENTILE_FOR_BUYING} — "
                f"too expensive to buy premium")
        return GuardrailResult("G11_IVGate", True,
            f"IV percentile {iv_pctile:.0f} OK for buying")

    def _g12_dte_gate(self, dte: Optional[int], is_buying: bool) -> GuardrailResult:
        if not is_buying or dte is None:
            return GuardrailResult("G12_DTEGate", True, "Not buying or no DTE")
        if dte < MIN_DTE_FOR_BUYING:
            return GuardrailResult("G12_DTEGate", False,
                f"DTE={dte} < min {MIN_DTE_FOR_BUYING} for buying. Theta crush risk.")
        return GuardrailResult("G12_DTEGate", True, f"DTE={dte} OK for buying")

    def _g13_premium_cap(self, premium: Optional[float]) -> GuardrailResult:
        if premium is None:
            return GuardrailResult("G13_PremiumCap", True, "No premium data")
        if premium > MAX_PREMIUM_RISK_INR:
            return GuardrailResult("G13_PremiumCap", False,
                f"Premium ₹{premium:,.0f} > cap ₹{MAX_PREMIUM_RISK_INR:,.0f}")
        return GuardrailResult("G13_PremiumCap", True,
            f"Premium ₹{premium:,.0f} within cap")

    def _g14_strategy_allowlist(self, strategy: str, action: str) -> GuardrailResult:
        if action == "HOLD":
            return GuardrailResult("G14_Allowlist", True, "HOLD — no strategy check")
        if strategy not in ALLOWED_STRATEGIES:
            return GuardrailResult("G14_Allowlist", False,
                f"Strategy '{strategy}' not in Phase 1+2 allowlist")
        return GuardrailResult("G14_Allowlist", True, f"Strategy '{strategy}' allowed")

    def _g15_expiry_day_gate(self, dte: Optional[int]) -> GuardrailResult:
        if dte is None or dte > 0:
            return GuardrailResult("G15_ExpiryDay", True, "Not expiry day")
        now_time = datetime.now(IST).strftime("%H:%M")
        if now_time > EXPIRY_DAY_CUTOFF_TIME:
            return GuardrailResult("G15_ExpiryDay", False,
                f"Expiry day after {EXPIRY_DAY_CUTOFF_TIME} IST — no new signals")
        return GuardrailResult("G15_ExpiryDay", True,
            f"Expiry day but before cutoff ({now_time})")

    # ── Data access helpers ────────────────────────────

    def get_daily_pnl_pct(self) -> float:
        try:
            conn   = get_connection()
            cursor = conn.cursor()
            today  = date.today().isoformat()
            cursor.execute("""
                SELECT COALESCE(SUM(pnl_total), 0) as total_pnl
                FROM options_trades_log
                WHERE DATE(entry_time) = ? AND mode = ?
            """, (today, TRADING_MODE))
            row       = cursor.fetchone()
            conn.close()
            total_pnl = float(row["total_pnl"]) if row else 0.0
            return round(total_pnl / CAPITAL_INR * 100, 3)
        except Exception:
            return 0.0

    def get_open_positions_count(self) -> int:
        try:
            conn   = get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM options_trades_log
                WHERE exit_time IS NULL AND mode = ?
            """, (TRADING_MODE,))
            row  = cursor.fetchone()
            conn.close()
            return int(row["cnt"]) if row else 0
        except Exception:
            return 0
