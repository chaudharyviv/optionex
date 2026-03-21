"""
OPTIONEX — Central Configuration
Single source of truth for all system parameters.
Never import from .env directly elsewhere — always go through config.py.

Adapted from COMMODEX v2.0 for NSE Index Options (Nifty / BankNifty).

Version: 1.0
"""

import os
import pathlib
from dotenv import load_dotenv
import warnings

load_dotenv()

# ─────────────────────────────────────────────────────────────────
# TRADING MODE
# ─────────────────────────────────────────────────────────────────
TRADING_MODE = os.getenv("TRADING_MODE", "demo")  # demo | paper | production

# ─────────────────────────────────────────────────────────────────
# LLM CONFIGURATION
# ─────────────────────────────────────────────────────────────────
LLM_CONFIG = {
    "demo": {
        "provider": "openai",
        "model":    "gpt-4o",
        "api_key":  os.getenv("OPENAI_API_KEY"),
    },
    "paper": {
        "provider": "anthropic",
        "model":    "claude-sonnet-4-6",
        "api_key":  os.getenv("ANTHROPIC_API_KEY"),
    },
    "production": {
        "provider": "anthropic",
        "model":    "claude-sonnet-4-6",
        "api_key":  os.getenv("ANTHROPIC_API_KEY"),
    },
}

ACTIVE_LLM = LLM_CONFIG[TRADING_MODE]

# ─────────────────────────────────────────────────────────────────
# GROWW API
# ─────────────────────────────────────────────────────────────────
GROWW_API_KEY      = os.getenv("GROWW_API_KEY")
GROWW_API_SECRET   = os.getenv("GROWW_API_SECRET")
GROWW_TOTP_SECRET  = os.getenv("GROWW_TOTP_SECRET")

# ─────────────────────────────────────────────────────────────────
# NEWS / SEARCH
# ─────────────────────────────────────────────────────────────────
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")

if not TAVILY_API_KEY:
    warnings.warn("TAVILY_API_KEY not set — news context unavailable")


# ─────────────────────────────────────────────────────────────────
# NSE F&O LOT CONFIGURATION
#
# Source: NSE circular — lot sizes revised effective Nov 2024
# Last updated: March 2026
#
# lot_size    : Number of shares (index units) per lot
# tick_size   : ₹0.05 minimum price movement for options
# pl_per_tick : lot_size × tick_size  = P&L per minimum tick per lot
#
# recommended : True = suitable for retail account
# active      : True = included in current version scope
# ─────────────────────────────────────────────────────────────────

NSE_LOT_CONFIG = {

    # ── NIFTY 50 ─────────────────────────────────────────────────

    "NIFTY": {
        "exchange":          "NSE",
        "segment":           "NFO",
        "lot_size":          75,               # units per lot
        "tick_size":         0.05,             # ₹0.05 minimum
        "pl_per_tick":       3.75,             # 75 × 0.05 = ₹3.75 per tick
        "friendly_name":     "Nifty 50 Options",
        "expiry_day":        "thursday",       # weekly expiry
        "strike_interval":   50,               # ₹50 between strikes
        "typical_spot":      23500,            # approx March 2026
        "recommended":       True,
        "active":            True,
    },

    # ── BANK NIFTY ───────────────────────────────────────────────

    "BANKNIFTY": {
        "exchange":          "NSE",
        "segment":           "NFO",
        "lot_size":          30,               # units per lot
        "tick_size":         0.05,
        "pl_per_tick":       1.50,             # 30 × 0.05 = ₹1.50 per tick
        "friendly_name":     "Bank Nifty Options",
        "expiry_day":        "wednesday",      # weekly expiry
        "strike_interval":   100,              # ₹100 between strikes
        "typical_spot":      50000,            # approx March 2026
        "recommended":       True,
        "active":            True,
    },
}

# ─────────────────────────────────────────────────────────────────
# ACTIVE SCOPE
# ─────────────────────────────────────────────────────────────────
ACTIVE_INDICES = ["NIFTY", "BANKNIFTY"]

ACTIVE_LOT_CONFIG = {
    k: v for k, v in NSE_LOT_CONFIG.items()
    if v.get("active", False)
}


# ─────────────────────────────────────────────────────────────────
# NSE MARKET HOURS (IST)
# ─────────────────────────────────────────────────────────────────
NSE_OPEN_TIME                  = "09:15"
NSE_CLOSE_TIME                 = "15:30"
INTRADAY_OPTIONS_CUTOFF_TIME   = "14:45"     # no new intraday signals after this
EXPIRY_DAY_CUTOFF_TIME         = "13:00"     # no new signals on expiry day after 1 PM
WEEKLY_EXPIRY_BLACKOUT_HOURS   = 2


# ─────────────────────────────────────────────────────────────────
# RISK PARAMETERS
# ─────────────────────────────────────────────────────────────────
CAPITAL_INR                    = float(os.getenv("CAPITAL_INR", 200000))
RISK_PCT_PER_TRADE             = float(os.getenv("RISK_PCT_PER_TRADE", 2.0))
MAX_OPEN_POSITIONS             = int(os.getenv("MAX_OPEN_POSITIONS", 3))
DAILY_LOSS_LIMIT_PCT           = float(os.getenv("DAILY_LOSS_LIMIT_PCT", 5.0))
MAX_LOTS_PER_SIGNAL            = 5
MIN_CONFIDENCE_THRESHOLD       = 55
MIN_RR_RATIO                   = 1.5

# v1.0: Options-specific
MAX_PREMIUM_RISK_INR           = 25000       # hard cap on premium paid per trade
MIN_DTE_FOR_BUYING             = 2           # don't buy options with < 2 DTE
MAX_DTE_FOR_WEEKLY             = 7           # weekly expiry only for intraday strategies
MAX_IV_PERCENTILE_FOR_BUYING   = 80          # don't buy when IV pctile > 80
MIN_IV_PERCENTILE_FOR_SELLING  = 40          # don't sell when IV pctile < 40
THETA_DECAY_WARNING_PCT        = 3.0         # warn if daily theta > 3% of premium

# Underlotted safety (same concept as COMMODEX)
RISK_OVERBUDGET_BLOCK_MULTIPLIER = 1.5

# B-grade position reduction
B_GRADE_POSITION_REDUCTION     = 0.5

# Confidence caps
CONFIDENCE_CAP_NO_NEWS         = 65
CONFIDENCE_CAP_HIGH_IMPACT     = 60
CONFIDENCE_CAP_VIX_SPIKE       = 55          # replaces INR/USD for options
CONFIDENCE_CAP_EXPIRY_DAY      = 55
CONFIDENCE_CAP_IV_EXTREME      = 60

# ─────────────────────────────────────────────────────────────────
# STRATEGY ALLOWLIST
# ─────────────────────────────────────────────────────────────────

# Phase 1 + Phase 2 — defined risk only
ALLOWED_STRATEGIES = [
    "long_call",
    "long_put",
    "bull_call_spread",
    "bear_put_spread",
    "long_straddle",
    "long_strangle",
    "iron_condor",
]

# Phase 3+ (future — require higher capital + validation)
FUTURE_STRATEGIES = [
    "short_straddle",
    "short_strangle",
    "ratio_spread",
    "calendar_spread",
]


# ─────────────────────────────────────────────────────────────────
# IV & GREEKS THRESHOLDS
# ─────────────────────────────────────────────────────────────────
IV_RANK_HIGH_THRESHOLD          = 70
IV_RANK_LOW_THRESHOLD           = 30
IV_CRUSH_WARNING_DTE            = 1
PCR_BULLISH_THRESHOLD           = 0.7        # PCR < 0.7 = bullish (heavy put writing)
PCR_BEARISH_THRESHOLD           = 1.3        # PCR > 1.3 = bearish (heavy put buying)
DELTA_RANGE_DIRECTIONAL         = (0.25, 0.45)
DELTA_RANGE_PREMIUM_SELL        = (0.15, 0.25)

# VIX thresholds
VIX_LOW                         = 12.0
VIX_NORMAL_HIGH                 = 18.0
VIX_ELEVATED                    = 22.0
VIX_SPIKE_CHANGE_PCT            = 10.0       # intraday VIX change > 10% = spike


# ─────────────────────────────────────────────────────────────────
# TECHNICAL INDICATOR THRESHOLDS (for spot index)
# Same as COMMODEX — applied to Nifty/BankNifty spot
# ─────────────────────────────────────────────────────────────────
ADX_RANGING_THRESHOLD           = 20
ADX_TRENDING_THRESHOLD          = 25
ADX_STRONG_THRESHOLD            = 40

VWAP_PREMIUM_PCT                = 0.3
VWAP_DISCOUNT_PCT               = 0.3

BB_SQUEEZE_TOLERANCE            = 1.05
SUPERTREND_PERIOD               = 10
SUPERTREND_MULTIPLIER           = 3.0
RSI_DIVERGENCE_LOOKBACK         = 30
RSI_PIVOT_ORDER                 = 5
FIB_LOOKBACK_CANDLES            = 50
FIB_PIVOT_ORDER                 = 5
STOCH_RSI_OVERBOUGHT            = 80
STOCH_RSI_OVERSOLD              = 20
VOLUME_CONFIRM_RATIO            = 1.2


# ─────────────────────────────────────────────────────────────────
# CACHE SETTINGS (minutes)
# ─────────────────────────────────────────────────────────────────
CACHE_OHLCV_INTRADAY_MIN        = 5
CACHE_OHLCV_DAILY_MIN           = 60
CACHE_NEWS_MIN                   = 60
CACHE_CHAIN_MIN                  = 2         # option chain refreshes fast
CACHE_VIX_MIN                    = 5


# ─────────────────────────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────────────────────────
BASE_DIR    = pathlib.Path(__file__).parent
DB_PATH     = BASE_DIR / "optionex.db"
BACKUP_DIR  = BASE_DIR / "data" / "backups"
PROMPTS_DIR = BASE_DIR / "prompts"


# ─────────────────────────────────────────────────────────────────
# POSITION SIZING — OPTIONS
#
# For DEFINED RISK strategies (Phase 1 + 2):
#   max_loss_per_lot = premium × lot_size       (long options)
#   max_loss_per_lot = spread_width × lot_size  (spreads, minus credit)
#
# position_lots = floor(risk_budget / max_loss_per_lot)
# position_lots = min(position_lots, MAX_LOTS_PER_SIGNAL)
# position_lots = max(position_lots, 1)
#
# Example — Nifty Bull Call Spread (23500/23600):
#   risk_budget = ₹200,000 × 2% = ₹4,000
#   spread_width = 100 pts, credit = 30 pts  → net risk = 70 pts
#   max_loss_per_lot = 70 × 75 = ₹5,250
#   raw_lots = 4000 / 5250 = 0.76 → 1 lot (underlotted)
#   actual_risk = ₹5,250 (1.31× budget — within 1.5× limit)
#
# Example — Nifty Long Call:
#   premium = ₹150, lot_size = 75
#   max_loss_per_lot = 150 × 75 = ₹11,250
#   raw_lots = 4000 / 11250 = 0.36 → 1 lot
#   Premium cap check: 11,250 < 25,000 → OK
# ─────────────────────────────────────────────────────────────────

def get_options_position_size(
    index:             str,
    strategy:          str,
    max_loss_per_lot:  float,
    premium_per_lot:   float = None,
    capital:           float = None,
    risk_pct:          float = None,
    signal_quality:    str   = None,
) -> dict:
    """
    Calculate position size for options strategies.
    Deterministic — never let LLM compute this.

    max_loss_per_lot:  absolute worst-case loss per lot in ₹
    premium_per_lot:   total premium outlay per lot in ₹ (for buy strategies)
    """
    cfg = NSE_LOT_CONFIG.get(index)
    if not cfg:
        return {"error": f"Unknown index: {index}"}

    capital   = capital  or CAPITAL_INR
    risk_pct  = risk_pct or RISK_PCT_PER_TRADE

    risk_budget = capital * (risk_pct / 100)

    if max_loss_per_lot <= 0:
        return {"error": "Max loss per lot must be positive"}

    raw_lots = risk_budget / max_loss_per_lot

    # B-grade reduction
    if signal_quality == "B":
        raw_lots = raw_lots * B_GRADE_POSITION_REDUCTION

    position_lots = max(1, min(int(raw_lots), MAX_LOTS_PER_SIGNAL))
    actual_risk   = position_lots * max_loss_per_lot

    # Premium cap check (for buying strategies)
    premium_total  = (premium_per_lot or 0) * position_lots
    premium_capped = False
    if premium_per_lot and premium_total > MAX_PREMIUM_RISK_INR:
        position_lots  = max(1, int(MAX_PREMIUM_RISK_INR / premium_per_lot))
        actual_risk    = position_lots * max_loss_per_lot
        premium_total  = premium_per_lot * position_lots
        premium_capped = True

    # Underlotted detection
    underlotted           = raw_lots < 1.0
    risk_overbudget_ratio = actual_risk / risk_budget if risk_budget > 0 else 0
    risk_blocked          = (
        underlotted
        and risk_overbudget_ratio > RISK_OVERBUDGET_BLOCK_MULTIPLIER
    )

    return {
        "index":                  index,
        "strategy":               strategy,
        "lot_size":               cfg["lot_size"],
        "max_loss_per_lot":       round(max_loss_per_lot, 2),
        "risk_budget_inr":        round(risk_budget, 2),
        "raw_lots_calculated":    round(raw_lots, 2),
        "position_lots":          position_lots,
        "actual_risk_inr":        round(actual_risk, 2),
        "actual_risk_pct":        round(actual_risk / capital * 100, 2),
        "premium_per_lot":        round(premium_per_lot or 0, 2),
        "premium_total":          round(premium_total, 2),
        "premium_capped":         premium_capped,
        "capped_at_max":          raw_lots > MAX_LOTS_PER_SIGNAL,
        "underlotted":            underlotted,
        "risk_overbudget_ratio":  round(risk_overbudget_ratio, 2),
        "risk_blocked":           risk_blocked,
        "risk_block_reason": (
            f"Max loss per lot ₹{max_loss_per_lot:,.0f} exceeds "
            f"{RISK_OVERBUDGET_BLOCK_MULTIPLIER}× budget of ₹{risk_budget:,.0f}. "
            f"Strategy too expensive for current capital."
        ) if risk_blocked else (
            f"Premium capped: ₹{premium_total:,.0f} "
            f"(max ₹{MAX_PREMIUM_RISK_INR:,.0f})."
        ) if premium_capped else None,
        "b_grade_reduced":        signal_quality == "B",
    }


# ─────────────────────────────────────────────────────────────────
# STARTUP VALIDATION
# ─────────────────────────────────────────────────────────────────

def validate_config() -> list[str]:
    """
    Called at app startup to surface missing or invalid config.
    Returns list of warning strings.
    """
    config_warnings = []

    if not GROWW_API_KEY or GROWW_API_KEY == "your_api_key_here":
        config_warnings.append("GROWW_API_KEY not set in .env")

    if not GROWW_TOTP_SECRET:
        config_warnings.append("GROWW_TOTP_SECRET not set in .env")

    if TRADING_MODE == "demo":
        if not ACTIVE_LLM["api_key"]:
            config_warnings.append(
                "OPENAI_API_KEY not set — demo LLM will not work"
            )

    if TRADING_MODE in ("paper", "production"):
        if not ACTIVE_LLM["api_key"]:
            config_warnings.append(
                "ANTHROPIC_API_KEY not set — paper/production LLM will not work"
            )

    if not TAVILY_API_KEY:
        config_warnings.append(
            "TAVILY_API_KEY not set — news context will be unavailable"
        )

    if CAPITAL_INR < 50000:
        config_warnings.append(
            f"CAPITAL_INR is ₹{CAPITAL_INR:,.0f} — "
            f"too low for NSE F&O margin requirements"
        )

    # Premium sanity check
    for index in ACTIVE_INDICES:
        cfg = NSE_LOT_CONFIG.get(index, {})
        typical = cfg.get("typical_spot", 0)
        lot     = cfg.get("lot_size", 1)
        # ATM premium ≈ 1-2% of spot
        est_atm_premium = typical * 0.015 * lot
        if est_atm_premium > CAPITAL_INR * 0.5:
            config_warnings.append(
                f"{cfg['friendly_name']}: estimated ATM premium ≈ "
                f"₹{est_atm_premium:,.0f} uses >{50}% of capital"
            )

    if TRADING_MODE == "production":
        config_warnings.append(
            "⚠ PRODUCTION MODE ACTIVE — real money at risk. "
            "Ensure paper trading validation is complete."
        )

    return config_warnings
