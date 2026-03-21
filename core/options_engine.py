"""
OPTIONEX — Options Analytics Engine v1.0
Computes all options-specific indicators from chain data.

Features:
  - Black-Scholes Greeks (delta, gamma, theta, vega)
  - IV Rank and IV Percentile (from historical IV data)
  - Put-Call Ratio (OI and Volume)
  - Max Pain calculation
  - OI concentration walls (support/resistance)
  - IV skew and term structure
  - Expected move from ATM straddle

Input:  Option chain snapshot + spot price + historical IV
Output: OptionsData dataclass ready for Agent 1 prompt

No LLM calls — pure deterministic computation.
"""

import logging
import math
import numpy as np
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# OUTPUT DATACLASS
# ─────────────────────────────────────────────────────────────────

@dataclass
class OptionsData:
    """
    Complete options analytics for one index.
    Computed from the option chain snapshot + historical IV data.
    """
    index:              str
    spot_price:         float
    futures_price:      float
    futures_basis:      float = 0.0
    futures_basis_pct:  float = 0.0
    timestamp:          str = ""

    # ── Expiry Map ─────────────────────────────────────
    available_expiries: list = field(default_factory=list)
    nearest_expiry:     str = ""
    monthly_expiry:     str = ""
    dte_nearest:        int = 0
    dte_monthly:        int = 0

    # ── IV Analytics ───────────────────────────────────
    atm_iv:             float = 0.0
    iv_rank:            float = 0.0
    iv_percentile:      float = 0.0
    iv_regime:          str = "normal"
    iv_skew:            float = 0.0
    iv_term_structure:  str = "flat"
    historical_vol_20:  float = 0.0
    iv_hv_spread:       float = 0.0

    # ── Put-Call Ratio ─────────────────────────────────
    pcr_oi:             float = 0.0
    pcr_volume:         float = 0.0
    pcr_signal:         str = "neutral"

    # ── Max Pain ───────────────────────────────────────
    max_pain_strike:    float = 0.0
    max_pain_distance:  float = 0.0
    max_pain_signal:    str = "at_max_pain"

    # ── OI Concentration ───────────────────────────────
    highest_call_oi_strike:   float = 0.0
    highest_put_oi_strike:    float = 0.0
    call_oi_wall:             int = 0
    put_oi_wall:              int = 0
    oi_shift_direction:       str = "stable"

    # ── ATM Greeks (per lot) ───────────────────────────
    atm_call_delta:     float = 0.0
    atm_call_theta:     float = 0.0
    atm_call_gamma:     float = 0.0
    atm_call_vega:      float = 0.0
    atm_put_delta:      float = 0.0
    atm_put_theta:      float = 0.0
    atm_put_gamma:      float = 0.0
    atm_put_vega:       float = 0.0

    # ── Chain Snapshot (strikes around ATM) ────────────
    chain_snapshot:     list = field(default_factory=list)

    def summary_string(self) -> str:
        """Compact summary for Agent 1 prompt injection."""
        lines = [
            f"Index: {self.index} | Spot: {self.spot_price:,.0f} | "
            f"Futures: {self.futures_price:,.0f} "
            f"(basis: {self.futures_basis_pct:+.2f}%)",
            f"Nearest Expiry: {self.nearest_expiry} ({self.dte_nearest} DTE)",
            "",
            "IV Analytics:",
            f"  ATM IV:         {self.atm_iv:.1f}%",
            f"  IV Rank:        {self.iv_rank:.0f} [{self.iv_regime}]",
            f"  IV Percentile:  {self.iv_percentile:.0f}",
            f"  HV(20):         {self.historical_vol_20:.1f}%",
            f"  IV-HV Spread:   {self.iv_hv_spread:+.1f}%",
            f"  IV Skew:        {self.iv_skew:+.2f}% (put - call)",
            f"  Term Structure: {self.iv_term_structure}",
            "",
            "Sentiment:",
            f"  PCR (OI):       {self.pcr_oi:.2f} [{self.pcr_signal}]",
            f"  Max Pain:       {self.max_pain_strike:,.0f} "
            f"({self.max_pain_distance:+,.0f} from spot) [{self.max_pain_signal}]",
            f"  Call OI Wall:   {self.highest_call_oi_strike:,.0f} "
            f"({self.call_oi_wall:,} OI)",
            f"  Put OI Wall:    {self.highest_put_oi_strike:,.0f} "
            f"({self.put_oi_wall:,} OI)",
            f"  OI Shift:       {self.oi_shift_direction}",
            "",
            "ATM Greeks (per lot):",
            f"  Call: Δ={self.atm_call_delta:.2f}  "
            f"θ=₹{self.atm_call_theta:.0f}/day  "
            f"γ={self.atm_call_gamma:.4f}  "
            f"ν={self.atm_call_vega:.1f}",
            f"  Put:  Δ={self.atm_put_delta:.2f}  "
            f"θ=₹{self.atm_put_theta:.0f}/day  "
            f"γ={self.atm_put_gamma:.4f}  "
            f"ν={self.atm_put_vega:.1f}",
        ]

        # Top strikes snapshot
        if self.chain_snapshot:
            lines.append("")
            lines.append("Option Chain (strikes around ATM):")
            lines.append(
                f"{'Strike':>8} | {'CE LTP':>8} {'CE IV':>6} "
                f"{'CE OI':>10} {'CE Δ':>6} | "
                f"{'PE LTP':>8} {'PE IV':>6} "
                f"{'PE OI':>10} {'PE Δ':>6}"
            )
            for s in self.chain_snapshot:
                lines.append(
                    f"{s['strike']:>8.0f} | "
                    f"{s.get('call_ltp',0):>8.1f} "
                    f"{s.get('call_iv',0):>5.1f}% "
                    f"{s.get('call_oi',0):>10,} "
                    f"{s.get('call_delta',0):>+6.2f} | "
                    f"{s.get('put_ltp',0):>8.1f} "
                    f"{s.get('put_iv',0):>5.1f}% "
                    f"{s.get('put_oi',0):>10,} "
                    f"{s.get('put_delta',0):>+6.2f}"
                )

        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────
# BLACK-SCHOLES GREEKS
# ─────────────────────────────────────────────────────────────────

def _norm_cdf(x: float) -> float:
    """Standard normal cumulative distribution (no scipy needed)."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    """Standard normal probability density."""
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def compute_bs_greeks(
    spot:        float,
    strike:      float,
    iv:          float,      # as percentage, e.g. 15.0 for 15%
    dte:         int,        # days to expiry
    r:           float = 0.065,   # risk-free rate (India ~6.5%)
    option_type: str   = "CE",
    lot_size:    int   = 75,
) -> dict:
    """
    Compute Black-Scholes Greeks for a single option.

    Returns dict with:
      delta, gamma, theta (₹/day/lot), vega (₹/1% IV move/lot),
      intrinsic, time_value, moneyness

    theta is returned as negative for long positions (daily cost).
    vega is returned in ₹ terms for 1% IV change.
    """
    if dte <= 0:
        # At expiry — pure intrinsic
        if option_type == "CE":
            intrinsic = max(0, spot - strike)
            delta = 1.0 if spot > strike else 0.0
        else:
            intrinsic = max(0, strike - spot)
            delta = -1.0 if spot < strike else 0.0
        return {
            "delta": delta, "gamma": 0.0,
            "theta": 0.0, "vega": 0.0,
            "intrinsic": intrinsic * lot_size,
            "time_value": 0.0,
            "moneyness": "ITM" if intrinsic > 0 else "OTM",
        }

    sigma = iv / 100.0
    T     = dte / 365.0
    sqrt_T = math.sqrt(T)

    try:
        d1 = (math.log(spot / strike) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrt_T)
        d2 = d1 - sigma * sqrt_T
    except (ValueError, ZeroDivisionError):
        return {
            "delta": 0.0, "gamma": 0.0,
            "theta": 0.0, "vega": 0.0,
            "intrinsic": 0.0, "time_value": 0.0,
            "moneyness": "ATM",
        }

    # Greeks
    gamma = _norm_pdf(d1) / (spot * sigma * sqrt_T)

    # Vega — same for call and put
    # In ₹ per 1% IV change per lot
    vega_per_share = spot * sqrt_T * _norm_pdf(d1) / 100.0
    vega_per_lot   = vega_per_share * lot_size

    if option_type == "CE":
        delta = _norm_cdf(d1)
        price = spot * _norm_cdf(d1) - strike * math.exp(-r * T) * _norm_cdf(d2)
        theta_per_share = (
            -(spot * _norm_pdf(d1) * sigma) / (2 * sqrt_T)
            - r * strike * math.exp(-r * T) * _norm_cdf(d2)
        ) / 365.0
        intrinsic = max(0, spot - strike)
    else:
        delta = _norm_cdf(d1) - 1.0
        price = strike * math.exp(-r * T) * _norm_cdf(-d2) - spot * _norm_cdf(-d1)
        theta_per_share = (
            -(spot * _norm_pdf(d1) * sigma) / (2 * sqrt_T)
            + r * strike * math.exp(-r * T) * _norm_cdf(-d2)
        ) / 365.0
        intrinsic = max(0, strike - spot)

    time_value = max(0, price - intrinsic)

    # Moneyness classification
    moneyness_pct = abs(spot - strike) / spot * 100
    if moneyness_pct < 0.5:
        moneyness = "ATM"
    elif (option_type == "CE" and spot > strike) or \
         (option_type == "PE" and spot < strike):
        moneyness = "ITM"
    else:
        moneyness = "OTM"

    return {
        "delta":      round(delta, 4),
        "gamma":      round(gamma, 6),
        "theta":      round(theta_per_share * lot_size, 2),   # ₹/day per lot
        "vega":       round(vega_per_lot, 2),                  # ₹/1%IV per lot
        "price_bs":   round(price, 2),
        "intrinsic":  round(intrinsic * lot_size, 2),
        "time_value": round(time_value * lot_size, 2),
        "moneyness":  moneyness,
    }


# ─────────────────────────────────────────────────────────────────
# IMPLIED VOLATILITY SOLVER
# Newton-Raphson method — computes IV from market premium
# Used when Groww API doesn't provide implied_volatility field
# ─────────────────────────────────────────────────────────────────

def _bs_price(spot, strike, sigma, T, r, option_type="CE"):
    """Raw BS price for IV solver (per share, not per lot)."""
    if T <= 0 or sigma <= 0:
        if option_type == "CE":
            return max(0, spot - strike)
        return max(0, strike - spot)

    sqrt_T = math.sqrt(T)
    d1 = (math.log(spot / strike) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T

    if option_type == "CE":
        return spot * _norm_cdf(d1) - strike * math.exp(-r * T) * _norm_cdf(d2)
    else:
        return strike * math.exp(-r * T) * _norm_cdf(-d2) - spot * _norm_cdf(-d1)


def _bs_vega_raw(spot, strike, sigma, T, r):
    """Raw vega (per share) for Newton-Raphson step."""
    if T <= 0 or sigma <= 0:
        return 0.0
    sqrt_T = math.sqrt(T)
    d1 = (math.log(spot / strike) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt_T)
    return spot * sqrt_T * _norm_pdf(d1)


def solve_iv(
    market_price: float,
    spot:         float,
    strike:       float,
    dte:          int,
    r:            float = 0.065,
    option_type:  str   = "CE",
    max_iter:     int   = 50,
    tol:          float = 0.0001,
) -> float:
    """
    Compute implied volatility from market premium using Newton-Raphson.

    market_price: option premium per share (not per lot)
    Returns IV as percentage (e.g. 15.0 for 15%)
    Returns 0.0 if solver fails to converge.
    """
    if market_price <= 0 or dte <= 0 or spot <= 0 or strike <= 0:
        return 0.0

    T = dte / 365.0

    # Intrinsic value check
    if option_type == "CE":
        intrinsic = max(0, spot - strike)
    else:
        intrinsic = max(0, strike - spot)

    if market_price < intrinsic:
        return 0.0  # arbitrage — shouldn't happen in practice

    # Initial guess: Brenner-Subrahmanyam approximation
    # σ ≈ sqrt(2π/T) × (price/spot)
    sigma = math.sqrt(2 * math.pi / T) * (market_price / spot)
    sigma = max(0.01, min(sigma, 5.0))  # clamp to 1% - 500%

    for _ in range(max_iter):
        price = _bs_price(spot, strike, sigma, T, r, option_type)
        vega  = _bs_vega_raw(spot, strike, sigma, T, r)

        if vega < 1e-10:
            break  # vega too small — can't step further

        diff  = price - market_price
        sigma = sigma - diff / vega

        if sigma <= 0:
            sigma = 0.01

        if abs(diff) < tol:
            return round(sigma * 100, 2)  # convert to percentage

    # Fallback — if Newton-Raphson didn't converge, try bisection
    return _solve_iv_bisection(market_price, spot, strike, T, r, option_type)


def _solve_iv_bisection(
    market_price, spot, strike, T, r, option_type,
    low=0.01, high=5.0, max_iter=100, tol=0.001,
) -> float:
    """Bisection fallback for IV solver — always converges."""
    for _ in range(max_iter):
        mid   = (low + high) / 2
        price = _bs_price(spot, strike, mid, T, r, option_type)

        if abs(price - market_price) < tol:
            return round(mid * 100, 2)

        if price > market_price:
            high = mid
        else:
            low = mid

    return round(((low + high) / 2) * 100, 2)


# ─────────────────────────────────────────────────────────────────
# OPTIONS ENGINE
# ─────────────────────────────────────────────────────────────────

class OptionsEngine:
    """
    Computes all options-specific analytics from chain data.
    No LLM calls — pure deterministic computation.
    Stateless — call compute() with fresh data each time.
    """

    def compute(
        self,
        chain_data:     list[dict],
        spot_price:     float,
        futures_price:  float,
        index:          str,
        nearest_expiry: str,
        available_expiries: list[str] = None,
        historical_iv:  list[float] = None,
        historical_closes: list[float] = None,
        previous_chain: list[dict] = None,
        lot_size:       int = 75,
    ) -> OptionsData:
        """
        Main computation entry point.

        chain_data: list of dicts per strike for the nearest expiry:
          {
            strike, expiry,
            call_ltp, call_oi, call_volume, call_iv,
            put_ltp,  put_oi,  put_volume,  put_iv,
          }

        historical_iv:     list of ~252 daily ATM IV values (most recent last)
        historical_closes: list of ~252 daily spot close prices (for HV)
        previous_chain:    previous chain snapshot (for OI shift detection)
        """
        result = OptionsData(
            index         = index,
            spot_price    = spot_price,
            futures_price = futures_price,
            timestamp     = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )

        # Futures basis
        result.futures_basis     = round(futures_price - spot_price, 2)
        result.futures_basis_pct = round(
            (futures_price - spot_price) / spot_price * 100, 3
        ) if spot_price else 0.0

        # Expiry info
        result.nearest_expiry     = nearest_expiry
        result.available_expiries = available_expiries or [nearest_expiry]
        result.dte_nearest        = self._compute_dte(nearest_expiry)

        if available_expiries and len(available_expiries) > 1:
            # Find monthly expiry (last Thursday of a month)
            result.monthly_expiry = available_expiries[-1]
            result.dte_monthly    = self._compute_dte(available_expiries[-1])

        # Filter chain to nearest expiry
        chain = [
            c for c in chain_data
            if c.get("expiry") == nearest_expiry
        ]
        if not chain:
            chain = chain_data  # fallback — use all

        if not chain:
            logger.warning("Empty chain data — returning minimal OptionsData")
            return result

        # ── ATM IV ──────────────────────────────────────
        try:
            atm_strike = self._find_atm_strike(chain, spot_price)
            atm_row    = self._get_strike_row(chain, atm_strike)
            if atm_row:
                call_iv = float(atm_row.get("call_iv", 0) or 0)
                put_iv  = float(atm_row.get("put_iv", 0) or 0)
                result.atm_iv  = round((call_iv + put_iv) / 2, 2) if (call_iv and put_iv) else call_iv or put_iv
                result.iv_skew = round(put_iv - call_iv, 2)
        except Exception as e:
            logger.warning(f"ATM IV computation failed: {e}")

        # ── IV Rank & Percentile ────────────────────────
        try:
            if historical_iv and len(historical_iv) >= 20:
                result.iv_rank       = self._compute_iv_rank(result.atm_iv, historical_iv)
                result.iv_percentile = self._compute_iv_percentile(result.atm_iv, historical_iv)
                result.iv_regime     = self._classify_iv_regime(result.iv_rank)
        except Exception as e:
            logger.warning(f"IV rank/percentile failed: {e}")

        # ── Historical Volatility (20-day) ──────────────
        try:
            if historical_closes and len(historical_closes) >= 21:
                result.historical_vol_20 = self._compute_hv(historical_closes, 20)
                result.iv_hv_spread = round(result.atm_iv - result.historical_vol_20, 2)
        except Exception as e:
            logger.warning(f"HV computation failed: {e}")

        # ── Put-Call Ratio ──────────────────────────────
        try:
            pcr_oi, pcr_vol = self._compute_pcr(chain)
            result.pcr_oi     = pcr_oi
            result.pcr_volume = pcr_vol
            result.pcr_signal = self._classify_pcr(pcr_oi)
        except Exception as e:
            logger.warning(f"PCR computation failed: {e}")

        # ── Max Pain ────────────────────────────────────
        try:
            result.max_pain_strike = self._compute_max_pain(chain)
            result.max_pain_distance = round(spot_price - result.max_pain_strike, 2)
            dist_pct = abs(result.max_pain_distance) / spot_price * 100 if spot_price else 0
            if dist_pct < 0.3:
                result.max_pain_signal = "at_max_pain"
            elif result.max_pain_distance > 0:
                result.max_pain_signal = "above_max_pain_gravitating_down"
            else:
                result.max_pain_signal = "below_max_pain_gravitating_up"
        except Exception as e:
            logger.warning(f"Max pain computation failed: {e}")

        # ── OI Concentration ────────────────────────────
        try:
            oi_walls = self._compute_oi_walls(chain)
            result.highest_call_oi_strike = oi_walls["call_strike"]
            result.highest_put_oi_strike  = oi_walls["put_strike"]
            result.call_oi_wall           = oi_walls["call_oi"]
            result.put_oi_wall            = oi_walls["put_oi"]
        except Exception as e:
            logger.warning(f"OI wall computation failed: {e}")

        # ── OI Shift Detection ──────────────────────────
        try:
            if previous_chain:
                result.oi_shift_direction = self._detect_oi_shift(
                    chain, previous_chain
                )
        except Exception as e:
            logger.warning(f"OI shift detection failed: {e}")

        # ── ATM Greeks ──────────────────────────────────
        try:
            if atm_row and result.atm_iv > 0:
                dte = result.dte_nearest
                call_greeks = compute_bs_greeks(
                    spot_price, atm_strike, result.atm_iv,
                    dte, option_type="CE", lot_size=lot_size,
                )
                put_greeks = compute_bs_greeks(
                    spot_price, atm_strike, result.atm_iv,
                    dte, option_type="PE", lot_size=lot_size,
                )
                result.atm_call_delta = call_greeks["delta"]
                result.atm_call_theta = call_greeks["theta"]
                result.atm_call_gamma = call_greeks["gamma"]
                result.atm_call_vega  = call_greeks["vega"]
                result.atm_put_delta  = put_greeks["delta"]
                result.atm_put_theta  = put_greeks["theta"]
                result.atm_put_gamma  = put_greeks["gamma"]
                result.atm_put_vega   = put_greeks["vega"]
        except Exception as e:
            logger.warning(f"ATM Greeks failed: {e}")

        # ── Chain Snapshot (10 above + 10 below ATM) ────
        try:
            result.chain_snapshot = self._build_chain_snapshot(
                chain, spot_price, atm_strike,
                result.dte_nearest, lot_size, n_each_side=10,
            )
        except Exception as e:
            logger.warning(f"Chain snapshot failed: {e}")

        # ── IV Term Structure ───────────────────────────
        try:
            if available_expiries and len(available_expiries) >= 2:
                near_chain = [c for c in chain_data if c.get("expiry") == available_expiries[0]]
                far_chain  = [c for c in chain_data if c.get("expiry") == available_expiries[-1]]
                result.iv_term_structure = self._compute_term_structure(
                    near_chain, far_chain, spot_price
                )
        except Exception as e:
            logger.warning(f"Term structure failed: {e}")

        logger.info(
            f"Options computed: {index} | "
            f"IV={result.atm_iv:.1f}% rank={result.iv_rank:.0f} | "
            f"PCR={result.pcr_oi:.2f} | "
            f"MaxPain={result.max_pain_strike:,.0f}"
        )
        return result

    # ─────────────────────────────────────────────────────────
    # PRIVATE METHODS
    # ─────────────────────────────────────────────────────────

    def _compute_dte(self, expiry_str: str) -> int:
        """Days to expiry from date string."""
        try:
            expiry_date = datetime.strptime(expiry_str[:10], "%Y-%m-%d").date()
            today       = datetime.today().date()
            return max(0, (expiry_date - today).days)
        except Exception:
            return 0

    def _find_atm_strike(self, chain: list[dict], spot: float) -> float:
        """Find the strike closest to spot price."""
        strikes = sorted(set(float(c["strike"]) for c in chain if c.get("strike")))
        if not strikes:
            return spot
        return min(strikes, key=lambda s: abs(s - spot))

    def _get_strike_row(self, chain: list[dict], strike: float) -> dict:
        """Get chain row for a specific strike."""
        for c in chain:
            if float(c.get("strike", 0)) == strike:
                return c
        return {}

    def _compute_iv_rank(self, current_iv: float, historical: list[float]) -> float:
        """IV Rank = (current - 1yr low) / (1yr high - 1yr low) × 100"""
        if not historical:
            return 50.0
        iv_high = max(historical)
        iv_low  = min(historical)
        if iv_high == iv_low:
            return 50.0
        return round(
            (current_iv - iv_low) / (iv_high - iv_low) * 100, 1
        )

    def _compute_iv_percentile(self, current_iv: float, historical: list[float]) -> float:
        """IV Percentile = % of days in last year where IV was lower than today."""
        if not historical:
            return 50.0
        below = sum(1 for iv in historical if iv < current_iv)
        return round(below / len(historical) * 100, 1)

    def _classify_iv_regime(self, iv_rank: float) -> str:
        """Classify IV environment."""
        if iv_rank >= 80:
            return "extreme"
        elif iv_rank >= 60:
            return "high"
        elif iv_rank <= 20:
            return "low"
        else:
            return "normal"

    def _compute_hv(self, closes: list[float], window: int = 20) -> float:
        """
        20-day historical (realised) volatility.
        HV = stdev(daily log returns) × sqrt(252) × 100
        """
        if len(closes) < window + 1:
            return 0.0
        recent = closes[-(window + 1):]
        log_returns = [
            math.log(recent[i] / recent[i - 1])
            for i in range(1, len(recent))
            if recent[i - 1] > 0
        ]
        if len(log_returns) < window:
            return 0.0
        stdev = float(np.std(log_returns, ddof=1))
        return round(stdev * math.sqrt(252) * 100, 2)

    def _compute_pcr(self, chain: list[dict]) -> tuple:
        """Put-Call Ratio by OI and Volume."""
        total_call_oi  = sum(int(c.get("call_oi", 0) or 0)     for c in chain)
        total_put_oi   = sum(int(c.get("put_oi", 0) or 0)      for c in chain)
        total_call_vol = sum(int(c.get("call_volume", 0) or 0)  for c in chain)
        total_put_vol  = sum(int(c.get("put_volume", 0) or 0)   for c in chain)

        pcr_oi  = round(total_put_oi / total_call_oi, 3)   if total_call_oi  else 0.0
        pcr_vol = round(total_put_vol / total_call_vol, 3)  if total_call_vol else 0.0
        return pcr_oi, pcr_vol

    def _classify_pcr(self, pcr: float) -> str:
        """Classify PCR signal."""
        from config import PCR_BULLISH_THRESHOLD, PCR_BEARISH_THRESHOLD
        if pcr > PCR_BEARISH_THRESHOLD:
            return "bearish"
        elif pcr < PCR_BULLISH_THRESHOLD:
            return "bullish"
        else:
            return "neutral"

    def _compute_max_pain(self, chain: list[dict]) -> float:
        """
        Max pain = strike where total option writer loss is minimized.
        For each possible expiry price (= each strike):
          pain = Σ(call_oi × max(0, price - strike) + put_oi × max(0, strike - price))
        The strike with minimum total pain is max pain.
        """
        strikes = sorted(set(float(c["strike"]) for c in chain if c.get("strike")))
        if not strikes:
            return 0.0

        # Build OI lookup
        call_oi_map = {}
        put_oi_map  = {}
        for c in chain:
            s = float(c.get("strike", 0))
            call_oi_map[s] = int(c.get("call_oi", 0) or 0)
            put_oi_map[s]  = int(c.get("put_oi", 0) or 0)

        min_pain   = float("inf")
        max_pain_strike = strikes[len(strikes) // 2]

        for test_price in strikes:
            total_pain = 0
            for s in strikes:
                call_pain = call_oi_map.get(s, 0) * max(0, test_price - s)
                put_pain  = put_oi_map.get(s, 0)  * max(0, s - test_price)
                total_pain += call_pain + put_pain
            if total_pain < min_pain:
                min_pain        = total_pain
                max_pain_strike = test_price

        return max_pain_strike

    def _compute_oi_walls(self, chain: list[dict]) -> dict:
        """Find strikes with highest call OI and put OI."""
        max_call_oi = 0
        max_put_oi  = 0
        call_strike = 0.0
        put_strike  = 0.0

        for c in chain:
            coi = int(c.get("call_oi", 0) or 0)
            poi = int(c.get("put_oi", 0) or 0)
            s   = float(c.get("strike", 0))

            if coi > max_call_oi:
                max_call_oi = coi
                call_strike = s
            if poi > max_put_oi:
                max_put_oi = poi
                put_strike = s

        return {
            "call_strike": call_strike,
            "call_oi":     max_call_oi,
            "put_strike":  put_strike,
            "put_oi":      max_put_oi,
        }

    def _detect_oi_shift(
        self,
        current_chain:  list[dict],
        previous_chain: list[dict],
    ) -> str:
        """Detect if call/put OI walls have shifted."""
        curr_walls = self._compute_oi_walls(current_chain)
        prev_walls = self._compute_oi_walls(previous_chain)

        call_shift = curr_walls["call_strike"] - prev_walls["call_strike"]
        put_shift  = curr_walls["put_strike"]  - prev_walls["put_strike"]

        if call_shift > 0 and put_shift > 0:
            return "both_shifting_up"
        elif call_shift < 0 and put_shift < 0:
            return "both_shifting_down"
        elif call_shift > 0:
            return "call_writers_moving_up"
        elif put_shift > 0:
            return "put_writers_moving_up"
        else:
            return "stable"

    def _build_chain_snapshot(
        self,
        chain:     list[dict],
        spot:      float,
        atm_strike: float,
        dte:       int,
        lot_size:  int,
        n_each_side: int = 10,
    ) -> list[dict]:
        """Build compact chain snapshot with Greeks for prompt."""
        strikes = sorted(set(float(c["strike"]) for c in chain))
        if not strikes:
            return []

        # Find ATM index
        atm_idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - atm_strike))
        start   = max(0, atm_idx - n_each_side)
        end     = min(len(strikes), atm_idx + n_each_side + 1)
        selected = strikes[start:end]

        snapshot = []
        for s in selected:
            row = self._get_strike_row(chain, s)
            if not row:
                continue

            call_iv = float(row.get("call_iv", 0) or 0)
            put_iv  = float(row.get("put_iv", 0) or 0)

            # Compute Greeks for each strike
            call_greeks = {}
            put_greeks  = {}
            if call_iv > 0 and dte > 0:
                call_greeks = compute_bs_greeks(
                    spot, s, call_iv, dte,
                    option_type="CE", lot_size=lot_size,
                )
            if put_iv > 0 and dte > 0:
                put_greeks = compute_bs_greeks(
                    spot, s, put_iv, dte,
                    option_type="PE", lot_size=lot_size,
                )

            snapshot.append({
                "strike":     s,
                "call_ltp":   float(row.get("call_ltp", 0) or 0),
                "call_iv":    call_iv,
                "call_oi":    int(row.get("call_oi", 0) or 0),
                "call_delta": call_greeks.get("delta", 0),
                "call_theta": call_greeks.get("theta", 0),
                "put_ltp":    float(row.get("put_ltp", 0) or 0),
                "put_iv":     put_iv,
                "put_oi":     int(row.get("put_oi", 0) or 0),
                "put_delta":  put_greeks.get("delta", 0),
                "put_theta":  put_greeks.get("theta", 0),
            })

        return snapshot

    def _compute_term_structure(
        self,
        near_chain: list[dict],
        far_chain:  list[dict],
        spot:       float,
    ) -> str:
        """
        Compare ATM IV of near vs far expiry.
        contango: far IV > near IV (normal)
        backwardation: near IV > far IV (event risk)
        """
        def get_atm_iv(chain):
            atm = self._find_atm_strike(chain, spot)
            row = self._get_strike_row(chain, atm)
            if row:
                c_iv = float(row.get("call_iv", 0) or 0)
                p_iv = float(row.get("put_iv", 0) or 0)
                return (c_iv + p_iv) / 2 if (c_iv and p_iv) else c_iv or p_iv
            return 0

        near_iv = get_atm_iv(near_chain) if near_chain else 0
        far_iv  = get_atm_iv(far_chain)  if far_chain  else 0

        if near_iv == 0 or far_iv == 0:
            return "unknown"

        diff = far_iv - near_iv
        if diff > 1.0:
            return "contango"
        elif diff < -1.0:
            return "backwardation"
        else:
            return "flat"
