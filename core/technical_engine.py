"""
COMMODEX — Technical Indicator Engine v2.0
Converts raw Groww candles into a full indicator set.

Uses 'ta' library (Windows compatible, Python 3.12 compatible)
instead of pandas-ta which has posix module issues on Windows.

v2.0 additions:
  TIER 1 (high ROI impact):
    - VWAP + VWAP bands (±1σ, ±2σ)
    - ADX + DI+/DI- (trend strength + direction)
    - Open Interest tracking (if available in candle data)
    - RSI divergence detection (bullish/bearish)
    - Bollinger Band squeeze detection
    - Volume-price confirmation

  TIER 2 (moderate ROI impact):
    - Stochastic RSI (%K, %D)
    - Supertrend (ATR multiplier 3, period 10)
    - EMA 200 (weekly anchor for positional trades)
    - Fibonacci retracement levels (auto-computed from swing H/L)

Input:  raw candles from GrowwClient.get_historical()
Output: TechnicalData dataclass ready for Agent 1 prompt
"""

import logging
import numpy as np
import pandas as pd
import ta
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# OUTPUT DATACLASS
# ─────────────────────────────────────────────────────────────────

@dataclass
class TechnicalData:
    """
    Complete technical indicator set for one commodity contract.
    All float values rounded to 2 decimal places.
    None means insufficient data to compute.
    """
    symbol:          str
    timeframe:       str
    candle_count:    int
    latest_price:    float
    latest_time:     str

    # ── Trend ─────────────────────────────────────
    ema_20:          Optional[float] = None
    ema_50:          Optional[float] = None
    ema_200:         Optional[float] = None     # NEW v2.0 — weekly anchor
    ema_trend:       Optional[str]   = None
    ema_200_trend:   Optional[str]   = None     # NEW v2.0 — above/below 200

    # ── ADX — Trend Strength (NEW v2.0) ──────────
    adx_14:          Optional[float] = None     # ADX value (0-100)
    adx_signal:      Optional[str]   = None     # trending | ranging | strong_trend
    plus_di:         Optional[float] = None     # +DI directional indicator
    minus_di:        Optional[float] = None     # -DI directional indicator
    di_cross:        Optional[str]   = None     # bullish_cross | bearish_cross | bullish | bearish

    # ── Momentum ──────────────────────────────────
    rsi_14:          Optional[float] = None
    rsi_signal:      Optional[str]   = None
    rsi_divergence:  Optional[str]   = None     # NEW v2.0 — bullish | bearish | none

    # ── Stochastic RSI (NEW v2.0) ─────────────────
    stoch_rsi_k:     Optional[float] = None     # %K line (0-100)
    stoch_rsi_d:     Optional[float] = None     # %D line (0-100)
    stoch_rsi_signal: Optional[str]  = None     # overbought | oversold | bullish_cross | bearish_cross | neutral

    # ── MACD ──────────────────────────────────────
    macd_line:       Optional[float] = None
    macd_signal:     Optional[float] = None
    macd_histogram:  Optional[float] = None
    macd_cross:      Optional[str]   = None

    # ── Bollinger Bands ───────────────────────────
    bb_upper:        Optional[float] = None
    bb_mid:          Optional[float] = None
    bb_lower:        Optional[float] = None
    bb_position:     Optional[str]   = None
    bb_width:        Optional[float] = None
    bb_squeeze:      Optional[bool]  = None     # NEW v2.0 — True = squeeze active

    # ── VWAP (NEW v2.0) ──────────────────────────
    vwap:            Optional[float] = None     # session VWAP
    vwap_upper_1:    Optional[float] = None     # +1σ band
    vwap_lower_1:    Optional[float] = None     # -1σ band
    vwap_upper_2:    Optional[float] = None     # +2σ band
    vwap_lower_2:    Optional[float] = None     # -2σ band
    vwap_position:   Optional[str]   = None     # above_vwap | below_vwap | at_vwap

    # ── Supertrend (NEW v2.0) ────────────────────
    supertrend:      Optional[float] = None     # supertrend level
    supertrend_dir:  Optional[str]   = None     # bullish | bearish
    supertrend_flip: Optional[bool]  = None     # True if flipped on latest candle

    # ── Volatility ────────────────────────────────
    atr_14:          Optional[float] = None
    atr_pct:         Optional[float] = None

    # ── Pivot Points ──────────────────────────────
    pivot:           Optional[float] = None
    r1:              Optional[float] = None
    r2:              Optional[float] = None
    s1:              Optional[float] = None
    s2:              Optional[float] = None

    # ── Fibonacci Retracement (NEW v2.0) ──────────
    fib_swing_high:  Optional[float] = None     # swing high used
    fib_swing_low:   Optional[float] = None     # swing low used
    fib_trend:       Optional[str]   = None     # up (retrace from high) | down (retrace from low)
    fib_236:         Optional[float] = None     # 23.6% level
    fib_382:         Optional[float] = None     # 38.2% level
    fib_500:         Optional[float] = None     # 50.0% level
    fib_618:         Optional[float] = None     # 61.8% level
    fib_786:         Optional[float] = None     # 78.6% level

    # ── Key Levels ────────────────────────────────
    day_high:        Optional[float] = None
    day_low:         Optional[float] = None
    prev_day_high:   Optional[float] = None
    prev_day_low:    Optional[float] = None
    week_high:       Optional[float] = None
    week_low:        Optional[float] = None

    # ── Volume ────────────────────────────────────
    volume_current:  Optional[int]   = None
    volume_avg_20:   Optional[float] = None
    volume_signal:   Optional[str]   = None
    volume_price_confirm: Optional[str] = None  # NEW v2.0 — confirmed | weak | divergent

    # ── Open Interest (NEW v2.0) ──────────────────
    oi_current:      Optional[int]   = None     # latest OI
    oi_prev_day:     Optional[int]   = None     # previous day OI
    oi_change_pct:   Optional[float] = None     # % change
    oi_interpretation: Optional[str] = None     # fresh_longs | short_covering | fresh_shorts | long_unwinding

    def to_prompt_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None}

    def summary_string(self) -> str:
        """Compact summary for Agent 1 prompt injection."""
        lines = [
            f"Symbol: {self.symbol} | Timeframe: {self.timeframe} | "
            f"Price: Rs{self.latest_price:,.2f} | "
            f"Candles: {self.candle_count} | Time: {self.latest_time}",
        ]

        # ── Trend ─────────────────────────────────
        if self.rsi_14 is not None:
            div_tag = f" [{self.rsi_divergence}]" if self.rsi_divergence and self.rsi_divergence != "none" else ""
            lines.append(
                f"RSI(14)   : {self.rsi_14} [{self.rsi_signal}]{div_tag}"
            )
        if self.stoch_rsi_k is not None:
            lines.append(
                f"StochRSI  : K={self.stoch_rsi_k} D={self.stoch_rsi_d} "
                f"[{self.stoch_rsi_signal}]"
            )
        if self.macd_line is not None:
            lines.append(
                f"MACD      : line={self.macd_line}  "
                f"signal={self.macd_signal}  "
                f"hist={self.macd_histogram}  [{self.macd_cross}]"
            )
        if self.ema_20 and self.ema_50:
            ema200_tag = f"  200={self.ema_200:,.0f}" if self.ema_200 else ""
            lines.append(
                f"EMA       : 20={self.ema_20:,.0f}  "
                f"50={self.ema_50:,.0f}{ema200_tag}  [{self.ema_trend}]"
            )
            if self.ema_200_trend:
                lines.append(
                    f"EMA200    : [{self.ema_200_trend}] — positional anchor"
                )
        if self.adx_14 is not None:
            lines.append(
                f"ADX(14)   : {self.adx_14} [{self.adx_signal}]  "
                f"+DI={self.plus_di}  -DI={self.minus_di}  [{self.di_cross}]"
            )
        if self.supertrend is not None:
            flip_tag = " ⚡FLIP" if self.supertrend_flip else ""
            lines.append(
                f"Supertrend: Rs{self.supertrend:,.0f} [{self.supertrend_dir}]{flip_tag}"
            )

        # ── Bands & Volatility ────────────────────
        if self.bb_upper:
            squeeze_tag = " ⚠ SQUEEZE" if self.bb_squeeze else ""
            lines.append(
                f"BB        : upper={self.bb_upper:,.0f}  "
                f"mid={self.bb_mid:,.0f}  "
                f"lower={self.bb_lower:,.0f}  "
                f"[{self.bb_position}]  width={self.bb_width}%{squeeze_tag}"
            )
        if self.vwap is not None:
            lines.append(
                f"VWAP      : Rs{self.vwap:,.0f}  [{self.vwap_position}]  "
                f"bands: ±1σ={self.vwap_upper_1:,.0f}/{self.vwap_lower_1:,.0f}  "
                f"±2σ={self.vwap_upper_2:,.0f}/{self.vwap_lower_2:,.0f}"
            )
        if self.atr_14:
            lines.append(
                f"ATR(14)   : {self.atr_14:,.0f}  ({self.atr_pct}% of price)"
            )

        # ── Levels ────────────────────────────────
        if self.pivot:
            lines.append(
                f"Pivots    : P={self.pivot:,.0f}  "
                f"R1={self.r1:,.0f}  R2={self.r2:,.0f}  "
                f"S1={self.s1:,.0f}  S2={self.s2:,.0f}"
            )
        if self.fib_382 is not None:
            lines.append(
                f"Fibonacci : swing {self.fib_trend}  "
                f"38.2%={self.fib_382:,.0f}  "
                f"50%={self.fib_500:,.0f}  "
                f"61.8%={self.fib_618:,.0f}"
            )
        if self.day_high and self.day_low:
            pdh = f"{self.prev_day_high:,.0f}" if self.prev_day_high else "N/A"
            pdl = f"{self.prev_day_low:,.0f}" if self.prev_day_low else "N/A"
            lines.append(
                f"Day range : H={self.day_high:,.0f}  L={self.day_low:,.0f}  "
                f"| PDH={pdh}  PDL={pdl}"
            )

        # ── Volume & OI ───────────────────────────
        if self.volume_signal:
            vpc_tag = f"  [{self.volume_price_confirm}]" if self.volume_price_confirm else ""
            lines.append(
                f"Volume    : {self.volume_current:,}  "
                f"(avg20={self.volume_avg_20:,.0f})  [{self.volume_signal}]{vpc_tag}"
            )
        if self.oi_current is not None:
            lines.append(
                f"OI        : {self.oi_current:,}  "
                f"change={self.oi_change_pct:+.1f}%  "
                f"[{self.oi_interpretation}]"
            )

        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────
# ENGINE
# ─────────────────────────────────────────────────────────────────

class TechnicalEngine:
    """
    Computes all technical indicators from raw candle data.
    Stateless — call compute() with fresh candles each time.
    Uses 'ta' library — Windows and Python 3.12 compatible.

    v2.0: Added VWAP, ADX, StochRSI, Supertrend, EMA200,
          Fibonacci, OI tracking, RSI divergence, BB squeeze,
          and volume-price confirmation.
    """

    def candles_to_df(self, candles: list[dict]) -> pd.DataFrame:
        """
        Convert raw candle list to clean pandas DataFrame.
        Handles both dict format and raw list format from Groww.
        """
        if not candles:
            raise ValueError("Empty candles list")

        rows = []
        for c in candles:
            if isinstance(c, dict):
                rows.append({
                    "timestamp": c.get("timestamp"),
                    "open":      float(c.get("open",  0)),
                    "high":      float(c.get("high",  0)),
                    "low":       float(c.get("low",   0)),
                    "close":     float(c.get("close", 0)),
                    "volume":    int(c.get("volume",  0)),
                    "oi":        int(c.get("oi", c.get("open_interest", 0))),
                })
            elif isinstance(c, (list, tuple)) and len(c) >= 5:
                rows.append({
                    "timestamp": c[0],
                    "open":      float(c[1]),
                    "high":      float(c[2]),
                    "low":       float(c[3]),
                    "close":     float(c[4]),
                    "volume":    int(c[5]) if len(c) > 5 else 0,
                    "oi":        int(c[6]) if len(c) > 6 else 0,
                })

        df = pd.DataFrame(rows)

        # Convert timestamp to datetime
        sample_ts = df["timestamp"].iloc[0]
        if sample_ts > 1e12:
            df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
        else:
            df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")

        df = df.set_index("datetime").sort_index()
        df = df[["open", "high", "low", "close", "volume", "oi"]]
        df = df[df["close"] > 0]

        logger.info(
            f"DataFrame: {len(df)} candles | "
            f"{df.index[0]} to {df.index[-1]}"
        )
        return df

    def compute(
        self,
        candles: list[dict],
        symbol: str = "UNKNOWN",
        timeframe: str = "15minute",
    ) -> TechnicalData:
        """
        Compute full indicator set from raw candles.
        Each indicator block is independent — one failure
        does not block others.
        """
        df = self.candles_to_df(candles)
        latest       = df.iloc[-1]
        latest_price = round(float(latest["close"]), 2)
        latest_time  = str(df.index[-1])

        result = TechnicalData(
            symbol       = symbol,
            timeframe    = timeframe,
            candle_count = len(df),
            latest_price = latest_price,
            latest_time  = latest_time,
        )

        # ── EMA (20, 50, 200) ────────────────────────────────
        try:
            ema20 = round(float(
                ta.trend.EMAIndicator(df["close"], window=20)
                .ema_indicator().iloc[-1]
            ), 2)
            ema50 = round(float(
                ta.trend.EMAIndicator(df["close"], window=50)
                .ema_indicator().iloc[-1]
            ), 2)
            result.ema_20 = ema20
            result.ema_50 = ema50

            if latest_price > ema20 > ema50:
                result.ema_trend = "above_both_bullish"
            elif latest_price < ema20 < ema50:
                result.ema_trend = "below_both_bearish"
            elif ema20 > ema50:
                result.ema_trend = "above_ema50_bullish"
            else:
                result.ema_trend = "below_ema50_bearish"

            # EMA 200 — needs 200+ candles to be meaningful
            if len(df) >= 200:
                ema200 = round(float(
                    ta.trend.EMAIndicator(df["close"], window=200)
                    .ema_indicator().iloc[-1]
                ), 2)
                result.ema_200 = ema200
                if latest_price > ema200:
                    result.ema_200_trend = "above_200_bullish_bias"
                else:
                    result.ema_200_trend = "below_200_bearish_bias"
            else:
                logger.info(
                    f"EMA200 skipped — need 200 candles, have {len(df)}"
                )
        except Exception as e:
            logger.warning(f"EMA failed: {e}")

        # ── ADX + DI+/DI- (Trend Strength) ──────────────────
        # NEW v2.0 — objective trend strength measurement
        try:
            adx_ind = ta.trend.ADXIndicator(
                df["high"], df["low"], df["close"], window=14
            )
            adx_val  = round(float(adx_ind.adx().iloc[-1]), 2)
            plus_di  = round(float(adx_ind.adx_pos().iloc[-1]), 2)
            minus_di = round(float(adx_ind.adx_neg().iloc[-1]), 2)

            result.adx_14   = adx_val
            result.plus_di  = plus_di
            result.minus_di = minus_di

            # ADX signal interpretation
            if adx_val >= 40:
                result.adx_signal = "strong_trend"
            elif adx_val >= 25:
                result.adx_signal = "trending"
            elif adx_val >= 20:
                result.adx_signal = "weak_trend"
            else:
                result.adx_signal = "ranging"

            # DI crossover detection
            prev_plus_di  = float(adx_ind.adx_pos().iloc[-2])
            prev_minus_di = float(adx_ind.adx_neg().iloc[-2])

            if plus_di > minus_di and prev_plus_di <= prev_minus_di:
                result.di_cross = "bullish_crossover"
            elif minus_di > plus_di and prev_minus_di <= prev_plus_di:
                result.di_cross = "bearish_crossover"
            elif plus_di > minus_di:
                result.di_cross = "bullish"
            else:
                result.di_cross = "bearish"

        except Exception as e:
            logger.warning(f"ADX failed: {e}")

        # ── RSI + Divergence Detection ───────────────────────
        try:
            rsi_series = ta.momentum.RSIIndicator(
                df["close"], window=14
            ).rsi()
            rsi_val = round(float(rsi_series.iloc[-1]), 2)
            result.rsi_14 = rsi_val

            if rsi_val >= 70:
                result.rsi_signal = "overbought"
            elif rsi_val <= 30:
                result.rsi_signal = "oversold"
            elif rsi_val >= 55:
                result.rsi_signal = "bullish_neutral"
            elif rsi_val <= 45:
                result.rsi_signal = "bearish_neutral"
            else:
                result.rsi_signal = "neutral"

            # RSI divergence detection — compare last two peaks/troughs
            result.rsi_divergence = self._detect_rsi_divergence(
                df["close"], rsi_series
            )

        except Exception as e:
            logger.warning(f"RSI failed: {e}")

        # ── Stochastic RSI (NEW v2.0) ────────────────────────
        # Applies stochastic oscillator to RSI for faster signals
        try:
            stoch_rsi = ta.momentum.StochRSIIndicator(
                df["close"], window=14, smooth1=3, smooth2=3
            )
            k_val = round(float(stoch_rsi.stochrsi_k().iloc[-1]) * 100, 2)
            d_val = round(float(stoch_rsi.stochrsi_d().iloc[-1]) * 100, 2)
            prev_k = float(stoch_rsi.stochrsi_k().iloc[-2]) * 100
            prev_d = float(stoch_rsi.stochrsi_d().iloc[-2]) * 100

            result.stoch_rsi_k = k_val
            result.stoch_rsi_d = d_val

            if k_val > 80 and d_val > 80:
                result.stoch_rsi_signal = "overbought"
            elif k_val < 20 and d_val < 20:
                result.stoch_rsi_signal = "oversold"
            elif k_val > d_val and prev_k <= prev_d and k_val < 30:
                result.stoch_rsi_signal = "bullish_cross_oversold"
            elif k_val > d_val and prev_k <= prev_d:
                result.stoch_rsi_signal = "bullish_cross"
            elif k_val < d_val and prev_k >= prev_d and k_val > 70:
                result.stoch_rsi_signal = "bearish_cross_overbought"
            elif k_val < d_val and prev_k >= prev_d:
                result.stoch_rsi_signal = "bearish_cross"
            else:
                result.stoch_rsi_signal = "neutral"

        except Exception as e:
            logger.warning(f"StochRSI failed: {e}")

        # ── MACD ──────────────────────────────────────────────
        try:
            macd_ind = ta.trend.MACD(
                df["close"], window_fast=12, window_slow=26, window_sign=9
            )
            macd_line = round(float(macd_ind.macd().iloc[-1]),          2)
            macd_sig  = round(float(macd_ind.macd_signal().iloc[-1]),   2)
            macd_hist = round(float(macd_ind.macd_diff().iloc[-1]),     2)
            prev_hist = float(macd_ind.macd_diff().iloc[-2])

            result.macd_line      = macd_line
            result.macd_signal    = macd_sig
            result.macd_histogram = macd_hist

            if macd_hist > 0 and prev_hist <= 0:
                result.macd_cross = "bullish_crossover"
            elif macd_hist < 0 and prev_hist >= 0:
                result.macd_cross = "bearish_crossover"
            elif macd_hist > 0:
                result.macd_cross = "bullish"
            else:
                result.macd_cross = "bearish"
        except Exception as e:
            logger.warning(f"MACD failed: {e}")

        # ── Bollinger Bands + Squeeze Detection ──────────────
        try:
            bb_ind = ta.volatility.BollingerBands(
                df["close"], window=20, window_dev=2
            )
            bb_upper = round(float(bb_ind.bollinger_hband().iloc[-1]), 2)
            bb_mid   = round(float(bb_ind.bollinger_mavg().iloc[-1]),  2)
            bb_lower = round(float(bb_ind.bollinger_lband().iloc[-1]), 2)

            result.bb_upper = bb_upper
            result.bb_mid   = bb_mid
            result.bb_lower = bb_lower

            bb_width_current = (bb_upper - bb_lower) / bb_mid * 100 if bb_mid else 0
            result.bb_width = round(bb_width_current, 2)

            if latest_price > bb_upper:
                result.bb_position = "above_upper_overbought"
            elif latest_price > bb_mid:
                result.bb_position = "upper_half_bullish"
            elif latest_price > bb_lower:
                result.bb_position = "lower_half_bearish"
            else:
                result.bb_position = "below_lower_oversold"

            # BB squeeze — width at 20-period low means breakout imminent
            bb_width_series = bb_ind.bollinger_wband()
            if len(bb_width_series) >= 20:
                recent_widths = bb_width_series.tail(20)
                current_width = float(bb_width_series.iloc[-1])
                min_width     = float(recent_widths.min())
                # Squeeze if current width within 5% of 20-period minimum
                result.bb_squeeze = (
                    current_width <= min_width * 1.05
                )
            else:
                result.bb_squeeze = False

        except Exception as e:
            logger.warning(f"Bollinger Bands failed: {e}")

        # ── VWAP + Bands (NEW v2.0) ──────────────────────────
        # Session VWAP resets daily — critical for intraday MCX
        try:
            result_vwap = self._compute_vwap(df, latest_price)
            result.vwap         = result_vwap["vwap"]
            result.vwap_upper_1 = result_vwap["upper_1"]
            result.vwap_lower_1 = result_vwap["lower_1"]
            result.vwap_upper_2 = result_vwap["upper_2"]
            result.vwap_lower_2 = result_vwap["lower_2"]
            result.vwap_position = result_vwap["position"]
        except Exception as e:
            logger.warning(f"VWAP failed: {e}")

        # ── Supertrend (NEW v2.0) ────────────────────────────
        # ATR multiplier=3, period=10 — popular MCX settings
        try:
            st_result = self._compute_supertrend(df, period=10, multiplier=3.0)
            result.supertrend      = st_result["level"]
            result.supertrend_dir  = st_result["direction"]
            result.supertrend_flip = st_result["flipped"]
        except Exception as e:
            logger.warning(f"Supertrend failed: {e}")

        # ── ATR ───────────────────────────────────────────────
        try:
            atr_val = round(float(
                ta.volatility.AverageTrueRange(
                    df["high"], df["low"], df["close"], window=14
                ).average_true_range().iloc[-1]
            ), 2)
            result.atr_14  = atr_val
            result.atr_pct = round(
                atr_val / latest_price * 100, 3
            ) if latest_price else None
        except Exception as e:
            logger.warning(f"ATR failed: {e}")

        # ── Pivot Points (Classic) ────────────────────────────
        try:
            daily = df["close"].resample("D").ohlc()
            if len(daily) >= 2:
                prev  = daily.iloc[-2]
                h     = float(prev["high"])
                l     = float(prev["low"])
                c     = float(prev["close"])
                pivot = round((h + l + c) / 3, 2)
                result.pivot = pivot
                result.r1    = round(2 * pivot - l, 2)
                result.r2    = round(pivot + (h - l), 2)
                result.s1    = round(2 * pivot - h, 2)
                result.s2    = round(pivot - (h - l), 2)
        except Exception as e:
            logger.warning(f"Pivot Points failed: {e}")

        # ── Fibonacci Retracement (NEW v2.0) ──────────────────
        try:
            fib = self._compute_fibonacci(df)
            if fib:
                result.fib_swing_high = fib["swing_high"]
                result.fib_swing_low  = fib["swing_low"]
                result.fib_trend      = fib["trend"]
                result.fib_236        = fib["fib_236"]
                result.fib_382        = fib["fib_382"]
                result.fib_500        = fib["fib_500"]
                result.fib_618        = fib["fib_618"]
                result.fib_786        = fib["fib_786"]
        except Exception as e:
            logger.warning(f"Fibonacci failed: {e}")

        # ── Key Levels ────────────────────────────────────────
        try:
            today    = df.index[-1].date()
            today_df = df[df.index.date == today]
            if not today_df.empty:
                result.day_high = round(float(today_df["high"].max()), 2)
                result.day_low  = round(float(today_df["low"].min()),  2)

            unique_dates = sorted(set(df.index.date.tolist()))
            if len(unique_dates) >= 2:
                prev_date = unique_dates[-2]
                prev_df   = df[df.index.date == prev_date]
                result.prev_day_high = round(float(prev_df["high"].max()), 2)
                result.prev_day_low  = round(float(prev_df["low"].min()),  2)

            week_start = df.index[-1] - pd.Timedelta(days=7)
            week_df    = df[df.index >= week_start]
            result.week_high = round(float(week_df["high"].max()), 2)
            result.week_low  = round(float(week_df["low"].min()),  2)
        except Exception as e:
            logger.warning(f"Key levels failed: {e}")

        # ── Volume + Price Confirmation (ENHANCED v2.0) ──────
        try:
            vol_current = int(latest["volume"])
            vol_avg     = round(float(df["volume"].tail(20).mean()), 0)
            result.volume_current = vol_current
            result.volume_avg_20  = vol_avg

            ratio = vol_current / vol_avg if vol_avg > 0 else 1
            if ratio >= 1.5:
                result.volume_signal = "high"
            elif ratio <= 0.5:
                result.volume_signal = "low"
            else:
                result.volume_signal = "normal"

            # Volume-price confirmation
            # Compare last close vs previous close with volume
            if len(df) >= 2:
                prev_close = float(df.iloc[-2]["close"])
                price_up   = latest_price > prev_close
                vol_high   = ratio >= 1.2

                if price_up and vol_high:
                    result.volume_price_confirm = "confirmed_bullish"
                elif not price_up and vol_high:
                    result.volume_price_confirm = "confirmed_bearish"
                elif price_up and not vol_high:
                    result.volume_price_confirm = "weak_bullish"
                elif not price_up and not vol_high:
                    result.volume_price_confirm = "weak_bearish"
                else:
                    result.volume_price_confirm = "neutral"
            else:
                result.volume_price_confirm = "insufficient_data"

        except Exception as e:
            logger.warning(f"Volume failed: {e}")

        # ── Open Interest (NEW v2.0) ──────────────────────────
        # OI data depends on Groww providing it in candle payload
        try:
            self._compute_oi(df, result)
        except Exception as e:
            logger.warning(f"OI computation failed: {e}")

        logger.info(
            f"Indicators computed: {symbol} | "
            f"RSI={result.rsi_14} | MACD={result.macd_cross} | "
            f"ADX={result.adx_14} | VWAP={result.vwap} | "
            f"ATR={result.atr_14} | ST={result.supertrend_dir}"
        )
        return result

    # ─────────────────────────────────────────────────────────
    # PRIVATE HELPER METHODS
    # ─────────────────────────────────────────────────────────

    def _compute_vwap(
        self,
        df: pd.DataFrame,
        latest_price: float,
    ) -> dict:
        """
        Compute session VWAP with ±1σ and ±2σ bands.
        VWAP resets at the start of each trading day.
        For MCX: session = 9:00 AM to 11:30 PM IST
        """
        # Use today's candles for session VWAP
        today    = df.index[-1].date()
        session  = df[df.index.date == today].copy()

        if session.empty or len(session) < 2:
            # Fall back to last 96 candles (~1 day of 15min data)
            session = df.tail(96).copy()

        # Typical price × volume cumulative
        typical_price = (session["high"] + session["low"] + session["close"]) / 3
        cum_tp_vol    = (typical_price * session["volume"]).cumsum()
        cum_vol       = session["volume"].cumsum()

        # Avoid division by zero
        cum_vol_safe = cum_vol.replace(0, np.nan)
        vwap_series  = cum_tp_vol / cum_vol_safe

        vwap = round(float(vwap_series.iloc[-1]), 2)

        # VWAP standard deviation bands
        # σ = sqrt(cumulative(TP² × vol) / cumulative(vol) - VWAP²)
        cum_tp2_vol = ((typical_price ** 2) * session["volume"]).cumsum()
        variance    = (cum_tp2_vol / cum_vol_safe) - (vwap_series ** 2)
        # Clip negative variance from floating point errors
        variance    = variance.clip(lower=0)
        std_dev     = np.sqrt(float(variance.iloc[-1]))

        upper_1 = round(vwap + std_dev, 2)
        lower_1 = round(vwap - std_dev, 2)
        upper_2 = round(vwap + 2 * std_dev, 2)
        lower_2 = round(vwap - 2 * std_dev, 2)

        # Position relative to VWAP
        pct_from_vwap = (latest_price - vwap) / vwap * 100 if vwap else 0
        if pct_from_vwap > 0.3:
            position = "above_vwap_premium"
        elif pct_from_vwap < -0.3:
            position = "below_vwap_discount"
        else:
            position = "at_vwap"

        return {
            "vwap":     vwap,
            "upper_1":  upper_1,
            "lower_1":  lower_1,
            "upper_2":  upper_2,
            "lower_2":  lower_2,
            "position": position,
        }

    def _compute_supertrend(
        self,
        df: pd.DataFrame,
        period: int = 10,
        multiplier: float = 3.0,
    ) -> dict:
        """
        Supertrend indicator — ATR-based trend following.
        Popular among Indian commodity traders.

        Returns: { level, direction, flipped }
        """
        high  = df["high"].values
        low   = df["low"].values
        close = df["close"].values
        n     = len(df)

        # Compute ATR for supertrend (separate from main ATR)
        atr_series = ta.volatility.AverageTrueRange(
            df["high"], df["low"], df["close"], window=period
        ).average_true_range().values

        # Basic bands
        hl2        = (high + low) / 2
        upper_band = hl2 + multiplier * atr_series
        lower_band = hl2 - multiplier * atr_series

        # Supertrend calculation
        supertrend = np.zeros(n)
        direction  = np.zeros(n)  # 1 = bullish, -1 = bearish

        supertrend[0] = upper_band[0]
        direction[0]  = -1

        for i in range(1, n):
            # Adjust bands based on previous values
            if lower_band[i] > lower_band[i - 1] or close[i - 1] < lower_band[i - 1]:
                pass  # keep current lower_band
            else:
                lower_band[i] = lower_band[i - 1]

            if upper_band[i] < upper_band[i - 1] or close[i - 1] > upper_band[i - 1]:
                pass
            else:
                upper_band[i] = upper_band[i - 1]

            # Direction logic
            if direction[i - 1] == 1:  # was bullish
                if close[i] < lower_band[i]:
                    direction[i]  = -1
                    supertrend[i] = upper_band[i]
                else:
                    direction[i]  = 1
                    supertrend[i] = lower_band[i]
            else:  # was bearish
                if close[i] > upper_band[i]:
                    direction[i]  = 1
                    supertrend[i] = lower_band[i]
                else:
                    direction[i]  = -1
                    supertrend[i] = upper_band[i]

        latest_dir  = "bullish" if direction[-1] == 1 else "bearish"
        flipped     = direction[-1] != direction[-2] if n >= 2 else False

        return {
            "level":     round(float(supertrend[-1]), 2),
            "direction": latest_dir,
            "flipped":   flipped,
        }

    def _detect_rsi_divergence(
        self,
        close: pd.Series,
        rsi: pd.Series,
        lookback: int = 30,
    ) -> str:
        """
        Detect bullish/bearish RSI divergence.

        Bullish divergence: price makes lower low, RSI makes higher low
        Bearish divergence: price makes higher high, RSI makes lower high

        Uses simple 5-bar pivot detection on the last `lookback` candles.
        """
        if len(close) < lookback or len(rsi) < lookback:
            return "none"

        close_arr = close.iloc[-lookback:].values
        rsi_arr   = rsi.iloc[-lookback:].values

        # Find local lows (for bullish divergence)
        lows = self._find_pivots(close_arr, kind="low", order=5)
        if len(lows) >= 2:
            # Last two price lows
            p1_idx, p2_idx = lows[-2], lows[-1]
            price_lower_low = close_arr[p2_idx] < close_arr[p1_idx]
            rsi_higher_low  = rsi_arr[p2_idx] > rsi_arr[p1_idx]
            if price_lower_low and rsi_higher_low:
                return "bullish"

        # Find local highs (for bearish divergence)
        highs = self._find_pivots(close_arr, kind="high", order=5)
        if len(highs) >= 2:
            p1_idx, p2_idx = highs[-2], highs[-1]
            price_higher_high = close_arr[p2_idx] > close_arr[p1_idx]
            rsi_lower_high    = rsi_arr[p2_idx] < rsi_arr[p1_idx]
            if price_higher_high and rsi_lower_high:
                return "bearish"

        return "none"

    def _find_pivots(
        self,
        data: np.ndarray,
        kind: str = "high",
        order: int = 5,
    ) -> list[int]:
        """
        Find pivot high/low indices in a 1D array.
        A pivot high at index i means data[i] is the max in
        [i-order, i+order]. Same logic inverted for lows.
        """
        pivots = []
        for i in range(order, len(data) - order):
            window = data[i - order : i + order + 1]
            if kind == "high" and data[i] == window.max():
                pivots.append(i)
            elif kind == "low" and data[i] == window.min():
                pivots.append(i)
        return pivots

    def _compute_fibonacci(self, df: pd.DataFrame) -> Optional[dict]:
        """
        Auto-compute Fibonacci retracement levels from the most
        recent significant swing high and swing low.

        Uses last 50 candles to find swings. Returns None if
        insufficient swing structure.
        """
        if len(df) < 20:
            return None

        lookback = min(50, len(df))
        recent   = df.tail(lookback)
        highs_arr = recent["high"].values
        lows_arr  = recent["low"].values
        close_arr = recent["close"].values

        # Find swing high and swing low using 5-bar pivots
        swing_high_idxs = self._find_pivots(highs_arr, kind="high", order=5)
        swing_low_idxs  = self._find_pivots(lows_arr, kind="low", order=5)

        if not swing_high_idxs or not swing_low_idxs:
            # Fallback: use lookback period high/low
            swing_high = float(recent["high"].max())
            swing_low  = float(recent["low"].min())
            high_idx   = int(recent["high"].values.argmax())
            low_idx    = int(recent["low"].values.argmin())
        else:
            # Use most recent swing points
            high_idx   = swing_high_idxs[-1]
            low_idx    = swing_low_idxs[-1]
            swing_high = float(highs_arr[high_idx])
            swing_low  = float(lows_arr[low_idx])

        if swing_high <= swing_low:
            return None

        diff = swing_high - swing_low

        # Determine trend direction — if swing high is more recent,
        # price dropped from high (retrace UP from low).
        # If swing low is more recent, price rose from low (retrace DOWN from high).
        if high_idx > low_idx:
            # Most recent point is the high → price went up
            # Retracement levels measured DOWN from high
            trend = "up"
            fib_levels = {
                "fib_236": round(swing_high - 0.236 * diff, 2),
                "fib_382": round(swing_high - 0.382 * diff, 2),
                "fib_500": round(swing_high - 0.500 * diff, 2),
                "fib_618": round(swing_high - 0.618 * diff, 2),
                "fib_786": round(swing_high - 0.786 * diff, 2),
            }
        else:
            # Most recent point is the low → price went down
            # Retracement levels measured UP from low
            trend = "down"
            fib_levels = {
                "fib_236": round(swing_low + 0.236 * diff, 2),
                "fib_382": round(swing_low + 0.382 * diff, 2),
                "fib_500": round(swing_low + 0.500 * diff, 2),
                "fib_618": round(swing_low + 0.618 * diff, 2),
                "fib_786": round(swing_low + 0.786 * diff, 2),
            }

        return {
            "swing_high": round(swing_high, 2),
            "swing_low":  round(swing_low, 2),
            "trend":      trend,
            **fib_levels,
        }

    def _compute_oi(self, df: pd.DataFrame, result: TechnicalData):
        """
        Compute Open Interest change and interpretation.
        OI data must be present in candle data (column 'oi').
        If Groww doesn't provide OI, this gracefully does nothing.
        """
        if "oi" not in df.columns:
            return

        # Filter out zero OI rows (means OI data not available)
        oi_data = df[df["oi"] > 0]
        if len(oi_data) < 2:
            return

        # Get today's and previous day's OI
        today = df.index[-1].date()
        today_oi = oi_data[oi_data.index.date == today]
        unique_dates = sorted(set(oi_data.index.date.tolist()))

        if len(unique_dates) < 2:
            return

        prev_date = unique_dates[-2]
        prev_oi   = oi_data[oi_data.index.date == prev_date]

        if today_oi.empty or prev_oi.empty:
            return

        current_oi  = int(today_oi["oi"].iloc[-1])
        prev_day_oi = int(prev_oi["oi"].iloc[-1])

        if prev_day_oi == 0:
            return

        oi_change_pct = round(
            (current_oi - prev_day_oi) / prev_day_oi * 100, 2
        )

        result.oi_current   = current_oi
        result.oi_prev_day  = prev_day_oi
        result.oi_change_pct = oi_change_pct

        # Price direction (today's last close vs previous day's last close)
        today_close = float(df.iloc[-1]["close"])
        prev_close  = float(prev_oi["close"].iloc[-1])
        price_up    = today_close > prev_close

        # OI-Price interpretation matrix
        oi_up = oi_change_pct > 1.0   # meaningful increase
        oi_dn = oi_change_pct < -1.0  # meaningful decrease

        if price_up and oi_up:
            result.oi_interpretation = "fresh_longs"         # strong bullish
        elif price_up and oi_dn:
            result.oi_interpretation = "short_covering"      # weak bullish
        elif not price_up and oi_up:
            result.oi_interpretation = "fresh_shorts"        # strong bearish
        elif not price_up and oi_dn:
            result.oi_interpretation = "long_unwinding"      # weak bearish
        else:
            result.oi_interpretation = "neutral"

        logger.info(
            f"OI: {current_oi:,} ({oi_change_pct:+.1f}%) "
            f"[{result.oi_interpretation}]"
        )
