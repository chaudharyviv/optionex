"""
SWINGTRADE — Swing Screener
Deterministic universe → shortlist filter. Zero LLM calls.
Runs before SwingSignalOrchestrator to shortlist candidates.

Input:  list of symbols (Nifty 500 or user watchlist)
Output: list of symbols that pass hard filters + ≥2 soft screens

The orchestrator then runs a full 3-agent analysis on each shortlisted symbol.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

from config import SWING_HARD_FILTERS, SWING_MAX_SCREENED

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────
# NIFTY 500 UNIVERSE (subset — top liquid names across sectors)
# Replace / extend from NSE website or your watchlist
# ─────────────────────────────────────────────────────────────────

NIFTY_500_SAMPLE = [
    # Large cap — high liquidity
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "HINDUNILVR",
    "ICICIBANK", "KOTAKBANK", "AXISBANK", "SBIN", "BAJFINANCE",
    "BHARTIARTL", "LT", "ASIANPAINT", "MARUTI", "SUNPHARMA",
    "TITAN", "NESTLEIND", "TECHM", "WIPRO", "HCLTECH",
    # Mid cap — good swing candidates
    "TATAMOTORS", "TATAPOWER", "TATACONSUM", "PERSISTENT", "MPHASIS",
    "COFORGE", "LTIM", "ZOMATO", "NYKAA", "PAYTM",
    "PIDILITIND", "BERGEPAINT", "MARICO", "GODREJCP", "COLPAL",
    "APOLLOHOSP", "MAXHEALTH", "FORTIS", "METROPOLIS", "LALPATHLAB",
    "IRCTC", "IRFC", "PFC", "RECLTD", "HUDCO",
    "CUMMINSIND", "THERMAX", "GRINDWELL", "SIEMENS", "ABB",
    "JSWSTEEL", "TATASTEEL", "HINDALCO", "VEDL", "NMDC",
    "DMART", "TRENT", "ABFRL", "PAGEIND", "MANYAVAR",
]


@dataclass
class ScreenResult:
    """Result from screening a single symbol."""
    symbol:           str
    passed:           bool
    soft_hits:        list = field(default_factory=list)   # screens that fired
    soft_hit_count:   int  = 0
    fail_reasons:     list = field(default_factory=list)   # why hard-filtered out
    spot_price:       Optional[float] = None
    avg_volume:       Optional[float] = None
    rsi_14:           Optional[float] = None
    ema_trend:        Optional[str]   = None
    supertrend_dir:   Optional[str]   = None


class SwingScreener:
    """
    Pre-filters a universe of cash-segment symbols.
    Uses lightweight data already available from GrowwClient + TechnicalEngine.
    All checks are deterministic — no LLM.

    Two-layer filter:
      Layer A (mandatory gate — ALL must pass):
        • price above EMA 200
        • EMA 20 above EMA 50
        • ADX > 25

      Layer B (timing events — must hit ≥ MIN_SOFT_HITS):
        near_52w_high_252, volume_surge_1_5x, rsi_crossed_above_50,
        supertrend_flip, bb_breakout, macd_bullish_cross

    Typical workflow:
        screener = SwingScreener(groww_client, tech_engine)
        shortlist = screener.screen(universe=NIFTY_500_SAMPLE)
        # → ["TATAMOTORS", "BHARTIARTL", ...]
        for symbol in shortlist:
            result = swing_orchestrator.generate(symbol)
    """

    MIN_SOFT_HITS = 3   # symbol must pass at least 3 Layer B timing screens

    def __init__(self, groww_client, tech_engine):
        self._groww = groww_client
        self._tech  = tech_engine

    def screen(
        self,
        universe:      list[str] = None,
        max_results:   int = None,
        exchange:      str = "NSE",
    ) -> list[str]:
        """
        Screen universe and return shortlist of symbols.
        Returns symbols only — detailed ScreenResult available via screen_with_details().
        """
        results = self.screen_with_details(
            universe=universe,
            max_results=max_results,
            exchange=exchange,
        )
        return [r.symbol for r in results if r.passed]

    def screen_with_details(
        self,
        universe:    list[str] = None,
        max_results: int = None,
        exchange:    str = "NSE",
    ) -> list[ScreenResult]:
        """
        Screen and return full ScreenResult objects, sorted by soft hit count.
        """
        universe    = universe    or NIFTY_500_SAMPLE
        max_results = max_results or SWING_MAX_SCREENED

        logger.info(f"Screening {len(universe)} symbols — max {max_results} results")

        candidates = []
        for symbol in universe:
            try:
                result = self._screen_one(symbol, exchange)
                if result.passed:
                    candidates.append(result)
                else:
                    logger.debug(
                        f"[SKIP] {symbol}: "
                        f"{result.fail_reasons or f'{result.soft_hit_count} soft hits'}"
                    )
            except Exception as e:
                logger.warning(f"Screen failed for {symbol}: {e}")

        # Sort by soft hit count descending (strongest setups first)
        candidates.sort(key=lambda r: r.soft_hit_count, reverse=True)

        shortlist = candidates[:max_results]
        logger.info(
            f"Screener complete: {len(shortlist)}/{len(universe)} passed | "
            f"top: {[r.symbol for r in shortlist[:5]]}"
        )
        return shortlist

    def _screen_one(self, symbol: str, exchange: str) -> ScreenResult:
        """Run all screens for a single symbol."""
        result = ScreenResult(symbol=symbol, passed=False)
        hf     = SWING_HARD_FILTERS

        # ── Fetch lightweight daily data ─────────────────────
        try:
            from datetime import datetime, timedelta
            end_dt   = datetime.today()
            start_dt = end_dt - timedelta(days=400)   # ~252 trading days + buffer

            raw = self._groww._groww.get_historical_candle_data(
                trading_symbol      = symbol,
                exchange            = self._groww._groww.EXCHANGE_NSE,
                segment             = self._groww._groww.SEGMENT_CASH,
                start_time          = start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                end_time            = end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                interval_in_minutes = 1440,
            )
            candles = self._parse_candles(raw)
        except Exception as e:
            result.fail_reasons.append(f"Data fetch failed: {e}")
            return result

        if not candles or len(candles) < 20:
            result.fail_reasons.append("Insufficient candle history")
            return result

        # ── Compute TA ────────────────────────────────────────
        try:
            tech = self._tech.compute(candles, symbol, "1day")
        except Exception as e:
            result.fail_reasons.append(f"TA compute failed: {e}")
            return result

        result.spot_price   = tech.latest_price
        result.avg_volume   = tech.volume_avg_20
        result.rsi_14       = tech.rsi_14
        result.ema_trend    = tech.ema_trend
        result.supertrend_dir = tech.supertrend_dir

        # ── Hard Filter 1: Minimum price ──────────────────────
        if tech.latest_price < hf["min_price_inr"]:
            result.fail_reasons.append(f"Price ₹{tech.latest_price:.2f} < ₹{hf['min_price_inr']}")
            return result

        # ── Hard Filter 2: Minimum volume ─────────────────────
        if tech.volume_avg_20 and tech.volume_avg_20 < hf["min_avg_daily_volume"]:
            result.fail_reasons.append(
                f"Vol {tech.volume_avg_20:,.0f} < {hf['min_avg_daily_volume']:,}"
            )
            return result

        # ── Layer A Gate (mandatory — ALL must pass) ──────────
        layer_a_fails = []

        # A1. Price above EMA 200 (long-term trend anchor)
        if tech.ema_200 and tech.latest_price < tech.ema_200:
            layer_a_fails.append(f"below_ema200 (price={tech.latest_price:.2f} ema200={tech.ema_200:.2f})")

        # A2. EMA 20 above EMA 50 (short-term trend bullish)
        if not (tech.ema_trend and "bullish" in tech.ema_trend.lower()):
            layer_a_fails.append("ema20_not_above_ema50")

        # A3. ADX > 25 (trending, not ranging)
        if not (tech.adx_14 and tech.adx_14 > 25):
            layer_a_fails.append(f"adx_below_25 (adx={tech.adx_14})")

        if layer_a_fails:
            result.fail_reasons.extend(layer_a_fails)
            return result

        # ── Layer B Soft Screens (timing events — need ≥ MIN_SOFT_HITS) ──
        soft_hits = []

        # 1. Near 52-week high — within 5% of actual 252-candle max
        candle_highs = [c["high"] for c in candles if c.get("high")]
        high_252 = max(candle_highs[-252:]) if len(candle_highs) >= 252 else max(candle_highs)
        if tech.latest_price >= high_252 * 0.95:
            soft_hits.append("near_52w_high_252")

        # 2. Volume surge (today > 1.5× 20-day avg)
        if (
            tech.volume_current and tech.volume_avg_20
            and tech.volume_current > tech.volume_avg_20 * 1.5
        ):
            soft_hits.append("volume_surge_1_5x")

        # 3. RSI crossed above 50 (from below — recent momentum trigger)
        try:
            import pandas as pd
            closes = pd.Series([c["close"] for c in candles], dtype=float)
            delta  = closes.diff()
            gain   = delta.clip(lower=0).ewm(com=13, adjust=False).mean()
            loss   = (-delta.clip(upper=0)).ewm(com=13, adjust=False).mean()
            rsi_s  = 100 - (100 / (1 + gain / loss))
            if len(rsi_s) >= 2 and rsi_s.iloc[-2] < 50 <= rsi_s.iloc[-1]:
                soft_hits.append("rsi_crossed_above_50")
        except Exception:
            pass

        # 4. Supertrend flipped bullish on latest candle
        if tech.supertrend_flip is True:
            soft_hits.append("supertrend_flip")

        # 5. BB breakout (squeeze released, price above upper band)
        if tech.bb_squeeze is False and tech.bb_position == "above":
            soft_hits.append("bb_breakout")

        # 6. MACD bullish cross
        if tech.macd_cross and "bullish" in tech.macd_cross.lower():
            soft_hits.append("macd_bullish_cross")

        result.soft_hits      = soft_hits
        result.soft_hit_count = len(soft_hits)
        result.passed         = result.soft_hit_count >= self.MIN_SOFT_HITS

        if result.passed:
            logger.info(
                f"[PASS] {symbol}: {result.soft_hit_count} hits "
                f"({', '.join(soft_hits)}) | "
                f"₹{tech.latest_price:,.2f} | RSI={tech.rsi_14}"
            )

        return result

    def _parse_candles(self, raw) -> list[dict]:
        """Normalise Groww SDK response to list[dict]."""
        import pandas as pd
        if isinstance(raw, pd.DataFrame):
            raw = raw.to_dict("records")
        elif isinstance(raw, dict):
            raw = raw.get("candles", raw.get("data", []))
        elif not isinstance(raw, list):
            raw = []

        candles = []
        for c in raw:
            if isinstance(c, (list, tuple)) and len(c) >= 5:
                candles.append({
                    "timestamp": c[0], "open": c[1], "high": c[2],
                    "low": c[3], "close": c[4],
                    "volume": c[5] if len(c) > 5 else 0,
                })
            elif isinstance(c, dict):
                candles.append(c)
        return candles
