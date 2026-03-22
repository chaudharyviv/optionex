"""
SWINGTRADE — Swing Data Bundle
Collects all data inputs for a single cash-segment stock analysis.
Mirrors OptionsDataBundle pattern exactly — same field layout,
same graceful partial-failure handling, same to_prompt_string() shape.

One bundle per symbol per analysis run.
All three swing agents receive this bundle unchanged.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime

from config import (
    SWING_CONFIDENCE_CAP_VIX_HIGH,
    SWING_CONFIDENCE_CAP_NO_NEWS,
    SWING_CONFIDENCE_CAP_RESULTS,
    SWING_VIX_MAX_LONGS,
    SWING_HARD_FILTERS,
    VIX_LOW, VIX_NORMAL_HIGH, VIX_ELEVATED,
)

logger = logging.getLogger(__name__)


@dataclass
class SwingDataBundle:
    """
    Complete data package for one swing trade analysis.
    Passed unchanged through all three swing agents.

    Key difference from OptionsDataBundle:
      - symbol is a cash stock (e.g. "RELIANCE"), not an index
      - no options chain data
      - two timeframes: daily (primary) and weekly (trend anchor)
      - fundamentals block for hard-filter context
      - sector field for concentration checks
    """

    # ── Request metadata ───────────────────────────────────────────
    symbol:           str
    exchange:         str = "NSE"          # NSE | BSE
    timestamp:        str = field(
        default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

    # ── Price ──────────────────────────────────────────────────────
    spot_price:       Optional[float] = None    # latest LTP / close
    spot_available:   bool = False

    # ── Technical Data (from TechnicalEngine) ─────────────────────
    # Daily candles — primary signal timeframe
    technicals_daily:    Optional[object] = None   # TechnicalData
    technicals_daily_ok: bool = False

    # Weekly candles — trend anchor (EMA200, higher-degree trend)
    technicals_weekly:    Optional[object] = None  # TechnicalData
    technicals_weekly_ok: bool = False

    # ── Market Context ─────────────────────────────────────────────
    india_vix:        Optional[float] = None
    india_vix_change: Optional[float] = None
    vix_signal:       Optional[str]   = None    # low | normal | elevated | spike

    # Nifty 50 context — what is the broader market doing?
    nifty_trend:      Optional[str] = None      # "bullish" | "bearish" | "ranging"
    nifty_ema_signal: Optional[str] = None      # "above_ema20" | "below_ema20"

    # ── Fundamentals (from Screener.in / scraper) ──────────────────
    market_cap_cr:    Optional[float] = None
    sector:           Optional[str]   = None
    promoter_holding: Optional[float] = None
    promoter_pledge:  Optional[float] = None    # % pledged
    debt_equity:      Optional[float] = None
    pe_ratio:         Optional[float] = None
    results_date:     Optional[str]   = None    # "2026-04-15" or None
    days_to_results:  Optional[int]   = None    # computed from results_date

    # ── Hard Filter Outcome ────────────────────────────────────────
    passes_hard_filters:  bool = True
    filter_fail_reason:   Optional[str] = None

    # ── News ───────────────────────────────────────────────────────
    news:             Optional[dict] = None
    news_available:   bool = False

    # ── Confidence Caps ────────────────────────────────────────────
    confidence_cap:   int  = 100
    cap_reasons:      list = field(default_factory=list)

    # ── Data Quality ───────────────────────────────────────────────
    # "full" = daily TA + weekly TA + fundamentals + news
    # "partial" = daily TA + weekly TA (no fundamentals or news)
    # "minimal" = daily TA only
    data_quality:     str  = "minimal"

    def apply_confidence_caps(self):
        """Apply swing-specific confidence caps. Mirrors OptionsDataBundle.apply_confidence_caps()."""

        # VIX gate
        if self.india_vix and self.india_vix > SWING_VIX_MAX_LONGS:
            self._apply_cap(
                SWING_CONFIDENCE_CAP_VIX_HIGH,
                f"India VIX elevated at {self.india_vix:.1f} — "
                f"capped at {SWING_CONFIDENCE_CAP_VIX_HIGH}%"
            )

        # News unavailable
        if not self.news_available:
            self._apply_cap(
                SWING_CONFIDENCE_CAP_NO_NEWS,
                f"News unavailable — capped at {SWING_CONFIDENCE_CAP_NO_NEWS}%"
            )

        # Results blackout
        blackout = SWING_HARD_FILTERS.get("results_blackout_days", 10)
        if self.days_to_results is not None and self.days_to_results <= blackout:
            self._apply_cap(
                SWING_CONFIDENCE_CAP_RESULTS,
                f"Results in {self.days_to_results} days — "
                f"capped at {SWING_CONFIDENCE_CAP_RESULTS}%"
            )

        # Data quality derivation
        daily_ok  = self.technicals_daily_ok
        weekly_ok = self.technicals_weekly_ok
        has_fund  = self.market_cap_cr is not None
        has_news  = self.news_available

        if daily_ok and weekly_ok and has_fund and has_news:
            self.data_quality = "full"
        elif daily_ok and weekly_ok:
            self.data_quality = "partial"
        elif daily_ok:
            self.data_quality = "minimal"
        else:
            self.data_quality = "insufficient"

    def _apply_cap(self, cap: int, reason: str):
        """Apply a confidence cap if it's lower than current cap."""
        if self.confidence_cap > cap:
            self.confidence_cap = cap
            self.cap_reasons.append(reason)

    def to_prompt_string(self) -> str:
        """
        Build complete context string for Agent 1 (SwingAnalystAgent) prompt.
        Mirrors OptionsDataBundle.to_prompt_string() structure.
        """
        lines = [
            "=" * 60,
            "SWING TRADE DATA BUNDLE — CASH SEGMENT",
            "=" * 60,
            f"Symbol    : {self.symbol} ({self.exchange})",
            f"Timestamp : {self.timestamp}",
            f"Data      : {self.data_quality}",
        ]

        if not self.passes_hard_filters:
            lines.append(f"⛔ HARD FILTER FAILED: {self.filter_fail_reason}")
            lines.append("=" * 60)
            return "\n".join(lines)

        if self.confidence_cap < 100:
            lines.append(f"⚠ Confidence cap: {self.confidence_cap}%")
            for r in self.cap_reasons:
                lines.append(f"  Reason: {r}")

        # ── Price ────────────────────────────────────────────
        lines.append("\n--- PRICE ---")
        if self.spot_available and self.spot_price:
            lines.append(f"LTP / Close: ₹{self.spot_price:,.2f}")
        else:
            lines.append("Price: unavailable")

        # ── Market context ───────────────────────────────────
        lines.append("\n--- MARKET CONTEXT ---")
        if self.india_vix:
            change_str = f" ({self.india_vix_change:+.2f}%)" if self.india_vix_change else ""
            lines.append(
                f"India VIX: {self.india_vix:.2f}{change_str} [{self.vix_signal or 'unknown'}]"
            )
        else:
            lines.append("India VIX: unavailable")

        if self.nifty_trend:
            lines.append(f"Nifty 50:  {self.nifty_trend} | {self.nifty_ema_signal or 'N/A'}")

        # ── Daily Technicals ─────────────────────────────────
        lines.append("\n--- DAILY TECHNICALS (primary timeframe) ---")
        if self.technicals_daily_ok and self.technicals_daily:
            lines.append(self.technicals_daily.summary_string())
        else:
            lines.append("Daily technicals: unavailable")

        # ── Weekly Technicals ────────────────────────────────
        lines.append("\n--- WEEKLY TECHNICALS (trend anchor) ---")
        if self.technicals_weekly_ok and self.technicals_weekly:
            lines.append(self.technicals_weekly.summary_string())
        else:
            lines.append("Weekly technicals: unavailable")

        # ── Fundamentals ─────────────────────────────────────
        lines.append("\n--- FUNDAMENTALS ---")
        lines.append(f"Sector:           {self.sector or 'Unknown'}")
        lines.append(f"Market Cap:       ₹{self.market_cap_cr:,.0f} Cr" if self.market_cap_cr else "Market Cap:       N/A")
        lines.append(f"Promoter Holding: {self.promoter_holding:.1f}%" if self.promoter_holding else "Promoter Holding: N/A")
        lines.append(f"Promoter Pledge:  {self.promoter_pledge:.1f}%" if self.promoter_pledge is not None else "Promoter Pledge:  N/A")
        lines.append(f"Debt/Equity:      {self.debt_equity:.2f}" if self.debt_equity is not None else "Debt/Equity:      N/A")
        lines.append(f"P/E Ratio:        {self.pe_ratio:.1f}" if self.pe_ratio else "P/E:              N/A")

        if self.days_to_results is not None:
            lines.append(f"Results in:       {self.days_to_results} trading days ({self.results_date})")
        else:
            lines.append("Results date:     Not imminent")

        # ── News ─────────────────────────────────────────────
        lines.append("\n--- NEWS CONTEXT ---")
        if self.news_available and self.news:
            lines.append(self.news.get("summary", "No summary"))
        else:
            lines.append("News: unavailable")

        lines.append("=" * 60)
        return "\n".join(lines)


class SwingDataBundleAssembler:
    """
    Orchestrates data collection for a single cash-segment stock.
    Returns a fully populated SwingDataBundle.
    Each source fetched independently — partial failures are handled gracefully.

    Reuses existing infrastructure:
      - GrowwClient  → price, OHLCV (SEGMENT_CASH)
      - TechnicalEngine → daily and weekly TA
      - NewsClient   → stock-specific news via Tavily
    """

    def __init__(self, groww_client, tech_engine, news_client=None):
        self._groww = groww_client
        self._tech  = tech_engine
        self._news  = news_client    # optional — same NewsClient as OPTIONEX

    def assemble(
        self,
        symbol:   str,
        exchange: str = "NSE",
    ) -> SwingDataBundle:
        """
        Assemble a complete SwingDataBundle for the given cash stock symbol.

        Fetch sequence:
          1. LTP (spot price)
          2. Daily OHLCV → TechnicalEngine
          3. Weekly OHLCV → TechnicalEngine (weekly anchor)
          4. India VIX (shared with OPTIONEX — reuse GrowwClient)
          5. Nifty 50 trend context
          6. Fundamentals (screener scraper)
          7. News
          8. Apply confidence caps
        """
        bundle = SwingDataBundle(symbol=symbol, exchange=exchange)

        # ── 1. Spot Price ────────────────────────────────────
        try:
            # For cash stocks, use SEGMENT_CASH directly
            result = self._groww._groww.get_ltp(
                segment=self._groww._groww.SEGMENT_CASH,
                exchange_trading_symbols=f"{exchange}_{symbol}",
            )
            if result:
                ltp = float(list(result.values())[0])
                if ltp > 0:
                    bundle.spot_price    = ltp
                    bundle.spot_available = True
                    logger.info(f"LTP {symbol}: ₹{ltp:,.2f}")
        except Exception as e:
            logger.warning(f"LTP fetch failed for {symbol}: {e}")

        # ── 2. Daily Technicals ──────────────────────────────
        try:
            # GrowwClient.get_historical() exists but is FNO-based.
            # For cash stocks, call get_historical_candle_data directly.
            from datetime import timedelta
            from datetime import datetime as dt

            end_dt   = dt.today()
            start_dt = end_dt - timedelta(days=120)   # 120 days → ~90 trading days

            raw = self._groww._groww.get_historical_candle_data(
                trading_symbol    = symbol,
                exchange          = self._groww._groww.EXCHANGE_NSE,
                segment           = self._groww._groww.SEGMENT_CASH,
                start_time        = start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                end_time          = end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                interval_in_minutes = 1440,   # 1-day candles
            )
            candles = self._parse_candles(raw)
            if candles and len(candles) >= 20:
                bundle.technicals_daily    = self._tech.compute(candles, symbol, "1day")
                bundle.technicals_daily_ok = True
                # Set spot price from last close if LTP unavailable
                if not bundle.spot_available and bundle.technicals_daily:
                    bundle.spot_price    = bundle.technicals_daily.latest_price
                    bundle.spot_available = True
                logger.info(f"Daily TA computed for {symbol}: {len(candles)} candles")
        except Exception as e:
            logger.warning(f"Daily TA failed for {symbol}: {e}")

        # ── 3. Weekly Technicals ─────────────────────────────
        try:
            from datetime import timedelta
            from datetime import datetime as dt

            end_dt   = dt.today()
            start_dt = end_dt - timedelta(days=520)   # ~2 years of weekly data

            raw = self._groww._groww.get_historical_candle_data(
                trading_symbol    = symbol,
                exchange          = self._groww._groww.EXCHANGE_NSE,
                segment           = self._groww._groww.SEGMENT_CASH,
                start_time        = start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                end_time          = end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                interval_in_minutes = 10080,   # 7 days in minutes
            )
            weekly_candles = self._parse_candles(raw)
            if weekly_candles and len(weekly_candles) >= 10:
                bundle.technicals_weekly    = self._tech.compute(
                    weekly_candles, symbol, "1week"
                )
                bundle.technicals_weekly_ok = True
                logger.info(f"Weekly TA computed for {symbol}: {len(weekly_candles)} candles")
        except Exception as e:
            logger.warning(f"Weekly TA failed for {symbol}: {e}")

        # ── 4. India VIX ─────────────────────────────────────
        try:
            vix_data = self._groww.get_india_vix()
            if vix_data and vix_data.get("available"):
                bundle.india_vix        = vix_data["vix"]
                bundle.india_vix_change = vix_data.get("change_pct", 0)
                vix = bundle.india_vix
                if vix < VIX_LOW:
                    bundle.vix_signal = "low"
                elif vix < VIX_NORMAL_HIGH:
                    bundle.vix_signal = "normal"
                elif vix < VIX_ELEVATED:
                    bundle.vix_signal = "elevated"
                else:
                    bundle.vix_signal = "spike"
        except Exception as e:
            logger.warning(f"VIX fetch failed: {e}")

        # ── 5. Nifty 50 trend context ─────────────────────────
        try:
            nifty_candles_raw = self._groww._groww.get_historical_candle_data(
                trading_symbol    = "NIFTY 50",
                exchange          = self._groww._groww.EXCHANGE_NSE,
                segment           = self._groww._groww.SEGMENT_CASH,
                start_time        = (datetime.today() - __import__('datetime').timedelta(days=60)).strftime("%Y-%m-%d %H:%M:%S"),
                end_time          = datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                interval_in_minutes = 1440,
            )
            nifty_candles = self._parse_candles(nifty_candles_raw)
            if nifty_candles and len(nifty_candles) >= 20:
                nifty_tech = self._tech.compute(nifty_candles, "NIFTY", "1day")
                bundle.nifty_trend      = nifty_tech.ema_trend or "neutral"
                bundle.nifty_ema_signal = nifty_tech.ema_200_trend or "unknown"
        except Exception as e:
            logger.warning(f"Nifty context fetch failed: {e}")

        # ── 6. Fundamentals ───────────────────────────────────
        try:
            fund = self._fetch_fundamentals(symbol)
            if fund:
                bundle.market_cap_cr    = fund.get("market_cap_cr")
                bundle.sector           = fund.get("sector")
                bundle.promoter_holding = fund.get("promoter_holding")
                bundle.promoter_pledge  = fund.get("promoter_pledge", 0.0)
                bundle.debt_equity      = fund.get("debt_equity")
                bundle.pe_ratio         = fund.get("pe_ratio")
                bundle.results_date     = fund.get("results_date")
                if bundle.results_date:
                    from datetime import date
                    try:
                        rd = date.fromisoformat(bundle.results_date)
                        bundle.days_to_results = (rd - date.today()).days
                    except ValueError:
                        pass
        except Exception as e:
            logger.warning(f"Fundamentals fetch failed for {symbol}: {e}")

        # ── 7. Hard Filter Check ──────────────────────────────
        bundle.passes_hard_filters, bundle.filter_fail_reason = (
            self._run_hard_filters(bundle)
        )

        # ── 8. News ───────────────────────────────────────────
        if self._news and bundle.passes_hard_filters:
            try:
                news = self._news.fetch(symbol)
                bundle.news           = news
                bundle.news_available = news.get("available", False)
            except Exception as e:
                logger.warning(f"News fetch failed for {symbol}: {e}")

        # ── 9. Confidence caps ────────────────────────────────
        bundle.apply_confidence_caps()

        logger.info(
            f"SwingBundle assembled: {symbol} | "
            f"quality={bundle.data_quality} | "
            f"cap={bundle.confidence_cap}% | "
            f"filters={'PASS' if bundle.passes_hard_filters else 'FAIL'}"
        )
        return bundle

    def _parse_candles(self, raw) -> list[dict]:
        """Normalise various Groww SDK response shapes to list[dict]."""
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

    def _fetch_fundamentals(self, symbol: str) -> dict:
        """
        Fetch fundamentals from Screener.in.
        Returns dict with market_cap_cr, sector, promoter fields.
        Falls back gracefully — all fields optional.

        Implementation note: Screener.in has no official API.
        Use requests + BeautifulSoup to scrape the stock page.
        URL pattern: https://www.screener.in/company/{SYMBOL}/
        """
        try:
            import requests
            from bs4 import BeautifulSoup

            url  = f"https://www.screener.in/company/{symbol}/"
            hdrs = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36"
                )
            }
            resp = requests.get(url, headers=hdrs, timeout=10)
            if resp.status_code != 200:
                return {}

            soup = BeautifulSoup(resp.text, "html.parser")
            result = {}

            # Market cap
            for li in soup.select("li.flex.flex-space-between"):
                label_el = li.select_one("span.name")
                value_el = li.select_one("span.number, span.value")
                if not label_el or not value_el:
                    continue
                label = label_el.get_text(strip=True).lower()
                value = value_el.get_text(strip=True).replace(",", "").replace("₹", "")
                if "market cap" in label:
                    try:
                        result["market_cap_cr"] = float(value)
                    except ValueError:
                        pass
                elif "p/e" in label:
                    try:
                        result["pe_ratio"] = float(value)
                    except ValueError:
                        pass
                elif "debt / equity" in label or "d/e" in label:
                    try:
                        result["debt_equity"] = float(value)
                    except ValueError:
                        pass

            # Promoter holding from shareholding section
            for row in soup.select("table.data-table tbody tr"):
                cells = row.find_all("td")
                if not cells:
                    continue
                label = cells[0].get_text(strip=True).lower()
                if "promoter" in label and len(cells) > 1:
                    try:
                        result["promoter_holding"] = float(
                            cells[-1].get_text(strip=True).replace("%", "")
                        )
                    except ValueError:
                        pass

            return result

        except ImportError:
            logger.warning("BeautifulSoup not installed — fundamentals unavailable. pip install beautifulsoup4")
            return {}
        except Exception as e:
            logger.warning(f"Screener fetch failed for {symbol}: {e}")
            return {}

    def _run_hard_filters(self, bundle: SwingDataBundle) -> tuple[bool, Optional[str]]:
        """
        Run all hard filters deterministically.
        Returns (passes: bool, fail_reason: str|None).
        These are non-negotiable — cannot be overridden by LLM.
        """
        from config import SWING_HARD_FILTERS as HF

        # Price check
        if bundle.spot_price and bundle.spot_price < HF["min_price_inr"]:
            return False, f"Price ₹{bundle.spot_price:.2f} < ₹{HF['min_price_inr']} min"

        # Market cap check
        if bundle.market_cap_cr and bundle.market_cap_cr < HF["min_market_cap_cr"]:
            return False, f"Market cap ₹{bundle.market_cap_cr:.0f} Cr < ₹{HF['min_market_cap_cr']} Cr min"

        # Promoter pledge check
        if bundle.promoter_pledge and bundle.promoter_pledge > HF["max_promoter_pledge_pct"]:
            return False, f"Promoter pledge {bundle.promoter_pledge:.1f}% > {HF['max_promoter_pledge_pct']}% limit"

        # Volume check (from daily TA)
        if bundle.technicals_daily_ok and bundle.technicals_daily:
            avg_vol = bundle.technicals_daily.volume_avg_20
            if avg_vol and avg_vol < HF["min_avg_daily_volume"]:
                return False, (
                    f"Avg daily volume {avg_vol:,.0f} < "
                    f"{HF['min_avg_daily_volume']:,} min"
                )

        return True, None
