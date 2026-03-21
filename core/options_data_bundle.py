"""
OPTIONEX — Options Data Bundle Assembler
Collects all data inputs and assembles the structured bundle
that gets passed to Agent 1 (Market & IV Analyst).

Single entry point for data collection.
All three agents receive this bundle as their primary input.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime

from core.options_engine import OptionsEngine, OptionsData
from config import (
    ACTIVE_INDICES,
    CONFIDENCE_CAP_NO_NEWS,
    CONFIDENCE_CAP_VIX_SPIKE,
    CONFIDENCE_CAP_EXPIRY_DAY,
    CONFIDENCE_CAP_IV_EXTREME,
    NSE_LOT_CONFIG,
    VIX_ELEVATED,
)

logger = logging.getLogger(__name__)


@dataclass
class OptionsDataBundle:
    """
    Complete market data package for one options signal request.
    Passed unchanged through all three agents.
    """
    # Request metadata
    index:            str
    timeframe:        str
    trading_style:    str
    timestamp:        str = field(
        default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

    # Underlying price
    spot_price:       Optional[float] = None
    spot_available:   bool = False
    futures_price:    Optional[float] = None

    # Technical indicators (on SPOT index)
    technicals:       Optional[object] = None   # TechnicalData from technical_engine
    technicals_ok:    bool = False

    # Options analytics
    options:          Optional[OptionsData] = None
    options_ok:       bool = False

    # News context
    news:             Optional[dict] = None
    news_available:   bool = False

    # India VIX
    india_vix:        Optional[float] = None
    india_vix_change: Optional[float] = None
    vix_signal:       Optional[str] = None

    # FII/DII data
    fii_oi_data:      Optional[dict] = None
    fii_signal:       Optional[str] = None

    # Confidence caps
    confidence_cap:   int = 100
    cap_reasons:      list = field(default_factory=list)

    # Lot config
    lot_config:       Optional[dict] = None

    # Data quality
    data_quality:     str = "full"

    def apply_confidence_caps(self):
        """Apply options-specific confidence caps based on data quality."""
        if not self.news_available:
            self._apply_cap(
                CONFIDENCE_CAP_NO_NEWS,
                f"News unavailable — capped at {CONFIDENCE_CAP_NO_NEWS}%"
            )

        if self.india_vix and self.india_vix > VIX_ELEVATED:
            self._apply_cap(
                CONFIDENCE_CAP_VIX_SPIKE,
                f"India VIX elevated at {self.india_vix:.1f} — "
                f"capped at {CONFIDENCE_CAP_VIX_SPIKE}%"
            )

        if self.options and self.options.dte_nearest <= 1:
            self._apply_cap(
                CONFIDENCE_CAP_EXPIRY_DAY,
                f"Expiry day ({self.options.dte_nearest} DTE) — "
                f"theta crush risk — capped at {CONFIDENCE_CAP_EXPIRY_DAY}%"
            )

        if self.options and self.options.iv_regime == "extreme":
            self._apply_cap(
                CONFIDENCE_CAP_IV_EXTREME,
                f"IV regime extreme (ATM IV={self.options.atm_iv:.1f}%) — "
                f"capped at {CONFIDENCE_CAP_IV_EXTREME}%"
            )

        # Data quality
        if self.technicals_ok and self.options_ok and self.news_available:
            self.data_quality = "full"
        elif self.technicals_ok and self.options_ok:
            self.data_quality = "partial"
        else:
            self.data_quality = "minimal"

    def _apply_cap(self, cap: int, reason: str):
        if self.confidence_cap > cap:
            self.confidence_cap = cap
            self.cap_reasons.append(reason)

    def to_prompt_string(self) -> str:
        """Build complete context string for Agent 1 prompt."""
        lines = [
            "=" * 60,
            "OPTIONS MARKET DATA BUNDLE",
            "=" * 60,
            f"Index     : {self.index}",
            f"Timeframe : {self.timeframe}",
            f"Style     : {self.trading_style}",
            f"Timestamp : {self.timestamp}",
            f"Data      : {self.data_quality}",
        ]

        if self.confidence_cap < 100:
            lines.append(f"⚠ Confidence cap: {self.confidence_cap}%")
            for r in self.cap_reasons:
                lines.append(f"  Reason: {r}")

        # Underlying
        lines.append("\n--- UNDERLYING ---")
        if self.spot_available:
            lines.append(f"Spot:    {self.spot_price:,.2f}")
            if self.futures_price:
                basis = self.futures_price - self.spot_price
                lines.append(
                    f"Futures: {self.futures_price:,.2f} "
                    f"(basis: {basis:+,.2f})"
                )
        else:
            lines.append("Spot: unavailable")

        # India VIX
        lines.append("\n--- INDIA VIX ---")
        if self.india_vix:
            change_str = f" ({self.india_vix_change:+.2f}%)" if self.india_vix_change else ""
            lines.append(
                f"VIX: {self.india_vix:.2f}{change_str} [{self.vix_signal or 'unknown'}]"
            )
        else:
            lines.append("VIX: unavailable")

        # Options Analytics
        lines.append("\n--- OPTIONS ANALYTICS ---")
        if self.options_ok and self.options:
            lines.append(self.options.summary_string())
        else:
            lines.append("Options data: unavailable")

        # Spot Technicals
        lines.append("\n--- SPOT TECHNICALS ---")
        if self.technicals_ok and self.technicals:
            lines.append(self.technicals.summary_string())
        else:
            lines.append("Spot technicals: unavailable")

        # FII/DII
        lines.append("\n--- INSTITUTIONAL FLOW ---")
        if self.fii_oi_data:
            lines.append(
                f"FII Index Futures: {self.fii_oi_data.get('futures_net', 'N/A')}"
            )
            lines.append(
                f"FII Index Options: {self.fii_oi_data.get('options_net', 'N/A')}"
            )
            lines.append(f"Signal: {self.fii_signal or 'N/A'}")
        else:
            lines.append("FII/DII data: unavailable")

        # News
        lines.append("\n--- NEWS CONTEXT ---")
        if self.news_available and self.news:
            lines.append(self.news.get("summary", "No summary"))
        else:
            lines.append("News: unavailable")

        lines.append("=" * 60)
        return "\n".join(lines)


class OptionsDataBundleAssembler:
    """
    Orchestrates data collection from all sources.
    Returns a fully populated OptionsDataBundle.
    Handles partial failures gracefully — never raises.
    """

    def __init__(
        self,
        groww_client,
        tech_engine,
        options_engine: OptionsEngine,
        news_client,
    ):
        self._groww    = groww_client
        self._tech     = tech_engine
        self._options  = options_engine
        self._news     = news_client

    def assemble(
        self,
        index:          str,
        timeframe:      str = "15minute",
        trading_style:  str = "system",
        days:           int = 30,
    ) -> OptionsDataBundle:
        """
        Assemble complete data bundle for an index.
        Each data source is fetched independently — one failure
        does not block others.
        """
        cfg = NSE_LOT_CONFIG.get(index)
        if not cfg:
            raise ValueError(f"Unknown index: {index}")

        bundle = OptionsDataBundle(
            index         = index,
            timeframe     = timeframe,
            trading_style = trading_style,
            lot_config    = cfg,
        )

        # ── 1. Spot Price ───────────────────────────────
        try:
            spot = self._groww.get_nse_spot(index)
            if spot:
                bundle.spot_price    = float(spot)
                bundle.spot_available = True
                logger.info(f"Spot: {bundle.spot_price:,.2f}")
        except Exception as e:
            logger.warning(f"Spot fetch failed: {e}")

        # ── 2. Futures Price ────────────────────────────
        try:
            futures_data = self._groww.get_nse_futures_price(index)
            if futures_data:
                bundle.futures_price = float(futures_data.get("ltp", 0))
        except Exception as e:
            logger.warning(f"Futures price fetch failed: {e}")

        # ── 3. India VIX ───────────────────────────────
        try:
            vix_data = self._groww.get_india_vix()
            if vix_data and vix_data.get("available"):
                bundle.india_vix        = vix_data["vix"]
                bundle.india_vix_change = vix_data.get("change_pct", 0)

                from config import VIX_LOW, VIX_NORMAL_HIGH, VIX_ELEVATED
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

        # ── 4. Option Chain + Options Analytics ────────
        try:
            chain_result = self._groww.get_option_chain(index)
            if chain_result:
                chain_data  = chain_result.get("chain", [])
                expiries    = chain_result.get("expiries", [])
                nearest_exp = chain_result.get("nearest_expiry", "")

                # Historical IV for rank/percentile
                hist_iv = self._get_historical_iv(index)

                # Historical closes for HV
                hist_closes = self._get_historical_closes(index)

                # Previous chain for OI shift
                prev_chain = self._get_previous_chain(index)

                lot_size = cfg.get("lot_size", 75)

                bundle.options = self._options.compute(
                    chain_data         = chain_data,
                    spot_price         = bundle.spot_price or cfg.get("typical_spot", 0),
                    futures_price      = bundle.futures_price or bundle.spot_price or 0,
                    index              = index,
                    nearest_expiry     = nearest_exp,
                    available_expiries = expiries,
                    historical_iv      = hist_iv,
                    historical_closes  = hist_closes,
                    previous_chain     = prev_chain,
                    lot_size           = lot_size,
                )
                bundle.options_ok = True

                # Save chain snapshot for future OI shift detection
                self._save_chain_snapshot(index, nearest_exp, chain_data)

                logger.info(
                    f"Options computed: IV={bundle.options.atm_iv:.1f}% "
                    f"rank={bundle.options.iv_rank:.0f} "
                    f"PCR={bundle.options.pcr_oi:.2f}"
                )
        except Exception as e:
            logger.warning(f"Options chain failed: {e}")

        # ── 5. Spot Technicals ─────────────────────────
        try:
            candles = self._groww.get_historical(
                index=index,
                interval=timeframe,
                days=days,
            )
            if candles:
                bundle.technicals    = self._tech.compute(
                    candles, index, timeframe
                )
                bundle.technicals_ok = True
        except Exception as e:
            logger.warning(f"Spot technicals failed: {e}")

        # ── 6. News ────────────────────────────────────
        try:
            news = self._news.fetch(index)
            bundle.news           = news
            bundle.news_available = news.get("available", False)
        except Exception as e:
            logger.warning(f"News fetch failed: {e}")

        # ── Apply confidence caps ──────────────────────
        bundle.apply_confidence_caps()

        logger.info(
            f"Bundle assembled: {index} | "
            f"quality={bundle.data_quality} | "
            f"cap={bundle.confidence_cap}%"
        )
        return bundle

    def _get_historical_iv(self, index: str) -> list:
        """Fetch historical IV from SQLite for IV rank computation."""
        try:
            from core.db import get_connection
            conn   = get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT atm_iv FROM iv_history
                WHERE index_name = ?
                ORDER BY date DESC
                LIMIT 252
            """, (index,))
            rows = cursor.fetchall()
            conn.close()
            return [float(r["atm_iv"]) for r in reversed(rows)]
        except Exception:
            return []

    def _get_historical_closes(self, index: str) -> list:
        """Fetch historical close prices for HV computation."""
        try:
            from core.db import get_connection
            conn   = get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT spot_close FROM iv_history
                WHERE index_name = ? AND spot_close IS NOT NULL
                ORDER BY date DESC
                LIMIT 252
            """, (index,))
            rows = cursor.fetchall()
            conn.close()
            return [float(r["spot_close"]) for r in reversed(rows)]
        except Exception:
            return []

    def _get_previous_chain(self, index: str) -> list:
        """Get most recent previous chain snapshot for OI shift detection."""
        try:
            from core.db import get_connection
            import json
            conn   = get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT chain_json FROM chain_snapshots
                WHERE index_name = ?
                ORDER BY snapshot_time DESC
                LIMIT 1
            """, (index,))
            row = cursor.fetchone()
            conn.close()
            if row:
                return json.loads(row["chain_json"])
        except Exception:
            pass
        return []

    def _save_chain_snapshot(self, index: str, expiry: str, chain: list):
        """Persist chain snapshot for future comparison."""
        try:
            from core.db import get_connection
            import json
            conn   = get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO chain_snapshots
                (index_name, expiry, snapshot_time, chain_json)
                VALUES (?, ?, ?, ?)
            """, (
                index, expiry,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                json.dumps(chain),
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning(f"Chain snapshot save failed: {e}")
