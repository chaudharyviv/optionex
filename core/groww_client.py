"""
OPTIONEX — Groww API Client (SDK-based)
Extended from COMMODEX for NSE F&O operations.

Uses official growwapi Python SDK.
Auth: pre-generated access token via GrowwAPI.get_access_token()

Covers:
  - NSE spot price (NIFTY / BANKNIFTY)
  - Near-month index futures price
  - Full option chain with OI and IV
  - India VIX
  - Historical candles (spot index)
  - NFO order placement (production only)
"""

import os
import logging
import pandas as pd
import pyotp
from datetime import datetime, timedelta
from typing import Optional
from growwapi import GrowwAPI

from config import (
    GROWW_API_KEY,
    GROWW_TOTP_SECRET,
    NSE_LOT_CONFIG,
    ACTIVE_INDICES,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# TOKEN GENERATION
# ─────────────────────────────────────────────────────────────────

def generate_access_token(
    api_key: str,
    secret: str = None,
    totp_secret: str = None,
) -> str:
    """Generate Groww access token. Store in .env as GROWW_ACCESS_TOKEN."""
    if secret:
        token = GrowwAPI.get_access_token(api_key=api_key, secret=secret)
    elif totp_secret:
        totp = pyotp.TOTP(totp_secret).now()
        token = GrowwAPI.get_access_token(api_key=api_key, totp=totp)
    else:
        raise ValueError("Provide either secret or totp_secret")
    logger.info("Access token generated successfully")
    return token


# ─────────────────────────────────────────────────────────────────
# MAIN CLIENT
# ─────────────────────────────────────────────────────────────────

class GrowwClient:
    """
    OPTIONEX wrapper around the official growwapi SDK.
    Focused on NSE F&O operations for index options pipeline.
    """

    def __init__(self, access_token: str = None):
        token = access_token or os.getenv("GROWW_ACCESS_TOKEN")
        if not token:
            raise ValueError(
                "No Groww access token. "
                "Set GROWW_ACCESS_TOKEN in .env or pass access_token."
            )
        self._groww = GrowwAPI(token)
        self._instruments_df: Optional[pd.DataFrame] = None
        logger.info("GrowwClient initialised (NSE F&O mode)")

    # ── Instruments ──────────────────────────────────────────────

    def get_instruments_df(self, force_refresh: bool = False) -> pd.DataFrame:
        """Load full instruments DataFrame from Groww CSV. Cached in memory."""
        if self._instruments_df is not None and not force_refresh:
            return self._instruments_df
        self._instruments_df = self._groww.get_all_instruments()
        logger.info(f"Loaded {len(self._instruments_df)} instruments")
        return self._instruments_df

    def get_nfo_instruments(self) -> pd.DataFrame:
        """Return NFO segment instruments (options + futures)."""
        df = self.get_instruments_df()
        return df[
            (df["exchange"] == "NSE") &
            (df["segment"] == "NFO")
        ].copy()

    def get_nfo_options(self, underlying: str) -> pd.DataFrame:
        """Return option contracts for an underlying (NIFTY / BANKNIFTY)."""
        nfo = self.get_nfo_instruments()
        options = nfo[
            (nfo["instrument_type"].isin(["CE", "PE"])) &
            (nfo["underlying_symbol"].str.upper() == underlying.upper())
        ].copy()
        if "expiry_date" in options.columns:
            options["expiry_date"] = pd.to_datetime(
                options["expiry_date"], errors="coerce"
            )
            today = pd.Timestamp.today().normalize()
            options = options[options["expiry_date"] >= today]
        return options

    # ── Spot Price ───────────────────────────────────────────────

    def get_nse_spot(self, index: str) -> Optional[float]:
        """
        Get current spot price for NIFTY or BANKNIFTY.
        Uses the index LTP from Groww API.
        """
        try:
            # Groww uses specific trading symbols for indices
            index_symbols = {
                "NIFTY":     "NIFTY 50",
                "BANKNIFTY": "NIFTY BANK",
            }
            symbol = index_symbols.get(index.upper())
            if not symbol:
                logger.warning(f"Unknown index: {index}")
                return None

            result = self._groww.get_ltp(
                segment=self._groww.SEGMENT_EQUITY,
                exchange_trading_symbols=f"NSE_{symbol}",
            )
            if result:
                key = f"NSE_{symbol}"
                if key in result:
                    ltp = float(result[key])
                    logger.info(f"Spot {index}: {ltp:,.2f}")
                    return ltp
                # Try first value
                for v in result.values():
                    return float(v)
            return None
        except Exception as e:
            logger.error(f"get_nse_spot({index}) failed: {e}")
            return None

    # ── Futures Price ────────────────────────────────────────────

    def get_nse_futures_price(self, index: str) -> Optional[dict]:
        """
        Get near-month futures price for NIFTY or BANKNIFTY.
        Returns dict with ltp, trading_symbol, expiry.
        """
        try:
            nfo = self.get_nfo_instruments()
            futures = nfo[
                (nfo["instrument_type"] == "FUT") &
                (nfo["underlying_symbol"].str.upper() == index.upper())
            ].copy()

            if futures.empty:
                return None

            futures["expiry_date"] = pd.to_datetime(
                futures["expiry_date"], errors="coerce"
            )
            today = pd.Timestamp.today().normalize()
            futures = futures[futures["expiry_date"] >= today]
            if futures.empty:
                return None

            futures = futures.sort_values("expiry_date")
            near = futures.iloc[0]
            ts = near["trading_symbol"]

            ltp_result = self._groww.get_ltp(
                segment=self._groww.SEGMENT_DERIVATIVE,
                exchange_trading_symbols=f"NSE_{ts}",
            )
            ltp = 0.0
            if ltp_result:
                key = f"NSE_{ts}"
                ltp = float(ltp_result.get(key, 0) or list(ltp_result.values())[0])

            return {
                "ltp":            ltp,
                "trading_symbol": ts,
                "expiry":         str(near["expiry_date"].date()),
            }
        except Exception as e:
            logger.error(f"get_nse_futures_price({index}) failed: {e}")
            return None

    # ── India VIX ────────────────────────────────────────────────

    def get_india_vix(self) -> dict:
        """
        Fetch India VIX (volatility index).
        Returns dict with vix, change_pct, available.
        """
        try:
            result = self._groww.get_ltp(
                segment=self._groww.SEGMENT_EQUITY,
                exchange_trading_symbols="NSE_INDIA VIX",
            )
            if result:
                vix = float(list(result.values())[0])
                return {
                    "vix":        vix,
                    "change_pct": 0.0,  # delta from previous close needs quote
                    "available":  True,
                    "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            return {"available": False, "vix": None}
        except Exception as e:
            logger.warning(f"India VIX fetch failed: {e}")
            return {"available": False, "vix": None, "error": str(e)}

    # ── Option Chain ─────────────────────────────────────────────

    def get_option_chain(self, index: str) -> Optional[dict]:
        """
        Build complete option chain for an index.

        Returns:
        {
            "chain":          [list of strike dicts],
            "expiries":       ["2026-03-27", ...],
            "nearest_expiry": "2026-03-27",
        }

        Each strike dict:
        {
            "strike":      23500.0,
            "expiry":      "2026-03-27",
            "call_ltp":    150.0,
            "call_oi":     1250000,
            "call_volume": 45000,
            "call_iv":     12.5,
            "put_ltp":     140.0,
            "put_oi":      980000,
            "put_volume":  38000,
            "put_iv":      13.2,
        }
        """
        try:
            options_df = self.get_nfo_options(index)
            if options_df.empty:
                logger.warning(f"No options found for {index}")
                return None

            # Get available expiries
            expiries = sorted(options_df["expiry_date"].dropna().unique())
            expiry_strs = [str(e.date()) if hasattr(e, 'date') else str(e)[:10]
                          for e in expiries]

            if not expiry_strs:
                return None

            nearest_expiry = expiry_strs[0]

            # Filter to nearest expiry for main chain
            nearest_dt = expiries[0]
            chain_df = options_df[options_df["expiry_date"] == nearest_dt]

            # Get unique strikes
            strikes = sorted(chain_df["strike_price"].dropna().unique())

            # Build chain: get LTP and OI for each strike
            # Strategy: try batch LTP first for speed, then individual quotes for OI
            chain = []

            # ── Get spot price for IV solver ──────────────
            spot_price = None
            try:
                spot_price = self.get_nse_spot(index)
            except Exception:
                pass

            dte = max(1, (nearest_dt - pd.Timestamp.today().normalize()).days)

            # ── Collect trading symbols for batch LTP ─────
            ce_symbols = {}   # strike → trading_symbol
            pe_symbols = {}
            for strike in strikes:
                ce = chain_df[
                    (chain_df["strike_price"] == strike) &
                    (chain_df["instrument_type"] == "CE")
                ]
                pe = chain_df[
                    (chain_df["strike_price"] == strike) &
                    (chain_df["instrument_type"] == "PE")
                ]
                if not ce.empty:
                    ce_symbols[strike] = ce.iloc[0]["trading_symbol"]
                if not pe.empty:
                    pe_symbols[strike] = pe.iloc[0]["trading_symbol"]

            # ── Batch LTP fetch (much faster than N individual calls) ──
            all_ts = []
            ts_to_strike_type = {}
            for strike, ts in ce_symbols.items():
                key = f"NSE_{ts}"
                all_ts.append(key)
                ts_to_strike_type[key] = (strike, "CE", ts)
            for strike, ts in pe_symbols.items():
                key = f"NSE_{ts}"
                all_ts.append(key)
                ts_to_strike_type[key] = (strike, "PE", ts)

            batch_ltp = {}
            try:
                if len(all_ts) == 1:
                    batch_ltp = self._groww.get_ltp(
                        segment=self._groww.SEGMENT_DERIVATIVE,
                        exchange_trading_symbols=all_ts[0],
                    ) or {}
                elif all_ts:
                    batch_ltp = self._groww.get_ltp(
                        segment=self._groww.SEGMENT_DERIVATIVE,
                        exchange_trading_symbols=tuple(all_ts),
                    ) or {}
            except Exception as e:
                logger.warning(f"Batch LTP failed, falling back to quotes: {e}")

            # ── Build chain per strike ────────────────────
            from core.options_engine import solve_iv

            for strike in strikes:
                row = {
                    "strike": float(strike),
                    "expiry": nearest_expiry,
                    "call_ltp": 0.0, "call_oi": 0, "call_volume": 0, "call_iv": 0.0,
                    "put_ltp": 0.0,  "put_oi": 0,  "put_volume": 0,  "put_iv": 0.0,
                }

                # CE data
                ce_ts = ce_symbols.get(strike)
                if ce_ts:
                    ce_key = f"NSE_{ce_ts}"
                    # LTP from batch
                    if ce_key in batch_ltp:
                        row["call_ltp"] = float(batch_ltp[ce_key])

                    # Full quote for OI + IV (individual call)
                    try:
                        ce_quote = self._groww.get_quote(
                            exchange="NSE",
                            segment=self._groww.SEGMENT_DERIVATIVE,
                            trading_symbol=ce_ts,
                        )
                        if ce_quote:
                            if not row["call_ltp"]:
                                row["call_ltp"] = float(ce_quote.get("last_price", 0) or 0)
                            row["call_oi"]     = int(ce_quote.get("open_interest", 0) or 0)
                            row["call_volume"] = int(ce_quote.get("volume", 0) or 0)
                            row["call_iv"]     = float(ce_quote.get("implied_volatility", 0) or 0)
                    except Exception:
                        pass

                    # ── IV solver fallback ─────────────────
                    if row["call_iv"] == 0 and row["call_ltp"] > 0 and spot_price:
                        row["call_iv"] = solve_iv(
                            market_price=row["call_ltp"],
                            spot=spot_price, strike=float(strike),
                            dte=dte, option_type="CE",
                        )

                # PE data
                pe_ts = pe_symbols.get(strike)
                if pe_ts:
                    pe_key = f"NSE_{pe_ts}"
                    if pe_key in batch_ltp:
                        row["put_ltp"] = float(batch_ltp[pe_key])

                    try:
                        pe_quote = self._groww.get_quote(
                            exchange="NSE",
                            segment=self._groww.SEGMENT_DERIVATIVE,
                            trading_symbol=pe_ts,
                        )
                        if pe_quote:
                            if not row["put_ltp"]:
                                row["put_ltp"] = float(pe_quote.get("last_price", 0) or 0)
                            row["put_oi"]     = int(pe_quote.get("open_interest", 0) or 0)
                            row["put_volume"] = int(pe_quote.get("volume", 0) or 0)
                            row["put_iv"]     = float(pe_quote.get("implied_volatility", 0) or 0)
                    except Exception:
                        pass

                    # ── IV solver fallback ─────────────────
                    if row["put_iv"] == 0 and row["put_ltp"] > 0 and spot_price:
                        row["put_iv"] = solve_iv(
                            market_price=row["put_ltp"],
                            spot=spot_price, strike=float(strike),
                            dte=dte, option_type="PE",
                        )

                chain.append(row)

            logger.info(
                f"Option chain: {index} | {len(chain)} strikes | "
                f"expiry={nearest_expiry} | {len(expiry_strs)} expiries"
            )
            return {
                "chain":          chain,
                "expiries":       expiry_strs,
                "nearest_expiry": nearest_expiry,
            }

        except Exception as e:
            logger.error(f"get_option_chain({index}) failed: {e}")
            return None

    # ── Historical Candles (Spot Index) ──────────────────────────

    def get_historical(
        self,
        index:    str,
        interval: str = "15minute",
        days:     int = 30,
    ) -> list[dict]:
        """
        Fetch historical OHLCV candles for an index spot.
        Uses the near-month futures contract as a proxy since
        Groww doesn't provide direct index candles.
        """
        try:
            # Get near-month futures trading symbol
            futures = self.get_nse_futures_price(index)
            if not futures:
                logger.warning(f"No futures contract for {index}")
                return []

            ts = futures["trading_symbol"]

            interval_map = {
                "1minute": 1, "5minute": 5, "10minute": 10,
                "15minute": 15, "30minute": 30, "1hour": 60,
                "4hour": 240, "1day": 1440,
            }
            interval_minutes = interval_map.get(interval, 15)

            end_dt   = datetime.today()
            start_dt = end_dt - timedelta(days=days)

            result = self._groww.get_historical_candle_data(
                trading_symbol=ts,
                exchange=self._groww.EXCHANGE_NSE,
                segment=self._groww.SEGMENT_DERIVATIVE,
                start_time=start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                end_time=end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                interval_in_minutes=interval_minutes,
            )

            if isinstance(result, pd.DataFrame):
                raw = result.to_dict("records")
            elif isinstance(result, dict):
                raw = result.get("candles", result.get("data", []))
            elif isinstance(result, list):
                raw = result
            else:
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

            logger.info(f"Historical: {len(candles)} candles | {index} | {interval}")
            return candles

        except Exception as e:
            logger.error(f"get_historical({index}) failed: {e}")
            return []

    # ── Find Specific NFO Contract ───────────────────────────────

    def find_nfo_contract(
        self,
        underlying:  str,
        expiry:      str,
        strike:      float,
        option_type: str,     # "CE" or "PE"
    ) -> Optional[dict]:
        """Find a specific NFO option contract instrument."""
        try:
            options = self.get_nfo_options(underlying)
            match = options[
                (options["strike_price"] == strike) &
                (options["instrument_type"] == option_type.upper())
            ]
            if not match.empty:
                # Filter by expiry
                match["expiry_str"] = match["expiry_date"].dt.strftime("%Y-%m-%d")
                expiry_match = match[match["expiry_str"] == expiry]
                if not expiry_match.empty:
                    return expiry_match.iloc[0].to_dict()
                return match.iloc[0].to_dict()
            return None
        except Exception as e:
            logger.error(f"find_nfo_contract failed: {e}")
            return None

    # ── NFO Order Placement (Production Only) ────────────────────

    def place_nfo_order(
        self,
        trading_symbol:   str,
        transaction_type: str,
        lots:             int,
        order_type:       str   = "MARKET",
        price:            float = 0.0,
        reference_id:     str   = None,
    ) -> dict:
        """Place NFO order. Production mode only."""
        from config import TRADING_MODE
        if TRADING_MODE != "production":
            raise RuntimeError(
                f"Order placement blocked in '{TRADING_MODE}' mode."
            )

        cfg = None
        for idx_cfg in NSE_LOT_CONFIG.values():
            if idx_cfg.get("active"):
                cfg = idx_cfg
                break
        lot_size = cfg["lot_size"] if cfg else 75
        quantity = lots * lot_size

        sdk_order_type = (
            self._groww.ORDER_TYPE_LIMIT
            if order_type.upper() == "LIMIT"
            else self._groww.ORDER_TYPE_MARKET
        )
        sdk_txn_type = (
            self._groww.TRANSACTION_TYPE_BUY
            if transaction_type.upper() == "BUY"
            else self._groww.TRANSACTION_TYPE_SELL
        )

        result = self._groww.place_order(
            validity          = self._groww.VALIDITY_DAY,
            exchange          = self._groww.EXCHANGE_NSE,
            order_type        = sdk_order_type,
            product           = self._groww.PRODUCT_NRML,
            quantity          = quantity,
            segment           = self._groww.SEGMENT_DERIVATIVE,
            trading_symbol    = trading_symbol,
            transaction_type  = sdk_txn_type,
            price             = price if order_type.upper() == "LIMIT" else 0.0,
            order_reference_id = reference_id,
        )
        logger.info(
            f"NFO order: {transaction_type} {lots}L ({quantity}qty) "
            f"{trading_symbol} [{order_type}] → {result}"
        )
        return result

    # ── Portfolio & Margin ───────────────────────────────────────

    def get_positions(self) -> list[dict]:
        try:
            result = self._groww.get_positions()
            if isinstance(result, pd.DataFrame):
                return result.to_dict("records")
            return result if isinstance(result, list) else []
        except Exception as e:
            logger.error(f"get_positions failed: {e}")
            return []

    def get_margin(self) -> dict:
        try:
            return self._groww.get_margin()
        except Exception as e:
            logger.error(f"get_margin failed: {e}")
            return {}

    # ── Health Check ─────────────────────────────────────────────

    def ping(self) -> dict:
        try:
            df = self.get_instruments_df()
            nfo_count = len(df[
                (df["exchange"] == "NSE") & (df["segment"] == "NFO")
            ])
            return {
                "status":           "ok",
                "total_instruments": len(df),
                "nfo_total":        nfo_count,
                "timestamp":        datetime.now().isoformat(),
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}
