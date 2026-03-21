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
            (df["segment"] == "FNO")
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
        Tries multiple approaches:
          1. CASH segment with known index symbols
          2. Fallback to near-month futures LTP (close proxy)
        """
        # Approach 1: Direct index LTP via CASH segment
        # Groww may use different symbol formats — try common ones
        candidate_symbols = {
            "NIFTY":     ["NIFTY 50", "NIFTY", "Nifty 50"],
            "BANKNIFTY": ["NIFTY BANK", "BANKNIFTY", "Nifty Bank"],
        }
        symbols = candidate_symbols.get(index.upper(), [index])

        for symbol in symbols:
            try:
                result = self._groww.get_ltp(
                    segment=self._groww.SEGMENT_CASH,
                    exchange_trading_symbols=f"NSE_{symbol}",
                )
                if result:
                    for v in result.values():
                        ltp = float(v)
                        if ltp > 0:
                            logger.info(f"Spot {index}: {ltp:,.2f} (via {symbol})")
                            return ltp
            except Exception:
                continue

        # Approach 2: Use near-month futures as proxy
        try:
            fut = self.get_nse_futures_price(index)
            if fut and fut.get("ltp"):
                ltp = float(fut["ltp"])
                logger.info(f"Spot {index}: {ltp:,.2f} (via futures proxy)")
                return ltp
        except Exception:
            pass

        logger.warning(f"get_nse_spot({index}): all approaches failed")
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
                segment=self._groww.SEGMENT_FNO,
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
        vix_symbols = ["INDIA VIX", "INDIAVIX", "India VIX"]
        for symbol in vix_symbols:
            try:
                result = self._groww.get_ltp(
                    segment=self._groww.SEGMENT_CASH,
                    exchange_trading_symbols=f"NSE_{symbol}",
                )
                if result:
                    vix = float(list(result.values())[0])
                    if vix > 0:
                        return {
                            "vix":        vix,
                            "change_pct": 0.0,
                            "available":  True,
                            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
            except Exception:
                continue

        logger.warning("India VIX: all symbol formats failed")
        return {"available": False, "vix": None}

    # ── Option Chain ─────────────────────────────────────────────

    def get_option_chain(
        self,
        index:           str,
        strikes_each_side: int = 15,
    ) -> Optional[dict]:
        """
        Build option chain for an index, filtered to strikes near ATM.

        Only fetches ~30 strikes (±15 around ATM) to avoid thousands
        of API calls. Each strike needs a get_quote() for OI data.

        Returns:
        {
            "chain":          [list of strike dicts],
            "expiries":       ["2026-03-27", ...],
            "nearest_expiry": "2026-03-27",
        }
        """
        try:
            options_df = self.get_nfo_options(index)
            if options_df.empty:
                logger.warning(f"No options found for {index}")
                return None

            # Get available expiries
            expiries = sorted(options_df["expiry_date"].dropna().unique())
            expiry_strs = [
                str(e.date()) if hasattr(e, "date") else str(e)[:10]
                for e in expiries
            ]
            if not expiry_strs:
                return None

            nearest_expiry = expiry_strs[0]
            nearest_dt     = expiries[0]

            # Filter to nearest expiry
            chain_df = options_df[options_df["expiry_date"] == nearest_dt]
            all_strikes = sorted(chain_df["strike_price"].dropna().unique())

            if not all_strikes:
                return None

            # ── Get spot price to find ATM ─────────────────
            spot_price = self.get_nse_spot(index)
            if not spot_price:
                # Fallback: use middle strike
                spot_price = float(all_strikes[len(all_strikes) // 2])

            dte = max(1, (nearest_dt - pd.Timestamp.today().normalize()).days)

            # ── Filter to ±N strikes around ATM ───────────
            atm_idx = min(
                range(len(all_strikes)),
                key=lambda i: abs(float(all_strikes[i]) - spot_price),
            )
            start = max(0, atm_idx - strikes_each_side)
            end   = min(len(all_strikes), atm_idx + strikes_each_side + 1)
            selected_strikes = all_strikes[start:end]

            logger.info(
                f"Chain filter: {len(all_strikes)} total strikes → "
                f"{len(selected_strikes)} selected "
                f"(ATM≈{all_strikes[atm_idx]}, spot={spot_price:,.0f})"
            )

            # ── Map strikes to trading symbols ─────────────
            ce_symbols = {}
            pe_symbols = {}
            for strike in selected_strikes:
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

            # ── Batch LTP for all selected strikes ─────────
            # Groww API limit: max 50 symbols per get_ltp call
            all_ts_keys = []
            for strike, ts in ce_symbols.items():
                all_ts_keys.append(f"NSE_{ts}")
            for strike, ts in pe_symbols.items():
                all_ts_keys.append(f"NSE_{ts}")

            batch_ltp = {}
            BATCH_SIZE = 50
            if all_ts_keys:
                for i in range(0, len(all_ts_keys), BATCH_SIZE):
                    chunk = all_ts_keys[i:i + BATCH_SIZE]
                    try:
                        if len(chunk) == 1:
                            result = self._groww.get_ltp(
                                segment=self._groww.SEGMENT_FNO,
                                exchange_trading_symbols=chunk[0],
                            ) or {}
                        else:
                            result = self._groww.get_ltp(
                                segment=self._groww.SEGMENT_FNO,
                                exchange_trading_symbols=tuple(chunk),
                            ) or {}
                        batch_ltp.update(result)
                    except Exception as e:
                        logger.warning(f"Batch LTP chunk failed: {e}")
                logger.info(f"Batch LTP: {len(batch_ltp)} prices fetched")

            # ── Fetch OI via individual quotes ─────────────
            # Only for the ~30 selected strikes — manageable
            from core.options_engine import solve_iv

            chain = []
            for strike in selected_strikes:
                row = {
                    "strike":      float(strike),
                    "expiry":      nearest_expiry,
                    "call_ltp":    0.0, "call_oi": 0, "call_volume": 0, "call_iv": 0.0,
                    "put_ltp":     0.0, "put_oi":  0, "put_volume":  0, "put_iv":  0.0,
                }

                # ── CE ─────────────────────────────────────
                ce_ts = ce_symbols.get(strike)
                if ce_ts:
                    ce_key = f"NSE_{ce_ts}"
                    if ce_key in batch_ltp:
                        row["call_ltp"] = float(batch_ltp[ce_key])

                    try:
                        ce_quote = self._groww.get_quote(
                            exchange="NSE",
                            segment=self._groww.SEGMENT_FNO,
                            trading_symbol=ce_ts,
                        )
                        if ce_quote:
                            if not row["call_ltp"]:
                                row["call_ltp"] = float(
                                    ce_quote.get("last_price", 0) or 0
                                )
                            row["call_oi"]     = int(
                                ce_quote.get("open_interest", 0) or 0
                            )
                            row["call_volume"] = int(
                                ce_quote.get("volume", 0) or 0
                            )
                            row["call_iv"]     = float(
                                ce_quote.get("implied_volatility", 0) or 0
                            )
                    except Exception as e:
                        logger.debug(f"CE quote {ce_ts}: {e}")

                    # IV solver fallback
                    if (
                        row["call_iv"] == 0
                        and row["call_ltp"] > 0
                        and spot_price
                    ):
                        row["call_iv"] = solve_iv(
                            market_price=row["call_ltp"],
                            spot=spot_price, strike=float(strike),
                            dte=dte, option_type="CE",
                        )

                # ── PE ─────────────────────────────────────
                pe_ts = pe_symbols.get(strike)
                if pe_ts:
                    pe_key = f"NSE_{pe_ts}"
                    if pe_key in batch_ltp:
                        row["put_ltp"] = float(batch_ltp[pe_key])

                    try:
                        pe_quote = self._groww.get_quote(
                            exchange="NSE",
                            segment=self._groww.SEGMENT_FNO,
                            trading_symbol=pe_ts,
                        )
                        if pe_quote:
                            if not row["put_ltp"]:
                                row["put_ltp"] = float(
                                    pe_quote.get("last_price", 0) or 0
                                )
                            row["put_oi"]     = int(
                                pe_quote.get("open_interest", 0) or 0
                            )
                            row["put_volume"] = int(
                                pe_quote.get("volume", 0) or 0
                            )
                            row["put_iv"]     = float(
                                pe_quote.get("implied_volatility", 0) or 0
                            )
                    except Exception as e:
                        logger.debug(f"PE quote {pe_ts}: {e}")

                    # IV solver fallback
                    if (
                        row["put_iv"] == 0
                        and row["put_ltp"] > 0
                        and spot_price
                    ):
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
                segment=self._groww.SEGMENT_FNO,
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
            segment           = self._groww.SEGMENT_FNO,
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
                (df["exchange"] == "NSE") & (df["segment"] == "FNO")
            ])
            return {
                "status":           "ok",
                "total_instruments": len(df),
                "nfo_total":        nfo_count,
                "timestamp":        datetime.now().isoformat(),
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}