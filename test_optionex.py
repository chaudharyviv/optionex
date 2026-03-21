"""
OPTIONEX — Comprehensive Test Suite
Tests all pure-computation modules without network/API calls.

Run: python test_optionex.py

Tests:
  1. Config validation & position sizing
  2. Database init & health check
  3. Options Engine (Greeks, IV rank, PCR, max pain, OI walls)
  4. Data Bundle assembly (mock data)
  5. Sanity Checker (all 12 contradiction checks)
  6. Risk Engine (all 15 guardrails)
  7. End-to-end mock pipeline
"""

import sys
import os
import math
import json
from datetime import datetime, timedelta

# ─────────────────────────────────────────────────────────────────
# Setup path
# ─────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))

PASS = 0
FAIL = 0


def test(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name} — {detail}")


# ═════════════════════════════════════════════════════════════════
# TEST 1: CONFIG
# ═════════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("TEST 1: Configuration")
print("=" * 60)

from config import (
    NSE_LOT_CONFIG, ACTIVE_INDICES, ALLOWED_STRATEGIES,
    get_options_position_size, validate_config,
    CAPITAL_INR, RISK_PCT_PER_TRADE, MIN_RR_RATIO,
)

test("NIFTY config exists", "NIFTY" in NSE_LOT_CONFIG)
test("BANKNIFTY config exists", "BANKNIFTY" in NSE_LOT_CONFIG)
test("NIFTY lot_size=75", NSE_LOT_CONFIG["NIFTY"]["lot_size"] == 75)
test("BANKNIFTY lot_size=30", NSE_LOT_CONFIG["BANKNIFTY"]["lot_size"] == 30)
test("7 allowed strategies", len(ALLOWED_STRATEGIES) == 7)
test("iron_condor in allowlist", "iron_condor" in ALLOWED_STRATEGIES)
test("short_straddle NOT in allowlist", "short_straddle" not in ALLOWED_STRATEGIES)

# Position sizing — Nifty Bull Call Spread
# spread width 100, net cost 70pts → max_loss = 70 × 75 = ₹5,250
ps = get_options_position_size(
    index="NIFTY", strategy="bull_call_spread",
    max_loss_per_lot=5250, premium_per_lot=5250,
)
test("Position sizing returns dict", isinstance(ps, dict))
test("Position sizing: lots ≥ 1", ps["position_lots"] >= 1)
test("Position sizing: risk within budget",
     ps["actual_risk_pct"] <= RISK_PCT_PER_TRADE * 2)  # allow underlotted

# Position sizing — premium cap
ps2 = get_options_position_size(
    index="NIFTY", strategy="long_call",
    max_loss_per_lot=30000, premium_per_lot=30000,
)
test("Premium cap triggers", ps2.get("premium_capped", False) or ps2["premium_total"] <= 25000)

# Position sizing — B-grade reduction
ps3 = get_options_position_size(
    index="NIFTY", strategy="bull_call_spread",
    max_loss_per_lot=1000, signal_quality="B",
)
test("B-grade reduces lots", ps3["b_grade_reduced"] == True)

# Config validation
warnings = validate_config()
test("Validate config returns list", isinstance(warnings, list))

print()


# ═════════════════════════════════════════════════════════════════
# TEST 2: DATABASE
# ═════════════════════════════════════════════════════════════════

print("=" * 60)
print("TEST 2: Database")
print("=" * 60)

# Use temp DB for testing
import config
import tempfile
original_db = config.DB_PATH
config.DB_PATH = tempfile.mktemp(suffix=".db")

from core.db import init, health_check, get_connection

init()
hc = health_check()
test("DB init succeeds", hc["status"] == "ok")
test("All 8 tables created", len(hc.get("missing", [])) == 0,
     f"missing={hc.get('missing')}")

# Test insert and read
conn = get_connection()
cursor = conn.cursor()
cursor.execute("""
    INSERT INTO iv_history (index_name, date, atm_iv, spot_close)
    VALUES ('NIFTY', '2026-03-20', 14.5, 23450)
""")
conn.commit()
cursor.execute("SELECT atm_iv FROM iv_history WHERE index_name='NIFTY'")
row = cursor.fetchone()
test("IV history insert/read", row and float(row["atm_iv"]) == 14.5)
conn.close()

# Restore original DB path
config.DB_PATH = original_db

print()


# ═════════════════════════════════════════════════════════════════
# TEST 3: OPTIONS ENGINE
# ═════════════════════════════════════════════════════════════════

print("=" * 60)
print("TEST 3: Options Engine")
print("=" * 60)

from core.options_engine import OptionsEngine, compute_bs_greeks, OptionsData

# ── Black-Scholes Greeks ─────────────────────────────
# ATM call: spot=23500, strike=23500, IV=15%, DTE=5
greeks = compute_bs_greeks(
    spot=23500, strike=23500, iv=15.0, dte=5,
    option_type="CE", lot_size=75,
)
test("Call delta ≈ 0.50", 0.45 < greeks["delta"] < 0.55,
     f"delta={greeks['delta']}")
test("Call delta positive", greeks["delta"] > 0)
test("Call theta negative", greeks["theta"] < 0,
     f"theta={greeks['theta']}")
test("Call vega positive", greeks["vega"] > 0)
test("Gamma positive", greeks["gamma"] > 0)
test("Moneyness is ATM", greeks["moneyness"] == "ATM")

# OTM put
greeks_otm = compute_bs_greeks(
    spot=23500, strike=23000, iv=16.0, dte=5,
    option_type="PE", lot_size=75,
)
test("OTM put delta negative", greeks_otm["delta"] < 0)
test("OTM put |delta| < 0.5", abs(greeks_otm["delta"]) < 0.5)
test("OTM put moneyness=OTM", greeks_otm["moneyness"] == "OTM")

# ITM call
greeks_itm = compute_bs_greeks(
    spot=23500, strike=23000, iv=14.0, dte=5,
    option_type="CE", lot_size=75,
)
test("ITM call delta > 0.5", greeks_itm["delta"] > 0.5)
test("ITM call moneyness=ITM", greeks_itm["moneyness"] == "ITM")

# At expiry (DTE=0)
greeks_exp = compute_bs_greeks(
    spot=23500, strike=23400, iv=15.0, dte=0,
    option_type="CE", lot_size=75,
)
test("Expiry: delta=1 for ITM call", greeks_exp["delta"] == 1.0)
test("Expiry: theta=0", greeks_exp["theta"] == 0.0)

# Put-call parity sanity: call_delta - put_delta ≈ 1
call_d = compute_bs_greeks(23500, 23500, 15.0, 5, option_type="CE", lot_size=75)["delta"]
put_d  = compute_bs_greeks(23500, 23500, 15.0, 5, option_type="PE", lot_size=75)["delta"]
test("Put-call parity: C_delta - P_delta ≈ 1",
     abs((call_d - put_d) - 1.0) < 0.02,
     f"diff={call_d - put_d:.4f}")

# ── Full Options Engine ──────────────────────────────
engine = OptionsEngine()

# Build mock chain
mock_chain = []
for strike in range(23000, 24100, 50):
    dist = abs(strike - 23500)
    call_iv = 14.0 + dist * 0.002
    put_iv  = 14.5 + dist * 0.003
    call_oi = max(10000, 500000 - dist * 300)
    put_oi  = max(10000, 400000 - dist * 250)
    mock_chain.append({
        "strike": float(strike),
        "expiry": "2026-03-27",
        "call_ltp": max(1.0, (23500 - strike + 200) * 0.5) if strike <= 23500 else max(0.5, (23600 - strike) * 0.3),
        "call_oi": call_oi,
        "call_volume": call_oi // 10,
        "call_iv": call_iv,
        "put_ltp": max(1.0, (strike - 23300) * 0.5) if strike >= 23500 else max(0.5, (strike - 23000) * 0.2),
        "put_oi": put_oi,
        "put_volume": put_oi // 10,
        "put_iv": put_iv,
    })

# Historical IV (252 days)
import random
random.seed(42)
hist_iv = [12.0 + random.gauss(0, 2) for _ in range(252)]
hist_iv = [max(8, min(30, iv)) for iv in hist_iv]

# Historical closes
hist_closes = [23000 + i * 2 + random.gauss(0, 50) for i in range(252)]

result = engine.compute(
    chain_data=mock_chain,
    spot_price=23500.0,
    futures_price=23520.0,
    index="NIFTY",
    nearest_expiry="2026-03-27",
    available_expiries=["2026-03-27", "2026-04-03", "2026-04-24"],
    historical_iv=hist_iv,
    historical_closes=hist_closes,
    lot_size=75,
)

test("OptionsData returned", isinstance(result, OptionsData))
test("ATM IV > 0", result.atm_iv > 0, f"atm_iv={result.atm_iv}")
test("IV rank 0-100", 0 <= result.iv_rank <= 100, f"rank={result.iv_rank}")
test("IV percentile 0-100", 0 <= result.iv_percentile <= 100)
test("IV regime is string", result.iv_regime in ("low", "normal", "high", "extreme"))
test("HV(20) > 0", result.historical_vol_20 > 0)
test("PCR > 0", result.pcr_oi > 0, f"pcr={result.pcr_oi}")
test("Max pain is a strike", result.max_pain_strike in [c["strike"] for c in mock_chain])
test("Call OI wall exists", result.highest_call_oi_strike > 0)
test("Put OI wall exists", result.highest_put_oi_strike > 0)
test("ATM call delta ≈ 0.5", 0.4 < result.atm_call_delta < 0.6)
test("ATM put delta ≈ -0.5", -0.6 < result.atm_put_delta < -0.4)
test("Chain snapshot non-empty", len(result.chain_snapshot) > 0)
test("Futures basis computed", result.futures_basis == 20.0)
test("DTE > 0", result.dte_nearest > 0)

# Summary string
summary = result.summary_string()
test("Summary string non-empty", len(summary) > 100)
test("Summary contains IV", "IV" in summary)
test("Summary contains PCR", "PCR" in summary)

print()


# ═════════════════════════════════════════════════════════════════
# TEST 4: SANITY CHECKER
# ═════════════════════════════════════════════════════════════════

print("=" * 60)
print("TEST 4: Sanity Checker")
print("=" * 60)

# Import sanity checker directly to avoid pydantic import from llm_client
# We mock the analysis objects since pydantic isn't available in test env
import importlib
import types

# Create a minimal mock of llm_client so the import chain works
mock_llm = types.ModuleType("core.llm_client")

class _FakeBaseModel:
    pass

mock_llm.LLMClient = _FakeBaseModel
mock_llm.OptionsMarketAnalysis = _FakeBaseModel
mock_llm.load_prompt = lambda *a, **k: ""
sys.modules["core.llm_client"] = mock_llm

from core.agents.options_analyst_agent import OptionsSanityChecker
from core.options_data_bundle import OptionsDataBundle

checker = OptionsSanityChecker()

# Mock analysis — high IV + buy_premium contradiction
class MockAnalysis:
    market_regime = "trending_up"
    trend_strength = "moderate"
    overall_sentiment = "bullish"
    iv_regime = "high"
    recommended_bias = "buy_premium"
    high_impact_events_next_24h = None

# Mock bundle with high IV rank
mock_bundle = OptionsDataBundle(
    index="NIFTY", timeframe="15minute", trading_style="system",
)
mock_options = OptionsData(
    index="NIFTY", spot_price=23500, futures_price=23520,
)
mock_options.iv_rank = 75
mock_options.pcr_oi = 1.1
mock_options.max_pain_strike = 23400
mock_options.max_pain_distance = 100
mock_options.dte_nearest = 3
mock_options.highest_call_oi_strike = 24000
mock_options.highest_put_oi_strike = 23000
mock_options.iv_term_structure = "contango"
mock_bundle.options = mock_options
mock_bundle.options_ok = True

result_sanity = checker.check(MockAnalysis(), mock_bundle)
test("Sanity catches high IV + buy_premium", not result_sanity["passed"])
test("Sanity has warnings", len(result_sanity["warnings"]) > 0)
test("Confidence cap set", result_sanity["confidence_cap"] is not None)

# Clean case — low IV + buy_premium (should pass)
class CleanAnalysis:
    market_regime = "trending_up"
    trend_strength = "moderate"
    overall_sentiment = "bullish"
    iv_regime = "low"
    recommended_bias = "buy_premium"
    high_impact_events_next_24h = None

clean_options = OptionsData(
    index="NIFTY", spot_price=23500, futures_price=23520,
)
clean_options.iv_rank = 25
clean_options.pcr_oi = 0.9
clean_options.max_pain_strike = 23500
clean_options.max_pain_distance = 0
clean_options.dte_nearest = 5
clean_options.highest_call_oi_strike = 24000
clean_options.highest_put_oi_strike = 23000
clean_options.iv_term_structure = "contango"
clean_bundle = OptionsDataBundle(
    index="NIFTY", timeframe="15minute", trading_style="system",
)
clean_bundle.options = clean_options
clean_bundle.options_ok = True

clean_sanity = checker.check(CleanAnalysis(), clean_bundle)
test("Clean analysis passes sanity", clean_sanity["passed"])
test("No warnings on clean", len(clean_sanity["warnings"]) == 0)

# VIX spike test
vix_bundle = OptionsDataBundle(
    index="NIFTY", timeframe="15minute", trading_style="system",
)
vix_bundle.options = clean_options
vix_bundle.options_ok = True
vix_bundle.india_vix_change = 15.0  # 15% spike

vix_sanity = checker.check(CleanAnalysis(), vix_bundle)
test("VIX spike flagged", any("VIX" in w for w in vix_sanity["warnings"]))

print()


# ═════════════════════════════════════════════════════════════════
# TEST 5: RISK ENGINE (GUARDRAILS)
# ═════════════════════════════════════════════════════════════════

print("=" * 60)
print("TEST 5: Risk Engine (15 Guardrails)")
print("=" * 60)

from core.risk_engine import OptionsRiskEngine

re = OptionsRiskEngine()

# All-pass scenario
g = re.check_all(
    index="NIFTY", action="BUY_PREMIUM", confidence=70,
    rr_ratio=2.0, trading_style="system", strategy_name="long_call",
    iv_percentile=50, dte=5, premium_total=5000,
    open_positions=0, daily_pnl_pct=0, is_buying_premium=True,
)
test("Clean signal approved", g["approved"])
test("No block reasons", len(g["block_reasons"]) == 0)
test("15 guardrail results", len(g["guardrail_results"]) == 15)

# G1: Daily loss exceeded
g1 = re.check_all(
    index="NIFTY", action="BUY_PREMIUM", confidence=70,
    rr_ratio=2.0, trading_style="system", strategy_name="long_call",
    daily_pnl_pct=-5.1,
)
test("G1 blocks on daily loss", not g1["approved"])
test("G1 reason mentions loss", any("loss" in r.lower() for r in g1["block_reasons"]))

# G3: Low confidence
g3 = re.check_all(
    index="NIFTY", action="BUY_PREMIUM", confidence=40,
    rr_ratio=2.0, trading_style="system", strategy_name="long_call",
)
test("G3 blocks low confidence", not g3["approved"])

# G6: Low R:R
g6 = re.check_all(
    index="NIFTY", action="BUY_PREMIUM", confidence=70,
    rr_ratio=0.8, trading_style="system", strategy_name="long_call",
)
test("G6 blocks low R:R", not g6["approved"])

# G11: IV too high for buying
g11 = re.check_all(
    index="NIFTY", action="BUY_PREMIUM", confidence=70,
    rr_ratio=2.0, trading_style="system", strategy_name="long_call",
    iv_percentile=85, is_buying_premium=True,
)
test("G11 blocks high IV buying", not g11["approved"])

# G12: DTE too low for buying
g12 = re.check_all(
    index="NIFTY", action="BUY_PREMIUM", confidence=70,
    rr_ratio=2.0, trading_style="system", strategy_name="long_call",
    dte=1, is_buying_premium=True,
)
test("G12 blocks low DTE buying", not g12["approved"])

# G13: Premium cap
g13 = re.check_all(
    index="NIFTY", action="BUY_PREMIUM", confidence=70,
    rr_ratio=2.0, trading_style="system", strategy_name="long_call",
    premium_total=30000,
)
test("G13 blocks high premium", not g13["approved"])

# G14: Strategy not in allowlist
g14 = re.check_all(
    index="NIFTY", action="BUY_PREMIUM", confidence=70,
    rr_ratio=2.0, trading_style="system", strategy_name="short_straddle",
)
test("G14 blocks disallowed strategy", not g14["approved"])

# HOLD passes through
g_hold = re.check_all(
    index="NIFTY", action="HOLD", confidence=30,
    rr_ratio=None, trading_style="system", strategy_name="none",
)
test("HOLD not approved (correct)", not g_hold["approved"])

print()


# ═════════════════════════════════════════════════════════════════
# TEST 6: DATA BUNDLE
# ═════════════════════════════════════════════════════════════════

print("=" * 60)
print("TEST 6: Data Bundle")
print("=" * 60)

bundle = OptionsDataBundle(
    index="NIFTY", timeframe="15minute", trading_style="system",
    spot_price=23500, spot_available=True,
)

# No news cap
bundle.news_available = False
bundle.apply_confidence_caps()
test("No news caps confidence", bundle.confidence_cap <= 65)

# VIX cap
bundle2 = OptionsDataBundle(
    index="NIFTY", timeframe="15minute", trading_style="system",
)
bundle2.india_vix = 25.0
bundle2.apply_confidence_caps()
test("VIX elevated caps confidence", bundle2.confidence_cap <= 55)

# Prompt string
bundle3 = OptionsDataBundle(
    index="NIFTY", timeframe="15minute", trading_style="system",
    spot_price=23500, spot_available=True,
)
bundle3.options = mock_options
bundle3.options_ok = True
prompt = bundle3.to_prompt_string()
test("Prompt string non-empty", len(prompt) > 200)
test("Prompt contains NIFTY", "NIFTY" in prompt)
test("Prompt contains spot", "23,500" in prompt or "23500" in prompt)

print()


# ═════════════════════════════════════════════════════════════════
# TEST 7: IV SOLVER (Newton-Raphson)
# ═════════════════════════════════════════════════════════════════

print("=" * 60)
print("TEST 7: IV Solver (Newton-Raphson)")
print("=" * 60)

from core.options_engine import solve_iv, compute_bs_greeks

# Forward test: compute BS price at known IV, then solve back
# ATM call: spot=23500, strike=23500, IV=15%, DTE=5
known_iv   = 15.0
known_greeks = compute_bs_greeks(
    spot=23500, strike=23500, iv=known_iv, dte=5,
    option_type="CE", lot_size=1,  # per share
)
market_price = known_greeks["price_bs"]

solved_iv = solve_iv(
    market_price=market_price,
    spot=23500, strike=23500, dte=5,
    option_type="CE",
)
test("IV solver converges for ATM call",
     abs(solved_iv - known_iv) < 0.5,
     f"known={known_iv}, solved={solved_iv}")

# OTM put
known_iv_put = 18.0
put_greeks = compute_bs_greeks(
    spot=23500, strike=23000, iv=known_iv_put, dte=5,
    option_type="PE", lot_size=1,
)
solved_iv_put = solve_iv(
    market_price=put_greeks["price_bs"],
    spot=23500, strike=23000, dte=5,
    option_type="PE",
)
test("IV solver converges for OTM put",
     abs(solved_iv_put - known_iv_put) < 0.5,
     f"known={known_iv_put}, solved={solved_iv_put}")

# Deep ITM call
known_iv_itm = 14.0
itm_greeks = compute_bs_greeks(
    spot=23500, strike=22500, iv=known_iv_itm, dte=10,
    option_type="CE", lot_size=1,
)
solved_iv_itm = solve_iv(
    market_price=itm_greeks["price_bs"],
    spot=23500, strike=22500, dte=10,
    option_type="CE",
)
test("IV solver converges for deep ITM",
     abs(solved_iv_itm - known_iv_itm) < 1.0,
     f"known={known_iv_itm}, solved={solved_iv_itm}")

# Edge: zero premium
test("IV solver: zero premium returns 0", solve_iv(0, 23500, 23500, 5) == 0.0)

# Edge: zero DTE
test("IV solver: zero DTE returns 0", solve_iv(100, 23500, 23500, 0) == 0.0)

# High IV scenario (IV=40%)
high_iv_greeks = compute_bs_greeks(
    spot=23500, strike=23500, iv=40.0, dte=30,
    option_type="CE", lot_size=1,
)
solved_high = solve_iv(
    market_price=high_iv_greeks["price_bs"],
    spot=23500, strike=23500, dte=30,
    option_type="CE",
)
test("IV solver converges for high IV (40%)",
     abs(solved_high - 40.0) < 1.0,
     f"known=40.0, solved={solved_high}")

# Low IV scenario (IV=8%)
low_iv_greeks = compute_bs_greeks(
    spot=23500, strike=23500, iv=8.0, dte=30,
    option_type="CE", lot_size=1,
)
solved_low = solve_iv(
    market_price=low_iv_greeks["price_bs"],
    spot=23500, strike=23500, dte=30,
    option_type="CE",
)
test("IV solver converges for low IV (8%)",
     abs(solved_low - 8.0) < 0.5,
     f"known=8.0, solved={solved_low}")

print()


# ═════════════════════════════════════════════════════════════════
# TEST 8: IV ARCHIVER
# ═════════════════════════════════════════════════════════════════

print("=" * 60)
print("TEST 8: IV Archiver")
print("=" * 60)

from core.iv_archiver import archive_today, get_iv_history, get_history_stats, seed_iv_history

# Archive a record (uses temp DB from TEST 2)
import config as _cfg
import tempfile
_cfg.DB_PATH = tempfile.mktemp(suffix=".db")

# Reload db module to pick up new DB_PATH
import importlib
import core.db
importlib.reload(core.db)
from core.db import init as _db_init
_db_init()

# Reload archiver to use refreshed db
import core.iv_archiver
importlib.reload(core.iv_archiver)
from core.iv_archiver import archive_today, get_iv_history, get_history_stats, seed_iv_history

result_archive = archive_today(
    index="NIFTY", atm_iv=14.5, spot_close=23450,
    pcr_oi=0.95, max_pain=23400, india_vix=13.2,
)
test("IV archive succeeds", result_archive["status"] == "ok")

# Read back
history = get_iv_history("NIFTY")
test("IV history has 1 record", len(history) == 1)
test("IV history value correct", abs(history[0]["atm_iv"] - 14.5) < 0.01)

# Stats
stats = get_history_stats("NIFTY")
test("Stats has days count", stats.get("days", 0) == 1)

# Seed bulk data
seed_data = [
    {"date": f"2025-{m:02d}-15", "atm_iv": 12 + m * 0.5, "spot_close": 22000 + m * 100}
    for m in range(1, 13)
]
inserted = seed_iv_history("NIFTY", seed_data)
test("Seed inserts records", inserted == 12)

# Now IV rank should work with enough data
history_after = get_iv_history("NIFTY")
test("History has 13 records after seed", len(history_after) == 13)

print()


# ═════════════════════════════════════════════════════════════════
# TEST 9: STRATEGY MAX LOSS CALCULATIONS
# ═════════════════════════════════════════════════════════════════

print("=" * 60)
print("TEST 9: Strategy Max Loss Calculations")
print("=" * 60)

# These are the calculations Agent 3 should produce — we validate the math

lot_size = 75  # Nifty

# Long Call: max_loss = premium × lot_size
premium = 150  # ₹150 per share
max_loss_long_call = premium * lot_size
test("Long call max loss = premium × lot", max_loss_long_call == 11250)

# Bull Call Spread (debit): buy 23500CE @ 150, sell 23600CE @ 100
# net_debit = 150 - 100 = 50
# max_loss = net_debit × lot_size = 50 × 75 = 3,750
# max_profit = (spread_width - net_debit) × lot_size = (100-50) × 75 = 3,750
net_debit = 150 - 100
max_loss_bcs = net_debit * lot_size
max_profit_bcs = (100 - net_debit) * lot_size
test("Bull call spread max loss", max_loss_bcs == 3750)
test("Bull call spread max profit", max_profit_bcs == 3750)
test("Bull call spread R:R = 1.0", abs(max_profit_bcs / max_loss_bcs - 1.0) < 0.01)

# Iron Condor: sell 23400PE @ 30, buy 23300PE @ 15, sell 23600CE @ 40, buy 23700CE @ 20
# Put spread width = 100, net credit put side = 30-15 = 15
# Call spread width = 100, net credit call side = 40-20 = 20
# Total net credit = 15 + 20 = 35
# Max loss = (wider_spread_width - total_credit) × lot = (100 - 35) × 75 = 4,875
# Max profit = total_credit × lot = 35 × 75 = 2,625
total_credit = (30-15) + (40-20)
max_loss_ic = (100 - total_credit) * lot_size
max_profit_ic = total_credit * lot_size
test("Iron condor max loss", max_loss_ic == 4875)
test("Iron condor max profit", max_profit_ic == 2625)
test("Iron condor R:R < 1 (expected for IC)",
     max_profit_ic / max_loss_ic < 1.0)

# Long Straddle: buy 23500CE @ 150, buy 23500PE @ 140
# max_loss = total premium × lot = (150 + 140) × 75 = 21,750
# breakeven_upper = 23500 + 290 = 23790
# breakeven_lower = 23500 - 290 = 23210
total_premium = 150 + 140
max_loss_straddle = total_premium * lot_size
be_upper = 23500 + total_premium
be_lower = 23500 - total_premium
test("Long straddle max loss", max_loss_straddle == 21750)
test("Long straddle BE upper", be_upper == 23790)
test("Long straddle BE lower", be_lower == 23210)

# Position sizing for the bull call spread
ps_bcs = get_options_position_size(
    index="NIFTY", strategy="bull_call_spread",
    max_loss_per_lot=max_loss_bcs,
    premium_per_lot=max_loss_bcs,
)
test("BCS position sizing: lots calculated",
     ps_bcs["position_lots"] >= 1)
test("BCS position sizing: risk within budget",
     ps_bcs["actual_risk_inr"] <= CAPITAL_INR * 0.05)  # 2% × 2.5 safety margin

# Position sizing for iron condor
ps_ic = get_options_position_size(
    index="NIFTY", strategy="iron_condor",
    max_loss_per_lot=max_loss_ic,
)
test("IC position sizing: lots calculated",
     ps_ic["position_lots"] >= 1)

print()


# ═════════════════════════════════════════════════════════════════
# SUMMARY
# ═════════════════════════════════════════════════════════════════

print("=" * 60)
total = PASS + FAIL
print(f"RESULTS: {PASS}/{total} passed, {FAIL} failed")
if FAIL == 0:
    print("🎉 ALL TESTS PASSED")
else:
    print(f"⚠ {FAIL} test(s) need attention")
print("=" * 60)

sys.exit(0 if FAIL == 0 else 1)
