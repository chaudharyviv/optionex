# test_groww.py — run all Step 3 checks in one go

import os
from dotenv import load_dotenv
load_dotenv()

from generate_token import generate_totp_token, save_token_to_env
token = generate_totp_token()
save_token_to_env(token)
os.environ["GROWW_ACCESS_TOKEN"] = token

from core.groww_client import GrowwClient
client = GrowwClient(access_token=token)
print("✅ Client ready\n")

# 3b: Instruments
print("=" * 50)
print("STEP 3b: Instruments")
print("=" * 50)
df = client.get_instruments_df()
print(f"Total instruments: {len(df)}")
print(f"Columns: {list(df.columns)}")

fno = df[(df["exchange"] == "NSE") & (df["segment"] == "FNO")]
print(f"FNO instruments: {len(fno)}")
if not fno.empty:
    print(f"Instrument types: {fno['instrument_type'].unique()}")

for col in ["trading_symbol", "underlying_symbol", "strike_price", "expiry_date", "instrument_type"]:
    print(f"  '{col}' exists: {col in df.columns}")

# 3c: Spot
print("\n" + "=" * 50)
print("STEP 3c: Spot Price")
print("=" * 50)
try:
    nifty = client.get_nse_spot("NIFTY")
    print(f"Nifty spot: {nifty}")
except Exception as e:
    print(f"❌ Nifty spot failed: {e}")

try:
    bnifty = client.get_nse_spot("BANKNIFTY")
    print(f"BankNifty spot: {bnifty}")
except Exception as e:
    print(f"❌ BankNifty spot failed: {e}")

# 3d: SDK constants
print("\n" + "=" * 50)
print("STEP 3d: SDK Constants")
print("=" * 50)
g = client._groww
for attr in ["SEGMENT_EQUITY", "SEGMENT_DERIVATIVE", "SEGMENT_COMMODITY", "EXCHANGE_NSE", "EXCHANGE_MCX"]:
    print(f"  {attr}: {getattr(g, attr, 'NOT FOUND')}")

# 3e: Futures
print("\n" + "=" * 50)
print("STEP 3e: Futures Price")
print("=" * 50)
try:
    fut = client.get_nse_futures_price("NIFTY")
    print(f"Futures: {fut}")
except Exception as e:
    print(f"❌ Futures failed: {e}")

# 3f: VIX
print("\n" + "=" * 50)
print("STEP 3f: India VIX")
print("=" * 50)
try:
    vix = client.get_india_vix()
    print(f"VIX: {vix}")
except Exception as e:
    print(f"❌ VIX failed: {e}")

# 3g: Option chain
print("\n" + "=" * 50)
print("STEP 3g: Option Chain")
print("=" * 50)
try:
    chain = client.get_option_chain("NIFTY")
    if chain:
        print(f"Strikes: {len(chain['chain'])}")
        print(f"Expiries: {chain['expiries'][:4]}")
        print(f"Nearest: {chain['nearest_expiry']}")
        if chain['chain']:
            mid = len(chain['chain']) // 2
            print(f"Sample strike: {chain['chain'][mid]}")
    else:
        print("❌ Chain returned None")
except Exception as e:
    print(f"❌ Chain failed: {e}")

# 3h: Single quote fields
print("\n" + "=" * 50)
print("STEP 3h: Option Quote Fields")
print("=" * 50)
try:
    opts = client.get_nfo_options("NIFTY")
    if not opts.empty:
        sample_ts = opts.iloc[0]["trading_symbol"]
        print(f"Sample: {sample_ts}")
        quote = g.get_quote(
            exchange="NSE",
            segment=g.SEGMENT_FNO,
            trading_symbol=sample_ts,
        )
        if quote:
            print(f"Quote keys: {list(quote.keys())}")
            for field in ["last_price", "open_interest", "volume", "implied_volatility", "oi_day_change"]:
                print(f"  '{field}': {quote.get(field, 'MISSING')}")
        else:
            print("❌ Quote returned None")
    else:
        print("❌ No NFO options found")
except Exception as e:
    print(f"❌ Quote failed: {e}")

print("\n✅ Step 3 complete")