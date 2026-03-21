# test_segments.py — discover actual Groww SDK constants and segment names

import os
from dotenv import load_dotenv
load_dotenv()

from generate_token import generate_totp_token, save_token_to_env
token = generate_totp_token()
save_token_to_env(token)
os.environ["GROWW_ACCESS_TOKEN"] = token

from growwapi import GrowwAPI
g = GrowwAPI(token)

# 1. Find ALL SDK constants
print("=" * 50)
print("ALL GrowwAPI ATTRIBUTES")
print("=" * 50)
for attr in sorted(dir(g)):
    if attr.startswith("SEGMENT") or attr.startswith("EXCHANGE") or attr.startswith("PRODUCT"):
        print(f"  {attr} = {getattr(g, attr)}")

print()

# 2. Find all unique segments in instruments
print("=" * 50)
print("SEGMENTS IN INSTRUMENTS CSV")
print("=" * 50)
df = g.get_all_instruments()
print(f"Unique exchanges: {df['exchange'].unique().tolist()}")
print(f"Unique segments:  {df['segment'].unique().tolist()}")
print()

# 3. Find Nifty options specifically
print("=" * 50)
print("FINDING NIFTY OPTIONS")
print("=" * 50)
nifty = df[df["underlying_symbol"].str.upper() == "NIFTY"]
print(f"Rows with underlying=NIFTY: {len(nifty)}")
if not nifty.empty:
    print(f"  Segments: {nifty['segment'].unique().tolist()}")
    print(f"  Exchanges: {nifty['exchange'].unique().tolist()}")
    print(f"  Instrument types: {nifty['instrument_type'].unique().tolist()}")
    print(f"  Sample trading_symbol: {nifty.iloc[0]['trading_symbol']}")
else:
    # Try partial match
    nifty2 = df[df["trading_symbol"].str.contains("NIFTY", case=False, na=False)]
    print(f"Rows with NIFTY in trading_symbol: {len(nifty2)}")
    if not nifty2.empty:
        print(f"  Segments: {nifty2['segment'].unique().tolist()}")
        print(f"  Exchanges: {nifty2['exchange'].unique().tolist()}")
        print(f"  Instrument types: {nifty2['instrument_type'].unique().tolist()}")
        print(f"  Sample rows:")
        for _, r in nifty2.head(5).iterrows():
            print(f"    {r['trading_symbol']} | {r['exchange']} | {r['segment']} | {r['instrument_type']}")

print()

# 4. Find BankNifty
print("=" * 50)
print("FINDING BANKNIFTY OPTIONS")
print("=" * 50)
bn = df[df["underlying_symbol"].str.contains("BANK", case=False, na=False)]
print(f"Rows with BANK in underlying: {len(bn)}")
if not bn.empty:
    print(f"  Segments: {bn['segment'].unique().tolist()}")
    print(f"  Sample underlying_symbols: {bn['underlying_symbol'].unique()[:5].tolist()}")

print()

# 5. Test get_ltp with different segment values
print("=" * 50)
print("TESTING LTP CALLS")
print("=" * 50)
for seg in df["segment"].unique():
    try:
        # Find a sample trading symbol for this segment
        seg_df = df[df["segment"] == seg]
        if seg_df.empty:
            continue
        sample = seg_df.iloc[0]["trading_symbol"]
        exch = seg_df.iloc[0]["exchange"]
        result = g.get_ltp(
            segment=seg,
            exchange_trading_symbols=f"{exch}_{sample}",
        )
        print(f"  segment='{seg}' → LTP works: {bool(result)}")
    except Exception as e:
        print(f"  segment='{seg}' → Error: {str(e)[:80]}")

print("\n✅ Diagnostic complete")