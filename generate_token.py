"""
OPTIONEX — Groww TOTP Token Generator
Same as COMMODEX — generates fresh access token using TOTP.
Run: python generate_token.py
"""

import os
import pyotp
from pathlib import Path
from dotenv import load_dotenv, set_key
from growwapi import GrowwAPI

load_dotenv()
ENV_PATH = Path(".env")


def generate_totp_token() -> str:
    api_key     = os.getenv("GROWW_API_KEY")
    totp_secret = os.getenv("GROWW_TOTP_SECRET")
    if not api_key:
        raise ValueError("GROWW_API_KEY not set in .env")
    if not totp_secret:
        raise ValueError("GROWW_TOTP_SECRET not set in .env")
    totp = pyotp.TOTP(totp_secret).now()
    return GrowwAPI.get_access_token(api_key=api_key, totp=totp)


def save_token_to_env(token: str):
    set_key(str(ENV_PATH), "GROWW_ACCESS_TOKEN", token)


if __name__ == "__main__":
    print("=" * 55)
    print("OPTIONEX — Groww TOTP Token Generator")
    print("=" * 55)
    try:
        token = generate_totp_token()
        print(f"Token: {token[:30]}...{token[-10:]}")
        save_token_to_env(token)
        print(".env updated — GROWW_ACCESS_TOKEN saved")
    except Exception as e:
        print(f"FAILED: {e}")
        exit(1)
