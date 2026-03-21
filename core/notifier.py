"""
OPTIONEX — Signal Notifier
Sends options trading signals to Telegram for mobile alerts.
Adapted from COMMODEX for multi-leg strategy display.
"""

import logging
import requests
import os
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")


class TelegramNotifier:
    """Sends options signal alerts to Telegram."""

    def __init__(self):
        self._token   = os.getenv("TELEGRAM_BOT_TOKEN")
        self._chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self._enabled = bool(self._token and self._chat_id)
        if not self._enabled:
            logger.warning("Telegram not configured")

    def send_signal(self, result) -> bool:
        if not self._enabled:
            return False
        if result.final_action == "HOLD":
            return False

        now = datetime.now(IST).strftime("%d %b %Y %H:%M IST")

        action_icon = {
            "BUY_PREMIUM": "🟢 BUY PREMIUM",
            "SELL_PREMIUM": "🔴 SELL PREMIUM",
            "DIRECTIONAL": "🔵 DIRECTIONAL",
            "HEDGE": "🟡 HEDGE",
        }.get(result.final_action, result.final_action)

        quality_icon = {"A": "⭐⭐⭐", "B": "⭐⭐", "C": "⭐"}.get(
            result.signal.signal_quality if result.signal else "C", "⭐"
        )

        lines = [
            f"⬡ *OPTIONEX SIGNAL*",
            f"",
            f"*{action_icon}* — {result.index}",
            f"Strategy: *{result.strategy_name}* ({result.direction})",
            f"Confidence: *{result.final_confidence}%* {quality_icon}",
            f"Time: {now}",
        ]

        if result.signal:
            lines += [
                f"",
                f"💡 *Reason*",
                f"{result.signal.primary_reason}",
                f"IV Edge: {result.signal.iv_edge}",
            ]

        # Legs
        if result.signal and result.signal.legs:
            lines += [f"", f"📋 *Legs*"]
            for i, leg in enumerate(result.signal.legs, 1):
                lines.append(
                    f"  {i}. {leg.action} {leg.option_type} {leg.strike:.0f} "
                    f"@ ₹{leg.approx_premium:.1f}"
                )

        if result.risk and result.approved:
            lines += [
                f"",
                f"🎯 *Risk Parameters*",
                f"Max Loss:  ₹{result.risk.max_loss_per_lot:,.0f}/lot",
                f"Max Profit: ₹{result.risk.max_profit_per_lot:,.0f}/lot",
                f"R:R Ratio: {result.risk.risk_reward_ratio:.1f}:1",
                f"Net θ: ₹{result.risk.net_theta_per_day:,.0f}/day",
            ]

        if result.position_sizing:
            ps = result.position_sizing
            lines += [
                f"",
                f"📐 *Position*",
                f"Lots: {ps.get('position_lots')}",
                f"Capital at risk: ₹{ps.get('actual_risk_inr'):,.0f} "
                f"({ps.get('actual_risk_pct')}%)",
            ]

        if result.sanity_warnings:
            lines += [f"", f"⚠️ *Warnings*"]
            for w in result.sanity_warnings[:2]:
                lines.append(f"• {w[:80]}...")

        lines += [
            f"",
            f"_Paper trading mode — advisory only_",
            f"_Review full analysis in OPTIONEX app_",
        ]

        return self._send("\n".join(lines))

    def send_test(self) -> bool:
        return self._send(
            "✅ *OPTIONEX Notifier Test*\n"
            "Telegram alerts are working.\n"
            "You'll receive options signals here."
        )

    def _send(self, message: str) -> bool:
        try:
            url  = f"https://api.telegram.org/bot{self._token}/sendMessage"
            data = {"chat_id": self._chat_id, "text": message, "parse_mode": "Markdown"}
            resp = requests.post(url, data=data, timeout=10)
            resp.raise_for_status()
            logger.info("Telegram notification sent")
            return True
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")
            return False
