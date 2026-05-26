
#!/usr/bin/env python3
"""
VIX75 Heikin Ashi Bearish Flip Alert Bot
Monitors Volatility 75 Index on H1 Heikin Ashi candles
and sends a Telegram alert when a bullish→bearish flip occurs.
"""

import asyncio
import json
import logging
from datetime import datetime
import websockets
import aiohttp

# ── Configuration ─────────────────────────────────────────
DERIV_WS_URL   = "wss://ws.binaryws.com/websockets/v3?app_id=1089"
DERIV_TOKEN    = "s7qSrLwiDnGYFLq"
SYMBOL         = "R_75"
GRANULARITY    = 3600          # 1 hour candles
CANDLE_COUNT   = 50

TELEGRAM_TOKEN = "8898798430:AAETxsMwlOQdWppKMu03TKA23zz98IzL7rc"
CHAT_ID        = "5158601624"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ── Heikin Ashi ───────────────────────────────────────────
def ha_from_raw(raw, prev_ha=None):
    o, h, l, c = raw["open"], raw["high"], raw["low"], raw["close"]
    ha_close = (o + h + l + c) / 4
    ha_open  = ((prev_ha["open"] + prev_ha["close"]) / 2) if prev_ha else ((o + c) / 2)
    ha_high  = max(h, ha_open, ha_close)
    ha_low   = min(l, ha_open, ha_close)
    return {
        "open":    ha_open,
        "close":   ha_close,
        "high":    ha_high,
        "low":     ha_low,
        "epoch":   raw["epoch"],
        "bullish": ha_close >= ha_open,
    }

def build_ha_series(candles):
    ha_list, prev = [], None
    for c in candles:
        ha = ha_from_raw(c, prev)
        ha_list.append(ha)
        prev = ha
    return ha_list

# ── Telegram ──────────────────────────────────────────────
async def send_telegram(session, text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        async with session.post(url, json=payload) as resp:
            if resp.status == 200:
                log.info("✅ Telegram alert sent")
            else:
                log.error(f"Telegram error: {resp.status} {await resp.text()}")
    except Exception as e:
        log.error(f"Telegram send failed: {e}")

async def send_startup_message(session):
    msg = (
        "🟢 <b>VIX75 Monitor Started</b>\n\n"
        "Hi Thakhie! Your alert bot is now live.\n"
        "📊 Instrument: <b>Volatility 75 Index</b>\n"
        "⏱ Timeframe: <b>H1 Heikin Ashi</b>\n"
        "🔔 You will be alerted every time a candle flips from Bullish → Bearish.\n\n"
        "Watching the market now..."
    )
    await send_telegram(session, msg)

async def send_flip_alert(session, ha_candle):
    t = datetime.utcfromtimestamp(ha_candle["epoch"]).strftime("%Y-%m-%d %H:%M UTC")
    msg = (
        "🔴 <b>BEARISH FLIP DETECTED</b>\n\n"
        "📊 <b>Volatility 75 Index</b>\n"
        "⏱ H1 Heikin Ashi\n\n"
        f"🕐 Candle Time: <b>{t}</b>\n"
        f"📂 HA Open:  <b>{ha_candle['open']:.2f}</b>\n"
        f"📉 HA Close: <b>{ha_candle['close']:.2f}</b>\n"
        f"⬆️ HA High:  <b>{ha_candle['high']:.2f}</b>\n"
        f"⬇️ HA Low:   <b>{ha_candle['low']:.2f}</b>\n\n"
        "⚠️ <i>Previous candle was BULLISH — now turned BEARISH</i>\n"
        "👉 Check your CTrader chart and manage your trade."
    )
    await send_telegram(session, msg)

# ── Main bot loop ─────────────────────────────────────────
async def run_bot():
    async with aiohttp.ClientSession() as session:
        await send_startup_message(session)

        while True:
            try:
                await monitor(session)
            except Exception as e:
                log.error(f"Connection lost: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)

async def monitor(session):
    log.info("Connecting to Deriv WebSocket...")
    async with websockets.connect(DERIV_WS_URL) as ws:
        # Authorize
        await ws.send(json.dumps({"authorize": DERIV_TOKEN}))
        resp = json.loads(await ws.recv())
        if resp.get("error"):
            raise Exception("Auth failed: " + resp["error"]["message"])
        log.info("✅ Authorized with Deriv")

        # Request candle history + subscribe
        await ws.send(json.dumps({
            "ticks_history": SYMBOL,
            "adjust_start_time": 1,
            "count": CANDLE_COUNT,
            "end": "latest",
            "granularity": GRANULARITY,
            "style": "candles",
            "subscribe": 1,
        }))

        raw_candles   = []   # closed candles (raw)
        current_raw   = None # forming candle (raw)
        last_ha       = None # last closed HA candle

        async for message in ws:
            msg = json.loads(message)

            # ── Historical candles loaded ──────────────────
            if msg.get("msg_type") == "candles":
                data = msg["candles"]
                raw_candles = [
                    {"open": float(c["open"]), "close": float(c["close"]),
                     "high": float(c["high"]),  "low":   float(c["low"]),
                     "epoch": c["epoch"]}
                    for c in data[:-1]          # exclude forming candle
                ]
                f = data[-1]
                current_raw = {
                    "open": float(f["open"]), "close": float(f["close"]),
                    "high": float(f["high"]),  "low":   float(f["low"]),
                    "epoch": f["epoch"]
                }
                ha_series = build_ha_series(raw_candles)
                last_ha   = ha_series[-1] if ha_series else None
                log.info(f"Loaded {len(raw_candles)} closed candles. Watching live...")

            # ── Live candle updates ────────────────────────
            elif msg.get("msg_type") == "ohlc":
                o = msg["ohlc"]
                new_raw = {
                    "open":  float(o["open"]),
                    "close": float(o["close"]),
                    "high":  float(o["high"]),
                    "low":   float(o["low"]),
                    "epoch": o["open_time"],
                }

                # New candle opened?
                if current_raw and new_raw["epoch"] != current_raw["epoch"]:
                    # Close out the old forming candle as HA
                    closed_ha = ha_from_raw(current_raw, last_ha)
                    log.info(
                        f"Candle closed @ {datetime.utcfromtimestamp(closed_ha['epoch']).strftime('%H:%M')} | "
                        f"HA Open={closed_ha['open']:.2f} Close={closed_ha['close']:.2f} | "
                        f"{'BULLISH' if closed_ha['bullish'] else 'BEARISH'}"
                    )

                    # Check for bearish flip
                    if last_ha and last_ha["bullish"] and not closed_ha["bullish"]:
                        log.info("🔴 BEARISH FLIP! Sending alert...")
                        await send_flip_alert(session, closed_ha)

                    # Update history
                    raw_candles = (raw_candles + [current_raw])[-CANDLE_COUNT:]
                    last_ha = closed_ha

                current_raw = new_raw

if __name__ == "__main__":
    log.info("🚀 VIX75 Heikin Ashi Alert Bot starting...")
    asyncio.run(run_bot())
