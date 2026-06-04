
#!/usr/bin/env python3
"""
VIX75 Heikin Ashi Two-Way Flip Alert Bot - Fixed
"""

import asyncio
import json
import logging
from datetime import datetime
import websockets
import aiohttp

DERIV_WS_URL   = "wss://ws.binaryws.com/websockets/v3?app_id=1089"
DERIV_TOKEN    = "s7qSrLwiDnGYFLq"
SYMBOL         = "R_75"
GRANULARITY    = 3600
CANDLE_COUNT   = 50
TELEGRAM_TOKEN = "8898798430:AAETxsMwlOQdWppKMu03TKA23zz98IzL7rc"
CHAT_ID        = "5158601624"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def ha_from_raw(raw, prev_ha=None):
    o, h, l, c = raw["open"], raw["high"], raw["low"], raw["close"]
    ha_close = (o + h + l + c) / 4
    ha_open  = ((prev_ha["open"] + prev_ha["close"]) / 2) if prev_ha else ((o + c) / 2)
    ha_high  = max(h, ha_open, ha_close)
    ha_low   = min(l, ha_open, ha_close)
    return {
        "open": ha_open, "close": ha_close,
        "high": ha_high, "low": ha_low,
        "epoch": raw["epoch"],
        "bullish": ha_close >= ha_open,
    }

def build_ha_series(candles):
    ha_list, prev = [], None
    for c in candles:
        ha = ha_from_raw(c, prev)
        ha_list.append(ha)
        prev = ha
    return ha_list

async def send_telegram(session, text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        async with session.post(url, json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"}) as resp:
            if resp.status == 200:
                log.info("✅ Telegram message sent")
            else:
                body = await resp.text()
                log.error(f"Telegram error {resp.status}: {body}")
    except Exception as e:
        log.error(f"Telegram failed: {e}")

async def send_startup(session):
    await send_telegram(session,
        "🟢 <b>VIX75 Monitor Started</b>\n\n"
        "Hi Thakhie! Bot is live.\n"
        "📊 <b>Volatility 75 Index — H1 Heikin Ashi</b>\n\n"
        "🔴 Alert on Bullish → Bearish flip\n"
        "🟢 Alert on Bearish → Bullish flip\n\n"
        "Watching the market now..."
    )

async def send_bearish(session, c):
    t = datetime.utcfromtimestamp(c["epoch"]).strftime("%Y-%m-%d %H:%M UTC")
    await send_telegram(session,
        f"🔴 <b>BEARISH FLIP DETECTED</b>\n\n"
        f"📊 Volatility 75 | H1 Heikin Ashi\n"
        f"🕐 <b>{t}</b>\n\n"
        f"Open:  <b>{c['open']:.2f}</b>\n"
        f"Close: <b>{c['close']:.2f}</b>\n"
        f"High:  <b>{c['high']:.2f}</b>\n"
        f"Low:   <b>{c['low']:.2f}</b>\n\n"
        f"⚠️ Was BULLISH — now BEARISH\n"
        f"👉 Consider a SELL on CTrader."
    )

async def send_bullish(session, c):
    t = datetime.utcfromtimestamp(c["epoch"]).strftime("%Y-%m-%d %H:%M UTC")
    await send_telegram(session,
        f"🟢 <b>BULLISH FLIP DETECTED</b>\n\n"
        f"📊 Volatility 75 | H1 Heikin Ashi\n"
        f"🕐 <b>{t}</b>\n\n"
        f"Open:  <b>{c['open']:.2f}</b>\n"
        f"Close: <b>{c['close']:.2f}</b>\n"
        f"High:  <b>{c['high']:.2f}</b>\n"
        f"Low:   <b>{c['low']:.2f}</b>\n\n"
        f"✅ Was BEARISH — now BULLISH\n"
        f"👉 Consider a BUY on CTrader."
    )

async def run_bot():
    async with aiohttp.ClientSession() as session:
        await send_startup(session)
        while True:
            try:
                await monitor(session)
            except Exception as e:
                log.error(f"Disconnected: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)

async def monitor(session):
    log.info("Connecting to Deriv...")
    async with websockets.connect(DERIV_WS_URL) as ws:
        await ws.send(json.dumps({"authorize": DERIV_TOKEN}))
        resp = json.loads(await ws.recv())
        if resp.get("error"):
            raise Exception("Auth failed: " + resp["error"]["message"])
        log.info("✅ Authorized")

        await ws.send(json.dumps({
            "ticks_history": SYMBOL,
            "adjust_start_time": 1,
            "count": CANDLE_COUNT,
            "end": "latest",
            "granularity": GRANULARITY,
            "style": "candles",
            "subscribe": 1,
        }))

        raw_candles = []
        current_raw = None
        last_ha = None

        async for message in ws:
            msg = json.loads(message)

            if msg.get("msg_type") == "candles":
                data = msg["candles"]
                raw_candles = [
                    {"open": float(c["open"]), "close": float(c["close"]),
                     "high": float(c["high"]), "low": float(c["low"]),
                     "epoch": c["epoch"]}
                    for c in data[:-1]
                ]
                f = data[-1]
                current_raw = {
                    "open": float(f["open"]), "close": float(f["close"]),
                    "high": float(f["high"]), "low": float(f["low"]),
                    "epoch": f["epoch"]
                }
                ha_series = build_ha_series(raw_candles)
                last_ha = ha_series[-1] if ha_series else None
                log.info(f"Loaded {len(raw_candles)} candles. Last HA: {'BULLISH' if last_ha and last_ha['bullish'] else 'BEARISH'}")

            elif msg.get("msg_type") == "ohlc":
                o = msg["ohlc"]
                new_raw = {
                    "open": float(o["open"]), "close": float(o["close"]),
                    "high": float(o["high"]), "low": float(o["low"]),
                    "epoch": o["open_time"],
                }

                if current_raw and new_raw["epoch"] != current_raw["epoch"]:
                    closed_ha = ha_from_raw(current_raw, last_ha)
                    log.info(
                        f"Candle closed {datetime.utcfromtimestamp(closed_ha['epoch']).strftime('%H:%M')} | "
                        f"{'BULLISH' if closed_ha['bullish'] else 'BEARISH'} | "
                        f"Prev: {'BULLISH' if last_ha and last_ha['bullish'] else 'BEARISH'}"
                    )

                    if last_ha is not None:
                        if last_ha["bullish"] and not closed_ha["bullish"]:
                            log.info("🔴 BEARISH FLIP!")
                            await send_bearish(session, closed_ha)
                        elif not last_ha["bullish"] and closed_ha["bullish"]:
                            log.info("🟢 BULLISH FLIP!")
                            await send_bullish(session, closed_ha)

                    raw_candles = (raw_candles + [current_raw])[-CANDLE_COUNT:]
                    last_ha = closed_ha

                current_raw = new_raw

if __name__ == "__main__":
    log.info("🚀 VIX75 Bot starting...")
    asyncio.run(run_bot())
