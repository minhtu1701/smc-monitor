"""
SMC Futures Monitor - Backend
FastAPI + ccxt + smartmoneyconcepts + Telegram Bot + WebSocket

Chạy:  python main.py      (hoặc: uvicorn main:app --host 0.0.0.0 --port 8000)
Mở:    http://localhost:8000
"""
import asyncio
import json
import logging
import os
import sys
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from pathlib import Path

import ccxt.async_support as ccxt
import httpx
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# smartmoneyconcepts prints a unicode banner on import, which crashes on the
# default Windows console codepage (cp1252) before our own code ever runs.
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

# Antivirus HTTPS-scanning (e.g. Avast/Kaspersky) MITMs TLS connections with
# its own CA, which the OS trusts but certifi (used by aiohttp/httpx) does
# not — this makes API calls fail with CERTIFICATE_VERIFY_FAILED. Falling
# back to the OS trust store fixes it transparently when available.
try:
    import truststore

    truststore.inject_into_ssl()
except ImportError:
    pass

from smartmoneyconcepts import smc

# ─────────────────────────── Config (.env) ───────────────────────────
SYMBOLS = [s.strip().upper() for s in os.getenv(
    "SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,DOGEUSDT,ADAUSDT,LINKUSDT"
).split(",") if s.strip()]
TIMEFRAMES = [t.strip() for t in os.getenv("TIMEFRAMES", "15m,1h").split(",") if t.strip()]
SWING_LENGTH = int(os.getenv("SWING_LENGTH", "10"))     # độ dài swing cho smc.swing_highs_lows
CANDLE_LIMIT = int(os.getenv("CANDLE_LIMIT", "500"))    # số nến tải mỗi lần quét
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "60"))   # giây giữa 2 lần quét
KLINE_POLL_INTERVAL = float(os.getenv("KLINE_POLL_INTERVAL", "2"))  # giây giữa 2 lần đẩy nến live qua /ws
FRESH_BARS = int(os.getenv("FRESH_BARS", "2"))          # chỉ báo tín hiệu có BoS trong N nến đóng gần nhất
MAX_GAP_BARS = int(os.getenv("MAX_GAP_BARS", "50"))     # khoảng cách tối đa (nến) giữa CHoCH và BoS
CONCURRENCY = int(os.getenv("CONCURRENCY", "8"))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Accumulation screener — tìm coin giá thấp đang "siết chặt" biến động (candidate tích lũy)
SCREENER_MAX_PRICE = float(os.getenv("SCREENER_MAX_PRICE", "0.1"))
SCREENER_MIN_QUOTE_VOLUME = float(os.getenv("SCREENER_MIN_QUOTE_VOLUME", "1000000"))
SCREENER_INTERVAL = int(os.getenv("SCREENER_INTERVAL", "3600"))  # 1 giờ — tín hiệu intraday cần quét thường xuyên
SCREENER_TOP_N = int(os.getenv("SCREENER_TOP_N", "25"))
SCREENER_INTRADAY_TIMEFRAME = os.getenv("SCREENER_INTRADAY_TIMEFRAME", "1h")
SCREENER_INTRADAY_LOOKBACK = int(os.getenv("SCREENER_INTRADAY_LOOKBACK", "48"))  # số nến 1h nhìn lại (~2 ngày)
SCREENER_DAILY_WEIGHT = float(os.getenv("SCREENER_DAILY_WEIGHT", "0.55"))

# Scalp/Swing strategy engine — entry theo vùng Order Block (H4/D1 bias) + lọc VWAP,
# đã kiểm chứng qua backtest (xem lịch sử chat) là tổ hợp có edge thật, RR ép cứng.
SCALP_RR = float(os.getenv("SCALP_RR", "2"))
SWING_RR = float(os.getenv("SWING_RR", "3"))
SCALP_INTERVAL = int(os.getenv("SCALP_INTERVAL", "45"))
SWING_INTERVAL = int(os.getenv("SWING_INTERVAL", "240"))
STRAT_SL_BUFFER = float(os.getenv("STRAT_SL_BUFFER", "0.0015"))
STRAT_MAX_RISK_PCT = float(os.getenv("STRAT_MAX_RISK_PCT", "0.08"))

# M1 Entry — thuần price action (không SMC zone, không VWAP): 1H bias -> 5M break-of-structure
# cùng hướng -> chờ giá hồi đúng mức đó -> entry bằng nến xác nhận (engulfing/wick dài) trên M1.
M1_RR = float(os.getenv("M1_RR", "2"))
M1_INTERVAL = int(os.getenv("M1_INTERVAL", "30"))
M1_SWING_1H = int(os.getenv("M1_SWING_1H", "8"))
M1_SWING_5M = int(os.getenv("M1_SWING_5M", "6"))
M1_RETEST_WINDOW_MIN = int(os.getenv("M1_RETEST_WINDOW_MIN", "300"))
M1_RETEST_TOLERANCE = float(os.getenv("M1_RETEST_TOLERANCE", "0.0015"))
M1_SL_BUFFER = float(os.getenv("M1_SL_BUFFER", "0.0015"))

ALLOWED_TIMEFRAMES = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"}
TF_SECONDS = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600,
              "2h": 7200, "4h": 14400, "6h": 21600, "12h": 43200, "1d": 86400}
INDEX_FILE = Path(__file__).parent / "index.html"
STRATEGY_STATE_FILE = Path(__file__).parent / "strategy_state.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("smc")

# ─────────────────────────── State ───────────────────────────
exchange: ccxt.binanceusdm | None = None
kline_exchange: ccxt.binanceusdm | None = None  # instance riêng, tránh bị nghẽn rate-limit chung với scanner/screener
http: httpx.AsyncClient | None = None
active_symbols: list[str] = list(SYMBOLS)
history: deque = deque(maxlen=200)   # tín hiệu gần nhất (mới nhất đứng đầu)
seen: set[str] = set()               # chống gửi trùng
status = {"last_scan": None, "duration": None, "scans": 0}
screener_state: dict = {
    "updated_at": None, "scanned": 0, "results": [],
    "max_price": SCREENER_MAX_PRICE, "interval_s": SCREENER_INTERVAL,
    "intraday_lookback_h": SCREENER_INTRADAY_LOOKBACK,
}
strategy_enabled: dict = {"scalp": True, "swing": True, "m1": True}
strategy_signals: dict[str, deque] = {"scalp": deque(maxlen=100), "swing": deque(maxlen=100), "m1": deque(maxlen=100)}
strategy_seen: set[str] = set()


def save_strategy_state():
    """Lưu tín hiệu Scalp/Swing/M1 ra file — không thì mỗi lần restart server (deploy fix...)
    lại mất hết lịch sử lời/lỗ, vô lý với tính năng Log."""
    try:
        data = {k: list(v) for k, v in strategy_signals.items()}
        STRATEGY_STATE_FILE.write_text(json.dumps(data), encoding="utf-8")
    except Exception:
        log.exception("Lưu %s lỗi", STRATEGY_STATE_FILE.name)


def load_strategy_state():
    if not STRATEGY_STATE_FILE.exists():
        return
    try:
        data = json.loads(STRATEGY_STATE_FILE.read_text(encoding="utf-8"))
        for k, v in data.items():
            if k in strategy_signals:
                strategy_signals[k] = deque(v, maxlen=100)
                strategy_seen.update(sig["id"] for sig in v)
        log.info("Đã nạp lại %d tín hiệu strategy từ %s", sum(len(v) for v in data.values()), STRATEGY_STATE_FILE.name)
    except Exception:
        log.exception("Đọc %s lỗi", STRATEGY_STATE_FILE.name)


class ConnectionManager:
    def __init__(self):
        self.active: set[WebSocket] = set()
        self.watches: dict[WebSocket, tuple[str, str]] = {}

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.add(ws)

    def disconnect(self, ws: WebSocket):
        self.active.discard(ws)
        self.watches.pop(ws, None)

    def set_watch(self, ws: WebSocket, symbol: str, timeframe: str):
        self.watches[ws] = (symbol.upper(), timeframe)

    def watched_pairs(self) -> set[tuple[str, str]]:
        return set(self.watches.values())

    async def send_to_watchers(self, symbol: str, timeframe: str, message: dict):
        dead = []
        for ws, pair in list(self.watches.items()):
            if pair != (symbol, timeframe):
                continue
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

    async def broadcast(self, message: dict):
        dead = []
        for ws in list(self.active):
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()


def status_payload() -> dict:
    return {
        **status,
        "symbols": active_symbols,
        "timeframes": TIMEFRAMES,
        "telegram": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
        "clients": len(manager.active),
        "strategy_enabled": dict(strategy_enabled),
    }


# ─────────────────────────── Market data ───────────────────────────
def to_ccxt_symbol(sym: str) -> str:
    """BTCUSDT -> BTC/USDT:USDT (định dạng perpetual của ccxt)."""
    sym = sym.upper().replace("/", "").split(":")[0]
    for quote in ("USDT", "USDC"):
        if sym.endswith(quote) and len(sym) > len(quote):
            return f"{sym[:-len(quote)]}/{quote}:{quote}"
    raise ValueError(f"Symbol không hợp lệ: {sym}")


async def fetch_ohlc(symbol: str, timeframe: str, limit: int = CANDLE_LIMIT, ex: "ccxt.binanceusdm | None" = None) -> pd.DataFrame:
    raw = await (ex or exchange).fetch_ohlcv(to_ccxt_symbol(symbol), timeframe, limit=limit)
    df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = (df["ts"] // 1000).astype(int)  # giây (Lightweight Charts dùng UNIX seconds)
    return df


# ─────────────────────────── SMC logic ───────────────────────────
def analyze(df: pd.DataFrame) -> list[dict]:
    """Trả về danh sách sự kiện cấu trúc (CHoCH / BoS) theo thứ tự thời điểm bị phá vỡ.
    df chỉ nên chứa nến ĐÃ ĐÓNG."""
    ohlc = df[["open", "high", "low", "close", "volume"]].reset_index(drop=True)
    swings = smc.swing_highs_lows(ohlc, swing_length=SWING_LENGTH)
    struct = smc.bos_choch(ohlc, swings, close_break=True)

    events = []
    for i, row in struct.iterrows():
        if pd.notna(row["CHOCH"]) and row["CHOCH"] != 0:
            kind, direction = "CHoCH", int(row["CHOCH"])
        elif pd.notna(row["BOS"]) and row["BOS"] != 0:
            kind, direction = "BoS", int(row["BOS"])
        else:
            continue
        if pd.isna(row["BrokenIndex"]):
            continue
        broken = int(row["BrokenIndex"])
        if broken >= len(df):
            continue
        events.append({
            "type": kind,
            "dir": direction,                       # 1 = bullish, -1 = bearish
            "level": float(row["Level"]),
            "swing_idx": int(i),
            "broken_idx": broken,
            "swing_time": int(df["ts"].iloc[i]),   # thời điểm swing (điểm bắt đầu đường level)
            "time": int(df["ts"].iloc[broken]),    # thời điểm nến phá vỡ (xác nhận)
        })
    events.sort(key=lambda e: (e["broken_idx"], e["swing_idx"]))
    return events


def find_signals(events: list[dict]) -> list[tuple[dict, dict]]:
    """Tín hiệu = CHoCH ngay sau đó là BoS (liền kề) cùng hướng."""
    out = []
    for a, b in zip(events, events[1:]):
        if (a["type"] == "CHoCH" and b["type"] == "BoS" and a["dir"] == b["dir"]
                and b["broken_idx"] - a["broken_idx"] <= MAX_GAP_BARS):
            out.append((a, b))
    return out


def build_signal(symbol: str, timeframe: str, choch: dict, bos: dict, df: pd.DataFrame) -> dict:
    return {
        "id": f"{symbol}-{timeframe}-{bos['time']}-{bos['dir']}",
        "symbol": symbol,
        "timeframe": timeframe,
        "direction": "bullish" if bos["dir"] > 0 else "bearish",
        "choch_level": choch["level"],
        "choch_time": choch["time"],
        "bos_level": bos["level"],
        "bos_time": bos["time"],
        "price": float(df["close"].iloc[bos["broken_idx"]]),
        "detected_at": int(time.time()),
    }


# ─────────────────────────── Accumulation screener ───────────────────────────
def _zscore_slope(series: pd.Series) -> float:
    """Độ dốc (xu hướng) của chuỗi sau khi chuẩn hoá z-score, để so sánh giữa các đại lượng khác đơn vị (giá vs OBV)."""
    y = series.to_numpy(dtype=float)
    std = y.std()
    if len(y) < 3 or std == 0:
        return 0.0
    y = (y - y.mean()) / std
    return float(np.polyfit(np.arange(len(y)), y, 1)[0])


def squeeze_metrics(df: pd.DataFrame) -> dict | None:
    """Đánh giá mức độ 'siết chặt' biến động (dấu hiệu tích lũy) từ nến ngày đã đóng.
    Điểm cao = biên độ/ATR đang thấp hơn hẳn so với chính lịch sử của nó, volume cạn dần,
    và OBV vẫn tăng trong lúc giá đi ngang (dòng tiền âm thầm gom hàng)."""
    d = df.iloc[:-1].reset_index(drop=True)  # bỏ nến ngày đang chạy
    if len(d) < 120:
        return None
    close, high, low, vol = d["close"], d["high"], d["low"], d["volume"]

    sma20, std20 = close.rolling(20).mean(), close.rolling(20).std()
    bb_width = (4 * std20) / sma20

    prev_close = close.shift(1)
    tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    atr_pct = tr.rolling(14).mean() / close

    lookback = min(180, len(d))
    bb_pctile = bb_width.tail(lookback).rank(pct=True).iloc[-1]
    atr_pctile = atr_pct.tail(lookback).rank(pct=True).iloc[-1]
    if pd.isna(bb_pctile) or pd.isna(atr_pctile):
        return None

    vol_base = vol.tail(30).mean()
    vol_ratio = (vol.tail(7).mean() / vol_base) if vol_base else 1.0

    obv = (np.sign(close.diff().fillna(0)) * vol).cumsum()
    window = min(30, len(d) - 1)
    obv_div = max(0.0, min(1.0, (_zscore_slope(obv.tail(window)) - _zscore_slope(close.tail(window))) / 2 + 0.5))

    score = ((1 - bb_pctile) * 35 + (1 - atr_pctile) * 25
             + max(0.0, 1 - min(vol_ratio, 1.5)) * 20 + obv_div * 20)

    tight = close.tail(lookback)
    band = tight.mean() * 0.15
    days_in_range = int((tight.sub(tight.mean()).abs() <= band).sum()) if band else 0

    return {
        "bb_pctile": round(float(bb_pctile) * 100, 1),
        "atr_pctile": round(float(atr_pctile) * 100, 1),
        "vol_ratio": round(float(vol_ratio), 2),
        "obv_div": round(float(obv_div) * 100, 1),
        "days_in_range": days_in_range,
        "daily_score": round(float(score), 1),
    }


def intraday_signal(df: pd.DataFrame) -> dict | None:
    """Tín hiệu gom hàng NGẮN HẠN (vài giờ - vài ngày, nến 1h): giá đi ngang trong khi
    volume + OBV bắt đầu tăng lên — dấu hiệu whale hấp thụ nguồn cung trước khi đẩy giá."""
    d = df.iloc[:-1].reset_index(drop=True)  # bỏ nến giờ đang chạy
    window = min(SCREENER_INTRADAY_LOOKBACK, len(d) - 1)
    if window < 24:
        return None
    close, vol = d["close"].tail(window + 1), d["volume"].tail(window + 1)

    price_range_pct = (close.max() - close.min()) / close.mean()
    flatness = max(0.0, min(1.0, 1 - price_range_pct / 0.12))  # càng phẳng (<12%) điểm càng cao

    vol_base = vol.mean()
    vol_pickup = (vol.tail(6).mean() / vol_base) if vol_base else 1.0
    pickup_score = max(0.0, min(1.0, (vol_pickup - 1) / 1.5))

    obv = (np.sign(close.diff().fillna(0)) * vol).cumsum()
    obv_div = max(0.0, min(1.0, (_zscore_slope(obv) - _zscore_slope(close)) / 2 + 0.5))

    score = flatness * 40 + pickup_score * 30 + obv_div * 30
    return {
        "intraday_flat_pct": round(float(price_range_pct) * 100, 1),
        "intraday_vol_pickup": round(float(vol_pickup), 2),
        "intraday_obv_div": round(float(obv_div) * 100, 1),
        "intraday_score": round(float(score), 1),
    }


async def screener_scan():
    try:
        tickers = await exchange.fetch_tickers()
    except Exception as e:
        log.warning("Screener: không lấy được tickers (%s)", e)
        return

    candidates = []
    for msym, market in exchange.markets.items():
        if not market.get("swap") or market.get("quote") != "USDT" or not market.get("active"):
            continue
        t = tickers.get(msym)
        price = (t or {}).get("last")
        qvol = (t or {}).get("quoteVolume") or 0
        if not price or price <= 0 or price >= SCREENER_MAX_PRICE or qvol < SCREENER_MIN_QUOTE_VOLUME:
            continue
        candidates.append((market["base"] + market["quote"], price, qvol))

    sem = asyncio.Semaphore(CONCURRENCY)

    async def one(sym: str, price: float, qvol: float) -> dict | None:
        async with sem:
            try:
                daily_df, hourly_df = await asyncio.gather(
                    fetch_ohlc(sym, "1d", 220),
                    fetch_ohlc(sym, SCREENER_INTRADAY_TIMEFRAME, SCREENER_INTRADAY_LOOKBACK + 5),
                )
            except Exception:
                return None
        daily_m = await asyncio.to_thread(squeeze_metrics, daily_df)
        if not daily_m:
            return None
        intraday_m = await asyncio.to_thread(intraday_signal, hourly_df)
        intraday_score = intraday_m["intraday_score"] if intraday_m else 0.0
        score = round(daily_m["daily_score"] * SCREENER_DAILY_WEIGHT + intraday_score * (1 - SCREENER_DAILY_WEIGHT), 1)
        return {
            "symbol": sym, "price": price, "quote_volume_24h": round(qvol), "score": score,
            **daily_m, **(intraday_m or {}),
        }

    results = await asyncio.gather(*(one(s, p, v) for s, p, v in candidates), return_exceptions=True)
    rows = sorted((r for r in results if isinstance(r, dict)), key=lambda r: r["score"], reverse=True)
    screener_state.update(updated_at=int(time.time()), scanned=len(candidates), results=rows[:SCREENER_TOP_N])
    log.info("Screener: %d/%d coin < $%.4g đủ điều kiện quét, top score %.1f",
              len(rows), len(candidates), SCREENER_MAX_PRICE, rows[0]["score"] if rows else 0)


async def screener_loop():
    while True:
        t0 = time.time()
        try:
            await screener_scan()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Screener loop lỗi")
        await asyncio.sleep(max(30.0, SCREENER_INTERVAL - (time.time() - t0)))


# ─────────────────────────── Telegram ───────────────────────────
def fmt_price(x: float) -> str:
    return f"{x:,.2f}" if x >= 100 else f"{x:.6g}"


def format_telegram(sig: dict) -> str:
    bull = sig["direction"] == "bullish"
    ts = datetime.fromtimestamp(sig["bos_time"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    test = " [TEST]" if sig.get("test") else ""
    return (
        f"{'🟢' if bull else '🔴'} <b>{sig['direction'].upper()} · CHoCH + BoS</b>{test}\n"
        f"<b>{sig['symbol']}</b> · {sig['timeframe']} · Binance Futures\n\n"
        f"CHoCH: <code>{fmt_price(sig['choch_level'])}</code>\n"
        f"BoS:   <code>{fmt_price(sig['bos_level'])}</code>\n"
        f"Close: <code>{fmt_price(sig['price'])}</code>\n"
        f"🕒 {ts}\n"
        f'<a href="https://www.binance.com/en/futures/{sig["symbol"]}">Mở trên Binance</a>'
    )


async def send_telegram(text: str):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return
    try:
        r = await http.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=10,
        )
        if r.status_code != 200:
            log.warning("Telegram lỗi %s: %s", r.status_code, r.text[:200])
    except Exception as e:
        log.warning("Telegram exception: %s", e)


async def emit_signal(sig: dict):
    history.appendleft(sig)
    log.info("SIGNAL %s %s %s @ %s", sig["symbol"], sig["timeframe"], sig["direction"], sig["price"])
    await manager.broadcast({"type": "signal", "data": sig})
    await send_telegram(format_telegram(sig))


# ─────────────────────────── Scanner ───────────────────────────
async def scan_symbol(symbol: str, timeframe: str, sem: asyncio.Semaphore):
    async with sem:
        try:
            df = await fetch_ohlc(symbol, timeframe)
        except Exception as e:
            log.warning("Fetch %s %s lỗi: %s", symbol, timeframe, e)
            return

    closed = df.iloc[:-1].reset_index(drop=True)  # bỏ nến đang chạy -> tránh repaint
    if len(closed) < SWING_LENGTH * 3:
        return
    try:
        events = await asyncio.to_thread(analyze, closed)
    except Exception:
        log.exception("Analyze %s %s lỗi", symbol, timeframe)
        return

    last_idx = len(closed) - 1
    for choch, bos in find_signals(events):
        if bos["broken_idx"] <= last_idx - FRESH_BARS:   # tín hiệu cũ
            continue
        sig = build_signal(symbol, timeframe, choch, bos, closed)
        if sig["id"] in seen:
            continue
        seen.add(sig["id"])
        await emit_signal(sig)


async def scanner_loop():
    sem = asyncio.Semaphore(CONCURRENCY)
    while True:
        t0 = time.time()
        try:
            await asyncio.gather(
                *(scan_symbol(s, tf, sem) for s in active_symbols for tf in TIMEFRAMES),
                return_exceptions=True,
            )
            status.update(last_scan=int(time.time()), duration=round(time.time() - t0, 2),
                          scans=status["scans"] + 1)
            await manager.broadcast({"type": "status", "data": status_payload()})
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Scanner loop lỗi")
        await asyncio.sleep(max(1.0, SCAN_INTERVAL - (time.time() - t0)))


async def kline_relay_loop():
    """Đẩy nến mới nhất cho các client đang xem qua /ws — dự phòng cho trường hợp
    trình duyệt không tự kết nối được thẳng tới WebSocket công khai của Binance
    (một số mạng/ISP chặn ngầm luồng dữ liệu dù bắt tay WebSocket vẫn "thành công")."""
    while True:
        try:
            for symbol, timeframe in manager.watched_pairs():
                try:
                    df = await fetch_ohlc(symbol, timeframe, limit=3, ex=kline_exchange)
                except Exception:
                    continue
                candles = df[["ts", "open", "high", "low", "close"]].rename(columns={"ts": "time"}).to_dict("records")
                if not candles:
                    continue
                await manager.send_to_watchers(symbol, timeframe, {
                    "type": "kline", "symbol": symbol, "timeframe": timeframe, "candles": candles[-2:],
                })
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Kline relay loop lỗi")
        await asyncio.sleep(KLINE_POLL_INTERVAL)


# ─────────────────────────── Scalp/Swing strategy engine ───────────────────────────
def strat_candle_patterns(df: pd.DataFrame) -> pd.DataFrame:
    """Nến trigger: Bullish/Bearish Engulfing, Pin bar, CISD (Change in State of Delivery)."""
    o, h, l, c = df["open"], df["high"], df["low"], df["close"]
    body = (c - o).abs()
    rng = (h - l).replace(0, np.nan)
    upper_wick = h - pd.concat([o, c], axis=1).max(axis=1)
    lower_wick = pd.concat([o, c], axis=1).min(axis=1) - l

    bull_engulf = (c > o) & (c.shift(1) < o.shift(1)) & (o <= c.shift(1)) & (c >= o.shift(1))
    bear_engulf = (c < o) & (c.shift(1) > o.shift(1)) & (o >= c.shift(1)) & (c <= o.shift(1))
    bull_pin = (lower_wick >= 2 * body) & (upper_wick <= body * 0.6) & ((c - l) / rng >= 0.6)
    bear_pin = (upper_wick >= 2 * body) & (lower_wick <= body * 0.6) & ((h - c) / rng >= 0.6)

    is_down, is_up = c < o, c > o
    bull_cisd = pd.Series(False, index=df.index)
    bear_cisd = pd.Series(False, index=df.index)
    run_open_down, run_len_down, run_open_up, run_len_up = None, 0, None, 0
    for i in range(1, len(df)):
        if is_down.iloc[i - 1]:
            if run_len_down == 0:
                run_open_down = o.iloc[i - 1]
            run_len_down += 1
        else:
            run_len_down = 0
        if is_up.iloc[i - 1]:
            if run_len_up == 0:
                run_open_up = o.iloc[i - 1]
            run_len_up += 1
        else:
            run_len_up = 0
        if run_len_down >= 2 and is_up.iloc[i] and c.iloc[i] > run_open_down:
            bull_cisd.iloc[i] = True
        if run_len_up >= 2 and is_down.iloc[i] and c.iloc[i] < run_open_up:
            bear_cisd.iloc[i] = True

    return pd.DataFrame({"bull_trigger": bull_engulf | bull_pin | bull_cisd,
                          "bear_trigger": bear_engulf | bear_pin | bear_cisd})


def strat_bias_series(ohlc: pd.DataFrame, swing_length: int) -> pd.Series:
    swings = smc.swing_highs_lows(ohlc, swing_length=swing_length)
    struct = smc.bos_choch(ohlc, swings, close_break=True)
    return struct["BOS"].fillna(struct["CHOCH"]).ffill()


def strat_build_zones(ohlc: pd.DataFrame, swing_length: int) -> pd.DataFrame:
    """FVG + Order Block, gộp thành 1 bảng zone chung (kind/dir/top/bottom/formed_idx/mitigated_idx)."""
    swings = smc.swing_highs_lows(ohlc, swing_length=swing_length)
    fvg = smc.fvg(ohlc, join_consecutive=False)
    ob = smc.ob(ohlc, swings, close_mitigation=False)
    rows = []
    for kind, data, col in (("FVG", fvg, "FVG"), ("OB", ob, "OB")):
        for idx, row in data.dropna(subset=[col]).iterrows():
            rows.append({"kind": kind, "dir": int(row[col]), "top": float(row["Top"]), "bottom": float(row["Bottom"]),
                         "formed_idx": int(idx), "mitigated_idx": int(row["MitigatedIndex"]) if row["MitigatedIndex"] else 0})
    return pd.DataFrame(rows, columns=["kind", "dir", "top", "bottom", "formed_idx", "mitigated_idx"])


def strat_active_zones(zones: pd.DataFrame, as_of_idx: int, direction: int) -> pd.DataFrame:
    if zones.empty:
        return zones
    return zones[(zones["dir"] == direction) & (zones["formed_idx"] <= as_of_idx)
                 & ((zones["mitigated_idx"] == 0) | (zones["mitigated_idx"] > as_of_idx))]


def strat_overlaps(top1, bottom1, top2, bottom2) -> bool:
    return not (top1 < bottom2 or top2 < bottom1)


def strat_session_vwap(df: pd.DataFrame) -> pd.Series:
    tp = (df["high"] + df["low"] + df["close"]) / 3
    day = df["ts"] // 86400
    pv = (tp * df["volume"]).groupby(day).cumsum()
    v = df["volume"].groupby(day).cumsum()
    return pv / v.replace(0, np.nan)


def strat_rolling_vwap(df: pd.DataFrame, window: int) -> pd.Series:
    tp = (df["high"] + df["low"] + df["close"]) / 3
    pv = (tp * df["volume"]).rolling(window).sum()
    v = df["volume"].rolling(window).sum()
    return pv / v.replace(0, np.nan)


def check_scalp_signal(symbol: str, h4: pd.DataFrame, m15: pd.DataFrame, m5: pd.DataFrame) -> dict | None:
    """H4 bias -> M15 OB/FVG confluence với H4 -> trigger nến M5, lọc thêm VWAP phiên (chiết khấu/premium)."""
    m5c = m5.iloc[:-1].reset_index(drop=True)  # bỏ nến đang chạy
    if len(m5c) < 30 or len(h4) < 30 or len(m15) < 30:
        return None
    h4_ohlc, m15_ohlc = h4[["open", "high", "low", "close", "volume"]], m15[["open", "high", "low", "close", "volume"]]

    bias = strat_bias_series(h4_ohlc, 8)
    if pd.isna(bias.iloc[-1]) or bias.iloc[-1] == 0:
        return None
    direction = int(bias.iloc[-1])

    i = len(m5c) - 1
    lo, hi = float(m5c["low"].iloc[i]), float(m5c["high"].iloc[i])
    h4_active = strat_active_zones(strat_build_zones(h4_ohlc, 8), len(h4_ohlc) - 1, direction)
    if h4_active.empty:
        return None
    h4_touch = h4_active[h4_active.apply(lambda z: strat_overlaps(hi, lo, z["top"], z["bottom"]), axis=1)]
    if h4_touch.empty:
        return None

    m15_active = strat_active_zones(strat_build_zones(m15_ohlc, 10), len(m15_ohlc) - 1, direction)
    if m15_active.empty:
        return None
    confluent = m15_active[m15_active.apply(
        lambda z: any(strat_overlaps(z["top"], z["bottom"], hz["top"], hz["bottom"]) for _, hz in h4_touch.iterrows()),
        axis=1)]
    confluent = confluent[confluent.apply(lambda z: strat_overlaps(hi, lo, z["top"], z["bottom"]), axis=1)]
    if confluent.empty:
        return None

    patt = strat_candle_patterns(m5c)
    if not (patt["bull_trigger"].iloc[i] if direction > 0 else patt["bear_trigger"].iloc[i]):
        return None

    vwap = strat_session_vwap(m5c)
    entry = float(m5c["close"].iloc[i])
    if pd.isna(vwap.iloc[i]):
        return None
    if direction > 0 and entry >= vwap.iloc[i]:
        return None
    if direction < 0 and entry <= vwap.iloc[i]:
        return None

    zone = confluent.iloc[0]
    sl = zone["bottom"] * (1 - STRAT_SL_BUFFER) if direction > 0 else zone["top"] * (1 + STRAT_SL_BUFFER)
    risk = abs(entry - sl)
    if risk <= 0 or risk / entry > STRAT_MAX_RISK_PCT:
        return None
    tp = entry + SCALP_RR * risk if direction > 0 else entry - SCALP_RR * risk
    entry_time = int(m5c["ts"].iloc[i]) + TF_SECONDS["5m"]  # giờ ĐÓNG nến trigger = lúc entry thực sự xác nhận
    status, exit_time, exit_price = resolve_outcome(m5, entry_time, sl, tp, direction > 0)
    return {
        "id": f"scalp-{symbol}-{entry_time}-{direction}", "system": "scalp", "symbol": symbol, "timeframe": "5m",
        "direction": "bullish" if direction > 0 else "bearish", "zone_kind": zone["kind"],
        "entry": entry, "sl": sl, "tp": tp, "rr": SCALP_RR, "entry_time": entry_time,
        "detected_at": int(time.time()), "status": status,
        "closed_at": int(time.time()) if status != "open" else None,
        "exit_time": exit_time, "exit_price": exit_price,
    }


def check_swing_signal(symbol: str, d1: pd.DataFrame, h4: pd.DataFrame) -> dict | None:
    """Daily bias -> H4 OB/FVG confluence -> trigger nến H4, lọc VWAP rolling 20 ngày."""
    if len(d1) < 30 or len(h4) < 150:
        return None
    d1_ohlc, h4_ohlc = d1[["open", "high", "low", "close", "volume"]], h4[["open", "high", "low", "close", "volume"]]

    bias = strat_bias_series(d1_ohlc, 8)
    if pd.isna(bias.iloc[-1]) or bias.iloc[-1] == 0:
        return None
    direction = int(bias.iloc[-1])

    i = len(h4) - 2  # bỏ nến H4 đang chạy
    if i < 30:
        return None
    lo, hi = float(h4["low"].iloc[i]), float(h4["high"].iloc[i])
    active = strat_active_zones(strat_build_zones(h4_ohlc, 10), i, direction)
    if active.empty:
        return None
    touch = active[active.apply(lambda z: strat_overlaps(hi, lo, z["top"], z["bottom"]), axis=1)]
    if touch.empty:
        return None

    patt = strat_candle_patterns(h4.iloc[:i + 1])
    if not (patt["bull_trigger"].iloc[i] if direction > 0 else patt["bear_trigger"].iloc[i]):
        return None

    vwap = strat_rolling_vwap(h4, window=20 * 6)
    entry = float(h4["close"].iloc[i])
    if pd.isna(vwap.iloc[i]):
        return None
    if direction > 0 and entry >= vwap.iloc[i]:
        return None
    if direction < 0 and entry <= vwap.iloc[i]:
        return None

    zone = touch.iloc[0]
    sl = zone["bottom"] * (1 - STRAT_SL_BUFFER) if direction > 0 else zone["top"] * (1 + STRAT_SL_BUFFER)
    risk = abs(entry - sl)
    if risk <= 0 or risk / entry > STRAT_MAX_RISK_PCT * 2:
        return None
    tp = entry + SWING_RR * risk if direction > 0 else entry - SWING_RR * risk
    entry_time = int(h4["ts"].iloc[i]) + TF_SECONDS["4h"]  # giờ ĐÓNG nến trigger = lúc entry thực sự xác nhận
    status, exit_time, exit_price = resolve_outcome(h4, entry_time, sl, tp, direction > 0)
    return {
        "id": f"swing-{symbol}-{entry_time}-{direction}", "system": "swing", "symbol": symbol, "timeframe": "4h",
        "direction": "bullish" if direction > 0 else "bearish", "zone_kind": zone["kind"],
        "entry": entry, "sl": sl, "tp": tp, "rr": SWING_RR, "entry_time": entry_time,
        "detected_at": int(time.time()), "status": status,
        "closed_at": int(time.time()) if status != "open" else None,
        "exit_time": exit_time, "exit_price": exit_price,
    }


def strat_struct_events(df: pd.DataFrame, swing_length: int) -> list[dict]:
    """BoS/CHoCH thuần price-action (level + broken_idx), dùng cho hệ thống M1 Entry —
    không phụ thuộc SWING_LENGTH toàn cục vì cần swing_length riêng cho từng khung."""
    ohlc = df[["open", "high", "low", "close", "volume"]].reset_index(drop=True)
    swings = smc.swing_highs_lows(ohlc, swing_length=swing_length)
    struct = smc.bos_choch(ohlc, swings, close_break=True)
    events = []
    for i, row in struct.iterrows():
        if pd.notna(row["CHOCH"]) and row["CHOCH"] != 0:
            direction = int(row["CHOCH"])
        elif pd.notna(row["BOS"]) and row["BOS"] != 0:
            direction = int(row["BOS"])
        else:
            continue
        if pd.isna(row["BrokenIndex"]):
            continue
        broken = int(row["BrokenIndex"])
        if broken >= len(df):
            continue
        events.append({"dir": direction, "level": float(row["Level"]), "broken_idx": broken})
    events.sort(key=lambda e: e["broken_idx"])
    return events


def check_m1_signal(symbol: str, h1: pd.DataFrame, m5: pd.DataFrame, m1: pd.DataFrame) -> dict | None:
    """1H bias -> 5M break-of-structure cùng hướng -> chờ giá hồi đúng mức đó trong
    M1_RETEST_WINDOW_MIN phút -> entry bằng nến xác nhận (engulfing/wick dài) trên M1.
    SL = ngay ngoài wick nến đó — không dùng SMC zone/VWAP, thuần price action."""
    h1_ohlc = h1[["open", "high", "low", "close", "volume"]]
    bias = strat_bias_series(h1_ohlc, M1_SWING_1H)
    if pd.isna(bias.iloc[-1]) or bias.iloc[-1] == 0:
        return None
    direction = int(bias.iloc[-1])

    m5c = m5.iloc[:-1].reset_index(drop=True)
    if len(m5c) < 30:
        return None
    events = [e for e in strat_struct_events(m5c, M1_SWING_5M) if e["dir"] == direction]
    if not events:
        return None
    ev = events[-1]
    broken_time = int(m5c["ts"].iloc[ev["broken_idx"]])
    level = ev["level"]

    m1c = m1.iloc[:-1].reset_index(drop=True)
    if len(m1c) < 30:
        return None
    i = len(m1c) - 1
    now_time = int(m1c["ts"].iloc[i])
    if now_time <= broken_time or now_time > broken_time + M1_RETEST_WINDOW_MIN * 60:
        return None  # ngoài cửa sổ retest hợp lệ (chưa tới hoặc đã hết hạn)

    hi, lo = float(m1c["high"].iloc[i]), float(m1c["low"].iloc[i])
    band_lo, band_hi = level * (1 - M1_RETEST_TOLERANCE), level * (1 + M1_RETEST_TOLERANCE)
    if hi < band_lo or lo > band_hi:
        return None

    patt = strat_candle_patterns(m1c)
    if not (patt["bull_trigger"].iloc[i] if direction > 0 else patt["bear_trigger"].iloc[i]):
        return None

    entry = float(m1c["close"].iloc[i])
    sl = float(m1c["low"].iloc[i]) * (1 - M1_SL_BUFFER) if direction > 0 else float(m1c["high"].iloc[i]) * (1 + M1_SL_BUFFER)
    risk = abs(entry - sl)
    if risk <= 0 or risk / entry > STRAT_MAX_RISK_PCT:
        return None
    tp = entry + M1_RR * risk if direction > 0 else entry - M1_RR * risk
    entry_time = now_time + TF_SECONDS["1m"]  # giờ ĐÓNG nến M1 trigger = lúc entry thực sự xác nhận
    status, exit_time, exit_price = resolve_outcome(m1, entry_time, sl, tp, direction > 0)
    return {
        "id": f"m1-{symbol}-{entry_time}-{direction}", "system": "m1", "symbol": symbol, "timeframe": "1m",
        "direction": "bullish" if direction > 0 else "bearish", "zone_kind": "BOS",
        "entry": entry, "sl": sl, "tp": tp, "rr": M1_RR, "entry_time": entry_time,
        "detected_at": int(time.time()), "status": status,
        "closed_at": int(time.time()) if status != "open" else None,
        "exit_time": exit_time, "exit_price": exit_price,
    }


def has_open_signal(system: str, symbol: str) -> bool:
    """Mỗi symbol chỉ giữ 1 lệnh mở/hệ thống tại 1 thời điểm — tránh bắn tín hiệu trùng khi
    điều kiện entry vẫn còn đúng ở nhiều nến liên tiếp (VD nhiều nến xác nhận sát nhau)."""
    return any(s["symbol"] == symbol and s["status"] == "open" for s in strategy_signals[system])


def resolve_outcome(df: pd.DataFrame, entry_time: int, sl: float, tp: float, bull: bool):
    """Quét nến từ sau lúc entry -> chạm SL trước tính lỗ, chạm TP trước tính lời.
    Dùng cả lúc tạo tín hiệu mới (phòng trường hợp giá đã chạy qua TP/SL ngay từ đầu — ví dụ
    sau khi server restart, tín hiệu cũ bị phát hiện lại và không nên mặc định là 'đang mở')
    và lúc cập nhật tín hiệu đang mở ở mỗi vòng quét."""
    sub = df[df["ts"] >= entry_time]  # entry_time = giờ đóng nến trigger = giờ mở nến kế tiếp
    for _, row in sub.iterrows():
        hit_sl = row["low"] <= sl if bull else row["high"] >= sl
        hit_tp = row["high"] >= tp if bull else row["low"] <= tp
        if hit_sl or hit_tp:
            return ("loss" if hit_sl else "win"), int(row["ts"]), (sl if hit_sl else tp)
    return "open", None, None


async def update_open_signals(system: str, symbol: str, df: pd.DataFrame):
    """Kiểm tra các tín hiệu đang mở của symbol này."""
    for sig in list(strategy_signals[system]):
        if sig["symbol"] != symbol or sig["status"] != "open":
            continue
        bull = sig["direction"] == "bullish"
        status, exit_time, exit_price = resolve_outcome(df, sig["entry_time"], sig["sl"], sig["tp"], bull)
        if status != "open":
            sig["status"] = status
            sig["closed_at"] = int(time.time())
            sig["exit_time"] = exit_time  # thời điểm nến chạm SL/TP (khác closed_at = lúc server phát hiện)
            sig["exit_price"] = exit_price
            save_strategy_state()
            await manager.broadcast({"type": "strategy_update", "data": sig})


async def scan_scalp_symbol(symbol: str, sem: asyncio.Semaphore):
    async with sem:
        try:
            h4 = await fetch_ohlc(symbol, "4h", 300)
            m15 = await fetch_ohlc(symbol, "15m", 300)
            m5 = await fetch_ohlc(symbol, "5m", 300)
        except Exception:
            return
    await update_open_signals("scalp", symbol, m5)
    try:
        sig = await asyncio.to_thread(check_scalp_signal, symbol, h4, m15, m5)
    except Exception:
        log.exception("check_scalp_signal lỗi %s", symbol)
        return
    if sig and sig["id"] not in strategy_seen and not has_open_signal("scalp", symbol):
        strategy_seen.add(sig["id"])
        strategy_signals["scalp"].appendleft(sig)
        save_strategy_state()
        log.info("SCALP %s %s %s @ %s (SL %s / TP %s)", symbol, sig["direction"], sig["zone_kind"],
                  sig["entry"], sig["sl"], sig["tp"])
        await manager.broadcast({"type": "strategy_signal", "data": sig})


async def scan_swing_symbol(symbol: str, sem: asyncio.Semaphore):
    async with sem:
        try:
            d1 = await fetch_ohlc(symbol, "1d", 300)
            h4 = await fetch_ohlc(symbol, "4h", 300)
        except Exception:
            return
    await update_open_signals("swing", symbol, h4)
    try:
        sig = await asyncio.to_thread(check_swing_signal, symbol, d1, h4)
    except Exception:
        log.exception("check_swing_signal lỗi %s", symbol)
        return
    if sig and sig["id"] not in strategy_seen and not has_open_signal("swing", symbol):
        strategy_seen.add(sig["id"])
        strategy_signals["swing"].appendleft(sig)
        save_strategy_state()
        log.info("SWING %s %s %s @ %s (SL %s / TP %s)", symbol, sig["direction"], sig["zone_kind"],
                  sig["entry"], sig["sl"], sig["tp"])
        await manager.broadcast({"type": "strategy_signal", "data": sig})


async def scan_m1_symbol(symbol: str, sem: asyncio.Semaphore):
    async with sem:
        try:
            h1 = await fetch_ohlc(symbol, "1h", 300)
            m5 = await fetch_ohlc(symbol, "5m", 300)
            m1 = await fetch_ohlc(symbol, "1m", 400)
        except Exception:
            return
    await update_open_signals("m1", symbol, m1)
    try:
        sig = await asyncio.to_thread(check_m1_signal, symbol, h1, m5, m1)
    except Exception:
        log.exception("check_m1_signal lỗi %s", symbol)
        return
    if sig and sig["id"] not in strategy_seen and not has_open_signal("m1", symbol):
        strategy_seen.add(sig["id"])
        strategy_signals["m1"].appendleft(sig)
        save_strategy_state()
        log.info("M1 %s %s @ %s (SL %s / TP %s)", symbol, sig["direction"], sig["entry"], sig["sl"], sig["tp"])
        await manager.broadcast({"type": "strategy_signal", "data": sig})


async def m1_loop():
    sem = asyncio.Semaphore(CONCURRENCY)
    while True:
        t0 = time.time()
        if strategy_enabled["m1"]:
            try:
                await asyncio.gather(*(scan_m1_symbol(s, sem) for s in active_symbols), return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("M1 loop lỗi")
        await asyncio.sleep(max(15.0, M1_INTERVAL - (time.time() - t0)))


async def scalp_loop():
    sem = asyncio.Semaphore(CONCURRENCY)
    while True:
        t0 = time.time()
        if strategy_enabled["scalp"]:
            try:
                await asyncio.gather(*(scan_scalp_symbol(s, sem) for s in active_symbols), return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Scalp loop lỗi")
        await asyncio.sleep(max(5.0, SCALP_INTERVAL - (time.time() - t0)))


async def swing_loop():
    sem = asyncio.Semaphore(CONCURRENCY)
    while True:
        t0 = time.time()
        if strategy_enabled["swing"]:
            try:
                await asyncio.gather(*(scan_swing_symbol(s, sem) for s in active_symbols), return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Swing loop lỗi")
        await asyncio.sleep(max(10.0, SWING_INTERVAL - (time.time() - t0)))


# ─────────────────────────── App ───────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global exchange, kline_exchange, http, active_symbols
    load_strategy_state()
    exchange = ccxt.binanceusdm({"enableRateLimit": True})
    kline_exchange = ccxt.binanceusdm({"enableRateLimit": True})
    http = httpx.AsyncClient()
    try:
        await exchange.load_markets()
        await kline_exchange.load_markets()
        valid = [s for s in SYMBOLS if to_ccxt_symbol(s) in exchange.markets]
        invalid = set(SYMBOLS) - set(valid)
        if invalid:
            log.warning("Bỏ qua symbol không tồn tại trên Binance Futures: %s", ", ".join(invalid))
        active_symbols = valid
    except Exception as e:
        log.error("Không load được markets Binance (%s) — vẫn quét danh sách gốc.", e)

    log.info("Theo dõi %d symbols × %s | Telegram: %s", len(active_symbols), TIMEFRAMES,
             "ON" if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID else "OFF")
    tasks = [asyncio.create_task(scanner_loop()), asyncio.create_task(screener_loop()),
             asyncio.create_task(kline_relay_loop()), asyncio.create_task(scalp_loop()),
             asyncio.create_task(swing_loop()), asyncio.create_task(m1_loop())]
    yield
    for task in tasks:
        task.cancel()
    for task in tasks:
        with suppress(asyncio.CancelledError):
            await task
    await exchange.close()
    await kline_exchange.close()
    await http.aclose()


app = FastAPI(title="SMC Futures Monitor", lifespan=lifespan)


@app.get("/")
async def index():
    return FileResponse(INDEX_FILE)


@app.get("/api/config")
async def get_config():
    return status_payload()


@app.get("/api/signals")
async def get_signals():
    return list(history)


@app.get("/api/screener")
async def get_screener():
    return screener_state


@app.get("/api/strategy/signals")
async def get_strategy_signals(system: str = Query("scalp")):
    if system not in strategy_signals:
        raise HTTPException(400, f"system không hợp lệ: {system}")
    rr = {"scalp": SCALP_RR, "swing": SWING_RR, "m1": M1_RR}[system]
    return {"enabled": strategy_enabled[system], "rr": rr, "signals": list(strategy_signals[system])}


@app.post("/api/strategy/toggle")
async def toggle_strategy(system: str = Query(...), enabled: bool = Query(...)):
    if system not in strategy_enabled:
        raise HTTPException(400, f"system không hợp lệ: {system}")
    strategy_enabled[system] = enabled
    log.info("Strategy %s: %s", system, "ON" if enabled else "OFF")
    await manager.broadcast({"type": "status", "data": status_payload()})
    return {"system": system, "enabled": enabled}


@app.get("/api/klines")
async def get_klines(
    symbol: str = Query("BTCUSDT"),
    timeframe: str = Query("15m"),
    limit: int = Query(500, ge=100, le=1500),
):
    if timeframe not in ALLOWED_TIMEFRAMES:
        raise HTTPException(400, f"Timeframe không hỗ trợ: {timeframe}")
    symbol = symbol.upper()
    try:
        df = await fetch_ohlc(symbol, timeframe, limit)
    except (ValueError, ccxt.BadSymbol) as e:
        raise HTTPException(404, str(e))
    except ccxt.BaseError as e:
        raise HTTPException(502, f"Binance lỗi: {e}")

    closed = df.iloc[:-1].reset_index(drop=True)
    events = await asyncio.to_thread(analyze, closed) if len(closed) >= SWING_LENGTH * 3 else []
    signals = [
        {"choch_time": a["time"], "bos_time": b["time"], "dir": b["dir"]}
        for a, b in find_signals(events)
    ]
    candles = df[["ts", "open", "high", "low", "close"]].rename(columns={"ts": "time"}).to_dict("records")
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "candles": candles,
        "events": [{k: e[k] for k in ("type", "dir", "level", "swing_time", "time")} for e in events[-40:]],
        "signals": signals[-10:],
    }


@app.post("/api/test-signal")
async def test_signal(symbol: str = "BTCUSDT", timeframe: str = "15m"):
    """Bắn một tín hiệu giả để kiểm tra WebSocket + Telegram."""
    try:
        df = await fetch_ohlc(symbol.upper(), timeframe, 50)
    except Exception as e:
        raise HTTPException(502, str(e))
    price, t = float(df["close"].iloc[-2]), int(df["ts"].iloc[-2])
    sig = {
        "id": f"TEST-{uuid.uuid4().hex[:8]}", "test": True,
        "symbol": symbol.upper(), "timeframe": timeframe, "direction": "bullish",
        "choch_level": price * 0.995, "choch_time": int(df["ts"].iloc[-10]),
        "bos_level": price * 0.999, "bos_time": t, "price": price,
        "detected_at": int(time.time()),
    }
    await emit_signal(sig)
    return sig


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        await ws.send_json({"type": "history", "data": list(history)})
        await ws.send_json({"type": "strategy_history",
                             "data": {k: list(v) for k, v in strategy_signals.items()}})
        await ws.send_json({"type": "status", "data": status_payload()})
        while True:
            msg = await ws.receive_text()
            if msg == "ping":
                await ws.send_json({"type": "pong"})
                continue
            try:
                data = json.loads(msg)
            except ValueError:
                continue
            if data.get("action") == "watch" and data.get("symbol") and data.get("timeframe"):
                manager.set_watch(ws, data["symbol"], data["timeframe"])
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(ws)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
