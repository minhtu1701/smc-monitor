"""
SMC Futures Monitor - Backend
FastAPI + ccxt + smartmoneyconcepts + Telegram Bot + WebSocket

Chạy:  python main.py      (hoặc: uvicorn main:app --host 0.0.0.0 --port 8000)
Mở:    http://localhost:8000
"""
import asyncio
import math
import re
import json
import logging
import os
import sys
import time
import uuid
from functools import partial
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
from fvg_filter import find_valid_fvgs

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


# Giới hạn rủi ro dùng chung cho NR7 H4 (SL = đầu kia nến NR7).

# H4 Pullback: xu hướng EMA50/EMA200, giá đóng cửa lại trên/dưới EMA20 sau khi nằm phía ngược lại. Thoát bằng trailing stop
# (stop = giá đóng tốt nhất -/+ PB_TRAIL x rủi ro ban đầu). Edge nhỏ (~+0.1R) nhưng cùng dấu ở cả 16 coin gốc lẫn 24 coin mới.
PB_TRAIL = float(os.getenv("PB_TRAIL", "3"))
PB_INTERVAL = int(os.getenv("PB_INTERVAL", "300"))
PB_SL_BARS = int(os.getenv("PB_SL_BARS", "10"))
PB_MIN_RISK = float(os.getenv("PB_MIN_RISK", "0.003"))
PB_MAX_RISK = float(os.getenv("PB_MAX_RISK", "0.10"))
# Filter đã kiểm chứng trên 16 coin gốc + 24 coin ngoài mẫu (5 năm H4): xu hướng còn "non" (tuổi EMA50>EMA200 <= 141 nến), giá chưa bị
# kéo xa EMA200 (<= 4.97 ATR) và nến xác nhận có thân (>= 43% biên độ). Nâng edge từ ~+0.12R lên ~+0.30R (TP 8R). Nửa sau của mẫu yếu hơn nửa đầu.
PB_MAX_AGE = int(os.getenv("PB_MAX_AGE", "141"))
PB_MAX_DIST200 = float(os.getenv("PB_MAX_DIST200", "4.97"))
PB_MIN_BODY = float(os.getenv("PB_MIN_BODY", "0.43"))
PB_EXIT = os.getenv("PB_EXIT", "tp")  # "tp" = TP cố định PB_RR; "trail" = trailing stop PB_TRAIL R
PB_RR = float(os.getenv("PB_RR", "10"))
# TP 1:10 thay cho 1:8 — đo lại ở cấp DANH MỤC (98 coin, 5 năm, không lọc funding):
#   TP 8 -> lãi/sụt 1.89, R/năm 297 | TP 10 -> lãi/sụt 2.26, R/năm 341 | TP 12 -> 2.10 | TP 15 -> 1.83
# Đỉnh mượt ở 10, cả 6/6 năm dương, train +0.802 / holdout +0.802.

# NR7 H4 + xu hướng H4: chỉ theo hướng EMA50/EMA200 H4 và giá đóng cùng phía EMA20. TP 5R: +0.14R/lệnh (16 coin gốc +0.17, 24 coin mới +0.13).

# Breakout H4 (vào ngay khi nến đóng phá vỡ) + filter đã kiểm chứng trên 40 coin: volume, BTC cùng hướng, giá cùng phía EMA200, phá xa biên, biến
# động không bị nén. TP 5R: ~+0.23R/lệnh (16 coin gốc +0.24, 24 coin mới +0.22). Ngưỡng chọn từ bảng phân bố của cả 40 coin.
# Supertrend flip H4 + xu hướng: Supertrend (hl2 +- 3 x ATR14) đổi hướng, chỉ giao dịch thuận EMA50/EMA200 H4; entry giá đóng, SL = 2 ATR14, TP 1:3.
# Giải đấu 30 phương pháp trên 98 coin: +0.16R (40 coin huấn luyện) và +0.20R (58 coin holdout, 81% coin dương). Tương quan với Breakout H4 (cùng họ xu hướng).
ST_RR = float(os.getenv("ST_RR", "8"))
# Lệnh LIMIT chờ giá hồi (nghiên cứu entry/SL 98 coin, 5 năm, kiểm chứng train 40 coin + holdout 58 coin):
#   PB: limit close -/+ 1 ATR, SL 2 ATR, TP 8R   (+0.27/+0.23R -> +0.40/+0.39R)
#   ST: limit close -/+ 0.25 ATR, SL 1 ATR, TP 8R (+0.21/+0.26R -> +0.31/+0.36R)
#   BO: limit close -/+ 0.5 ATR, SL cực trị 10 nến -/+ 0.1 ATR, TP 5R (+0.20/+0.31R -> +0.39/+0.34R)
# Lệnh chờ hiệu lực LIMIT_BARS nến H4; không khớp -> "expired" (không tính vào win rate). ATR = Wilder 14.
LIMIT_BARS = int(os.getenv("LIMIT_BARS", "6"))
# ── SNR khung NGÀY (Malaysian SNR "kháng cự thành hỗ trợ") ──
# Mức = đỉnh/đáy swing (xác nhận sau SNR_L nến) mà giá rời đi >= SNR_MOVE x ATR14. Sau lần chạm lại đầu tiên,
# nếu giá ĐÓNG xuyên mức >= SNR_BREAK_ATR x ATR thì mức đổi vai trò -> đặt LIMIT tại mức, hiệu lực SNR_EXP_D ngày.
# Chỉ đặt khi nến phá vỡ đóng cùng phía MA50 với chiều lệnh. SL = mức -/+ SNR_SLK x ATR (ATR lúc ĐẶT lệnh), TP = SNR_RR x rủi ro.
# Backtest 98 coin 5 năm (train 40 / holdout 58): E +0.264R / +0.342R, WR 29.4%, t tuần +2.62, 6/6 năm dương,
# 18/18 cấu hình tham số dương ở cả hai tập. Chỉ trùng 12% lệnh với PB/BO/ST/VC.
# TP 1:8 + giữ tối đa 90 ngày (tối ưu ở cấp DANH MỤC, lưới hold 30/60/90/120 × TP 5/8/10/12/15R):
#   giữ 30 + TP 5R -> R/năm 194, sụt -162, lãi/sụt 1.19 (cấu hình cũ)
#   giữ 30 + TP 8R -> 216 / -163 / 1.33 | giữ 45 -> 220 / -158 / 1.39 | giữ 60 -> 240 / -135 / 1.78 | giữ 90 -> 244 / -120 / 2.03
#   TP 8R là đỉnh ở CẢ BỐN mức giữ lệnh -> đỉnh nội tại, không phải mép lưới.
#   Trần 14 ngày (yêu cầu của user: giữ lệnh <= 1-2 tuần). Chi phí chỉ 17%: trần 60 -> R/năm 240, lãi/sụt 1.78;
#   trần 14 -> R/năm 200, lãi/sụt 1.34, vẫn 6/6 năm dương. Giữ lệnh thực tế: trung vị 7 ngày, TB 7.8, 90% trong 14.
#   (PB thì NGƯỢC LẠI: cắt còn 14 ngày làm lãi/sụt rơi 2.26 -> 0.93, mất 59% — PB cần đuôi dài mới có lãi, để nguyên.)
SNR_L = int(os.getenv("SNR_L", "2"))
SNR_MOVE = float(os.getenv("SNR_MOVE", "2.0"))
SNR_BREAK_ATR = float(os.getenv("SNR_BREAK_ATR", "0.2"))
SNR_SLK = float(os.getenv("SNR_SLK", "1.0"))
SNR_RR = float(os.getenv("SNR_RR", "8"))
SNR_EXP_D = int(os.getenv("SNR_EXP_D", "20"))
SNR_HOLD_D = int(os.getenv("SNR_HOLD_D", "14"))
SNR_MA = int(os.getenv("SNR_MA", "50"))
# Bỏ qua khi thị trường đang NÉN: ATR%/trung bình ATR% 180 nến < ngưỡng -> mức bị phá thường là phá vờ.
# Đo ở cấp DANH MỤC GỘP: không lọc -> lãi/sụt 8.86, Sharpe 2.21, SNR WR 33.9%
#                        >= 0.85  -> lãi/sụt 10.34, Sharpe 2.27, SNR WR 38.0% (giữ 61% lệnh, R/năm -5%, sụt -19%)
# Vùng 0.75-1.10 đều cho 9.97-11.20 nên không phải dò tham số.
SNR_MIN_ATR_RATIO = float(os.getenv("SNR_MIN_ATR_RATIO", "0.85"))
# Xu hướng khung NGÀY: chỉ vào khi EMA nhanh/chậm của chính khung 1d cùng chiều lệnh.
SNR_D1_FAST = int(os.getenv("SNR_D1_FAST", "20"))
SNR_D1_SLOW = int(os.getenv("SNR_D1_SLOW", "50"))
SNR_INTERVAL = int(os.getenv("SNR_INTERVAL", "900"))
# ── Lọc funding ngược đám đông ──
# Chỉ vào lệnh khi funding đang nghiêng về phía NGƯỢC với lệnh (mua khi funding âm, bán khi funding dương):
# đám đông đang trả phí để giữ vị thế ngược mình. Backtest 98 coin 5 năm:
#   ST giữ 74% lệnh, E +0.701R -> +0.896R (train +0.740 / holdout +0.993), t của hiệu +4.50, 5/6 năm tốt hơn
#   PB giữ 50% lệnh, E +0.695R -> +0.887R (train +0.867 / holdout +0.901), t của hiệu +2.61, 5/6 năm tốt hơn
# BO (t=+2.45 nhưng chỉ 3/6 năm) và VC (t=+0.19) không dùng.
# TẮT mặc định: lọc funding nâng kỳ vọng MỖI LỆNH nhưng hại ở cấp DANH MỤC vì bỏ mất nửa số lệnh vẫn đang lãi.
#   PB: không lọc -> lãi/sụt 1.89, R/năm 297 | lọc funding -> lãi/sụt 1.27, R/năm 190 (E mỗi lệnh +0.695 -> +0.887)
#   ST: chỉ lọc BTC -> lãi/sụt 3.61, R/năm 271 | thêm funding -> lãi/sụt 3.14, R/năm 236
# Bài học: luôn đo bộ lọc ở cấp danh mục (R/năm và sụt giảm), không chỉ kỳ vọng mỗi lệnh.
FUND_SYSTEMS = tuple(x.strip() for x in os.getenv("FUND_SYSTEMS", "").split(",") if x.strip())
FUND_INTERVAL = int(os.getenv("FUND_INTERVAL", "300"))
# ── Lọc theo chế độ BTC ──
# Chỉ vào lệnh khi BTC đã đi CÙNG CHIỀU lệnh trong BTC_TREND_DAYS ngày qua. Backtest 98 coin 5 năm:
#   BO (chỉ mua): giữ 68% lệnh, E +0.591R -> +0.854R; lệnh bị bỏ chỉ +0.033R và ÂM 4/6 năm. t của hiệu +7.39.
#   ST: giữ 60% lệnh, E +0.701R -> +1.088R (train +0.960 vs -0.103, holdout +1.187 vs +0.286). t +6.24.
# Bền với mọi cửa sổ 10/20/30/45/60 ngày (t từ 2.7 đến 7.4) nên không phải dò tham số.
# KHÔNG áp dụng cho PB (t +0.58), VC (chỉ N=30 dương, các cửa sổ khác âm -> nhiễu) và SNR (t -0.13).
BTC_TREND_SYSTEMS = tuple(x.strip() for x in os.getenv("BTC_TREND_SYSTEMS", "bo4h,st4h,st15").split(",") if x.strip())
BTC_TREND_DAYS = int(os.getenv("BTC_TREND_DAYS", "30"))
# Dấu hiệu độ tin cậy (không chặn lệnh): hệ thống khác đã báo cùng coin, cùng chiều trong CONFLUENCE_H giờ qua.
# Backtest: PB +0.695R -> +1.963R, ST +0.701R -> +1.677R ở nhóm có đồng thuận. Nhưng nhóm KHÔNG có đồng thuận
# vẫn lãi +0.55R và chiếm 90% lợi nhuận, nên chỉ hiển thị để cân nhắc vào nặng tay hơn, tuyệt đối không lọc bỏ.
CONFLUENCE_H = int(os.getenv("CONFLUENCE_H", "48"))
PB_ENTRY_ATR = float(os.getenv("PB_ENTRY_ATR", "1.0"))
# Thoát theo thời gian: sau PB_TSTOP nến H4 kể từ khi khớp mà chưa lời được PB_TMFE R -> đóng ở giá đóng nến.
# Đo ở cấp DANH MỤC: không có -> R/năm 341, sụt -150.7, lãi/sụt 2.26, Sharpe 1.28, giữ TB 13.1 ngày
#                    24 nến/2R -> R/năm 264, sụt  -88.2, lãi/sụt 2.99, Sharpe 1.40, giữ TB  7.8 ngày
# Cả vùng tstop 24-30 nến × tmfe 1.5-3.0R đều >= 2.68 nên không phải dò tham số. train +0.577 / holdout +0.560.
PB_TSTOP = int(os.getenv("PB_TSTOP", "24"))
PB_TMFE = float(os.getenv("PB_TMFE", "2.0"))
PB_SL_ATR = float(os.getenv("PB_SL_ATR", "2.0"))
# Dùng CHUNG cho st4h và st15 (hai hệ thống chỉ khác TP: ST_RR vs ST15_RR).
# Vào bằng LIMIT tốt hơn 0.25 ATR là điều kiện SỐNG CÒN: cùng tín hiệu, cùng SL 1 ATR,
# vào ở giá đóng cho -0.611R còn vào bằng limit cho +0.396R (TP 1:1.5, 98 coin/5 năm).
ST4_ENTRY_ATR = float(os.getenv("ST4_ENTRY_ATR", "0.25"))
ST4_SL_ATR = float(os.getenv("ST4_SL_ATR", "1.0"))
BO_ENTRY_ATR = float(os.getenv("BO_ENTRY_ATR", "0"))
# 0 = đặt limit ngay tại giá đóng nến phá vỡ (không chờ hồi). Đo ở cấp DANH MỤC, lưới 192 cấu hình:
#   limit 0 ATR -> R/năm 169, sụt -36.7, lãi/sụt 4.59, 6/6 năm | limit 0.5 ATR (cũ) -> 172 / -44.5 / 3.87
#   Bền với trượt giá: +0.05% -> 4.44 | +0.10% -> 4.29 | +0.20% -> 4.01, vẫn hơn cấu hình cũ.
#   Lý do: cú phá vỡ nào hồi ngay 0.5 ATR thường là cú phá vỡ yếu — chờ hồi được giá tốt hơn nhưng vào toàn setup tệ hơn.
BO_SL_BARS = int(os.getenv("BO_SL_BARS", "10"))
# Nghiên cứu bối cảnh (98 coin, train/holdout): lệnh thắng nhiều hơn khi coin CHƯA vượt trội BTC theo hướng lệnh trong 30 ngày
# (mua coin tụt lại BTC / bán coin mạnh hơn BTC). BO: E +0.39/+0.34R -> +0.57/+0.82R, lãi/sụt giảm 1.19 -> 1.78.
BO_MAX_RS = float(os.getenv("BO_MAX_RS", "0.03"))
# Thoát theo thời gian: breakout không chạy được 1R sau 12 nến H4 (2 ngày) kể từ khi khớp -> đóng ở giá đóng. Backtest: win 30% -> 41%,
# sụt giảm tối đa 95R -> 46R, lãi/sụt giảm 1.98 -> 3.74, R/năm 193 -> 172.
BO_TSTOP = int(os.getenv("BO_TSTOP", "12"))
# Volume Capitulation (VC H4): BÁN TIẾP ĐÀ khi nến H4 đóng lúc 16:00/20:00 UTC, volume > 3x TB 20 nến trước, 3 nến GIẢM liên tiếp.
# (Chiều ngược lại - mua khi bơm có volume - ~0R.)
# Tìm bằng quét ~53k tổ hợp chỉ báo, chọn trên 40 coin, kiểm định 58 coin holdout: TP1.5/SL1.5 ATR, giữ tối đa 18 nến (3 ngày):
# win 67%, +0.33R/lệnh (train +0.34 / holdout +0.32, coin lớn +0.28 / alt +0.34, dương 6/6 năm, t=3.7); bán ngẫu nhiên cùng thoát chỉ +0.02R.
VC_TP_ATR = float(os.getenv("VC_TP_ATR", "1.5"))
VC_SL_ATR = float(os.getenv("VC_SL_ATR", "1.5"))
VC_HOLD = int(os.getenv("VC_HOLD", "30"))   # 30 nến H4 = 5 ngày. Đo ở cấp danh mục: giữ 18 -> lãi/sụt 6.24, giữ 30 -> 7.04, giữ 48 -> 7.08 (chọn 30, đã vào vùng bằng phẳng)
VC_VOLX = float(os.getenv("VC_VOLX", "3.0"))
# Lọc chỉ báo thêm (thử RSI/BB/MACD/Stoch/ADX trên chính các lệnh của hệ thống, train 40 coin + holdout 58 coin):
#   PB: bỏ khi dải Bollinger quá hẹp (độ rộng < phân vị 33 của 100 nến) -> E +0.69/+0.70R thành +1.07/+1.07R, lãi/sụt giảm 1.84 -> 2.38
#   ST: bỏ khi Stochastic đã quá cực đoan theo hướng lệnh -> E +0.56/+0.81R thành +0.74/+1.05R, lãi/sụt giảm 1.91 -> 2.45, tốt hơn ở cả 6 năm
#   BO: cần ADX >= 19 (có xu hướng thật) -> E +0.46/+0.75R thành +0.58/+0.91R, lãi/sụt giảm 3.74 -> 4.15
#   VC: không chỉ báo nào cải thiện -> giữ nguyên.
PB_MIN_BBW_PCTL = float(os.getenv("PB_MIN_BBW_PCTL", "0.33"))
# Lọc Stochastic: >= 100 là TẮT. Đã đo 20/09/2026 và TẮT lại — nâng kỳ vọng mỗi lệnh của ST
# (+1.096 -> +1.318R) nhưng cắt 296 lệnh VẪN ĐANG LÃI (+0.422R, dương 5/6 năm), nên ở cấp
# danh mục gộp làm lãi/sụt tụt 9.19 -> 8.16. Cùng khuôn mẫu với các bộ lọc ST đã loại.
ST_MAX_STOCH = float(os.getenv("ST_MAX_STOCH", "100"))
BO_MIN_ADX = float(os.getenv("BO_MIN_ADX", "19"))


def bb_width_pctl(c: np.ndarray, i: int, n: int = 20, look: int = 100) -> float:
    """Phân vị (0-1) của độ rộng dải Bollinger hiện tại so với `look` nến gần nhất."""
    s_ = pd.Series(c)
    w = (4 * s_.rolling(n).std() / s_.rolling(n).mean()).to_numpy()
    seg = w[max(i - look + 1, 0):i + 1]
    seg = seg[~np.isnan(seg)]
    return float((seg <= w[i]).mean()) if len(seg) and not np.isnan(w[i]) else 1.0


def stoch_k(h: np.ndarray, l: np.ndarray, c: np.ndarray, i: int, n: int = 14, smooth: int = 3) -> float:
    ll = pd.Series(l).rolling(n).min()
    hh = pd.Series(h).rolling(n).max()
    k = (100 * (pd.Series(c) - ll) / (hh - ll + 1e-12)).rolling(smooth).mean().to_numpy()
    return float(k[i]) if not np.isnan(k[i]) else 50.0


def adx14(h: np.ndarray, l: np.ndarray, c: np.ndarray, i: int) -> float:
    atr = wilder_atr(h, l, c)
    up = np.maximum(h - np.r_[h[0], h[:-1]], 0)
    dn = np.maximum(np.r_[l[0], l[:-1]] - l, 0)
    f = lambda x: pd.Series(x).ewm(alpha=1 / 14, adjust=False).mean().to_numpy()  # noqa: E731
    pdi, mdi = 100 * f(np.where(up > dn, up, 0)) / atr, 100 * f(np.where(dn > up, dn, 0)) / atr
    a = f(100 * abs(pdi - mdi) / (pdi + mdi + 1e-12))
    return float(a[i]) if not np.isnan(a[i]) else 0.0
# BO: lệnh BÁN gần như không có lãi (+0.02/+0.16R, +7R/năm) -> chỉ MUA.
BO_LONG_ONLY = os.getenv("BO_LONG_ONLY", "1") == "1"
BO_TMFE = float(os.getenv("BO_TMFE", "1.0"))
# Supertrend H4: lệnh MUA âm ở cả train (-0.10R) lẫn holdout (-0.09R) và 5/6 năm; chỉ BÁN: +0.55/+0.59R.
ST4_SHORT_ONLY = os.getenv("ST4_SHORT_ONLY", "1") == "1"
ST_INTERVAL = int(os.getenv("ST_INTERVAL", "300"))
ST_SL_ATR = float(os.getenv("ST_SL_ATR", "2"))
ST_MIN_RISK = float(os.getenv("ST_MIN_RISK", "0.003"))
ST_MAX_RISK = float(os.getenv("ST_MAX_RISK", "0.12"))
# TP 8R: với entry limit -0.5 ATR + lọc sức mạnh tương đối, 8R > 5R ở cả train (+0.78 vs +0.60R) và holdout (+1.04 vs +0.75R), lãi/sụt giảm 2.39 vs 1.64.
BO_RR = float(os.getenv("BO_RR", "8"))
# Bản RR thấp của Breakout H4 / Supertrend H4 (cùng tín hiệu, TP 1:1.5): thắng ~45% (hoà vốn 40%), kỳ vọng ~ +0.13R/lệnh ở 58 coin holdout
# (Breakout +0.135R, Supertrend +0.123R) — đường vốn mượt hơn nhưng kỳ vọng mỗi lệnh chỉ khoảng một nửa bản RR cao.
ST15_RR = float(os.getenv("ST15_RR", "1.5"))
BO_INTERVAL = int(os.getenv("BO_INTERVAL", "300"))
BO_BOX = int(os.getenv("BO_BOX", "20"))
BO_MIN_VOLX = float(os.getenv("BO_MIN_VOLX", "1.235"))
BO_MIN_BRK = float(os.getenv("BO_MIN_BRK", "0.63"))
BO_MIN_DIST200 = float(os.getenv("BO_MIN_DIST200", "0.11"))
BO_MIN_ATR_RATIO = float(os.getenv("BO_MIN_ATR_RATIO", "0.73"))
BO_MIN_BODY = float(os.getenv("BO_MIN_BODY", "0.6"))

ALLOWED_TIMEFRAMES = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"}
TF_SECONDS = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600,
              "2h": 7200, "4h": 14400, "6h": 21600, "12h": 43200, "1d": 86400}
INDEX_FILE = Path(__file__).parent / "index.html"
STRATEGY_STATE_FILE = Path(__file__).parent / "strategy_state.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("smc")

# ─────────────────────────── State ───────────────────────────
exchange: ccxt.binanceusdm | None = None
kline_exchange: ccxt.binanceusdm | None = None  # instance riêng, tránh bị nghẽn rate-limit chung với scanner
http: httpx.AsyncClient | None = None
active_symbols: list[str] = list(SYMBOLS)
history: deque = deque(maxlen=200)   # tín hiệu gần nhất (mới nhất đứng đầu)
seen: set[str] = set()               # chống gửi trùng
status = {"last_scan": None, "duration": None, "scans": 0}
STRATEGY_SYSTEMS = ("pb4h", "bo4h", "st4h", "st15", "news", "vc4h", "snr1d")
strategy_enabled: dict = {**{k: True for k in STRATEGY_SYSTEMS}, "zone": True, "ny": True}  # "zone" = cảnh báo vùng HTF (không phải chiến lược vào lệnh)
# "ny" = chỉ nhận tín hiệu khi nến H4 đóng lúc 16:00/20:00 UTC (phiên New York). Backtest 98 coin: PB +0.22R -> +0.46R, BO +0.25R -> +0.35R, ST +0.24R -> +0.34R.
# BO H4 không dùng lọc NY: nghiên cứu bối cảnh cho thấy lọc giờ làm BO xấu đi (lãi/sụt giảm 1.19 -> 0.68); BO dùng lọc sức mạnh tương đối thay thế.
NY_SYSTEMS = ("pb4h", "st4h", "st15")
# Các hệ thống dùng CHUNG một tín hiệu gốc -> không được tính là "đồng thuận" của nhau.
# st4h và st15 đều là Supertrend flip H4, chỉ khác TP (1:8 vs 1:1.5).
SAME_SIGNAL = {"st4h": {"st4h", "st15"}, "st15": {"st4h", "st15"}}
NY_HOURS = (16, 20)
strategy_signals: dict[str, deque] = {k: deque(maxlen=100) for k in STRATEGY_SYSTEMS}
strategy_seen: set[str] = set()
# Đồng thuận: tín hiệu chỉ hợp lệ khi >= CONSENSUS_FRAC số coin đang quét (khác coin này) cũng có tín hiệu CÙNG hệ thống, CÙNG hướng trong 24h.
# Đo lại ở cấp DANH MỤC (không chỉ kỳ vọng mỗi lệnh) -> CHỈ còn áp dụng cho BO:
#   BO: không lọc -> R/năm 172, sụt -44.5, lãi/sụt 3.87 | lọc -> R/năm 149, sụt -26.3, lãi/sụt 5.68  (GIỮ)
#   PB: không lọc -> R/năm 251, lãi/sụt 2.24, Sharpe 1.16, 6/6 năm | lọc -> R/năm 189, lãi/sụt 2.22, Sharpe 0.99, 5/6 năm  (BỎ)
#   ST: không lọc -> R/năm 253, lãi/sụt 3.24 | lọc -> R/năm 221, lãi/sụt 3.09  (BỎ)
# Lý do hợp lý: BO là phá vỡ — cả thị trường cùng phá mới là sóng thật; PB là hồi về trung bình, chuyện riêng từng coin.
CONSENSUS_SYSTEMS = tuple(x for x in os.getenv("CONSENSUS_SYSTEMS", "bo4h").split(",") if x)
CONSENSUS_FRAC = float(os.getenv("CONSENSUS_FRAC", "0.03"))
CONSENSUS_WINDOW = 6 * 14400
raw_signals: dict[str, dict] = {}
consensus_skipped: set[str] = set()
news_flags: dict[str, dict] = {}   # symbol perp -> tin rủi ro gần nhất (Monitoring / huỷ niêm yết) -> chặn lệnh MUA 30 ngày
NEWS_BLOCK_DAYS = 30
NEWS_SL_PCT = float(os.getenv("NEWS_SL_PCT", "10"))
NEWS_HOLD_H = int(os.getenv("NEWS_HOLD_H", "24"))
NEWS_MAX_AGE_MIN = 30   # chỉ vào lệnh nếu phát hiện tin trong 30 phút sau khi đăng
btc_h4_r30: dict[int, float] = {}  # ts nến H4 BTC -> lợi nhuận 180 nến (30 ngày) của BTC, dùng cho lọc sức mạnh tương đối của BO H4
btc_h4_regime: dict[int, int] = {}  # ts nến H4 BTC -> +1 nếu đóng trên EMA200, -1 nếu dưới (dùng làm filter cho Breakout H4)


def save_strategy_state():
    """Lưu tín hiệu Scalp/Swing/M1 ra file — không thì mỗi lần restart server (deploy fix...)
    lại mất hết lịch sử lời/lỗ, vô lý với tính năng Log."""
    try:
        data = {k: list(v) for k, v in strategy_signals.items()}
        # Sổ đếm đồng thuận cũng phải sống qua restart: nếu không, sau mỗi lần watchdog bật lại
        # server thì book rỗng và lệnh BO bị chặn oan cho tới khi gom đủ 24h tín hiệu mới.
        data["__raw__"] = [[sysname, s, int(t), d] for sysname, bk in raw_signals.items() for (s, t, d) in bk]
        # Ghi nguyên tử: write_text cắt file về 0 byte TRƯỚC khi ghi, nên nếu watchdog kill
        # server đúng lúc đó thì file JSON hỏng -> mất sạch lệnh đang mở và lịch sử lời/lỗ.
        tmp = STRATEGY_STATE_FILE.with_suffix(STRATEGY_STATE_FILE.suffix + ".tmp")
        tmp.write_text(json.dumps(data), encoding="utf-8")
        os.replace(tmp, STRATEGY_STATE_FILE)
    except Exception:
        log.exception("Lưu %s lỗi", STRATEGY_STATE_FILE.name)


def load_strategy_state():
    if not STRATEGY_STATE_FILE.exists():
        return
    try:
        data = json.loads(STRATEGY_STATE_FILE.read_text(encoding="utf-8"))
        try:
            for sysname, s, t, d in data.get("__raw__", []):
                raw_signals.setdefault(sysname, {})[(s, int(t), d)] = int(t)
        except Exception:
            log.exception("Bỏ qua sổ đồng thuận hỏng")
        for k, v in data.items():
            if k not in strategy_signals:
                continue
            try:                                  # hỏng một hệ thống thì không kéo các hệ thống khác theo
                ids = [sig["id"] for sig in v]
            except Exception:
                log.exception("Bỏ qua state hỏng của %s", k)
                continue
            strategy_signals[k] = deque(v, maxlen=100)
            strategy_seen.update(ids)
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


# ─────────────────────────── Volwatch: xác suất biến động mạnh 24h ───────────────────────────
volwatch_state: dict = {"updated_at": None, "scanned": 0, "results": [], "interval_s": 3600}
volwatch_exchange: "ccxt.binanceusdm | None" = None


async def volwatch_loop():
    import volwatch
    while True:
        try:
            volwatch_state.update(await volwatch.scan(volwatch_exchange, log))
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Volwatch loop lỗi")
        await asyncio.sleep(volwatch_state["interval_s"])


# ─────────────────────────── News (thông báo Binance) ───────────────────────────
news_seen: set[str] = set()


def _perp_of(ticker: str) -> str | None:
    for cand in (f"{ticker}USDT", f"1000{ticker}USDT"):
        m = exchange.markets_by_id.get(cand) if exchange and exchange.markets_by_id else None
        if m:
            m = m[0] if isinstance(m, list) else m
            if m.get("active") and m.get("swap"):
                return cand
    return None


async def news_update_open():
    """Lệnh News: BÁN, SL +NEWS_SL_PCT%, đóng sau NEWS_HOLD_H giờ ở giá hiện tại."""
    for sig in list(strategy_signals["news"]):
        if sig["status"] != "open":
            continue
        try:
            df = await fetch_ohlc(sig["symbol"], "5m", 400)
        except Exception:
            continue
        sub = df[df["ts"] >= sig["entry_time"]]
        mark = float(df["close"].iloc[-1])
        hit = sub[sub["high"] >= sig["sl"]]
        exit_px = exit_t = None
        if len(hit):
            exit_px, exit_t = sig["sl"], int(hit["ts"].iloc[0])
        elif time.time() >= sig["exit_at"]:
            after = df[df["ts"] >= sig["exit_at"] - 300]
            exit_px = float(after["close"].iloc[0]) if len(after) else mark
            exit_t = int(sig["exit_at"])
        if exit_px is not None:
            r = (sig["entry"] - exit_px) / (sig["sl"] - sig["entry"])
            sig.update(status="win" if r > 0 else "loss", exit_price=float(exit_px), exit_time=exit_t, closed_at=int(time.time()), r=round(r, 3), mark=mark)
            save_strategy_state()
            await manager.broadcast({"type": "strategy_update", "data": sig})
        elif sig.get("mark") != mark:
            sig["mark"], sig["mark_time"] = mark, int(time.time())
            await manager.broadcast({"type": "strategy_update", "data": sig})


async def news_handle(a: dict, fresh: bool):
    import news
    for typ, tk in news.classify(a["title"]):
        sym = _perp_of(tk)
        if typ in ("MONITOR", "DELIST", "FUT_DELIST") and sym:
            if a["ts"] > news_flags.get(sym, {}).get("ts", 0):
                news_flags[sym] = {"type": typ, "ts": a["ts"], "title": a["title"]}
        if not fresh:
            continue
        label = news.TRADE_TYPES.get(typ) or news.INFO_TYPES.get(typ, typ)
        import translate
        if "title_vi" not in a:
            a["title_vi"] = await translate.to_vi(http, a["title"])
        await manager.broadcast({"type": "news_alert", "data": {"id": f"{a['code']}-{tk}", "kind": typ, "label": label, "ticker": tk, "symbol": sym,
                                                                  "title": a["title"], "title_vi": a["title_vi"], "ts": a["ts"], "stats": news.STATS.get(typ, "")}})
        log.info("News %s %s (%s): %s", typ, tk, sym or "không có perp", a["title"][:90])
        age_min = (time.time() - a["ts"]) / 60
        if typ in news.TRADE_TYPES and sym and strategy_enabled.get("news") and age_min <= NEWS_MAX_AGE_MIN and not has_open_signal("news", sym):
            try:
                t = await exchange.fetch_ticker(to_ccxt_symbol(sym))
                px = float(t["last"])
            except Exception:
                continue
            now = int(time.time())
            sig = {"id": f"news-{sym}-{a['ts']}", "system": "news", "symbol": sym, "timeframe": "5m", "direction": "bearish",
                   "zone_kind": label, "entry": px, "sl": px * (1 + NEWS_SL_PCT / 100), "tp": None, "rr": None, "hold_h": NEWS_HOLD_H,
                   "entry_time": now, "exit_at": now + NEWS_HOLD_H * 3600, "detected_at": now, "status": "open",
                   "closed_at": None, "exit_time": None, "exit_price": None, "title": a["title"], "title_vi": a.get("title_vi"), "news_ts": a["ts"]}
            if sig["id"] not in strategy_seen:
                strategy_seen.add(sig["id"])
                strategy_signals["news"].appendleft(sig)
                save_strategy_state()
                await manager.broadcast({"type": "strategy_signal", "data": sig})


feed_items: deque = deque(maxlen=300)
feed_seen: set[str] = set()


def _perp_bases() -> dict[str, str]:
    """base asset -> symbol perp (vd PEPE -> 1000PEPEUSDT)."""
    out = {}
    for m in (exchange.markets or {}).values():
        if m.get("swap") and m.get("quote") == "USDT" and m.get("active") and m.get("info", {}).get("contractType") == "PERPETUAL":
            b = m.get("base", "")
            b2 = re.sub(r"^1000+", "", b)
            out.setdefault(b2, m["id"])
    return out


async def feed_loop():
    import feed
    for it in feed.load_recent():
        feed_seen.add(it["id"])
        feed_items.appendleft(it)
    first = not feed_seen
    while True:
        try:
            bases = _perp_bases()
            raw = await feed.fetch_all(http)
            new = [it for it in sorted(raw, key=lambda x: x["ts"]) if it["id"] not in feed_seen]
            if new:
                need = set()
                for it in new:
                    it["coins"] = [bases[b] for b in feed.tag(it["text"], set(bases))]
                    need.update(it["coins"])
                prices = {}
                if need:
                    try:
                        tk = await exchange.fetch_tickers()
                        for msym, t in tk.items():
                            mid = exchange.markets.get(msym, {}).get("id")
                            if mid in need and t.get("last"):
                                prices[mid] = float(t["last"])
                    except Exception:
                        pass
                import translate
                await translate.batch(http, list(reversed(new)), limit=60)
                for it in new:
                    it["prices"] = {c: prices.get(c) for c in it["coins"]}
                    it["seen_at"] = int(time.time())
                    feed_seen.add(it["id"])
                    feed_items.appendleft(it)
                feed.append(new)
                if not first:
                    open_syms = {x["symbol"] for v in strategy_signals.values() for x in v if x["status"] in ("open", "pending")}
                    for it in new:
                        await manager.broadcast({"type": "feed_item", "data": {**it, "hot": bool(set(it["coins"]) & open_syms)}})
                log.info("Feed: +%d bài (%d gắn coin)", len(new), sum(1 for it in new if it["coins"]))
            import translate
            await translate.batch(http, list(feed_items), limit=20)
            first = False
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Feed loop lỗi")
        await asyncio.sleep(90)


async def news_loop():
    import news
    first = True
    while True:
        try:
            arts = await news.fetch_latest(http, 50 if first else 20)
            for a in sorted(arts, key=lambda x: x["ts"]):
                if a["code"] in news_seen:
                    continue
                news_seen.add(a["code"])
                await news_handle(a, fresh=not first)
            first = False
            await news_update_open()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("News loop lỗi")
        await asyncio.sleep(60)


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


# ─────────────────────────── Strategy engine (Pullback H4, NR7 H4, Breakout H4) ───────────────────────────
def has_open_signal(system: str, symbol: str) -> bool:
    """Mỗi symbol chỉ giữ 1 lệnh mở/hệ thống tại 1 thời điểm — tránh bắn tín hiệu trùng khi
    điều kiện entry vẫn còn đúng ở nhiều nến liên tiếp (VD nhiều nến xác nhận sát nhau)."""
    return any(s["symbol"] == symbol and s["status"] in ("open", "pending") for s in strategy_signals[system])


def wilder_atr(h: np.ndarray, l: np.ndarray, c: np.ndarray, n: int = 14) -> np.ndarray:
    pc = np.r_[c[0], c[:-1]]
    tr = np.maximum(h - l, np.maximum(abs(h - pc), abs(l - pc)))
    tr[0] = h[0] - l[0]
    return pd.Series(tr).ewm(alpha=1.0 / n, adjust=False).mean().to_numpy()


def resolve_limit(df: pd.DataFrame, placed: int, limit: float, sl: float, tp: float, bull: bool, expires: int, tstop: int = 0, tmfe: float = 0.0,
                  tf_sec: int | None = None, hold: int = 0, known_fill: int | None = None):
    """Lệnh limit đặt lúc `placed` (giờ đóng nến tín hiệu), hiệu lực tới `expires`. Khớp khi giá chạm limit; nếu chính nến khớp chạm SL
    -> thua (bảo thủ, TP chưa xét ở nến khớp). Sau đó SL kiểm tra trước TP như resolve_outcome.
    tstop > 0: sau tstop nến kể từ nến khớp mà giá chưa đi được tmfe R -> đóng ở giá đóng nến thứ tstop (chỉ xét nến đã đóng).
    hold > 0: sau hold nến kể từ nến khớp mà chưa chạm SL/TP -> đóng ở giá đóng (thoát theo thời gian).
    Trả về (status, fill_time, exit_time, exit_price) với status in pending/expired/open/win/loss."""
    fill = known_fill
    risk = abs(limit - sl)
    mfe, nb = 0.0, 0
    now = time.time()
    tf_sec = tf_sec or TF_SECONDS["4h"]
    # Lệnh đã khớp thì tính TIẾP từ nến khớp, không suy lại từ đầu: nếu để nguyên, một lệnh
    # giữ lâu hơn cửa sổ nến tải về sẽ có nến đầu tiên thoả ts >= expires và bị đổi nhầm
    # thành "expired" (mất trắng khỏi sổ, mà toàn rơi vào lệnh thắng lớn đang chạy dài).
    # Đã biết lệnh khớp lúc nào thì quét từ đó; nhánh "expired" nằm trong `if fill is None`
    # nên không bao giờ chạm tới nữa. Nếu nến khớp đã trôi khỏi cửa sổ, ta vẫn quét phần
    # còn thấy được để bắt SL/TP, thay vì kết luận sai.
    for row in df[df["ts"] >= (placed if fill is None else fill)].itertuples():
        if fill is None:
            if row.ts >= expires:
                return "expired", None, None, None
            if (row.low <= limit) if bull else (row.high >= limit):
                fill = int(row.ts)
                if (row.low <= sl) if bull else (row.high >= sl):
                    return "loss", fill, fill, sl
            continue
        if row.ts == fill:
            continue                              # nến khớp không tính vào nb/mfe (giữ như cũ)
        hit_sl = row.low <= sl if bull else row.high >= sl
        hit_tp = row.high >= tp if bull else row.low <= tp
        if hit_sl or hit_tp:
            return ("loss" if hit_sl else "win"), fill, int(row.ts), (sl if hit_sl else tp)
        nb += 1
        mfe = max(mfe, ((row.high - limit) if bull else (limit - row.low)) / risk)
        if tstop and nb >= tstop and mfe < tmfe and row.ts + tf_sec <= now:
            px = float(row.close)
            return ("win" if (px - limit) * (1 if bull else -1) > 0 else "loss"), fill, int(row.ts), px
        if hold and nb >= hold and row.ts + tf_sec <= now:
            px = float(row.close)
            return ("win" if (px - limit) * (1 if bull else -1) > 0 else "loss"), fill, int(row.ts), px
    if fill is None:
        return ("expired" if time.time() >= expires else "pending"), None, None, None
    return "open", fill, None, None


def limit_signal(system: str, symbol: str, h4: pd.DataFrame, placed: int, bull: bool, limit: float, sl: float, rr: float, kind: str,
                 tstop: int = 0, tmfe: float = 0.0, tf: str = "4h", exp_bars: int = 0, hold: int = 0,
                 max_risk: float = 0.12) -> dict | None:
    risk = abs(limit - sl)
    if risk <= 0 or (sl >= limit if bull else sl <= limit) or not (0.003 <= risk / limit <= max_risk):
        return None
    tp = limit + rr * risk if bull else limit - rr * risk
    tf_sec = TF_SECONDS[tf]
    expires = placed + (exp_bars or LIMIT_BARS) * tf_sec
    status, fill_time, exit_time, exit_price = resolve_limit(h4, placed, limit, sl, tp, bull, expires, tstop, tmfe, tf_sec, hold)
    r_real = round((exit_price - limit) * (1 if bull else -1) / risk, 3) if status in ("win", "loss") and exit_price is not None else None
    return {
        "tstop": tstop, "tmfe": tmfe, "r": r_real, "hold_limit": hold, "tf_sec": tf_sec,
        "id": f"{system}-{symbol}-{placed}-{'L' if bull else 'S'}", "system": system, "symbol": symbol, "timeframe": tf,
        "direction": "bullish" if bull else "bearish", "zone_kind": kind, "order": "limit",
        "entry": float(limit), "sl": float(sl), "tp": float(tp), "rr": rr, "entry_time": placed, "expires_at": expires, "fill_time": fill_time,
        "detected_at": int(time.time()), "status": status,
        "closed_at": int(time.time()) if status not in ("open", "pending") else None,
        "exit_time": exit_time, "exit_price": exit_price,
    }


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


def resolve_trail(df: pd.DataFrame, entry_time: int, entry: float, sl0: float, bull: bool, trail: float):
    """Trailing stop kiểu chandelier theo giá đóng cửa: stop = giá đóng tốt nhất -/+ trail x rủi ro ban đầu, chỉ dời theo
    hướng có lợi. Stop được đánh giá trước khi cập nhật bằng nến hiện tại (giống backtest). Trả về
    (status, exit_time, exit_price, stop_hiện_tại, R). Tính lại từ lúc entry mỗi lần gọi nên không cần lưu trạng thái trung gian."""
    risk = abs(entry - sl0)
    stop, best = sl0, entry
    for row in df[df["ts"] >= entry_time].itertuples():
        if (row.low <= stop) if bull else (row.high >= stop):
            r = ((stop - entry) if bull else (entry - stop)) / risk
            return ("win" if r > 0 else "loss"), int(row.ts), stop, stop, r
        best = max(best, row.close) if bull else min(best, row.close)
        new_stop = best - trail * risk if bull else best + trail * risk
        stop = max(stop, new_stop) if bull else min(stop, new_stop)
    return "open", None, None, stop, None


def check_pb_signal(symbol: str, h4: pd.DataFrame) -> dict | None:
    """Xu hướng EMA50 vs EMA200 (H4). Mua: giá đóng vừa cắt lên EMA20 sau khi nến trước đóng dưới EMA20. Bán ngược lại.
    SL = cực trị PB_SL_BARS nến gần nhất, không có TP cố định — thoát bằng trailing stop PB_TRAIL R."""
    d = h4.iloc[:-1].reset_index(drop=True)
    if len(d) < 300:
        return None
    c = d["close"].astype(float)
    e20, e50, e200 = (c.ewm(span=n, adjust=False).mean() for n in (20, 50, 200))
    i = len(d) - 1
    ci, pc, pe20 = float(c.iloc[i]), float(c.iloc[i - 1]), float(e20.iloc[i - 1])
    if e50.iloc[i] > e200.iloc[i] and pc < pe20 and ci > e20.iloc[i]:
        bull, sl = True, float(d["low"].iloc[i - PB_SL_BARS:i + 1].min())
    elif e50.iloc[i] < e200.iloc[i] and pc > pe20 and ci < e20.iloc[i]:
        bull, sl = False, float(d["high"].iloc[i - PB_SL_BARS:i + 1].max())
    else:
        return None
    risk = abs(ci - sl)
    if PB_ENTRY_ATR <= 0 and ((bull and sl >= ci) or (not bull and sl <= ci) or risk / ci < PB_MIN_RISK or risk / ci > PB_MAX_RISK):
        return None

    # ---- filter đã kiểm chứng: xu hướng non, chưa kéo xa EMA200, nến xác nhận có thân
    sign = np.sign((e50 - e200).to_numpy())
    age = 0
    for k in range(i - 1, -1, -1):
        if sign[k] != sign[i]:
            break
        age += 1
    if age > PB_MAX_AGE:
        return None
    hi, lo, op = d["high"].astype(float).to_numpy(), d["low"].astype(float).to_numpy(), d["open"].astype(float).to_numpy()
    cl = c.to_numpy()
    prev = np.r_[cl[0], cl[:-1]]
    tr = np.maximum(hi - lo, np.maximum(abs(hi - prev), abs(lo - prev)))
    atr14 = float(tr[i - 13:i + 1].mean())
    if atr14 <= 0 or (1 if bull else -1) * (ci - float(e200.iloc[i])) / atr14 > PB_MAX_DIST200:
        return None
    rng = hi[i] - lo[i]
    if rng <= 0 or abs(ci - op[i]) / rng < PB_MIN_BODY:
        return None

    entry_time = int(d["ts"].iloc[i]) + TF_SECONDS["4h"]
    if PB_ENTRY_ATR > 0:
        if bb_width_pctl(cl, i) < PB_MIN_BBW_PCTL:
            return None
        a = float(wilder_atr(hi, lo, cl)[i])
        sign = 1 if bull else -1
        limit = ci - sign * PB_ENTRY_ATR * a
        return limit_signal("pb4h", symbol, h4, entry_time, bull, limit, limit - sign * PB_SL_ATR * a, PB_RR, "Pullback EMA20 · limit",
                            PB_TSTOP, PB_TMFE)
    sid = f"pb4h-{symbol}-{entry_time}-{'L' if bull else 'S'}"
    if PB_EXIT == "tp":
        tp = ci + PB_RR * risk if bull else ci - PB_RR * risk
        status, exit_time, exit_price = resolve_outcome(h4, entry_time, sl, tp, bull)
        return {
            "id": sid, "system": "pb4h", "symbol": symbol, "timeframe": "4h",
            "direction": "bullish" if bull else "bearish", "zone_kind": "Pullback EMA20",
            "entry": ci, "sl": sl, "tp": tp, "rr": PB_RR, "entry_time": entry_time,
            "detected_at": int(time.time()), "status": status,
            "closed_at": int(time.time()) if status != "open" else None,
            "exit_time": exit_time, "exit_price": exit_price,
        }
    status, exit_time, exit_price, stop, r = resolve_trail(h4, entry_time, ci, sl, bull, PB_TRAIL)
    return {
        "id": f"pb4h-{symbol}-{entry_time}-{'L' if bull else 'S'}", "system": "pb4h", "symbol": symbol, "timeframe": "4h",
        "direction": "bullish" if bull else "bearish", "zone_kind": "Pullback EMA20",
        "entry": ci, "sl": stop, "sl0": sl, "tp": None, "rr": None, "trail": PB_TRAIL, "r": r,
        "entry_time": entry_time, "detected_at": int(time.time()), "status": status,
        "closed_at": int(time.time()) if status != "open" else None,
        "exit_time": exit_time, "exit_price": exit_price,
    }


def check_bo_signal(symbol: str, h4: pd.DataFrame, rr: float = BO_RR, system: str = "bo4h") -> dict | None:
    """Nến H4 vừa đóng phá vỡ biên BO_BOX nến trước >= 0.2 ATR (thân >= BO_MIN_BODY biên độ), vào NGAY tại giá đóng, SL đầu kia nến phá vỡ -/+ 0.25 ATR,
    TP BO_RR x rủi ro. Lọc: volume, phá xa biên, giá cùng phía EMA200, biến động không nén, BTC (H4) cùng hướng so với EMA200."""
    d = h4.iloc[:-1].reset_index(drop=True)
    if len(d) < 300:
        return None
    o, h, l, c, v = (d[k].astype(float).to_numpy() for k in ("open", "high", "low", "close", "volume"))
    b = len(d) - 1
    pc = np.r_[c[0], c[:-1]]
    tr = np.maximum(h - l, np.maximum(abs(h - pc), abs(l - pc)))
    atr = pd.Series(tr).rolling(14).mean().to_numpy()
    a = float(atr[b])
    if not a > 0 or h[b] <= l[b]:
        return None
    if abs(c[b] - o[b]) / (h[b] - l[b]) < BO_MIN_BODY:
        return None
    hi_prev, lo_prev = float(h[b - BO_BOX:b].max()), float(l[b - BO_BOX:b].min())
    if c[b] > hi_prev + 0.2 * a and c[b] > o[b]:
        bull, level, sl = True, hi_prev, float(l[b]) - 0.25 * a
    elif c[b] < lo_prev - 0.2 * a and c[b] < o[b]:
        bull, level, sl = False, lo_prev, float(h[b]) + 0.25 * a
    else:
        return None
    ci = float(c[b])
    risk = abs(ci - sl)
    if system != "bo4h" and (risk / ci * 100 < 0.2 or risk / ci * 100 > 8.0):
        return None
    sign = 1 if bull else -1
    volx = v[b] / v[b - 20:b].mean() if v[b - 20:b].mean() > 0 else 0
    if volx < BO_MIN_VOLX or sign * (ci - level) / a < BO_MIN_BRK:
        return None
    e200 = float(pd.Series(c).ewm(span=200, adjust=False).mean().iloc[b])
    if sign * (ci - e200) / a <= BO_MIN_DIST200:
        return None
    atr_pct = atr / c * 100
    ratio = atr_pct[b] / np.nanmean(atr_pct[b - 249:b + 1])
    if not ratio >= BO_MIN_ATR_RATIO:
        return None
    if btc_h4_regime.get(int(d["ts"].iloc[b])) != sign:
        return None
    if system == "bo4h" and BO_ENTRY_ATR >= 0:   # ek = 0 -> limit đặt ĐÚNG giá đóng (vẫn qua nhánh limit để giữ đủ bộ lọc + SL cực trị 10 nến + thoát theo thời gian)
        if BO_LONG_ONLY and not bull:
            return None
        if b < 180:
            return None
        if adx14(h, l, c, b) < BO_MIN_ADX:
            return None
        btc_r30 = btc_h4_r30.get(int(d["ts"].iloc[b]))
        if btc_r30 is None or sign * ((c[b] / c[b - 180] - 1) - btc_r30) > BO_MAX_RS:
            return None
        wa = float(wilder_atr(h, l, c)[b])
        limit = ci - sign * BO_ENTRY_ATR * wa
        lo_ = max(b - BO_SL_BARS + 1, 0)
        sl2 = (float(l[lo_:b + 1].min()) if bull else float(h[lo_:b + 1].max())) - sign * 0.1 * wa
        return limit_signal("bo4h", symbol, h4, int(d["ts"].iloc[b]) + TF_SECONDS["4h"], bull, limit, sl2, rr, "Breakout + volume · limit",
                            BO_TSTOP, BO_TMFE)
    tp = ci + rr * risk if bull else ci - rr * risk
    entry_time = int(d["ts"].iloc[b]) + TF_SECONDS["4h"]
    status, exit_time, exit_price = resolve_outcome(h4, entry_time, sl, float(tp), bull)
    return {
        "id": f"{system}-{symbol}-{entry_time}-{'L' if bull else 'S'}", "system": system, "symbol": symbol, "timeframe": "4h",
        "direction": "bullish" if bull else "bearish", "zone_kind": "Breakout + volume",
        "entry": ci, "sl": sl, "tp": float(tp), "rr": rr, "entry_time": entry_time,
        "detected_at": int(time.time()), "status": status,
        "closed_at": int(time.time()) if status != "open" else None,
        "exit_time": exit_time, "exit_price": exit_price,
    }


def resolve_time(df: pd.DataFrame, entry_time: int, sl: float, tp: float, bull: bool, hold_bars: int, tf_sec: int):
    """Như resolve_outcome nhưng có thoát theo thời gian: sau hold_bars nến (đã đóng) kể từ khi vào -> đóng ở giá đóng nến đó."""
    nb = 0
    now = time.time()
    for row in df[df["ts"] >= entry_time].itertuples():
        hit_sl = row.low <= sl if bull else row.high >= sl
        hit_tp = row.high >= tp if bull else row.low <= tp
        if hit_sl or hit_tp:
            return ("loss" if hit_sl else "win"), int(row.ts), (sl if hit_sl else tp)
        nb += 1
        if nb >= hold_bars and row.ts + tf_sec <= now:
            return "time", int(row.ts), float(row.close)
    return "open", None, None


def check_vc_signal(symbol: str, h4: pd.DataFrame) -> dict | None:
    """BÁN tiếp đà khi nến H4 vừa đóng lúc 16:00/20:00 UTC có volume > VC_VOLX x TB 20 nến trước và là nến GIẢM thứ 3 liên tiếp (bán tháo có volume)."""
    d = h4.iloc[:-1].reset_index(drop=True)
    if len(d) < 60:
        return None
    o, h, l, c, v = (d[k].astype(float).to_numpy() for k in ("open", "high", "low", "close", "volume"))
    i = len(d) - 1
    close_ts = int(d["ts"].iloc[i]) + TF_SECONDS["4h"]
    if (close_ts // 3600) % 24 not in (16, 20):
        return None
    va = v[i - 20:i].mean()
    if not (va > 0 and v[i] > VC_VOLX * va and c[i] < c[i - 1] < c[i - 2] < c[i - 3]):
        return None
    a = float(wilder_atr(h, l, c)[i])
    ci = float(c[i])
    if not (a > 0 and 0.003 <= VC_SL_ATR * a / ci <= 0.12):
        return None
    sl, tp = ci + VC_SL_ATR * a, ci - VC_TP_ATR * a
    status, exit_time, exit_price = resolve_time(h4, close_ts, sl, tp, False, VC_HOLD, TF_SECONDS["4h"])
    r = None
    if status in ("win", "loss", "time"):
        r = round((ci - exit_price) / (sl - ci), 3)
        status = "win" if r > 0 else "loss"
    return {
        "id": f"vc4h-{symbol}-{close_ts}-S", "system": "vc4h", "symbol": symbol, "timeframe": "4h", "direction": "bearish",
        "zone_kind": f"Bán tháo volume x{v[i] / va:.1f} · tối đa 3 ngày", "entry": ci, "sl": sl, "tp": tp, "rr": round(VC_TP_ATR / VC_SL_ATR, 2),
        "entry_time": close_ts, "hold_bars": VC_HOLD, "detected_at": int(time.time()), "status": status, "r": r,
        "closed_at": int(time.time()) if status not in ("open",) else None, "exit_time": exit_time, "exit_price": exit_price,
    }


def check_snr_signal(symbol: str, d1: pd.DataFrame) -> dict | None:
    """Malaysian SNR khung ngày. Chỉ trả tín hiệu khi nến ngày VỪA ĐÓNG là nến phá vỡ làm mức đổi vai trò
    (kháng cự thành hỗ trợ hoặc ngược lại) -> đặt LIMIT tại mức đó, chờ giá quay lại trong SNR_EXP_D ngày.
    Quét TIẾN một lần y hệt backtest: swing xác nhận sau SNR_L nến -> lần chạm lại đầu tiên -> nến đóng xuyên mức -> đổi vai trò."""
    d = d1.iloc[:-1].reset_index(drop=True)          # bỏ nến đang chạy
    if len(d) < SNR_MA + 60:
        return None
    h, l, c = (d[k].astype(float).to_numpy() for k in ("high", "low", "close"))
    n = len(c)
    pc = np.r_[c[0], c[:-1]]
    tr = np.maximum(h - l, np.maximum(abs(h - pc), abs(l - pc)))
    atr = pd.Series(tr).rolling(14).mean().to_numpy()
    ma = pd.Series(c).rolling(SNR_MA).mean().to_numpy()
    atrp = atr / c
    atr_ratio = atrp / pd.Series(atrp).rolling(180, min_periods=60).mean().to_numpy()
    d1_fast = pd.Series(c).ewm(span=SNR_D1_FAST, adjust=False).mean().to_numpy()
    d1_slow = pd.Series(c).ewm(span=SNR_D1_SLOW, adjust=False).mean().to_numpy()
    last = n - 1                                      # nến ngày vừa đóng
    if not np.isfinite(atr[last]) or atr[last] <= 0 or not np.isfinite(ma[last]):
        return None
    L = SNR_L
    hmax = pd.Series(h).rolling(2 * L + 1).max().to_numpy()
    lmin = pd.Series(l).rolling(2 * L + 1).min().to_numpy()
    for t0 in range(max(2 * L + 6, last - 60), last):  # mức xác nhận trong 60 ngày gần đây
        p = t0 - L
        if not np.isfinite(atr[p]) or atr[p] <= 0:
            continue
        cands = []
        if l[p] == lmin[t0] and (h[p:t0 + 1].max() - l[p]) >= SNR_MOVE * atr[p]:
            cands.append((l[p], 1))
        if h[p] == hmax[t0] and (h[p] - l[p:t0 + 1].min()) >= SNR_MOVE * atr[p]:
            cands.append((h[p], -1))
        for px, kind in cands:
            side, first = kind, None
            for k in range(t0 + 1, last + 1):
                if not np.isfinite(atr[k]):
                    continue
                if first is None and k <= t0 + SNR_EXP_D and ((side > 0 and l[k] <= px) or (side < 0 and h[k] >= px)):
                    first = k                          # lần chạm lại đầu tiên (vai trò gốc) — không vào lệnh
                    continue
                if first is None:
                    continue
                broke = (c[k] < px - SNR_BREAK_ATR * atr[k]) if kind > 0 else (c[k] > px + SNR_BREAK_ATR * atr[k])
                if not broke:
                    continue
                if k != last:
                    break                              # cú phá vỡ không phải hôm nay -> mức này đã xử lý rồi
                side = -kind
                if side != (1 if c[k] > ma[k] else -1):
                    break                              # nến phá vỡ phải đóng cùng phía MA50 với chiều lệnh
                if atr_ratio[k] < SNR_MIN_ATR_RATIO:
                    break                              # thị trường đang nén -> bỏ qua
                if side != (1 if d1_fast[k] > d1_slow[k] else -1):
                    break                              # ngược xu hướng khung ngày -> bỏ qua
                bull = side > 0
                sl = px - side * SNR_SLK * atr[k]
                return limit_signal("snr1d", symbol, d1, int(d["ts"].iloc[last]) + TF_SECONDS["1d"], bull, float(px), float(sl), SNR_RR,
                                    "SNR mức đổi vai trò · limit", tf="1d", exp_bars=SNR_EXP_D, hold=SNR_HOLD_D, max_risk=0.15)
    return None


def check_st_signal(symbol: str, h4: pd.DataFrame, rr: float = ST_RR, system: str = "st4h") -> dict | None:
    """Supertrend (hl2 +- 3 x ATR14 Wilder) vừa đổi hướng ở nến H4 đóng cửa, thuận xu hướng EMA50/EMA200 (H4). SL = ST_SL_ATR x ATR14, TP = ST_RR x rủi ro."""
    d = h4.iloc[:-1].reset_index(drop=True)
    if len(d) < 300:
        return None
    h, l, c = (d[k].astype(float).to_numpy() for k in ("high", "low", "close"))
    pc = np.r_[c[0], c[:-1]]
    tr = np.maximum(h - l, np.maximum(abs(h - pc), abs(l - pc)))
    tr[0] = h[0] - l[0]
    atr = pd.Series(tr).ewm(alpha=1.0 / 14, adjust=False).mean().to_numpy()
    hl2 = (h + l) / 2
    ub, lb = hl2 + 3 * atr, hl2 - 3 * atr
    up, lo, dr = ub.copy(), lb.copy(), np.ones(len(c), dtype=int)
    for i in range(1, len(c)):
        lo[i] = lb[i] if (lb[i] > lo[i - 1] or c[i - 1] < lo[i - 1]) else lo[i - 1]
        up[i] = ub[i] if (ub[i] < up[i - 1] or c[i - 1] > up[i - 1]) else up[i - 1]
        dr[i] = (-1 if c[i] > up[i] else 1) if dr[i - 1] == 1 else (1 if c[i] < lo[i] else -1)
    i = len(c) - 1
    e50 = pd.Series(c).ewm(span=50, adjust=False).mean().iloc[i]
    e200 = pd.Series(c).ewm(span=200, adjust=False).mean().iloc[i]
    if dr[i] == -1 and dr[i - 1] == 1 and e50 > e200:
        bull = True
    elif dr[i] == 1 and dr[i - 1] == -1 and e50 < e200:
        bull = False
    else:
        return None
    ci = float(c[i])
    if ST4_SHORT_ONLY and bull:
        return None
    # st4h VÀ st15 dùng CHUNG cách vào lệnh (limit ST4_ENTRY_ATR tốt hơn, SL ST4_SL_ATR x ATR),
    # chỉ khác TP. Trước đây st15 rơi xuống nhánh vào giá đóng + SL 2 ATR -> kỳ vọng -0.007R
    # (backtest bản limit: +0.396R). Xem ghi chú ở phần cấu hình ST.
    if ST4_ENTRY_ATR > 0:
        if ST_MAX_STOCH < 100:
            k_ = stoch_k(h, l, c, i)
            if (k_ if bull else 100 - k_) >= ST_MAX_STOCH:
                return None
        sign = 1 if bull else -1
        limit = ci - sign * ST4_ENTRY_ATR * float(atr[i])
        return limit_signal(system, symbol, h4, int(d["ts"].iloc[i]) + TF_SECONDS["4h"], bull, limit, limit - sign * ST4_SL_ATR * float(atr[i]), rr,
                            "Supertrend flip · limit")
    risk = ST_SL_ATR * float(atr[i])
    if risk / ci < ST_MIN_RISK or risk / ci > ST_MAX_RISK:
        return None
    sl = ci - risk if bull else ci + risk
    tp = ci + rr * risk if bull else ci - rr * risk
    entry_time = int(d["ts"].iloc[i]) + TF_SECONDS["4h"]
    status, exit_time, exit_price = resolve_outcome(h4, entry_time, sl, tp, bull)
    return {
        "id": f"{system}-{symbol}-{entry_time}-{'L' if bull else 'S'}", "system": system, "symbol": symbol, "timeframe": "4h",
        "direction": "bullish" if bull else "bearish", "zone_kind": "Supertrend flip",
        "entry": ci, "sl": sl, "tp": tp, "rr": rr, "entry_time": entry_time,
        "detected_at": int(time.time()), "status": status,
        "closed_at": int(time.time()) if status != "open" else None,
        "exit_time": exit_time, "exit_price": exit_price,
    }


async def refresh_btc_regime():
    """Cập nhật chế độ BTC H4 (đóng trên/dưới EMA200) cho filter của Breakout H4."""
    try:
        df = await fetch_ohlc("BTCUSDT", "4h", 700)
    except Exception:
        return
    d = df.iloc[:-1]
    c = d["close"].astype(float)
    reg = np.where(c > c.ewm(span=200, adjust=False).mean(), 1, -1)
    r30 = (c / c.shift(180) - 1).to_numpy()
    for ts_, r, x in zip(d["ts"].to_numpy()[-60:], reg[-60:], r30[-60:]):
        btc_h4_regime[int(ts_)] = int(r)
        if not np.isnan(x):
            btc_h4_r30[int(ts_)] = float(x)


async def update_open_signals(system: str, symbol: str, df: pd.DataFrame):
    """Kiểm tra các tín hiệu đang mở của symbol này."""
    for sig in list(strategy_signals[system]):
        if sig["symbol"] != symbol or sig["status"] not in ("open", "pending"):
            continue
        mark = float(df["close"].iloc[-1])
        if sig.get("mark") != mark:
            sig["mark"], sig["mark_time"] = mark, int(time.time())
            await manager.broadcast({"type": "strategy_update", "data": sig})
        bull = sig["direction"] == "bullish"
        if sig.get("hold_bars"):
            status, exit_time, exit_price = resolve_time(df, sig["entry_time"], sig["sl"], sig["tp"], bull, sig["hold_bars"], TF_SECONDS[sig["timeframe"]])
            if status != "open":
                r = (exit_price - sig["entry"]) * (1 if bull else -1) / abs(sig["entry"] - sig["sl"])
                sig.update(status="win" if r > 0 else "loss", exit_time=exit_time, exit_price=exit_price, closed_at=int(time.time()), r=round(r, 3))
                save_strategy_state()
                await manager.broadcast({"type": "strategy_update", "data": sig})
            continue
        if sig.get("order") == "limit":
            status, fill_time, exit_time, exit_price = resolve_limit(df, sig["entry_time"], sig["entry"], sig["sl"], sig["tp"], bull, sig["expires_at"],
                                                                     sig.get("tstop", 0), sig.get("tmfe", 0.0),
                                                                     sig.get("tf_sec") or TF_SECONDS.get(sig["timeframe"], TF_SECONDS["4h"]),
                                                                     sig.get("hold_limit", 0), sig.get("fill_time"))
            if status != sig["status"] or fill_time != sig.get("fill_time"):
                if status in ("win", "loss") and exit_price is not None:
                    sig["r"] = round((exit_price - sig["entry"]) * (1 if bull else -1) / abs(sig["entry"] - sig["sl"]), 3)
                sig.update(status=status, fill_time=fill_time, exit_time=exit_time, exit_price=exit_price,
                           closed_at=int(time.time()) if status not in ("open", "pending") else None)
                save_strategy_state()
                await manager.broadcast({"type": "strategy_update", "data": sig})
            continue
        if sig.get("trail"):
            status, exit_time, exit_price, stop, r = resolve_trail(df, sig["entry_time"], sig["entry"], sig["sl0"], bull, sig["trail"])
            moved = abs(stop - sig["sl"]) > 1e-12
            sig["sl"] = stop
            if status != "open":
                sig.update(status=status, closed_at=int(time.time()), exit_time=exit_time, exit_price=exit_price, r=r)
            if status != "open" or moved:
                save_strategy_state()
                await manager.broadcast({"type": "strategy_update", "data": sig})
            continue
        status, exit_time, exit_price = resolve_outcome(df, sig["entry_time"], sig["sl"], sig["tp"], bull)
        if status != "open":
            sig["status"] = status
            sig["closed_at"] = int(time.time())
            sig["exit_time"] = exit_time  # thời điểm nến chạm SL/TP (khác closed_at = lúc server phát hiện)
            sig["exit_price"] = exit_price
            save_strategy_state()
            await manager.broadcast({"type": "strategy_update", "data": sig})


funding_rates: dict[str, float] = {}


async def funding_loop():
    """Cache funding rate hiện tại của mọi perp (1 request cho cả sàn) để lọc lệnh ngược đám đông."""
    while True:
        try:
            rows = await volwatch_exchange.fapiPublicGetPremiumIndex()
            funding_rates.clear()
            for x in rows:
                try:
                    funding_rates[x["symbol"]] = float(x.get("lastFundingRate") or 0.0)
                except (TypeError, ValueError):
                    continue
            log.info("Funding: cập nhật %d coin", len(funding_rates))
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("funding loop lỗi")
        await asyncio.sleep(FUND_INTERVAL)


def btc_trend_ok(system: str, direction: str, entry_time: int) -> bool:
    """True nếu BTC đã đi cùng chiều lệnh trong BTC_TREND_DAYS ngày qua — xem BTC_TREND_SYSTEMS."""
    if system not in BTC_TREND_SYSTEMS or not btc_h4_r30:
        return True
    r = btc_h4_r30.get(int(entry_time) - TF_SECONDS["4h"])
    if r is None:
        r = btc_h4_r30[max(btc_h4_r30)]               # chưa có đúng nến -> lấy giá trị mới nhất
    return r * (1 if direction == "bullish" else -1) > 0


def confluence_count(system: str, symbol: str, direction: str, entry_time: int) -> int:
    """Số hệ thống KHÁC đã báo cùng coin, cùng chiều trong CONFLUENCE_H giờ TRƯỚC đó. Chỉ để hiển thị."""
    lo = int(entry_time) - CONFLUENCE_H * 3600
    seen = set()
    same = SAME_SIGNAL.get(system, {system})      # st4h/st15 là CÙNG một tín hiệu Supertrend
    for other in STRATEGY_SYSTEMS:
        if other in same:
            continue
        for s in strategy_signals[other]:
            if s["symbol"] == symbol and s["direction"] == direction and lo <= int(s["entry_time"]) <= int(entry_time):
                seen.add(other)
                break
    return len(seen)


def funding_ok(system: str, symbol: str, direction: str) -> bool:
    """True nếu funding đang NGƯỢC chiều lệnh (đám đông đứng phía đối diện) — xem FUND_SYSTEMS."""
    if system not in FUND_SYSTEMS:
        return True
    fr = funding_rates.get(symbol)
    if fr is None or fr == 0.0:
        return True                                   # chưa có dữ liệu -> không chặn
    return fr * (1 if direction == "bullish" else -1) < 0


async def scan_generic(system: str, symbol: str, timeframe: str, limit: int, check, sem: asyncio.Semaphore):
    async with sem:
        try:
            df = await fetch_ohlc(symbol, timeframe, limit)
        except Exception:
            log.warning("tải nến %s %s %s lỗi", system, symbol, timeframe, exc_info=True)
            return
    try:
        await update_open_signals(system, symbol, df)
    except Exception:
        # Trước đây lỗi ở đây thoát ra gather(return_exceptions=True) rồi bị lọc bỏ im lặng:
        # coin đó vừa không cập nhật lệnh đang mở, vừa không bao giờ sinh tín hiệu mới nữa.
        log.exception("cập nhật lệnh mở %s %s lỗi", system, symbol)
    try:
        sig = await asyncio.to_thread(check, symbol, df)
    except Exception:
        log.exception("check %s lỗi %s", system, symbol)
        return
    if sig and strategy_enabled["ny"] and system in NY_SYSTEMS and (int(sig["entry_time"]) // 3600) % 24 not in NY_HOURS:
        return None
    if sig and not funding_ok(system, symbol, sig["direction"]):
        log.info("Bỏ %s %s %s: funding %+.4f%% thuận chiều lệnh (đám đông cùng phía)",
                 system, symbol, sig["direction"], funding_rates.get(symbol, 0.0) * 100)
        return None
    if sig and not btc_trend_ok(system, sig["direction"], int(sig["entry_time"])):
        log.info("Bỏ %s %s %s: BTC %d ngày đi ngược chiều lệnh", system, symbol, sig["direction"], BTC_TREND_DAYS)
        return None
    return sig


async def emit_signal(system: str, symbol: str, sig: dict):
    if sig.get("direction") == "bullish" and symbol in news_flags and time.time() - news_flags[symbol]["ts"] < NEWS_BLOCK_DAYS * 86400:
        log.info("Bỏ lệnh MUA %s %s: có tin %s", system, symbol, news_flags[symbol]["type"])
        return
    if sig["id"] not in strategy_seen and not has_open_signal(system, symbol):
        n_conf = confluence_count(system, symbol, sig["direction"], int(sig["entry_time"]))
        if n_conf:
            sig["confluence"] = n_conf        # chỉ hiển thị: lệnh có đồng thuận có kỳ vọng cao hơn nhiều
        strategy_seen.add(sig["id"])
        strategy_signals[system].appendleft(sig)
        save_strategy_state()
        log.info("%s %s %s @ %s (SL %s / TP %s)", system.upper(), symbol, sig["direction"], sig["entry"], sig["sl"], sig["tp"])
        await manager.broadcast({"type": "strategy_signal", "data": sig})


async def generic_loop(system: str, timeframe: str, limit: int, check, interval: int, pre=None):
    sem = asyncio.Semaphore(CONCURRENCY)
    while True:
        t0 = time.time()
        if strategy_enabled[system]:
            try:
                if pre:
                    await pre()
                res = await asyncio.gather(*(scan_generic(system, s, timeframe, limit, check, sem) for s in active_symbols),
                                           return_exceptions=True)
                cands = [(s, r) for s, r in zip(active_symbols, res) if isinstance(r, dict)]
                book = raw_signals.setdefault(system, {})
                for s, r in cands:
                    book[(s, int(r["entry_time"]), r["direction"])] = int(r["entry_time"])
                cutoff = time.time() - 3 * 86400
                for k in [k for k, v in book.items() if v < cutoff]:
                    del book[k]
                for s, r in cands:
                    if system in CONSENSUS_SYSTEMS:
                        t = int(r["entry_time"])
                        n = sum(1 for (s2, t2, d2) in book if s2 != s and d2 == r["direction"] and t - CONSENSUS_WINDOW < t2 <= t)
                        need = max(1, math.ceil(CONSENSUS_FRAC * len(active_symbols)))
                        if n < need:
                            if r["id"] not in consensus_skipped:
                                consensus_skipped.add(r["id"])
                                log.info("Bỏ %s %s %s: chỉ %d coin khác cùng tín hiệu trong 24h (cần %d)", system, s, r["direction"], n, need)
                            continue
                        r["consensus"] = n
                        r["zone_kind"] = f'{r.get("zone_kind", "")} · đồng thuận {n + 1} coin'
                    await emit_signal(system, s, r)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("%s loop lỗi", system)
        else:
            # hệ thống đã tắt: không tìm tín hiệu mới nhưng vẫn theo dõi các lệnh đang mở / chờ khớp tới khi đóng
            for s in {x["symbol"] for x in strategy_signals[system] if x["status"] in ("open", "pending")}:
                try:
                    async with sem:
                        df = await fetch_ohlc(s, timeframe, limit)
                    await update_open_signals(system, s, df)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("%s cập nhật lệnh mở lỗi %s", system, s)
        await asyncio.sleep(max(30.0, interval - (time.time() - t0)))


# ─────────────────────────── HTF Zone alert ───────────────────────────
# Không phải chiến lược có TP/SL: chỉ báo khi bias HTF (theo cú phá cấu trúc BOS/CHoCH gần nhất) đang tăng/giảm và giá hồi về vùng
# OB / FVG / iFVG CÙNG hướng bias. Người dùng tự chờ CHoCH / engulfing / pin ở khung nhỏ. Backtest cho thấy kiểu vào lệnh này
# không có edge tự động (~0R), nên đây chỉ là công cụ theo dõi/kỷ luật.
ZONE_TFS = [t.strip() for t in os.getenv("ZONE_TFS", "4h,1d").split(",") if t.strip()]
ZONE_INTERVAL = int(os.getenv("ZONE_INTERVAL", "120"))
ZONE_SWING = {"1h": 8, "4h": 8, "1d": 5}
ZONE_MAX_AGE_BARS = {"1h": 240, "4h": 180, "1d": 60}   # bỏ qua vùng quá cũ
ZONE_COOLDOWN_BARS = {"1h": 12, "4h": 6, "1d": 3}     # gộp các lần chạm liên tiếp trong cùng 1 nhịp hồi: mỗi (coin, khung) tối đa 1 cảnh báo / N nến
zone_last: dict[tuple, int] = {}
# Giảm nhiễu (ước lượng backtest 90 ngày/10 coin: bỏ cả hai -> ~36 cảnh báo/ngày cho 63 coin H4, bật cả hai -> ~5/ngày):
ZONE_ALIGN_D1 = os.getenv("ZONE_ALIGN_D1", "1") == "1"        # cảnh báo H4/H1 chỉ khi bias D1 cùng hướng
ZONE_CONFLUENCE = os.getenv("ZONE_CONFLUENCE", "1") == "1"    # (OB/iFVG) chỉ báo khi vùng có OB (ước lượng: ~2/ngày cho 63 coin H4; đặt 0 để báo cả iFVG đơn lẻ: ~12/ngày)
# FVG thường KHÔNG còn báo theo kiểu cũ: chỉ báo "FVG★" khi đạt đủ 4 quy tắc (fvg_filter.py): thuận xu hướng HTF, gắn với BOS/CHoCH,
# nằm ở Discount/Premium, dùng một lần. Đặt ZONE_FVG_RULES=0 để tắt cảnh báo FVG★.
ZONE_FVG_RULES = os.getenv("ZONE_FVG_RULES", "1") == "1"
zone_d1: dict[str, pd.DataFrame] = {}   # nến D1 mới nhất của từng coin (khung lớn cho quy tắc xu hướng của FVG★ ở H4)
ZONE_STATE_FILE = Path(__file__).parent / "zone_state.json"
ZONE_KIND = {0: "OB", 1: "FVG", 2: "iFVG"}
zone_alerts: deque = deque(maxlen=200)
zone_seen: dict[str, int] = {}          # id vùng đã báo -> thời điểm báo (mỗi vùng chỉ báo 1 lần)
zone_cache: dict[tuple, dict] = {}      # (symbol, tf) -> vùng + bias, chỉ tính lại khi có nến HTF mới đóng
zone_first_pass = {"silent": False}     # lần chạy đầu tiên (chưa có file trạng thái): ghi nhận vùng đang chạm mà không báo


def save_zone_state():
    try:
        cutoff = int(time.time()) - 90 * 86400
        seen = {k: v for k, v in zone_seen.items() if v >= cutoff}
        ZONE_STATE_FILE.write_text(json.dumps({"alerts": list(zone_alerts), "seen": seen}), encoding="utf-8")
    except Exception:
        log.exception("Lưu %s lỗi", ZONE_STATE_FILE.name)


def load_zone_state():
    if not ZONE_STATE_FILE.exists():
        zone_first_pass["silent"] = True
        return
    try:
        data = json.loads(ZONE_STATE_FILE.read_text(encoding="utf-8"))
        zone_alerts.extend(reversed(data.get("alerts", [])))
        zone_seen.update(data.get("seen", {}))
    except Exception:
        log.exception("Đọc %s lỗi", ZONE_STATE_FILE.name)


def zone_struct_events(ohlc: pd.DataFrame, swl: int) -> list[dict]:
    swings = smc.swing_highs_lows(ohlc, swing_length=swl)
    struct = smc.bos_choch(ohlc, swings, close_break=True)
    events = []
    for i, row in struct.iterrows():
        if pd.notna(row["CHOCH"]) and row["CHOCH"] != 0:
            d = int(row["CHOCH"])
        elif pd.notna(row["BOS"]) and row["BOS"] != 0:
            d = int(row["BOS"])
        else:
            continue
        if pd.isna(row["BrokenIndex"]) or int(row["BrokenIndex"]) >= len(ohlc):
            continue
        events.append({"dir": d, "swing_idx": int(i), "broken_idx": int(row["BrokenIndex"])})
    events.sort(key=lambda e: e["broken_idx"])
    return events


def zone_build(df: pd.DataFrame, tf_sec: int, events: list[dict]) -> dict:
    """OB (nến cực trị giữa swing và nến phá, biết khi nến phá đóng), FVG (biết từ nến k+1) và iFVG (FVG bị đóng cửa xuyên thủng
    -> đảo vai trò). Vùng chết khi 1 nến ĐÓNG cửa xuyên cạnh xa."""
    h, l, c = (df[k].astype(float).to_numpy() for k in ("high", "low", "close"))
    ts = df["ts"].to_numpy()
    n = len(df)
    Z = {"top": [], "bot": [], "dir": [], "known": [], "dead": [], "kind": []}

    def add(kind, d, top, bot, kb):
        rest = c[kb + 1:]
        hit = np.where(rest < bot)[0] if d > 0 else np.where(rest > top)[0]
        dead = ts[kb + 1 + hit[0]] + tf_sec if len(hit) else np.inf
        for key, v in zip(Z, (top, bot, d, ts[kb] + tf_sec, dead, kind)):
            Z[key].append(v)
        return int(kb + 1 + hit[0]) if len(hit) else None

    for k in range(1, n - 1):
        for d, cond, top, bot in ((1, l[k + 1] > h[k - 1], l[k + 1], h[k - 1]), (-1, h[k + 1] < l[k - 1], l[k - 1], h[k + 1])):
            if cond and (top - bot) / c[k] >= 0.0002:
                m = add(1, d, top, bot, k + 1)
                if m is not None:
                    add(2, -d, top, bot, m)
    for ev in events:
        a, b = ev["swing_idx"] + 1, ev["broken_idx"]
        if b - a < 1:
            continue
        seg = np.arange(a, b)
        d = ev["dir"]
        k = seg[np.where(l[seg] == l[seg].min())[0][-1]] if d > 0 else seg[np.where(h[seg] == h[seg].max())[0][-1]]
        add(0, d, h[k], l[k], b)
    return {k: np.array(v) for k, v in Z.items()}


def zone_check(symbol: str, tf: str, df_full: pd.DataFrame):
    """df_full gồm cả nến đang chạy ở cuối. Trả về (alert, ids_mới) hoặc None."""
    tf_sec = TF_SECONDS[tf]
    closed = df_full.iloc[:-1].reset_index(drop=True)
    if len(closed) < 120:
        return None
    last_ts = int(closed["ts"].iloc[-1])
    cache = zone_cache.get((symbol, tf))
    if cache is None or cache["last_ts"] != last_ts:
        ohlc = closed[["open", "high", "low", "close", "volume"]]
        events = zone_struct_events(ohlc, ZONE_SWING[tf])
        if not events:
            return None
        e = events[-1]
        cache = {"last_ts": last_ts, "Z": zone_build(closed, tf_sec, events), "bias": e["dir"],
                 "bias_ts": int(closed["ts"].iloc[e["broken_idx"]]) + tf_sec,
                 "leg_start": int(closed["ts"].iloc[e["swing_idx"]])}   # chỉ xét vùng hình thành trong đợt sóng tạo cú phá cấu trúc này
        zone_cache[(symbol, tf)] = cache
    Z, d = cache["Z"], cache["bias"]
    if not len(Z["top"]):
        return None
    if int(time.time()) - zone_last.get((symbol, tf), 0) < ZONE_COOLDOWN_BARS[tf] * tf_sec:
        return None
    if ZONE_ALIGN_D1 and tf != "1d":
        d1 = zone_cache.get((symbol, "1d"))
        if d1 is None or d1["bias"] != d:
            return None
    live = df_full.iloc[-1]
    lo, hi, cl = float(live["low"]), float(live["high"]), float(live["close"])
    t = int(time.time())
    m = (Z["dir"] == d) & (Z["known"] <= t) & (Z["dead"] > t) & (Z["known"] >= last_ts + tf_sec - ZONE_MAX_AGE_BARS[tf] * tf_sec)
    m &= Z["known"] >= cache["leg_start"]
    m &= (Z["top"] >= lo) & (Z["bot"] <= hi)                     # nến đang chạy chạm vùng
    m &= (cl >= Z["bot"]) if d > 0 else (cl <= Z["top"])          # chưa bị xuyên thủng ở giá hiện tại
    found = []
    for z in np.where(m)[0]:
        if int(Z["kind"][z]) == 1:      # FVG thường: chỉ báo qua fvg_check (đủ 4 quy tắc)
            continue
        zid = f"{symbol}-{tf}-{d}-{int(Z['kind'][z])}-{Z['top'][z]:.10g}-{Z['bot'][z]:.10g}-{int(Z['known'][z])}"
        if zid not in zone_seen:
            found.append({"id": zid, "kind": ZONE_KIND[int(Z["kind"][z])], "top": float(Z["top"][z]), "bot": float(Z["bot"][z]),
                          "known": int(Z["known"][z])})
    if not found:
        return None
    # Chỉ gộp các vùng THỰC SỰ chồng nhau (hội tụ): lấy vùng hẹp nhất làm mốc, cộng các vùng giao với nó; hiển thị phần giao nhau.
    anchor = min(found, key=lambda x: x["top"] - x["bot"])
    group = [x for x in found if x["top"] >= anchor["bot"] and x["bot"] <= anchor["top"]]
    kinds = sorted({x["kind"] for x in group}, key=list(ZONE_KIND.values()).index)
    if ZONE_CONFLUENCE and not ("OB" in kinds or len(kinds) >= 2):
        return None
    top, bot = min(x["top"] for x in group), max(x["bot"] for x in group)
    if top <= bot:
        top, bot = anchor["top"], anchor["bot"]
    ids = [x["id"] for x in found]
    alert = {
        "id": "zone-" + anchor["id"], "symbol": symbol, "timeframe": tf, "direction": "bullish" if d > 0 else "bearish",
        "kinds": kinds, "zone_top": top, "zone_bottom": bot,
        "zone_since": min(x["known"] for x in group), "bias_since": cache["bias_ts"], "price": cl, "detected_at": t,
    }
    return alert, ids


def fvg_check(symbol: str, tf: str, df_full: pd.DataFrame, d1_full: pd.DataFrame | None):
    """Cảnh báo FVG★: FVG đạt đủ 4 quy tắc (thuận xu hướng D1/khung lớn, gắn với BOS/CHoCH, Discount/Premium, dùng một lần), chưa bị chạm ở các nến
    đã đóng, và nến đang chạy vừa chạm vào. Trả về (alert, ids) hoặc None."""
    if len(df_full) < 150 or (tf != "1d" and (d1_full is None or len(d1_full) < 150)):
        return None
    last_ts = int(df_full["ts"].iloc[-2])
    d1_ts = int(d1_full["ts"].iloc[-2]) if (tf != "1d" and d1_full is not None) else 0
    key = (symbol, tf, "fvg")
    cache = zone_cache.get(key)
    if cache is None or cache["last_ts"] != last_ts or cache["d1_ts"] != d1_ts:
        f = find_valid_fvgs(df_full, htf_df=None if tf == "1d" else d1_full, swing_len=ZONE_SWING[tf],
                            htf_swing_len=ZONE_SWING["1d"], max_age=ZONE_MAX_AGE_BARS[tf])
        cache = {"last_ts": last_ts, "d1_ts": d1_ts, "f": f[f["valid_now"]] if len(f) else f}
        zone_cache[key] = cache
    f = cache["f"]
    if f is None or f.empty:
        return None
    live = df_full.iloc[-1]
    lo, hi, cl = float(live["low"]), float(live["high"]), float(live["close"])
    hit = []
    for r in f.itertuples():
        if lo <= r.top and hi >= r.bottom and ((cl >= r.bottom) if r.dir > 0 else (cl <= r.top)):
            zid = f"{symbol}-{tf}-fvg-{r.dir}-{r.top:.10g}-{r.bottom:.10g}-{int(r.formed_time)}"
            if zid not in zone_seen:
                hit.append((r, zid))
    if not hit:
        return None
    r = max(hit, key=lambda x: x[0].formed_idx)[0]
    alert = {
        "id": "zone-" + max(hit, key=lambda x: x[0].formed_idx)[1], "symbol": symbol, "timeframe": tf,
        "direction": "bullish" if r.dir > 0 else "bearish", "kinds": ["FVG★"], "zone_top": float(r.top), "zone_bottom": float(r.bottom),
        "zone_since": int(r.formed_time), "bias_since": int(r.formed_time), "price": cl, "detected_at": int(time.time()),
        "note": f"Đủ 4 quy tắc · {'Discount' if r.dir > 0 else 'Premium'}",
    }
    return alert, [x[1] for x in hit]


async def zone_emit(alert: dict, ids: list[str], cooldown_key=None):
    now = int(time.time())
    for zid in ids:
        zone_seen[zid] = now
    if zone_first_pass["silent"]:
        return
    if cooldown_key:
        zone_last[cooldown_key] = now
    zone_alerts.appendleft(alert)
    save_zone_state()
    up = alert["direction"] == "bullish"
    tf, symbol = alert["timeframe"], alert["symbol"]
    log.info("ZONE %s %s %s -> %s %s-%s", symbol, tf, "TĂNG" if up else "GIẢM", "+".join(alert["kinds"]), alert["zone_bottom"], alert["zone_top"])
    await manager.broadcast({"type": "zone_alert", "data": alert})
    head = alert.get("note") or f"bias {'TĂNG' if up else 'GIẢM'}"
    await send_telegram(
        f"🎯 <b>{symbol}</b> {tf.upper()} {head} — giá hồi về vùng {'+'.join(alert['kinds'])} "
        f"{fmt_price(alert['zone_bottom'])}–{fmt_price(alert['zone_top'])}\nGiá hiện tại {fmt_price(alert['price'])}. "
        f"Chờ CHoCH / engulfing / pin ở M1–M15. Vô hiệu nếu nến {tf.upper()} đóng cửa {'dưới' if up else 'trên'} vùng.")


async def zone_scan_symbol(symbol: str, tf: str, sem: asyncio.Semaphore):
    async with sem:
        try:
            df = await fetch_ohlc(symbol, tf, 500)
        except Exception:
            return
    if tf == "1d":
        zone_d1[symbol] = df
    try:
        res = await asyncio.to_thread(zone_check, symbol, tf, df)
    except Exception:
        log.exception("zone_check lỗi %s %s", symbol, tf)
        res = None
    if res:
        await zone_emit(res[0], res[1], (symbol, tf))
    if ZONE_FVG_RULES:
        try:
            res = await asyncio.to_thread(fvg_check, symbol, tf, df, zone_d1.get(symbol))
        except Exception:
            log.exception("fvg_check lỗi %s %s", symbol, tf)
            return
        if res:
            await zone_emit(res[0], res[1])


async def zone_loop():
    sem = asyncio.Semaphore(CONCURRENCY)
    while True:
        t0 = time.time()
        if strategy_enabled.get("zone"):
            try:
                for tf in sorted(ZONE_TFS, key=lambda x: -TF_SECONDS[x]):   # khung lớn trước để có bias D1 cho lọc đồng thuận của H4
                    await asyncio.gather(*(zone_scan_symbol(s, tf, sem) for s in active_symbols), return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("zone loop lỗi")
            if zone_first_pass["silent"]:
                zone_first_pass["silent"] = False
                save_zone_state()
                log.info("Zone alert: đã ghi nhận %d vùng đang chạm (không báo lần đầu)", len(zone_seen))
        await asyncio.sleep(max(30.0, ZONE_INTERVAL - (time.time() - t0)))


# ─────────────────────────── App ───────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global exchange, kline_exchange, http, active_symbols
    load_strategy_state()
    load_zone_state()
    exchange = ccxt.binanceusdm({"enableRateLimit": True})
    kline_exchange = ccxt.binanceusdm({"enableRateLimit": True})
    global volwatch_exchange
    volwatch_exchange = ccxt.binanceusdm({"enableRateLimit": True})
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
    tasks = [asyncio.create_task(scanner_loop()),
             asyncio.create_task(kline_relay_loop()), asyncio.create_task(volwatch_loop()), asyncio.create_task(news_loop()), asyncio.create_task(feed_loop()),
             asyncio.create_task(zone_loop()),
             asyncio.create_task(generic_loop("pb4h", "4h", 600, check_pb_signal, PB_INTERVAL)),
             asyncio.create_task(generic_loop("bo4h", "4h", 600, check_bo_signal, BO_INTERVAL, pre=refresh_btc_regime)),
             asyncio.create_task(generic_loop("st4h", "4h", 600, check_st_signal, ST_INTERVAL, pre=refresh_btc_regime)),
             asyncio.create_task(generic_loop("vc4h", "4h", 300, check_vc_signal, ST_INTERVAL)),
             asyncio.create_task(generic_loop("st15", "4h", 600, partial(check_st_signal, rr=ST15_RR, system="st15"), ST_INTERVAL, pre=refresh_btc_regime)),
             asyncio.create_task(generic_loop("snr1d", "1d", 400, check_snr_signal, SNR_INTERVAL)),
             asyncio.create_task(funding_loop())]
    yield
    for task in tasks:
        task.cancel()
    for task in tasks:
        with suppress(asyncio.CancelledError):
            await task
    await exchange.close()
    await kline_exchange.close()
    await volwatch_exchange.close()
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


@app.get("/api/feed")
async def get_feed():
    return list(feed_items)


@app.get("/api/news-flags")
async def get_news_flags():
    return news_flags


@app.get("/api/volwatch")
async def get_volwatch():
    return volwatch_state


@app.get("/api/zone-alerts")
async def get_zone_alerts():
    return {"enabled": strategy_enabled.get("zone", True), "timeframes": ZONE_TFS, "alerts": list(zone_alerts)}


@app.get("/api/strategy/signals")
async def get_strategy_signals(system: str = Query("pb4h")):
    if system not in strategy_signals:
        raise HTTPException(400, f"system không hợp lệ: {system}")
    rr = {"pb4h": None if PB_EXIT == "trail" else PB_RR, "bo4h": BO_RR, "st4h": ST_RR, "st15": ST15_RR, "news": None, "vc4h": round(VC_TP_ATR / VC_SL_ATR, 2), "snr1d": SNR_RR}[system]
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
        await ws.send_json({"type": "zone_history", "data": list(zone_alerts)})
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
