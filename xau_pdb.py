"""VÀNG — phá đỉnh/đáy ngày hôm trước (XAUUSDT, khung 5m). Nghiên cứu 22/09/2026 (xau_c5.py, dữ liệu PAXG 2025+ và COMEX).

Quy tắc: nến 5m ĐẦU TIÊN trong ngày UTC đóng vượt đỉnh (thủng đáy) của ngày hôm trước -> vào theo chiều phá ở giá đóng,
CHỈ KHI (a) cùng chiều xu hướng ngày: giá đóng hôm trước trên EMA20 ngày thì chỉ mua, dưới thì chỉ bán, và (b) biên độ
hôm trước hẹp hơn trung vị biên độ 20 ngày gần nhất (tính cả hôm trước). SL = 0,3 x ATR14 ngày (của hôm trước), TP = 2 x SL,
tự đóng sau 3 ngày.
Cập nhật 22/09/2026 (xau_c5_robust.py): SL cố định 20 giá -> 0,3 x ATR ngày. Lý do cơ chế: vàng 2.600 -> 4.400 nên 20 giá tự hẹp
dần. Qua luật quyết định đặt trước: hơn ngẫu nhiên PAXG +0,342 -> +0,393, COMEX +0,149 -> +0,236, và COMEX 2024 (chưa dùng để
chọn) -0,177 -> +0,037. 108/108 biến thể lân cận của bản cũ đều dương trên cả hai nguồn (đỉnh rộng).
Lần phá đầu tiên mỗi phía mỗi ngày mới tính — nếu nó bị bộ lọc loại thì phía đó bỏ qua cả ngày.

Backtest (không phí): 131 lệnh/21 tháng, thắng 49,6%, +0,466R (~+9 giá/lệnh), MCPT p=0,010, walk-forward +0,359R,
COMEX hơn ngẫu nhiên +0,149R, có lợi thế ở cả lệnh mua lẫn bán. Cảnh báo: mẫu nhỏ; COMEX 2024 (ngoài giai đoạn chọn) ≈ hoà.
"""
import numpy as np
import pandas as pd

ATR_K, RR, HOLD_H = 0.3, 2.0, 72
DAY = 86400


def daily_context(d1: pd.DataFrame, day: int):
    """d1 = nến NGÀY ĐÃ ĐÓNG (ts giây, UTC). Trả về (đỉnh, đáy, hẹp?, xu_hướng_tăng?, ATR14) của ngày `day - 1`, hoặc None."""
    days = (d1["ts"].to_numpy(np.int64) // DAY)
    pos = np.where(days == day - 1)[0]
    if not len(pos):
        return None
    p = int(pos[0])
    if p < 19:
        return None
    h, l, c = (d1[k].astype(float).to_numpy() for k in ("high", "low", "close"))
    rng = h - l
    narrow = rng[p] < float(np.median(rng[p - 19:p + 1]))
    ema = pd.Series(c[:p + 1]).ewm(span=20, adjust=False).mean().iloc[-1]
    pc = np.r_[np.nan, c[:-1]]
    tr = np.nanmax(np.vstack([rng, np.abs(h - pc), np.abs(l - pc)]), axis=0)
    atr = pd.Series(tr[:p + 1]).ewm(alpha=1 / 14, adjust=False).mean().iloc[-1]
    return float(h[p]), float(l[p]), bool(narrow), bool(c[p] > ema), float(atr)


def check(d5: pd.DataFrame, d1: pd.DataFrame):
    """d5 = nến 5m ĐÃ ĐÓNG, d1 = nến ngày ĐÃ ĐÓNG. Xét nến 5m cuối cùng. Trả về (chiều, giá vào, SL, TP) hoặc None."""
    if len(d5) < 1:
        return None
    ts = d5["ts"].to_numpy(np.int64)
    c = d5["close"].astype(float).to_numpy()
    i = len(c) - 1
    day = int(ts[i] // DAY)
    ctx = daily_context(d1, day)
    if ctx is None:
        return None
    pdh, pdl, narrow, up, atr = ctx
    today = (ts // DAY) == day
    prev = c[today][:-1]                     # các nến ĐÃ ĐÓNG trước đó trong cùng ngày
    for d, broke, first in ((1, c[i] > pdh, not (prev > pdh).any()), (-1, c[i] < pdl, not (prev < pdl).any())):
        if broke and first and narrow and (up == (d > 0)) and atr > 0:
            e = float(c[i])
            sl = ATR_K * atr
            return d, e, e - d * sl, e + d * RR * sl
    return None
