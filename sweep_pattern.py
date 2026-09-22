"""Mẫu hình "quét nhịp nhỏ": CHoCH + BoS gần nhau -> nhịp đi tiếp ngắn -> giá quay ngược QUÉT hết đỉnh (đáy)
của chính nhịp đó -> đóng cửa trở lại -> báo. Chỉ là CẢNH BÁO để người dùng tự quyết, không phải hệ vào lệnh.

Đã đo 22/09/2026 (xem memory rejected_strategies): ở mọi khung có đủ mẫu (102 coin, 15m/1h/4h) lợi thế thô
KHÔNG hơn vào lệnh ngẫu nhiên, và phí ăn 0,1–0,5R/lệnh ở khung nhỏ. Người dùng muốn theo dõi để tự đánh giá.

Định nghĩa (y hệt bản backtest sweep_minitrend2.py; swing xác nhận sau SWL nến nên hoàn toàn nhân quả):
 1. CHoCH (phá đáy/đỉnh swing gần nhất khi xu hướng đang ngược), rồi BoS cùng chiều trong <= GAP nến.
 2. Thêm >= 1 BoS nữa (nhịp nhỏ). Mức thanh khoản S = đỉnh cao nhất (mẫu giảm) của các swing sau CHoCH.
 3. Giá vượt S nhưng không đóng qua đỉnh gốc O (swing cuối trước CHoCH).
 4. Nến đóng trở lại dưới S trong <= BACK nến kể từ lúc quét -> báo. SL gợi ý = cực trị cú quét +/- 0,1 ATR.
 Mọi thứ phải xong trong <= WIN nến sau BoS đầu tiên.
"""
import math

import numpy as np

SWL, GAP, WIN, BACK = 5, 10, 192, 24


def detect(o, h, l, c, atr):
    """Trả về danh sách kích hoạt (cũ nhất trước). Mỗi phần tử là dict với chỉ số nến kích hoạt `i`."""
    n = len(c)
    out = []
    lastSH = lastSL = math.nan
    shUsed = slUsed = True
    trend = 0
    # chỉ số 0 = mẫu GIẢM (báo bán), 1 = mẫu TĂNG (báo mua)
    st = [0, 0]
    t1 = [0, 0]
    t2 = [0, 0]
    tsw = [0, 0]
    O = [0.0, 0.0]
    S = [0.0, 0.0]
    ext = [0.0, 0.0]
    lv_choch = [0.0, 0.0]
    lv_bos = [0.0, 0.0]
    for t in range(2 * SWL, n):
        p = t - SWL
        wh = h[p - SWL:p + SWL + 1]
        wl = l[p - SWL:p + SWL + 1]
        if h[p] >= wh.max():
            lastSH, shUsed = h[p], False
            if st[0] in (1, 2, 3) and p > t1[0] and h[p] > S[0]:
                S[0] = h[p]
        if l[p] <= wl.min():
            lastSL, slUsed = l[p], False
            if st[1] in (1, 2, 3) and p > t1[1] and l[p] < S[1]:
                S[1] = l[p]
        bear = (not slUsed) and c[t] < lastSL
        bull = (not shUsed) and c[t] > lastSH
        if bear:
            slUsed = True
            if trend >= 0:
                if not math.isnan(lastSH):
                    st[0], t1[0], O[0], S[0], lv_choch[0] = 1, t, lastSH, -1e300, lastSL
                trend = -1
            else:
                if st[0] == 1:
                    if t - t1[0] <= GAP:
                        st[0], t2[0], lv_bos[0] = 2, t, lastSL
                    else:
                        st[0] = 0
                elif st[0] == 2:
                    st[0] = 3
        if bull:
            shUsed = True
            if trend <= 0:
                if not math.isnan(lastSL):
                    st[1], t1[1], O[1], S[1], lv_choch[1] = 1, t, lastSL, 1e300, lastSH
                trend = 1
            else:
                if st[1] == 1:
                    if t - t1[1] <= GAP:
                        st[1], t2[1], lv_bos[1] = 2, t, lastSH
                    else:
                        st[1] = 0
                elif st[1] == 2:
                    st[1] = 3
        if st[0] >= 1 and c[t] > O[0]:
            st[0] = 0
        if st[1] >= 1 and c[t] < O[1]:
            st[1] = 0
        for s in (0, 1):
            if st[s] >= 2 and t - t2[s] > WIN:
                st[s] = 0
        for s in (0, 1):
            d = -1 if s == 0 else 1
            if st[s] == 3:
                swept = (h[t] > S[0]) if s == 0 else (l[t] < S[1])
                valid = (S[0] > -1e299) if s == 0 else (S[1] < 1e299)
                if valid and swept:
                    st[s], tsw[s], ext[s] = 4, t, (h[t] if s == 0 else l[t])
            elif st[s] == 4:
                if s == 0 and h[t] > ext[0]:
                    ext[0] = h[t]
                if s == 1 and l[t] < ext[1]:
                    ext[1] = l[t]
            if st[s] == 4:
                back = (c[t] < S[0]) if s == 0 else (c[t] > S[1])
                if back:
                    st[s] = 0
                    if not (atr[t] > 0):
                        continue
                    e = float(c[t])
                    sl = ext[s] - d * 0.1 * atr[t]
                    if (s == 0 and sl >= O[0] + 0.5 * atr[t]) or (s == 1 and sl <= O[1] - 0.5 * atr[t]):
                        continue
                    if (e - sl) * d <= 0:
                        continue
                    out.append({"i": t, "dir": d, "entry": e, "sl": float(sl), "swept": float(S[s]),
                                "origin": float(O[s]), "choch_level": float(lv_choch[s]), "bos_level": float(lv_bos[s]),
                                "choch_i": t1[s], "bos_i": t2[s]})
                elif t - tsw[s] >= BACK:
                    st[s] = 0
    return out


def wilder_atr(h, l, c, n=14):
    pc = np.r_[c[0], c[:-1]]
    tr = np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc)))
    a = np.empty_like(tr)
    a[0] = tr[0]
    k = 1.0 / n
    for i in range(1, len(tr)):
        a[i] = a[i - 1] + k * (tr[i] - a[i - 1])
    return a
