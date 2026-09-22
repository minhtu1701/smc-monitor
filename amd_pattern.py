"""AMD (Tích luỹ -> Thao túng -> Phân phối) trên H4 — bản Python thuần của amd.py đã backtest (22/09/2026).

Kết quả backtest (102 coin, 5 năm, TP 2R): n=207, +0,140R/lệnh sau phí, dương 5/6 năm, train/holdout +0,146/+0,135
— nhưng t=1,95 (dưới ngưỡng 2,5 đã khai báo), vào ngẫu nhiên cùng SL/TP đã được +0,104R (mô hình chỉ góp ~+0,04R),
và thêm vào danh mục làm lãi/sụt 4,74 -> 4,65 (NY tắt). Người dùng quyết định vẫn chạy để tự theo dõi.

Bản GIẢM (bản tăng lật ngược), swing xác nhận sau SWL nến nên hoàn toàn nhân quả:
 1. Tích luỹ: RANGE nến trước cú quét, biên độ <= WMAX ATR, >= 2 đỉnh swing chạm biên trên và >= 2 đáy swing chạm
    biên dưới (sai lệch <= TOL ATR).
 2. Thao túng: high vượt biên trên, rồi đóng trở vào vùng trong <= BACK nến.
 3. Phân phối: có một LH (swing high sau cú quét, thấp hơn đỉnh cú quét), rồi nến ĐÓNG DƯỚI BIÊN DƯỚI = BOS -> báo.
    Phải xong trong <= DIST nến sau khi đóng trở vào; huỷ nếu đóng trên đỉnh cú quét.
 SL = đỉnh cú quét + 0,1 ATR.
"""
import numpy as np

SWL, RANGE, WMAX, TOL, BACK, DIST = 5, 48, 8.0, 0.5, 3, 60


def detect(o, h, l, c, atr):
    """Trả về danh sách kích hoạt (cũ nhất trước): dict với chỉ số nến `i`, chiều `dir`, `entry`, `sl`, biên vùng."""
    n = len(c)
    isPH = np.zeros(n, bool)
    isPL = np.zeros(n, bool)
    for p in range(SWL, n - SWL):
        isPH[p] = h[p] >= h[p - SWL:p + SWL + 1].max()
        isPL[p] = l[p] <= l[p - SWL:p + SWL + 1].min()
    out = []
    st = [0, 0]
    RH = [0.0, 0.0]
    RL = [0.0, 0.0]
    SX = [0.0, 0.0]
    tsw = [0, 0]
    trc = [0, 0]
    have = [False, False]
    for t in range(RANGE + 2 * SWL + 2, n):
        a = atr[t - 1]
        for s in (0, 1):
            if st[s] != 0 or not (a > 0):
                continue
            hi = h[t - RANGE:t].max()
            lo = l[t - RANGE:t].min()
            if hi - lo > WMAX * a:
                continue
            if not ((h[t] > hi) if s == 0 else (l[t] < lo)):
                continue
            ps = np.arange(t - RANGE, t - SWL)
            nh = int((isPH[ps] & (h[ps] >= hi - TOL * a)).sum())
            nl = int((isPL[ps] & (l[ps] <= lo + TOL * a)).sum())
            if nh < 2 or nl < 2:
                continue
            st[s], RH[s], RL[s], tsw[s], have[s] = 1, hi, lo, t, False
            SX[s] = h[t] if s == 0 else l[t]
        for s in (0, 1):
            d = -1 if s == 0 else 1
            if st[s] == 1:
                if s == 0 and h[t] > SX[0]:
                    SX[0] = h[t]
                if s == 1 and l[t] < SX[1]:
                    SX[1] = l[t]
                if (c[t] < RH[0]) if s == 0 else (c[t] > RL[1]):
                    st[s], trc[s] = 2, t
                elif t - tsw[s] >= BACK - 1:
                    st[s] = 0
                continue
            if st[s] == 2:
                if (s == 0 and c[t] > SX[0]) or (s == 1 and c[t] < SX[1]) or t - trc[s] > DIST:
                    st[s] = 0
                    continue
                p = t - SWL
                if p > tsw[s]:
                    if s == 0 and isPH[p] and h[p] < SX[0]:
                        have[0] = True
                    if s == 1 and isPL[p] and l[p] > SX[1]:
                        have[1] = True
                if have[s] and ((c[t] < RL[0]) if s == 0 else (c[t] > RH[1])):
                    st[s] = 0
                    if not (atr[t] > 0):
                        continue
                    e = float(c[t])
                    sl = SX[s] - d * 0.1 * atr[t]
                    if (e - sl) * d <= 0:
                        continue
                    out.append({"i": t, "dir": d, "entry": e, "sl": float(sl),
                                "range_high": float(RH[s]), "range_low": float(RL[s]), "sweep": float(SX[s])})
    return out
