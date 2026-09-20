"""
Lọc Fair Value Gap (FVG) theo 4 quy tắc SMC. Chỉ dùng nến ĐÃ ĐÓNG (không nhìn trước).

    from fvg_filter import find_valid_fvgs
    fvgs = find_valid_fvgs(ltf_df, htf_df=h4_df)     # df: cột ts (giây, giờ MỞ nến), open, high, low, close, volume

FVG 3 nến (k-1, k, k+1): bullish khi low[k+1] > high[k-1] (vùng = [high[k-1], low[k+1]]), bearish ngược lại. FVG chỉ tồn tại khi nến k+1 đã đóng.
  R1 thuận xu hướng : hướng FVG == hướng cú phá cấu trúc (BOS/CHoCH) gần nhất của khung lớn tại thời điểm FVG hình thành.
  R2 gắn với phá vỡ : nến hình thành FVG (hoặc `break_lookback` nến trước đó) đóng cửa phá swing theo hướng FVG -> loại FVG "internal" lửng lơ.
  R3 Discount/Premium: bullish nằm dưới 50% của nhịp sóng gần nhất (đáy -> đỉnh), bearish nằm trên 50%.
  R4 dùng một lần   : lần chạm đầu tiên là lần duy nhất; đóng cửa xuyên cạnh xa trước khi chạm => vô hiệu.
Cột kết quả: r1/r2/r3 (đạt quy tắc), touch_idx (nến chạm đầu tiên, -1 nếu chưa), dead_idx, valid_now (đạt R1-R3 và chưa chạm/chết),
entry_ok (đạt R1-R3, có nến chạm đầu tiên và chưa chết trước đó). only_valid=False trả về mọi FVG kèm cờ để tự bật/tắt quy tắc.
"""
import numpy as np
import pandas as pd
from smartmoneyconcepts import smc

COLS = ["open", "high", "low", "close", "volume"]


def _bar_seconds(df: pd.DataFrame) -> int:
    return int(np.median(np.diff(df["ts"].to_numpy()[-50:])))


def _structure_events(df: pd.DataFrame, swing_len: int) -> pd.DataFrame:
    """BOS/CHoCH: (dir, swing_idx, broken_idx, close_time). Sự kiện chỉ 'biết' khi nến phá đóng cửa."""
    ohlc = df[COLS].reset_index(drop=True)
    st = smc.bos_choch(ohlc, smc.swing_highs_lows(ohlc, swing_length=swing_len), close_break=True)
    d = st["CHOCH"].where(st["CHOCH"].fillna(0) != 0, st["BOS"])
    ev = pd.DataFrame({"dir": d, "swing_idx": st.index, "broken_idx": st["BrokenIndex"]}).dropna()
    ev = ev[ev["dir"] != 0].astype(int)
    ev = ev[ev["broken_idx"] < len(df)].sort_values("broken_idx").reset_index(drop=True)
    ev["close_time"] = df["ts"].to_numpy()[ev["broken_idx"]] + _bar_seconds(df)
    return ev


def find_valid_fvgs(df: pd.DataFrame, htf_df: pd.DataFrame | None = None, swing_len: int = 8, htf_swing_len: int = 8,
                    min_gap: float = 0.0002, break_lookback: int = 0, max_age: int = 200,
                    drop_forming: bool = True, only_valid: bool = True) -> pd.DataFrame:
    """df = khung tìm FVG; htf_df = khung lớn để xác định xu hướng (None -> dùng chính df)."""
    df = (df.iloc[:-1] if drop_forming else df).reset_index(drop=True)          # bỏ nến đang chạy
    htf = df if htf_df is None else (htf_df.iloc[:-1] if drop_forming else htf_df).reset_index(drop=True)
    h, l, c = (df[k].to_numpy(float) for k in ("high", "low", "close"))
    ts, n, bar = df["ts"].to_numpy(), len(df), _bar_seconds(df)

    # ---- FVG 3 nến (vector hoá): k = nến giữa, hình thành khi nến k+1 đóng
    k = np.arange(1, n - 1)
    bull, bear = l[k + 1] > h[k - 1], h[k + 1] < l[k - 1]
    top, bot = np.where(bull, l[k + 1], l[k - 1]), np.where(bull, h[k - 1], h[k + 1])
    keep = (bull | bear) & ((top - bot) / c[k] >= min_gap)
    f = pd.DataFrame({"dir": np.where(bull, 1, -1)[keep], "mid_idx": k[keep], "top": top[keep], "bottom": bot[keep]})
    f["formed_idx"] = f["mid_idx"] + 1
    f["formed_time"] = ts[f["formed_idx"]] + bar
    if f.empty:
        return f

    # ---- R1: xu hướng khung lớn tại thời điểm FVG hình thành (sự kiện HTF gần nhất đã đóng)
    hev = _structure_events(htf, htf_swing_len)
    j = np.searchsorted(hev["close_time"].to_numpy(), f["formed_time"].to_numpy(), side="right") - 1
    f["trend"] = np.where(j >= 0, hev["dir"].to_numpy()[np.clip(j, 0, None)], 0)
    f["r1"] = f["dir"] == f["trend"]

    # ---- R2: cú phá cấu trúc (khung của FVG) trùng nến hình thành FVG -> swing bị phá
    ev = _structure_events(df, swing_len)
    brk = {d: dict(zip(g["broken_idx"], g["swing_idx"])) for d, g in ev.groupby("dir")}
    def _swing(r):
        b = brk.get(r.dir, {})
        return next((b[i] for i in range(r.formed_idx, r.mid_idx - break_lookback - 1, -1) if i in b), -1)
    f["swing_idx"] = [_swing(r) for r in f.itertuples()]
    f["r2"] = f["swing_idx"] >= 0

    # ---- R3: FVG ở Discount (bullish) / Premium (bearish) của nhịp sóng vừa tạo cú phá cấu trúc (chỉ định nghĩa khi có R2)
    sw = np.where(f["r2"], f["swing_idx"], 0)
    f["range_low"] = [l[s:fi + 1].min() for s, fi in zip(sw, f["formed_idx"])]
    f["range_high"] = [h[s:fi + 1].max() for s, fi in zip(sw, f["formed_idx"])]
    f["eq"] = (f["range_low"] + f["range_high"]) / 2
    f["mid"] = (f["top"] + f["bottom"]) / 2
    f["r3"] = f["r2"] & np.where(f["dir"] > 0, f["mid"] < f["eq"], f["mid"] > f["eq"])
    f["zone"] = np.where(f["dir"] > 0, "discount", "premium")

    if only_valid:                                    # R4 chỉ cần tính cho FVG đạt R1-R3
        f = f[f["r1"] & f["r2"] & f["r3"]].copy()

    # ---- R4: dùng một lần — lần chạm đầu tiên; đóng cửa xuyên cạnh xa trước khi chạm => vô hiệu
    touch, dead = [], []
    for r in f.itertuples():
        a, b = r.formed_idx + 1, min(r.formed_idx + 1 + max_age, n)
        t = np.where(l[a:b] <= r.top)[0] if r.dir > 0 else np.where(h[a:b] >= r.bottom)[0]
        x = np.where(c[a:b] < r.bottom)[0] if r.dir > 0 else np.where(c[a:b] > r.top)[0]
        t0, x0 = (a + int(t[0]) if len(t) else -1), (a + int(x[0]) if len(x) else -1)
        touch.append(t0)
        dead.append(x0 if (x0 >= 0 and (t0 < 0 or x0 < t0)) else -1)
    f["touch_idx"], f["dead_idx"] = touch, dead
    ok = f["r1"] & f["r2"] & f["r3"]
    f["valid_now"] = ok & (f["touch_idx"] < 0) & (f["dead_idx"] < 0) & (n - 1 - f["formed_idx"] <= max_age)
    f["entry_ok"] = ok & (f["touch_idx"] >= 0) & ~((f["dead_idx"] >= 0) & (f["dead_idx"] < f["touch_idx"]))
    return f.drop(columns=["mid_idx"]).reset_index(drop=True)
