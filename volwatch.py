"""Danh sách "Sắp biến động mạnh": chấm điểm mọi hợp đồng USDT perpetual bằng mô hình logistic trong vol_model.json.

Mô hình dự báo khả năng biến động MẠNH trong 24h (cả hai chiều), KHÔNG dự báo hướng: trong nhóm điểm cao nhất xác suất bơm +30%
và sập -20% đều tăng. Xác suất hiển thị lấy từ bảng hiệu chỉnh đo ngoài mẫu (nửa sau năm dữ liệu)."""
import asyncio
import json
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

MODEL = json.loads((Path(__file__).with_name("vol_model.json")).read_text(encoding="utf-8"))
# Hai mô hình gradient boosting (P chạm +15% / chạm -15% trong 24h), học nửa đầu năm + hiệu chỉnh isotonic trên nửa sau (ngoài mẫu). Ngoài mẫu: AUC 0.80 (tăng) / 0.89 (sập), xác suất khớp tần suất thực
# (vd dự báo ~53% -> thực tế ~49%). Phân biệt tốt coin RỦI RO SẬP cao/thấp; KHÔNG chọn được coin sẽ tăng (lợi nhuận TB 24h ~0 ở mọi nhóm).
DIR = joblib.load(Path(__file__).with_name("vol_dir_models.joblib"))
# ── SAU CÚ BƠM: giá thường về đâu trong 48h tới ──────────────────────────────
# Đo 25/09/2026 trên 4.266 cú bơm, 98 coin, nến 15m, 5 năm (dump_depth.py / dump_volume.py).
# Nền để so: mốc NGẪU NHIÊN bất kỳ cho đáy 48h trung vị −4,0% và 37% khả năng chạm +5%.
# Ổn định giữa hai nửa thời gian (bơm ≥30%: −11,2% / −10,0%) và đơn điệu theo cỡ cú bơm.
# ĐÃ ĐO VÀ KHÔNG DÙNG: volume/OI KHÔNG làm rõ được độ sâu (mọi nhóm đều quanh −8%), nên độ sâu
# chỉ tra theo CỠ CÚ BƠM. Volume/OI chỉ dùng cho câu hỏi "còn lên tiếp không".
PUMP_TABLE = [                       # (ngưỡng bơm 24h, đáy p25, đáy trung vị, đỉnh trung vị, % chạm +5%)
    (0.50, -21.3, -14.3, +16.3, 84),
    (0.30, -16.6, -10.6, +12.6, 75),
    (0.20, -13.3, -8.7, +8.2, 64),
]
# Nhịp volume + thay đổi OI chia nhóm "tiền mới vào" và "hết hơi". Đo trên nhóm bơm ≥20% (nền nhóm này là 64%):
#   nhịp cao + OI tăng >5%: 70% còn chạm +5% (hai nửa 71/69), đỉnh trung vị +10,5%  -> lệch +6 điểm
#   nhịp thấp + OI không tăng: 50% (hai nửa 58/44), đỉnh trung vị +4,9%             -> lệch -14 điểm
# Áp LỆCH vào nền của từng nhóm cỡ bơm, KHÔNG thay thế: bơm ≥50% có nền 84% nên "tiền mới vào" phải là 90%,
# chứ ghi đè 70% sẽ thấp hơn cả nền — sai hướng.
PUMP_FLOW = {"moi": +6, "het_hoi": -14}
MIN_QUOTE_VOL = 1_000_000
TOP_N = 20
LABELS = {  # (nhãn khi đặc trưng CAO, nhãn khi THẤP)
    "rng7": ("biên độ 7 ngày rộng", "biên độ 7 ngày hẹp"), "vr3_30": ("volume 3 ngày tăng mạnh", "volume 3 ngày cạn"),
    "vr1_7": ("volume 24h tăng", "volume 24h giảm"), "r7": ("tăng mạnh 7 ngày", "giảm mạnh 7 ngày"), "r30": ("tăng mạnh 30 ngày", "giảm mạnh 30 ngày"),
    "r24": ("tăng mạnh 24h", "giảm mạnh 24h"), "up90": ("xa đáy 90 ngày", "sát đáy 90 ngày"), "dd90": ("sát đỉnh 90 ngày", "giảm sâu từ đỉnh 90 ngày"),
    "age": ("niêm yết lâu", "mới niêm yết"), "tbr24": ("mua chủ động 24h cao", "bán chủ động 24h cao"), "tbr72": ("mua chủ động 3 ngày cao", "bán chủ động 3 ngày cao"),
    "size_r": ("lệnh lớn hơn thường", "lệnh nhỏ hơn thường"), "volr": ("biến động 24h tăng", "biến động 24h giảm"), "liq": ("thanh khoản lớn", "thanh khoản nhỏ"),
    "fund": ("funding dương", "funding âm"), "fund3": ("funding dương", "funding âm"), "tbr_d": ("mua chủ động tăng", "mua chủ động giảm"),
}


def pump_outlook(price: float, r24: float, kl: pd.DataFrame, doi: float | None) -> dict | None:
    """Thống kê điều kiện cho coin vừa bơm ≥20% trong 24h. KHÔNG phải tín hiệu vào lệnh —
    chỉ là phân phối đã đo của 48h tiếp theo, nêu cả hai chiều để không đọc thành 'chắc chắn sập'."""
    row = next((x for x in PUMP_TABLE if r24 >= x[0]), None)
    if row is None or len(kl) < 30:
        return None
    thr, p25, med, up_med, p_base = row
    v = kl["qv"].to_numpy()[-24:]
    nhip = v[-6:].sum() / (v[:-6].sum() / 3) if v[:-6].sum() > 0 else None      # 6h cuối so với 18h trước
    kind, p_up = None, p_base
    if nhip is not None and doi is not None:
        if nhip > 1.5 and doi > 0.05:
            kind = "moi"
        elif nhip <= 1.0 and doi <= 0.05:
            kind = "het_hoi"
        if kind:
            p_up = int(min(95, max(30, p_base + PUMP_FLOW[kind])))
    return dict(thr=int(thr * 100), day_tv=round(price * (1 + med / 100), 8), day_p25=round(price * (1 + p25 / 100), 8),
                dinh_tv=round(price * (1 + up_med / 100), 8), pct_tv=med, pct_p25=p25, pct_dinh=up_med,
                p_len=p_up, nhip=round(nhip, 2) if nhip else None, doi=round(doi * 100, 1) if doi is not None else None,
                kind=kind)


def _features(kl: pd.DataFrame, d1: pd.DataFrame, fund: float, age_days: float, btc: tuple = (np.nan, np.nan)) -> dict | None:
    """kl: nến 1h ĐÃ ĐÓNG (>= 721 nến), cột c,h,l,qv,n,tbqv. d1: nến ngày 90 ngày gần nhất (h,l)."""
    if len(kl) < 721:
        return None
    c, h, l, o = kl.c.to_numpy(float), kl.h.to_numpy(float), kl.l.to_numpy(float), kl.o.to_numpy(float)
    qv, n, tb = kl.qv.to_numpy(float), kl.n.to_numpy(float), kl.tbqv.to_numpy(float)
    t = len(c) - 1
    s = lambda x, k: x[t - k + 1:t + 1].sum()  # noqa: E731
    q24, q72, q7d, q30 = s(qv, 24), s(qv, 72), s(qv, 168), s(qv, 720)
    n24, n7d = s(n, 24), s(n, 168)
    tb24, tb72, tb30 = s(tb, 24), s(tb, 72), s(tb, 720)
    lr = np.diff(np.log(c))
    rv24, rv30 = lr[-24:].std(ddof=1), lr[-720:].std(ddof=1)
    prev7, prev30 = (q7d - q24) / 6, (q30 - q72) / 27
    if q24 <= 0 or q30 <= 0:
        return None
    hi90 = max(float(d1.h.max()), h[-24:].max()) if len(d1) else h.max()
    lo90 = min(float(d1.l.min()), l[-24:].min()) if len(d1) else l.min()
    return dict(
        liq=np.log10(q24 + 1), vr1_7=q24 / prev7 if prev7 > 0 else np.nan, vr3_30=(q72 / 3) / prev30 if prev30 > 0 else np.nan,
        r24=c[t] / c[t - 24] - 1, r7=c[t] / c[t - 168] - 1, r30=c[t] / c[t - 720] - 1,
        rng7=(h[t - 167:t + 1].max() - l[t - 167:t + 1].min()) / c[t], volr=rv24 / rv30 if rv30 > 0 else np.nan,
        tbr24=tb24 / q24, tbr72=tb72 / q72 if q72 > 0 else np.nan, tbr_d=tb24 / q24 - tb30 / q30,
        size_r=(q24 / n24) / ((q7d - q24) / (n7d - n24)) if n24 > 0 and n7d - n24 > 0 and q7d - q24 > 0 else np.nan,
        fund=fund, fund3=fund, dd90=c[t] / hi90 - 1, up90=c[t] / lo90 - 1, age=age_days, price=float(c[t]),
        off_h24=c[t] / h[-24:].max() - 1, off_l24=c[t] / l[-24:].min() - 1, r4=c[t] / c[t - 4] - 1,
        wick=float(((h - np.maximum(o, c))[-24:].sum() - (np.minimum(o, c) - l)[-24:].sum()) / max((h - l)[-24:].sum(), 1e-12)),
        btc_r24=btc[0], btc_r7=btc[1],
    )


def score(f: dict) -> tuple[float, int, list[str]]:
    x, contrib = [], []
    for k, name in enumerate(MODEL["feats"]):
        v = f.get(name)
        q = MODEL["q"][name]
        if v is None or not np.isfinite(v):
            v = q[100]
        if name in MODEL["log"]:
            v = np.log1p(max(v, 0.0))
        if name == "age":
            v = min(v, np.log1p(300))
        r = float(np.interp(v, q, np.linspace(0, 1, 201)))
        x.append(r)
        contrib.append((MODEL["coef"][k] * (r - 0.5), LABELS.get(name, (name, name))[0 if r >= 0.5 else 1]))
    z = MODEL["intercept"] + float(np.dot(MODEL["coef"], x))
    p = 1 / (1 + np.exp(-z))
    b = int(np.clip(np.searchsorted(MODEL["edges"], p, "right") - 1, 0, len(MODEL["cal_pump"]) - 1))
    top = [lb for c_, lb in sorted(contrib, reverse=True)[:3] if c_ > 0.05]
    return p, b, list(dict.fromkeys(top))


async def scan(ex, log) -> dict:
    t0 = time.time()
    tickers = await ex.fetch_tickers()
    prem = {x["symbol"]: float(x.get("lastFundingRate") or 0) for x in await ex.fapiPublicGetPremiumIndex()}
    cands = []
    for msym, m in ex.markets.items():
        if not m.get("swap") or m.get("quote") != "USDT" or not m.get("active") or m.get("info", {}).get("contractType") != "PERPETUAL":
            continue
        tk = tickers.get(msym) or {}
        if (tk.get("quoteVolume") or 0) < MIN_QUOTE_VOL:
            continue
        onboard = float(m.get("info", {}).get("onboardDate") or 0) / 1000
        cands.append((m["id"], (time.time() - onboard) / 86400 if onboard else 999))
    sem = asyncio.Semaphore(4)
    rows = []
    bk = await ex.fapiPublicGetKlines({"symbol": "BTCUSDT", "interval": "1h", "limit": 200})
    bc = np.array([float(z[4]) for z in bk[:-1]])
    btc = (bc[-1] / bc[-25] - 1, bc[-1] / bc[-169] - 1)

    async def one(sym, age):
        async with sem:
            try:
                r = await ex.fapiPublicGetKlines({"symbol": sym, "interval": "1h", "limit": 800})
                d = await ex.fapiPublicGetKlines({"symbol": sym, "interval": "1d", "limit": 91})
            except Exception:
                return
        kl = pd.DataFrame([[float(z[1]), float(z[2]), float(z[3]), float(z[4]), float(z[7]), float(z[8]), float(z[10])] for z in r[:-1]],
                          columns=["o", "h", "l", "c", "qv", "n", "tbqv"])
        d1 = pd.DataFrame([[float(z[2]), float(z[3])] for z in d], columns=["h", "l"])
        f = _features(kl, d1, prem.get(sym, 0.0), age, btc)
        if not f:
            return
        p, b, why = score(f)
        # Chỉ gọi thêm API open interest cho coin vừa bơm ≥20% (thường chỉ vài mã mỗi lượt quét)
        po = None
        if f["r24"] >= 0.20:
            doi = None
            try:
                oi = await ex.fapiDataGetOpenInterestHist({"symbol": sym, "period": "1h", "limit": 25})
                if len(oi) >= 25 and float(oi[0]["sumOpenInterest"]) > 0:
                    doi = float(oi[-1]["sumOpenInterest"]) / float(oi[0]["sumOpenInterest"]) - 1
            except Exception:
                pass
            po = pump_outlook(f["price"], f["r24"], kl, doi)
        xv = pd.DataFrame([[f.get(k, np.nan) for k in DIR["feats"]]], columns=DIR["feats"]).astype(float)
        pu = float(DIR["cal"]["up"].predict([DIR["models"]["up"].predict_proba(xv)[0, 1]])[0])
        pd_ = float(DIR["cal"]["dn"].predict([DIR["models"]["dn"].predict_proba(xv)[0, 1]])[0])
        rows.append(dict(p_up=round(pu * 100, 1), p_dn=round(pd_ * 100, 1), big=round((pu + pd_) * 100, 1),symbol=sym, price=f["price"], chg24=round(f["r24"] * 100, 2), chg7=round(f["r7"] * 100, 1),
                         p_pump=round(MODEL["cal_pump"][b] * 100, 1), p_dump=round(MODEL["cal_dump"][b] * 100, 1), score=round(p * 100, 2),
                         vol24=round(10 ** f["liq"]), vr3=round(f["vr3_30"], 2) if np.isfinite(f["vr3_30"]) else None,
                         rng7=round(f["rng7"] * 100, 1), fund=round(f["fund"] * 100, 4), age=round(age), why=why, pump=po))
    await asyncio.gather(*(one(s, a) for s, a in cands))
    rows.sort(key=lambda r: -r["big"])
    log.info("Volwatch: chấm %d/%d coin trong %.0fs, top %s", len(rows), len(cands), time.time() - t0, rows[0]["symbol"] if rows else "-")
    return dict(updated_at=int(time.time()), scanned=len(rows), results=rows[:TOP_N],
                base_pump=round(MODEL["base_pump"] * 100, 1), base_dump=round(MODEL["base_dump"] * 100, 1),
                base_up=round(DIR["base"]["up"] * 100, 1), base_dn=round(DIR["base"]["dn"] * 100, 1))
