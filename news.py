"""Theo dõi thông báo Binance (niêm yết / huỷ niêm yết / nhãn Monitoring) và phân loại theo coin.

Nghiên cứu sự kiện (giá USDT perp, vượt BTC, vào ở giá đóng nến giờ chứa thông báo):
  - Gắn nhãn Monitoring (n=115, 2024-2026): +24h -3.3% (âm 66%), BÁN giữ 24h SL +10%: +2.6%/lệnh, thắng 61%, t=3.6
  - Huỷ niêm yết spot    (n=67, 2022-2026):  +4h -6.8%, +24h -6.2%, BÁN giữ 24h SL +10%: +5.6%/lệnh, thắng 58%, t=3.4
  - Seed Tag / HODLer / Futures delist: bơm hoặc biến động mạnh ngay giờ ra tin rồi phai dần, không đủ ổn định để vào lệnh tự động."""
import re

URL = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
CATALOGS = (48, 161, 49)
TRADE_TYPES = {"MONITOR": "Nhãn Monitoring", "DELIST": "Huỷ niêm yết spot"}
INFO_TYPES = {"SEED": "Niêm yết spot (Seed Tag)", "HODLER": "HODLer/Launchpool", "FUT_DELIST": "Huỷ niêm yết Futures",
              "SPOT_LIST": "Niêm yết spot", "ALPHA_FUT": "Alpha + Futures"}
STATS = {
    "MONITOR": "Lịch sử (115 lần): 24h sau tin giảm TB -3.3% so với BTC, 66% trường hợp giảm. Bán giữ 24h, SL +10%: TB +2.6%/lệnh.",
    "DELIST": "Lịch sử (67 lần): 4h sau tin giảm TB -6.8%, 24h -6.2% so với BTC; ~75% giảm. Bán giữ 24h, SL +10%: TB +5.6%/lệnh.",
    "SEED": "Lịch sử: bơm TB +22% ngay giờ ra tin rồi phai (7 ngày trung vị -17%). Không vào lệnh tự động.",
    "HODLER": "Lịch sử: bơm TB +10% giờ ra tin, 7 ngày sau TB -17% so với BTC. Không vào lệnh tự động.",
    "FUT_DELIST": "Lịch sử: không có hướng rõ, biến động rất mạnh (24h có lúc +29%). Nên tránh giao dịch coin này.",
    "SPOT_LIST": "Tin niêm yết spot.", "ALPHA_FUT": "Tin Alpha + Futures.",
}
STOP = {"USDT", "USDC", "BTC", "BNB", "ETH", "FDUSD", "TRY", "EUR", "BRL", "USD", "AND", "ON", "THE", "SPOT", "TAG", "UTC", "NFT", "API", "ETF", "RWA", "AI", "M", "COIN"}


def _tickers(t: str) -> list[str]:
    xs = re.findall(r"\(([A-Z0-9]{1,12})\)", t)
    if not xs:
        seg = re.split(r" on \d{4}|\bon\b", t)[0]
        xs = re.findall(r"\b([A-Z][A-Z0-9]{1,11})\b", seg)
    return [x for x in dict.fromkeys(xs) if x not in STOP]


def classify(title: str) -> list[tuple[str, str]]:
    """-> [(loại, ticker)]"""
    t = title
    if re.search(r"Monitoring Tag", t, re.I):
        seg = t.split("Include")[-1] if "Include" in t else t
        return [("MONITOR", x) for x in _tickers(seg)]
    if re.match(r"Binance Futures Will Delist", t):
        return [("FUT_DELIST", m[:-4]) for m in dict.fromkeys(re.findall(r"([A-Z0-9]{2,15}USDT)", t))]
    if re.match(r"Binance Will Delist", t):
        return [("DELIST", x) for x in _tickers(t)]
    if re.match(r"Binance Will List", t):
        return [("SEED" if "Seed Tag" in t else "SPOT_LIST", x) for x in _tickers(t)]
    if re.search(r"HODLer Airdrop|Launchpool|Megadrop", t):
        return [("HODLER", x) for x in _tickers(t)]
    if re.search(r"Binance Alpha and Binance Futures", t):
        return [("ALPHA_FUT", x) for x in _tickers(t)]
    return []


async def fetch_latest(http, page_size: int = 20) -> list[dict]:
    out = []
    for cid in CATALOGS:
        try:
            r = await http.get(URL, params={"type": 1, "catalogId": cid, "pageNo": 1, "pageSize": page_size}, timeout=20)
            for c in r.json().get("data", {}).get("catalogs", []):
                for a in c.get("articles", []):
                    out.append({"code": a.get("code") or f"{cid}-{a['releaseDate']}", "ts": a["releaseDate"] // 1000, "title": a["title"]})
        except Exception:
            continue
    return out
