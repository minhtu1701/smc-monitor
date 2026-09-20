"""Thu thập tin miễn phí: kênh Telegram công khai (trang xem trước t.me/s/<kênh>) + RSS báo crypto.
Mỗi bài được gắn mã coin có USDT perp và lưu vào feed_log.jsonl kèm giá lúc đó -> dữ liệu để sau này đo tin nào làm giá chạy."""
import email.utils
import html
import json
import re
from pathlib import Path

TELEGRAM = {
    "whale_alert_io": "Whale Alert", "TreeNewsFeed": "Tree News", "binance_announcements": "Binance",
    "WuBlockchainEnglish": "Wu Blockchain", "cointelegraph": "Cointelegraph TG", "WatcherGuru": "Watcher Guru", "unfolded": "Unfolded",
}
RSS = {
    "CoinDesk": "https://www.coindesk.com/arc/outboundfeeds/rss/", "Cointelegraph": "https://cointelegraph.com/rss",
    "The Block": "https://www.theblock.co/rss.xml", "Decrypt": "https://decrypt.co/feed",
}
LOG = Path(__file__).with_name("feed_log.jsonl")
NAMES = {"bitcoin": "BTC", "ethereum": "ETH", "ether": "ETH", "solana": "SOL", "ripple": "XRP", "dogecoin": "DOGE", "cardano": "ADA",
         "chainlink": "LINK", "avalanche": "AVAX", "polkadot": "DOT", "litecoin": "LTC", "tron": "TRX", "toncoin": "TON", "sui": "SUI",
         "aptos": "APT", "arbitrum": "ARB", "optimism": "OP", "hyperliquid": "HYPE", "pepe": "PEPE", "shiba inu": "SHIB", "uniswap": "UNI",
         "aave": "AAVE", "near protocol": "NEAR", "polygon": "POL", "stellar": "XLM", "filecoin": "FIL", "injective": "INJ", "celestia": "TIA",
         "worldcoin": "WLD", "ethena": "ENA", "ondo": "ONDO", "bnb": "BNB"}
# chữ in hoa hay gặp nhưng không phải tên coin
COMMON = {"USD", "USDT", "USDC", "CEO", "SEC", "ETF", "ETFS", "CPI", "PPI", "FED", "FOMC", "GDP", "THE", "NEW", "NOW", "JUST", "BREAKING", "HUGE",
          "US", "UK", "EU", "AI", "API", "NFT", "DEX", "CEX", "TVL", "ATH", "ALL", "ONE", "BIG", "TOP", "WIN", "ANY", "ME", "IT", "BE", "GO",
          "OF", "TO", "IN", "ON", "AT", "BY", "OR", "AND", "FOR", "IS", "ARE", "WAS", "LIVE", "SAFE", "OPEN", "REAL", "GAS", "DOG", "CAT", "MOVE",
          "TRUMP", "TIME", "PEOPLE", "TRUTH", "SUN", "HIGH", "LOW", "BANK", "CHINA", "JAPAN", "NFP", "DOJ", "IRS", "FBI", "CFTC", "OTC", "IPO"}


def _clean(s: str) -> str:
    s = re.sub(r"<br\s*/?>", " ", s)
    s = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", html.unescape(s)).strip()


def parse_telegram(ch: str, text: str) -> list[dict]:
    out = []
    for blk in re.split(r'(?=<div class="tgme_widget_message_wrap)', text)[1:]:
        pid = re.search(r'data-post="([^"]+)"', blk)
        tm = re.search(r'<time datetime="([^"]+)"', blk)
        body = re.search(r'class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>', blk, re.S)
        if not (pid and tm and body):
            continue
        from datetime import datetime
        ts = int(datetime.fromisoformat(tm.group(1)).timestamp())
        out.append({"id": "tg:" + pid.group(1), "source": TELEGRAM.get(ch, ch), "ts": ts, "text": _clean(body.group(1))[:600],
                    "url": f"https://t.me/{pid.group(1)}"})
    return out


def parse_rss(name: str, text: str) -> list[dict]:
    out = []
    for it in re.findall(r"<item>(.*?)</item>", text, re.S):
        g = lambda tag: (re.search(rf"<{tag}[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</{tag}>", it, re.S) or [None, ""])[1]  # noqa: E731
        title, link, pub = _clean(g("title")), _clean(g("link")), g("pubDate")
        try:
            ts = int(email.utils.parsedate_to_datetime(pub).timestamp())
        except Exception:
            continue
        desc = _clean(g("description"))[:300]
        out.append({"id": "rss:" + (link or title), "source": name, "ts": ts, "text": title + (" — " + desc if desc else ""), "url": link})
    return out


STABLE = {"USDC", "USDT", "FDUSD", "TUSD", "USDP", "DAI", "USDE", "USD1", "PYUSD"}


def tag(text: str, bases: set[str]) -> list[str]:
    bases = bases - STABLE
    found = []
    for m in re.findall(r"[$#]([A-Za-z][A-Za-z0-9]{1,11})\b", text):
        if m.upper() in bases:
            found.append(m.upper())
    for m in re.findall(r"\b([A-Z][A-Z0-9]{2,10})\b", text):
        if m in bases and m not in COMMON:
            found.append(m)
    low = text.lower()
    for name, b in NAMES.items():
        if re.search(rf"\b{re.escape(name)}\b", low) and b in bases:
            found.append(b)
    return list(dict.fromkeys(found))


async def fetch_all(http) -> list[dict]:
    items = []
    hdr = {"User-Agent": "Mozilla/5.0"}
    for ch in TELEGRAM:
        try:
            r = await http.get(f"https://t.me/s/{ch}", headers=hdr, timeout=20, follow_redirects=True)
            items += parse_telegram(ch, r.text)
        except Exception:
            pass
    for name, url in RSS.items():
        try:
            r = await http.get(url, headers=hdr, timeout=20, follow_redirects=True)
            items += parse_rss(name, r.text)
        except Exception:
            pass
    return items


def load_recent(n: int = 3000) -> list[dict]:
    if not LOG.exists():
        return []
    lines = LOG.read_text(encoding="utf-8").splitlines()[-n:]
    out = []
    for ln in lines:
        try:
            out.append(json.loads(ln))
        except Exception:
            pass
    return out


def append(items: list[dict]):
    with LOG.open("a", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")
