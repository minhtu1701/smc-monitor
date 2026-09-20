"""Dịch tin tiếng Anh sang tiếng Việt bằng dịch vụ miễn phí (không cần khoá API): Google (clients5) -> dự phòng MyMemory.
Có bộ nhớ đệm; lỗi thì trả None để giao diện hiện bản gốc."""
import asyncio

_cache: dict[str, str] = {}
UA = {"User-Agent": "Mozilla/5.0"}


async def to_vi(http, text: str) -> str | None:
    text = (text or "").strip()
    if not text:
        return None
    key = text[:500]
    if key in _cache:
        return _cache[key]
    out = None
    try:
        r = await http.get("https://clients5.google.com/translate_a/t",
                           params={"client": "dict-chrome-ex", "sl": "auto", "tl": "vi", "q": key}, headers=UA, timeout=15)
        if r.status_code == 200:
            j = r.json()
            out = j[0][0] if isinstance(j[0], list) else j[0]
    except Exception:
        out = None
    if not out:
        try:
            r = await http.get("https://api.mymemory.translated.net/get", params={"q": key[:480], "langpair": "en|vi"}, headers=UA, timeout=15)
            if r.status_code == 200:
                t = r.json().get("responseData", {}).get("translatedText")
                if t and "MYMEMORY WARNING" not in t:
                    out = t
        except Exception:
            out = None
    if out:
        _cache[key] = out
    return out


async def batch(http, items: list[dict], field: str = "text", out_field: str = "text_vi", limit: int = 40, pause: float = 0.25):
    """Dịch tối đa `limit` phần tử chưa có bản dịch."""
    n = 0
    for it in items:
        if n >= limit:
            break
        if it.get(out_field) or not it.get(field):
            continue
        vi = await to_vi(http, it[field])
        if vi:
            it[out_field] = vi
        n += 1
        await asyncio.sleep(pause)
    return n
