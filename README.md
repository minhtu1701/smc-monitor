# SMC Futures Monitor

Giám sát Binance Futures theo Smart Money Concepts (CHoCH + BoS liền kề), gửi cảnh báo Telegram và realtime qua WebSocket, hiển thị trên dashboard Vue 3 + TradingView Lightweight Charts.

## Cấu trúc

```
smc-monitor/
├── main.py           # FastAPI backend (scanner + SMC + Telegram + WebSocket)
├── index.html         # Frontend Vue 3 (CDN, không cần build)
├── requirements.txt
└── .env.example        # copy -> .env
```

## Cài đặt & chạy

```bash
cd smc-monitor
python -m venv venv
venv\Scripts\activate          # Windows
pip install -r requirements.txt

copy .env.example .env         # điền TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID (tuỳ chọn)

python main.py
```

Mở trình duyệt: **http://localhost:8000**

## Cách hoạt động

- **Backend** dùng `ccxt` tải nến Binance USDT-M Futures cho từng symbol × timeframe trong `.env`, mỗi `SCAN_INTERVAL` giây.
- Dùng thư viện `smartmoneyconcepts` (`swing_highs_lows` + `bos_choch`) để phát hiện cấu trúc thị trường. Chỉ tính trên **nến đã đóng** để tránh tín hiệu bị "vẽ lại".
- Tín hiệu hợp lệ = một **CHoCH** được xác nhận, ngay sau đó là một **BoS cùng hướng** trong vòng `MAX_GAP_BARS` nến — tức xác nhận đổi cấu trúc rồi tiếp diễn.
- Tín hiệu mới (trong `FRESH_BARS` nến gần nhất) được: lưu vào lịch sử, gửi Telegram (nếu cấu hình), và broadcast tới mọi client qua `/ws`.
- **Frontend**: chọn coin/khung thời gian → gọi `GET /api/klines` để vẽ chart + đường CHoCH/BoS; nến realtime lấy trực tiếp từ Binance kline WebSocket (nhẹ cho server); bảng tín hiệu bên phải nhận qua `/ws`, click vào 1 dòng sẽ nhảy chart sang đúng coin/thời điểm đó.

## API chính

| Method | Path | Mô tả |
|---|---|---|
| GET | `/api/config` | Trạng thái scanner, danh sách symbol/timeframe |
| GET | `/api/klines?symbol=BTCUSDT&timeframe=15m` | Nến + sự kiện CHoCH/BoS để vẽ chart |
| GET | `/api/signals` | Lịch sử tín hiệu gần nhất |
| POST | `/api/test-signal` | Bắn tín hiệu giả để test Telegram/WebSocket |
| WS | `/ws` | Stream tín hiệu + status realtime |

## Tuỳ chỉnh

Tất cả tham số nằm trong `.env` (xem `.env.example`): danh sách coin, timeframe, độ nhạy swing (`SWING_LENGTH`), khoảng cách CHoCH→BoS tối đa, chu kỳ quét…

## Lưu ý

- Không cần API key Binance (chỉ đọc dữ liệu công khai). Nếu deploy production, cân nhắc giới hạn CORS/reverse proxy và chạy `uvicorn main:app --workers 1` (biến `history`/`seen` đang ở bộ nhớ, không nên chạy nhiều worker).
- Thư viện `smartmoneyconcepts` cần pandas/numpy — cài qua `requirements.txt` là đủ.
