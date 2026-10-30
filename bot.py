import os
import logging
import threading
from collections import deque
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from otcharts import Client
from dotenv import load_dotenv

# تحميل المتغيرات من ملف .env
load_dotenv()

# ============================================================
# 1) الإعدادات (تقرأ من Environment Variables)
# ============================================================
OTCHARTS_API_KEY = os.getenv("OTCHARTS_API_KEY", "")
API_TOKEN = os.getenv("API_TOKEN", "my_secret_token_12345")
SYMBOLS = os.getenv("SYMBOLS", "EURUSD_otc,GBPUSD_otc,AUDJPY_otc,EURJPY_otc,GBPJPY_otc")
VENUE = os.getenv("VENUE", "otc")

SYMBOLS_LIST = [s.strip() for s in SYMBOLS.split(",") if s.strip()]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("otc")

# ============================================================
# 2) المؤشرات الفنية (Indicators)
# ============================================================
def calculate_ema(prices: list, period: int) -> float | None:
    if len(prices) < period: return None
    multiplier = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for price in prices[period:]:
        ema = (price - ema) * multiplier + ema
    return ema

def calculate_rsi(prices: list, period: int = 14) -> float | None:
    if len(prices) < period + 1: return None
    gains, losses = 0, 0
    for i in range(1, period + 1):
        change = prices[i] - prices[i-1]
        if change > 0: gains += change
        else: losses -= change
    
    avg_gain = gains / period
    avg_loss = losses / period
    
    if avg_loss == 0: return 100.0
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_macd(prices: list):
    if len(prices) < 26: return None, None
    ema12 = calculate_ema(prices[-26:], 12)
    ema26 = calculate_ema(prices[-26:], 26)
    if ema12 is None or ema26 is None: return None, None
    macd_line = ema12 - ema26
    return macd_line, 0

class PriceStore:
    def __init__(self):
        self.lock = threading.Lock()
        self.latest = {}
        self.price_history = {}

    def update(self, symbol: str, price: float, ts: str):
        with self.lock:
            self.latest[symbol] = {"price": price, "time": ts}
            self.price_history.setdefault(symbol, [])
            self.price_history[symbol].append(price)
            if len(self.price_history[symbol]) > 100:
                self.price_history[symbol].pop(0)

    def get_latest(self, symbol: str | None = None):
        with self.lock:
            if symbol: return self.latest.get(symbol)
            return dict(self.latest)

    def get_prices(self, symbol: str):
        with self.lock:
            return list(self.price_history.get(symbol, []))

prices = PriceStore()

# ============================================================
# 3) إدارة التدفق
# ============================================================
class StreamManager:
    def __init__(self):
        self.client = Client(api_key=OTCHARTS_API_KEY)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self):
        if self._thread and self._thread.is_alive(): return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        log.info(f"✅ بدأ التدفق لـ: {SYMBOLS_LIST}")

    def stop(self):
        self._stop.set()

    def _run(self):
        while not self._stop.is_set():
            try:
                for tick in self.client.stream(VENUE, SYMBOLS_LIST):
                    if self._stop.is_set(): break
                    ts = datetime.now(timezone.utc).isoformat()
                    prices.update(tick.symbol, float(tick.price), ts)
            except Exception as e:
                log.error(f"انقطع التدفق: {e} — إعادة محاولة بعد 5 ثوانٍ")
                self._stop.wait(5)

stream_mgr = StreamManager()

# ============================================================
# 4) FastAPI
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    stream_mgr.start()
    yield
    stream_mgr.stop()

app = FastAPI(lifespan=lifespan, title="OTC Lite Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def verify(authorization: str | None = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Token مفقود")
    if authorization.split(" ", 1)[1] != API_TOKEN:
        raise HTTPException(403, "Token خاطئ")

@app.get("/api/health")
def health():
    return {"status": "ok", "symbols": SYMBOLS_LIST, "latest": prices.get_latest()}

@app.get("/api/check-trade")
def check_trade(symbol: str, authorization: str | None = Header(None)):
    verify(authorization)
    
    data = prices.get_latest(symbol)
    history = prices.get_prices(symbol)

    if not data or len(history) < 30:
        return {"status": "waiting", "message": "جاري جمع البيانات الكافية للتحليل... انتظر 15 ثانية وحاول مجدداً"}

    current_price = data['price']
    ema9 = calculate_ema(history, 9)
    ema21 = calculate_ema(history, 21)
    rsi = calculate_rsi(history, 14)
    macd, _ = calculate_macd(history)

    if ema9 is None or ema21 is None or rsi is None or macd is None:
        return {"status": "waiting", "message": "المؤشرات لا تزال قيد الحساب..."}

    signal = "NEUTRAL"
    reasons = []
    score = 0

    is_uptrend = ema9 > ema21
    is_downtrend = ema9 < ema21
    is_rsi_safe_buy = 40 < rsi < 70
    is_rsi_safe_sell = 30 < rsi < 60
    is_macd_bullish = macd > 0
    is_macd_bearish = macd < 0

    if is_uptrend and is_rsi_safe_buy and is_macd_bullish and current_price > ema9:
        signal = "BUY"
        reasons.append("اتجاه صاعد (EMA 9 > 21)")
        reasons.append("زخم إيجابي (MACD > 0)")
        reasons.append(f"RSI في منطقة آمنة ({rsi:.1f})")
        score = 95
    elif is_downtrend and is_rsi_safe_sell and is_macd_bearish and current_price < ema9:
        signal = "SELL"
        reasons.append("اتجاه هابط (EMA 9 < 21)")
        reasons.append("زخم سلبي (MACD < 0)")
        reasons.append(f"RSI في منطقة آمنة ({rsi:.1f})")
        score = 95

    if signal == "NEUTRAL":
        return {"status": "none", "message": "المؤشرات غير متوافقة حالياً. السوق متذبذب أو غير واضح. الأفضل الانتظار."}

    return {
        "status": "success",
        "signal": {
            "symbol": symbol,
            "direction": signal,
            "price": current_price,
            "score": score,
            "reasons": reasons,
            "time": data['time']
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("bot:app", host="0.0.0.0", port=8000, reload=False)