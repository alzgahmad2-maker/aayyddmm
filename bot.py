import os
import logging
import threading
from collections import deque
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from otcharts import Client
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# 1) الإعدادات
# ============================================================
OTCHARTS_API_KEY = os.getenv("OTCHARTS_API_KEY", "")
API_TOKEN = os.getenv("API_TOKEN", "my_secret_token_12345")
SYMBOLS = os.getenv("SYMBOLS", "EURUSD_otc,GBPUSD_otc,AUDJPY_otc,EURJPY_otc,GBPJPY_otc")
VENUE = os.getenv("VENUE", "otc")

SYMBOLS_LIST = [s.strip() for s in SYMBOLS.split(",") if s.strip()]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("otc")

# ============================================================
# 2) المؤشرات الفنية
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
    return 100 - (100 / (1 + rs))

def calculate_macd(prices: list):
    if len(prices) < 26: return None, None
    ema12 = calculate_ema(prices[-26:], 12)
    ema26 = calculate_ema(prices[-26:], 26)
    if ema12 is None or ema26 is None: return None, None
    return ema12 - ema26, 0

# ============================================================
# 3) مخزن الأسعار وإدارة المخاطر
# ============================================================
class PriceStore:
    def __init__(self):
        self.lock = threading.Lock()
        self.latest = {}
        self.price_history = {}
        self.signal_history = deque(maxlen=10) # سجل آخر 10 إشارات
        self.consecutive_losses = 0
        self.pause_until = None

    def update(self, symbol: str, price: float, ts: str):
        with self.lock:
            self.latest[symbol] = {"price": price, "time": ts}
            self.price_history.setdefault(symbol, [])
            self.price_history[symbol].append(price)
            if len(self.price_history[symbol]) > 300: # تاريخ أطول لحساب التقلب
                self.price_history[symbol].pop(0)

    def get_latest(self, symbol: str | None = None):
        with self.lock:
            if symbol: return self.latest.get(symbol)
            return dict(self.latest)

    def get_prices(self, symbol: str):
        with self.lock:
            return list(self.price_history.get(symbol, []))

    def add_signal_result(self, symbol: str, direction: str, result: str):
        with self.lock:
            self.signal_history.appendleft({
                "symbol": symbol,
                "direction": direction,
                "result": result,
                "time": datetime.now(timezone.utc).isoformat()
            })
            if result == "LOSS":
                self.consecutive_losses += 1
                if self.consecutive_losses >= 2:
                    self.pause_until = datetime.now(timezone.utc) + timedelta(minutes=15)
                    log.warning("⛔ تم إيقاف الإشارات مؤقتاً بسبب خسارتين متتاليتين.")
            elif result == "WIN":
                self.consecutive_losses = 0

prices = PriceStore()

# ============================================================
# 4) إدارة التدفق
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
# 5) FastAPI
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    stream_mgr.start()
    yield
    stream_mgr.stop()

app = FastAPI(lifespan=lifespan, title="OTC Pro Backend")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

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

    # 1. التحقق من مانع الخسائر
    if prices.pause_until and datetime.now(timezone.utc) < prices.pause_until:
        remaining = int((prices.pause_until - datetime.now(timezone.utc)).total_seconds() / 60)
        return {"status": "blocked", "message": f"تم إيقاف الإشارات مؤقتاً (خسارتان متتاليتان). حاول بعد {remaining} دقيقة."}

    data = prices.get_latest(symbol)
    history = prices.get_prices(symbol)

    if not data or len(history) < 50:
        return {"status": "waiting", "message": "جاري جمع البيانات..."}

    current_price = data['price']
    ema9 = calculate_ema(history, 9)
    ema21 = calculate_ema(history, 21)
    rsi = calculate_rsi(history, 14)
    macd, _ = calculate_macd(history)

    if not all([ema9, ema21, rsi, macd]):
        return {"status": "waiting", "message": "المؤشرات قيد الحساب..."}

    # 2. فلتر التقلب (التحقق من حركة السعر في آخر 5 دقائق)
    recent_prices = history[-50:] # تقريباً آخر فترة
    price_range = max(recent_prices) - min(recent_prices)
    volatility = (price_range / current_price) * 100
    if volatility < 0.0005: # إذا كانت الحركة أقل من 0.05%
        return {"status": "none", "message": "السوق راكد جداً (تقلب منخفض). لا يُنصح بالتداول الآن."}

    # 3. تأكيد الاتجاه (تقريبي للفريم الأعلى)
    # نستخدم المتوسط الأطول (50) للتأكد من الاتجاه العام
    ema50 = calculate_ema(history, 50)
    
    is_uptrend = ema9 > ema21 and (ema50 is None or current_price > ema50)
    is_downtrend = ema9 < ema21 and (ema50 is None or current_price < ema50)
    
    is_rsi_safe_buy = 40 < rsi < 70
    is_rsi_safe_sell = 30 < rsi < 60
    is_macd_bullish = macd > 0
    is_macd_bearish = macd < 0

    signal = "NEUTRAL"
    reasons = []

    if is_uptrend and is_rsi_safe_buy and is_macd_bullish and current_price > ema9:
        signal = "BUY"
        reasons.append("اتجاه صاعد (EMA 9 > 21 > 50)")
        reasons.append("زخم إيجابي (MACD > 0)")
        reasons.append(f"RSI آمن ({rsi:.1f})")
    elif is_downtrend and is_rsi_safe_sell and is_macd_bearish and current_price < ema9:
        signal = "SELL"
        reasons.append("اتجاه هابط (EMA 9 < 21 < 50)")
        reasons.append("زخم سلبي (MACD < 0)")
        reasons.append(f"RSI آمن ({rsi:.1f})")

    if signal == "NEUTRAL":
        return {"status": "none", "message": "المؤشرات غير متوافقة. الأفضل الانتظار."}

    return {
        "status": "success",
        "signal": {
            "symbol": symbol, "direction": signal, "price": current_price,
            "score": 95, "reasons": reasons, "time": data['time']
        }
    }

# 4. نقطة نهاية جديدة لتسجيل نتائج الصفقات (للتحسين المستقبلي)
@app.post("/api/feedback")
def feedback(symbol: str, direction: str, result: str, authorization: str | None = Header(None)):
    verify(authorization)
    prices.add_signal_result(symbol, direction, result)
    return {"status": "ok", "message": "تم تسجيل النتيجة."}

# 5. نقطة نهاية جديدة لجلب سجل الإشارات
@app.get("/api/history")
def history(authorization: str | None = Header(None)):
    verify(authorization)
    return {"history": list(prices.signal_history), "consecutive_losses": prices.consecutive_losses}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("bot:app", host="0.0.0.0", port=8000, reload=False)
