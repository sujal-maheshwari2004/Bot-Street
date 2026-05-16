from dotenv import load_dotenv
import os

load_dotenv()

# ── Kafka ─────────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

# ── MongoDB ───────────────────────────────────────────────────────────────────
MONGO_URI = os.getenv("MONGO_URI", "")
MONGO_DB  = os.getenv("MONGO_DB", "bot-street")

# ── Topics ────────────────────────────────────────────────────────────────────
TOPIC_MARKET_ORDERS    = "market-orders"
TOPIC_TRADE_EXECUTED   = "trade-executed"
TOPIC_PRICE_UPDATE     = "price-update"
TOPIC_MARKET_SENTIMENT = "market-sentiment"
TOPIC_PORTFOLIO_SNAP   = "portfolio-snapshot"
TOPIC_CANDLES          = "candles"
TOPIC_MARKET_HALT      = "market-halt"
TOPIC_ORDER_EXPIRED    = "order-expired"

ALL_TOPICS = [
    TOPIC_MARKET_ORDERS,
    TOPIC_TRADE_EXECUTED,
    TOPIC_PRICE_UPDATE,
    TOPIC_MARKET_SENTIMENT,
    TOPIC_PORTFOLIO_SNAP,
    TOPIC_CANDLES,
    TOPIC_MARKET_HALT,
    TOPIC_ORDER_EXPIRED,
]

# ── Symbols (parody) ──────────────────────────────────────────────────────────
# symbol → (display_name, initial_price, volatility_profile)
SYMBOLS = {
    "PEAR":  ("Pear Technologies",    150.00, "medium"),
    "TSLA":  ("TeslaCoil Motors",      89.50, "high"),
    "LBRY":  ("Labyrinth Search",     312.10, "low"),
    "RNFR":  ("Rainforest Commerce",  178.40, "medium"),
    "MHRD":  ("Microhard Corp",       412.00, "low"),
}

SYMBOL_LIST        = list(SYMBOLS.keys())
DEFAULT_SYMBOL     = "PEAR"

# ── Order book ────────────────────────────────────────────────────────────────
TICK_SIZE          = 0.01
LOT_SIZE           = 1
ORDER_TTL_SECONDS  = 60
TTL_SWEEP_INTERVAL = 5

# ── Circuit breaker ───────────────────────────────────────────────────────────
CIRCUIT_BREAKER_THRESHOLD = 0.05
CIRCUIT_BREAKER_WINDOW    = 5
CIRCUIT_BREAKER_DURATION  = 30

# ── Candle aggregator ─────────────────────────────────────────────────────────
CANDLE_INTERVAL_SECONDS = 10
CANDLE_DISPLAY_COUNT    = 20

# ── Sentiment engine ──────────────────────────────────────────────────────────
SENTIMENT_WINDOW     = 20
BULLISH_THRESHOLD    = 0.60
BEARISH_THRESHOLD    = 0.40

# ── Quant indicators ──────────────────────────────────────────────────────────
RSI_PERIOD           = 14
EMA_SHORT            = 9
EMA_LONG             = 21
MACD_SIGNAL          = 9
BOLLINGER_PERIOD     = 20
BOLLINGER_STD        = 2.0
VWAP_RESET_INTERVAL  = 3600

# ── Risk metrics ──────────────────────────────────────────────────────────────
VAR_CONFIDENCE       = 0.95
VOLATILITY_WINDOW    = 30
SHARPE_RISK_FREE     = 0.0

# ── Portfolio ─────────────────────────────────────────────────────────────────
INITIAL_CASH         = 10_000.00
MAX_POSITION_PCT     = 0.40
MAX_POSITION_SHARES  = 500

# ── Bots ──────────────────────────────────────────────────────────────────────
BOT_TICK_SECONDS     = 1.5

# Spread tightened from $0.10 → $0.01 so random/momentum bots can cross it.
# At $0.10 each side the book never fills — aggressive bots place market orders
# but the market maker's quotes are too wide for them to hit at seed prices.
MARKET_MAKER_SPREAD  = 0.01        # $0.01 each side of mid
MARKET_MAKER_QTY     = 10

MOMENTUM_QTY         = 5
MOMENTUM_THRESHOLD   = 0.65

RANDOM_QTY_MAX       = 8

MEAN_REVERSION_QTY   = 5
RSI_OVERBOUGHT       = 70
RSI_OVERSOLD         = 30
VWAP_DEVIATION_PCT   = 0.02

# ── Participants ──────────────────────────────────────────────────────────────
USER_CLIENT_ID         = "user"
MARKET_MAKER_CLIENT_ID = "bot-market-maker"
MOMENTUM_CLIENT_ID     = "bot-momentum"
RANDOM_CLIENT_ID       = "bot-random"
MEAN_REV_CLIENT_ID     = "bot-mean-reversion"

ALL_CLIENT_IDS = [
    USER_CLIENT_ID,
    MARKET_MAKER_CLIENT_ID,
    MOMENTUM_CLIENT_ID,
    RANDOM_CLIENT_ID,
    MEAN_REV_CLIENT_ID,
]