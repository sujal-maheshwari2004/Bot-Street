# Market Simulator

A terminal-based algorithmic market simulator built on Apache Kafka (KRaft mode).
Every participant — user, bots, and engines — communicates exclusively via Kafka messages.

Dual purpose: working simulator AND trading concepts learning tool.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Message Broker | Apache Kafka KRaft (no Zookeeper) |
| Container | Docker Compose |
| Backend | Python 3.12 |
| Kafka Client | confluent-kafka |
| REST API | FastAPI + Uvicorn |
| Terminal UI | Rich |
| CLI | Click |
| Package Manager | UV |

---

## Quick Start

### 1. Prerequisites
- Docker Desktop running
- Python 3.12+
- UV installed: pip install uv

### 2. Start Kafka
docker compose up -d

Verify:
docker compose ps
You should see market-kafka with status healthy.

### 3. Install dependencies
uv sync

### 4. Run the simulator

Full mode — simulation + dashboard:
uv run python main.py dashboard --symbol PEAR

In a second terminal — trading CLI:
uv run python main.py trade

Full mode with REST API:
uv run python main.py all --symbol PEAR
API docs: http://localhost:8000/docs

---

## Run Modes

| Command | Description |
|---|---|
| main.py sim | Headless simulation, no UI |
| main.py dashboard --symbol X | Simulation + terminal dashboard |
| main.py api | Simulation + FastAPI REST server |
| main.py all | Simulation + API + dashboard |
| main.py dash --symbol X | Dashboard only, sim already running |
| main.py trade | Interactive trading CLI |

---

## Directory Structure

```
market-simulator/
│
├── docker-compose.yml          # Kafka KRaft single node
├── config.py                   # All constants and tuning knobs
├── main.py                     # Entry point — all run modes
│
├── core/
│   ├── schemas.py              # All Kafka message types
│   └── kafka_client.py         # Producer/consumer wrappers
│
├── engine/
│   ├── order_book.py           # Limit order book — price-time priority
│   ├── matching_engine.py      # Kafka → order book → Kafka
│   ├── portfolio_ledger.py     # Holdings, cash, P&L, risk per client
│   ├── circuit_breaker.py      # Halts trading on extreme price moves
│   ├── candle_aggregator.py    # OHLCV 10-second buckets
│   └── trade_logger.py         # Appends every trade to trades.jsonl
│
├── market/
│   ├── price_feed.py           # Last price, VWAP, all indicators
│   ├── sentiment_engine.py     # Bullish/bearish signal from trade flow
│   └── quant/
│       ├── indicators.py       # EMA, RSI, MACD, Bollinger, VWAP, ATR
│       ├── risk.py             # Sharpe, Sortino, VaR, CVaR, drawdown
│       └── microstructure.py   # OFI, spread, market impact, Kyle lambda
│
├── participants/
│   ├── base_bot.py             # Shared loop all bots inherit
│   ├── market_maker.py         # Posts both sides, adjusts with sentiment
│   ├── momentum_bot.py         # Trend follower using EMA crossover
│   ├── random_bot.py           # Noise trader
│   └── mean_reversion_bot.py   # Fades RSI extremes back to VWAP
│
├── api/
│   ├── main.py                 # FastAPI app with lifespan management
│   ├── models.py               # Pydantic request/response models
│   ├── mcp_server.py           # MCP server for LLM agent trading
│   └── routes/
│       ├── orders.py           # POST /orders, DELETE /orders/{id}
│       ├── market.py           # GET price, orderbook, candles, sentiment
│       ├── portfolio.py        # GET holdings, P&L, risk metrics
│       └── market_status.py    # GET health, halt status, leaderboard
│
├── user/
│   └── cli.py                  # Interactive trading terminal
│
├── display/
│   └── dashboard.py            # Live Rich terminal dashboard
│
└── data/
    └── trades.jsonl            # Append-only trade audit log
```
---

## Kafka Topics

| Topic | Produced By | Consumed By |
|---|---|---|
| market-orders | User, Bots, API | Matching Engine |
| trade-executed | Matching Engine | Ledger, Price Feed, Candles, Logger |
| price-update | Price Feed | Bots, Circuit Breaker, Sentiment, Dashboard |
| market-sentiment | Sentiment Engine | Bots, Dashboard, API |
| portfolio-snapshot | Portfolio Ledger | Dashboard, API |
| candles | Candle Aggregator | Dashboard, API |
| market-halt | Circuit Breaker | Matching Engine, Bots, Dashboard |
| order-expired | Matching Engine | Dashboard, Logger |

---

## Symbols (Parody)

| Symbol | Company | Volatility |
|---|---|---|
| PEAR | Pear Technologies | Medium |
| TSLA | TeslaCoil Motors | High |
| LBRY | Labyrinth Search | Low |
| RNFR | Rainforest Commerce | Medium |
| MHRD | Microhard Corp | Low |

---

## Participants

| Participant | Strategy |
|---|---|
| User | Manual orders via CLI or REST API |
| Market Maker | Posts bid + ask around mid, skews with sentiment and OFI |
| Momentum Bot | Buys bullish, sells bearish, scales with EMA crossover confirmation |
| Random Bot | Noise trader — random side, size, price every tick |
| Mean Reversion Bot | Fades RSI extremes, anchors to VWAP deviation |

---

## Trading CLI Commands

market> buy PEAR 10 150.50    limit buy 10 shares of PEAR at 150.50
market> sell PEAR 5           market sell 5 shares of PEAR
market> prices                show all current prices
market> portfolio             show holdings and P&L
market> help                  show all commands
market> quit                  exit

---

## REST API Endpoints

Orders:
  POST   /orders                       place order
  DELETE /orders/{order_id}            cancel order

Market Data:
  GET    /market/{symbol}/price        latest price + all indicators
  GET    /market/{symbol}/orderbook    top 10 bids and asks
  GET    /market/{symbol}/candles      last N OHLCV candles
  GET    /market/{symbol}/sentiment    bullish/bearish signal
  GET    /market/all/prices            all symbol prices

Portfolio:
  GET    /portfolio/{client_id}        holdings, cash, P&L, risk metrics

System:
  GET    /system/health                Kafka connectivity check
  GET    /system/status                all symbols halt/active status
  GET    /system/leaderboard           all participants ranked by P&L

---

## Quant Concepts Implemented

Technical Indicators:
  EMA (9, 21)       Exponential moving average, crossover signals
  RSI (14)          Relative strength index, overbought/oversold
  MACD (12/26/9)    Momentum convergence/divergence
  Bollinger Bands   Volatility envelope, squeeze detection
  VWAP              Volume weighted average price, institutional benchmark
  ATR (14)          Average true range, volatility measure

Risk Metrics:
  Sharpe Ratio      Return per unit of total risk
  Sortino Ratio     Return per unit of downside risk only
  Max Drawdown      Worst peak-to-trough decline
  VaR 95%           Value at risk, worst expected loss 95% of the time
  CVaR 95%          Expected loss beyond VaR threshold
  Calmar Ratio      Return divided by max drawdown
  Profit Factor     Total wins divided by total losses

Market Microstructure:
  Bid-Ask Spread    Cost of immediacy, liquidity measure
  OFI               Order flow imbalance, directional pressure
  Market Impact     Price move per unit of volume traded
  Kyle Lambda       Price impact coefficient from Kyle (1985)
  Amihud Illiquidity  Price impact per dollar of volume
  Roll Spread       Implied spread from serial price correlation
  Trade Arrival Rate  Activity level, trades per second

---

## POC Constraints

- No persistence — everything in-memory except trades.jsonl
- No authentication on API or CLI
- Bots run as threads inside one process
- Single Docker container for Kafka
- No short selling — bots can only sell what they hold
- Order book depth display uses synthetic levels for POC

---

## MCP Server (LLM Agent Trading)

The MCP server wraps the FastAPI as tools for any MCP-compatible agent.
Start the full simulation first, then connect your agent to the MCP server.

Available tools:
  get_prices            current price for all symbols
  get_order_book        top bids and asks for a symbol
  get_sentiment         bullish/bearish signal and strength
  get_indicators        RSI, MACD, Bollinger, VWAP, EMA
  get_portfolio         holdings, cash, P&L for any client
  get_candles           last N OHLCV candles
  place_order           submit a buy or sell order
  get_leaderboard       all participants ranked by P&L
  get_market_status     halted/active status per symbol

---

## License

MIT
