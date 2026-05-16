# Bot Street Backend

Kafka-native distributed market infrastructure for a real-time algorithmic trading simulator.

Bot Street Backend powers the exchange engine, trading agents, analytics pipeline, portfolio system, sentiment engine, REST APIs, and MCP-compatible AI trading interfaces.

---

# Overview

This repository implements a distributed event-driven market simulation architecture inspired by modern electronic exchanges.

Every subsystem communicates exclusively through Kafka topics:

* matching engine
* trading bots
* portfolio ledger
* candle aggregation
* market sentiment
* circuit breakers
* API services
* AI agents

The system is deployed on:

* Google Kubernetes Engine (GKE)
* Apache Kafka (KRaft mode)
* MongoDB
* Docker containers

Frontend terminal is deployed independently on Google Cloud Run.

---

# Core Concepts

Bot Street models actual exchange infrastructure concepts:

* price-time priority matching
* distributed event streaming
* market microstructure analytics
* sentiment propagation
* quant indicators
* portfolio accounting
* risk metrics
* circuit breakers
* autonomous trading participants

This is not a toy REST-only simulator.

The architecture is intentionally event-driven and service-oriented.

---

# Architecture

```text
                         ┌────────────────────┐
                         │ React Terminal UI  │
                         │  Cloud Run         │
                         └─────────┬──────────┘
                                   │ REST / MCP
                                   ▼
                     ┌──────────────────────────┐
                     │ FastAPI Gateway Service  │
                     │ MCP Server               │
                     └──────────┬───────────────┘
                                │
                                ▼
                    ┌──────────────────────────┐
                    │ Apache Kafka (KRaft)     │
                    │ Event Backbone           │
                    └──────┬─────────┬─────────┘
                           │         │
         ┌─────────────────┘         └─────────────────┐
         ▼                                             ▼

┌─────────────────┐                        ┌─────────────────┐
│ Matching Engine │                        │ Sentiment Engine│
└─────────────────┘                        └─────────────────┘

┌─────────────────┐                        ┌─────────────────┐
│ Portfolio Ledger│                        │ Candle Engine   │
└─────────────────┘                        └─────────────────┘

┌─────────────────┐                        ┌─────────────────┐
│ Trading Bots    │                        │ Risk Analytics  │
└─────────────────┘                        └─────────────────┘
```

---

# Tech Stack

| Layer            | Technology               |
| ---------------- | ------------------------ |
| Language         | Python 3.12              |
| API              | FastAPI                  |
| Messaging        | Apache Kafka             |
| Kafka Client     | confluent-kafka          |
| Database         | MongoDB                  |
| Containerization | Docker                   |
| Orchestration    | Kubernetes (GKE)         |
| AI Protocol      | MCP                      |
| Deployment       | Google Kubernetes Engine |
| Package Manager  | UV                       |

---

# Distributed Services

Each service is independently deployable.

| Service    | Responsibility                   |
| ---------- | -------------------------------- |
| api        | FastAPI gateway + MCP server     |
| engine     | Matching engine                  |
| bots       | Algorithmic trading participants |
| ledger     | Portfolio + PnL accounting       |
| candles    | OHLCV aggregation                |
| sentiment  | Market sentiment analysis        |
| price-feed | Market state propagation         |
| circuit    | Circuit breaker management       |

All services consume and produce Kafka events.

---

# Repository Structure

```text
bot-street/
├── api/
│   ├── routes/
│   ├── models.py
│   ├── main.py
│   └── mcp_server.py
│
├── engine/
│   ├── matching_engine.py
│   ├── order_book.py
│   ├── portfolio_ledger.py
│   ├── candle_aggregator.py
│   └── circuit_breaker.py
│
├── market/
│   ├── price_feed.py
│   ├── sentiment_engine.py
│   └── quant/
│       ├── indicators.py
│       ├── risk.py
│       └── microstructure.py
│
├── participants/
│   ├── market_maker.py
│   ├── momentum_bot.py
│   ├── mean_reversion_bot.py
│   └── random_bot.py
│
├── db/
├── core/
├── services/
├── k8s/
└── kafka.yaml
```

---

# Kafka Topics

| Topic              | Purpose                 |
| ------------------ | ----------------------- |
| market-orders      | Incoming order stream   |
| trade-executed     | Trade settlement events |
| price-update       | Real-time market state  |
| candles            | OHLCV candles           |
| portfolio-snapshot | Portfolio state         |
| market-sentiment   | Sentiment propagation   |
| market-halt        | Circuit breaker events  |
| order-expired      | TTL expiration events   |

---

# Trading Engine

The matching engine implements:

* price-time priority
* limit orders
* market orders
* partial fills
* order expiration
* bid/ask book management

All executions are emitted as Kafka events.

---

# Quant Infrastructure

## Indicators

Implemented in `market/quant/indicators.py`

* EMA
* RSI
* MACD
* Bollinger Bands
* VWAP
* ATR

---

## Risk Metrics

Implemented in `market/quant/risk.py`

* Sharpe Ratio
* Sortino Ratio
* VaR / CVaR
* Max Drawdown
* Calmar Ratio
* Profit Factor

---

## Market Microstructure

Implemented in `market/quant/microstructure.py`

* Order Flow Imbalance
* Bid-Ask Spread
* Kyle Lambda
* Market Impact
* Amihud Illiquidity
* Trade Arrival Rate

---

# Trading Bots

The simulator includes autonomous market participants.

| Bot                | Strategy                            |
| ------------------ | ----------------------------------- |
| Market Maker       | Provides liquidity around mid-price |
| Momentum Bot       | EMA crossover trend following       |
| Mean Reversion Bot | RSI + VWAP mean reversion           |
| Random Bot         | Noise trading                       |

Bots consume Kafka events and trade continuously.

---

# REST API

## Orders

```http
POST /orders
DELETE /orders/{id}
```

## Market Data

```http
GET /market/{symbol}/price
GET /market/{symbol}/candles
GET /market/{symbol}/sentiment
GET /market/all/prices
```

## System

```http
GET /system/health
GET /system/status
GET /system/leaderboard
```

Swagger Docs:

```text
http://localhost:8000/docs
```

---

# MCP Server

The backend exposes an MCP-compatible interface for AI agents.

Mounted at:

```text
/mcp
```

Available tools include:

* get_prices
* get_order_book
* get_sentiment
* get_indicators
* get_portfolio
* place_order
* get_leaderboard
* get_market_status

This enables autonomous LLM agents to trade directly against the exchange.

---

# Kubernetes Deployment

Infrastructure is designed for GKE deployment.

Included manifests:

```text
k8s/
├── deployments/
├── services/
├── ingress/
├── kafka/
├── configmap.yaml
└── secret.yaml
```

---

# Deployment Architecture

| Component          | Platform                 |
| ------------------ | ------------------------ |
| Backend Services   | Google Kubernetes Engine |
| Kafka              | GKE StatefulSet          |
| API Gateway        | GKE                      |
| Frontend           | Google Cloud Run         |
| Container Registry | Artifact Registry        |

---

# Local Development

## Requirements

* Python 3.12+
* Docker
* Kubernetes CLI
* UV
* Kafka

---

## Install

```bash
uv sync
```

---

## Run API

```bash
uvicorn api.main:app --reload
```

---

## Run Services

```bash
python services/run_engine.py
python services/run_bots.py
python services/run_sentiment.py
```

---

# Example Symbols

| Symbol | Company             |
| ------ | ------------------- |
| PEAR   | Pear Technologies   |
| TSLA   | TeslaCoil Motors    |
| LBRY   | Labyrinth Search    |
| RNFR   | Rainforest Commerce |
| MHRD   | Microhard Corp      |

---

# Design Goals

* Event-driven infrastructure
* Distributed service orchestration
* AI-native interfaces
* Quantitative analytics
* Real-time market simulation
* Cloud-native deployment
* Low coupling between services

---

# License

MIT

---

# Author

Sujal Maheshwari
