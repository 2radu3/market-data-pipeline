# 📈 Market Data Pipeline

A scalable, low-latency data pipeline designed to ingest, process, store, and serve real-time and historical financial market data (equities, crypto, forex).

---

## 🔭 Overview

The **Market Data Pipeline** is built to handle streaming ticks, order book updates, and OHLCV (Open, High, Low, Close, Volume) candlestick aggregations from financial exchanges and market data APIs. It guarantees fault tolerance, backpressure handling, and low-latency persistence for downstream quantitative research, trading algorithms, and real-time dashboards.

---

## 🏗 Architecture

```text
 ┌─────────────────┐
 │ Market Feeds    │ (WebSockets / REST APIs: Binance, Alpaca, Polygon, etc.)
 └────────┬────────┘
          │ (Raw JSON / Protocol Buffers)
          ▼
 ┌─────────────────┐
 │ Ingestion Layer │ (Producers / Connectors)
 └────────┬────────┘
          │
          ▼
 ┌─────────────────┐
 │ Message Broker  │ (Apache Kafka / Redpanda / RabbitMQ)
 └────────┬────────┘
          │
          ▼
 ┌─────────────────┐
 │ Stream Engine   │ (Windowing, Deduplication & Candlestick Aggregation)
 └────────┬────────┘
          │
          ├────────────────────────┐
          ▼                        ▼
 ┌─────────────────┐      ┌─────────────────┐
 │ Time-Series DB  │      │ Cache / Serving │
 │ (TimescaleDB /  │      │ (Redis / FastAPI│
 │  ClickHouse)    │      │  WebSockets)    │
 └─────────────────┘      └─────────────────┘
```
## 🚀 Getting Started
```
git clone https://github.com/2radu3/market-data-pipeline
cd market-data-pipeline
```

