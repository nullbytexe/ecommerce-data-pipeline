# E-commerce Data Engineering Pipeline

![License](https://img.shields.io/badge/license-MIT-blue) ![Docker](https://img.shields.io/badge/docker-compose-2496ED) ![Python](https://img.shields.io/badge/python-3.11-3776AB)

An event-driven data pipeline for e-commerce transaction data. Synthetic orders are streamed through Kafka, persisted to PostgreSQL, aggregated on a schedule by Airflow, and served through a Flask REST API backed by Redis for caching and real-time counters.

## Table of Contents
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [API Reference](#api-reference)
- [Database Schema](#database-schema)
- [Airflow Orchestration](#airflow-orchestration)
- [Project Structure](#project-structure)
- [Monitoring & Troubleshooting](#monitoring--troubleshooting)
- [Design Notes](#design-notes)
- [Roadmap](#roadmap)
- [License](#license)

## Architecture

```
Producer ──▶ Kafka (KRaft) ──▶ Consumer ──▶ PostgreSQL (raw)
 (1 order/2s)                      │                │
                                   ▼                ▼
                                 Redis      Airflow DAG (@hourly)
                          (cache + counters)         │
                                   │                  ▼
                                   │      PostgreSQL (processed / analytics)
                                   │                  │
                                   └─────────┬────────┘
                                             ▼
                                         Flask API
```

## Tech Stack

| Layer | Technology |
|---|---|
| Message broker | Apache Kafka (KRaft mode, no Zookeeper) |
| Ingestion | Python (`kafka-python`, `faker`) |
| Storage | PostgreSQL 15 — three schemas: `raw`, `processed`, `analytics` |
| Cache / counters | Redis 7 |
| Orchestration | Apache Airflow 2.9.3 (LocalExecutor) |
| API | Flask 3.0 + Flask-CORS |
| Infrastructure | Docker Compose |

## Getting Started

### Prerequisites
- Docker & Docker Compose
- 8GB RAM and 10GB disk space recommended

### Installation

```bash
git clone https://github.com/nullbytexe/ecommerce-data-pipeline
cd ecommerce-data-pipeline
cp .env.example .env      # set POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_DB
docker-compose up -d
docker-compose ps          # confirm all services are healthy
```

On first run, `airflow-init` performs a one-time DB migration and creates the `admin` user before `airflow-webserver` and `airflow-scheduler` are allowed to start, so initial startup takes a bit longer than subsequent ones.

### Service Endpoints

All ports are bound to `127.0.0.1` only.

| Service | Address | Auth |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| Flask API | http://localhost:5000 | — |
| Airflow UI | http://localhost:8081 | `admin` / `admin` |
| PostgreSQL | `localhost:5433` | via `.env` |
| Redis | `localhost:6379` | — |

## API Reference

| Method | Endpoint | Data Source | Notes |
|---|---|---|---|
| GET | `/api/dashboard` | `raw.orders`, `raw.customers`, `raw.products` | Cache-aside via Redis, key `dashboard_metrics`, 30s TTL |
| GET | `/api/sales/daily?days=` | `processed.daily_sales` | |
| GET | `/api/products/top?limit=` | view `analytics.top_products` | |
| GET | `/api/customers/segments` | view `analytics.customer_segments` | |
| GET | `/api/orders/recent?limit=` | `raw.orders` joined with `raw.customers` | |
| GET | `/api/metrics/realtime` | Redis keys `total_orders`, `total_revenue` | Reads counters directly; no PostgreSQL query, no TTL |
| GET | `/api/sales/overview?days=` | view `analytics.sales_overview` | |
| GET | `/api/customers/<customer_id>` | `raw.customers` | Cache-aside via Redis, key `customer:{id}`, 1h TTL |
| GET | `/api/products/<product_id>` | `raw.products` | Cache-aside via Redis, key `product:{id}`, 1h TTL |

## Database Schema

Primary database: value of `POSTGRES_DB` in `.env` (default `ecommerce`).

| Schema | Object | Type | Written by |
|---|---|---|---|
| `raw` | `orders`, `order_items`, `customers`, `products` | Table | Consumer (upserted from Kafka) |
| `processed` | `daily_sales`, `product_performance` | Table | Airflow (current day only) |
| `processed` | `customer_metrics` | Table | Airflow (full history) |
| `analytics` | `hourly_metrics` | Table | Airflow (trailing 1 hour) |
| `analytics` | `sales_overview`, `top_products`, `customer_segments` | View | Computed on query, not materialized |

A separate physical database, `airflow_meta` (created by `postgres/03-airflow-db.sql`), holds Airflow's own metadata and is isolated from the business database above.

## Airflow Orchestration

The DAG `ecommerce_metrics_aggregation` (`airflow/dags/ecommerce_metrics_dag.py`) performs scheduled aggregation of raw order data into the `processed` and `analytics` schemas.

| Property | Value |
|---|---|
| Schedule | `@hourly` |
| Executor | `LocalExecutor` |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| Retries | 2, 2-minute delay |

Four tasks run in parallel — each reads only from `raw.*` and writes to its own table, so there are no inter-task dependencies:

| Task | Writes to | Scope |
|---|---|---|
| `process_daily_sales` | `processed.daily_sales` | `CURRENT_DATE` |
| `process_product_performance` | `processed.product_performance` | `CURRENT_DATE` |
| `process_customer_metrics` | `processed.customer_metrics` | Full history |
| `process_hourly_metrics` | `analytics.hourly_metrics` | Trailing 1 hour |

## Project Structure

```
.
├── producer/           # Generates synthetic orders, publishes to Kafka
├── consumer/           # Consumes Kafka messages, writes to PostgreSQL + Redis
├── api/                # Flask REST API
├── airflow/
│   └── dags/           # Scheduled metrics aggregation DAG
├── postgres/           # init.sql, schema.sql, 03-airflow-db.sql
├── kafka/               # kraft-config.properties
└── docker-compose.yml
```

## Monitoring & Troubleshooting

```bash
# List Kafka topics
docker exec -it kafka_broker kafka-topics --list --bootstrap-server localhost:9092

# Connect to PostgreSQL
docker exec -it postgres_db psql -U dataeng -d ecommerce

# Connect to Redis
docker exec -it redis_cache redis-cli

# Inspect Airflow DAGs
docker exec -it airflow_scheduler airflow dags list
docker exec -it airflow_scheduler airflow dags trigger ecommerce_metrics_aggregation

# Tail logs for specific services
docker-compose logs -f producer consumer api airflow-scheduler airflow-webserver

# Full reset (drops volumes)
docker-compose down -v && docker-compose up -d
```

## Design Notes

- **Ingestion vs. aggregation freshness.** The Producer → Kafka → Consumer path and the two Redis counters (`total_orders`, `total_revenue`) update in real time. The `processed.*` tables and `analytics.hourly_metrics` are refreshed by the Airflow DAG on an hourly schedule, so they can lag ingestion by up to one hour.
- **Caching strategy.** `/api/dashboard`, `/api/customers/<id>`, and `/api/products/<id>` use cache-aside with Redis. `/api/metrics/realtime` instead reads pre-aggregated counters that the Consumer increments on every write — there is no query being cached there.

## License
MIT License