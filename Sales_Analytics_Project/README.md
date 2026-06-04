# Project 1 — Real-Time Sales Analytics with Spark Structured Streaming

> **Platform:** Databricks Free Edition | **Language:** PySpark | **Sink:** Delta Lake

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Why This Flow? — Business Necessity](#2-why-this-flow--business-necessity)
3. [Architecture](#3-architecture)
4. [Component Breakdown](#4-component-breakdown)
5. [Key Streaming Concepts Used](#5-key-streaming-concepts-used)
6. [Folder & Table Structure](#6-folder--table-structure)
7. [How to Run](#7-how-to-run)
8. [Limitations & Design Decisions](#8-limitations--design-decisions)
9. [What's Next — Project 2 Preview](#9-whats-next--project-2-preview)

---

## 1. Project Overview

This project implements a **Spark Structured Streaming pipeline** that continuously ingests retail sales transaction events, performs real-time windowed aggregations, and writes results to a Delta Lake table — all running on **Databricks Free Edition** using serverless compute.

The pipeline answers a simple but business-critical question in near real-time:

> *"How much revenue has each product category generated in the last minute?"*

---

## 2. Why This Flow? — Business Necessity

In traditional batch processing, sales reports are generated once a day (or once an hour at best). A retail business operating at scale cannot afford to wait that long to detect issues such as:

- A sudden drop in Electronics sales (possible inventory or pricing bug)
- A spike in Grocery orders (flash sale response)
- Identifying peak load windows to dynamically adjust staffing or logistics

**Structured Streaming solves this** by treating the incoming data as a continuously growing table. Instead of re-running a full batch job, Spark incrementally processes only the new data in each micro-batch trigger — giving you **up-to-date aggregations with low latency and high reliability**.

The specific aggregation chosen — **revenue and order count per product category per 1-minute tumbling window** — is a foundational real-time analytics pattern used in e-commerce dashboards, fraud monitoring, and operations control towers.

---

## 3. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATABRICKS FREE EDITION                      │
│                   (Serverless Compute)                          │
│                                                                 │
│  ┌──────────────┐     ┌────────────────────────────────────┐   │
│  │   PRODUCER   │     │     SPARK STRUCTURED STREAMING     │   │
│  │  (Notebook)  │     │                                    │   │
│  │              │     │  readStream (Auto Loader)          │   │
│  │  Generates   │────▶│       ↓                            │   │
│  │  fake JSON   │     │  withWatermark("event_time","2m")  │   │
│  │  transaction │     │       ↓                            │   │
│  │  files       │     │  groupBy(window(1 min), category)  │   │
│  └──────┬───────┘     │       ↓                            │   │
│         │             │  agg(sum(amount), count(id))       │   │
│         ▼             │       ↓                            │   │
│  ┌──────────────┐     │  writeStream (update mode)         │   │
│  │Unity Catalog │     │       ↓                            │   │
│  │   VOLUME     │     └──────────────┬─────────────────────┘   │
│  │              │                    │                          │
│  │raw_transactions◀──────────────────┘                         │
│  │  (JSON files)│                    │                          │
│  │              │                    ▼                          │
│  │  checkpoints │     ┌──────────────────────────────────┐     │
│  │  (state)     │     │         DELTA TABLE              │     │
│  └──────────────┘     │  revenue_by_category             │     │
│                        │  (window_start, window_end,      │     │
│                        │   category, total_revenue,       │     │
│                        │   order_count)                   │     │
│                        └──────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow Summary

```
Producer Notebook
    │
    │  Writes batch_<uuid>.json files
    ▼
Unity Catalog Volume  (/Volumes/streaming_demo/sales/raw_transactions/)
    │
    │  Auto Loader detects new files (cloudFiles format)
    ▼
readStream  →  Schema enforcement  →  Watermark (2 min)
    │
    │  Tumbling window groupBy (1 minute) + category
    ▼
Aggregation  (SUM of amount, COUNT of transaction_id)
    │
    │  outputMode = "update"
    ▼
writeStream  →  Checkpoint  →  Delta Table (revenue_by_category)
```

---

## 4. Component Breakdown

### 4.1 Producer (Simulated Source)
A Databricks notebook that generates synthetic sales transactions using Python's `random` and `uuid` libraries. Each run writes a new JSON file with 20 transaction records into the landing Volume. This simulates an upstream system (POS terminal, mobile app, API gateway) dropping event files.

**Why JSON files and not Kafka?**
Databricks Free Edition restricts outbound internet access to a limited set of trusted domains. An external Kafka broker is therefore unreachable. JSON files on a Unity Catalog Volume are a practical and valid substitute for local learning — Auto Loader provides the same offset-tracking and exactly-once guarantees that the Kafka connector would.

### 4.2 Unity Catalog Volume (Landing Zone)
A governed storage container under Unity Catalog that holds the incoming JSON files. Unlike DBFS (which is not accessible in Free Edition), Volumes are fully supported and provide proper access control, file system semantics, and compatibility with all compute types in Free Edition.

Two sub-paths are used:
- `raw_transactions/` — landing zone for incoming JSON files
- `checkpoints/` — Spark's checkpoint directory for fault tolerance

### 4.3 Auto Loader (`cloudFiles` format)
Databricks' optimised file streaming source. It maintains an internal state of which files have already been ingested, so re-running or restarting the stream never reprocesses old files. This gives **exactly-once ingestion semantics** without any custom deduplication logic.

### 4.4 Watermark
`.withWatermark("event_time", "2 minutes")` tells Spark to tolerate event records that arrive up to 2 minutes late relative to the maximum event timestamp seen so far. Once a window's watermark threshold is crossed, Spark finalises and evicts that window's state from memory — preventing unbounded state growth.

### 4.5 Tumbling Window Aggregation
A non-overlapping, fixed-size time window of 1 minute. Every transaction is assigned to exactly one window bucket based on its `event_time`. The aggregation computes `SUM(amount)` as `total_revenue` and `COUNT(transaction_id)` as `order_count` per window per category.

### 4.6 Checkpoint
Stored in the Unity Catalog Volume at `checkpoints/agg_query/`. The checkpoint persists the streaming query's progress (offsets processed, aggregation state) to durable storage. If the query crashes or is restarted, Spark recovers exactly from where it left off — no data loss, no duplication.

### 4.7 Delta Table (Sink)
The aggregated results are written as a managed Delta table (`revenue_by_category`) in the `streaming_demo.sales` schema. Delta Lake provides ACID transactions, schema enforcement, and time-travel on the output — making the results immediately queryable via SQL while the stream is still running.

---

## 5. Key Streaming Concepts Used

| Concept | Implementation | Purpose |
|---|---|---|
| `readStream` | `spark.readStream.format("cloudFiles")` | Continuous file ingestion |
| Schema enforcement | `StructType` defined explicitly | Prevents schema drift errors |
| Watermarking | `.withWatermark("event_time", "2 minutes")` | Handles late data; bounds state size |
| Tumbling window | `window(col("event_time"), "1 minute")` | Fixed 1-min aggregation buckets |
| Output mode | `outputMode("update")` | Emits only rows whose aggregates changed |
| Checkpointing | `.option("checkpointLocation", ...)` | Fault tolerance and exactly-once semantics |
| `writeStream` | `.writeStream.format("delta").toTable(...)` | Durable, queryable output |
| Idempotency | Auto Loader file tracking | No duplicate processing on restart |

---

## 6. Folder & Table Structure

```
Unity Catalog
└── streaming_demo                        (Catalog)
    └── sales                             (Schema)
        ├── raw_transactions              (Volume — JSON landing zone)
        │   ├── batch_a1b2c3d4.json
        │   ├── batch_e5f6g7h8.json
        │   └── ...
        ├── checkpoints                   (Volume — Spark state)
        │   ├── schema/                   (Auto Loader schema inference)
        │   └── agg_query/               (writeStream checkpoint)
        └── revenue_by_category          (Managed Delta Table — output)
            columns:
              window_start  TIMESTAMP
              window_end    TIMESTAMP
              category      STRING
              total_revenue DOUBLE
              order_count   BIGINT
```

---

## 7. How to Run

**Prerequisites:** A Databricks Free Edition account at [databricks.com/learn/free-edition](https://www.databricks.com/learn/free-edition)

**Step 1 — Setup (run once)**
Open a notebook and execute the catalog/schema/volume creation SQL from the setup step.

**Step 2 — Start the streaming query**
Run the readStream → aggregation → writeStream notebook. Keep this notebook running; the query will stay active and wait for new files.

**Step 3 — Produce data**
In a separate notebook, run the producer cell. Each run drops a new JSON file into the Volume.

**Step 4 — Query results**
```sql
SELECT *
FROM streaming_demo.sales.revenue_by_category
ORDER BY window_start DESC, total_revenue DESC;
```

**Step 5 — Repeat**
Run the producer again, wait a few seconds, re-query. Watch the `total_revenue` and `order_count` values update for active windows.

**To stop the stream:**
```python
query.stop()
```

---

## 8. Limitations & Design Decisions

This project was designed within the constraints of **Databricks Free Edition**. The following limitations shaped several architectural choices:

### 8.1 No External Network Access
Free Edition serverless compute restricts outbound internet traffic to a limited set of pre-approved domains. This means **external Kafka brokers, Kinesis streams, or any self-hosted message queue are inaccessible**. As a result, file-based streaming via a Unity Catalog Volume was chosen as the source instead of the more production-typical Kafka connector.

*In Project 2, this will be addressed using Confluent Cloud's managed Kafka service, which is accessible over HTTPS/SASL and falls within allowed domains.*

### 8.2 No DBFS Access
DBFS (Databricks File System) is not available in Free Edition. All file I/O — including checkpoint storage and the JSON landing zone — uses **Unity Catalog Volumes**, which are the supported alternative and provide equivalent functionality with added governance.

### 8.3 Serverless Compute Only
Free Edition provides only serverless compute. Custom cluster configurations, GPU runtimes, and classic interactive clusters are unavailable. This limits the ability to install arbitrary JVM libraries or custom Kafka connectors that require cluster-init scripts.

### 8.4 Single Active Pipeline
Free Edition allows only **one active Lakeflow Spark Declarative Pipeline** at a time. Interactive notebook streaming queries (as used in this project) are not subject to this limit and can run concurrently up to the fair-usage compute quota.

### 8.5 Simulated Producer, Not a Real Source
The producer is a manually triggered notebook cell, not a continuously running process. This means the pipeline demonstrates **triggered micro-batch semantics**, not continuous low-latency streaming. The streaming logic itself is identical — only the frequency of source data arrival differs.

### 8.6 No Monitoring Infrastructure
Production streaming pipelines are typically monitored via Grafana, Prometheus, or Datadog. Free Edition does not support custom monitoring integrations. The built-in Spark UI (`query.lastProgress`) is the only available observability tool in this setup.

### 8.7 `outputMode("update")` with Delta
`update` mode is used because the aggregation includes a watermark, making it the correct and only valid mode for aggregations with late-data handling when writing to Delta. `complete` mode would require holding the entire result set in memory and rewriting it each trigger — impractical for unbounded streams. `append` mode is incompatible with aggregations that can be updated by late-arriving data.

### 8.8 `writeStream` — Trigger Option Must Be Explicitly Set
Databricks Free Edition's serverless compute **does not allow the default continuous trigger** on `writeStream`. Running a stream without an explicit trigger will fail or behave unexpectedly. You must always specify:

```python
.trigger(availableNow=True)
```

`availableNow=True` processes all data currently available in the source in one or more micro-batches and then stops the query automatically — similar to a batch job, but using the full Structured Streaming engine (with checkpointing, watermarks, and state). This is the only reliable trigger mode supported on Free Edition serverless.

> **Production note:** In a paid Databricks environment with classic compute, you would typically use `.trigger(processingTime="10 seconds")` for continuous near-real-time processing, or omit the trigger entirely to use the default (as-fast-as-possible) mode.

### 8.9 `outputMode("append")` Instead of `"update"`
Free Edition serverless compute **does not support `outputMode("update")`** on `writeStream`. Despite `update` being the theoretically correct mode for watermarked aggregations (it emits only rows that changed), you must use:

```python
.outputMode("append")
```

With `append` mode and a watermark in place, Spark will only emit a window's aggregated result **once** — after the watermark has passed that window's end time and the window is considered finalised. This means results appear slightly later than `update` mode (you wait for the watermark to advance past the window), but the output is correct and non-duplicated.

| Mode | Behaviour | Free Edition Support |
|---|---|---|
| `append` | Emits a window row only once, after watermark finalises it | ✅ Supported |
| `update` | Emits a window row every trigger it changes | ❌ Not supported on serverless |
| `complete` | Rewrites the entire result table every trigger | ❌ Impractical for unbounded streams |

---

## 9. What's Next — Project 2 Preview

Project 2 will introduce increased complexity on top of this foundation:

- **Kafka (Confluent Cloud) as the source** — real unbounded stream, no manual file drops
- **Sliding windows** — overlapping time windows for rolling averages
- **Stream-stream joins** — enriching transactions with a live promotions stream
- **`foreachBatch` with Delta MERGE** — custom upsert logic for full idempotency control
- **Schema evolution** — handling upstream changes to the event payload

---

*Built for learning purposes on Databricks Free Edition. Not intended for production use.*
