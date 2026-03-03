# Throughput Monitor for MongoDB

A .NET 8 console tool that monitors message throughput between RabbitMQ and MongoDB in real time. Built for measuring whether ServiceControl can keep up with message load during performance testing.

## What It Does

Polls RabbitMQ and MongoDB every 2 seconds and displays a live dashboard showing:

| Metric                                             | Source                  |
| -------------------------------------------------- | ----------------------- |
| Publish / Deliver / Ack rates (msg/s)              | RabbitMQ Management API |
| Queue depth + trend (stable/growing/drain/BACKLOG) | RabbitMQ Management API |
| MongoDB insert rate (msg/s)                        | Delta between polls     |
| Body write rate + lag                              | Delta between polls     |
| TTL deletes (del/s)                                | MongoDB `serverStatus`  |
| WiredTiger dirty/used cache %                      | MongoDB `serverStatus`  |
| Ping latency (ms)                                  | MongoDB `ping` command  |
| Storage size (MB)                                  | MongoDB `dbStats`       |

Each sample is also written to a timestamped CSV file for later analysis.

On exit (Ctrl+C or timeout), a summary is printed with avg/min/max/P95 for every metric, plus a verdict on whether ServiceControl kept up.

## Prerequisites

- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0)
- RabbitMQ with Management plugin enabled (default: `localhost:15672`, `guest`/`guest`)
- MongoDB (default: `localhost:27017`)
- A MongoDB database named `audit` with `processedMessages` and `messageBodies` collections

## Usage

```bash
dotnet run
```

Configuration is via environment variables, with defaults suitable for local development:

| Environment Variable      | Default                     | Description                  |
| ------------------------- | --------------------------- | ---------------------------- |
| `RABBIT_MANAGEMENT_URL`   | `http://localhost:15672`    | RabbitMQ Management API URL  |
| `RABBIT_USER`             | `guest`                     | RabbitMQ username            |
| `RABBIT_PASS`             | `guest`                     | RabbitMQ password            |
| `RABBIT_VHOST`            | `%2F`                       | RabbitMQ vhost (URL-encoded) |
| `RABBIT_QUEUE`            | `audit`                     | Queue to monitor             |
| `MONGO_CONNECTION_STRING` | `mongodb://localhost:27017` | MongoDB connection string    |
| `MONGO_DATABASE`          | `audit`                     | MongoDB database name        |
| `MONGO_COLLECTION`        | `processedMessages`         | Primary collection to track  |

`pollIntervalMs` (2000) and `maxRunTime` (60 minutes) are set in `Program.cs`.

## Status Indicators

| Status    | Meaning                                        |
| --------- | ---------------------------------------------- |
| `OK`      | All metrics within thresholds                  |
| `DIRTY!`  | WiredTiger dirty cache >= 15%                  |
| `SLOW!`   | MongoDB ping latency >= 500ms                  |
| `BACKLOG` | Queue depth growing for 5+ consecutive samples |
| `ERR`     | Failed to query MongoDB health                 |

## Queue Trend Detection

The monitor tracks queue depth over a sliding window of 15 samples (30 seconds). The trend indicator shows:

- **stable** — queue depth not consistently changing
- **growing** — 2–4 consecutive increases
- **BACKLOG** — 5+ consecutive increases (ServiceControl falling behind)
- **drain** — queue depth decreasing

## Output

### Console

A formatted table refreshed every poll interval with column headers repeated every 20 rows.

### CSV

Written to the working directory as `throughput-YYYYMMDD-HHmmss.csv` with columns:
`Timestamp, RMQ_Publish_MsgSec, RMQ_Deliver_MsgSec, RMQ_Ack_MsgSec, Queue_Depth, Mongo_Docs, Mongo_MsgSec, Body_MsgSec, Body_Lag, TTL_Del_Sec, TTL_Pass, Dirty_Pct, Used_Pct, Ping_Ms, Data_MB, Storage_MB, Index_MB, Queue_Trend, Status`

### Summary

Printed on exit with avg/min/max/P95 for all metrics, storage high-water marks, threshold breach counts, and a verdict:

- **"ServiceControl could NOT keep up"** — BACKLOG detected during the run
- **"kept up but queue depth was elevated"** — no backlog but queue averaged > 50
- **"ServiceControl kept up with the load"** — healthy
