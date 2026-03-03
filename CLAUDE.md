# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Important Conventions

When making meaningful changes to the code (new metrics, configuration options, status indicators, output format changes, or behavioral changes), update [README.md](README.md) to reflect those changes.

## Build & Run

```bash
dotnet build
dotnet run
```

There are no tests in this project. No linter is configured.

## What This Project Does

A single-file .NET 8 console app that monitors throughput between RabbitMQ and MongoDB in real time. It polls the RabbitMQ Management API and MongoDB every 2 seconds, prints a formatted table to the console, and writes each sample to a timestamped CSV file.

Designed for measuring whether ServiceControl can keep up with message load — the summary verdict at the end reports backlog, elevated queue depth, or healthy throughput.

## Architecture

Everything lives in [Program.cs](Program.cs) using top-level statements (no `Main` method, no namespace). The file contains:

- **Configuration block** (lines 8–24): hardcoded connection strings, thresholds, and timing. These are local variables, not a config file.
- **Polling loop** (lines 83–312): each iteration fetches RabbitMQ queue stats via HTTP, MongoDB doc counts via `EstimatedDocumentCountAsync`, and MongoDB health via `serverStatus`/`dbStats` commands.
- **Rate calculation**: RabbitMQ rates come from the Management API's built-in `*_details.rate` fields. MongoDB insert rates are computed as deltas between polls, with TTL deletes split proportionally across collections.
- **Queue trend detection**: a sliding window (`trendWindowSize=15` samples) tracks whether the queue is growing, draining, or stable. 5+ consecutive growth samples triggers BACKLOG status.
- **Body lag smoothing**: raw lag between `processedMessages` and `messageBodies` collections is smoothed over a 30-sample window to dampen TTL timing artifacts.
- **`MetricAccumulator`** (line 392): collects min/max/avg/P95 for each metric across the run.
- **`SummaryStats`** (line 421): aggregates all accumulators; printed on exit (Ctrl+C or timeout).

## Key Dependencies

- `MongoDB.Driver` 3.1.0 — the only NuGet package
- RabbitMQ Management HTTP API (no AMQP client library; uses `HttpClient` directly)

## Runtime Requirements

Expects local RabbitMQ (port 15672 management, guest/guest) and MongoDB (port 27017) with a database named `audit` containing `processedMessages` and `messageBodies` collections.
