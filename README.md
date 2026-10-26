# Wildlife Bioacoustics Streaming Platform

An end-to-end data engineering project for collecting, processing, and analyzing wildlife acoustic detections using Apache Kafka, Spark Structured Streaming, and Delta Lake.

## Overview

This project explores how continuously arriving wildlife detection records can be ingested, distributed, processed, and stored for historical and near-real-time analysis.

BirdWeather provides acoustic detection records from monitoring stations, including species identification, detection timestamps, confidence values, station information, and geographic coordinates.

The primary goal is to learn and implement a streaming data platform from the ground up.

## Planned Architecture

```text
BirdWeather GraphQL API
        |
        v
Python Ingestion Client
        |
        v
Apache Kafka
        |
        v
Spark Structured Streaming
        |
        v
Delta Lake
   |     |     |
 Bronze Silver Gold
        |
        v
SQL Analytics
```

The initial ingestion approach uses GraphQL queries and polling. A native GraphQL subscription may be revisited if a working public subscription transport becomes available.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | API ingestion and event publishing |
| BirdWeather GraphQL API | Wildlife detection source |
| Apache Kafka | Event streaming and retention |
| Apache Spark / PySpark | Distributed data processing |
| Spark Structured Streaming | Continuous event processing |
| Delta Lake | Reliable historical data storage |
| Databricks | Spark and Delta Lake learning environment |
| Docker Compose | Reproducible local infrastructure |
| GitHub Actions | Future automated testing |

## Development Progress

**Completed**
- [x] Explore BirdWeather's GraphQL API
- [x] Inspect detection schemas and nested fields
- [x] Build a Python HTTP ingestion client
- [x] Add HTTP and GraphQL error checks
- [x] Implement cursor-based pagination
- [x] Add basic in-memory detection deduplication

**In progress**
- [ ] Set up a local Kafka broker using Docker Compose

**Planned**
- [ ] Learn Kafka topics, producers, consumers, partitions, and offsets
- [ ] Publish BirdWeather detection records to Kafka
- [ ] Implement Spark Structured Streaming consumption
- [ ] Explore event time, windows, watermarks, and checkpoints
- [ ] Store detections using Delta Lake
- [ ] Build Bronze, Silver, and Gold tables
- [ ] Query wildlife activity with SQL
- [ ] Build an optional visualization/dashboard

## Example Analytical Questions

- Which species are detected most frequently?
- Which stations record the most activity?
- How does detection activity change throughout the day?
- How many distinct species are detected within a given time window?
- How does wildlife activity vary by geographic location?

## Current Limitations

The Python client currently performs bounded GraphQL retrieval rather than continuous ingestion.

Detection deduplication is maintained in memory and does not yet survive process restarts. Durable checkpoints, polling recovery, Kafka integration, and downstream processing are future milestones.

## Project Scope

Version 1 focuses on data engineering, event streaming, distributed processing, and lakehouse fundamentals.

Machine learning and wildlife anomaly detection are intentionally excluded from V1 and may be explored in later versions.

## Development Philosophy

Each component is introduced to solve a specific engineering problem. The emphasis is on understanding system behavior, reliability, failure recovery, and reproducibility rather than maximizing the number of technologies used.
