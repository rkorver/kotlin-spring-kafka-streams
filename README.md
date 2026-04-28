# Parcel Tracking — Kafka Streams

A parcel journey tracking service built with **Kotlin**, **Spring Boot**, and **Kafka Streams**. It aggregates events from three independent sources into a single completed journey, persists it, and exposes it via a REST API.

## Architecture

The project follows a **Ports & Adapters** (hexagonal) architecture:

```
┌─────────────────────────────────────────────────────────────────┐
│                          Adapters                               │
│                                                                 │
│  ┌──────────────┐   ┌──────────────────┐   ┌────────────────┐  │
│  │  REST API    │   │  Kafka Streams   │   │  Kafka         │  │
│  │  Controller  │   │  Topology        │   │  Consumer      │  │
│  │  (inbound)   │   │  (inbound)       │   │  (inbound)     │  │
│  └──────┬───────┘   └──────────────────┘   └───────┬────────┘  │
│         │                                          │            │
│         │           ┌──────────────────┐           │            │
│         │           │  JPA Repository  │           │            │
│         │           │  (outbound)      │           │            │
│         │           └──────┬───────────┘           │            │
│         │                  │                       │            │
├─────────┼──────────────────┼───────────────────────┼────────────┤
│         ▼                  ▼                       ▼            │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                     Domain Core                         │    │
│  │                                                         │    │
│  │  ParcelJourney  ·  ParcelJourneyService  ·  Ports       │    │
│  │  (models)         (business logic)         (interfaces)  │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

The **domain** has no dependencies on frameworks or infrastructure. Adapters implement the ports:

| Layer | Package | Responsibility |
|-------|---------|----------------|
| Domain | `domain.model` | Value objects: `ParcelJourney`, `ParcelId`, `TrackingCode`, checkpoints |
| Domain | `domain.port` | `ParcelJourneyRepository` interface |
| Domain | `domain.service` | `ParcelJourneyService` — orchestrates persistence |
| Adapter | `adapters.kafka.topology` | Kafka Streams topology (event aggregation) |
| Adapter | `adapters.kafka.consumer` | `@KafkaListener` for completed journeys |
| Adapter | `adapters.persistence` | JPA implementation of `ParcelJourneyRepository` |
| Adapter | `adapters.web` | REST controller (`GET /parcels/{id}`) |
| SPI | `spi` | Avro schemas, shared topic names, handler contracts |
| API | `api` | REST API interface and response DTOs |

## Event Flow

Three Kafka topics carry parcel lifecycle events, each produced by an independent upstream system:

```
 parcel.received-at-postal-office.v1 ──┐
                                       │
 parcel.sorting-center-events.v1 ──────┤──▶  Kafka Streams   ──▶  parcel.journey-completed.v1
   (filtered: READY_FOR_DELIVERY)      │     Transformer           │
                                       │     (keyed by parcelId)   │
 parcel.delivered-to-customer.v1 ──────┘                           ▼
                                                              @KafkaListener
                                                                   │
                                                                   ▼
                                                              PostgreSQL
                                                                   │
                                                                   ▼
                                                            GET /parcels/{id}
```

1. Events arrive in any order on three topics
2. The Kafka Streams topology merges all three streams, re-keys by `parcelId`
3. A `Transformer` with a local RocksDB state store accumulates partial state
4. Once all three checkpoints are present, a `ParcelJourneyCompleted` event is emitted
5. The state store entry is **deleted** immediately — keeping the store bounded to in-flight parcels only
6. A plain `@KafkaListener` consumes completed events and persists them via JPA

## Why Kafka Streams + Keyed State Store

Each parcel's state is held in a RocksDB-backed `KeyValueStore`, keyed by `parcelId`. This works well because:

- **Automatic sharding** — state is partitioned across stream tasks, no single bottleneck
- **Horizontal scaling** — add more instances to distribute partitions
- **Disk-backed** — RocksDB spills to disk, so millions of in-flight parcels don't require memory proportional to state size
- **Bounded state** — completed journeys are deleted from the store immediately via the Transformer, so the store only holds parcels currently in transit

### Why Transformer instead of KTable aggregate

A plain `.aggregate()` on a `KTable` would keep every parcel's state **forever** — including completed ones. At scale (e.g. 1M parcels/day), this becomes an unbounded storage leak. The `Transformer` gives direct access to the state store, allowing us to `delete(key)` after emitting the completed event.

## Multi-Module Structure

```
├── api/       REST API interface + response DTOs
├── app/       Spring Boot application, all adapters, domain, config
└── spi/       Avro schemas, Kafka topic constants, handler contracts
```

## Tech Stack

- **Kotlin 1.9** / **Java 21**
- **Spring Boot 3.3** with Spring Kafka
- **Kafka Streams** (Confluent 7.6)
- **Avro** for event serialization (Schema Registry)
- **PostgreSQL** + Flyway + Spring Data JPA
- **Testcontainers** for integration tests (Kafka, Schema Registry, PostgreSQL)

## Running Locally

### Prerequisites

- Java 21+
- Docker (for Testcontainers and local infra)

### Run tests

```bash
./mvnw verify
```

### Start infrastructure

Start Kafka, Schema Registry, and PostgreSQL (e.g. via Docker Compose or your preferred method), then:

```bash
./mvnw spring-boot:run -pl app
```

### Query a completed journey

```bash
curl http://localhost:8080/parcels/{parcelId}
```
