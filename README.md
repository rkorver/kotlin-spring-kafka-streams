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
| Adapter | `adapters.persistence` | jOOQ implementation of `ParcelJourneyRepository` |
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
6. A plain `@KafkaListener` consumes completed events and persists them via jOOQ

## Multi-Module Structure

```
├── api/       REST API interface + response DTOs
├── app/       Spring Boot application, all adapters, domain, config
└── spi/       Avro schemas, Kafka topic constants, handler contracts
```

## Tech Stack

- **Kotlin 2.3** / **Java 25**
- **Spring Boot 4.0** with Spring Kafka
- **Kafka Streams** (Confluent 8.1)
- **Avro 1.12** for event serialization (Schema Registry)
- **PostgreSQL** + Flyway + **jOOQ**
- **Testcontainers 2.0** for integration tests (Redpanda, PostgreSQL)

## Running Locally

### Prerequisites

- Java 25+
- Docker (for Testcontainers)

### Run tests

```bash
./mvnw verify
```

### Run locally (with Testcontainers)

Starts the app with Testcontainers providing PostgreSQL, Redpanda (Kafka + Schema Registry):

```bash
./mvnw spring-boot:test-run -pl app
```

### Query a completed journey

```bash
curl http://localhost:8080/parcels/{parcelId}
```
