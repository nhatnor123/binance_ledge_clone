# Binance Ledger Clone

A high-throughput, fault-tolerant, in-memory ledger system built with Java 21, inspired by [Binance's Ledger architecture](https://www.binance.com/en/blog/tech/5409682424466769892).

This project demonstrates how a modern crypto-exchange processes millions of transactions with zero data loss, 24/7 availability, and no row-lock contention — even on "hot accounts" with extreme traffic concentration.

---

## 🎯 What This Project Demonstrates

| Problem | Traditional Approach | Our Approach (Binance-style) |
|---------|---------------------|------------------------------|
| **Hot Account Contention** | Row-level locks in RDBMS → deadlocks, timeouts | Single-threaded state machine → no locks at all |
| **High Throughput** | Thread pool + connection pool bottleneck | LMAX Disruptor ring buffer → lock-free, cache-friendly |
| **Fault Tolerance** | Primary-replica DB replication (async, lossy) | Raft consensus → strong consistency, automatic failover |
| **Read/Write Scaling** | Same DB for reads and writes | CQRS: Raft domain (writes) + Kafka → PostgreSQL (reads) |
| **State Recovery** | WAL replay (slow for large datasets) | RocksDB snapshots + Raft log replay (fast cold start) |

---

## 🏗️ Architecture Overview

```
                          ┌─────────────────────────────────────────┐
                          │           Write Domain (Raft)           │
                          │                                         │
  gRPC Client ──────────► │  Network Layer (Virtual Threads)        │
                          │       │                                 │
                          │       ▼                                 │
                          │  LMAX Disruptor Ring Buffer             │
                          │       │                                 │
                          │       ▼                                 │
                          │  State Machine (Single Platform Thread) │
                          │       │         │                       │
                          │       ▼         ▼                       │
                          │  Raft Layer   RocksDB                   │
                          └───────┬─────────────────────────────────┘
                                  │
                    ┌─────────────┼─────────────┐
                    ▼             ▼             ▼
              Follower 1    Follower 2     Learner Node
                                                │
                                                ▼
                                             Kafka
                                                │
                          ┌─────────────────────┘
                          ▼
                  ┌───────────────┐        ┌──────────────┐
                  │ Kafka Consumer│──────► │  PostgreSQL  │
                  └───────────────┘        └──────┬───────┘
                                                  │
                                                  ▼
                                          REST Query API
```

For a detailed architecture breakdown, see [doc/architecture.md](doc/architecture.md).

---

## 🛠️ Technology Stack

| Component | Technology | Why |
|-----------|-----------|-----|
| Language | Java 21 | Virtual threads for I/O, platform threads for hot paths |
| Build | Gradle 8.x | Multi-module project |
| Framework | Spring Boot 3.2+ | Native virtual thread support |
| Ring Buffer | LMAX Disruptor 4.0 | Lock-free inter-thread messaging (~millions events/sec) |
| Consensus | Apache Ratis 3.1.x | Production-grade Raft implementation |
| State Store | RocksDB 9.x (JNI) | Embedded KV store for snapshots |
| RPC | gRPC + Protobuf | Write domain API (high-performance binary protocol) |
| Messaging | Apache Kafka 3.x | CQRS event streaming |
| Query DB | PostgreSQL 16 | Read domain storage |
| API Docs | SpringDoc OpenAPI 2.x | Auto-generated Swagger UI |
| Benchmark | JMH 1.37 | Micro-benchmarks |
| Container | Docker + Compose | Full-stack deployment |

---

## 📁 Project Structure

```
binance_ledge_clone/
├── doc/                        # Documentation
│   ├── architecture.md         # Architecture deep-dive
│   ├── api-spec.yml            # OpenAPI 3.0 specification
│   ├── deployment-guide.md     # How to run everything
│   ├── test-scenarios.md       # HA, correctness, performance test plans
│   └── implementation-plan.md  # Technical implementation plan
│
├── ledger-common/              # Shared models, protobuf definitions
├── ledger-core/                # State machine + LMAX Disruptor + RocksDB
├── ledger-raft/                # Raft consensus layer (Apache Ratis)
├── ledger-server/              # gRPC write server (Spring Boot)
├── ledger-view/                # REST read server + Kafka consumer
├── ledger-benchmark/           # JMH performance benchmarks
├── ledger-loadgen/             # Load generator + test scenarios
│
├── docker-compose.yml          # Full stack deployment
├── Dockerfile                  # Multi-stage build (write server)
├── Dockerfile.view             # Build for view server
└── README.md                   # This file
```

---

## 🚀 Quick Start

### Prerequisites

- Java 21+
- Docker & Docker Compose
- Gradle 8.x (or use the included wrapper)

### 1. Build the Project

```bash
./gradlew clean build
```

### 2. Start Infrastructure + Cluster

```bash
docker compose up -d
```

This starts:
- **3 Raft nodes** (ledger-node-1, ledger-node-2, ledger-node-3)
- **1 Learner node** (streams to Kafka)
- **Kafka** (KRaft mode, no ZooKeeper)
- **PostgreSQL** (query database)
- **Ledger View** service (REST API)

### 3. Run the Load Generator

```bash
./gradlew :ledger-loadgen:run --args="--scenario=deposit --tps=1000 --duration=30s"
```

### 4. Query Results

```bash
# Check account balance via REST API
curl http://localhost:8080/api/v1/accounts/user001

# View Swagger UI
open http://localhost:8080/swagger-ui.html
```

For detailed deployment instructions, see [doc/deployment-guide.md](doc/deployment-guide.md).

---

## 🧪 Testing

```bash
# Run all unit + integration tests
./gradlew test

# Run JMH benchmarks
./gradlew :ledger-benchmark:jmh

# Run specific test scenarios (HA, correctness, performance)
# See doc/test-scenarios.md for full details
```

---

## 📖 Documentation

| Document | Description |
|----------|-------------|
| [Architecture](doc/architecture.md) | Deep-dive into system design, threading model, data flow |
| [API Specification](doc/api-spec.yml) | OpenAPI 3.0 spec for REST and gRPC endpoints |
| [Deployment Guide](doc/deployment-guide.md) | Step-by-step instructions to run the cluster |
| [Test Scenarios](doc/test-scenarios.md) | HA, correctness, performance validation plans |
| [Implementation Plan](doc/implementation-plan.md) | Technical plan with phased execution |

---

## 📜 Key Design Decisions

### Why Single-Threaded State Machine?
The "Hot Account Problem" in exchanges: a popular trading account receives thousands of concurrent deposits/withdrawals. With traditional RDBMS, each transaction locks the balance row, causing contention, deadlocks, and throughput collapse. A single-threaded state machine eliminates all locking — transactions are processed sequentially at memory speed (~millions/sec).

### Why LMAX Disruptor?
The Disruptor bridges multi-threaded network I/O (thousands of gRPC connections) with the single-threaded state machine. It uses a pre-allocated ring buffer with cache-line padding, achieving ~100ns inter-thread communication vs ~1μs for `BlockingQueue`.

### Why Raft (not Paxos or async replication)?
Raft provides strong consistency with understandable semantics: a transaction is committed only when a majority of nodes have persisted it. Apache Ratis handles leader election, log replication, and membership changes — we focus on the state machine.

### Why CQRS?
Write path (Raft domain) is optimized for throughput and consistency. Read path (PostgreSQL) is optimized for flexible queries, pagination, and analytics. They are decoupled via Kafka, allowing independent scaling.

---

## 📄 License

This project is for educational and demonstration purposes only.
