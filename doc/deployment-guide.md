# Deployment Guide

Step-by-step instructions to build, configure, and run the Binance Ledger Clone cluster.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Build from Source](#2-build-from-source)
3. [Configuration Reference](#3-configuration-reference)
4. [Docker Compose Deployment (Recommended)](#4-docker-compose-deployment-recommended)
5. [Manual Local Deployment](#5-manual-local-deployment)
6. [Verifying the Deployment](#6-verifying-the-deployment)
7. [Scaling the Cluster](#7-scaling-the-cluster)
8. [Stopping & Cleanup](#8-stopping--cleanup)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Java | 21+ | Runtime |
| Gradle | 8.x | Build (or use the included `./gradlew` wrapper) |
| Docker | 24+ | Containerization |
| Docker Compose | 2.20+ | Multi-container orchestration |
| `grpcurl` (optional) | latest | CLI tool for testing gRPC endpoints |

Verify your setup:

```bash
java -version     # Should show 21+
docker --version  # Should show 24+
docker compose version
```

---

## 2. Build from Source

```bash
# Clone the repository
git clone <repo-url> binance_ledge_clone
cd binance_ledge_clone

# Build all modules (skip tests for faster build)
./gradlew clean build -x test

# Build Docker images
docker compose build
```

To run tests:

```bash
# Unit + Integration tests
./gradlew test

# JMH benchmarks only
./gradlew :ledger-benchmark:jmh
```

---

## 3. Configuration Reference

### 3.1 Raft Node Configuration

All Raft parameters are configurable via environment variables or `application.yml`.

| Parameter | Env Variable | Default | Description |
|-----------|-------------|---------|-------------|
| Node ID | `RAFT_NODE_ID` | `node1` | Unique identifier for this node |
| Bind address | `RAFT_ADDRESS` | `0.0.0.0` | Network interface to bind |
| Raft port | `RAFT_PORT` | `10001` | Port for Raft communication |
| gRPC port | `GRPC_PORT` | `9090` | Port for client gRPC requests |
| Peers | `RAFT_PEERS` | `node1:10001,node2:10002,node3:10003` | Comma-separated list of all voting peers |
| Learners | `RAFT_LEARNERS` | (empty) | Comma-separated list of learner nodes |
| Storage directory | `RAFT_STORAGE_DIR` | `/data/raft` | Directory for Raft log storage |
| Snapshot interval (entries) | `RAFT_SNAPSHOT_INTERVAL` | `1000` | Take snapshot every N log entries |
| Snapshot interval (seconds) | `RAFT_SNAPSHOT_INTERVAL_SECONDS` | `3600` | Take snapshot every X seconds (0 to disable) |
| Election timeout | `RAFT_ELECTION_TIMEOUT` | `5000` | Leader election timeout in milliseconds |

### 3.2 Disruptor Configuration

| Parameter | Env Variable | Default | Description |
|-----------|-------------|---------|-------------|
| Ring buffer size | `DISRUPTOR_RING_BUFFER_SIZE` | `1048576` | Must be power of 2 |
| Wait strategy | `DISRUPTOR_WAIT_STRATEGY` | `YIELDING` | `BUSY_SPIN`, `YIELDING`, or `SLEEPING` |

### 3.3 View Service Configuration

| Parameter | Env Variable | Default | Description |
|-----------|-------------|---------|-------------|
| Server port | `SERVER_PORT` | `8080` | REST API port |
| DB URL | `SPRING_DATASOURCE_URL` | `jdbc:postgresql://localhost:5432/ledger_view` | PostgreSQL connection |
| DB user | `SPRING_DATASOURCE_USERNAME` | `ledger` | Database username |
| DB password | `SPRING_DATASOURCE_PASSWORD` | `ledger` | Database password |
| Kafka servers | `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers |
| Reconciliation interval | `RECONCILIATION_INTERVAL_SECONDS` | `60` | How often to run reconciliation |

### 3.4 RocksDB Configuration

| Parameter | Env Variable | Default | Description |
|-----------|-------------|---------|-------------|
| DB path | `ROCKSDB_PATH` | `/data/rocksdb` | RocksDB data directory |
| Backup path | `SNAPSHOT_BACKUP_PATH` | `/data/backup` | Snapshot backup directory |

---

## 4. Docker Compose Deployment (Recommended)

### 4.1 Start the Full Stack

```bash
# Start everything in detached mode
docker compose up -d

# Follow logs
docker compose logs -f
```

This starts:
- **kafka** — Event streaming (using KRaft mode, no ZooKeeper)
- **postgres** — Read domain database
- **ledger-node-1, ledger-node-2, ledger-node-3** — Raft cluster (3 voting nodes)
- **ledger-view** — REST query API

### 4.2 Verify Everything is Running

```bash
# Check container status
docker compose ps

# Expected output:
# NAME              STATUS    PORTS
# kafka             Up        9092
# postgres          Up        5432
# ledger-node-1     Up        9090, 10001
# ledger-node-2     Up        9091, 10002
# ledger-node-3     Up        9092, 10003
# ledger-view       Up        8080
```

### 4.3 Access Points

| Service | URL/Address | Protocol |
|---------|------------|----------|
| Write API (Node 1) | `localhost:9090` | gRPC |
| Write API (Node 2) | `localhost:9091` | gRPC |
| Write API (Node 3) | `localhost:9092` | gRPC |
| Read API | `http://localhost:8080` | REST |
| Swagger UI | `http://localhost:8080/swagger-ui.html` | HTTP |
| OpenAPI Spec | `http://localhost:8080/v3/api-docs` | HTTP |

---

## 5. Manual Local Deployment

For development or debugging, you can run services individually.

### 5.1 Start Infrastructure

```bash
# Start only Kafka + PostgreSQL
docker compose up -d kafka postgres
```

### 5.2 Start Raft Nodes (3 Terminals)

**Terminal 1 — Node 1:**
```bash
RAFT_NODE_ID=node1 RAFT_PORT=10001 GRPC_PORT=9090 \
RAFT_PEERS=node1:localhost:10001,node2:localhost:10002,node3:localhost:10003 \
./gradlew :ledger-server:bootRun
```

**Terminal 2 — Node 2:**
```bash
RAFT_NODE_ID=node2 RAFT_PORT=10002 GRPC_PORT=9091 \
RAFT_PEERS=node1:localhost:10001,node2:localhost:10002,node3:localhost:10003 \
./gradlew :ledger-server:bootRun
```

**Terminal 3 — Node 3:**
```bash
RAFT_NODE_ID=node3 RAFT_PORT=10003 GRPC_PORT=9092 \
RAFT_PEERS=node1:localhost:10001,node2:localhost:10002,node3:localhost:10003 \
./gradlew :ledger-server:bootRun
```

### 5.3 Start View Service

**Terminal 4:**
```bash
SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/ledger_view \
SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
./gradlew :ledger-view:bootRun
```

---

## 6. Verifying the Deployment

### 6.1 Check Cluster Health

```bash
curl http://localhost:8080/api/v1/health/cluster | jq
```

Expected: `clusterStatus: HEALTHY` with a leader and active followers.

### 6.2 Submit a Test Transaction (via gRPC)

```bash
# Using grpcurl
grpcurl -plaintext -d '{
  "userId": "user001",
  "asset": "BTC",
  "amount": "1.5"
}' localhost:9090 ledger.LedgerService/Deposit
```

### 6.3 Query Balance (via REST)

```bash
curl http://localhost:8080/api/v1/accounts/user001 | jq
```

### 6.4 Run Load Generator

```bash
./gradlew :ledger-loadgen:run --args="--scenario=deposit --tps=1000 --duration=30s --target=localhost:9090"
```

---

## 7. Scaling the Cluster

### 7.1 Adding More Raft Nodes

To scale from 3 to 5 nodes, update `docker-compose.yml`:

```yaml
  ledger-node-4:
    build: .
    environment:
      RAFT_NODE_ID: node4
      RAFT_PORT: 10004
      RAFT_PEERS: node1:10001,node2:10002,node3:10003,node4:10004,node5:10005

  ledger-node-5:
    build: .
    environment:
      RAFT_NODE_ID: node5
      RAFT_PORT: 10005
      RAFT_PEERS: node1:10001,node2:10002,node3:10003,node4:10004,node5:10005
```

> **Important**: Update `RAFT_PEERS` on ALL existing nodes to include the new nodes, then rolling-restart the cluster.

### 7.2 Adding Learner Nodes

Learner nodes are non-voting replicas that stream data to Kafka:

```yaml
  ledger-learner-1:
    build: .
    environment:
      RAFT_NODE_ID: learner1
      RAFT_PORT: 10010
      RAFT_ROLE: LEARNER
      RAFT_PEERS: node1:10001,node2:10002,node3:10003
```

---

## 8. Stopping & Cleanup

```bash
# Stop all containers
docker compose down

# Stop and remove volumes (WARNING: deletes all data)
docker compose down -v

# Remove Docker images
docker compose down --rmi all
```

---

## 9. Troubleshooting

### Leader Election Not Happening

```bash
# Check Raft node logs
docker compose logs ledger-node-1 | grep -i "election\|leader"

# Common causes:
# - Nodes can't reach each other (check RAFT_PEERS hostnames)
# - Election timeout too short (increase RAFT_ELECTION_TIMEOUT)
# - Fewer than 3 nodes running (need majority for quorum)
```

### Kafka Consumer Lag Growing

```bash
# Check consumer lag
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group ledger-view-consumer \
  --describe
```

### PostgreSQL Connection Issues

```bash
# Check database connectivity
docker compose exec postgres psql -U ledger -d ledger_view -c "SELECT 1"

# Check if tables exist
docker compose exec postgres psql -U ledger -d ledger_view -c "\dt"
```

### RocksDB Errors

```bash
# Check disk space in container
docker compose exec ledger-node-1 df -h /data

# Check RocksDB directory
docker compose exec ledger-node-1 ls -la /data/rocksdb/
```
