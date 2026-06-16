# Architecture Overview

This document provides a deep-dive into the Binance Ledger Clone architecture, covering every layer from network ingress to persistent storage, and the rationale behind each design choice.

---

## Table of Contents

1. [System Context](#1-system-context)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Write Domain (Raft Domain)](#3-write-domain-raft-domain)
4. [Read Domain (View Domain)](#4-read-domain-view-domain)
5. [Threading Model](#5-threading-model)
6. [Data Flow — Transaction Lifecycle](#6-data-flow--transaction-lifecycle)
7. [Raft Consensus Deep-Dive](#7-raft-consensus-deep-dive)
8. [State Persistence & Recovery](#8-state-persistence--recovery)
9. [CQRS & Event Streaming](#9-cqrs--event-streaming)
10. [Snapshot & Backup Strategy](#10-snapshot--backup-strategy)
11. [Failure Scenarios & Recovery](#11-failure-scenarios--recovery)

---

## 1. System Context

```
┌───────────────┐       gRPC        ┌────────────────────────────┐
│  Trading      │ ──────────────►   │                            │
│  Engine       │                   │   Binance Ledger Clone     │
│               │ ◄──────────────   │   (This System)            │
└───────────────┘                   │                            │
                                    │  ┌──────────────────────┐  │
┌───────────────┐       gRPC        │  │  Write Domain (Raft) │  │
│  Deposit      │ ──────────────►   │  │  - Balance Updates   │  │
│  Service      │                   │  │  - Transaction Log   │  │
└───────────────┘                   │  └──────────────────────┘  │
                                    │                            │
┌───────────────┐       REST        │  ┌──────────────────────┐  │
│  User Portal  │ ──────────────►   │  │  Read Domain (View)  │  │
│  (Query)      │                   │  │  - Balance Queries   │  │
└───────────────┘                   │  │  - TransactionHistory│  │
                                    │  └──────────────────────┘  │
                                    └────────────────────────────┘
```

The ledger is the **source of truth** for all account balances. Upstream services (trading engine, deposit/withdrawal processors) submit transactions via gRPC. Downstream consumers (user portals, analytics) query via REST.

---

## 2. High-Level Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│                         WRITE DOMAIN (Raft Domain)                     │
│                                                                        │
│  ┌──────────┐    ┌────────────┐    ┌────────────┐    ┌──────────────┐  │
│  │  gRPC    │    │   LMAX     │    │   Ledger   │    │    Raft      │  │
│  │  Server  │───►│  Disruptor │───►│   State    │───►│   Layer      │  │
│  │          │    │  (Request) │    │  Machine   │    │  (Ratis)     │  │
│  │ Virtual  │    │            │    │  (Single   │    │              │  │
│  │ Threads  │◄───│  Disruptor │◄───│  Platform  │◄───│  Committed   │  │
│  │          │    │  (Response)│    │  Thread)   │    │  Entries     │  │
│  └──────────┘    └────────────┘    └─────┬──────┘    └──────┬───────┘  │
│                                          │                  │          │
│                                    ┌─────▼──────┐    ┌──────▼───────┐  │
│                                    │  RocksDB   │    │  Followers   │  │
│                                    │  (State)   │    │  + Learners  │  │
│                                    └────────────┘    └──────────────┘  │
└────────────────────────────────────────────────────────────────────────┘

                              │ Learner streams to Kafka
                              ▼

┌─────────────────────────────────────────────────────────────────────────┐
│                         READ DOMAIN (View Domain)                       │
│                                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────────┐   │
│  │   Kafka      │    │  PostgreSQL  │    │  REST API                │   │
│  │   Consumer   │───►│  (accounts,  │◄───│  (Spring Boot + OpenAPI) │   │
│  │              │    │  transactions│    │                          │   │
│  └──────────────┘    └──────────────┘    └──────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Write Domain (Raft Domain)

### 3.1 Network Layer (gRPC Server)

- **Protocol**: gRPC with Protobuf serialization
- **Threading**: Java 21 virtual threads — each incoming RPC is handled by a virtual thread, allowing millions of concurrent connections without thread pool exhaustion
- **Operations**: `Deposit`, `Withdraw`, `Transfer`, `GetBalance`

### 3.2 LMAX Disruptor (Request/Response Ring Buffers)

The Disruptor bridges the **multi-threaded** gRPC layer with the **single-threaded** state machine.

```
  gRPC Thread 1 ──┐
  gRPC Thread 2 ──┤     ┌────────────────────────────┐
  gRPC Thread 3 ──┼────►│     Ring Buffer (Request)  │────► Single Ledger Thread
  gRPC Thread N ──┘     │  [0] [1] [2] ... [1023]    │         (Consumer)
                        └────────────────────────────┘

  Single Thread ──────► ┌────────────────────────────┐ ────► gRPC Thread (wakes up
                        │     Ring Buffer (Response) │       via CompletableFuture)
                        └────────────────────────────┘
```

**Why not a simple `BlockingQueue`?**

| Aspect | `BlockingQueue` | LMAX Disruptor |
|--------|----------------|----------------|
| Allocation | Allocates per-event | Pre-allocated ring buffer |
| Locking | `ReentrantLock` + `Condition` | Lock-free CAS sequences |
| Cache behavior | Poor (queue nodes scattered in heap) | Excellent (contiguous array, cache-line padding) |
| Latency | ~1μs per operation | ~100ns per operation |
| GC pressure | High (object allocation + deallocation) | Near-zero (object reuse) |

**Configuration** (via `application.yml`):
```yaml
ledger:
  disruptor:
    ring-buffer-size: 1048576   # 2^20, must be power of 2
    wait-strategy: YIELDING     # BUSY_SPIN | YIELDING | SLEEPING
```

### 3.3 Ledger State Machine

The **single-threaded state machine** is the core innovation from the Binance article. It eliminates the "Hot Account Problem":

```
Traditional RDBMS (Hot Account):
  Thread 1: BEGIN; UPDATE balance WHERE user=X → LOCK ROW → WAIT
  Thread 2: BEGIN; UPDATE balance WHERE user=X → LOCK ROW → WAIT
  Thread 3: BEGIN; UPDATE balance WHERE user=X → LOCK ROW → WAIT
  Result: Contention, deadlocks, throughput collapse

Our State Machine:
  Single Thread: process(tx1) → process(tx2) → process(tx3)
  Result: No locks, no contention, memory-speed sequential processing
```

**Data structure**: `ConcurrentHashMap<AccountKey, BigDecimal>`
- `AccountKey` = `(userId, asset)` composite key
- `ConcurrentHashMap` is used (despite single-threaded writes) to allow safe concurrent reads for balance queries and snapshot creation
- All mutations happen on a single platform thread

### 3.4 Raft Layer (Apache Ratis)

See [Section 7](#7-raft-consensus-deep-dive) for the Raft deep-dive.

---

## 4. Read Domain (View Domain)

The read domain is a **completely separate** Spring Boot application implementing CQRS.

### 4.1 Kafka Consumer

- Consumes `ledger-balance-changes` topic
- Each Kafka message = one committed transaction from the Raft log
- **Idempotent**: uses `tx_id` as deduplication key (PostgreSQL `ON CONFLICT DO NOTHING`)
- Consumer group ensures exactly-once processing

### 4.2 PostgreSQL

Stores denormalized query-optimized data:
- `accounts` table: current balance per (user, asset) pair
- `transactions` table: full transaction history with indexes

### 4.3 REST API

- Spring Boot 3.2+ with virtual threads enabled
- SpringDoc OpenAPI auto-generates Swagger UI
- Endpoints for balance queries, transaction history, cluster health

### 4.4 Reconciliation Daemon

- Runs every 60 seconds (configurable)
- Calls `GetBalance` gRPC on the write domain
- Compares against PostgreSQL balances
- Logs any discrepancies with severity levels

---

## 5. Threading Model

```
┌──────────────────────────────────────────────────────────────────┐
│  Java 21 Threading Model                                         │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │  VIRTUAL THREADS (I/O-bound, high concurrency)              │ │
│  │  ✅ gRPC server request handlers                            │ │
│  │  ✅ Kafka consumer polling                                  │ │
│  │  ✅ REST API request handlers                               │ │
│  │  ✅ Load generator client threads                           │ │
│  │  ✅ Reconciliation daemon                                   │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │  PLATFORM THREADS (CPU-bound, latency-critical, JNI/sync)   │ │
│  │  ❌ LMAX Disruptor consumer (busy-spin hot loop)            │ │
│  │  ❌ State Machine single thread (sequential processing)     │ │
│  │  ❌ Raft internal threads (Ratis uses synchronized blocks)  │ │
│  │  ❌ RocksDB I/O (JNI calls pin carrier threads)             │ │
│  └─────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

**Rationale**: JEP 444 (Virtual Threads) warns that virtual threads pinned during `synchronized` blocks or JNI calls lose their scalability advantage. The Disruptor, Ratis, and RocksDB all trigger pinning, so platform threads are mandatory for those components.

---

## 6. Data Flow — Transaction Lifecycle

### 6.1 Write Path (Deposit Example)

```
Step 1: Client sends Deposit(userId=U1, asset=BTC, amount=1.5) via gRPC
    │
    ▼
Step 2: gRPC virtual thread receives request, creates TransactionEvent
    │
    ▼
Step 3: Publishes event to Disruptor Request Ring Buffer
        (producer: multi-threaded, lock-free CAS on sequence counter)
    │
    ▼
Step 4: Disruptor consumer (single platform thread) picks up event
    │
    ▼
Step 5: Submits to Raft leader via RatisClient.send()
        (serialized as Protobuf bytes in Raft log entry)
    │
    ▼
Step 6: Raft leader writes to local log, replicates to followers
        Waits for majority ack (2 of 3 nodes)
    │
    ▼
Step 7: Entry committed → applyTransaction() called on state machine
        InMemoryBalanceStore.credit(U1, BTC, 1.5)
        Balance: 0.0 → 1.5
    │
    ▼
Step 8: Result published to Disruptor Response Ring Buffer
    │
    ▼
Step 9: gRPC virtual thread wakes up (CompletableFuture completes)
        Returns TransactionResponse(success=true, newBalance=1.5)
```

### 6.2 Read Path (Balance Query)

```
Step 1: REST client sends GET /api/v1/accounts/U1/assets/BTC
    │
    ▼
Step 2: QueryController queries PostgreSQL
        SELECT balance FROM accounts WHERE user_id='U1' AND asset='BTC'
    │
    ▼
Step 3: Returns { "userId": "U1", "asset": "BTC", "balance": "1.5" }

Note: Read path has eventual consistency — there may be a small delay
      (typically < 100ms) between write commit and PostgreSQL update.
```

---

## 7. Raft Consensus Deep-Dive

### 7.1 Cluster Topology

```
                    ┌─────────────┐
                    │   Leader    │  ◄── Handles all writes
                    │   (Node 1)  │      Replicates to followers
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │ Follower │ │ Follower │ │ Learner  │
        │ (Node 2) │ │ (Node 3) │ │ (Node 4) │
        └──────────┘ └──────────┘ └────┬─────┘
                                       │
                                       ▼
                                    Kafka
```

- **Leader**: Processes all write requests, replicates log entries to followers
- **Followers**: Receive and persist log entries, participate in majority voting
- **Learner**: Non-voting member that receives log entries and streams them to Kafka for the read domain

### 7.2 Configuration (Externalized)

All parameters are configurable via environment variables:

| Parameter | Env Var | Default | Description |
|-----------|---------|---------|-------------|
| Node ID | `RAFT_NODE_ID` | `node1` | Unique identifier for this node |
| Address | `RAFT_ADDRESS` | `0.0.0.0` | Bind address |
| Port | `RAFT_PORT` | `10001` | Raft communication port |
| Peers | `RAFT_PEERS` | `node1:10001,node2:10002,node3:10003` | Comma-separated peer list |
| Learners | `RAFT_LEARNERS` | (empty) | Comma-separated learner list |
| Snapshot interval | `RAFT_SNAPSHOT_INTERVAL` | `1000` | Take snapshot every N entries |
| Election timeout | `RAFT_ELECTION_TIMEOUT` | `5000` | Leader election timeout (ms) |
| Storage dir | `RAFT_STORAGE_DIR` | `/data/raft` | Raft log storage directory |

### 7.3 Consistency Guarantees

- **Linearizable writes**: Every committed transaction is applied in the same order on all nodes
- **Majority quorum**: A transaction is committed only when `⌊N/2⌋ + 1` nodes have persisted it
- **Leader lease**: Reads on the leader can be served from in-memory state without an additional Raft round-trip (since the leader knows it has the latest state)

---

## 8. State Persistence & Recovery

### 8.1 RocksDB State Store

RocksDB serves as the durable state for the in-memory balance store:

```
┌────────────────────────────────┐
│          RocksDB               │
│                                │
│  Column Family: "balances"     │
│  Key: (userId + asset)         │
│  Value: BigDecimal bytes       │
│                                │
│  Column Family: "metadata"     │
│  Key: "lastAppliedIndex"       │
│  Value: Raft log index (long)  │
│                                │
│  Key: "lastAppliedTerm"        │
│  Value: Raft term (long)       │
└────────────────────────────────┘
```

### 8.2 Recovery Process

```
Node starts up
    │
    ├── RocksDB exists?
    │   ├── YES: Load balances + metadata from RocksDB
    │   │        Replay Raft log from lastAppliedIndex+1
    │   │        Resume normal operation
    │   │
    │   └── NO: Backup exists?
    │       ├── YES: Restore RocksDB from backup
    │       │        Replay Raft log
    │       │
    │       └── NO: Start with empty state
    │                Raft InstallSnapshot from leader
    │                (leader sends full state to new/recovered node)
```

---

## 9. CQRS & Event Streaming

### 9.1 Write Domain → Read Domain Flow

```
Raft Commit ──► Learner Node ──► Kafka Topic ──► Consumer ──► PostgreSQL
                                    │
                        "ledger-balance-changes"
                                    │
                        Message Format:
                        {
                          "txId": "tx-001",
                          "type": "DEPOSIT",
                          "userId": "U1",
                          "asset": "BTC",
                          "amount": "1.5",
                          "newBalance": "10.5",
                          "raftLogIndex": 42,
                          "timestamp": "2024-01-01T00:00:00Z"
                        }
```

### 9.2 Consistency Model

| Domain | Consistency | Latency | Use Case |
|--------|------------|---------|----------|
| Write (Raft) | Strong (linearizable) | < 10ms | Transaction processing |
| Read (PostgreSQL) | Eventual (< 100ms lag) | < 5ms | Balance queries, history |

---

## 10. Snapshot & Backup Strategy

### 10.1 Current Implementation (Local Filesystem)

```
SnapshotManager
    │
    ├── takeSnapshot(logIndex)
    │   └── RocksDB.checkpoint() → /data/snapshots/snapshot-{logIndex}/
    │
    ├── backupSnapshot(logIndex)
    │   └── Copy to /data/backup/snapshot-{logIndex}/
    │
    └── restoreFromBackup(logIndex)
        └── Copy from /data/backup/ → RocksDB data dir
```

### 10.2 TODO: S3 Snapshot Backend

The `SnapshotManager` uses a pluggable `SnapshotBackend` interface:

```java
public interface SnapshotBackend {
    void upload(Path snapshotDir, String snapshotId);
    void download(String snapshotId, Path targetDir);
    List<String> listSnapshots();
}

// Current implementation
public class LocalSnapshotBackend implements SnapshotBackend { ... }

// Future implementation
public class S3SnapshotBackend implements SnapshotBackend { ... }  // TODO
```

---

## 11. Failure Scenarios & Recovery

| Scenario | Behavior | Recovery |
|----------|----------|----------|
| **Follower crash** | Leader continues with remaining majority | Node restarts, catches up via Raft log replay |
| **Leader crash** | Followers detect timeout, elect new leader | Old leader restarts as follower, catches up |
| **All nodes crash** | System unavailable | Nodes restart, load from RocksDB + replay remaining log |
| **Kafka unavailable** | Read domain stale, write domain unaffected | Consumer resumes from last committed offset |
| **PostgreSQL down** | Read queries fail, write domain unaffected | Restart PostgreSQL, consumer replays from Kafka |
| **Network partition** | Minority side cannot commit, majority continues | Partition heals, minority catches up |

For detailed test procedures for each scenario, see [test-scenarios.md](test-scenarios.md).
