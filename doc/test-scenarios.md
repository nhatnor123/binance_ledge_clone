# Test Scenarios

This document outlines the test scenarios to validate the correctness, high availability (HA), and performance of the Binance Ledger Clone. These scenarios prove that the architecture solves the problems it claims to solve.

---

## Table of Contents

1. [High Availability (HA) Scenarios](#1-high-availability-ha-scenarios)
2. [Correctness Scenarios](#2-correctness-scenarios)
3. [Performance Scenarios](#3-performance-scenarios)
4. [Using the Load Generator](#4-using-the-load-generator)

---

## 1. High Availability (HA) Scenarios

The system uses a 3-node Raft cluster. It can tolerate the loss of $F$ nodes where $2F + 1 = N$. For a 3-node cluster, it can tolerate the loss of $1$ node without any downtime.

### Scenario HA-1: Follower Node Crash

**Objective**: Prove the cluster continues to process transactions when a minority of nodes fail.

1. **Start load**: Run a continuous background load generator (e.g., 100 TPS).
2. **Identify leader**: Check the cluster health API (`GET /api/v1/health/cluster`) to find the current leader.
3. **Kill a follower**: Use `docker kill ledger-node-X` on a follower node.
4. **Observe**:
   - The load generator should report 0 errors.
   - Throughput should remain stable.
5. **Recover**: Start the follower back up (`docker start ledger-node-X`).
6. **Verify**: The follower should reconnect, download missing log entries from the leader, and catch up to the current index.

### Scenario HA-2: Leader Node Crash (Failover)

**Objective**: Prove the cluster automatically elects a new leader and resumes processing when the leader fails.

1. **Start load**: Run the continuous load generator.
2. **Identify leader**: Find the current leader via the health API.
3. **Kill the leader**: Use `docker kill` on the leader node.
4. **Observe**:
   - The load generator will experience a brief pause and some gRPC timeouts (usually < 2 seconds) while the election happens.
   - The remaining 2 nodes will elect a new leader.
   - The load generator's gRPC client (with retries enabled) will route traffic to the new leader.
   - Transactions resume successfully.
5. **Recover**: Start the old leader. It will rejoin as a follower and sync the logs it missed.

### Scenario HA-3: Network Partition

**Objective**: Prove the system handles split-brain scenarios safely.

1. **Simulate partition**: Use `iptables` or Docker networking to isolate the leader from the other two nodes.
2. **Observe**:
   - The isolated leader will step down because it cannot reach a quorum.
   - The two connected followers will elect a new leader among themselves.
   - Writes to the isolated node will fail.
   - Writes to the new majority cluster will succeed.
3. **Heal partition**: Remove the network isolation.
4. **Verify**: The old leader rejoins the cluster as a follower, truncates any uncommitted logs, and syncs from the new leader.

---

## 2. Correctness Scenarios

Correctness proves that the ledger acts as a perfect source of truth, never losing or creating funds out of thin air.

### Scenario C-1: The Hot Account (Lock-Free Correctness)

**Objective**: Prove that the single-threaded state machine prevents race conditions without using locks, even under extreme concurrent load on a single account.

1. **Setup**: Create an account `EXCHANGE_FEE_POOL` with 0 balance.
2. **Execution**: Run the load generator's `HotAccount` scenario. This simulates 1,000 concurrent threads all sending `Transfer` requests to deposit into the `EXCHANGE_FEE_POOL` simultaneously.
3. **Verify**:
   - After 100,000 transactions of exactly 1.0 token, the balance of `EXCHANGE_FEE_POOL` must be *exactly* 100,000.0.
   - Not a single cent can be lost to race conditions (which would happen in a naive multi-threaded hash map).
   - In a traditional DB, this would cause massive lock contention. Here, it should process smoothly.

### Scenario C-2: Insufficient Funds Prevention

**Objective**: Ensure transactions fail deterministically when funds are insufficient.

1. **Setup**: Account `A` has 10.0 tokens.
2. **Execution**: Send a `Withdraw` request for 11.0 tokens.
3. **Verify**: The transaction must be rejected with status `FAILED` and an `INSUFFICIENT_FUNDS` error. The balance must remain exactly 10.0.

### Scenario C-3: CQRS Eventual Consistency and Reconciliation

**Objective**: Prove that the read domain (PostgreSQL) eventually matches the write domain (Raft state machine).

1. **Setup**: Run a burst of 10,000 transactions.
2. **Observe Lag**: While the burst is running, the PostgreSQL query API may lag slightly behind the gRPC write API.
3. **Wait**: Wait 2 seconds after the burst completes.
4. **Verify via Reconciliation**:
   - Call `GET /api/v1/reconciliation/status`
   - The daemon fetches all balances from the Raft in-memory state via gRPC.
   - It compares them to the SUMs in PostgreSQL.
   - The result must be `CONSISTENT` with 0 discrepancies.

### Scenario C-4: Recovery from Snapshot

**Objective**: Prove state is correctly restored from RocksDB after a full cluster restart.

1. **Setup**: Run load until the Raft log index is past the snapshot interval (e.g., index 5000). A snapshot should be triggered and saved to RocksDB.
2. **Execution**: Stop the entire cluster (`docker compose stop`).
3. **Restart**: Start the cluster (`docker compose start`).
4. **Verify**:
   - Nodes should load the snapshot from RocksDB.
   - Account balances should match the state exactly as it was before the shutdown.

---

## 3. Performance Scenarios

These scenarios use JMH (Java Microbenchmark Harness) and the load generator to measure throughput and latency.

### Scenario P-1: LMAX Disruptor Throughput (Microbenchmark)

**Objective**: Prove the Disruptor can handle millions of events per second, eliminating the queue bottleneck.

1. **Run**: `./gradlew :ledger-benchmark:jmh -Pinclude=".*DisruptorBenchmark.*"`
2. **Verify**: Compare the Disruptor throughput against a standard `ArrayBlockingQueue`. The Disruptor should consistently show > 5x higher throughput (~millions ops/sec).

### Scenario P-2: State Machine Throughput (Microbenchmark)

**Objective**: Measure the raw speed of the single-threaded state machine without network or consensus overhead.

1. **Run**: `./gradlew :ledger-benchmark:jmh -Pinclude=".*StateMachineBenchmark.*"`
2. **Verify**: A single thread applying `credit` and `debit` operations to the `ConcurrentHashMap` should achieve > 1,000,000 ops/sec.

### Scenario P-3: End-to-End Latency

**Objective**: Measure the P50, P95, and P99 latency of a full gRPC transaction (Network -> Disruptor -> Raft Consensus -> State Machine -> Network).

1. **Execution**: Run the load generator with a moderate, sustainable TPS (e.g., 500 TPS) for 5 minutes.
2. **Verify**:
   - **P50 Latency**: Should be low (dominated by network and Raft disk sync, typically < 10ms).
   - **P99 Latency**: Should remain stable, showing that the system doesn't suffer from GC pauses or lock contention spikes.

---

## 4. Using the Load Generator

The `ledger-loadgen` module is a CLI tool designed to execute these scenarios.

### Basic Usage

```bash
# Run a deposit scenario at 1000 TPS for 60 seconds
./gradlew :ledger-loadgen:run --args="--scenario=deposit --tps=1000 --duration=60s"

# Run the hot account scenario to prove lock-free correctness
./gradlew :ledger-loadgen:run --args="--scenario=hot-account --tps=5000 --duration=30s --target-account=EXCHANGE_FEE"
```

### Available Scenarios

- `deposit`: Simulates users depositing external funds into their accounts.
- `withdraw`: Simulates users withdrawing funds.
- `transfer`: Simulates random P2P transfers between user accounts.
- `hot-account`: Concentrates 100% of the traffic into a single target account.
