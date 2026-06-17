package com.ledger.clone.core.persistence;

import com.ledger.clone.common.model.AccountKey;
import com.ledger.clone.core.statemachine.InMemoryBalanceStore;
import com.ledger.clone.core.statemachine.TransactionProcessor;
import com.ledger.clone.common.model.Transaction;
import com.ledger.clone.common.model.TransactionType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RocksDBStateStoreTest {

    @BeforeAll
    static void initRocksDB() {
        RocksDBStateStore.class.getName(); // trigger static init
    }

    @Test
    void shouldStoreAndRetrieveBalance(@TempDir Path tempDir) throws Exception {
        try (var store = new RocksDBStateStore(tempDir.resolve("rocksdb"))) {
            var key = AccountKey.of("user1", "BTC");
            store.putBalance(key, new BigDecimal("100.50"));
            var balance = store.getBalance(key);
            assertThat(balance).isEqualByComparingTo(new BigDecimal("100.50"));
        }
    }

    @Test
    void shouldReturnNullForMissingKey(@TempDir Path tempDir) throws Exception {
        try (var store = new RocksDBStateStore(tempDir.resolve("rocksdb"))) {
            var balance = store.getBalance(AccountKey.of("nonexistent", "BTC"));
            assertThat(balance).isNull();
        }
    }

    @Test
    void shouldDeleteBalance(@TempDir Path tempDir) throws Exception {
        try (var store = new RocksDBStateStore(tempDir.resolve("rocksdb"))) {
            var key = AccountKey.of("user1", "ETH");
            store.putBalance(key, new BigDecimal("50"));
            store.deleteBalance(key);
            assertThat(store.getBalance(key)).isNull();
        }
    }

    @Test
    void shouldLoadAllBalancesIntoMemory(@TempDir Path tempDir) throws Exception {
        try (var store = new RocksDBStateStore(tempDir.resolve("rocksdb"))) {
            store.putBalance(AccountKey.of("user1", "BTC"), new BigDecimal("1.5"));
            store.putBalance(AccountKey.of("user1", "ETH"), new BigDecimal("25"));
            store.putBalance(AccountKey.of("user2", "BTC"), new BigDecimal("10"));
        }
        try (var store = new RocksDBStateStore(tempDir.resolve("rocksdb"))) {
            var memoryStore = store.loadAllBalances();
            assertThat(memoryStore.size()).isEqualTo(3);
            assertThat(memoryStore.getBalance(AccountKey.of("user1", "BTC")))
                    .isEqualByComparingTo("1.5");
            assertThat(memoryStore.getBalance(AccountKey.of("user2", "BTC")))
                    .isEqualByComparingTo("10");
        }
    }

    @Test
    void shouldPersistAndRecoverMetadata(@TempDir Path tempDir) throws Exception {
        try (var store = new RocksDBStateStore(tempDir.resolve("rocksdb"))) {
            store.setLastAppliedIndex(42L);
            store.setLastAppliedTerm(7L);
        }
        try (var store = new RocksDBStateStore(tempDir.resolve("rocksdb"))) {
            assertThat(store.getLastAppliedIndex()).isEqualTo(42L);
            assertThat(store.getLastAppliedTerm()).isEqualTo(7L);
        }
    }

    @Test
    void shouldReturnZeroForMissingMetadata(@TempDir Path tempDir) throws Exception {
        try (var store = new RocksDBStateStore(tempDir.resolve("rocksdb"))) {
            assertThat(store.getLastAppliedIndex()).isZero();
            assertThat(store.getLastAppliedTerm()).isZero();
        }
    }

    @Test
    void shouldCreateCheckpoint(@TempDir Path tempDir) throws Exception {
        try (var store = new RocksDBStateStore(tempDir.resolve("rocksdb"))) {
            store.putBalance(AccountKey.of("user1", "BTC"), new BigDecimal("99.99"));
            store.flush();
            var checkpointPath = store.createCheckpoint();
            assertThat(checkpointPath).exists();
            assertThat(checkpointPath.resolve("CURRENT")).exists();
        }
    }

    @Test
    void shouldRestoreBalancesAfterReopen(@TempDir Path tempDir) throws Exception {
        var dbPath = tempDir.resolve("rocksdb");
        try (var store = new RocksDBStateStore(dbPath)) {
            store.putBalance(AccountKey.of("alice", "BTC"), new BigDecimal("5.0"));
            store.putBalance(AccountKey.of("bob", "ETH"), new BigDecimal("10.0"));
        }
        // reopen and verify state survived
        try (var store = new RocksDBStateStore(dbPath)) {
            assertThat(store.getBalance(AccountKey.of("alice", "BTC")))
                    .isEqualByComparingTo("5.0");
            assertThat(store.getBalance(AccountKey.of("bob", "ETH")))
                    .isEqualByComparingTo("10.0");
            assertThat(store.getBalance(AccountKey.of("alice", "ETH"))).isNull();
        }
    }

    @Test
    void stateMachineToRocksDBRoundTrip(@TempDir Path tempDir) throws Exception {
        var dbPath = tempDir.resolve("rocksdb");
        var stateStore = new InMemoryBalanceStore();
        var processor = new TransactionProcessor(stateStore);

        stateStore.put(AccountKey.of("alice", "BTC"), new BigDecimal("100"));
        stateStore.put(AccountKey.of("bob", "BTC"), new BigDecimal("50"));

        var tx = Transaction.builder()
                .type(TransactionType.TRANSFER)
                .fromUserId("alice")
                .toUserId("bob")
                .asset("BTC")
                .amount(new BigDecimal("30"))
                .build();
        processor.process(tx);

        // write to RocksDB
        try (var rocksStore = new RocksDBStateStore(dbPath)) {
            for (var entry : stateStore.getAllBalances().entrySet()) {
                rocksStore.putBalance(entry.getKey(), entry.getValue());
            }
        }

        // reload and verify
        try (var rocksStore = new RocksDBStateStore(dbPath)) {
            var reloaded = rocksStore.loadAllBalances();
            assertThat(reloaded.getBalance(AccountKey.of("alice", "BTC")))
                    .isEqualByComparingTo("70");
            assertThat(reloaded.getBalance(AccountKey.of("bob", "BTC")))
                    .isEqualByComparingTo("80");
        }
    }

    @Test
    void shouldTakeAndRestoreSnapshot(@TempDir Path tempDir) throws Exception {
        var dbPath = tempDir.resolve("rocksdb");
        var backupPath = tempDir.resolve("backup");

        try (var store = new RocksDBStateStore(dbPath)) {
            store.putBalance(AccountKey.of("user1", "BTC"), new BigDecimal("1000"));
            store.putBalance(AccountKey.of("user2", "USDT"), new BigDecimal("5000"));
            store.setLastAppliedIndex(99L);
        }

        var snapshotId = "snapshot-99";
        try (var store = new RocksDBStateStore(dbPath)) {
            var backup = new LocalSnapshotBackend(backupPath);
            var snapManager = new SnapshotManager(store, backup);
            assertThat(snapManager.takeSnapshot(99L)).isEqualTo(snapshotId);
            assertThat(snapManager.listSnapshots()).contains(snapshotId);
        }

        var restorePath = tempDir.resolve("restored");
        try (var store = new RocksDBStateStore(dbPath)) {
            var backup = new LocalSnapshotBackend(backupPath);
            var snapManager = new SnapshotManager(store, backup);
            var restoredStore = snapManager.restoreFromSnapshot(snapshotId, restorePath);
            assertThat(restoredStore.getBalance(AccountKey.of("user1", "BTC")))
                    .isEqualByComparingTo("1000");
            assertThat(restoredStore.getBalance(AccountKey.of("user2", "USDT")))
                    .isEqualByComparingTo("5000");
            assertThat(restoredStore.size()).isEqualTo(2);
        }

        try (var reopened = new RocksDBStateStore(restorePath)) {
            assertThat(reopened.getLastAppliedIndex()).isEqualTo(99L);
        }
    }

    @Test
    void forEachBalanceVisitsAllEntries(@TempDir Path tempDir) throws Exception {
        try (var store = new RocksDBStateStore(tempDir.resolve("rocksdb"))) {
            store.putBalance(AccountKey.of("a", "BTC"), new BigDecimal("1"));
            store.putBalance(AccountKey.of("b", "ETH"), new BigDecimal("2"));
            store.putBalance(AccountKey.of("c", "USDT"), new BigDecimal("3"));
            var count = new int[]{0};
            store.forEachBalance((k, v) -> count[0]++);
            assertThat(count[0]).isEqualTo(3);
        }
    }

    @Test
    void shouldHandleLargeBalances(@TempDir Path tempDir) throws Exception {
        try (var store = new RocksDBStateStore(tempDir.resolve("rocksdb"))) {
            var large = new BigDecimal("999999999999999999.999999999999999999");
            store.putBalance(AccountKey.of("whale", "BTC"), large);
        }
        try (var store = new RocksDBStateStore(tempDir.resolve("rocksdb"))) {
            var loaded = store.getBalance(AccountKey.of("whale", "BTC"));
            assertThat(loaded).isEqualByComparingTo("999999999999999999.999999999999999999");
        }
    }
}
