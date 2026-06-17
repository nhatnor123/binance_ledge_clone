package com.ledger.clone.core.persistence;

import com.ledger.clone.core.statemachine.InMemoryBalanceStore;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

@RequiredArgsConstructor
public class SnapshotManager {
    private final RocksDBStateStore stateStore;
    private final SnapshotBackend backend;

    public String takeSnapshot(long logIndex) throws IOException {
        stateStore.setLastAppliedIndex(logIndex);
        stateStore.flush();
        var checkpointDir = stateStore.createCheckpoint();
        var snapshotId = "snapshot-" + logIndex;
        backend.upload(checkpointDir, snapshotId);
        deleteRecursively(checkpointDir);
        return snapshotId;
    }

    public InMemoryBalanceStore restoreFromSnapshot(String snapshotId, Path targetDbPath) throws IOException {
        backend.download(snapshotId, targetDbPath);
        try (var restoredStore = new RocksDBStateStore(targetDbPath)) {
            return restoredStore.loadAllBalances();
        }
    }

    public List<String> listSnapshots() throws IOException {
        return backend.listSnapshots();
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (var walk = Files.walk(dir).sorted(Comparator.reverseOrder())) {
            for (var p : (Iterable<Path>) walk::iterator) {
                Files.deleteIfExists(p);
            }
        }
    }
}
