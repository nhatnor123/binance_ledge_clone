package com.ledger.clone.core.persistence;

import com.ledger.clone.common.model.AccountKey;
import com.ledger.clone.core.statemachine.InMemoryBalanceStore;
import org.rocksdb.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class RocksDBStateStore implements AutoCloseable {
    static {
        RocksDB.loadLibrary();
    }

    private static final String BALANCES_CF = "balances";
    private static final String METADATA_CF = "metadata";
    private static final String LAST_APPLIED_INDEX = "last_applied_index";
    private static final String LAST_APPLIED_TERM = "last_applied_term";

    private final RocksDB db;
    private final ColumnFamilyHandle balancesCf;
    private final ColumnFamilyHandle metadataCf;
    private final Path dbPath;

    public RocksDBStateStore(Path dbPath) throws IOException {
        this.dbPath = dbPath;
        var options = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);
        var cfDescriptors = List.of(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
                new ColumnFamilyDescriptor(BALANCES_CF.getBytes(StandardCharsets.UTF_8)),
                new ColumnFamilyDescriptor(METADATA_CF.getBytes(StandardCharsets.UTF_8))
        );
        try {
            var handles = new ArrayList<ColumnFamilyHandle>();
            this.db = RocksDB.open(options, dbPath.toString(), cfDescriptors, handles);
            this.balancesCf = handles.get(1);
            this.metadataCf = handles.get(2);
            handles.getFirst().close();
        } catch (RocksDBException e) {
            throw new IOException("Failed to open RocksDB at " + dbPath, e);
        }
    }

    public BigDecimal getBalance(AccountKey key) throws IOException {
        var bytes = getRaw(balancesCf, key.toCompositeKey());
        return bytes != null ? new BigDecimal(new String(bytes, StandardCharsets.UTF_8)) : null;
    }

    public void putBalance(AccountKey key, BigDecimal balance) throws IOException {
        putRaw(balancesCf, key.toCompositeKey(), balance.toPlainString().getBytes(StandardCharsets.UTF_8));
    }

    public void deleteBalance(AccountKey key) throws IOException {
        deleteRaw(balancesCf, key.toCompositeKey());
    }

    public long getLastAppliedIndex() throws IOException {
        var bytes = getRaw(metadataCf, LAST_APPLIED_INDEX);
        return bytes != null ? Long.parseLong(new String(bytes, StandardCharsets.UTF_8)) : 0L;
    }

    public void setLastAppliedIndex(long index) throws IOException {
        putRaw(metadataCf, LAST_APPLIED_INDEX, Long.toString(index).getBytes(StandardCharsets.UTF_8));
    }

    public long getLastAppliedTerm() throws IOException {
        var bytes = getRaw(metadataCf, LAST_APPLIED_TERM);
        return bytes != null ? Long.parseLong(new String(bytes, StandardCharsets.UTF_8)) : 0L;
    }

    public void setLastAppliedTerm(long term) throws IOException {
        putRaw(metadataCf, LAST_APPLIED_TERM, Long.toString(term).getBytes(StandardCharsets.UTF_8));
    }

    public InMemoryBalanceStore loadAllBalances() throws IOException {
        var store = new InMemoryBalanceStore();
        forEachBalance(store::put);
        return store;
    }

    public void forEachBalance(BiConsumer<AccountKey, BigDecimal> consumer) {
        try (var iter = db.newIterator(balancesCf)) {
            iter.seekToFirst();
            while (iter.isValid()) {
                var key = AccountKey.fromCompositeKey(new String(iter.key(), StandardCharsets.UTF_8));
                var balance = new BigDecimal(new String(iter.value(), StandardCharsets.UTF_8));
                consumer.accept(key, balance);
                iter.next();
            }
        }
    }

    public Path createCheckpoint() throws IOException {
        try {
            var checkpointDir = dbPath.resolveSibling(
                    dbPath.getFileName() + "_checkpoint_" + System.currentTimeMillis());
            Checkpoint.create(db).createCheckpoint(checkpointDir.toString());
            return checkpointDir;
        } catch (RocksDBException e) {
            throw new IOException("Failed to create checkpoint", e);
        }
    }

    public void flush() throws IOException {
        try {
            db.flush(new FlushOptions().setWaitForFlush(true));
        } catch (RocksDBException e) {
            throw new IOException("Failed to flush RocksDB", e);
        }
    }

    @Override
    public void close() {
        try {
            balancesCf.close();
        } catch (Exception ignored) {
        }
        try {
            metadataCf.close();
        } catch (Exception ignored) {
        }
        try {
            db.close();
        } catch (Exception ignored) {
        }
    }

    private byte[] getRaw(ColumnFamilyHandle cf, String key) throws IOException {
        try {
            return db.get(cf, key.getBytes(StandardCharsets.UTF_8));
        } catch (RocksDBException e) {
            throw new IOException("Failed to get key: " + key, e);
        }
    }

    private void putRaw(ColumnFamilyHandle cf, String key, byte[] value) throws IOException {
        try {
            db.put(cf, key.getBytes(StandardCharsets.UTF_8), value);
        } catch (RocksDBException e) {
            throw new IOException("Failed to put key: " + key, e);
        }
    }

    private void deleteRaw(ColumnFamilyHandle cf, String key) throws IOException {
        try {
            db.delete(cf, key.getBytes(StandardCharsets.UTF_8));
        } catch (RocksDBException e) {
            throw new IOException("Failed to delete key: " + key, e);
        }
    }
}
