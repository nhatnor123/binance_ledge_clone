package com.ledger.clone.common.config;

public final class LedgerConstants {

    private LedgerConstants() {
    }

    public static final String DEFAULT_ASSET = "BTC";
    public static final String KAFKA_BALANCE_TOPIC = "ledger-balance-changes";
    public static final String KAFKA_CONSUMER_GROUP = "ledger-view-consumer";

    public static final int ROCKSDB_DEFAULT_PORT = 10001;
    public static final String ROCKSDB_DEFAULT_PATH = "/data/rocksdb";
    public static final String SNAPSHOT_DEFAULT_PATH = "/data/backup";

    public static final int RING_BUFFER_DEFAULT_SIZE = 1024 * 1024;
    public static final String DISRUPTOR_DEFAULT_WAIT_STRATEGY = "YIELDING";
}
