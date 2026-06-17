package com.ledger.clone.raft.config;

import lombok.Getter;

import java.util.List;

@Getter
public class RaftConfig {

    private final String nodeId;
    private final String address;
    private final int port;
    private final List<String> peers;
    private final List<String> learners;
    private final String storageDir;
    private final int snapshotInterval;
    private final int snapshotIntervalSeconds;
    private final int electionTimeoutMs;
    private final String role;

    public RaftConfig() {
        this.nodeId = getEnv("RAFT_NODE_ID", "node1");
        this.address = getEnv("RAFT_ADDRESS", "0.0.0.0");
        this.port = Integer.parseInt(getEnv("RAFT_PORT", "10001"));
        this.peers = parseList(getEnv("RAFT_PEERS", "node1:localhost:10001,node2:localhost:10002,node3:localhost:10003"));
        this.learners = parseList(getEnv("RAFT_LEARNERS", ""));
        this.storageDir = getEnv("RAFT_STORAGE_DIR", "/data/raft");
        this.snapshotInterval = Integer.parseInt(getEnv("RAFT_SNAPSHOT_INTERVAL", "1000"));
        this.snapshotIntervalSeconds = Integer.parseInt(getEnv("RAFT_SNAPSHOT_INTERVAL_SECONDS", "3600"));
        this.electionTimeoutMs = Integer.parseInt(getEnv("RAFT_ELECTION_TIMEOUT", "5000"));
        this.role = getEnv("RAFT_ROLE", "VOTER");
    }

    public boolean isLearner() {
        return "LEARNER".equalsIgnoreCase(role);
    }

    private static String getEnv(String key, String defaultValue) {
        var value = System.getenv(key);
        return value != null ? value : defaultValue;
    }

    private static List<String> parseList(String value) {
        if (value == null || value.isBlank()) {
            return List.of();
        }
        return List.of(value.split(","));
    }
}
