package com.ledger.clone.raft;

import com.ledger.clone.raft.config.RaftConfig;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class RaftNodeRunner implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(RaftNodeRunner.class);

    private static final String CLUSTER_ID = "ledger-cluster";

    private final RaftConfig config;
    private final LedgerRaftStateMachine stateMachine;
    private RaftServer server;

    public RaftNodeRunner(RaftConfig config) {
        this.config = config;
        this.stateMachine = new LedgerRaftStateMachine(config);
    }

    public void start() throws IOException {
        var properties = createRaftProperties();
        var peerId = RaftPeerId.valueOf(config.getNodeId());
        var peers = buildPeers();
        var groupId = RaftGroupId.valueOf(
                UUID.nameUUIDFromBytes(CLUSTER_ID.getBytes()));
        var group = RaftGroup.valueOf(groupId, peers);

        var builder = RaftServer.newBuilder()
                .setServerId(peerId)
                .setStateMachine(stateMachine)
                .setProperties(properties)
                .setGroup(group);

        this.server = builder.build();
        server.start();

        log.info("Raft node {} started on {}:{} (group={}, role={})",
                config.getNodeId(), config.getAddress(), config.getPort(),
                groupId, config.isLearner() ? "LEARNER" : "VOTER");
    }

    public void awaitReady() throws InterruptedException {
        var deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            var state = server.getLifeCycleState();
            if (state == LifeCycle.State.RUNNING) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }
        throw new InterruptedException("Timed out waiting for Raft server to be ready");
    }

    public LedgerRaftStateMachine getStateMachine() {
        return stateMachine;
    }

    @Override
    public void close() throws IOException {
        if (server != null) {
            stateMachine.shutdown();
            server.close();
        }
    }

    private RaftProperties createRaftProperties() {
        var props = new RaftProperties();

        props.set("raft.server.rpc.port", String.valueOf(config.getPort()));
        props.set("raft.server.storage.dir", config.getStorageDir());

        return props;
    }

    private RaftPeer[] buildPeers() {
        return config.getPeers().stream()
                .map(this::parsePeer)
                .toArray(RaftPeer[]::new);
    }

    private RaftPeer parsePeer(String peerStr) {
        var parts = peerStr.split(":");
        var id = parts[0];
        var host = parts.length >= 3 ? parts[1] : id;
        var port = parts.length >= 3 ? Integer.parseInt(parts[2]) : Integer.parseInt(parts[1]);
        var address = host + ":" + port;
        return RaftPeer.newBuilder()
                .setId(RaftPeerId.valueOf(id))
                .setAddress(address)
                .build();
    }
}
