package com.ledger.clone.raft;

import com.ledger.clone.common.model.TransactionType;
import com.ledger.clone.common.proto.TransactionRequest;
import com.ledger.clone.core.statemachine.InMemoryBalanceStore;
import com.ledger.clone.core.statemachine.TransactionProcessor;
import com.ledger.clone.raft.config.RaftConfig;
import lombok.Getter;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;

public class LedgerRaftStateMachine extends BaseStateMachine {

    private static final Logger log = LoggerFactory.getLogger(LedgerRaftStateMachine.class);

    private final RaftConfig config;
    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
    private final Object lock = new Object();

    @Getter
    private InMemoryBalanceStore balanceStore;
    private TransactionProcessor processor;
    @Getter
    private volatile boolean running;

    public LedgerRaftStateMachine(RaftConfig config) {
        this.config = config;
        this.balanceStore = new InMemoryBalanceStore();
        this.processor = new TransactionProcessor(balanceStore);
    }

    @Override
    public void initialize(RaftServer server, org.apache.ratis.protocol.RaftGroupId groupId,
                           RaftStorage raftStorage) throws IOException {
        super.initialize(server, groupId, raftStorage);
        storage.init(raftStorage);
        if (balanceStore == null) {
            this.balanceStore = new InMemoryBalanceStore();
            this.processor = new TransactionProcessor(balanceStore);
        }
        running = true;
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        var logEntry = trx.getLogEntry();
        var logData = logEntry.getStateMachineLogEntry().getLogData();
        byte[] rawBytes = logData.toByteArray();

        TransactionRequest proto;
        try {
            proto = TransactionRequest.parseFrom(rawBytes);
        } catch (Exception e) {
            log.error("Failed to parse transaction: {}", e.getMessage());
            return CompletableFuture.completedFuture(
                    Message.valueOf("ERROR: Invalid transaction data"));
        }

        synchronized (lock) {
            var result = applyToStateMachine(proto);
            updateLastAppliedTermIndex(logEntry.getTerm(), logEntry.getIndex());
            return CompletableFuture.completedFuture(
                    Message.valueOf(result ? "OK" : "FAIL"));
        }
    }

    private boolean applyToStateMachine(TransactionRequest proto) {
        try {
            var transaction = com.ledger.clone.common.model.Transaction.builder()
                    .type(switch (proto.getType()) {
                        case DEPOSIT -> TransactionType.DEPOSIT;
                        case WITHDRAWAL -> TransactionType.WITHDRAWAL;
                        case TRANSFER -> TransactionType.TRANSFER;
                        default -> throw new IllegalArgumentException("Unknown type: " + proto.getType());
                    })
                    .fromUserId(proto.getFromUserId().isEmpty() ? null : proto.getFromUserId())
                    .toUserId(proto.getToUserId().isEmpty() ? null : proto.getToUserId())
                    .asset(proto.getAsset())
                    .amount(new BigDecimal(proto.getAmount()))
                    .build();

            var txResult = processor.process(transaction);
            log.debug("Applied transaction {}: {}", proto.getRequestId(), txResult.isSuccess());
            return txResult.isSuccess();
        } catch (Exception e) {
            log.error("Failed to apply transaction: {}", e.getMessage());
            return false;
        }
    }

    public void shutdown() {
        running = false;
    }
}
