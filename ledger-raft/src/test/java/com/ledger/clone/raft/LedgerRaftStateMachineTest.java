package com.ledger.clone.raft;

import com.ledger.clone.common.model.AccountKey;
import com.ledger.clone.common.proto.TransactionRequest;
import com.ledger.clone.common.proto.TransactionType;
import com.ledger.clone.raft.config.RaftConfig;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LedgerRaftStateMachineTest {

    private RaftConfig config;

    @BeforeEach
    void setUp() {
        config = new RaftConfig();
    }

    @Test
    void shouldInitializeWithEmptyState() {
        var stateMachine = new LedgerRaftStateMachine(config);
        assertThat(stateMachine.isRunning()).isFalse();
        assertThat(stateMachine.getBalanceStore()).isNotNull();
        assertThat(stateMachine.getBalanceStore().size()).isZero();
    }

    @Test
    void shouldApplyDepositTransaction() throws Exception {
        var stateMachine = new LedgerRaftStateMachine(config);
        stateMachine.getBalanceStore().put(AccountKey.of("user1", "BTC"), new BigDecimal("100"));

        var result = apply(stateMachine, depositProto("tx-001", "user1", "BTC", "100.50"), 1, 1);
        assertThat(result.getContent().toStringUtf8()).isEqualTo("OK");
        assertThat(stateMachine.getLastAppliedTermIndex().getIndex()).isEqualTo(1);

        stateMachine.shutdown();
    }

    @Test
    void shouldApplyWithdrawalTransaction() throws Exception {
        var stateMachine = new LedgerRaftStateMachine(config);
        stateMachine.getBalanceStore().put(AccountKey.of("user1", "BTC"), new BigDecimal("100"));

        var result = apply(stateMachine, withdrawalProto("tx-002", "user1", "BTC", "50"), 2, 1);
        assertThat(result.getContent().toStringUtf8()).isEqualTo("OK");
        assertThat(stateMachine.getLastAppliedTermIndex().getIndex()).isEqualTo(2);

        stateMachine.shutdown();
    }

    @Test
    void shouldApplyTransferTransaction() throws Exception {
        var stateMachine = new LedgerRaftStateMachine(config);
        stateMachine.getBalanceStore().put(AccountKey.of("alice", "BTC"), new BigDecimal("100"));

        var result = apply(stateMachine, transferProto("tx-003", "alice", "bob", "BTC", "30"), 3, 1);
        assertThat(result.getContent().toStringUtf8()).isEqualTo("OK");
        assertThat(stateMachine.getLastAppliedTermIndex().getIndex()).isEqualTo(3);

        stateMachine.shutdown();
    }

    @Test
    void shouldTrackTransactionIndex() throws Exception {
        var stateMachine = new LedgerRaftStateMachine(config);

        for (int i = 1; i <= 5; i++) {
            var result = apply(stateMachine, depositProto("tx-" + i, "user1", "BTC", "10"), i, 1);
            assertThat(result.getContent().toStringUtf8()).isEqualTo("OK");
            assertThat(stateMachine.getLastAppliedTermIndex().getIndex()).isEqualTo(i);
        }

        assertThat(stateMachine.getLastAppliedTermIndex().getIndex()).isEqualTo(5);
        stateMachine.shutdown();
    }

    @Test
    void shouldRejectInvalidAmount() throws Exception {
        var stateMachine = new LedgerRaftStateMachine(config);

        var result = apply(stateMachine, depositProto("tx-bad", "user1", "BTC", "-50"), 1, 1);
        assertThat(result.getContent().toStringUtf8()).isEqualTo("FAIL");
        assertThat(stateMachine.getLastAppliedTermIndex().getIndex()).isEqualTo(1);

        stateMachine.shutdown();
    }

    @Test
    void shouldRejectInsufficientFunds() throws Exception {
        var stateMachine = new LedgerRaftStateMachine(config);

        var result = apply(stateMachine, withdrawalProto("tx-nofunds", "user1", "BTC", "100"), 1, 1);
        assertThat(result.getContent().toStringUtf8()).isEqualTo("FAIL");

        stateMachine.shutdown();
    }

    private TransactionRequest depositProto(String requestId, String userId, String asset, String amount) {
        return TransactionRequest.newBuilder()
                .setRequestId(requestId)
                .setType(TransactionType.DEPOSIT)
                .setToUserId(userId)
                .setAsset(asset)
                .setAmount(amount)
                .build();
    }

    private TransactionRequest withdrawalProto(String requestId, String userId, String asset, String amount) {
        return TransactionRequest.newBuilder()
                .setRequestId(requestId)
                .setType(TransactionType.WITHDRAWAL)
                .setFromUserId(userId)
                .setAsset(asset)
                .setAmount(amount)
                .build();
    }

    private TransactionRequest transferProto(String requestId, String from, String to, String asset, String amount) {
        return TransactionRequest.newBuilder()
                .setRequestId(requestId)
                .setType(TransactionType.TRANSFER)
                .setFromUserId(from)
                .setToUserId(to)
                .setAsset(asset)
                .setAmount(amount)
                .build();
    }

    private Message apply(
            LedgerRaftStateMachine stateMachine,
            TransactionRequest proto,
            long logIndex,
            long term) throws ExecutionException, InterruptedException {

        var logEntry = LogEntryProto.newBuilder()
                .setIndex(logIndex)
                .setTerm(term)
                .setStateMachineLogEntry(
                        StateMachineLogEntryProto.newBuilder()
                                .setLogData(
                                        org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(
                                                proto.toByteArray()))
                                .build())
                .build();

        var trx = mock(TransactionContext.class);
        when(trx.getLogEntry()).thenReturn(logEntry);

        return stateMachine.applyTransaction(trx).get();
    }
}
