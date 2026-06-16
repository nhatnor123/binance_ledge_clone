package com.ledger.clone.core.statemachine;

import com.ledger.clone.common.model.AccountKey;
import com.ledger.clone.common.model.Transaction;
import com.ledger.clone.common.model.TransactionType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class TransactionProcessorTest {

    private InMemoryBalanceStore balanceStore;
    private TransactionProcessor processor;

    @BeforeEach
    void setUp() {
        balanceStore = new InMemoryBalanceStore();
        processor = new TransactionProcessor(balanceStore);
    }

    @Test
    void shouldProcessDeposit() {
        var tx = Transaction.builder().type(TransactionType.DEPOSIT).toUserId("user001").asset("BTC").amount(new BigDecimal("10.0")).build();
        var result = processor.process(tx);
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getNewBalance()).isEqualByComparingTo("10.0");
        assertThat(balanceStore.getBalance(AccountKey.of("user001", "BTC"))).isEqualByComparingTo("10.0");
    }

    @Test
    void shouldProcessWithdrawal() {
        balanceStore.credit(AccountKey.of("user001", "BTC"), new BigDecimal("100.0"));
        var tx = Transaction.builder().type(TransactionType.WITHDRAWAL).fromUserId("user001").asset("BTC").amount(new BigDecimal("30.0")).build();
        var result = processor.process(tx);
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getNewBalance()).isEqualByComparingTo("70.0");
    }

    @Test
    void shouldRejectInsufficientFundsWithdrawal() {
        var tx = Transaction.builder().type(TransactionType.WITHDRAWAL).fromUserId("user001").asset("BTC").amount(new BigDecimal("10.0")).build();
        var result = processor.process(tx);
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getCode()).isEqualTo(TransactionResult.Code.INSUFFICIENT_FUNDS);
        assertThat(balanceStore.getBalance(AccountKey.of("user001", "BTC"))).isEqualByComparingTo(BigDecimal.ZERO);
    }

    @Test
    void shouldProcessTransfer() {
        balanceStore.credit(AccountKey.of("alice", "BTC"), new BigDecimal("50.0"));
        var tx = Transaction.builder().type(TransactionType.TRANSFER).fromUserId("alice").toUserId("bob").asset("BTC").amount(new BigDecimal("20.0")).build();
        var result = processor.process(tx);
        assertThat(result.isSuccess()).isTrue();
        assertThat(balanceStore.getBalance(AccountKey.of("alice", "BTC"))).isEqualByComparingTo("30.0");
        assertThat(balanceStore.getBalance(AccountKey.of("bob", "BTC"))).isEqualByComparingTo("20.0");
    }

    @Test
    void shouldRejectTransferWithInsufficientFunds() {
        balanceStore.credit(AccountKey.of("alice", "BTC"), new BigDecimal("5.0"));
        var tx = Transaction.builder().type(TransactionType.TRANSFER).fromUserId("alice").toUserId("bob").asset("BTC").amount(new BigDecimal("20.0")).build();
        var result = processor.process(tx);
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getCode()).isEqualTo(TransactionResult.Code.INSUFFICIENT_FUNDS);
        assertThat(balanceStore.getBalance(AccountKey.of("alice", "BTC"))).isEqualByComparingTo("5.0");
        assertThat(balanceStore.getBalance(AccountKey.of("bob", "BTC"))).isEqualByComparingTo(BigDecimal.ZERO);
    }

    @Test
    void shouldRejectTransactionWithNegativeAmount() {
        var tx = Transaction.builder().type(TransactionType.DEPOSIT).toUserId("user001").asset("BTC").amount(new BigDecimal("-10.0")).build();
        var result = processor.process(tx);
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getCode()).isEqualTo(TransactionResult.Code.INVALID_AMOUNT);
    }

    @Test
    void shouldRejectTransactionWithZeroAmount() {
        var tx = Transaction.builder().type(TransactionType.DEPOSIT).toUserId("user001").asset("BTC").amount(BigDecimal.ZERO).build();
        var result = processor.process(tx);
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getCode()).isEqualTo(TransactionResult.Code.INVALID_AMOUNT);
    }

    @Test
    void shouldHandleMultipleConsecutiveTransactions() {
        var tx1 = Transaction.builder().type(TransactionType.DEPOSIT).toUserId("user001").asset("USDT").amount(new BigDecimal("1000.0")).build();
        assertThat(processor.process(tx1).isSuccess()).isTrue();

        var tx2 = Transaction.builder().type(TransactionType.WITHDRAWAL).fromUserId("user001").asset("USDT").amount(new BigDecimal("200.0")).build();
        assertThat(processor.process(tx2).isSuccess()).isTrue();

        var tx3 = Transaction.builder().type(TransactionType.TRANSFER).fromUserId("user001").toUserId("user002").asset("USDT").amount(new BigDecimal("300.0")).build();
        assertThat(processor.process(tx3).isSuccess()).isTrue();

        assertThat(balanceStore.getBalance(AccountKey.of("user001", "USDT"))).isEqualByComparingTo("500.0");
        assertThat(balanceStore.getBalance(AccountKey.of("user002", "USDT"))).isEqualByComparingTo("300.0");
    }

    @Test
    void shouldHandlePrecisionCorrectly() {
        var tx = Transaction.builder().type(TransactionType.DEPOSIT).toUserId("user001").asset("BTC").amount(new BigDecimal("0.00000001")).build();
        var result = processor.process(tx);
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getNewBalance()).isEqualByComparingTo("0.00000001");
    }
}
