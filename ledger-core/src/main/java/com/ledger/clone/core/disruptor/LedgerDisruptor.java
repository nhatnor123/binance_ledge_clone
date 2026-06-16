package com.ledger.clone.core.disruptor;

import com.ledger.clone.common.model.Transaction;
import com.ledger.clone.core.statemachine.InMemoryBalanceStore;
import com.ledger.clone.core.statemachine.TransactionProcessor;
import com.ledger.clone.core.statemachine.TransactionResult;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import lombok.Getter;

import java.util.concurrent.CompletableFuture;

public class LedgerDisruptor implements AutoCloseable {

    private final Disruptor<TransactionEvent> disruptor;
    @Getter
    private final TransactionProcessor processor;
    @Getter
    private final LedgerDisruptorConfig config;

    public LedgerDisruptor(InMemoryBalanceStore balanceStore) {
        this(balanceStore, new LedgerDisruptorConfig());
    }

    public LedgerDisruptor(InMemoryBalanceStore balanceStore, LedgerDisruptorConfig config) {
        this.config = config;
        this.processor = new TransactionProcessor(balanceStore);
        this.disruptor = new Disruptor<>(
                TransactionEvent::new,
                config.getRingBufferSize(),
                DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI,
                config.toLmaxWaitStrategy()
        );
        this.disruptor.handleEventsWith(new TransactionEventHandler(processor));
        this.disruptor.start();
    }

    public TransactionResult publishTransaction(Transaction transaction) {
        var future = new CompletableFuture<TransactionResult>();
        disruptor.publishEvent(new TransactionEventTranslator(future), transaction);
        return future.join();
    }

    public void publishTransactionAsync(Transaction transaction, CompletableFuture<TransactionResult> future) {
        disruptor.publishEvent(new TransactionEventTranslator(future), transaction);
    }

    @Override
    public void close() {
        disruptor.shutdown();
    }
}
