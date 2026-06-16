package com.ledger.clone.core.pipeline;

import com.ledger.clone.common.model.Transaction;
import com.ledger.clone.core.disruptor.LedgerDisruptor;
import com.ledger.clone.core.statemachine.InMemoryBalanceStore;
import com.ledger.clone.core.statemachine.TransactionProcessor;
import com.ledger.clone.core.statemachine.TransactionResult;

public class LedgerPipeline implements AutoCloseable {

    private final InMemoryBalanceStore balanceStore;
    private final TransactionProcessor processor;
    private final LedgerDisruptor disruptor;

    public LedgerPipeline() {
        this.balanceStore = new InMemoryBalanceStore();
        this.processor = new TransactionProcessor(balanceStore);
        this.disruptor = new LedgerDisruptor(balanceStore);
    }

    public LedgerPipeline(InMemoryBalanceStore balanceStore) {
        this.balanceStore = balanceStore;
        this.processor = new TransactionProcessor(balanceStore);
        this.disruptor = new LedgerDisruptor(balanceStore);
    }

    public TransactionResult submitTransaction(Transaction transaction) {
        return disruptor.publishTransaction(transaction);
    }

    public TransactionResult processDirect(Transaction transaction) {
        return processor.process(transaction);
    }

    public InMemoryBalanceStore getBalanceStore() { return balanceStore; }

    public TransactionProcessor getProcessor() { return processor; }

    public LedgerDisruptor getDisruptor() { return disruptor; }

    @Override
    public void close() {
        disruptor.close();
    }
}
