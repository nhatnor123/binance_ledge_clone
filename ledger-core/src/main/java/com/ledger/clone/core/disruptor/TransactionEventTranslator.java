package com.ledger.clone.core.disruptor;

import com.ledger.clone.common.model.Transaction;
import com.ledger.clone.core.statemachine.TransactionResult;
import com.lmax.disruptor.EventTranslatorOneArg;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
public class TransactionEventTranslator implements EventTranslatorOneArg<TransactionEvent, Transaction> {

    private final CompletableFuture<TransactionResult> future;

    @Override
    public void translateTo(TransactionEvent event, long sequence, Transaction transaction) {
        event.setTransaction(transaction);
        event.setResult(null);
        event.setFuture(future);
    }
}
