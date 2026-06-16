package com.ledger.clone.core.disruptor;

import com.ledger.clone.common.model.Transaction;
import com.ledger.clone.core.statemachine.TransactionResult;
import lombok.Data;

import java.util.concurrent.CompletableFuture;

@Data
public class TransactionEvent {

    Transaction transaction;
    TransactionResult result;
    CompletableFuture<TransactionResult> future;

    public void clear() {
        this.transaction = null;
        this.result = null;
        this.future = null;
    }
}
