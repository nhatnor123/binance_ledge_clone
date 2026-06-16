package com.ledger.clone.core.disruptor;

import com.ledger.clone.core.statemachine.TransactionProcessor;
import com.ledger.clone.core.statemachine.TransactionResult;
import com.lmax.disruptor.EventHandler;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TransactionEventHandler implements EventHandler<TransactionEvent> {

    private final TransactionProcessor processor;

    @Override
    public void onEvent(TransactionEvent event, long sequence, boolean endOfBatch) {
        try {
            var result = processor.process(event.getTransaction());
            event.setResult(result);
            if (event.getFuture() != null) {
                event.getFuture().complete(result);
            }
        } catch (Exception e) {
            var errorResult = TransactionResult.failed(TransactionResult.Code.SYSTEM_ERROR,
                    "Unexpected error: " + e.getMessage());
            event.setResult(errorResult);
            if (event.getFuture() != null) {
                event.getFuture().complete(errorResult);
            }
        }
    }
}
