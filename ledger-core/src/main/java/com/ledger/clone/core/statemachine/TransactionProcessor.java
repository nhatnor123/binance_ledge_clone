package com.ledger.clone.core.statemachine;

import com.ledger.clone.common.model.AccountKey;
import com.ledger.clone.common.model.Transaction;
import com.ledger.clone.common.model.TransactionStatus;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;

@RequiredArgsConstructor
public class TransactionProcessor {

    private final InMemoryBalanceStore balanceStore;

    public TransactionResult process(Transaction transaction) {
        if (transaction.getAmount() == null || transaction.getAmount().compareTo(BigDecimal.ZERO) <= 0) {
            transaction.setStatus(TransactionStatus.FAILED);
            transaction.setErrorMessage("Amount must be positive");
            return TransactionResult.failed(TransactionResult.Code.INVALID_AMOUNT, "Amount must be positive");
        }

        return switch (transaction.getType()) {
            case DEPOSIT -> processDeposit(transaction);
            case WITHDRAWAL -> processWithdrawal(transaction);
            case TRANSFER -> processTransfer(transaction);
        };
    }

    private TransactionResult processDeposit(Transaction tx) {
        var newBalance = balanceStore.credit(AccountKey.of(tx.getToUserId(), tx.getAsset()), tx.getAmount());
        tx.setStatus(TransactionStatus.SUCCESS);
        return TransactionResult.success(newBalance);
    }

    private TransactionResult processWithdrawal(Transaction tx) {
        var key = AccountKey.of(tx.getFromUserId(), tx.getAsset());

        if (!balanceStore.hasSufficientFunds(key, tx.getAmount())) {
            tx.setStatus(TransactionStatus.FAILED);
            tx.setErrorMessage("Insufficient funds");
            return TransactionResult.failed(TransactionResult.Code.INSUFFICIENT_FUNDS,
                    "Insufficient funds for account " + tx.getFromUserId());
        }

        var newBalance = balanceStore.debit(key, tx.getAmount());
        tx.setStatus(TransactionStatus.SUCCESS);
        return TransactionResult.success(newBalance);
    }

    private TransactionResult processTransfer(Transaction tx) {
        var fromKey = AccountKey.of(tx.getFromUserId(), tx.getAsset());
        var toKey = AccountKey.of(tx.getToUserId(), tx.getAsset());

        if (!balanceStore.hasSufficientFunds(fromKey, tx.getAmount())) {
            tx.setStatus(TransactionStatus.FAILED);
            tx.setErrorMessage("Insufficient funds");
            return TransactionResult.failed(TransactionResult.Code.INSUFFICIENT_FUNDS,
                    "Insufficient funds for transfer from " + tx.getFromUserId());
        }

        balanceStore.debit(fromKey, tx.getAmount());
        var newBalance = balanceStore.credit(toKey, tx.getAmount());
        tx.setStatus(TransactionStatus.SUCCESS);
        return TransactionResult.success(newBalance);
    }
}
