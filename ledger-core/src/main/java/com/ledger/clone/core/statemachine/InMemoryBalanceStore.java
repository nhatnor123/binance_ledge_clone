package com.ledger.clone.core.statemachine;

import com.ledger.clone.common.model.AccountKey;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryBalanceStore {

    private final Map<AccountKey, BigDecimal> balances = new ConcurrentHashMap<>();

    public BigDecimal getBalance(AccountKey key) {
        return balances.getOrDefault(key, BigDecimal.ZERO);
    }

    public BigDecimal credit(AccountKey key, BigDecimal amount) {
        return balances.merge(key, amount, BigDecimal::add);
    }

    public BigDecimal debit(AccountKey key, BigDecimal amount) {
        return balances.computeIfPresent(key, (k, v) -> v.subtract(amount));
    }

    public boolean hasSufficientFunds(AccountKey key, BigDecimal amount) {
        return getBalance(key).compareTo(amount) >= 0;
    }

    public Map<AccountKey, BigDecimal> getAllBalances() {
        return Map.copyOf(balances);
    }

    public void put(AccountKey key, BigDecimal balance) {
        balances.put(key, balance);
    }

    public void clear() {
        balances.clear();
    }

    public int size() {
        return balances.size();
    }
}
