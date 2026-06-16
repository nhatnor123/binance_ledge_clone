package com.ledger.clone.core.statemachine;

import com.ledger.clone.common.model.AccountKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryBalanceStoreTest {

    private InMemoryBalanceStore store;
    private AccountKey key;

    @BeforeEach
    void setUp() {
        store = new InMemoryBalanceStore();
        key = AccountKey.of("user001", "BTC");
    }

    @Test
    void shouldReturnZeroForNonExistentAccount() {
        assertThat(store.getBalance(key)).isEqualByComparingTo(BigDecimal.ZERO);
    }

    @Test
    void shouldCreditBalance() {
        BigDecimal newBalance = store.credit(key, new BigDecimal("10.5"));
        assertThat(newBalance).isEqualByComparingTo("10.5");
        assertThat(store.getBalance(key)).isEqualByComparingTo("10.5");
    }

    @Test
    void shouldAccumulateCredits() {
        store.credit(key, new BigDecimal("10.0"));
        store.credit(key, new BigDecimal("5.5"));
        assertThat(store.getBalance(key)).isEqualByComparingTo("15.5");
    }

    @Test
    void shouldDebitBalance() {
        store.credit(key, new BigDecimal("100.0"));
        BigDecimal newBalance = store.debit(key, new BigDecimal("30.0"));
        assertThat(newBalance).isEqualByComparingTo("70.0");
    }

    @Test
    void shouldAllowNegativeBalanceOnDebit() {
        store.credit(key, new BigDecimal("10.0"));
        BigDecimal newBalance = store.debit(key, new BigDecimal("100.0"));
        assertThat(newBalance).isEqualByComparingTo("-90.0");
    }

    @Test
    void shouldCheckSufficientFunds() {
        store.credit(key, new BigDecimal("50.0"));
        assertThat(store.hasSufficientFunds(key, new BigDecimal("30.0"))).isTrue();
        assertThat(store.hasSufficientFunds(key, new BigDecimal("50.0"))).isTrue();
        assertThat(store.hasSufficientFunds(key, new BigDecimal("50.01"))).isFalse();
    }

    @Test
    void shouldReturnFalseForSufficientFundsOnEmptyAccount() {
        assertThat(store.hasSufficientFunds(key, new BigDecimal("1.0"))).isFalse();
    }

    @Test
    void shouldGetAllBalances() {
        AccountKey key1 = AccountKey.of("user001", "BTC");
        AccountKey key2 = AccountKey.of("user002", "ETH");
        store.credit(key1, new BigDecimal("1.5"));
        store.credit(key2, new BigDecimal("10.0"));

        Map<AccountKey, BigDecimal> all = store.getAllBalances();
        assertThat(all).hasSize(2);
        assertThat(all.get(key1)).isEqualByComparingTo("1.5");
        assertThat(all.get(key2)).isEqualByComparingTo("10.0");
    }

    @Test
    void shouldClearAllBalances() {
        store.credit(key, new BigDecimal("100.0"));
        store.clear();
        assertThat(store.getBalance(key)).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(store.size()).isZero();
    }

    @Test
    void shouldSupportMultipleAssetsForSameUser() {
        AccountKey btcKey = AccountKey.of("user001", "BTC");
        AccountKey ethKey = AccountKey.of("user001", "ETH");
        store.credit(btcKey, new BigDecimal("2.0"));
        store.credit(ethKey, new BigDecimal("5.0"));

        assertThat(store.getBalance(btcKey)).isEqualByComparingTo("2.0");
        assertThat(store.getBalance(ethKey)).isEqualByComparingTo("5.0");
    }
}
