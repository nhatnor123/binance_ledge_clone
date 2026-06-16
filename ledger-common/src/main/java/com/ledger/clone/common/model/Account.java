package com.ledger.clone.common.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@EqualsAndHashCode(of = {"userId", "asset"})
public class Account {
    String userId;
    String asset;
    BigDecimal balance;
    Instant updatedAt;

    public Account(String userId, String asset, BigDecimal balance) {
        this.userId = userId;
        this.asset = asset;
        this.balance = balance;
        this.updatedAt = Instant.now();
    }
}
