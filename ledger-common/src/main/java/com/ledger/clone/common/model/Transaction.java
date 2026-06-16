package com.ledger.clone.common.model;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Data
@EqualsAndHashCode(of = "txId")
@Builder
public class Transaction {
    @Builder.Default
    String txId = UUID.randomUUID().toString();
    TransactionType type;
    String fromUserId;
    String toUserId;
    String asset;
    BigDecimal amount;
    @Builder.Default
    TransactionStatus status = TransactionStatus.SUCCESS;
    String errorMessage;
    long raftLogIndex;
    @Builder.Default
    Instant createdAt = Instant.now();
}
