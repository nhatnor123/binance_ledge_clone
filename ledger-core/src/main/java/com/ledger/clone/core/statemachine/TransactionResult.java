package com.ledger.clone.core.statemachine;

import lombok.Getter;

import java.math.BigDecimal;

@Getter
public class TransactionResult {

    public enum Code {
        SUCCESS,
        INSUFFICIENT_FUNDS,
        INVALID_ACCOUNT,
        INVALID_AMOUNT,
        SYSTEM_ERROR
    }

    private final boolean success;
    private final Code code;
    private final String errorMessage;
    private final BigDecimal newBalance;

    private TransactionResult(boolean success, Code code, String errorMessage, BigDecimal newBalance) {
        this.success = success;
        this.code = code;
        this.errorMessage = errorMessage;
        this.newBalance = newBalance;
    }

    public static TransactionResult success(BigDecimal newBalance) {
        return new TransactionResult(true, Code.SUCCESS, null, newBalance);
    }

    public static TransactionResult failed(Code code, String errorMessage) {
        return new TransactionResult(false, code, errorMessage, null);
    }

    @Override
    public String toString() {
        return success
                ? "TransactionResult{SUCCESS, newBalance=" + newBalance + "}"
                : "TransactionResult{FAILED, code=" + code + ", message='" + errorMessage + "'}";
    }
}
