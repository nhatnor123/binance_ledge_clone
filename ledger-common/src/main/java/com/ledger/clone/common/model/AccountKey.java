package com.ledger.clone.common.model;

public record AccountKey(String userId, String asset) {

    public static AccountKey of(String userId, String asset) {
        return new AccountKey(userId, asset);
    }

    public String toCompositeKey() {
        return userId + ":" + asset;
    }

    public static AccountKey fromCompositeKey(String compositeKey) {
        String[] parts = compositeKey.split(":", 2);
        return new AccountKey(parts[0], parts[1]);
    }
}
