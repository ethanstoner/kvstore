package com.ethanstoner.kvstore;

/**
 * A value plus its absolute expiration time in epoch milliseconds.
 * {@code Long.MAX_VALUE} means "never expires".
 */
public record ValueEntry(String value, long expiresAtMs) {
    public static final long NEVER = Long.MAX_VALUE;

    public boolean isExpired(long nowMs) {
        return nowMs >= expiresAtMs;
    }

    public boolean hasTtl() {
        return expiresAtMs != NEVER;
    }
}
