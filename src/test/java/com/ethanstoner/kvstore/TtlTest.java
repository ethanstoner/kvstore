package com.ethanstoner.kvstore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class TtlTest {

    @Test
    void putWithExpiryReadsBackWhileFresh(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            long expiry = System.currentTimeMillis() + 60_000;  // 60s from now
            store.put("k", "v", expiry);
            assertEquals(Optional.of("v"), store.get("k"));
        }
    }

    @Test
    void expiredKeyIsInvisible(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            long pastExpiry = System.currentTimeMillis() - 1;
            store.put("k", "v", pastExpiry);
            assertTrue(store.get("k").isEmpty());
        }
    }

    @Test
    void ttlReturnsRemainingTime(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("k", "v", System.currentTimeMillis() + 5_000);
            long remaining = store.ttl("k");
            assertTrue(remaining > 0 && remaining <= 5_000);
        }
    }

    @Test
    void ttlReturnsMinusOneForNoExpiry(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("k", "v");
            assertEquals(-1, store.ttl("k"));
        }
    }

    @Test
    void ttlReturnsMinusTwoForMissingKey(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            assertEquals(-2, store.ttl("missing"));
        }
    }

    @Test
    void expireOnExistingKeySucceeds(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("k", "v");
            assertTrue(store.expire("k", System.currentTimeMillis() + 10_000));
            assertTrue(store.ttl("k") > 0);
        }
    }

    @Test
    void expireOnMissingKeyFails(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            assertFalse(store.expire("missing", System.currentTimeMillis() + 10_000));
        }
    }

    @Test
    void persistRemovesTtl(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("k", "v", System.currentTimeMillis() + 10_000);
            assertTrue(store.persist("k"));
            assertEquals(-1, store.ttl("k"));
        }
    }

    @Test
    void persistOnKeyWithoutTtlReturnsFalse(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("k", "v");
            assertFalse(store.persist("k"));
        }
    }

    @Test
    void ttlSurvivesRestart(@TempDir Path dir) throws IOException {
        long expiry = System.currentTimeMillis() + 60_000;
        try (KvStore store = new KvStore(dir)) {
            store.put("k", "v", expiry);
        }
        try (KvStore store = new KvStore(dir)) {
            long remaining = store.ttl("k");
            assertTrue(remaining > 0 && remaining <= 60_000);
        }
    }

    @Test
    void expiredKeyInvisibleAfterFlushToSsTable(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            // Put a key with a past expiry, then flush it to SSTable
            long pastExpiry = System.currentTimeMillis() - 1;
            store.put("expired", "v", pastExpiry);
            // Force flush by writing a lot of data
            String bigValue = "x".repeat(100);
            for (int i = 0; i < 20_000; i++) {
                store.put("filler-" + String.format("%06d", i), bigValue);
            }
            // The expired key should still be invisible even from the SSTable
            assertTrue(store.get("expired").isEmpty());
        }
    }

    @Test
    void scanExcludesExpiredKeys(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("a", "alive");
            store.put("b", "will-expire", System.currentTimeMillis() - 1);
            store.put("c", "also-alive");
            var result = store.scan("a", "d");
            assertTrue(result.containsKey("a"));
            assertFalse(result.containsKey("b"), "expired key should be excluded from scan");
            assertTrue(result.containsKey("c"));
        }
    }
}
