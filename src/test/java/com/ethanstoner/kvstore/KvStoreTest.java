package com.ethanstoner.kvstore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KvStoreTest {

    @Test
    void putThenGet(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("user:42", "Ethan");
            assertEquals(Optional.of("Ethan"), store.get("user:42"));
        }
    }

    @Test
    void getMissingKeyIsEmpty(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            assertTrue(store.get("nope").isEmpty());
        }
    }

    @Test
    void deletedKeyReadsAsEmpty(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("k", "v");
            store.delete("k");
            assertTrue(store.get("k").isEmpty());
        }
    }

    @Test
    void scanReturnsSortedRangeWithoutTombstones(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("k1", "a");
            store.put("k2", "b");
            store.put("k3", "c");
            store.delete("k2");
            Map<String, String> rows = store.scan("k1", "k9");
            assertEquals(2, rows.size());
            assertEquals("a", rows.get("k1"));
            assertFalse(rows.containsKey("k2")); // deleted -> hidden
            assertEquals("c", rows.get("k3"));
        }
    }

    /**
     * The headline guarantee: data written before a "crash" (here, simply
     * closing the store) is rebuilt from the WAL when the store re-opens.
     */
    @Test
    void recoversAfterReopen(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("persist", "yes");
            store.put("temp", "1");
            store.delete("temp");
        } // simulate process exit / crash

        try (KvStore reopened = new KvStore(dir)) {
            assertEquals(Optional.of("yes"), reopened.get("persist"));
            assertTrue(reopened.get("temp").isEmpty()); // delete survived too
        }
    }

    @Test
    void flushWritesSStableWhenMemtableExceedsThreshold(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            String bigValue = "x".repeat(100);
            for (int i = 0; i < 20_000; i++) {
                store.put("key-" + String.format("%06d", i), bigValue);
            }
            long sstCount = java.nio.file.Files.list(dir)
                    .filter(p -> p.getFileName().toString().endsWith(".db"))
                    .count();
            assertTrue(sstCount >= 1, "Expected at least 1 SSTable, found " + sstCount);
        }
    }

    @Test
    void dataSurvivesFlushAndReopen(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            String bigValue = "x".repeat(100);
            for (int i = 0; i < 20_000; i++) {
                store.put("key-" + String.format("%06d", i), bigValue);
            }
        }
        try (KvStore reopened = new KvStore(dir)) {
            assertEquals(Optional.of("x".repeat(100)), reopened.get("key-000000"));
            assertEquals(Optional.of("x".repeat(100)), reopened.get("key-019999"));
        }
    }

    @Test
    void getReadsFromSStableAfterFlush(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("early-key", "early-value");
            String bigValue = "x".repeat(100);
            for (int i = 0; i < 20_000; i++) {
                store.put("filler-" + String.format("%06d", i), bigValue);
            }
            assertEquals(Optional.of("early-value"), store.get("early-key"));
        }
    }

    @Test
    void deleteInMemtableHidesSStableValue(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("victim", "alive");
            String bigValue = "x".repeat(100);
            for (int i = 0; i < 20_000; i++) {
                store.put("pad-" + String.format("%06d", i), bigValue);
            }
            store.delete("victim");
            assertTrue(store.get("victim").isEmpty());
        }
    }

    @Test
    void scanMergesMemtableAndSStables(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("a", "from-memtable");
            String bigValue = "x".repeat(100);
            for (int i = 0; i < 20_000; i++) {
                store.put("filler-" + String.format("%06d", i), bigValue);
            }
            store.put("b", "also-memtable");
            Map<String, String> result = store.scan("a", "c");
            assertEquals("from-memtable", result.get("a"));
            assertEquals("also-memtable", result.get("b"));
        }
    }

    @Test
    void newerValueOverridesOlderInScan(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("key", "old-value");
            String bigValue = "x".repeat(100);
            for (int i = 0; i < 20_000; i++) {
                store.put("filler-" + String.format("%06d", i), bigValue);
            }
            store.put("key", "new-value");
            assertEquals(Optional.of("new-value"), store.get("key"));
            Map<String, String> scan = store.scan("key", "key\0");
            assertEquals("new-value", scan.get("key"));
        }
    }
}
