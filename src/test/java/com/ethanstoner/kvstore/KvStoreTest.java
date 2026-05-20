package com.ethanstoner.kvstore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    @Test
    void compactionMergesSSTables(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            String bigValue = "x".repeat(100);
            for (int i = 0; i < 80_000; i++) {
                store.put("key-" + String.format("%06d", i), bigValue);
            }
            long l0Before = java.nio.file.Files.list(dir)
                    .filter(p -> p.getFileName().toString().endsWith(".db"))
                    .filter(p -> SSTable.levelOf(p) == 0)
                    .count();
            assertTrue(l0Before >= 4, "Expected >=4 L0 SSTables before compaction, got " + l0Before);

            // With leveled compaction a single call promotes all of L0 into L1.
            // Run up to 3 passes so any remaining L0 files are also compacted.
            store.compactNow();
            store.compactNow();
            store.compactNow();

            long l0After = java.nio.file.Files.list(dir)
                    .filter(p -> p.getFileName().toString().endsWith(".db"))
                    .filter(p -> SSTable.levelOf(p) == 0)
                    .count();
            assertTrue(l0After < l0Before,
                    "Expected fewer L0 SSTables after compaction: before=" + l0Before + " after=" + l0After);

            assertEquals(Optional.of(bigValue), store.get("key-000000"));
            assertEquals(Optional.of(bigValue), store.get("key-079999"));
        }
    }

    @Test
    void compactionDropsTombstones(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("doomed", "value");
            String bigValue = "x".repeat(100);
            for (int i = 0; i < 20_000; i++) {
                store.put("pad1-" + String.format("%06d", i), bigValue);
            }
            store.delete("doomed");
            for (int i = 0; i < 20_000; i++) {
                store.put("pad2-" + String.format("%06d", i), bigValue);
            }

            // With leveled compaction a tombstone may need several passes to
            // reach the deepest level (where it is safe to drop).
            for (int i = 0; i < 5; i++) {
                store.compactNow();
            }

            assertTrue(store.get("doomed").isEmpty());
        }
    }

    @Test
    void deleteIfPresentReturnsTrueWhenKeyExists(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("k", "v");
            assertTrue(store.deleteIfPresent("k"));
            assertTrue(store.get("k").isEmpty());
        }
    }

    @Test
    void deleteIfPresentReturnsFalseWhenKeyMissing(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            assertFalse(store.deleteIfPresent("nope"));
        }
    }

    @Test
    void deleteIfPresentReturnsFalseForAlreadyDeletedKey(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("k", "v");
            store.delete("k");
            assertFalse(store.deleteIfPresent("k"));
        }
    }

    @Test
    void incrementByCreatesKeyIfAbsent(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            assertEquals(5L, store.incrementBy("counter", 5));
            assertEquals(Optional.of("5"), store.get("counter"));
        }
    }

    @Test
    void incrementByExistingIntegerWorks(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("counter", "10");
            assertEquals(13L, store.incrementBy("counter", 3));
            assertEquals(Optional.of("13"), store.get("counter"));
        }
    }

    @Test
    void incrementByNegativeDecrements(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("counter", "100");
            assertEquals(95L, store.incrementBy("counter", -5));
        }
    }

    @Test
    void incrementByNonIntegerThrows(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("name", "alice");
            assertThrows(NumberFormatException.class,
                    () -> store.incrementBy("name", 1));
        }
    }

    @Test
    void incrementByOverflowThrows(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("counter", Long.toString(Long.MAX_VALUE));
            assertThrows(ArithmeticException.class,
                    () -> store.incrementBy("counter", 1));
        }
    }

    @Test
    void concurrentIncrementsAreAtomic(@TempDir Path dir) throws Exception {
        try (KvStore store = new KvStore(dir)) {
            int writers = 10;
            int incrementsPerWriter = 100;
            java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(writers);
            for (int w = 0; w < writers; w++) {
                Thread.startVirtualThread(() -> {
                    try {
                        for (int i = 0; i < incrementsPerWriter; i++) {
                            store.incrementBy("counter", 1);
                        }
                    } catch (IOException ignored) {
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(30, java.util.concurrent.TimeUnit.SECONDS));
            // Final value MUST be exactly writers * incrementsPerWriter
            assertEquals(Optional.of(Integer.toString(writers * incrementsPerWriter)),
                    store.get("counter"));
        }
    }

    @Test
    void repeatedReadsHitBlockCache(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("hot-key", "hot-value");
            // Force flush so the key lives in an SSTable
            String bigValue = "x".repeat(100);
            for (int i = 0; i < 20_000; i++) {
                store.put("filler-" + String.format("%06d", i), bigValue);
            }

            // First read populates the cache (was a miss)
            store.get("hot-key");
            long missesAfterFirst = store.blockCache().misses();

            // Next reads should be hits
            for (int i = 0; i < 100; i++) {
                assertEquals(Optional.of("hot-value"), store.get("hot-key"));
            }
            long hits = store.blockCache().hits();
            long misses = store.blockCache().misses();

            assertEquals(missesAfterFirst, misses, "no new misses on repeat reads");
            assertTrue(hits >= 100, "expected >=100 hits, got " + hits);
        }
    }
}
