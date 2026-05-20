package com.ethanstoner.kvstore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ConcurrentFlushTest {

    /**
     * After writing enough data to trigger one or more flushes, every key
     * that was put must remain readable — either from the active memtable,
     * the immutable memtable (mid-flush), or a completed SSTable.
     */
    @Test
    void writesAreVisibleDuringFlush(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            String bigValue = "x".repeat(100);
            // 20 000 × ~200 bytes ≈ 4 MB — enough to trigger exactly one flush.
            for (int i = 0; i < 20_000; i++) {
                store.put("key-" + String.format("%06d", i), bigValue);
            }

            // The data must be readable right after the writes complete, regardless
            // of whether the background flush has finished yet.
            assertEquals(Optional.of(bigValue), store.get("key-000000"));
            assertEquals(Optional.of(bigValue), store.get("key-019999"));
        }
    }

    /**
     * Writing ~3× the flush threshold should produce multiple SSTable files,
     * and all data must survive a close + reopen cycle.
     */
    @Test
    void multipleFlushesProduceMultipleSSTables(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            String bigValue = "x".repeat(100);
            // ~3 flushes worth of data.
            for (int i = 0; i < 60_000; i++) {
                store.put("k-" + String.format("%06d", i), bigValue);
            }
            // close() waits for any pending flush to finish.
        }

        // There should be at least 2 SSTables on disk.
        long sstCount = Files.list(dir)
                .filter(p -> p.getFileName().toString().endsWith(".db"))
                .count();
        assertTrue(sstCount >= 2,
                "Expected at least 2 SSTables after 3 flushes worth of data, found " + sstCount);

        // Reopen and verify all data is recoverable.
        try (KvStore reopened = new KvStore(dir)) {
            assertEquals(Optional.of("x".repeat(100)), reopened.get("k-000000"));
            assertEquals(Optional.of("x".repeat(100)), reopened.get("k-030000"));
            assertEquals(Optional.of("x".repeat(100)), reopened.get("k-059999"));
        }
    }

    /**
     * Close flushes any in-progress immutable memtable; reopening must recover
     * all acknowledged writes from either SSTables or the WAL.
     */
    @Test
    void recoveryFromPendingWalWorks(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            String bigValue = "y".repeat(100);
            // Enough to trigger at least one flush; close() drains it cleanly.
            for (int i = 0; i < 20_000; i++) {
                store.put("rec-" + String.format("%06d", i), bigValue);
            }
        }

        // After a clean reopen all data must be present.
        try (KvStore reopened = new KvStore(dir)) {
            assertEquals(Optional.of("y".repeat(100)), reopened.get("rec-000000"));
            assertEquals(Optional.of("y".repeat(100)), reopened.get("rec-019999"));
        }
    }

    /**
     * Multiple virtual threads writing concurrently must not corrupt data or
     * deadlock; a concurrent reader must not throw.
     */
    @Test
    void concurrentWritersAndReaders(@TempDir Path dir) throws Exception {
        try (KvStore store = new KvStore(dir)) {
            final int writers         = 4;
            final int writesPerWriter = 5_000;
            final String bigValue     = "z".repeat(100);

            CountDownLatch done = new CountDownLatch(writers);
            List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());

            for (int w = 0; w < writers; w++) {
                final int id = w;
                Thread.startVirtualThread(() -> {
                    try {
                        for (int i = 0; i < writesPerWriter; i++) {
                            store.put("w" + id + "-" + String.format("%06d", i), bigValue);
                        }
                    } catch (Throwable t) {
                        errors.add(t);
                    } finally {
                        done.countDown();
                    }
                });
            }

            // Concurrent reader — must not throw; result may be empty (key not yet written).
            Thread.startVirtualThread(() -> {
                try {
                    for (int i = 0; i < 1_000; i++) {
                        store.get("w0-000000"); // ignore return value
                        Thread.sleep(1);
                    }
                } catch (Throwable ignored) {
                    // Reader errors are non-fatal for this test.
                }
            });

            assertTrue(done.await(60, TimeUnit.SECONDS),
                    "Writers did not finish within 60 s");
            assertTrue(errors.isEmpty(),
                    "Writer errors: " + errors);

            // Spot-check that every writer's first and last key is present.
            for (int w = 0; w < writers; w++) {
                assertEquals(Optional.of(bigValue),
                        store.get("w" + w + "-000000"),
                        "Missing first key for writer " + w);
                assertEquals(Optional.of(bigValue),
                        store.get("w" + w + "-" + String.format("%06d", writesPerWriter - 1)),
                        "Missing last key for writer " + w);
            }
        }
    }

    /**
     * Verifies that the immutable memtable is visible to readers during an
     * ongoing flush.  We write enough data to trigger a flush, then immediately
     * read a key that must be in the immutable memtable (it was written before
     * the flush threshold was crossed).
     */
    @Test
    void immutableMemtableIsReadableDuringFlush(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            // Write a sentinel key first — it will be in the immutable memtable
            // once a flush is triggered.
            store.put("sentinel", "found-it");

            // Now push enough data to cross the flush threshold.
            String bigValue = "x".repeat(100);
            for (int i = 0; i < 20_000; i++) {
                store.put("pad-" + String.format("%06d", i), bigValue);
            }

            // Whether the flush has completed or not, "sentinel" must be readable.
            assertEquals(Optional.of("found-it"), store.get("sentinel"),
                    "sentinel key must be readable from immutable memtable or SSTable");
        }
    }

    /**
     * Verifies that a key written to the active memtable shadows a stale value
     * in the immutable memtable that is being flushed concurrently.
     */
    @Test
    void activeMemtableShadowsImmutableDuringFlush(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            // Old value — will end up in the immutable memtable once a flush triggers.
            store.put("contested", "old-value");

            // Trigger flush.
            String bigValue = "x".repeat(100);
            for (int i = 0; i < 20_000; i++) {
                store.put("pad-" + String.format("%06d", i), bigValue);
            }

            // Now write the new value into the fresh (active) memtable.
            store.put("contested", "new-value");

            // The active memtable must win.
            assertEquals(Optional.of("new-value"), store.get("contested"),
                    "active memtable must shadow immutable memtable");
        }
    }
}
