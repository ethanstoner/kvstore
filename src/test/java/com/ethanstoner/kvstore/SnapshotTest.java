package com.ethanstoner.kvstore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotTest {

    @Test
    void snapshotProducesFile(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("k1", "v1");
            store.put("k2", "v2");
            Path snap = store.snapshot();
            assertTrue(Files.exists(snap));
            assertTrue(Files.size(snap) > 0);
        }
    }

    @Test
    void snapshotIsAValidSSTable(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("apple", "red");
            store.put("banana", "yellow");
            store.put("cherry", "dark");
            Path snap = store.snapshot();
            // Open the snapshot as an SSTable and verify contents
            try (SSTable sst = SSTable.open(snap)) {
                assertEquals("red",    sst.get("apple").orElseThrow().value());
                assertEquals("yellow", sst.get("banana").orElseThrow().value());
                assertEquals("dark",   sst.get("cherry").orElseThrow().value());
            }
        }
    }

    @Test
    void snapshotExcludesTombstones(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("keep", "yes");
            store.put("doomed", "x");
            store.delete("doomed");
            Path snap = store.snapshot();
            try (SSTable sst = SSTable.open(snap)) {
                assertTrue(sst.get("keep").isPresent());
                assertTrue(sst.get("doomed").isEmpty());
            }
        }
    }

    @Test
    void snapshotExcludesExpiredEntries(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("alive", "yes");
            store.put("expired", "stale", System.currentTimeMillis() - 1);
            Path snap = store.snapshot();
            try (SSTable sst = SSTable.open(snap)) {
                assertTrue(sst.get("alive").isPresent());
                assertTrue(sst.get("expired").isEmpty());
            }
        }
    }

    @Test
    void snapshotMergesMemtableAndSSTables(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            // Force some keys into an SSTable first
            store.put("old", "v");
            String bigValue = "x".repeat(100);
            for (int i = 0; i < 20_000; i++) {
                store.put("pad-" + String.format("%06d", i), bigValue);
            }
            // Now "old" is in an SSTable. Add a new key to memtable.
            store.put("new", "fresh");

            Path snap = store.snapshot();
            try (SSTable sst = SSTable.open(snap)) {
                assertEquals("v", sst.get("old").orElseThrow().value());
                assertEquals("fresh", sst.get("new").orElseThrow().value());
            }
        }
    }

    @Test
    void newerWritesShadowOlderInSnapshot(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            store.put("k", "old");
            // Force flush
            String bigValue = "x".repeat(100);
            for (int i = 0; i < 20_000; i++) {
                store.put("pad-" + String.format("%06d", i), bigValue);
            }
            // Overwrite in memtable
            store.put("k", "new");
            Path snap = store.snapshot();
            try (SSTable sst = SSTable.open(snap)) {
                assertEquals("new", sst.get("k").orElseThrow().value());
            }
        }
    }

    @Test
    void lastSnapshotEpochAdvances(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            assertEquals(0L, store.lastSnapshotEpochSec());
            store.put("k", "v");
            store.snapshot();
            assertTrue(store.lastSnapshotEpochSec() > 0);
        }
    }

    @Test
    void backgroundSnapshotCompletes(@TempDir Path dir) throws Exception {
        try (KvStore store = new KvStore(dir)) {
            store.put("k", "v");
            java.util.concurrent.Future<?> f = store.bgsnapshot();
            f.get(10, java.util.concurrent.TimeUnit.SECONDS);
            assertTrue(store.lastSnapshotEpochSec() > 0);
            // Snapshot file should exist
            long snapCount = Files.list(dir)
                    .filter(p -> p.getFileName().toString().startsWith("snapshot-"))
                    .count();
            assertTrue(snapCount >= 1);
        }
    }

    @Test
    void existingSSTablesAreNotAffectedBySnapshot(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            String bigValue = "x".repeat(100);
            for (int i = 0; i < 20_000; i++) {
                store.put("k-" + String.format("%06d", i), bigValue);
            }
            long sstBefore = Files.list(dir)
                    .filter(p -> p.getFileName().toString().endsWith(".db"))
                    .filter(p -> !p.getFileName().toString().startsWith("snapshot-"))
                    .count();

            store.snapshot();

            long sstAfter = Files.list(dir)
                    .filter(p -> p.getFileName().toString().endsWith(".db"))
                    .filter(p -> !p.getFileName().toString().startsWith("snapshot-"))
                    .count();
            assertEquals(sstBefore, sstAfter, "snapshot must not affect existing SSTables");

            // Original data still readable
            assertEquals(Optional.of(bigValue), store.get("k-000000"));
        }
    }
}
