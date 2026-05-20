package com.ethanstoner.kvstore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class LeveledCompactionTest {

    @Test
    void flushedFilesGoToLevel0(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            String bigValue = "x".repeat(100);
            for (int i = 0; i < 20_000; i++) {
                store.put("k-" + String.format("%06d", i), bigValue);
            }
        }
        List<Path> sstables = Files.list(dir)
                .filter(p -> p.getFileName().toString().endsWith(".db"))
                .toList();
        assertFalse(sstables.isEmpty(), "expected at least one SSTable");
        for (Path p : sstables) {
            assertEquals(0, SSTable.levelOf(p),
                    "flush should produce L0 files only, got " + p.getFileName());
        }
    }

    @Test
    void l0ToL1CompactionRunsAfterEnoughFlushes(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            String bigValue = "x".repeat(100);
            // Trigger >= 4 flushes to fill L0 and force compaction
            for (int i = 0; i < 80_000; i++) {
                store.put("k-" + String.format("%06d", i), bigValue);
            }
            store.compactNow();
            store.compactNow(); // may need a second pass if L0 still has files
            store.compactNow();
        }

        long l0 = countAtLevel(dir, 0);
        long l1 = countAtLevel(dir, 1);
        assertTrue(l1 >= 1, "expected at least one L1 SSTable, got " + l1);
        assertTrue(l0 < 4, "L0 should have <4 SSTables after compaction, got " + l0);
    }

    @Test
    void l1FilesHaveNonOverlappingKeys(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            String bigValue = "x".repeat(100);
            // Distinct keys, sequential so L1 splits cleanly
            for (int i = 0; i < 80_000; i++) {
                store.put("k-" + String.format("%06d", i), bigValue);
            }
            store.compactNow();
            store.compactNow();
            store.compactNow();
        }

        try (KvStore store = new KvStore(dir)) {
            // Read the L1 SSTables' min/max keys, sort, verify no overlap.
            List<SSTable> l1 = store.sstableSnapshot().stream()
                    .filter(s -> s.level() == 1)
                    .sorted((a, b) -> a.keySet().first().compareTo(b.keySet().first()))
                    .toList();
            for (int i = 1; i < l1.size(); i++) {
                String prevMax = l1.get(i - 1).keySet().last();
                String curMin = l1.get(i).keySet().first();
                assertTrue(curMin.compareTo(prevMax) > 0,
                        "L1 files overlap: prev max=" + prevMax + ", cur min=" + curMin);
            }
        }
    }

    @Test
    void dataIntegrityAcrossLevels(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir)) {
            String bigValue = "x".repeat(100);
            for (int i = 0; i < 80_000; i++) {
                store.put("k-" + String.format("%06d", i), bigValue);
            }
            store.compactNow();
            store.compactNow();
            store.compactNow();

            // Spot check across the key space
            for (int i = 0; i < 80_000; i += 5_000) {
                assertEquals(Optional.of(bigValue),
                        store.get("k-" + String.format("%06d", i)),
                        "missing or wrong value for key " + i);
            }
        }
    }

    @Test
    void backwardCompatWithLegacyFilenames(@TempDir Path dir) throws IOException {
        // Manually create a legacy-named SSTable
        Path legacy = dir.resolve("sst-000001.db");
        SSTable.write(legacy, List.of(
                Map.entry("legacy-key", "legacy-value")));
        // Open via SSTable to confirm parsing
        try (SSTable s = SSTable.open(legacy)) {
            assertEquals(0, s.level(), "legacy sst-* should be treated as L0");
        }
        // KvStore should open it and find the key
        try (KvStore store = new KvStore(dir)) {
            assertEquals(Optional.of("legacy-value"), store.get("legacy-key"));
        }
    }

    private static long countAtLevel(Path dir, int level) throws IOException {
        return Files.list(dir)
                .filter(p -> p.getFileName().toString().endsWith(".db"))
                .filter(p -> SSTable.levelOf(p) == level)
                .count();
    }
}
