package com.ethanstoner.kvstore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteAheadLogTest {

    /**
     * Replays the log into a map; null value = delete (key removed).
     * Uses the new TriConsumer signature; the expiresAtMs argument is ignored here
     * since these tests only care about key/value correctness.
     */
    private Map<String, String> replayInto(Path logPath) throws IOException {
        Map<String, String> state = new LinkedHashMap<>();
        try (WriteAheadLog wal = new WriteAheadLog(logPath)) {
            wal.replay((k, v, expiresAtMs) -> {
                if (v == null) {
                    state.remove(k);
                } else {
                    state.put(k, v);
                }
            });
        }
        return state;
    }

    @Test
    void appendedRecordsReplayInOrder(@TempDir Path dir) throws IOException {
        Path log = dir.resolve("wal.log");
        try (WriteAheadLog wal = new WriteAheadLog(log)) {
            wal.appendPut("a", "1");
            wal.appendPut("b", "2");
            wal.appendPut("a", "3"); // overwrite
        }
        Map<String, String> state = replayInto(log);
        assertEquals("3", state.get("a"));
        assertEquals("2", state.get("b"));
    }

    @Test
    void deleteRecordReplaysAsRemoval(@TempDir Path dir) throws IOException {
        Path log = dir.resolve("wal.log");
        try (WriteAheadLog wal = new WriteAheadLog(log)) {
            wal.appendPut("gone", "x");
            wal.appendDelete("gone");
        }
        assertNull(replayInto(log).get("gone"));
    }

    @Test
    void replayOnMissingFileIsEmptyNotAnError(@TempDir Path dir) throws IOException {
        // Opening a brand-new store must not blow up.
        assertTrue(replayInto(dir.resolve("does-not-exist.log")).isEmpty());
    }

    @Test
    void reopeningAppendsRatherThanTruncating(@TempDir Path dir) throws IOException {
        Path log = dir.resolve("wal.log");
        try (WriteAheadLog wal = new WriteAheadLog(log)) {
            wal.appendPut("first", "1");
        }
        try (WriteAheadLog wal = new WriteAheadLog(log)) {
            wal.appendPut("second", "2");
        }
        Map<String, String> state = replayInto(log);
        assertEquals("1", state.get("first"));
        assertEquals("2", state.get("second"));
    }

    @Test
    void resetTruncatesExistingLog(@TempDir Path dir) throws IOException {
        Path log = dir.resolve("wal.log");
        try (WriteAheadLog wal = new WriteAheadLog(log, false)) {
            wal.appendPut("old", "data");
        }
        try (WriteAheadLog wal = new WriteAheadLog(log, true)) {
            wal.appendPut("new", "data");
        }
        Map<String, String> state = replayInto(log);
        assertNull(state.get("old"));
        assertEquals("data", state.get("new"));
    }

    @Test
    void replayStopsAtCorruptedRecord(@TempDir Path dir) throws IOException {
        Path log = dir.resolve("wal.log");

        // Write 3 records
        try (WriteAheadLog wal = new WriteAheadLog(log)) {
            wal.appendPut("a", "1");
            wal.appendPut("b", "2");
            wal.appendPut("c", "3");
        }

        // Corrupt a byte in the middle of the file (in record 2's payload or CRC)
        byte[] bytes = java.nio.file.Files.readAllBytes(log);
        // Flip a byte near the middle
        bytes[bytes.length / 2] ^= (byte) 0xff;
        java.nio.file.Files.write(log, bytes);

        Map<String, String> state = replayInto(log);

        // Record 1 should have survived. Record 2 onward is gone (replay stops at first bad CRC).
        assertEquals("1", state.get("a"));
        assertNull(state.get("c"), "records after corruption must not be replayed");
    }

    @Test
    void replayHandlesTruncatedTail(@TempDir Path dir) throws IOException {
        Path log = dir.resolve("wal.log");

        // Write 3 records
        try (WriteAheadLog wal = new WriteAheadLog(log)) {
            wal.appendPut("first", "1");
            wal.appendPut("second", "2");
            wal.appendPut("third", "3");
        }

        // Truncate the file by 3 bytes (simulates a partial write / mid-record crash)
        byte[] bytes = java.nio.file.Files.readAllBytes(log);
        byte[] truncated = new byte[bytes.length - 3];
        System.arraycopy(bytes, 0, truncated, 0, truncated.length);
        java.nio.file.Files.write(log, truncated);

        Map<String, String> state = replayInto(log);

        // The last record's bytes are partial — replay must stop there gracefully.
        // First two records must survive.
        assertEquals("1", state.get("first"));
        assertEquals("2", state.get("second"));
        assertNull(state.get("third"));
    }

    @Test
    void emptyLogReplayIsClean(@TempDir Path dir) throws IOException {
        Path log = dir.resolve("wal.log");
        // Create an empty WAL file
        try (WriteAheadLog wal = new WriteAheadLog(log)) {}
        Map<String, String> state = replayInto(log);
        assertTrue(state.isEmpty());
    }

    @Test
    void appendPutWithExpiryReplaysWithCorrectExpiry(@TempDir Path dir) throws IOException {
        Path log = dir.resolve("wal.log");
        long expiry = System.currentTimeMillis() + 60_000;
        try (WriteAheadLog wal = new WriteAheadLog(log)) {
            wal.appendPut("k", "v", expiry);
        }
        Map<String, Long> expiryMap = new LinkedHashMap<>();
        try (WriteAheadLog wal = new WriteAheadLog(log)) {
            wal.replay((k, v, expiresAtMs) -> {
                if (v != null) expiryMap.put(k, expiresAtMs);
            });
        }
        assertEquals(expiry, expiryMap.get("k"));
    }

    @Test
    void appendPutWithoutExpiryReplaysAsNever(@TempDir Path dir) throws IOException {
        Path log = dir.resolve("wal.log");
        try (WriteAheadLog wal = new WriteAheadLog(log)) {
            wal.appendPut("k", "v"); // no expiry
        }
        Map<String, Long> expiryMap = new LinkedHashMap<>();
        try (WriteAheadLog wal = new WriteAheadLog(log)) {
            wal.replay((k, v, expiresAtMs) -> {
                if (v != null) expiryMap.put(k, expiresAtMs);
            });
        }
        assertEquals(ValueEntry.NEVER, expiryMap.get("k"));
    }
}
