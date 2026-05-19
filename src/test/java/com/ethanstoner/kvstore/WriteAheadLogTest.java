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

    /** Replays the log into a map; null value = delete (key removed). */
    private Map<String, String> replayInto(Path logPath) throws IOException {
        Map<String, String> state = new LinkedHashMap<>();
        try (WriteAheadLog wal = new WriteAheadLog(logPath)) {
            wal.replay((k, v) -> {
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
}
