package com.ethanstoner.kvstore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class SSTableTest {

    @Test
    void writeAndReadBack(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("sst-000001.db");
        List<Map.Entry<String, String>> entries = List.of(
                Map.entry("apple", "red"),
                Map.entry("banana", "yellow"),
                Map.entry("cherry", "dark red")
        );
        SSTable.write(file, entries);
        try (SSTable sst = SSTable.open(file)) {
            assertEquals(Optional.of("red"), sst.get("apple"));
            assertEquals(Optional.of("yellow"), sst.get("banana"));
            assertEquals(Optional.of("dark red"), sst.get("cherry"));
            assertTrue(sst.get("dragonfruit").isEmpty());
        }
    }

    @Test
    void tombstonesArePreserved(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("sst-000001.db");
        List<Map.Entry<String, String>> entries = List.of(
                Map.entry("alive", "yes"),
                Map.entry("dead", Memtable.TOMBSTONE)
        );
        SSTable.write(file, entries);
        try (SSTable sst = SSTable.open(file)) {
            assertEquals(Optional.of("yes"), sst.get("alive"));
            assertTrue(sst.get("dead").isPresent());
            assertEquals(Memtable.TOMBSTONE, sst.get("dead").get());
        }
    }

    @Test
    void keysInSortedOrder(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("sst-000001.db");
        List<Map.Entry<String, String>> entries = List.of(
                Map.entry("a", "1"),
                Map.entry("b", "2"),
                Map.entry("c", "3"),
                Map.entry("d", "4")
        );
        SSTable.write(file, entries);
        try (SSTable sst = SSTable.open(file)) {
            NavigableMap<String, String> range = sst.scan("b", "d");
            assertEquals(List.of("b", "c"), new ArrayList<>(range.keySet()));
            assertEquals("2", range.get("b"));
            assertEquals("3", range.get("c"));
        }
    }

    @Test
    void bloomFilterSkipsMissingKeys(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("sst-000001.db");
        List<Map.Entry<String, String>> entries = List.of(
                Map.entry("x", "1"),
                Map.entry("y", "2")
        );
        SSTable.write(file, entries);
        try (SSTable sst = SSTable.open(file)) {
            assertTrue(sst.get("z").isEmpty());
            assertTrue(sst.get("a").isEmpty());
        }
    }

    @Test
    void corruptFileIsDetected(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("sst-corrupt.db");
        java.nio.file.Files.write(file, new byte[]{1, 2, 3, 4, 5});
        assertThrows(IOException.class, () -> SSTable.open(file));
    }

    @Test
    void extractsSequenceNumberFromFilename() {
        assertEquals(1, SSTable.sequenceNumber(Path.of("sst-000001.db")));
        assertEquals(42, SSTable.sequenceNumber(Path.of("sst-000042.db")));
        assertEquals(999999, SSTable.sequenceNumber(Path.of("sst-999999.db")));
    }

    @Test
    void entryCountMatchesNumberWritten(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("sst-000001.db");
        List<Map.Entry<String, String>> entries = List.of(
                Map.entry("a", "1"),
                Map.entry("b", "2"),
                Map.entry("c", "3"));
        SSTable.write(file, entries);
        try (SSTable sst = SSTable.open(file)) {
            assertEquals(3, sst.entryCount());
        }
    }
}
