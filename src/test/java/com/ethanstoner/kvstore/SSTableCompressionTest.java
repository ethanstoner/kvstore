package com.ethanstoner.kvstore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SSTableCompressionTest {

    @Test
    void largeRepetitiveValueShrinksOnDisk(@TempDir Path dir) throws IOException {
        // Build entries with HIGHLY compressible values (lots of repetition).
        // Note: we still need keys to be sorted.
        List<Map.Entry<String, String>> entries = new ArrayList<>();
        String compressible = "abcdefgh".repeat(200); // 1600 bytes of repeating text
        for (int i = 0; i < 100; i++) {
            entries.add(Map.entry(String.format("k%04d", i), compressible));
        }

        Path file = dir.resolve("sst-000001.db");
        SSTable.write(file, entries);
        long fileSize = Files.size(file);

        // Uncompressed size estimate: ~100 keys * (~1 + 2 + 4 + 4 + 1600) bytes
        // plus bloom + index + footer. Should be well over 160 KB.
        // Compressed should be << that.
        long uncompressedDataApprox = 100L * (1 + 2 + 4 + 4 + 1600);
        assertTrue(fileSize < uncompressedDataApprox / 4,
                "expected file size " + fileSize + " to be << uncompressed estimate "
                        + uncompressedDataApprox);
    }

    @Test
    void compressedValuesRoundTripCorrectly(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("sst-000001.db");
        String big = "hello world ".repeat(50); // > 64 bytes -> will be compressed
        List<Map.Entry<String, String>> entries = List.of(
                Map.entry("a", "small"),                          // small, uncompressed
                Map.entry("b", big),                              // large, compressed
                Map.entry("c", "x".repeat(500))                   // large, compressed
        );
        SSTable.write(file, entries);
        try (SSTable sst = SSTable.open(file)) {
            assertEquals(java.util.Optional.of("small"), sst.get("a"));
            assertEquals(java.util.Optional.of(big), sst.get("b"));
            assertEquals(java.util.Optional.of("x".repeat(500)), sst.get("c"));
        }
    }

    @Test
    void incompressibleValueStoredRaw(@TempDir Path dir) throws IOException {
        // Random data doesn't compress; the writer should fall back to raw.
        java.util.Random rnd = new java.util.Random(42);
        byte[] randomBytes = new byte[200];
        rnd.nextBytes(randomBytes);
        // Map to a printable-but-random String — really pseudo-random characters.
        StringBuilder sb = new StringBuilder();
        for (byte b : randomBytes) sb.append((char) (((b & 0xff) % 95) + 32));
        String random = sb.toString();

        Path file = dir.resolve("sst-000001.db");
        SSTable.write(file, List.of(Map.entry("rand", random)));
        try (SSTable sst = SSTable.open(file)) {
            // Round-trip must work whether stored raw or compressed.
            assertEquals(java.util.Optional.of(random), sst.get("rand"));
        }
    }

    @Test
    void compressedAndUncompressedMix(@TempDir Path dir) throws IOException {
        // Many small + a few large to confirm both paths coexist.
        List<Map.Entry<String, String>> entries = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            entries.add(Map.entry(String.format("small-%03d", i), "v" + i));
        }
        for (int i = 0; i < 5; i++) {
            entries.add(Map.entry(String.format("zlarge-%03d", i),
                    ("payload-" + i + " ").repeat(20)));   // > 64 bytes
        }
        entries.sort(Map.Entry.comparingByKey());

        Path file = dir.resolve("sst-000001.db");
        SSTable.write(file, entries);
        try (SSTable sst = SSTable.open(file)) {
            for (int i = 0; i < 10; i++) {
                assertEquals(java.util.Optional.of("v" + i),
                        sst.get(String.format("small-%03d", i)));
            }
            for (int i = 0; i < 5; i++) {
                assertEquals(java.util.Optional.of(("payload-" + i + " ").repeat(20)),
                        sst.get(String.format("zlarge-%03d", i)));
            }
        }
    }
}
