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
}
