package com.ethanstoner.kvstore;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MemtableTest {

    @Test
    void putThenGetReturnsValue() {
        Memtable m = new Memtable();
        m.put("a", "1");
        assertEquals("1", m.get("a"));
    }

    @Test
    void putOverwritesPreviousValue() {
        Memtable m = new Memtable();
        m.put("a", "1");
        m.put("a", "2");
        assertEquals("2", m.get("a"));
    }

    @Test
    void missingKeyReturnsNull() {
        assertNull(new Memtable().get("nope"));
    }

    @Test
    void deleteLeavesATombstoneNotNull() {
        Memtable m = new Memtable();
        m.put("a", "1");
        m.delete("a");
        // The key is NOT gone — it carries a tombstone so the delete can be
        // propagated to disk later. KvStore is what hides this from callers.
        assertEquals(Memtable.TOMBSTONE, m.get("a"));
    }

    @Test
    void entriesAreYieldedInSortedKeyOrder() {
        Memtable m = new Memtable();
        m.put("banana", "b");
        m.put("apple", "a");
        m.put("cherry", "c");
        List<String> keys = new ArrayList<>();
        for (Map.Entry<String, ValueEntry> e : m.entries().entrySet()) {
            keys.add(e.getKey());
        }
        assertEquals(List.of("apple", "banana", "cherry"), keys);
    }

    @Test
    void scanIsInclusiveFromExclusiveTo() {
        Memtable m = new Memtable();
        m.put("k1", "1");
        m.put("k2", "2");
        m.put("k3", "3");
        assertEquals(List.of("k1", "k2"),
                new ArrayList<>(m.scan("k1", "k3").keySet()));
    }

    @Test
    void nullKeyOrValueIsRejected() {
        Memtable m = new Memtable();
        assertThrows(IllegalArgumentException.class, () -> m.put(null, "v"));
        assertThrows(IllegalArgumentException.class, () -> m.put("k", null));
    }

    @Test
    void approximateBytesGrowsWithWrites() {
        Memtable m = new Memtable();
        assertTrue(m.isEmpty());
        m.put("key", "value");
        assertTrue(m.approximateBytes() > 0);
    }

    @Test
    void getEntryReturnsValueAndExpiry() {
        Memtable m = new Memtable();
        long expiry = System.currentTimeMillis() + 60_000;
        m.put("k", "v", expiry);
        ValueEntry e = m.getEntry("k");
        assertEquals("v", e.value());
        assertEquals(expiry, e.expiresAtMs());
    }

    @Test
    void getEntryWithNoExpiryReturnsNever() {
        Memtable m = new Memtable();
        m.put("k", "v");
        ValueEntry e = m.getEntry("k");
        assertEquals(ValueEntry.NEVER, e.expiresAtMs());
    }
}
