package com.ethanstoner.kvstore;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BlockCacheTest {

    @Test
    void putThenGetReturnsValue() {
        BlockCache c = new BlockCache(10);
        c.put(1, 100L, "hello");
        assertEquals("hello", c.get(1, 100L));
    }

    @Test
    void missReturnsNullAndIncrementsMisses() {
        BlockCache c = new BlockCache(10);
        assertNull(c.get(1, 100L));
        assertEquals(1, c.misses());
        assertEquals(0, c.hits());
    }

    @Test
    void hitIncrementsHits() {
        BlockCache c = new BlockCache(10);
        c.put(1, 100L, "v");
        c.get(1, 100L);
        c.get(1, 100L);
        assertEquals(2, c.hits());
    }

    @Test
    void evictsLeastRecentlyUsedAtCapacity() {
        BlockCache c = new BlockCache(2);
        c.put(1, 1L, "a");
        c.put(1, 2L, "b");
        c.put(1, 3L, "c");  // should evict (1,1)
        assertNull(c.get(1, 1L));
        assertEquals("b", c.get(1, 2L));
        assertEquals("c", c.get(1, 3L));
    }

    @Test
    void keyIncludesSequenceNumber() {
        BlockCache c = new BlockCache(10);
        c.put(1, 100L, "from-sst-1");
        c.put(2, 100L, "from-sst-2");
        assertEquals("from-sst-1", c.get(1, 100L));
        assertEquals("from-sst-2", c.get(2, 100L));
    }
}
