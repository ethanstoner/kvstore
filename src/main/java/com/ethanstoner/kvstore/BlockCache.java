package com.ethanstoner.kvstore;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A small thread-safe LRU cache for SSTable value reads, keyed by
 * {@code (sstable sequence number, file offset)}. Avoids redundant
 * disk seeks for hot keys.
 *
 * <p>Sized by entry count rather than bytes for simplicity. Default
 * capacity 1024 entries — about a few hundred KB at typical value sizes.
 */
public final class BlockCache {

    /** Composite key: sstable sequence number + offset. */
    record Key(int seq, long offset) {}

    private final int capacity;
    private final LinkedHashMap<Key, String> map;
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();

    public BlockCache(int capacity) {
        this.capacity = capacity;
        this.map = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Key, String> eldest) {
                return size() > BlockCache.this.capacity;
            }
        };
    }

    /** @return cached value, or {@code null} on miss. */
    public synchronized String get(int seq, long offset) {
        String v = map.get(new Key(seq, offset));
        if (v == null) misses.incrementAndGet();
        else hits.incrementAndGet();
        return v;
    }

    public synchronized void put(int seq, long offset, String value) {
        map.put(new Key(seq, offset), value);
    }

    public long hits()   { return hits.get(); }
    public long misses() { return misses.get(); }

    public long size() {
        synchronized (this) { return map.size(); }
    }

    public int capacity() { return capacity; }
}
