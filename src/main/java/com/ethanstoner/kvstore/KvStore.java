package com.ethanstoner.kvstore;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The public key-value store. This is the class users actually touch.
 *
 * <p>It wires together the two pieces you've already seen:
 * <pre>
 *   put(k,v) ─► WAL.append (durable)  ─► Memtable.put (fast in-RAM)
 *   get(k)   ─► Memtable.get          ─► (later: SSTables on disk)
 * </pre>
 *
 * <p>On construction it {@link WriteAheadLog#replay replays} the WAL so the
 * store comes back exactly as it was before the last shutdown or crash &mdash;
 * that round-trip is the property the {@code recoversAfterReopen} test proves.
 *
 * <h2>Roadmap (this is the part you implement as you learn — see README)</h2>
 * <ul>
 *   <li>When the memtable exceeds a size threshold, flush it to an immutable,
 *       sorted <b>SSTable</b> file and start a fresh memtable + WAL.</li>
 *   <li>{@code get} should fall through memtable &rarr; newest SSTable &rarr;
 *       older SSTables.</li>
 *   <li>A background <b>compaction</b> thread merges SSTables, dropping
 *       shadowed values and tombstones.</li>
 *   <li>A Bloom filter per SSTable to skip files that can't contain the key.</li>
 * </ul>
 */
public final class KvStore implements AutoCloseable {

    private final Memtable memtable = new Memtable();
    private final WriteAheadLog wal;

    /** Opens (or creates) a store rooted at {@code dataDir}. */
    public KvStore(Path dataDir) throws IOException {
        this.wal = new WriteAheadLog(dataDir.resolve("wal.log"));
        // Rebuild in-memory state from the log before accepting new writes.
        wal.replay((key, value) -> {
            if (value == null) {
                memtable.delete(key);
            } else {
                memtable.put(key, value);
            }
        });
    }

    public void put(String key, String value) throws IOException {
        wal.appendPut(key, value);   // durable first
        memtable.put(key, value);    // then visible
    }

    public Optional<String> get(String key) {
        String v = memtable.get(key);
        if (v == null || Memtable.TOMBSTONE.equals(v)) {
            return Optional.empty(); // never written, or deleted
        }
        return Optional.of(v);
    }

    public void delete(String key) throws IOException {
        wal.appendDelete(key);
        memtable.delete(key);
    }

    /** Ordered range scan, {@code [fromInclusive, toExclusive)}, tombstones hidden. */
    public Map<String, String> scan(String fromInclusive, String toExclusive) {
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : memtable.scan(fromInclusive, toExclusive).entrySet()) {
            if (!Memtable.TOMBSTONE.equals(e.getValue())) {
                result.put(e.getKey(), e.getValue());
            }
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        wal.close();
    }
}
