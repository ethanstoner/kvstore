package com.ethanstoner.kvstore;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The public key-value store. This is the class users actually touch.
 *
 * <p>It wires together the three pieces of an LSM-tree:
 * <pre>
 *   put(k,v) ─► WAL.append (durable)  ─► Memtable.put (fast in-RAM)
 *                                              │
 *                                    [flush when too big]
 *                                              ▼
 *                                          SSTable (immutable, on disk)
 *   get(k)   ─► Memtable.get  ─► newest SSTable ─► older SSTables
 * </pre>
 *
 * <p>On construction it {@link WriteAheadLog#replay replays} the WAL so the
 * store comes back exactly as it was before the last shutdown or crash.
 */
public final class KvStore implements AutoCloseable {

    /** Flush the memtable to disk once it exceeds this many bytes. */
    static final long FLUSH_THRESHOLD_BYTES = 4L * 1024 * 1024; // 4 MB

    private final Path dataDir;

    /** Sequence counter for the next SSTable file. Guarded by {@link #flushLock}. */
    private int nextSequence;

    /** In-memory write buffer. Replaced atomically on flush. */
    private volatile Memtable memtable;

    /**
     * Ordered list of on-disk SSTables, oldest first.
     * Always an immutable list; replaced atomically (under {@link #flushLock}).
     */
    private volatile List<SSTable> sstables;

    /** Current WAL. Replaced atomically on flush. */
    private WriteAheadLog wal;

    /** Serialises flush operations (writes and flushes may interleave reads). */
    private final ReentrantLock flushLock = new ReentrantLock();

    // =========================================================================
    // Constructor
    // =========================================================================

    /**
     * Opens (or creates) a store rooted at {@code dataDir}.
     *
     * <ul>
     *   <li>Scans {@code dataDir} for existing {@code sst-*.db} files and opens
     *       them (corrupt files are silently skipped).</li>
     *   <li>Replays the WAL to rebuild any writes that were not yet flushed.</li>
     * </ul>
     */
    public KvStore(Path dataDir) throws IOException {
        this.dataDir = dataDir;
        Files.createDirectories(dataDir);

        // --- 1. Open existing SSTables ------------------------------------------
        List<SSTable> loaded = new ArrayList<>();
        try (DirectoryStream<Path> ds =
                     Files.newDirectoryStream(dataDir, "sst-*.db")) {
            for (Path p : ds) {
                try {
                    loaded.add(SSTable.open(p));
                } catch (IOException e) {
                    // Skip corrupt / incomplete files.
                }
            }
        }
        // Sort oldest → newest by sequence number.
        loaded.sort((a, b) -> Integer.compare(a.sequenceNum(), b.sequenceNum()));
        this.sstables = Collections.unmodifiableList(loaded);

        // Determine next sequence number.
        int maxSeq = loaded.isEmpty() ? -1
                : loaded.get(loaded.size() - 1).sequenceNum();
        this.nextSequence = maxSeq + 1;

        // --- 2. Start fresh memtable and replay WAL -----------------------------
        this.memtable = new Memtable();
        this.wal = new WriteAheadLog(dataDir.resolve("wal.log"));
        wal.replay((key, value) -> {
            if (value == null) {
                memtable.delete(key);
            } else {
                memtable.put(key, value);
            }
        });
    }

    // =========================================================================
    // Write operations
    // =========================================================================

    public void put(String key, String value) throws IOException {
        flushLock.lock();
        try {
            wal.appendPut(key, value);   // durable first
            memtable.put(key, value);    // then visible
            maybeFlush();
        } finally {
            flushLock.unlock();
        }
    }

    public void delete(String key) throws IOException {
        flushLock.lock();
        try {
            wal.appendDelete(key);
            memtable.delete(key);
            maybeFlush();
        } finally {
            flushLock.unlock();
        }
    }

    // =========================================================================
    // Read operations
    // =========================================================================

    /**
     * Point lookup. Checks the memtable first, then SSTables newest-to-oldest.
     *
     * @return the value, or {@code Optional.empty()} if absent / deleted
     */
    public Optional<String> get(String key) throws IOException {
        // 1. Check memtable (most recent data).
        String v = memtable.get(key);
        if (v != null) {
            return Memtable.TOMBSTONE.equals(v) ? Optional.empty() : Optional.of(v);
        }

        // 2. Walk SSTables newest-to-oldest.
        List<SSTable> tables = sstables;
        for (int i = tables.size() - 1; i >= 0; i--) {
            Optional<String> found = tables.get(i).get(key);
            if (found.isPresent()) {
                String val = found.get();
                return Memtable.TOMBSTONE.equals(val) ? Optional.empty() : Optional.of(val);
            }
        }

        return Optional.empty();
    }

    /**
     * Ordered range scan, {@code [fromInclusive, toExclusive)}, tombstones hidden.
     *
     * <p>Uses a "first write wins" merge: the memtable (newest) populates the
     * result map first, then SSTables newest-to-oldest fill in any gaps.
     * Tombstones are filtered out at the end.
     */
    public Map<String, String> scan(String fromInclusive, String toExclusive)
            throws IOException {

        // Use TreeMap so result is sorted.
        TreeMap<String, String> merged = new TreeMap<>();

        // 1. Memtable entries (newest, populate first).
        for (Map.Entry<String, String> e :
                memtable.scan(fromInclusive, toExclusive).entrySet()) {
            merged.put(e.getKey(), e.getValue());
        }

        // 2. SSTables newest-to-oldest; putIfAbsent keeps the newest value.
        List<SSTable> tables = sstables;
        for (int i = tables.size() - 1; i >= 0; i--) {
            for (Map.Entry<String, String> e :
                    tables.get(i).scan(fromInclusive, toExclusive).entrySet()) {
                merged.putIfAbsent(e.getKey(), e.getValue());
            }
        }

        // 3. Filter out tombstones.
        merged.entrySet().removeIf(e -> Memtable.TOMBSTONE.equals(e.getValue()));

        return merged;
    }

    // =========================================================================
    // Flush
    // =========================================================================

    /**
     * Flushes the memtable to a new SSTable if the size threshold is exceeded.
     *
     * <p>Must be called while holding {@link #flushLock}.
     */
    private void maybeFlush() throws IOException {
        if (memtable.approximateBytes() < FLUSH_THRESHOLD_BYTES) {
            return;
        }

        // Capture the memtable we are flushing.
        Memtable flushing = this.memtable;

        // Write a new SSTable.
        Path sstPath = SSTable.fileName(dataDir, nextSequence);
        SSTable.write(sstPath, flushing.entries());
        SSTable newSst = SSTable.open(sstPath);

        // Atomically extend the sstables list.
        List<SSTable> current = new ArrayList<>(sstables);
        current.add(newSst);
        this.sstables = Collections.unmodifiableList(current);

        // Advance sequence counter.
        nextSequence++;

        // Swap in a fresh memtable.
        this.memtable = new Memtable();

        // Truncate (reset) the WAL — the flushed data is now safely on disk.
        WriteAheadLog oldWal = this.wal;
        this.wal = new WriteAheadLog(dataDir.resolve("wal.log"), true);
        oldWal.close();
    }

    // =========================================================================
    // AutoCloseable
    // =========================================================================

    @Override
    public void close() throws IOException {
        flushLock.lock();
        try {
            wal.close();
            IOException first = null;
            for (SSTable sst : sstables) {
                try {
                    sst.close();
                } catch (IOException e) {
                    if (first == null) first = e;
                }
            }
            if (first != null) throw first;
        } finally {
            flushLock.unlock();
        }
    }
}
