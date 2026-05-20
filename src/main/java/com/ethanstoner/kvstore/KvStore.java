package com.ethanstoner.kvstore;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
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
 *                                    immutableMemtable ─► SSTable (background)
 *   get(k)   ─► active memtable  ─► immutable memtable  ─► SSTables (newest→oldest)
 * </pre>
 *
 * <h2>TTL / expiration</h2>
 * <p>Each entry carries an optional absolute expiration timestamp (epoch ms).
 * Expired entries are invisible to reads and dropped during compaction.
 * The expiry is durable: it is stored in the WAL and in the SSTable.
 *
 * <h2>Concurrent flush</h2>
 * <p>When a flush is triggered the active memtable moves to an
 * {@link #immutableMemtable} slot and is written to an SSTable on a single
 * background thread ({@link #flushExecutor}).  Writers return immediately after
 * the swap (the I/O happens off the write path).  Reads always check all three
 * layers in recency order without holding any lock.
 *
 * <h2>Two-WAL crash safety (LevelDB-style)</h2>
 * <ul>
 *   <li>{@code wal.log} — active WAL for the current memtable.</li>
 *   <li>{@code wal-pending.log} — exists only while a flush is in progress;
 *       holds the WAL of the immutable memtable being written to disk.</li>
 * </ul>
 * <p>On startup both WALs are replayed (pending first, active second) before
 * normal operation resumes. {@code wal-pending.log} is deleted by the
 * background thread after the corresponding SSTable is successfully synced.
 *
 * <p>On construction it {@link WriteAheadLog#replay replays} the WAL(s) so the
 * store comes back exactly as it was before the last shutdown or crash.
 */
public final class KvStore implements AutoCloseable {

    /** Flush the memtable to disk once it exceeds this many bytes. */
    static final long FLUSH_THRESHOLD_BYTES = 4L * 1024 * 1024; // 4 MB

    // ---- Leveled compaction knobs -------------------------------------------
    /** Trigger L0 → L1 compaction once L0 holds this many files. */
    static final int L0_TRIGGER_FILES = 4;
    /** Target total size for L1 in bytes. */
    static final long L1_TARGET_BYTES = 10L * 1024 * 1024;   // 10 MB
    /** Each successive level is this many times larger than the previous. */
    static final int LEVEL_SIZE_MULTIPLIER = 10;
    /** Maximum size of a single output SSTable produced by compaction. */
    static final long TARGET_SSTABLE_BYTES = 2L * 1024 * 1024; // 2 MB

    private static final String WAL_NAME         = "wal.log";
    private static final String WAL_PENDING_NAME = "wal-pending.log";

    private final Path dataDir;

    /** Sequence counter for the next SSTable file. Guarded by {@link #flushLock}. */
    private int nextSequence;

    /**
     * Active in-memory write buffer. Volatile so reads observe the latest
     * reference without holding {@link #flushLock}.
     */
    private volatile Memtable memtable;

    /**
     * Non-null only while a background flush is in progress.  Readers check
     * this <em>after</em> the active memtable and <em>before</em> SSTables.
     * Written and cleared under {@link #flushLock}; read without any lock.
     */
    private volatile Memtable immutableMemtable;

    /**
     * Ordered list of on-disk SSTables, oldest first.
     * Always an immutable list; replaced atomically (under {@link #flushLock}).
     */
    private volatile List<SSTable> sstables;

    /**
     * Current active WAL — corresponds to {@link #memtable}.
     * Guarded by {@link #flushLock} for structural changes (swap / rotate).
     */
    private WriteAheadLog wal;

    /** Serialises writes and structural mutations (flush trigger, compaction). */
    private final ReentrantLock flushLock = new ReentrantLock();

    /**
     * Signalled (under {@link #flushLock}) when a background flush finishes
     * and {@link #immutableMemtable} is cleared back to {@code null}.
     */
    private final Condition pendingFlushDone = flushLock.newCondition();

    /**
     * Single background thread for flushing.  One thread means at most one
     * immutable memtable exists at a time; the {@link #immutableMemtable} field
     * is effectively a one-slot queue.
     */
    private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "kvstore-flush");
        t.setDaemon(true);
        return t;
    });

    /** Background compaction thread. */
    private Thread compactionThread;

    /** Shared LRU block cache — one instance for all SSTables. */
    private final BlockCache blockCache = new BlockCache(1024);

    // ── Snapshot support ──────────────────────────────────────────────────────

    private final ExecutorService snapshotExecutor =
            Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "kvstore-snapshot");
                t.setDaemon(true);
                return t;
            });

    /** Epoch seconds of the last successful SAVE / BGSAVE; 0 if never. */
    private volatile long lastSnapshotEpochSec = 0;

    /** Non-null while a background snapshot is in flight; may be done. */
    private volatile Future<?> pendingSnapshot;

    // =========================================================================
    // Constructor
    // =========================================================================

    /**
     * Opens (or creates) a store rooted at {@code dataDir}.
     *
     * <ul>
     *   <li>Scans {@code dataDir} for existing {@code sst-*.db} files and opens
     *       them (corrupt files are silently skipped).</li>
     *   <li>Replays {@code wal-pending.log} first (if it exists), then
     *       {@code wal.log}, to rebuild any writes that were not yet flushed.</li>
     * </ul>
     */
    public KvStore(Path dataDir) throws IOException {
        this.dataDir = dataDir;
        Files.createDirectories(dataDir);

        // --- 1. Open existing SSTables ------------------------------------------
        // Accept both legacy "sst-*.db" names (treated as L0) and new "L*-*.db" names.
        List<SSTable> loaded = new ArrayList<>();
        for (String glob : new String[]{"sst-*.db", "L*-*.db"}) {
            try (DirectoryStream<Path> ds =
                         Files.newDirectoryStream(dataDir, glob)) {
                for (Path p : ds) {
                    try {
                        SSTable sst = SSTable.open(p);
                        sst.attachCache(blockCache);
                        loaded.add(sst);
                    } catch (IOException e) {
                        // Skip corrupt / incomplete files.
                    }
                }
            }
        }
        // Sort oldest → newest by sequence number.
        loaded.sort(Comparator.comparingInt(SSTable::sequenceNum));
        this.sstables = Collections.unmodifiableList(loaded);

        // Determine next sequence number (consider all loaded files).
        int maxSeq = loaded.stream().mapToInt(SSTable::sequenceNum).max().orElse(-1);
        this.nextSequence = maxSeq + 1;

        // --- 2. Build fresh memtable from WAL replay ----------------------------
        this.memtable = new Memtable();
        this.immutableMemtable = null;

        // Replay wal-pending.log first (older data — was being flushed at crash).
        Path pendingWalPath = dataDir.resolve(WAL_PENDING_NAME);
        if (Files.exists(pendingWalPath)) {
            try (WriteAheadLog pendingWal = new WriteAheadLog(pendingWalPath)) {
                pendingWal.replay((key, value, expiresAtMs) -> {
                    if (value == null) memtable.delete(key);
                    else              memtable.put(key, value, expiresAtMs);
                });
            }
        }

        // Replay active WAL (newer data — normal writes since last flush).
        Path walPath = dataDir.resolve(WAL_NAME);
        this.wal = new WriteAheadLog(walPath);
        wal.replay((key, value, expiresAtMs) -> {
            if (value == null) memtable.delete(key);
            else               memtable.put(key, value, expiresAtMs);
        });

        startCompactionThread();
    }

    // =========================================================================
    // Write operations
    // =========================================================================

    public void put(String key, String value) throws IOException {
        put(key, value, ValueEntry.NEVER);
    }

    public void put(String key, String value, long expiresAtMs) throws IOException {
        flushLock.lock();
        try {
            waitForFlushSlot();
            wal.appendPut(key, value, expiresAtMs);   // durable first
            memtable.put(key, value, expiresAtMs);    // then visible
            triggerFlushIfNeeded();
        } finally {
            flushLock.unlock();
        }
    }

    public void delete(String key) throws IOException {
        flushLock.lock();
        try {
            waitForFlushSlot();
            wal.appendDelete(key);
            memtable.delete(key);
            triggerFlushIfNeeded();
        } finally {
            flushLock.unlock();
        }
    }

    /**
     * Sets an expiry on an existing key. Returns {@code true} if the key existed
     * (and the expiry was set), {@code false} if the key is missing or expired.
     */
    public boolean expire(String key, long expiresAtMs) throws IOException {
        flushLock.lock();
        try {
            Optional<ValueEntry> existing = getEntry(key);
            if (existing.isEmpty() || Memtable.TOMBSTONE.equals(existing.get().value())) {
                return false;
            }
            String value = existing.get().value();
            wal.appendPut(key, value, expiresAtMs);
            memtable.put(key, value, expiresAtMs);
            triggerFlushIfNeeded();
            return true;
        } finally {
            flushLock.unlock();
        }
    }

    /**
     * Removes the TTL from an existing key. Returns {@code true} if the key had
     * a TTL that was removed, {@code false} if the key is missing, expired, or
     * had no TTL.
     */
    public boolean persist(String key) throws IOException {
        flushLock.lock();
        try {
            Optional<ValueEntry> existing = getEntry(key);
            if (existing.isEmpty() || Memtable.TOMBSTONE.equals(existing.get().value())) {
                return false;
            }
            ValueEntry e = existing.get();
            if (!e.hasTtl()) {
                return false;
            }
            String value = e.value();
            wal.appendPut(key, value, ValueEntry.NEVER);
            memtable.put(key, value, ValueEntry.NEVER);
            triggerFlushIfNeeded();
            return true;
        } finally {
            flushLock.unlock();
        }
    }

    /**
     * Returns the remaining TTL in milliseconds, {@code -1} if the key has no
     * TTL, or {@code -2} if the key is missing or expired.
     */
    public long ttl(String key) throws IOException {
        Optional<ValueEntry> e = getEntry(key);
        if (e.isEmpty()) return -2;
        long expiresAt = e.get().expiresAtMs();
        if (expiresAt == ValueEntry.NEVER) return -1;
        return Math.max(0, expiresAt - System.currentTimeMillis());
    }

    /**
     * Atomic increment-or-create. Reads the current value, parses it as a
     * 64-bit signed integer (treating missing keys as 0), adds {@code delta},
     * and writes the result back as a string. The whole operation is
     * serialized under {@link #flushLock}, so concurrent clients see
     * race-free counters.
     *
     * @return the new value after the increment
     * @throws NumberFormatException if the existing value is not a valid integer
     * @throws ArithmeticException   on signed-long overflow
     */
    public long incrementBy(String key, long delta) throws IOException {
        flushLock.lock();
        try {
            // Read current value WITHOUT releasing the lock
            long current = 0;
            Optional<String> existing = get(key);
            if (existing.isPresent()) {
                try {
                    current = Long.parseLong(existing.get());
                } catch (NumberFormatException e) {
                    throw new NumberFormatException(
                            "value is not an integer or out of range");
                }
            }
            long updated = Math.addExact(current, delta);  // throws on overflow

            // Write the new value (preserve existing TTL if any)
            String newValue = Long.toString(updated);
            // Check if existing key has TTL to preserve
            Optional<ValueEntry> existingEntry = getEntry(key);
            long expiresAtMs = existingEntry.isPresent() ? existingEntry.get().expiresAtMs() : ValueEntry.NEVER;
            wal.appendPut(key, newValue, expiresAtMs);
            memtable.put(key, newValue, expiresAtMs);
            triggerFlushIfNeeded();
            return updated;
        } finally {
            flushLock.unlock();
        }
    }

    /**
     * Atomic check-and-delete: returns {@code true} if the key existed (and
     * is now deleted), {@code false} if it was already absent.
     *
     * <p>Used by the network DEL command to count keys that actually existed,
     * without a TOCTOU race against concurrent writers.
     */
    public boolean deleteIfPresent(String key) throws IOException {
        flushLock.lock();
        try {
            boolean exists = get(key).isPresent();
            if (exists) {
                waitForFlushSlot();
                wal.appendDelete(key);
                memtable.delete(key);
                triggerFlushIfNeeded();
            }
            return exists;
        } finally {
            flushLock.unlock();
        }
    }

    // =========================================================================
    // Read operations
    // =========================================================================

    /**
     * Point lookup. Checks the memtable first, then the immutable memtable (if
     * a flush is in progress), then SSTables newest-to-oldest.
     *
     * <p>Expired entries are treated as absent.
     *
     * <p>Intentionally lock-free: takes a snapshot of each volatile field once.
     *
     * @return the value, or {@code Optional.empty()} if absent / deleted / expired
     */
    public Optional<String> get(String key) throws IOException {
        Optional<ValueEntry> entry = getEntry(key);
        if (entry.isEmpty() || Memtable.TOMBSTONE.equals(entry.get().value())) {
            return Optional.empty();
        }
        return Optional.of(entry.get().value());
    }

    /**
     * Internal point lookup returning the full {@link ValueEntry} (value + expiry).
     * Filters out expired entries. Returns {@code Optional.empty()} for missing,
     * deleted, or expired keys.
     */
    private Optional<ValueEntry> getEntry(String key) throws IOException {
        long now = System.currentTimeMillis();

        // Snapshot all volatile references once; no lock needed.
        Memtable active    = memtable;
        Memtable immutable = immutableMemtable;
        List<SSTable> tables = sstables;

        // 1. Active memtable (most recent writes).
        ValueEntry e = active.getEntry(key);
        if (e != null) {
            if (Memtable.TOMBSTONE.equals(e.value())) return Optional.empty();
            if (e.isExpired(now)) return Optional.empty();
            return Optional.of(e);
        }

        // 2. Immutable memtable (in RAM, slightly older — being flushed right now).
        if (immutable != null) {
            e = immutable.getEntry(key);
            if (e != null) {
                if (Memtable.TOMBSTONE.equals(e.value())) return Optional.empty();
                if (e.isExpired(now)) return Optional.empty();
                return Optional.of(e);
            }
        }

        // 3. SSTables newest-to-oldest.
        for (int i = tables.size() - 1; i >= 0; i--) {
            Optional<ValueEntry> found = tables.get(i).get(key);
            if (found.isPresent()) {
                ValueEntry entry = found.get();
                if (Memtable.TOMBSTONE.equals(entry.value())) return Optional.empty();
                if (entry.isExpired(now)) return Optional.empty();
                return found;
            }
        }

        return Optional.empty();
    }

    /**
     * Ordered range scan, {@code [fromInclusive, toExclusive)}, tombstones and
     * expired entries hidden.
     *
     * <p>Merges active memtable, immutable memtable (if any), and all SSTables
     * newest-to-oldest; the first writer wins (newest data takes precedence).
     */
    public Map<String, String> scan(String fromInclusive, String toExclusive)
            throws IOException {

        long now = System.currentTimeMillis();

        // Snapshot all volatile references once; no lock needed.
        Memtable active    = memtable;
        Memtable immutable = immutableMemtable;
        List<SSTable> tables = sstables;

        // Use TreeMap so result is sorted.
        TreeMap<String, ValueEntry> merged = new TreeMap<>();

        // 1. Active memtable entries (newest — populate first).
        for (Map.Entry<String, ValueEntry> e :
                active.scan(fromInclusive, toExclusive).entrySet()) {
            merged.put(e.getKey(), e.getValue());
        }

        // 2. Immutable memtable (if a flush is in progress — older than active).
        if (immutable != null) {
            for (Map.Entry<String, ValueEntry> e :
                    immutable.scan(fromInclusive, toExclusive).entrySet()) {
                merged.putIfAbsent(e.getKey(), e.getValue());
            }
        }

        // 3. SSTables newest-to-oldest; putIfAbsent keeps the newest value.
        for (int i = tables.size() - 1; i >= 0; i--) {
            for (Map.Entry<String, ValueEntry> e :
                    tables.get(i).scan(fromInclusive, toExclusive).entrySet()) {
                merged.putIfAbsent(e.getKey(), e.getValue());
            }
        }

        // 4. Filter out tombstones and expired entries; convert to String values.
        TreeMap<String, String> result = new TreeMap<>();
        for (Map.Entry<String, ValueEntry> e : merged.entrySet()) {
            ValueEntry ve = e.getValue();
            if (Memtable.TOMBSTONE.equals(ve.value())) continue;
            if (ve.isExpired(now)) continue;
            result.put(e.getKey(), ve.value());
        }

        return result;
    }

    // =========================================================================
    // Flush — trigger (fast, under lock) + background task (slow, no lock)
    // =========================================================================

    /**
     * Waits until the immutable-memtable slot is free.
     *
     * <p>Must be called while holding {@link #flushLock}.
     */
    private void waitForFlushSlot() throws IOException {
        while (immutableMemtable != null) {
            try {
                pendingFlushDone.await();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted waiting for flush slot", ie);
            }
        }
    }

    /**
     * If the active memtable has grown past the flush threshold, snapshots it
     * into the immutable slot, rotates the WAL, and submits a background flush.
     *
     * <p>Must be called while holding {@link #flushLock}.
     * Callers must have already ensured {@link #immutableMemtable} is {@code null}
     * (i.e. called {@link #waitForFlushSlot} first).
     */
    private void triggerFlushIfNeeded() throws IOException {
        if (memtable.approximateBytes() < FLUSH_THRESHOLD_BYTES) {
            return;
        }

        // Move active memtable to immutable slot.
        Memtable snapshot = memtable;
        immutableMemtable = snapshot;

        // Rotate WAL.
        wal.close();
        Files.move(
                dataDir.resolve(WAL_NAME),
                dataDir.resolve(WAL_PENDING_NAME),
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE);
        wal = new WriteAheadLog(dataDir.resolve(WAL_NAME), true);

        // Open fresh memtable for incoming writes.
        memtable = new Memtable();

        // Capture sequence number for this flush.
        final int flushSeq = nextSequence++;

        // Submit the I/O work to the background thread.
        flushExecutor.submit(() -> doBackgroundFlush(snapshot, flushSeq));
    }

    /**
     * Runs on the background flush thread.  Writes the immutable memtable to a
     * new SSTable, then — under {@link #flushLock} — installs it and clears the
     * immutable slot.
     */
    private void doBackgroundFlush(Memtable immutable, int seq) {
        try {
            // Flush outputs always go to L0.
            Path sstPath = SSTable.fileName(dataDir, 0, seq);
            SSTable.write(sstPath, immutable.entries().entrySet());
            SSTable newSst = SSTable.open(sstPath);
            newSst.attachCache(blockCache);

            flushLock.lock();
            try {
                List<SSTable> updated = new ArrayList<>(sstables);
                updated.add(newSst);
                sstables = Collections.unmodifiableList(updated);

                Files.deleteIfExists(dataDir.resolve(WAL_PENDING_NAME));

                immutableMemtable = null;
                pendingFlushDone.signalAll();
            } finally {
                flushLock.unlock();
            }

        } catch (IOException e) {
            System.err.println("[kvstore-flush] background flush failed (seq=" + seq + "): " + e);
        }
    }

    // =========================================================================
    // Compaction
    // =========================================================================

    private void startCompactionThread() {
        compactionThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(5000);
                    runCompaction();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    // Log and continue
                }
            }
        }, "kvstore-compaction");
        compactionThread.setDaemon(true);
        compactionThread.start();
    }

    /**
     * Triggers compaction immediately (used in tests and for manual compaction).
     */
    public void compactNow() throws IOException {
        runCompaction();
    }

    /**
     * Returns the maximum total bytes allowed for a given level.
     * L0 is gated by file count, not size, so we return {@link Long#MAX_VALUE}.
     */
    private static long sizeLimitForLevel(int level) {
        if (level == 0) return Long.MAX_VALUE;
        long limit = L1_TARGET_BYTES;
        for (int i = 1; i < level; i++) limit *= LEVEL_SIZE_MULTIPLIER;
        return limit;
    }

    /**
     * LevelDB-style leveled compaction.
     */
    private void runCompaction() throws IOException {
        List<SSTable> snapshot = sstables;
        if (snapshot.isEmpty()) return;

        // ── Group by level ────────────────────────────────────────────────────
        TreeMap<Integer, List<SSTable>> byLevel = new TreeMap<>();
        int maxLevel = 0;
        for (SSTable sst : snapshot) {
            int lvl = sst.level();
            byLevel.computeIfAbsent(lvl, k -> new ArrayList<>()).add(sst);
            if (lvl > maxLevel) maxLevel = lvl;
        }

        // ── Pick a level to compact ───────────────────────────────────────────
        int compactLevel = -1;
        if (byLevel.getOrDefault(0, List.of()).size() >= L0_TRIGGER_FILES) {
            compactLevel = 0;
        } else {
            for (int lvl = maxLevel; lvl >= 1; lvl--) {
                long total = byLevel.getOrDefault(lvl, List.of()).stream()
                        .mapToLong(s -> {
                            try { return s.fileSize(); } catch (IOException e) { return 0L; }
                        })
                        .sum();
                if (total > sizeLimitForLevel(lvl)) {
                    compactLevel = lvl;
                    break;
                }
            }
        }
        if (compactLevel < 0) return; // nothing to do

        int outputLevel = compactLevel + 1;

        // ── Choose inputs ─────────────────────────────────────────────────────
        List<SSTable> inputs = new ArrayList<>();
        List<SSTable> currentLevel = new ArrayList<>(
                byLevel.getOrDefault(compactLevel, List.of()));
        currentLevel.sort(Comparator.comparingInt(SSTable::sequenceNum));

        String inputMinKey, inputMaxKey;
        if (compactLevel == 0) {
            inputs.addAll(currentLevel);
            if (inputs.isEmpty()) return;
            inputMinKey = inputs.stream()
                    .filter(s -> !s.keySet().isEmpty())
                    .map(s -> s.keySet().first())
                    .min(Comparator.naturalOrder()).orElse(null);
            inputMaxKey = inputs.stream()
                    .filter(s -> !s.keySet().isEmpty())
                    .map(s -> s.keySet().last())
                    .max(Comparator.naturalOrder()).orElse(null);
        } else {
            SSTable picked = currentLevel.get(0);
            NavigableSet<String> keys = picked.keySet();
            if (keys.isEmpty()) return;
            inputs.add(picked);
            inputMinKey = keys.first();
            inputMaxKey = keys.last();
        }
        if (inputMinKey == null) return;

        // Add all overlapping SSTables from outputLevel.
        for (SSTable s : byLevel.getOrDefault(outputLevel, List.of())) {
            NavigableSet<String> keys = s.keySet();
            if (keys.isEmpty()) continue;
            String smin = keys.first();
            String smax = keys.last();
            if (!(smax.compareTo(inputMinKey) < 0 || smin.compareTo(inputMaxKey) > 0)) {
                inputs.add(s);
            }
        }

        // ── Tombstone-drop safety ─────────────────────────────────────────────
        boolean canDropTombstones = true;
        for (int lvl = outputLevel + 1; lvl <= maxLevel; lvl++) {
            if (!byLevel.getOrDefault(lvl, List.of()).isEmpty()) {
                canDropTombstones = false;
                break;
            }
        }

        // ── K-way merge (newest sequence wins per key) ────────────────────────
        long now = System.currentTimeMillis();
        TreeMap<String, ValueEntry> merged = new TreeMap<>();
        List<SSTable> sortedByAge = new ArrayList<>(inputs);
        sortedByAge.sort(Comparator.comparingInt(SSTable::sequenceNum).reversed());
        for (SSTable s : sortedByAge) {
            NavigableSet<String> keys = s.keySet();
            if (keys.isEmpty()) continue;
            NavigableMap<String, ValueEntry> all = s.scan(keys.first(), keys.last() + "\0");
            for (Map.Entry<String, ValueEntry> e : all.entrySet()) {
                merged.putIfAbsent(e.getKey(), e.getValue());
            }
        }

        if (canDropTombstones) {
            // Drop tombstones and expired entries when it's safe to do so.
            merged.entrySet().removeIf(e ->
                    Memtable.TOMBSTONE.equals(e.getValue().value())
                    || e.getValue().isExpired(now));
        }

        // ── Split output into bounded-size SSTables ───────────────────────────
        List<List<Map.Entry<String, ValueEntry>>> chunks = new ArrayList<>();
        List<Map.Entry<String, ValueEntry>> chunk = new ArrayList<>();
        long chunkBytes = 0L;
        for (Map.Entry<String, ValueEntry> e : merged.entrySet()) {
            chunk.add(e);
            chunkBytes += e.getKey().length() + e.getValue().value().length() + 18; // +8 for expiry long
            if (chunkBytes >= TARGET_SSTABLE_BYTES) {
                chunks.add(chunk);
                chunk = new ArrayList<>();
                chunkBytes = 0L;
            }
        }
        if (!chunk.isEmpty()) chunks.add(chunk);

        // ── Allocate sequence numbers (under lock, but do disk I/O outside) ───
        int firstSeq;
        flushLock.lock();
        try {
            firstSeq = nextSequence;
            nextSequence += Math.max(1, chunks.size());
        } finally {
            flushLock.unlock();
        }

        // ── Write output SSTables (no lock during disk I/O) ───────────────────
        List<SSTable> newSstables = new ArrayList<>();
        List<Path>    newPaths    = new ArrayList<>();
        try {
            if (chunks.isEmpty()) {
                // All entries were tombstones/expired and got dropped — nothing to write.
            } else {
                for (int i = 0; i < chunks.size(); i++) {
                    Path outPath = SSTable.fileName(dataDir, outputLevel, firstSeq + i);
                    SSTable.write(outPath, chunks.get(i));
                    SSTable opened = SSTable.open(outPath);
                    opened.attachCache(blockCache);
                    newSstables.add(opened);
                    newPaths.add(outPath);
                }
            }
        } catch (IOException e) {
            for (SSTable s : newSstables) {
                try { s.close(); } catch (IOException ignored) {}
            }
            for (Path p : newPaths) {
                try { Files.deleteIfExists(p); } catch (IOException ignored) {}
            }
            throw e;
        }

        // ── Atomic swap under lock ─────────────────────────────────────────────
        flushLock.lock();
        try {
            List<SSTable> updated = new ArrayList<>(sstables);
            for (SSTable old : inputs) {
                updated.remove(old);
                try { old.close(); } catch (IOException ignored) {}
                try { Files.deleteIfExists(old.path()); } catch (IOException ignored) {}
            }
            updated.addAll(newSstables);
            updated.sort(Comparator.comparingInt(SSTable::sequenceNum));
            sstables = Collections.unmodifiableList(updated);
        } finally {
            flushLock.unlock();
        }
    }

    // =========================================================================
    // Server accessors
    // =========================================================================

    /** @return snapshot of currently-loaded SSTables (newest last). */
    public java.util.List<SSTable> sstableSnapshot() {
        return sstables;   // already an unmodifiable list
    }

    /** @return current active memtable size in approximate bytes. */
    public long memtableApproximateBytes() {
        return memtable.approximateBytes();
    }

    /** @return the shared block cache used by all SSTables in this store. */
    public BlockCache blockCache() { return blockCache; }

    // =========================================================================
    // Snapshot — SAVE / BGSAVE / LASTSAVE
    // =========================================================================

    /**
     * Synchronously writes all current data to a single sorted snapshot file
     * at {@code dataDir/snapshot-<epochMs>.db}. Returns the path written.
     */
    public Path snapshot() throws IOException {
        long ts = System.currentTimeMillis();
        Path target = dataDir.resolve(String.format("snapshot-%013d.db", ts));
        writeSnapshotTo(target);
        lastSnapshotEpochSec = ts / 1000;
        return target;
    }

    /**
     * Triggers a background snapshot. Returns immediately. Subsequent calls
     * while a snapshot is in flight return a Future for the existing one.
     */
    public Future<?> bgsnapshot() {
        Future<?> existing = pendingSnapshot;
        if (existing != null && !existing.isDone()) return existing;
        pendingSnapshot = snapshotExecutor.submit(() -> {
            try { snapshot(); } catch (IOException e) {
                System.err.println("BGSAVE failed: " + e);
            }
        });
        return pendingSnapshot;
    }

    /** @return epoch seconds of last successful snapshot, or 0 if never. */
    public long lastSnapshotEpochSec() { return lastSnapshotEpochSec; }

    /**
     * Merges all data layers (active memtable, immutable memtable, SSTables)
     * into a single sorted, tombstone-free, non-expired snapshot file.
     */
    private void writeSnapshotTo(Path target) throws IOException {
        TreeMap<String, ValueEntry> merged = new TreeMap<>();
        long now = System.currentTimeMillis();

        // Capture snapshots of state under flushLock to get a consistent view.
        Memtable activeSnap;
        Memtable immutableSnap;
        List<SSTable> ssts;
        flushLock.lock();
        try {
            activeSnap    = memtable;
            immutableSnap = immutableMemtable;
            ssts          = sstables;
        } finally {
            flushLock.unlock();
        }

        // 1. Active memtable (newest — wins over everything).
        for (Map.Entry<String, ValueEntry> e : activeSnap.entries().entrySet()) {
            merged.put(e.getKey(), e.getValue());
        }
        // 2. Immutable memtable (older than active, newer than SSTables).
        if (immutableSnap != null) {
            for (Map.Entry<String, ValueEntry> e : immutableSnap.entries().entrySet()) {
                merged.putIfAbsent(e.getKey(), e.getValue());
            }
        }
        // 3. SSTables newest-to-oldest; putIfAbsent so newer sources win.
        List<SSTable> sortedByAge = new ArrayList<>(ssts);
        sortedByAge.sort(Comparator.comparingInt(SSTable::sequenceNum).reversed());
        for (SSTable s : sortedByAge) {
            NavigableSet<String> keys = s.keySet();
            if (keys.isEmpty()) continue;
            NavigableMap<String, ValueEntry> all = s.scan(keys.first(), keys.last() + "\0");
            for (Map.Entry<String, ValueEntry> e : all.entrySet()) {
                merged.putIfAbsent(e.getKey(), e.getValue());
            }
        }

        // Filter: drop tombstones and expired entries.
        merged.entrySet().removeIf(e ->
                Memtable.TOMBSTONE.equals(e.getValue().value())
                || e.getValue().isExpired(now));

        // Write using the SSTable format (self-contained sorted file).
        SSTable.write(target, merged.entrySet());
    }

    // =========================================================================
    // AutoCloseable
    // =========================================================================

    /**
     * Closes the store cleanly.
     */
    @Override
    public void close() throws IOException {
        if (compactionThread != null) {
            compactionThread.interrupt();
        }

        // Drain the snapshot executor BEFORE acquiring flushLock.
        // The snapshot background task acquires flushLock internally, so
        // awaiting termination while holding the lock would deadlock.
        snapshotExecutor.shutdown();
        try {
            snapshotExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }

        flushLock.lock();
        try {
            while (immutableMemtable != null) {
                try {
                    pendingFlushDone.await();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            flushExecutor.shutdown();
            try {
                flushExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }

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
