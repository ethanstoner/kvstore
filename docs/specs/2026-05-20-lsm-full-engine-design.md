# LSM-Tree Full Engine Design

**Date:** 2026-05-20
**Status:** Approved
**Approach:** Clarity-first (Approach A)

## Overview

Extend the existing memtable + WAL kvstore into a complete LSM-tree storage engine with SSTable flush, multi-level reads, size-tiered compaction, bloom filters, and JMH benchmarks.

## Architecture

```
put(k, v) / delete(k)
        |
        v
  Write-Ahead Log        1. append (durable, fsync'd)
  (append-only file)
        |
        v
  Memtable                2. apply (fast, in RAM)
  (ConcurrentSkipListMap)
        | flush when > 4 MB (approximateBytes)
        v
  SSTables on disk        3. immutable sorted runs
  + bloom filter per file
        | merge when 4+ same-size
        v
  Compacted SSTables      4. merged, tombstones dropped
```

## New Classes

| Class | Purpose |
|-------|---------|
| `SSTable` | Immutable sorted file on disk. Writer writes entries sequentially; reader uses in-memory key-to-offset index for lookups. |
| `BloomFilter` | Probabilistic set membership. Avoids reading SSTables that cannot contain a key. |

## SSTable File Format

All multi-byte integers are big-endian (matching `DataOutputStream`/`DataInputStream`). Max key size: 65,535 UTF-8 bytes (matching the 2-byte length prefix).

Binary, sequential layout:

```
[Data Section — entries sorted by key, offsets are absolute from file start]
  Each entry:
    op:       1 byte    (0 = TOMBSTONE, 1 = VALUE)
    keyLen:   2 bytes   (unsigned short, big-endian)
    key:      keyLen bytes (UTF-8)
    valueLen: 4 bytes   (big-endian, only if op = VALUE)
    value:    valueLen bytes (only if op = VALUE)

[Bloom Filter Section]
    bitArrayLen: 4 bytes (big-endian)
    bits:        bitArrayLen bytes
    hashCount:   4 bytes (big-endian)

[Index Section — one entry per key]
  Each entry:
    keyLen: 2 bytes (big-endian)
    key:    keyLen bytes (UTF-8)
    offset: 8 bytes (absolute file position of the data entry, big-endian)

[Footer — fixed 24 bytes, read first on open]
    bloomOffset: 8 bytes (big-endian)
    indexOffset: 8 bytes (big-endian)
    entryCount:  4 bytes (big-endian)
    magic:       4 bytes (0x4B565354 = "KVST", used for corruption detection)
```

An SSTable with zero entries is never written — flush and compaction skip writing if there are no entries to emit.

### Reading an entry at a given offset

1. Seek to offset
2. Read 1 byte `op`
3. Read 2-byte `keyLen`, then `keyLen` bytes for the key
4. If `op == 1` (VALUE): read 4-byte `valueLen`, then `valueLen` bytes for the value
5. If `op == 0` (TOMBSTONE): no value bytes follow; return a tombstone marker

### Opening an SSTable

1. Seek to end - 24, read footer. Validate magic = `0x4B565354`; if not, the file is corrupt — log a warning and skip it.
2. Seek to bloomOffset, deserialize BloomFilter
3. Seek to indexOffset, read all index entries into a `TreeMap<String, Long>`
4. Keys live in memory; values stay on disk, read on demand via seek

### SSTable Naming

Sequential numbering: `sst-000001.db`, `sst-000002.db`, etc. Higher number = newer. Next sequence number determined by scanning the data directory on startup. Gaps in sequence numbers are silently accepted (caused by crash during compaction cleanup).

## Memtable Flush

The 4 MB threshold is measured in `Memtable.approximateBytes()` — the existing counter that estimates `key.length() * 2 + value.length() * 2` per entry. This is an intentional overestimate for ASCII data; exact measurement is not required.

Triggered synchronously when a `put()` pushes the memtable past the threshold. The flush sequence is ordered for crash safety:

1. Write all memtable entries (already sorted) to a new SSTable file. **Fsync the SSTable file** to guarantee it is durable on disk before proceeding.
2. Create and open a **new** WAL file (same path: `wal.log`). This requires closing the old `WriteAheadLog` instance first.
3. Replace the memtable instance with a fresh empty `Memtable` (do not add a `clear()` method — just swap the reference).
4. Delete the old WAL file. Since step 2 already created the new WAL at the same path via close-then-reopen, this step is effectively a no-op — the old WAL's contents were overwritten when the new `WriteAheadLog` constructor opened the file in non-append mode (truncate).

**Crash during flush:** If the process crashes after step 1 but before step 2-4, on restart both the SSTable and the old WAL exist. The WAL is replayed into the memtable, producing duplicate keys that also exist in the SSTable. This is correct — reads check memtable first, so the memtable copy wins, and the values are identical. The inflated `approximateBytes` may trigger an immediate re-flush on the next write; this is acceptable.

### Changes to existing classes

- `Memtable.java`: add a `clear()` method is NOT needed — KvStore swaps the memtable reference.
- `WriteAheadLog.java`: add a `reset()` class method or constructor mode that truncates (not appends) for use after flush. Alternatively, close the old WAL and construct a new one with `append=false`.

## Multi-Level Reads

### Point read: `get(key)`

1. Check memtable:
   - If `memtable.get(key)` returns a non-null value: if it equals `TOMBSTONE`, return `Optional.empty()`; otherwise return the value. **Do not fall through to SSTables.**
   - If `memtable.get(key)` returns `null`: key is not in memtable, fall through.
2. Iterate SSTables newest-to-oldest:
   a. Check bloom filter — if `mightContain(key)` returns false, skip this SSTable
   b. Check in-memory index — if key is not in the TreeMap, skip
   c. Seek to the offset from the index, read the entry (see "Reading an entry at a given offset")
   d. If tombstone, return `Optional.empty()` — do not check older SSTables
   e. If value, return `Optional.of(value)`
3. If no SSTable has the key, return `Optional.empty()`

### Range scan: `scan(from, to)`

Merge across memtable + all SSTables using a priority queue:

- Each source (memtable, SSTable_0, SSTable_1, ...) provides an iterator over entries in `[from, to)` sorted by key
- SSTable iterators read lazily from disk: seek to the first key >= `from` in the index, then read entries sequentially until key >= `to`
- Priority queue comparator: **(key ascending, source age ascending)** where lower source index = newer. This means for duplicate keys, the newest source is polled first.
- When polling from the queue: peek at the current minimum key. Drain all entries with that same key from all iterators (advance each iterator past that key). Among the drained entries, take the one from the newest source.
- If the winning entry is a tombstone, skip the key entirely (do not emit it). If it is a value, emit it.
- Continue until all iterators are exhausted.

## Size-Tiered Compaction

A background daemon thread wakes every 5 seconds:

1. Compute the file size of each SSTable. Group by size tier: tier = `floor(log10(fileSize))`. (On restart, tiers are recomputed from file sizes — no manifest file needed.)
2. If any tier has 4 or more SSTables, select that tier for compaction.
3. K-way merge the selected SSTables into a single new SSTable:
   - For duplicate keys, keep only the value from the newest SSTable (highest sequence number)
   - **Tombstone rule:** drop a tombstone only if *every* SSTable with a lower sequence number than this tombstone's source is included in this compaction. In other words, tombstones are only dropped during a "full compaction" that includes all SSTables at or below the tombstone's origin. Conservative but correct — tombstones may linger until a full compaction occurs.
4. If the merged result has zero entries (all tombstones were dropped), do not write an SSTable.
5. Fsync the new SSTable file.
6. Acquire the SSTable list lock. Atomically replace the old SSTables with the new one in the active list. Release the lock.
7. Delete the old SSTable files.

### Concurrency: SSTable list protection

The active SSTable list is a `volatile` reference to an immutable `List<SSTable>`. Readers (get/scan) read the reference once and iterate over the snapshot — no lock needed. Writers (flush/compaction) synchronize on a dedicated `Object flushLock` to swap the reference. This avoids blocking reads during compaction.

### Crash safety during compaction

If the process crashes mid-compaction:
- The new merged SSTable may or may not exist on disk
- The old SSTables may or may not be deleted
- On restart, directory scan picks up whatever `.db` files exist. Files with an invalid magic number in the footer are skipped (corrupt/partial writes).
- Duplicate data across SSTables is harmless — newest-first ordering ensures correct reads

## Bloom Filter

Probabilistic set membership test embedded in each SSTable.

### Parameters

- Target false positive rate: ~1%
- For N keys (N must be >= 1): bit array size = `ceil(-N * ln(0.01) / (ln(2)^2))` bits (~10N)
- Optimal hash count k = `ceil((bitArraySize / N) * ln(2))` (~7)

### Hash strategy

Double hashing: `h(i) = (h1 + i * h2) mod bitArraySize` for i in 0..k-1
- h1 = lower 32 bits of `murmurHash3_128(key)` (implemented inline, no external dependency)
- h2 = upper 32 bits of `murmurHash3_128(key)`

### Serialization

The BloomFilter is serialized directly into the SSTable file stream (not via a separate `byte[]` round-trip):
- **Write:** `writeTo(DataOutputStream out)` — writes bitArrayLen, bits, hashCount
- **Read:** `static BloomFilter readFrom(DataInputStream in)` — reads the same fields

### Integration

- SSTable writer: builds a BloomFilter, adds every key, serializes into the file
- SSTable reader: deserializes the BloomFilter on open
- KvStore.get(): checks `bloom.mightContain(key)` before checking the index

## Startup Recovery

1. Scan data directory for all `sst-*.db` files
2. Attempt to open each one (read footer, validate magic, load bloom filter and index). Skip files with invalid magic (corrupt partial writes from crashes).
3. Sort open SSTables by sequence number (ascending = oldest first; list is searched newest-first)
4. Open/create `wal.log`, replay into a fresh memtable. If SSTables exist and the WAL contains overlapping keys (crash-after-flush), the memtable will contain duplicates — this is correct and handled by read ordering.
5. Determine next SSTable sequence number = max existing + 1 (gaps in numbering are OK)
6. Start the compaction background thread
7. Ready to serve reads and writes

## JMH Benchmarks

Measure the engine under realistic workloads:

- **Sequential write throughput** — best case for LSM (append-friendly)
- **Random write throughput** — realistic workload
- **Point read throughput** — existing keys and missing keys separately
- **Scan throughput** — range queries
- **Mixed read/write** — 80% reads / 20% writes
- **Bloom filter impact** — before/after comparison

Requires adding `jmh-core` and `jmh-generator-annprocess` dependencies to `pom.xml`, plus the `maven-shade-plugin` to produce a benchmarks jar.

## Test Plan

### New test classes

- `SSTableTest` — write entries, read back, sorted order, tombstone handling, large entries, corrupt file detection (bad magic)
- `BloomFilterTest` — zero false negatives, bounded false positive rate, serialization round-trip, edge case N=1

### Expanded existing tests

- `KvStoreTest` — flush-triggered scenarios (write more than 4 MB, verify data survives in SSTable), recovery across SSTables + WAL, compaction correctness (duplicate keys merged, tombstones dropped in full compaction), scan across multiple SSTables, multi-level get falls through memtable to SSTable, bloom filter skip verified

## File Layout

```
src/main/java/com/ethanstoner/kvstore/
  Memtable.java           (existing, unchanged)
  WriteAheadLog.java       (existing, add truncate-mode constructor for post-flush reset)
  KvStore.java             (existing, major expansion: flush, multi-level reads, compaction thread)
  SSTable.java             (new)
  BloomFilter.java         (new)
  cli/Main.java            (existing, unchanged)
src/test/java/com/ethanstoner/kvstore/
  MemtableTest.java        (existing, unchanged)
  WriteAheadLogTest.java   (existing, unchanged)
  KvStoreTest.java         (existing, expanded)
  SSTableTest.java         (new)
  BloomFilterTest.java     (new)
src/main/java/com/ethanstoner/kvstore/benchmark/
  KvStoreBenchmark.java    (new, JMH)
```

## Concurrency Model

- Memtable: `ConcurrentSkipListMap` handles concurrent reads safely
- WAL: `synchronized` append methods (existing), `FileDescriptor.sync()` added for true fsync
- Flush: synchronized on `flushLock`, blocks writes briefly, swaps memtable + WAL references
- Compaction: background daemon thread, synchronizes on `flushLock` to swap SSTable list
- SSTable list: `volatile` immutable `List<SSTable>` reference — readers snapshot, writers swap under lock
- SSTable readers: immutable after construction, thread-safe for reads
- BloomFilter: immutable after construction, thread-safe
