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
  Write-Ahead Log        1. append (durable)
  (append-only file)
        |
        v
  Memtable                2. apply (fast, in RAM)
  (ConcurrentSkipListMap)
        | flush when > 4 MB
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

Binary, sequential layout:

```
[Data Section — entries sorted by key]
  Each entry:
    op:       1 byte    (0 = TOMBSTONE, 1 = VALUE)
    keyLen:   2 bytes   (unsigned short)
    key:      keyLen bytes (UTF-8)
    valueLen: 4 bytes   (only if op = VALUE)
    value:    valueLen bytes (only if op = VALUE)

[Bloom Filter Section]
    bitArrayLen: 4 bytes
    bits:        bitArrayLen bytes
    hashCount:   4 bytes

[Index Section — one entry per key]
  Each entry:
    keyLen: 2 bytes
    key:    keyLen bytes (UTF-8)
    offset: 8 bytes (position in data section)

[Footer — fixed 20 bytes, read first on open]
    bloomOffset: 8 bytes
    indexOffset: 8 bytes
    entryCount:  4 bytes
```

### Opening an SSTable

1. Seek to end - 20, read footer
2. Seek to bloomOffset, deserialize BloomFilter
3. Seek to indexOffset, read all index entries into a `TreeMap<String, Long>`
4. Keys live in memory; values stay on disk, read on demand via seek

### SSTable Naming

Sequential numbering: `sst-000001.db`, `sst-000002.db`, etc. Higher number = newer. Next sequence number determined by scanning the data directory on startup.

## Memtable Flush

Triggered synchronously when a `put()` pushes the memtable past the 4 MB threshold:

1. Write all memtable entries (already sorted) to a new SSTable file
2. Clear the memtable, reset `approximateBytes` to 0
3. Delete the old WAL file
4. Create a fresh WAL for the new empty memtable

Flush blocks writes briefly. Acceptable for a clarity-first design.

## Multi-Level Reads

### Point read: `get(key)`

1. Check memtable — if found (value or tombstone), return immediately
2. Iterate SSTables newest-to-oldest:
   a. Check bloom filter — skip if "definitely not here"
   b. Check in-memory index — skip if key not present
   c. Seek to offset, read value from disk
   d. If tombstone, return empty (key was deleted)
   e. If value, return it
3. If no SSTable has the key, return empty

### Range scan: `scan(from, to)`

Merge across memtable + all SSTables:
- Use a priority queue (min-heap) of iterators, one per source
- When multiple sources have the same key, take the value from the newest source
- Hide tombstones from the result

## Size-Tiered Compaction

A background daemon thread wakes periodically (e.g., every 5 seconds):

1. Group SSTables into size tiers (e.g., by order of magnitude of file size)
2. If any tier has 4 or more SSTables, select that tier for compaction
3. K-way merge the selected SSTables into a single new SSTable:
   - For duplicate keys, keep only the newest value
   - Drop tombstones only if no older SSTable outside this compaction could contain the key
4. Delete the old SSTable files
5. Add the new merged SSTable to the active list

### Crash safety during compaction

If the process crashes mid-compaction:
- The new merged SSTable may or may not exist
- The old SSTables may or may not be deleted
- On restart, directory scan picks up whatever files exist
- Duplicate data is harmless — newest-first ordering ensures correct reads
- Orphaned partial files can be detected by attempting to read the footer

## Bloom Filter

Probabilistic set membership test embedded in each SSTable.

### Parameters

- Target false positive rate: ~1%
- For N keys: bit array size = `ceil(-N * ln(0.01) / (ln(2)^2))` bits (~10N)
- Optimal hash count k = `ceil(bitArraySize / N * ln(2))` (~7)

### Hash strategy

Double hashing: `h(i) = (h1 + i * h2) mod bitArraySize`
- h1 = `murmurHash3(key)` (or similar quality hash)
- h2 = derived by bit-shifting h1

### API

```java
BloomFilter(int expectedInsertions, double falsePositiveRate)
void add(String key)
boolean mightContain(String key)
byte[] toBytes()
static BloomFilter fromBytes(byte[] data)
```

### Integration

- SSTable writer: builds a BloomFilter, adds every key, serializes into the file
- SSTable reader: deserializes the BloomFilter on open
- KvStore.get(): checks `bloom.mightContain(key)` before checking the index

## Startup Recovery

1. Scan data directory for all `sst-*.db` files
2. Sort by sequence number (ascending = oldest first; list is searched newest first)
3. Open each SSTable (read footer, bloom filter, index)
4. Open/create `wal.log`, replay into a fresh memtable
5. Determine next SSTable sequence number = max existing + 1
6. Ready to serve reads and writes

## JMH Benchmarks

Measure the engine under realistic workloads:

- **Sequential write throughput** — best case for LSM (append-friendly)
- **Random write throughput** — realistic workload
- **Point read throughput** — existing keys and missing keys separately
- **Scan throughput** — range queries
- **Mixed read/write** — 80% reads / 20% writes
- **Bloom filter impact** — before/after comparison

## Test Plan

### New test classes

- `SSTableTest` — write entries, read back, sorted order, tombstone handling, empty table, large entries
- `BloomFilterTest` — zero false negatives, bounded false positive rate, serialization round-trip

### Expanded existing tests

- `KvStoreTest` — flush-triggered scenarios (write more than 4 MB, verify data survives), recovery across SSTables + WAL, compaction correctness, scan across multiple SSTables, delete + compaction drops tombstone

## File Layout

```
src/main/java/com/ethanstoner/kvstore/
  Memtable.java           (existing, unchanged)
  WriteAheadLog.java       (existing, minor changes for reset)
  KvStore.java             (existing, major expansion: flush, multi-level reads, compaction)
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
- WAL: `synchronized` append methods (existing)
- Flush: synchronized on KvStore, blocks writes briefly
- Compaction: background daemon thread, acquires a lock to swap the SSTable list
- SSTable readers: immutable after construction, thread-safe for reads
- BloomFilter: immutable after construction, thread-safe
