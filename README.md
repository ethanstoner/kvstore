# kvstore

A persistent key-value store with an **LSM-tree storage engine**, written from scratch in Java 21 — no database libraries. The same architecture used by LevelDB, RocksDB, and the storage layer of Cassandra.

[![CI](https://github.com/ethanstoner/kvstore/actions/workflows/ci.yml/badge.svg)](https://github.com/ethanstoner/kvstore/actions/workflows/ci.yml)

```bash
$ java -jar kvstore.jar put user:42 Ethan
OK
$ java -jar kvstore.jar get user:42
Ethan
# kill the process, run again — the value is still there
```

## Why this project exists

The public API is three functions (`put`, `get`, `delete`). All the engineering is in the internals — and those internals *are* the core CS fundamentals: balanced/sorted structures, binary search, merge algorithms, amortized complexity, file I/O, concurrency, and crash recovery.

## Architecture

```
        put(k, v) / delete(k)
                │
                ▼
   ┌────────────────────────┐   1. append (durable, fsync'd)
   │   Write-Ahead Log      │
   │   (append-only file)   │
   └────────────────────────┘
                │ replay on startup
                ▼
   ┌────────────────────────┐   2. apply (fast, in RAM)
   │   Memtable             │   ConcurrentSkipListMap
   │   (sorted, in-memory)  │   — O(log n), keeps keys ordered
   └────────────────────────┘
                │ flush when > 4 MB
                ▼
   ┌────────────────────────┐   3. immutable sorted runs
   │   SSTables on disk     │   binary format + bloom filter
   │   + in-memory index    │   per file for fast lookups
   └────────────────────────┘
                │ merge when 4+ same-size
                ▼
   ┌────────────────────────┐
   │   Compacted SSTables   │   k-way merged, tombstones dropped
   │   (background thread)  │   size-tiered compaction strategy
   └────────────────────────┘
```

Every write is logged to disk **before** it is applied in memory — that ordering is what makes a crash survivable. When the memtable exceeds 4 MB, it is flushed to an immutable SSTable on disk. A background thread merges SSTables of similar size to bound read amplification and reclaim space from deleted keys.

## Complexity

| Operation | Cost | Why |
|-----------|------|-----|
| `put` / `delete` | O(log n) + O(1) amortized | sorted skip-list insert + sequential WAL append |
| `get` | O(log n) per level | memtable → SSTables newest-first, bloom filter skips irrelevant files |
| `scan(from, to)` | O(log n + k) per level | sub-map lookups merged across levels, k = results |
| Flush | O(m) | sequential write of m memtable entries to sorted SSTable |
| Compaction | O(m) | k-way merge of m total entries across selected SSTables |
| Recovery | O(m) | replay m logged WAL records + open existing SSTables |

## Features

- **Durable writes** — write-ahead log with fsync; data survives crashes
- **SSTable flush** — memtable spills to immutable sorted files when full
- **Multi-level reads** — get/scan falls through memtable → newest SSTable → older ones
- **Bloom filters** — probabilistic filter per SSTable skips files that can't contain a key (~1% false positive rate)
- **Size-tiered compaction** — background thread merges SSTables of similar size, drops tombstones
- **Crash recovery** — replays WAL on startup, skips corrupt SSTable files
- **Range scans** — ordered `[from, to)` scans merged across all levels
- **JMH benchmarks** — measure write/read/scan throughput

## Build & run

Requires JDK 21+ and Maven.

```bash
mvn verify                    # compile + run the full test suite
mvn package                   # build target/kvstore-0.1.0.jar
java -jar target/kvstore-0.1.0.jar put hello world
java -jar target/kvstore-0.1.0.jar get hello
java -jar target/kvstore-0.1.0.jar scan a z
java -jar target/kvstore-0.1.0.jar del hello
```

## Benchmarks

```bash
mvn package
java -jar target/kvstore-0.1.0-benchmarks.jar           # run all benchmarks
java -jar target/kvstore-0.1.0-benchmarks.jar ".*Read.*" # run only read benchmarks
```

Benchmarks measured with JMH (Java Microbenchmark Harness):

| Benchmark | What it measures |
|-----------|-----------------|
| `sequentialWrite` | Sequential key inserts (best case for LSM) |
| `randomWrite` | Random key inserts/updates |
| `pointReadExistingKey` | Reads of keys known to exist |
| `pointReadMissingKey` | Reads of keys that don't exist (bloom filter path) |
| `scanRange` | Range scans of ~100 keys |

## Layout

```
src/main/java/com/ethanstoner/kvstore/
  Memtable.java          sorted in-memory buffer (skip list)
  WriteAheadLog.java     append-only durability + replay
  SSTable.java           immutable sorted file (binary format + bloom filter + index)
  BloomFilter.java       probabilistic set membership (MurmurHash3)
  KvStore.java           ties it together; public API + flush + compaction
  cli/Main.java          put / get / del / scan command line
  benchmark/             JMH throughput benchmarks
src/test/java/...        JUnit 5 suite (memtable, WAL, SSTable, bloom filter, recovery, compaction)
.github/workflows/       CI
```

## Roadmap

Potential future work:

- **Leveled compaction** — non-overlapping key ranges per level for better read amplification
- **Block cache** — LRU cache for frequently-read SSTable blocks
- **Concurrent flush** — immutable memtable during flush so writes don't stall
- **WAL checksums** — detect partial/corrupt WAL entries on replay
- **Compression** — Snappy or LZ4 per SSTable block

## License

[MIT](LICENSE)
