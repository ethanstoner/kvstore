# kvstore

A persistent key-value store with an **LSM-tree storage engine**, written from scratch in Java 21 — no database libraries. The same architecture used by LevelDB, RocksDB, and the storage layer of Cassandra.

[![CI](https://github.com/ethanstoner/kvstore/actions/workflows/ci.yml/badge.svg)](https://github.com/ethanstoner/kvstore/actions/workflows/ci.yml)

```bash
$ java -jar kvstore.jar put user:42 Ethan
OK
$ java -jar kvstore.jar get user:42
Ethan
# kill the process, run again — the value is still there (rebuilt from the WAL)
```

## Why this project exists

The public API is three functions (`put`, `get`, `delete`). All the engineering is in the internals — and those internals *are* the core CS fundamentals: balanced/sorted structures, binary search, merge algorithms, amortized complexity, file I/O, concurrency, and crash recovery.

## Architecture

```
        put(k, v) / delete(k)
                │
                ▼
   ┌────────────────────────┐   1. append (durable)
   │   Write-Ahead Log      │ ◄─────────────────────┐
   │   (append-only file)   │                       │
   └────────────────────────┘                       │
                │ replay on startup                  │
                ▼                                    │
   ┌────────────────────────┐   2. apply (fast, in RAM)
   │   Memtable             │   ConcurrentSkipListMap
   │   (sorted, in-memory)  │   — O(log n), keeps keys ordered
   └────────────────────────┘
                │  (roadmap) flush when full
                ▼
   ┌────────────────────────┐
   │   SSTables on disk      │   immutable, sorted runs
   │   + background compaction│   merged + tombstones dropped
   └────────────────────────┘
```

Every write is logged to disk **before** it is applied in memory — that ordering is what makes a crash survivable. On startup the log is replayed to rebuild the exact prior state.

## Complexity

| Operation | Cost | Why |
|-----------|------|-----|
| `put` / `delete` | O(log n) memtable insert + O(1) amortized WAL append | sorted skip-list insert; sequential file write |
| `get` (current) | O(log n) | single sorted memtable lookup |
| `get` (after SSTables land) | O(log n) per level, newest-first | memtable → SSTables, short-circuits on first hit |
| `scan(from, to)` | O(log n + k) | sub-map over a sorted structure, k = results |
| Recovery on startup | O(m) | replay m logged records |

## Build & run

Requires JDK 21+ and Maven.

```bash
mvn verify                    # compile + run the full test suite
mvn package                   # build target/kvstore-0.1.0.jar
java -jar target/kvstore-0.1.0.jar put hello world
java -jar target/kvstore-0.1.0.jar get hello
java -jar target/kvstore-0.1.0.jar scan a z
```

CI (GitHub Actions) runs `mvn verify` on every push — see the badge above.

## Status

**Working today:** durable `put`/`get`/`delete`/`scan`, write-ahead log, full crash-recovery (data survives a restart), JUnit 5 test suite, CI.

**Roadmap — the LSM pieces, implemented as a learning path** (each is a self-contained next step; see the `// Roadmap` comments in `KvStore.java`):

1. **SSTable flush** — when the memtable exceeds a size threshold, write it out as an immutable sorted file and start a fresh memtable + WAL.
2. **Multi-level reads** — `get` falls through memtable → newest SSTable → older ones.
3. **Compaction** — a background thread merges SSTables, dropping shadowed values and tombstones (k-way merge of sorted runs).
4. **Bloom filters** — skip SSTables that provably can't contain a key.
5. **JMH benchmarks** — measure write/read throughput before vs. after each step.

## Layout

```
src/main/java/com/ethanstoner/kvstore/
  Memtable.java        sorted in-memory buffer (skip list)
  WriteAheadLog.java   append-only durability + replay
  KvStore.java         ties it together; public API
  cli/Main.java        put / get / del / scan command line
src/test/java/...      JUnit 5 suite (memtable, WAL, recovery)
.github/workflows/     CI
```

## License

[MIT](LICENSE)
