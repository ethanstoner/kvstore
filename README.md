# kvstore

A persistent key-value store with an **LSM-tree storage engine** and a **Redis-compatible network server**, written from scratch in Java 21 — no database libraries. The same storage architecture used by LevelDB, RocksDB, and the storage layer of Cassandra, fronted by a TCP server speaking the RESP protocol so any Redis client can connect.

[![CI](https://github.com/ethanstoner/kvstore/actions/workflows/ci.yml/badge.svg)](https://github.com/ethanstoner/kvstore/actions/workflows/ci.yml)

```bash
# Start the server
$ java -jar kvstore.jar serve --port 6379

# Connect with any Redis client
$ redis-cli -p 6379 set user:42 Ethan
OK
$ redis-cli -p 6379 get user:42
"Ethan"
```

Or use it as an embedded library:
```bash
$ java -jar kvstore.jar put hello world
OK
$ java -jar kvstore.jar get hello
world
# data persists across restarts via the WAL
```

## Why this project exists

The public API is small. All the engineering is in the internals — and those internals *are* the core CS fundamentals: balanced/sorted structures, binary search, merge algorithms, amortized complexity, file I/O, concurrency, crash recovery, and a real network protocol.

## Architecture

```
   ┌──────────────────────────────────────────────────────────┐
   │  redis-cli / redis-py / Jedis / any RESP client          │
   └────────────────────┬─────────────────────────────────────┘
                        │ TCP + RESP protocol
                        ▼
   ┌────────────────────────┐
   │   KvServer             │   accept loop + 1 vthread/conn
   │   (Java 21 vthreads)   │   RESP parser → command handler
   └────────────────────────┘
                        │
                        ▼
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

## Features

- **Redis wire-protocol compatible** — connect with `redis-cli`, `redis-py`, `Jedis`, or any Redis client library
- **Virtual-thread server** — Java 21 vthreads, one per connection, scales to thousands of concurrent clients
- **Durable writes** — write-ahead log with fsync; data survives crashes
- **SSTable flush** — memtable spills to immutable sorted files when full
- **Multi-level reads** — get/scan falls through memtable → newest SSTable → older ones
- **Bloom filters** — probabilistic filter per SSTable skips files that can't contain a key (~1% false positive rate)
- **Size-tiered compaction** — background thread merges SSTables of similar size, drops tombstones
- **Atomic DEL** — check-and-delete under the write lock; concurrent clients see accurate counts
- **Range scans** — ordered `[from, to)` scans merged across all levels
- **Crash recovery** — replays WAL on startup, skips corrupt SSTable files
- **JMH benchmarks** — measure write/read/scan throughput

## Complexity

| Operation | Cost | Why |
|-----------|------|-----|
| `put` / `delete` | O(log n) + O(1) amortized | sorted skip-list insert + sequential WAL append |
| `get` | O(log n) per level | memtable → SSTables newest-first, bloom filter skips irrelevant files |
| `scan(from, to)` | O(log n + k) per level | sub-map lookups merged across levels, k = results |
| Flush | O(m) | sequential write of m memtable entries to sorted SSTable |
| Compaction | O(m) | k-way merge of m total entries across selected SSTables |
| Recovery | O(m) | replay m logged WAL records + open existing SSTables |

## Build & run

Requires JDK 21+ and Maven.

```bash
mvn verify                    # compile + run the full test suite
mvn package                   # build target/kvstore-0.1.0.jar
```

### As a network server (Redis-compatible)

```bash
java -jar target/kvstore-0.1.0.jar serve                     # port 6379
java -jar target/kvstore-0.1.0.jar serve --port 6380         # different port
java -jar target/kvstore-0.1.0.jar serve --data /var/kv      # different data dir

# In another terminal — works with any Redis client:
redis-cli -p 6379 ping                  # PONG
redis-cli -p 6379 set hello world       # OK
redis-cli -p 6379 get hello             # "world"
redis-cli -p 6379 mget hello missing    # 1) "world"  2) (nil)
redis-cli -p 6379 info                  # server / stats / storage sections
redis-cli -p 6379 shutdown              # graceful stop
```

Supported commands: `PING`, `SET`, `GET`, `DEL`, `EXISTS`, `MGET`, `MSET`, `SCAN from to` (range scan, not cursor), `DBSIZE`, `INFO`, `COMMAND`, `QUIT`, `SHUTDOWN`.

### As an embedded one-shot CLI

```bash
java -jar target/kvstore-0.1.0.jar put hello world
java -jar target/kvstore-0.1.0.jar get hello
java -jar target/kvstore-0.1.0.jar scan a z
java -jar target/kvstore-0.1.0.jar del hello
```

### As a Java library

```java
try (KvStore db = new KvStore(Path.of("/var/data/myapp"))) {
    db.put("user:42", "{...json...}");
    String user = db.get("user:42").orElse(null);
    Map<String, String> users = db.scan("user:", "user;");
    db.delete("user:42");
}
```

## Benchmarks

```bash
mvn package
java -jar target/kvstore-0.1.0-benchmarks.jar           # run all benchmarks
java -jar target/kvstore-0.1.0-benchmarks.jar ".*Read.*" # run only read benchmarks
```

Measured with JMH (Java Microbenchmark Harness):

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
  server/
    KvServer.java        TCP server, accept loop, graceful shutdown
    ClientConnection.java  per-connection vthread loop
    CommandHandler.java  dispatches RESP commands to KvStore
    resp/
      RespParser.java    reads RESP frames (5 types + inline form)
      RespWriter.java    writes RESP responses
  cli/
    Cli.java             routes between `serve` and one-shot commands
    Main.java            one-shot: put / get / del / scan
    ServeCommand.java    starts the network server
  benchmark/
    KvStoreBenchmark.java  JMH throughput benchmarks
src/test/java/...        JUnit 5 suite: memtable, WAL, SSTable, bloom filter,
                         recovery, compaction, RESP codec, command handler,
                         server integration
.github/workflows/       CI
```

## Roadmap

Potential future work (Phase 2):

- **WAL checksums** — CRC32 per record, detect partial/corrupt WAL entries on replay
- **Block cache** — LRU cache for frequently-read SSTable values
- **Compression** — Snappy or LZ4 per SSTable
- **Concurrent flush** — immutable memtable during flush so writes don't stall
- **Leveled compaction** — non-overlapping key ranges per level for better read amplification at scale
- **Replace `synchronized` with `FileChannel` + `ReentrantLock`** — eliminate virtual-thread carrier pinning during SSTable reads

## License

[MIT](LICENSE)
