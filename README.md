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
- **TLS encryption** — optional `SSLServerSocket` via `--tls-keystore`, TLSv1.3
- **Multi-user authentication** — Redis 6-style `AUTH <user> <pass>`, multiple users via `--user`, constant-time password comparison
- **Virtual-thread server** — Java 21 vthreads, one per connection, scales to thousands of concurrent clients. SSTable reads use `FileChannel` positional reads (no carrier pinning)
- **Durable writes** — write-ahead log with fsync and **CRC32 per record** for corruption detection
- **SSTable flush** — memtable spills to immutable sorted files when full
- **Multi-level reads** — get/scan falls through memtable → newest SSTable → older ones
- **Bloom filters** — probabilistic filter per SSTable skips files that can't contain a key (~1% false positive rate)
- **LRU block cache** — value cache shared across SSTables; hot reads skip the disk seek entirely
- **Per-value compression** — values > 64 bytes compressed with stdlib Deflate; auto-fallback on incompressible data
- **Concurrent flush** — non-blocking memtable flush; writes don't stall on disk I/O. Crash-safe via dual-WAL
- **Leveled compaction** — LevelDB/RocksDB-style: L0 (overlapping) + L1+ (non-overlapping, 10× growth). Bounded read amplification, lower space amp
- **Atomic DEL** — check-and-delete under the write lock; concurrent clients see accurate counts
- **Range scans** — ordered `[from, to)` scans merged across all levels
- **Crash recovery** — replays WAL on startup (stops at first corrupt record), skips corrupt SSTable files
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
java -jar target/kvstore-0.1.0.jar serve                              # port 6379, no auth
java -jar target/kvstore-0.1.0.jar serve --port 6380                  # different port
java -jar target/kvstore-0.1.0.jar serve --data /var/kv               # different data dir
java -jar target/kvstore-0.1.0.jar serve --requirepass topsecret      # single password
java -jar target/kvstore-0.1.0.jar serve --user alice:s1 --user bob:s2  # multi-user ACL
java -jar target/kvstore-0.1.0.jar serve --tls-keystore kv.jks --tls-keystore-pass changeit  # TLS

# In another terminal — works with any Redis client:
redis-cli -p 6379 ping                       # PONG
redis-cli -p 6379 set hello world            # OK
redis-cli -p 6379 get hello                  # "world"
redis-cli -p 6379 mget hello missing         # 1) "world"  2) (nil)
redis-cli -p 6379 info                       # server / stats / storage / cache
redis-cli -p 6379 shutdown                   # graceful stop

# With auth:
redis-cli -p 6379 -a topsecret get hello              # AUTH <password>
redis-cli -p 6379 --user alice --pass s1 get hello    # AUTH <user> <password>
redis-cli -p 6379 get hello                           # (error) NOAUTH Authentication required.

# With TLS:
redis-cli -p 6379 --tls --cacert ca.crt ping          # encrypted handshake
```

Supported commands: `AUTH`, `PING`, `SET`, `GET`, `DEL`, `EXISTS`, `MGET`, `MSET`, `SCAN from to` (range scan, not cursor), `DBSIZE`, `INFO`, `COMMAND`, `QUIT`, `SHUTDOWN`.

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
  BlockCache.java        thread-safe LRU value cache shared across SSTables
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

Potential future work:

- **Block-level (not per-value) compression** — better ratio for small values
- **Per-user permissions** — read-only / write-only / command allowlists on top of multi-user AUTH
- **Replication / streaming WAL** — multi-node durability via a follower protocol
- **Sharding** — multiple nodes, hashed key range partitioning

## License

[MIT](LICENSE)
