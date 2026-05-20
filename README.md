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

### High-level

```
  ┌─────────────────────────────────────────────────────────┐
  │  Clients   redis-cli · redis-py · Jedis · any RESP lib  │
  └─────────────────────────────┬───────────────────────────┘
                                │  TCP  (optional TLSv1.3)
                                │  RESP protocol
                                ▼
  ┌─────────────────────────────────────────────────────────┐
  │  Network layer                                          │
  │                                                         │
  │    accept loop  ──►  vthread/connection  ──►  RESP I/O  │
  │    (SSL/plain)       (Java 21)                │         │
  │                                               ▼         │
  │                                       AUTH ──► dispatch │
  │                                       (multi-user)      │
  └─────────────────────────────┬───────────────────────────┘
                                │  put / get / delete / scan
                                ▼
  ┌─────────────────────────────────────────────────────────┐
  │  Storage engine  (LSM-tree)                             │
  │                                                         │
  │    WAL ──► Memtable ──► Immutable ──► SSTables          │
  │   (CRC32) (skip-list)   memtable    (L0 ─► L1 ─► L2…)   │
  │                                                         │
  │  Background:  concurrent flush · leveled compaction     │
  │  Shared:      LRU block cache · bloom filter per file   │
  └─────────────────────────────────────────────────────────┘
```

### Storage engine deep-dive

```
      put(k,v) / delete(k)
             │
             ▼
  ┌─────────────────────────┐   1. append (durable, fsync'd, CRC32 per record)
  │  Write-Ahead Log        │      replayed on startup; survives crashes
  │  wal.log + wal-pending  │
  └────────────┬────────────┘
               │ apply
               ▼
  ┌─────────────────────────┐   2. in-memory write buffer
  │  Active Memtable        │      ConcurrentSkipListMap, O(log n)
  │  (sorted, mutable)      │
  └────────────┬────────────┘
               │ atomic swap when > 4 MB
               ▼
  ┌─────────────────────────┐   reads still hit this until flush completes
  │  Immutable Memtable     │   background flush thread writes it to L0
  │  (sorted, frozen)       │   — concurrent with new writes —
  └────────────┬────────────┘
               │ flush
               ▼
  ┌─────────────────────────────────────────────────────────┐
  │  SSTables on disk  (leveled, LevelDB/RocksDB style)     │
  │                                                         │
  │   L0:  [ ] [ ] [ ] [ ]    ≤ 4 files, may overlap        │
  │            │  compact when full                         │
  │            ▼                                            │
  │   L1:  [   ][    ][  ]    non-overlapping, ≤ 10 MB      │
  │            │  compact when over budget                  │
  │            ▼                                            │
  │   L2:  [        ][      ] non-overlapping, ≤ 100 MB     │
  │                           (each level 10× larger)       │
  │                                                         │
  │   Per file:  sorted data section · bloom filter ·       │
  │              in-memory key→offset index · footer        │
  │   Per value: Deflate-compressed when > 64 bytes         │
  │   Per read:  FileChannel positional read (no vthread    │
  │              carrier pinning), then LRU block cache     │
  └─────────────────────────────────────────────────────────┘

  Read path:  Memtable → Immutable → L0 (all 4) → L1 → L2 → ...
              stops at the first match (bloom filter skips levels
              that provably don't contain the key)
```

Every write is logged to disk **before** it is applied in memory — that ordering is what makes a crash survivable. When the memtable exceeds 4 MB it is atomically swapped to an "immutable" slot and a background thread writes it to an L0 SSTable; new writes continue against a fresh memtable without blocking. Reads check active → immutable → L0 (all files) → L1 (one file, binary search) → L2 (one file) → … with bloom filters skipping levels that can't contain the key. A background thread compacts L_n → L_n+1, dropping tombstones only when they reach the deepest level.

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

### With Docker

```bash
# Build the image
docker build -t kvstore .

# Run the server (data persisted to a Docker volume)
docker run -d --name kvstore -p 6379:6379 -v kvdata:/data kvstore

# Connect from anywhere with a Redis client
redis-cli -p 6379 ping
redis-cli -p 6379 set hello world
redis-cli -p 6379 get hello

# With auth + TLS:
docker run -d --name kvstore -p 6379:6379 -v kvdata:/data \
    -v /path/to/kv.jks:/certs/kv.jks:ro \
    kvstore serve --port 6379 --data /data \
    --user alice:secret1 --user bob:secret2 \
    --tls-keystore /certs/kv.jks --tls-keystore-pass changeit

# Tail logs
docker logs -f kvstore

# Stop
docker stop kvstore
```

The image runs as a non-root user, exposes port 6379, and stores all data under `/data` (declared as a volume).

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
