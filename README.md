# kvstore

> **A persistent, Redis-compatible key-value database. Built from scratch in Java 21 — no database libraries.**

The same LSM-tree storage engine that powers LevelDB and RocksDB, fronted by a TCP server speaking the Redis RESP protocol. Any Redis client (`redis-cli`, `redis-py`, `Jedis`, etc.) connects out of the box.

[![CI](https://github.com/ethanstoner/kvstore/actions/workflows/ci.yml/badge.svg)](https://github.com/ethanstoner/kvstore/actions/workflows/ci.yml) &nbsp; **203 tests** · **5,200 LOC** · **JDK 21** · **MIT License**

```bash
$ java -jar kvstore.jar serve --port 6379
kvstore server listening on 6379

$ redis-cli -p 6379 set user:42 Ethan
OK
$ redis-cli -p 6379 incr visits
(integer) 1
$ redis-cli -p 6379 set session:abc Alice EX 60
OK
$ redis-cli -p 6379 ttl session:abc
(integer) 58
```

---

## Why this project exists

The public API is small — a few dozen commands. **All the engineering is in the internals**, and those internals *are* the canonical CS fundamentals: balanced/sorted structures, binary search, merge algorithms, amortized complexity, file I/O, concurrency, crash recovery, a real network protocol, encryption, and authentication.

Most engineers use databases as black boxes. This project demonstrates I can build one — including the tricky parts: crash safety, concurrent flush without write stalls, leveled compaction with bounded read amplification, virtual-thread I/O without carrier pinning, and a wire protocol third-party clients actually speak.

---

## Try it in 30 seconds

```bash
git clone https://github.com/ethanstoner/kvstore && cd kvstore
mvn package -DskipTests
java -jar target/kvstore-0.1.0.jar serve

# In another terminal:
redis-cli -p 6379 ping
redis-cli -p 6379 set hello world
redis-cli -p 6379 get hello
```

Or use the all-in-one Docker image:

```bash
docker build -t kvstore .
docker run -d -p 6379:6379 -v kvdata:/data kvstore
```

---

## Performance

Measured with [JMH](https://openjdk.org/projects/code-tools/jmh/) on JDK 21 (Microsoft OpenJDK 21.0.11), single-threaded, 100K-key dataset:

| Workload | Throughput |
|---|---|
| Sequential write | **~100K ops/sec** |
| Random write | **~106K ops/sec** |
| Point read (existing key) | **~131K ops/sec** |
| Point read (missing key, bloom-filter short-circuit) | **~14.4M ops/sec** |
| Range scan (100 keys per scan) | **~165K keys/sec** |

The 100× speedup for missing-key lookups is the bloom filter doing its job — most queries skip the disk entirely. Reproduce locally:

```bash
mvn package
java -jar target/kvstore-0.1.0-benchmarks.jar
```

---

## What it does

| Capability | How |
|---|---|
| **Persistent key-value storage** | LSM-tree: WAL → memtable → SSTables → leveled compaction |
| **Redis wire compatibility** | RESP protocol parser/writer, virtual-thread I/O |
| **Crash safety** | Every write hits the WAL (with CRC32 per record) before it's acknowledged. Verified by a `kill -9` test that recovers 100% of pre-crash writes. |
| **Bounded read latency** | Leveled compaction (LevelDB-style) + per-file bloom filter + LRU value cache |
| **Non-blocking writes** | Concurrent flush — memtable swap is O(1); the actual SSTable write happens on a background thread |
| **Encryption** | TLSv1.3 via `--tls-keystore` |
| **Authentication** | Multi-user with constant-time password compare + per-user command ACLs |
| **Pub/Sub** | `SUBSCRIBE`, `PSUBSCRIBE` (glob patterns), `PUBLISH` with async delivery |
| **TTL** | `SET key val EX 60`, `EXPIRE`, `TTL`, `PERSIST` — expired entries invisible to reads, dropped during compaction |
| **Backup** | `BGSAVE` produces a point-in-time snapshot file |
| **Operational** | Connection limits, INFO stats (peak/rejected connections, cache hit rate, snapshot epoch), JMH benchmarks, GitHub Actions CI |

### Full command catalog

```
Keys/strings:   SET (with EX/PX) · GET · DEL · EXISTS · MGET · MSET · SCAN
Numeric:        INCR · DECR · INCRBY · DECRBY
TTL:            EXPIRE · PEXPIRE · TTL · PTTL · PERSIST
Pub/Sub:        SUBSCRIBE · UNSUBSCRIBE · PSUBSCRIBE · PUNSUBSCRIBE · PUBLISH
Snapshots:      SAVE · BGSAVE · LASTSAVE
Server:         AUTH · PING · DBSIZE · INFO · COMMAND · QUIT · SHUTDOWN
```

---

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
  │                                       (multi-user/ACL)  │
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

Every write is logged to disk **before** it is applied in memory — that ordering is what makes a crash survivable. When the memtable exceeds 4 MB it is atomically swapped to an "immutable" slot and a background thread writes it to an L0 SSTable; new writes continue against a fresh memtable without blocking. A background thread compacts L_n → L_n+1, dropping tombstones only when they reach the deepest level.

---

## Design decisions

A handful of non-obvious engineering choices and the reasoning behind them.

### Leveled compaction over size-tiered

**Size-tiered** (the original choice, since replaced): group SSTables by file size, merge when 4+ of similar size exist. Simple, but reads must check every SSTable; bloom filters help but space amplification is high (the same key can sit in many files).

**Leveled** (LevelDB/RocksDB style): L0 may overlap (4 files max), L1+ have non-overlapping key ranges and grow 10× per level. Reads check at most 4 L0 files + 1 file per level below — bounded read amplification. Compaction does more total work (write amplification), but reads are predictable. The right trade for a read-heavy workload, which is most caches/sessions.

### Concurrent flush over synchronous

The naïve approach: when the memtable fills, write it to disk before accepting more writes. That stalls writers for 10–50 ms on commodity SSDs.

Instead: an atomic swap puts the full memtable into an "immutable" slot in microseconds; a background thread writes it. Reads consult both memtables. Crash safety preserved via a second WAL file (`wal-pending.log`) holding the in-flight memtable's records until the SSTable lands. Writes never stall on disk.

### `FileChannel.read(buffer, position)` instead of `RandomAccessFile`

`RandomAccessFile.seek() + read()` requires `synchronized` to be thread-safe. In JDK 21, `synchronized` blocks **pin the virtual thread's carrier**, defeating the whole "scales to thousands of connections" pitch. `FileChannel.read(ByteBuffer dst, long position)` is stateless and thread-safe — concurrent reads from many vthreads proceed in parallel without pinning.

### Per-value Deflate compression with auto-fallback

Compress values > 64 bytes (smaller values don't compress well — overhead > savings). After compression, compare sizes: if the compressed payload isn't smaller (e.g., random data), store raw. Reader branches on a per-entry op byte, so no file-format version bump was needed. Block cache stores **decompressed** values, so cache hits skip both seek and inflate.

### Single-password AUTH alongside multi-user

The first auth implementation accepted only `--requirepass`. Adding multi-user later threatened to break backward compat. Resolution: internally everyone is a "user" — `--requirepass secret` creates a user named `"default"`. Both `AUTH secret` and `AUTH default secret` work; multi-user is just adding more users. Zero breakage.

### `synchronized` writes for Pub/Sub delivery

When `PUBLISH` arrives, the publisher's vthread iterates subscribers and writes to each subscriber's socket directly. But that socket's `OutputStream` is also being used by the subscriber's own vthread (to write its own command replies). Resolution: a per-connection `writeLock` synchronized object. Subscribers' command loops hold it during `handle() + flush()`; PubSubHub holds it during async deliveries. Simple, correct, minimal overhead — async delivery is rare relative to normal command throughput.

### Snapshots reuse the SSTable format

`BGSAVE` writes a single file containing all current key→value pairs (memtable + immutable + all SSTable levels merged, newest wins, tombstones and expired entries dropped). That file IS a valid SSTable. To restore: stop the server, rename `snapshot-N.db` → `L0-NNNNNN.db`, restart. No special restore code path needed.

---

## Verified to work

| Test | Result |
|---|---|
| **203 unit + integration tests** | ✅ All pass on `mvn clean verify` |
| **End-to-end RESP smoke test** | ✅ 17 commands over real TCP — see `verify.ps1` |
| **`kill -9` crash recovery** | ✅ 5,000 writes, hard-killed server mid-write, restart → all data present |
| **JMH performance benchmarks** | ✅ Numbers above |
| **Concurrent vthread reads** | ✅ 100 vthreads × 50 reads against shared SSTable — no errors, correct values |
| **Concurrent writers under flush** | ✅ 4 vthreads × 5,000 writes during background flush — no lost writes |
| **WAL corruption handling** | ✅ Flip a byte mid-log → replay stops cleanly, prior records survive |

---

## Build & run

Requires JDK 21+ and Maven.

```bash
mvn verify                          # compile + run the full test suite
mvn package                         # build target/kvstore-0.1.0.jar
```

### As a network server

```bash
java -jar target/kvstore-0.1.0.jar serve                              # port 6379, no auth
java -jar target/kvstore-0.1.0.jar serve --port 6380                  # different port
java -jar target/kvstore-0.1.0.jar serve --data /var/kv               # custom data dir
java -jar target/kvstore-0.1.0.jar serve --requirepass topsecret      # single password
java -jar target/kvstore-0.1.0.jar serve --user alice:s1 --user bob:s2  # multi-user
java -jar target/kvstore-0.1.0.jar serve --user reader:s1:GET,EXISTS,MGET  # read-only ACL
java -jar target/kvstore-0.1.0.jar serve --max-connections 1000       # connection cap
java -jar target/kvstore-0.1.0.jar serve --tls-keystore kv.jks --tls-keystore-pass changeit  # TLS
```

Use any Redis client:

```bash
redis-cli -p 6379 ping                                # → PONG
redis-cli -p 6379 set hello world                     # → OK
redis-cli -p 6379 mget hello missing                  # → 1) "world"  2) (nil)
redis-cli -p 6379 info                                # → server / stats / storage / cache
redis-cli -p 6379 -a topsecret get hello              # AUTH <password>
redis-cli -p 6379 --user alice --pass s1 get hello    # AUTH <user> <password>
redis-cli -p 6379 --tls --cacert ca.crt ping          # TLS handshake
```

### As a Java library

```java
try (KvStore db = new KvStore(Path.of("/var/data/myapp"))) {
    db.put("user:42", "Ethan");
    db.put("session:abc", "user:42", System.currentTimeMillis() + 60_000); // TTL
    String user = db.get("user:42").orElse(null);
    long visits = db.incrementBy("counter", 1);
    Map<String, String> all = db.scan("user:", "user;");
}
```

### As a Docker container

```bash
docker build -t kvstore .
docker run -d --name kvstore -p 6379:6379 -v kvdata:/data kvstore

# With auth + TLS + data persisted to a host volume:
docker run -d --name kvstore -p 6379:6379 -v $(pwd)/data:/data \
    -v $(pwd)/kv.jks:/certs/kv.jks:ro \
    kvstore serve --port 6379 --data /data \
    --user alice:secret1 --user bob:secret2 \
    --tls-keystore /certs/kv.jks --tls-keystore-pass changeit
```

The image runs as a non-root user, exposes port 6379, and stores data under `/data`.

---

## Code organization

```
src/main/java/com/ethanstoner/kvstore/
  Memtable.java          sorted in-memory buffer (skip list)
  ValueEntry.java        value + expiration timestamp (record)
  WriteAheadLog.java     append-only durability + replay (CRC32)
  SSTable.java           immutable sorted file (binary + bloom + index + compression)
  BloomFilter.java       probabilistic set membership (MurmurHash3)
  BlockCache.java        thread-safe LRU value cache shared across SSTables
  KvStore.java           public API + flush + compaction + snapshots
  server/
    KvServer.java        TCP server, accept loop, graceful shutdown, connection limits
    ClientConnection.java  per-connection vthread loop, subscribe state, auth state
    CommandHandler.java  RESP command dispatch
    PubSubHub.java       channel/pattern subscription routing
    UserStore.java       users + passwords + per-user command ACLs
    AuthState.java       interface implemented by ClientConnection
    TlsConfig.java       TLSv1.3 keystore loader
    resp/
      RespParser.java    reads RESP frames (5 types + inline form)
      RespWriter.java    writes RESP responses
  cli/
    Cli.java             routes between `serve` and one-shot commands
    Main.java            one-shot: put / get / del / scan
    ServeCommand.java    starts the network server
  benchmark/
    KvStoreBenchmark.java  JMH throughput benchmarks

src/test/java/...      203 JUnit 5 tests covering every module
.github/workflows/     CI
Dockerfile             multi-stage build
```

---

## Roadmap

Tracked stretch goals (the current project is feature-complete as a single-node Redis-compatible store):

- **Replication** — master/replica streaming WAL for multi-node durability (~1 week MVP, multi-week for failover)
- **Sharding** — hash-partitioned multi-node clusters
- **Block-level compression** — better ratio than per-value for small values

---

## License

[MIT](LICENSE)
