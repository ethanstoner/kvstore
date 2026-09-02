# kvstore

A persistent, Redis-compatible key-value database I wrote from scratch in Java 21. The storage layer is an LSM-tree of the same shape as LevelDB and RocksDB: write-ahead log, in-memory memtable, sorted on-disk SSTables, leveled compaction. The network layer is a TCP server speaking the Redis RESP protocol, so existing Redis clients like `redis-cli`, `redis-py`, and `Jedis` work without modification.

[![CI](https://github.com/ethanstoner/kvstore/actions/workflows/ci.yml/badge.svg)](https://github.com/ethanstoner/kvstore/actions/workflows/ci.yml) &nbsp; 209 tests · ~5,200 lines of Java · MIT

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

## Why I built it

Most engineers treat databases as black boxes: call `SET`, get back a response. I wanted to build one to understand what they hide. How does a WAL keep writes durable across crashes? How are SSTables merged without blocking reads? How does a bloom filter avoid disk seeks for keys that don't exist? How do Java 21 virtual threads change the calculus of network I/O?

The public API of a key-value store is tiny. Everything interesting is in the internals, and the internals are the canonical CS fundamentals — balanced/sorted structures, binary search, merge algorithms, file I/O, concurrency, crash recovery, a real wire protocol.

## What it does

Stores key-value pairs durably and serves them over the network. The pieces:

- **Storage** — LSM-tree with a write-ahead log (CRC32 per record), an in-memory sorted memtable, and on-disk SSTables organized into levels (L0 through L_n, each ten times larger than the previous). Background threads handle flush and compaction without stalling writes.
- **Performance** — per-file bloom filters skip irrelevant SSTables; an LRU value cache is shared across all files; values larger than 64 bytes get Deflate-compressed.
- **Network** — TCP server on Java 21 virtual threads (one per connection). Speaks the RESP protocol, so any Redis client connects.
- **Security** — optional TLSv1.3, multi-user authentication with constant-time password comparison, per-user command ACLs.
- **Features** — TTL (`SET ... EX`, `EXPIRE`, `PERSIST`), atomic counters (`INCR`, `DECR`), pub/sub with glob-pattern matching, point-in-time snapshots (`BGSAVE`), range scans, an `INFO` endpoint with cache and storage stats, configurable connection limits.

Full command surface:

```
Keys/strings:  SET (with EX/PX) · SETEX · PSETEX · GET · DEL · EXISTS · MGET · MSET · SCAN
Numeric:       INCR · DECR · INCRBY · DECRBY
TTL:           EXPIRE · PEXPIRE · TTL · PTTL · PERSIST
Pub/Sub:       SUBSCRIBE · UNSUBSCRIBE · PSUBSCRIBE · PUNSUBSCRIBE · PUBLISH
Snapshots:     SAVE · BGSAVE · LASTSAVE
Server:        AUTH · PING · DBSIZE · INFO · COMMAND · QUIT · SHUTDOWN
```

Two deliberate deviations from Redis, both of which report a clear error rather than
misbehaving: the wire protocol is RESP2 only (no `HELLO`/RESP3 handshake — clients
negotiate down automatically), and `SCAN` is a range scan, `SCAN <from> <to>`, rather
than Redis's cursor-based iteration. Anything outside the list above is answered with
`ERR unknown command`.

## Performance

Single-threaded JMH on JDK 21 (Microsoft OpenJDK 21.0.11), 100K-key dataset:

| Workload | Throughput |
|---|---|
| Sequential write | ~100K ops/sec |
| Random write | ~106K ops/sec |
| Point read, existing key | ~131K ops/sec |
| Point read, missing key | ~14M ops/sec |
| Range scan, 100 keys per scan | ~165K keys/sec |

The huge gap between existing and missing key reads is the bloom filter: when it correctly tells us a level can't contain a key, we skip the disk seek entirely. Reproduce locally with `java -jar target/kvstore-0.1.0-benchmarks.jar`.

## Architecture

```
  ┌────────────────────────────────────────────────────────────┐
  │  Clients     redis-cli · redis-py · Jedis · any RESP lib   │
  └──────────────────────────────┬─────────────────────────────┘
                                 │   TCP (optional TLSv1.3)
                                 ▼
  ┌────────────────────────────────────────────────────────────┐
  │  Network layer                                             │
  │                                                            │
  │     accept loop  ──►  one vthread per connection           │
  │                                  │                         │
  │                                  ▼                         │
  │     RESP parser  ──►  AUTH + ACL  ──►  command dispatch    │
  └──────────────────────────────┬─────────────────────────────┘
                                 │   put / get / delete / scan
                                 ▼
  ┌────────────────────────────────────────────────────────────┐
  │  Storage engine  (LSM-tree)                                │
  │                                                            │
  │     WAL  ──►  Memtable  ──►  Immutable  ──►  SSTables      │
  │    (CRC32) (skip-list)    memtable      (L0 → L1 → L2 …)   │
  │                                                            │
  │     Background:   concurrent flush, leveled compaction     │
  │     Shared:       LRU block cache, bloom filter per file   │
  └────────────────────────────────────────────────────────────┘
```

Every write is logged to disk before it is applied in memory; that ordering is what makes a crash survivable. When the memtable exceeds 4 MB it is atomically swapped to an immutable slot and a background thread writes it to an L0 SSTable. New writes continue against a fresh memtable without blocking. A separate background thread compacts L_n into L_n+1, dropping tombstones only when they reach the deepest level.

Reads check the active memtable, then the immutable memtable (if a flush is in progress), then SSTables newest-first. Each SSTable's bloom filter is consulted first; if it says the key can't be present, we skip the file's index and disk seek entirely.

## Design notes

A few of the more interesting decisions and the reasoning that drove them.

### Leveled compaction, not size-tiered

The first version of compaction was size-tiered: group SSTables by order-of-magnitude file size, merge when four or more of similar size accumulate. Simpler to implement, but it has a real cost — any given key can live in any SSTable, so reads have to consult all of them (bloom filters help, but they're probabilistic).

Switching to leveled compaction (the LevelDB and RocksDB approach) caps L0 at four files and gives L1 and below non-overlapping key ranges. Each read touches at most one file per level, with bloom filters short-circuiting further. Compaction itself does more total work, but reads become predictable and space amplification drops.

### Concurrent flush

The naive memtable flush blocks writes for 10–50 ms on commodity SSDs. The version in the repo does an atomic swap that moves the full memtable into an "immutable" slot in microseconds, then lets a background thread write the SSTable. New writes continue against a fresh memtable immediately.

Crash safety is preserved by a second WAL file (`wal-pending.log`) holding the in-flight memtable's records until the corresponding SSTable lands on disk. On startup, the recovery code replays the pending WAL first, then the active WAL. Both either fully exist or don't, so there's no torn state to resolve.

### `FileChannel` instead of `RandomAccessFile`

`RandomAccessFile` keeps an implicit cursor, so concurrent seeks require a `synchronized` block. In Java 21, `synchronized` blocks pin the virtual thread's carrier — which defeats the whole reason for using virtual threads in the network layer. `FileChannel.read(ByteBuffer, position)` is stateless: it takes the offset as an argument, so it's safe to call from many threads in parallel without any locking.

### Per-value compression with a fallback

Values larger than 64 bytes get Deflate-compressed before writing to the SSTable. If the compressed output isn't actually smaller (e.g. random or already-compressed data), the original bytes get stored instead. A per-entry op byte tells the reader which path to take, so no file-format version bump was needed when this was added. The block cache stores decompressed values, so cache hits skip both the disk seek and the inflate step.

### Snapshots reuse the SSTable format

`BGSAVE` writes a single file containing the merged view of every level — newest values win for duplicate keys, tombstones and expired entries are dropped. That file is just a valid SSTable. There's no special restore code path: stop the server, rename `snapshot-N.db` to `L0-NNNNNN.db`, and restart. It loads as if it were any other SSTable.

## Build and run

Requires JDK 21 or newer and Maven.

```bash
mvn verify                            # compile and run the 209-test suite
mvn package                           # produces target/kvstore-0.1.0.jar
java -jar target/kvstore-0.1.0.jar serve
```

Server flags:

```bash
serve --port 6379                                # default
serve --data /var/kvstore                        # data directory (default ./kvdata)
serve --requirepass topsecret                    # single-password auth
serve --user alice:s1 --user bob:s2              # multi-user (repeatable)
serve --user reader:s1:GET,EXISTS,MGET           # per-user command allowlist
serve --max-connections 1000                     # connection cap
serve --tls-keystore kv.jks --tls-keystore-pass changeit
```

As a Java library:

```java
try (KvStore db = new KvStore(Path.of("/var/data/myapp"))) {
    db.put("user:42", "Ethan");
    db.put("session:abc", "Alice", System.currentTimeMillis() + 60_000); // with TTL
    Optional<String> user = db.get("user:42");
    long visits = db.incrementBy("counter", 1);
    Map<String, String> all = db.scan("user:", "user;");
}
```

As a Docker container (multi-stage build, non-root user, data persisted to `/data`):

```bash
docker build -t kvstore .
docker run -d -p 6379:6379 -v kvdata:/data kvstore
```

## Verification

Beyond the unit suite, `verify.ps1` runs an end-to-end check: it starts the server, issues a RESP smoke test of 17 commands over a real TCP socket, kills the server with `kill -9` mid-write, restarts it, and confirms all 5,000 pre-crash writes survived. It also runs the JMH benchmarks. CI runs `mvn verify` on every push.

## Code layout

```
src/main/java/com/ethanstoner/kvstore/
  Memtable, WriteAheadLog, SSTable, BloomFilter, BlockCache    storage primitives
  ValueEntry                                                    value + expiration
  KvStore                                                       public API, flush, compaction, snapshots
  server/
    KvServer, ClientConnection                                  TCP lifecycle, vthread per connection
    CommandHandler                                              dispatches parsed RESP to KvStore + PubSubHub
    PubSubHub                                                   channel and pattern routing
    UserStore, AuthState, TlsConfig                             auth and TLS
    resp/
      RespParser, RespWriter                                    protocol codec
  cli/
    Cli, Main, ServeCommand                                     one-shot CLI + server entry point
  benchmark/
    KvStoreBenchmark                                            JMH

src/test/java/...     18 test classes, 209 tests
.github/workflows/    CI runs mvn verify on every push
Dockerfile            multi-stage build
verify.ps1            end-to-end verification script
```

## Not in here

Things I'd build next if I were taking this further:

- **Replication.** Master/replica streaming WAL. An MVP — async-only, manual failover — is about a week of focused work. Full automatic failover is multi-week and crosses into distributed consensus territory.
- **Sharding.** Multi-node, key-range or hash-based partitioning.
- **Block-level compression.** Per-value compression already runs; grouping small values into compressed blocks would compress better at the cost of file-format complexity.

## License

[MIT](LICENSE).
