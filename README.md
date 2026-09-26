# kvstore

A persistent, Redis-compatible key-value database written from scratch in Java 21: an LSM-tree storage engine (the LevelDB/RocksDB design) behind a TCP server that speaks the Redis RESP protocol, so `redis-cli`, `redis-py` and Jedis connect without modification.

[![CI](https://github.com/ethanstoner/kvstore/actions/workflows/ci.yml/badge.svg)](https://github.com/ethanstoner/kvstore/actions/workflows/ci.yml) [![Java 21](https://img.shields.io/badge/Java-21-orange)](https://openjdk.org/projects/jdk/21/) [![License: MIT](https://img.shields.io/badge/License-MIT-blue)](LICENSE)

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

### Highlights

- ~130K point reads/s and ~100K writes/s single-threaded on a 100K-key dataset (JMH)
- Missing-key lookups run at ~11.8M ops/s, about 90x faster than existing-key reads, because per-file bloom filters skip the disk entirely
- Recovers from `kill -9`: an end-to-end test writes 5,000 keys over TCP, kills the server, restarts it, and reads the first, middle and last keys back from the write-ahead log
- 209 JUnit tests across 18 test classes, run in CI on every push

**Java 21 · virtual threads · Maven · JUnit 5 · JMH · Docker · GitHub Actions**

## Overview

The public API of a key-value store is tiny; everything interesting is in the internals. I built this to understand what databases hide: how a WAL keeps writes durable across crashes, how SSTables are merged without blocking reads, how a bloom filter avoids disk seeks for keys that don't exist, and how Java 21 virtual threads change network I/O. It is about 3,800 lines of Java, plus 3,300 lines of tests.

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

**Writes** are appended to the WAL (CRC32 per record) before they touch memory, which is what makes a crash survivable. When the memtable passes 4 MB it is swapped into an immutable slot and a background thread writes it out as an L0 SSTable while new writes continue against a fresh memtable. A second background thread compacts L_n into L_n+1 (each level 10x the size of the one above) and drops tombstones only at the deepest level.

**Reads** check the active memtable, then the immutable memtable, then SSTables newest-first. Each SSTable's bloom filter is consulted before its index; a negative answer skips the file.

## Engineering Highlights

- **Designed** an LSM-tree storage engine end to end: write-ahead log with per-record CRC32 checksums, a `ConcurrentSkipListMap` memtable, an on-disk SSTable format with a per-key offset index and a MurmurHash3 bloom filter per file.
- **Replaced** size-tiered compaction with leveled compaction (LevelDB-style): L0 compacts once it holds 4 files, and L1 and below hold non-overlapping key ranges, so a read touches at most one file per level.
- **Moved flush off the write path** with an immutable-memtable swap and a two-WAL scheme (`wal.log` + `wal-pending.log`); recovery replays both, so no acknowledged write is lost at any point in the flush lifecycle.
- **Removed virtual-thread carrier pinning** on the read path by migrating SSTable reads from `RandomAccessFile` + `synchronized` to stateless `FileChannel` positional reads.
- **Implemented** a RESP2 server on Java 21 virtual threads (one per connection) covering 33 Redis commands, including TTLs, atomic counters, pub/sub with glob patterns and `BGSAVE` snapshots.
- **Secured** the server with optional TLSv1.3, multi-user `AUTH` with constant-time password comparison (`MessageDigest.isEqual`), and per-user command allowlists.
- **Measured** throughput with JMH (table below) and wrote an end-to-end script that exercises the real server over TCP, including a `kill -9` crash-recovery check.

## Performance

JMH 1.37, single-threaded, Microsoft OpenJDK 21.0.11, 100K-key dataset:

| Workload | Throughput |
|---|---|
| Sequential write | ~102.5K ops/s |
| Random write | ~105.9K ops/s |
| Point read, existing key | ~129.3K ops/s |
| Point read, missing key | ~11.76M ops/s |
| Range scan, 100 keys per scan | ~1,685 scans/s (~168K keys/s) |

The gap between existing- and missing-key reads is the bloom filter: when it says a file can't hold the key, the index lookup and disk seek are skipped. Reproduce with `java -jar target/kvstore-0.1.0-benchmarks.jar`.

## Commands

```
Keys/strings:  SET (with EX/PX) · SETEX · PSETEX · GET · DEL · EXISTS · MGET · MSET · SCAN
Numeric:       INCR · DECR · INCRBY · DECRBY
TTL:           EXPIRE · PEXPIRE · TTL · PTTL · PERSIST
Pub/Sub:       SUBSCRIBE · UNSUBSCRIBE · PSUBSCRIBE · PUNSUBSCRIBE · PUBLISH
Snapshots:     SAVE · BGSAVE · LASTSAVE
Server:        AUTH · PING · DBSIZE · INFO · COMMAND · QUIT · SHUTDOWN
```

Two deliberate deviations from Redis, both reported as a clear error rather than misbehaving: the wire protocol is RESP2 only (no `HELLO`/RESP3 handshake; clients negotiate down automatically), and `SCAN` is a range scan, `SCAN <from> <to>`, rather than cursor-based iteration. Anything outside the list above gets `ERR unknown command`.

## Design notes

**Per-value compression with a fallback.** Values over 64 bytes are Deflate-compressed; if the output isn't smaller (random or already-compressed data), the original bytes are stored. A per-entry op byte tells the reader which path to take, so adding compression needed no file-format version bump. The block cache holds decompressed values, so a cache hit skips both the seek and the inflate.

**Snapshots reuse the SSTable format.** `BGSAVE` writes the merged view of every level (newest value wins, tombstones and expired entries dropped) as a single valid SSTable. There is no special restore path: stop the server, rename `snapshot-N.db` to `L0-NNNNNN.db`, and restart.

## Getting Started

Requires JDK 21+ and Maven.

```bash
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

## Testing

```bash
mvn verify          # 209 tests in 18 classes; CI runs this on every push
./verify.ps1        # end-to-end checks against a real server
```

The unit suite covers the WAL, memtable, SSTable format, bloom filter, block cache, compression, leveled compaction, concurrent flush, TTLs, snapshots, the RESP codec, the server, pub/sub, auth and TLS. `verify.ps1` starts the server and sends a 17-command RESP smoke test over a real TCP socket, then writes 5,000 keys, kills the server with `kill -9`, restarts it and spot-checks the first, middle and last keys. It also runs the JMH benchmarks (skip with `-SkipBench`).

## Project Structure

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
```

## What I Learned

- **Simple compaction has a read cost.** Size-tiered compaction was easier to write, but any key could live in any SSTable, so every read had to consult every file. Leveled compaction does more total merge work in exchange for bounded, predictable reads.
- **`synchronized` and virtual threads don't mix.** `RandomAccessFile` keeps an implicit cursor, so concurrent reads needed a lock, and on Java 21 a `synchronized` block pins the virtual thread to its carrier, undoing the point of using virtual threads. Positional `FileChannel` reads take the offset as an argument and need no lock.
- **Background work complicates shutdown.** Adding `BGSAVE` introduced a deadlock in `close()` until the snapshot executor was drained before taking the flush lock; every new background thread needs a defined place in the shutdown order.

## License

[MIT](LICENSE)
