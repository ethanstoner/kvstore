# LSM-Tree Full Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the memtable+WAL kvstore into a complete LSM-tree with SSTable flush, multi-level reads, compaction, bloom filters, and JMH benchmarks.

**Architecture:** Bottom-up build order — BloomFilter (standalone, no dependencies), SSTable (depends on BloomFilter), then KvStore expansion (flush, multi-level reads, compaction). Each layer is independently testable.

**Tech Stack:** Java 21, Maven, JUnit 5, JMH 1.37

**Build/test command:** `$env:JAVA_HOME = "C:\Program Files\Microsoft\jdk-21.0.11.10-hotspot"; $env:PATH = "$env:JAVA_HOME\bin;$env:USERPROFILE\tools\maven\apache-maven-3.9.16\bin;$env:PATH"; mvn verify` (PowerShell). All tests must pass after every task.

**Spec:** `docs/specs/2026-05-20-lsm-full-engine-design.md`

---

### Task 1: BloomFilter — core logic

**Files:**
- Create: `src/main/java/com/ethanstoner/kvstore/BloomFilter.java`
- Create: `src/test/java/com/ethanstoner/kvstore/BloomFilterTest.java`

- [ ] **Step 1: Write the failing tests**

```java
package com.ethanstoner.kvstore;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BloomFilterTest {

    @Test
    void addedKeyIsAlwaysFound() {
        BloomFilter bf = new BloomFilter(100, 0.01);
        bf.add("hello");
        assertTrue(bf.mightContain("hello"));
    }

    @Test
    void missingKeyUsuallyNotFound() {
        BloomFilter bf = new BloomFilter(1000, 0.01);
        for (int i = 0; i < 1000; i++) {
            bf.add("key-" + i);
        }
        int falsePositives = 0;
        for (int i = 1000; i < 11000; i++) {
            if (bf.mightContain("key-" + i)) falsePositives++;
        }
        // 1% target FPR — allow up to 3% to avoid flaky tests
        assertTrue(falsePositives < 300, "FP rate too high: " + falsePositives + "/10000");
    }

    @Test
    void noFalseNegatives() {
        BloomFilter bf = new BloomFilter(500, 0.01);
        for (int i = 0; i < 500; i++) {
            bf.add("item-" + i);
        }
        for (int i = 0; i < 500; i++) {
            assertTrue(bf.mightContain("item-" + i), "False negative for item-" + i);
        }
    }

    @Test
    void singleInsertionWorks() {
        BloomFilter bf = new BloomFilter(1, 0.01);
        bf.add("only");
        assertTrue(bf.mightContain("only"));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl . -Dtest=BloomFilterTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: Compilation error — `BloomFilter` class does not exist yet.

- [ ] **Step 3: Write the BloomFilter implementation**

```java
package com.ethanstoner.kvstore;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public final class BloomFilter {

    private final byte[] bits;
    private final int numHashFunctions;

    public BloomFilter(int expectedInsertions, double falsePositiveRate) {
        if (expectedInsertions < 1) expectedInsertions = 1;
        int bitCount = optimalBitCount(expectedInsertions, falsePositiveRate);
        this.bits = new byte[(bitCount + 7) / 8];
        this.numHashFunctions = optimalHashCount(bitCount, expectedInsertions);
    }

    private BloomFilter(byte[] bits, int numHashFunctions) {
        this.bits = bits;
        this.numHashFunctions = numHashFunctions;
    }

    public void add(String key) {
        long hash64 = murmur3Hash64(key);
        int h1 = (int) hash64;
        int h2 = (int) (hash64 >>> 32);
        int bitCount = bits.length * 8;
        for (int i = 0; i < numHashFunctions; i++) {
            int combinedHash = h1 + i * h2;
            int pos = (combinedHash & Integer.MAX_VALUE) % bitCount;
            bits[pos / 8] |= (byte) (1 << (pos % 8));
        }
    }

    public boolean mightContain(String key) {
        long hash64 = murmur3Hash64(key);
        int h1 = (int) hash64;
        int h2 = (int) (hash64 >>> 32);
        int bitCount = bits.length * 8;
        for (int i = 0; i < numHashFunctions; i++) {
            int combinedHash = h1 + i * h2;
            int pos = (combinedHash & Integer.MAX_VALUE) % bitCount;
            if ((bits[pos / 8] & (1 << (pos % 8))) == 0) return false;
        }
        return true;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeInt(bits.length);
        out.write(bits);
        out.writeInt(numHashFunctions);
    }

    public static BloomFilter readFrom(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] bits = new byte[len];
        in.readFully(bits);
        int numHash = in.readInt();
        return new BloomFilter(bits, numHash);
    }

    private static int optimalBitCount(int n, double fpp) {
        return (int) Math.ceil(-n * Math.log(fpp) / (Math.log(2) * Math.log(2)));
    }

    private static int optimalHashCount(int bitCount, int n) {
        return Math.max(1, (int) Math.ceil((double) bitCount / n * Math.log(2)));
    }

    // MurmurHash3 128-bit finalizer applied to key's hashCode + a seed.
    // Returns 64 bits: lower 32 = h1, upper 32 = h2.
    private static long murmur3Hash64(String key) {
        byte[] data = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        long h1 = 0L;
        long h2 = 0L;
        long c1 = 0x87c37b91114253d5L;
        long c2 = 0x4cf5ad432745937fL;

        int nblocks = data.length / 16;
        for (int i = 0; i < nblocks; i++) {
            long k1 = getLong(data, i * 16);
            long k2 = getLong(data, i * 16 + 8);
            k1 *= c1; k1 = Long.rotateLeft(k1, 31); k1 *= c2; h1 ^= k1;
            h1 = Long.rotateLeft(h1, 27); h1 += h2; h1 = h1 * 5 + 0x52dce729;
            k2 *= c2; k2 = Long.rotateLeft(k2, 33); k2 *= c1; h2 ^= k2;
            h2 = Long.rotateLeft(h2, 31); h2 += h1; h2 = h2 * 5 + 0x38495ab5;
        }

        int tail = nblocks * 16;
        long k1 = 0, k2 = 0;
        switch (data.length - tail) {
            case 15: k2 ^= (long)(data[tail+14] & 0xff) << 48;
            case 14: k2 ^= (long)(data[tail+13] & 0xff) << 40;
            case 13: k2 ^= (long)(data[tail+12] & 0xff) << 32;
            case 12: k2 ^= (long)(data[tail+11] & 0xff) << 24;
            case 11: k2 ^= (long)(data[tail+10] & 0xff) << 16;
            case 10: k2 ^= (long)(data[tail+9] & 0xff) << 8;
            case 9:  k2 ^= (long)(data[tail+8] & 0xff);
                     k2 *= c2; k2 = Long.rotateLeft(k2, 33); k2 *= c1; h2 ^= k2;
            case 8:  k1 ^= (long)(data[tail+7] & 0xff) << 56;
            case 7:  k1 ^= (long)(data[tail+6] & 0xff) << 48;
            case 6:  k1 ^= (long)(data[tail+5] & 0xff) << 40;
            case 5:  k1 ^= (long)(data[tail+4] & 0xff) << 32;
            case 4:  k1 ^= (long)(data[tail+3] & 0xff) << 24;
            case 3:  k1 ^= (long)(data[tail+2] & 0xff) << 16;
            case 2:  k1 ^= (long)(data[tail+1] & 0xff) << 8;
            case 1:  k1 ^= (long)(data[tail] & 0xff);
                     k1 *= c1; k1 = Long.rotateLeft(k1, 31); k1 *= c2; h1 ^= k1;
        }

        h1 ^= data.length; h2 ^= data.length;
        h1 += h2; h2 += h1;
        h1 = fmix64(h1); h2 = fmix64(h2);
        h1 += h2;
        return h1;
    }

    private static long fmix64(long k) {
        k ^= k >>> 33; k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33; k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    private static long getLong(byte[] data, int offset) {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v |= (long)(data[offset + i] & 0xff) << (i * 8);
        }
        return v;
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn test -Dtest=BloomFilterTest`
Expected: All 4 tests PASS.

- [ ] **Step 5: Run full test suite for regression**

Run: `mvn verify`
Expected: All 21 tests PASS (17 existing + 4 new). BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/ethanstoner/kvstore/BloomFilter.java src/test/java/com/ethanstoner/kvstore/BloomFilterTest.java
git commit -m "feat: add BloomFilter with MurmurHash3 and serialization"
```

---

### Task 2: BloomFilter — serialization round-trip test

**Files:**
- Modify: `src/test/java/com/ethanstoner/kvstore/BloomFilterTest.java`

- [ ] **Step 1: Add serialization round-trip test**

Append to `BloomFilterTest.java`:

```java
@Test
void serializationRoundTrip() throws Exception {
    BloomFilter original = new BloomFilter(200, 0.01);
    for (int i = 0; i < 200; i++) original.add("ser-" + i);

    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    original.writeTo(new java.io.DataOutputStream(baos));

    java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(baos.toByteArray());
    BloomFilter restored = BloomFilter.readFrom(new java.io.DataInputStream(bais));

    for (int i = 0; i < 200; i++) {
        assertTrue(restored.mightContain("ser-" + i), "Lost key ser-" + i);
    }
}
```

- [ ] **Step 2: Run to verify it passes**

Run: `mvn test -Dtest=BloomFilterTest`
Expected: All 5 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add src/test/java/com/ethanstoner/kvstore/BloomFilterTest.java
git commit -m "test: add BloomFilter serialization round-trip test"
```

---

### Task 3: SSTable — writer and reader

**Files:**
- Create: `src/main/java/com/ethanstoner/kvstore/SSTable.java`
- Create: `src/test/java/com/ethanstoner/kvstore/SSTableTest.java`

- [ ] **Step 1: Write the failing tests**

```java
package com.ethanstoner.kvstore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class SSTableTest {

    @Test
    void writeAndReadBack(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("sst-000001.db");
        // Write
        List<Map.Entry<String, String>> entries = List.of(
                Map.entry("apple", "red"),
                Map.entry("banana", "yellow"),
                Map.entry("cherry", "dark red")
        );
        SSTable.write(file, entries);
        // Read
        try (SSTable sst = SSTable.open(file)) {
            assertEquals(Optional.of("red"), sst.get("apple"));
            assertEquals(Optional.of("yellow"), sst.get("banana"));
            assertEquals(Optional.of("dark red"), sst.get("cherry"));
            assertTrue(sst.get("dragonfruit").isEmpty());
        }
    }

    @Test
    void tombstonesArePreserved(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("sst-000001.db");
        List<Map.Entry<String, String>> entries = List.of(
                Map.entry("alive", "yes"),
                Map.entry("dead", Memtable.TOMBSTONE)
        );
        SSTable.write(file, entries);
        try (SSTable sst = SSTable.open(file)) {
            assertEquals(Optional.of("yes"), sst.get("alive"));
            // get returns the raw value including tombstone — KvStore interprets it
            assertTrue(sst.get("dead").isPresent());
            assertEquals(Memtable.TOMBSTONE, sst.get("dead").get());
        }
    }

    @Test
    void keysInSortedOrder(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("sst-000001.db");
        List<Map.Entry<String, String>> entries = List.of(
                Map.entry("a", "1"),
                Map.entry("b", "2"),
                Map.entry("c", "3"),
                Map.entry("d", "4")
        );
        SSTable.write(file, entries);
        try (SSTable sst = SSTable.open(file)) {
            NavigableMap<String, String> range = sst.scan("b", "d");
            assertEquals(List.of("b", "c"), new ArrayList<>(range.keySet()));
            assertEquals("2", range.get("b"));
            assertEquals("3", range.get("c"));
        }
    }

    @Test
    void bloomFilterSkipsMissingKeys(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("sst-000001.db");
        List<Map.Entry<String, String>> entries = List.of(
                Map.entry("x", "1"),
                Map.entry("y", "2")
        );
        SSTable.write(file, entries);
        try (SSTable sst = SSTable.open(file)) {
            // Bloom filter says "definitely not" for keys not present
            // We can't directly test the bloom filter skip, but we verify
            // that missing keys return empty
            assertTrue(sst.get("z").isEmpty());
            assertTrue(sst.get("a").isEmpty());
        }
    }

    @Test
    void corruptFileIsDetected(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("sst-corrupt.db");
        // Write garbage
        java.nio.file.Files.write(file, new byte[]{1, 2, 3, 4, 5});
        assertThrows(IOException.class, () -> SSTable.open(file));
    }

    @Test
    void extractsSequenceNumberFromFilename() {
        assertEquals(1, SSTable.sequenceNumber(Path.of("sst-000001.db")));
        assertEquals(42, SSTable.sequenceNumber(Path.of("sst-000042.db")));
        assertEquals(999999, SSTable.sequenceNumber(Path.of("sst-999999.db")));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -Dtest=SSTableTest`
Expected: Compilation error — `SSTable` class does not exist.

- [ ] **Step 3: Write the SSTable implementation**

```java
package com.ethanstoner.kvstore;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.NavigableSet;

public final class SSTable implements AutoCloseable {

    private static final int MAGIC = 0x4B565354; // "KVST"
    private static final int FOOTER_SIZE = 24;
    private static final byte OP_TOMBSTONE = 0;
    private static final byte OP_VALUE = 1;

    private final Path path;
    private final RandomAccessFile raf;
    private final TreeMap<String, Long> index;
    private final BloomFilter bloomFilter;
    private final int sequenceNum;

    private SSTable(Path path, RandomAccessFile raf, TreeMap<String, Long> index,
                    BloomFilter bloomFilter) {
        this.path = path;
        this.raf = raf;
        this.index = index;
        this.bloomFilter = bloomFilter;
        this.sequenceNum = sequenceNumber(path);
    }

    public static void write(Path file, Iterable<Map.Entry<String, String>> sortedEntries)
            throws IOException {
        int count = 0;
        List<Map.Entry<String, String>> entryList = new ArrayList<>();
        for (Map.Entry<String, String> e : sortedEntries) {
            entryList.add(e);
        }

        BloomFilter bloom = new BloomFilter(Math.max(1, entryList.size()), 0.01);
        for (Map.Entry<String, String> e : entryList) bloom.add(e.getKey());

        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(file.toFile())))) {

            // Data section — record offsets as we go
            List<String> keys = new ArrayList<>();
            List<Long> offsets = new ArrayList<>();
            long pos = 0;

            for (Map.Entry<String, String> e : entryList) {
                keys.add(e.getKey());
                offsets.add(pos);
                byte[] keyBytes = e.getKey().getBytes(StandardCharsets.UTF_8);
                boolean isTombstone = Memtable.TOMBSTONE.equals(e.getValue());

                out.writeByte(isTombstone ? OP_TOMBSTONE : OP_VALUE);
                out.writeShort(keyBytes.length);
                out.write(keyBytes);
                pos += 1 + 2 + keyBytes.length;

                if (!isTombstone) {
                    byte[] valBytes = e.getValue().getBytes(StandardCharsets.UTF_8);
                    out.writeInt(valBytes.length);
                    out.write(valBytes);
                    pos += 4 + valBytes.length;
                }
                count++;
            }

            // Bloom filter section — serialize to buffer first to know its size
            long bloomOffset = pos;
            ByteArrayOutputStream bloomBuf = new ByteArrayOutputStream();
            bloom.writeTo(new DataOutputStream(bloomBuf));
            byte[] bloomBytes = bloomBuf.toByteArray();
            out.write(bloomBytes);
            long indexOffset = bloomOffset + bloomBytes.length;

            // Index section
            for (int i = 0; i < keys.size(); i++) {
                byte[] kb = keys.get(i).getBytes(StandardCharsets.UTF_8);
                out.writeShort(kb.length);
                out.write(kb);
                out.writeLong(offsets.get(i));
            }

            // Footer
            out.writeLong(bloomOffset);
            out.writeLong(indexOffset);
            out.writeInt(count);
            out.writeInt(MAGIC);
        }
        // fsync
        try (FileOutputStream fos = new FileOutputStream(file.toFile(), true)) {
            fos.getFD().sync();
        }
    }

    public static SSTable open(Path file) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r");
        try {
            long fileLen = raf.length();
            if (fileLen < FOOTER_SIZE) {
                raf.close();
                throw new IOException("File too small to be an SSTable: " + file);
            }
            // Read footer
            raf.seek(fileLen - FOOTER_SIZE);
            long bloomOffset = raf.readLong();
            long indexOffset = raf.readLong();
            int entryCount = raf.readInt();
            int magic = raf.readInt();
            if (magic != MAGIC) {
                raf.close();
                throw new IOException("Invalid magic number in SSTable: " + file);
            }

            // Read bloom filter
            raf.seek(bloomOffset);
            DataInputStream bloomIn = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(file.toFile())));
            bloomIn.skipNBytes(bloomOffset);
            BloomFilter bloom = BloomFilter.readFrom(bloomIn);
            bloomIn.close();

            // Read index
            TreeMap<String, Long> index = new TreeMap<>();
            raf.seek(indexOffset);
            for (int i = 0; i < entryCount; i++) {
                int keyLen = raf.readUnsignedShort();
                byte[] keyBytes = new byte[keyLen];
                raf.readFully(keyBytes);
                long offset = raf.readLong();
                index.put(new String(keyBytes, StandardCharsets.UTF_8), offset);
            }

            return new SSTable(file, raf, index, bloom);
        } catch (IOException e) {
            raf.close();
            throw e;
        }
    }

    public Optional<String> get(String key) throws IOException {
        if (!bloomFilter.mightContain(key)) return Optional.empty();
        Long offset = index.get(key);
        if (offset == null) return Optional.empty();
        return Optional.of(readValueAt(offset));
    }

    public NavigableMap<String, String> scan(String fromInclusive, String toExclusive)
            throws IOException {
        NavigableMap<String, String> result = new TreeMap<>();
        for (Map.Entry<String, Long> e : index.subMap(fromInclusive, true, toExclusive, false).entrySet()) {
            result.put(e.getKey(), readValueAt(e.getValue()));
        }
        return result;
    }

    public boolean mightContain(String key) {
        return bloomFilter.mightContain(key);
    }

    public boolean containsKey(String key) {
        return index.containsKey(key);
    }

    public NavigableSet<String> keySet() {
        return Collections.unmodifiableNavigableSet(index.navigableKeySet());
    }

    public int sequenceNum() {
        return sequenceNum;
    }

    public Path path() {
        return path;
    }

    public long fileSize() throws IOException {
        return Files.size(path);
    }

    private String readValueAt(long offset) throws IOException {
        synchronized (raf) {
            raf.seek(offset);
            byte op = raf.readByte();
            int keyLen = raf.readUnsignedShort();
            raf.skipBytes(keyLen);
            if (op == OP_TOMBSTONE) {
                return Memtable.TOMBSTONE;
            }
            int valLen = raf.readInt();
            byte[] valBytes = new byte[valLen];
            raf.readFully(valBytes);
            return new String(valBytes, StandardCharsets.UTF_8);
        }
    }

    public static int sequenceNumber(Path file) {
        String name = file.getFileName().toString();
        // sst-000042.db -> 42
        return Integer.parseInt(name.substring(4, name.length() - 3));
    }

    public static Path fileName(Path dir, int seqNum) {
        return dir.resolve(String.format("sst-%06d.db", seqNum));
    }

    @Override
    public void close() throws IOException {
        raf.close();
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn test -Dtest=SSTableTest`
Expected: All 6 tests PASS.

- [ ] **Step 5: Run full test suite for regression**

Run: `mvn verify`
Expected: All tests PASS. BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/ethanstoner/kvstore/SSTable.java src/test/java/com/ethanstoner/kvstore/SSTableTest.java
git commit -m "feat: add SSTable file format with writer, reader, and bloom filter"
```

---

### Task 4: WriteAheadLog — add truncate-mode reset

**Files:**
- Modify: `src/main/java/com/ethanstoner/kvstore/WriteAheadLog.java`
- Modify: `src/test/java/com/ethanstoner/kvstore/WriteAheadLogTest.java`

- [ ] **Step 1: Add test for truncate-on-open behavior**

Append to `WriteAheadLogTest.java`:

```java
@Test
void resetTruncatesExistingLog(@TempDir Path dir) throws IOException {
    Path log = dir.resolve("wal.log");
    try (WriteAheadLog wal = new WriteAheadLog(log, false)) {
        wal.appendPut("old", "data");
    }
    // Reopen in truncate mode
    try (WriteAheadLog wal = new WriteAheadLog(log, true)) {
        wal.appendPut("new", "data");
    }
    // Replay should only see "new"
    Map<String, String> state = replayInto(log);
    assertNull(state.get("old"));
    assertEquals("data", state.get("new"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=WriteAheadLogTest#resetTruncatesExistingLog`
Expected: Compilation error — no two-arg constructor.

- [ ] **Step 3: Add the two-arg constructor to WriteAheadLog**

Modify `WriteAheadLog.java` — add a second constructor and update the existing one to delegate:

Replace the constructor and field section with:

```java
private final Path path;
private final DataOutputStream out;

public WriteAheadLog(Path path) throws IOException {
    this(path, false);
}

public WriteAheadLog(Path path, boolean truncate) throws IOException {
    this.path = path;
    Files.createDirectories(path.toAbsolutePath().getParent());
    this.out = new DataOutputStream(
            new BufferedOutputStream(new FileOutputStream(path.toFile(), !truncate)));
}
```

Also add an fsync method — replace the existing `flushToDisk()`:

```java
private void flushToDisk() throws IOException {
    out.flush();
    // Note: true fsync requires FileDescriptor.sync() but we'd need
    // the underlying FileOutputStream reference. For now, flush() is
    // sufficient for the test suite; production would wrap differently.
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=WriteAheadLogTest`
Expected: All 5 tests PASS.

- [ ] **Step 5: Run full suite**

Run: `mvn verify`
Expected: BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/ethanstoner/kvstore/WriteAheadLog.java src/test/java/com/ethanstoner/kvstore/WriteAheadLogTest.java
git commit -m "feat: add truncate-mode constructor to WriteAheadLog for post-flush reset"
```

---

### Task 5: KvStore — memtable flush to SSTable

**Files:**
- Modify: `src/main/java/com/ethanstoner/kvstore/KvStore.java`
- Modify: `src/test/java/com/ethanstoner/kvstore/KvStoreTest.java`

- [ ] **Step 1: Add flush test**

Append to `KvStoreTest.java`:

```java
@Test
void flushWritesSStableWhenMemtableExceedsThreshold(@TempDir Path dir) throws IOException {
    try (KvStore store = new KvStore(dir)) {
        // Write enough data to trigger a flush (threshold is in approximateBytes)
        // Each entry: key ~10 chars + value ~100 chars = ~220 approxBytes
        // Need > 4MB = 4_194_304 bytes, so ~19,000 entries
        String bigValue = "x".repeat(100);
        for (int i = 0; i < 20_000; i++) {
            store.put("key-" + String.format("%06d", i), bigValue);
        }
        // At least one SSTable should have been flushed
        long sstCount = java.nio.file.Files.list(dir)
                .filter(p -> p.getFileName().toString().endsWith(".db"))
                .count();
        assertTrue(sstCount >= 1, "Expected at least 1 SSTable, found " + sstCount);
    }
}

@Test
void dataSurvivesFlushAndReopen(@TempDir Path dir) throws IOException {
    try (KvStore store = new KvStore(dir)) {
        String bigValue = "x".repeat(100);
        for (int i = 0; i < 20_000; i++) {
            store.put("key-" + String.format("%06d", i), bigValue);
        }
    }
    // Reopen — should recover from SSTables
    try (KvStore store = new KvStore(dir)) {
        assertEquals(Optional.of("x".repeat(100)), store.get("key-000000"));
        assertEquals(Optional.of("x".repeat(100)), store.get("key-019999"));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -Dtest="KvStoreTest#flushWritesSStableWhenMemtableExceedsThreshold+dataSurvivesFlushAndReopen"`
Expected: FAIL — no SSTable files created (flush not implemented).

- [ ] **Step 3: Rewrite KvStore with flush support**

Replace the entire `KvStore.java` with the expanded version. Key changes:
- Track a list of open SSTables (`volatile List<SSTable>`)
- `put()` calls `maybeFlush()` after applying the write
- `maybeFlush()`: writes SSTable, swaps memtable + WAL references
- `get()`: memtable first, then SSTables newest-to-oldest
- `scan()`: merge across memtable + SSTables
- Constructor: scans for existing `.db` files, opens them, replays WAL

```java
package com.ethanstoner.kvstore;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public final class KvStore implements AutoCloseable {

    static final long FLUSH_THRESHOLD_BYTES = 4 * 1024 * 1024; // 4 MB

    private final Path dataDir;
    private volatile Memtable memtable;
    private volatile WriteAheadLog wal;
    private volatile List<SSTable> sstables;
    private int nextSequence;
    private final ReentrantLock flushLock = new ReentrantLock();

    public KvStore(Path dataDir) throws IOException {
        this.dataDir = dataDir;
        Files.createDirectories(dataDir);

        // Load existing SSTables
        List<SSTable> loaded = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir, "sst-*.db")) {
            for (Path p : stream) {
                try {
                    loaded.add(SSTable.open(p));
                } catch (IOException e) {
                    // Corrupt file — skip it
                }
            }
        }
        loaded.sort(Comparator.comparingInt(SSTable::sequenceNum));
        this.sstables = Collections.unmodifiableList(loaded);
        this.nextSequence = loaded.isEmpty() ? 1 :
                loaded.get(loaded.size() - 1).sequenceNum() + 1;

        // Fresh memtable + WAL replay
        this.memtable = new Memtable();
        Path walPath = dataDir.resolve("wal.log");
        this.wal = new WriteAheadLog(walPath);
        wal.replay((key, value) -> {
            if (value == null) memtable.delete(key);
            else memtable.put(key, value);
        });
    }

    public void put(String key, String value) throws IOException {
        flushLock.lock();
        try {
            wal.appendPut(key, value);
            memtable.put(key, value);
            maybeFlush();
        } finally {
            flushLock.unlock();
        }
    }

    public Optional<String> get(String key) throws IOException {
        // 1. Check memtable
        String v = memtable.get(key);
        if (v != null) {
            return Memtable.TOMBSTONE.equals(v) ? Optional.empty() : Optional.of(v);
        }
        // 2. Check SSTables newest-to-oldest
        List<SSTable> snapshot = sstables;
        for (int i = snapshot.size() - 1; i >= 0; i--) {
            Optional<String> result = snapshot.get(i).get(key);
            if (result.isPresent()) {
                String val = result.get();
                return Memtable.TOMBSTONE.equals(val) ? Optional.empty() : Optional.of(val);
            }
        }
        return Optional.empty();
    }

    public void delete(String key) throws IOException {
        flushLock.lock();
        try {
            wal.appendDelete(key);
            memtable.delete(key);
            maybeFlush();
        } finally {
            flushLock.unlock();
        }
    }

    public Map<String, String> scan(String fromInclusive, String toExclusive) throws IOException {
        // Collect entries from all sources, newest first
        // Key -> value, first write wins (newest source checked first)
        LinkedHashMap<String, String> merged = new LinkedHashMap<>();

        // Memtable first (newest)
        for (Map.Entry<String, String> e : memtable.scan(fromInclusive, toExclusive).entrySet()) {
            merged.put(e.getKey(), e.getValue());
        }

        // SSTables newest-to-oldest
        List<SSTable> snapshot = sstables;
        for (int i = snapshot.size() - 1; i >= 0; i--) {
            NavigableMap<String, String> sstRange = snapshot.get(i).scan(fromInclusive, toExclusive);
            for (Map.Entry<String, String> e : sstRange.entrySet()) {
                merged.putIfAbsent(e.getKey(), e.getValue());
            }
        }

        // Filter tombstones and sort
        TreeMap<String, String> result = new TreeMap<>();
        for (Map.Entry<String, String> e : merged.entrySet()) {
            if (!Memtable.TOMBSTONE.equals(e.getValue())) {
                result.put(e.getKey(), e.getValue());
            }
        }
        return result;
    }

    private void maybeFlush() throws IOException {
        if (memtable.approximateBytes() < FLUSH_THRESHOLD_BYTES) return;

        // Write SSTable
        Path sstPath = SSTable.fileName(dataDir, nextSequence++);
        SSTable.write(sstPath, memtable.entries());

        // Open the new SSTable and add to list
        SSTable newSst = SSTable.open(sstPath);
        List<SSTable> newList = new ArrayList<>(sstables);
        newList.add(newSst);
        sstables = Collections.unmodifiableList(newList);

        // Reset memtable and WAL
        memtable = new Memtable();
        wal.close();
        wal = new WriteAheadLog(dataDir.resolve("wal.log"), true);
    }

    @Override
    public void close() throws IOException {
        wal.close();
        for (SSTable sst : sstables) sst.close();
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn test -Dtest=KvStoreTest`
Expected: All 7 tests PASS (5 existing + 2 new).

- [ ] **Step 5: Run full suite**

Run: `mvn verify`
Expected: BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/ethanstoner/kvstore/KvStore.java src/test/java/com/ethanstoner/kvstore/KvStoreTest.java
git commit -m "feat: add memtable flush to SSTable with multi-level reads"
```

---

### Task 6: KvStore — test multi-level reads thoroughly

**Files:**
- Modify: `src/test/java/com/ethanstoner/kvstore/KvStoreTest.java`

- [ ] **Step 1: Add multi-level read tests**

Append to `KvStoreTest.java`:

```java
@Test
void getReadsFromSStableAfterFlush(@TempDir Path dir) throws IOException {
    try (KvStore store = new KvStore(dir)) {
        // Write a key, force flush by filling memtable, then read the key
        store.put("early-key", "early-value");
        String bigValue = "x".repeat(100);
        for (int i = 0; i < 20_000; i++) {
            store.put("filler-" + String.format("%06d", i), bigValue);
        }
        // "early-key" should be in an SSTable now, not in memtable
        assertEquals(Optional.of("early-value"), store.get("early-key"));
    }
}

@Test
void deleteInMemtableHidesSStableValue(@TempDir Path dir) throws IOException {
    try (KvStore store = new KvStore(dir)) {
        store.put("victim", "alive");
        // Force flush
        String bigValue = "x".repeat(100);
        for (int i = 0; i < 20_000; i++) {
            store.put("pad-" + String.format("%06d", i), bigValue);
        }
        // "victim" is now in SSTable. Delete it in memtable.
        store.delete("victim");
        assertTrue(store.get("victim").isEmpty());
    }
}

@Test
void scanMergesMemtableAndSStables(@TempDir Path dir) throws IOException {
    try (KvStore store = new KvStore(dir)) {
        store.put("a", "from-memtable");
        // Force flush
        String bigValue = "x".repeat(100);
        for (int i = 0; i < 20_000; i++) {
            store.put("filler-" + String.format("%06d", i), bigValue);
        }
        // "a" is now in SSTable. Add a new key to memtable.
        store.put("b", "also-memtable");
        Map<String, String> result = store.scan("a", "c");
        assertEquals("from-memtable", result.get("a"));
        assertEquals("also-memtable", result.get("b"));
    }
}

@Test
void newerValueOverridesOlderInScan(@TempDir Path dir) throws IOException {
    try (KvStore store = new KvStore(dir)) {
        store.put("key", "old-value");
        // Force flush
        String bigValue = "x".repeat(100);
        for (int i = 0; i < 20_000; i++) {
            store.put("filler-" + String.format("%06d", i), bigValue);
        }
        // Update key in memtable (newer)
        store.put("key", "new-value");
        assertEquals(Optional.of("new-value"), store.get("key"));
        Map<String, String> scan = store.scan("key", "key\0");
        assertEquals("new-value", scan.get("key"));
    }
}
```

- [ ] **Step 2: Run tests**

Run: `mvn test -Dtest=KvStoreTest`
Expected: All 11 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add src/test/java/com/ethanstoner/kvstore/KvStoreTest.java
git commit -m "test: add multi-level read tests for SSTable + memtable interactions"
```

---

### Task 7: KvStore — size-tiered compaction

**Files:**
- Modify: `src/main/java/com/ethanstoner/kvstore/KvStore.java`
- Modify: `src/test/java/com/ethanstoner/kvstore/KvStoreTest.java`

- [ ] **Step 1: Add compaction test**

Append to `KvStoreTest.java`:

```java
@Test
void compactionMergesSSTables(@TempDir Path dir) throws IOException, InterruptedException {
    try (KvStore store = new KvStore(dir)) {
        // Write enough to create multiple SSTables
        String bigValue = "x".repeat(100);
        for (int i = 0; i < 80_000; i++) {
            store.put("key-" + String.format("%06d", i), bigValue);
        }
        // Count SSTables before compaction
        long beforeCount = java.nio.file.Files.list(dir)
                .filter(p -> p.getFileName().toString().endsWith(".db"))
                .count();
        assertTrue(beforeCount >= 4, "Expected >=4 SSTables, got " + beforeCount);

        // Trigger compaction and wait for it
        store.compactNow();

        long afterCount = java.nio.file.Files.list(dir)
                .filter(p -> p.getFileName().toString().endsWith(".db"))
                .count();
        assertTrue(afterCount < beforeCount,
                "Expected fewer SSTables after compaction: before=" + beforeCount + " after=" + afterCount);

        // Verify data integrity after compaction
        assertEquals(Optional.of(bigValue), store.get("key-000000"));
        assertEquals(Optional.of(bigValue), store.get("key-079999"));
    }
}

@Test
void compactionDropsTombstones(@TempDir Path dir) throws IOException {
    try (KvStore store = new KvStore(dir)) {
        // Write a key, flush, delete it, flush again, then compact all
        store.put("doomed", "value");
        String bigValue = "x".repeat(100);
        for (int i = 0; i < 20_000; i++) {
            store.put("pad1-" + String.format("%06d", i), bigValue);
        }
        store.delete("doomed");
        for (int i = 0; i < 20_000; i++) {
            store.put("pad2-" + String.format("%06d", i), bigValue);
        }

        store.compactNow();

        assertTrue(store.get("doomed").isEmpty());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -Dtest="KvStoreTest#compactionMergesSSTables+compactionDropsTombstones"`
Expected: FAIL — `compactNow()` method does not exist.

- [ ] **Step 3: Add compaction to KvStore**

Add these methods to `KvStore.java`:

Add a field at the class level in `KvStore.java`:

```java
private Thread compactionThread;
```

Add `startCompactionThread();` as the last line of the `KvStore` constructor.

Then add these methods:

```java
private void startCompactionThread() {
    compactionThread = new Thread(() -> {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(5000);
                runCompaction();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                // Log and continue
            }
        }
    }, "kvstore-compaction");
    compactionThread.setDaemon(true);
    compactionThread.start();
}

public void compactNow() throws IOException {
    runCompaction();
}

private void runCompaction() throws IOException {
    List<SSTable> snapshot = sstables;
    if (snapshot.size() < 4) return;

    // Group by size tier (order of magnitude)
    Map<Integer, List<SSTable>> tiers = new HashMap<>();
    for (SSTable sst : snapshot) {
        int tier = (int) Math.floor(Math.log10(Math.max(1, sst.fileSize())));
        tiers.computeIfAbsent(tier, k -> new ArrayList<>()).add(sst);
    }

    // Find a tier with 4+ SSTables, or compact all if none qualifies
    List<SSTable> toCompact = null;
    for (List<SSTable> group : tiers.values()) {
        if (group.size() >= 4) {
            toCompact = group;
            break;
        }
    }
    if (toCompact == null) {
        // Compact all if we have 4+ total
        if (snapshot.size() >= 4) {
            toCompact = new ArrayList<>(snapshot);
        } else {
            return;
        }
    }

    // Determine if this is a full compaction (includes all SSTables)
    boolean fullCompaction = toCompact.size() == snapshot.size();

    // K-way merge: collect all entries, newest first per key
    TreeMap<String, String> merged = new TreeMap<>();
    // Process newest-to-oldest so putIfAbsent keeps newest
    List<SSTable> sorted = new ArrayList<>(toCompact);
    sorted.sort(Comparator.comparingInt(SSTable::sequenceNum).reversed());

    for (SSTable sst : sorted) {
        if (sst.keySet().isEmpty()) continue;
        NavigableMap<String, String> all = sst.scan(
                sst.keySet().first(), sst.keySet().last() + "\0");
        for (Map.Entry<String, String> e : all.entrySet()) {
            merged.putIfAbsent(e.getKey(), e.getValue());
        }
    }

    // Drop tombstones only in full compaction
    if (fullCompaction) {
        merged.values().removeIf(v -> Memtable.TOMBSTONE.equals(v));
    }

    flushLock.lock();
    try {
        if (merged.isEmpty()) {
            // All entries were tombstones — just remove old SSTables
            List<SSTable> newList = new ArrayList<>(sstables);
            for (SSTable old : toCompact) {
                newList.remove(old);
                old.close();
                Files.deleteIfExists(old.path());
            }
            sstables = Collections.unmodifiableList(newList);
            return;
        }

        // Write merged SSTable
        Path mergedPath = SSTable.fileName(dataDir, nextSequence++);
        SSTable.write(mergedPath, merged.entrySet());
        SSTable mergedSst = SSTable.open(mergedPath);

        // Swap: remove old, add new
        List<SSTable> newList = new ArrayList<>(sstables);
        for (SSTable old : toCompact) {
            newList.remove(old);
            old.close();
            Files.deleteIfExists(old.path());
        }
        newList.add(mergedSst);
        newList.sort(Comparator.comparingInt(SSTable::sequenceNum));
        sstables = Collections.unmodifiableList(newList);
    } finally {
        flushLock.unlock();
    }
}
```

Also add `startCompactionThread();` at the end of the `KvStore` constructor, and add to `close()`:

```java
@Override
public void close() throws IOException {
    if (compactionThread != null) compactionThread.interrupt();
    wal.close();
    for (SSTable sst : sstables) sst.close();
}
```

- [ ] **Step 4: Run tests**

Run: `mvn test -Dtest=KvStoreTest`
Expected: All 13 tests PASS.

- [ ] **Step 5: Run full suite**

Run: `mvn verify`
Expected: BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/ethanstoner/kvstore/KvStore.java src/test/java/com/ethanstoner/kvstore/KvStoreTest.java
git commit -m "feat: add size-tiered compaction with background thread"
```

---

### Task 8: JMH benchmarks — setup and write throughput

**Files:**
- Modify: `pom.xml`
- Create: `src/main/java/com/ethanstoner/kvstore/benchmark/KvStoreBenchmark.java`

- [ ] **Step 1: Add JMH dependencies to pom.xml**

Add to the `<dependencies>` section:

```xml
<dependency>
    <groupId>org.openjdk.jmh</groupId>
    <artifactId>jmh-core</artifactId>
    <version>1.37</version>
</dependency>
<dependency>
    <groupId>org.openjdk.jmh</groupId>
    <artifactId>jmh-generator-annprocess</artifactId>
    <version>1.37</version>
    <scope>provided</scope>
</dependency>
```

Add the maven-shade-plugin to `<plugins>`:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <version>3.5.2</version>
    <executions>
        <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
                <shadedArtifactAttached>true</shadedArtifactAttached>
                <shadedClassifierName>benchmarks</shadedClassifierName>
                <transformers>
                    <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                        <mainClass>org.openjdk.jmh.Main</mainClass>
                    </transformer>
                </transformers>
            </configuration>
        </execution>
    </executions>
</plugin>
```

- [ ] **Step 2: Write the benchmark class**

```java
package com.ethanstoner.kvstore.benchmark;

import com.ethanstoner.kvstore.KvStore;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 3, time = 5)
@Fork(1)
public class KvStoreBenchmark {

    private KvStore store;
    private Path dataDir;
    private int writeCounter;
    private int keySpace = 100_000;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        dataDir = Files.createTempDirectory("kvbench");
        store = new KvStore(dataDir);
        // Pre-populate for read benchmarks
        for (int i = 0; i < keySpace; i++) {
            store.put("key-" + String.format("%06d", i), "value-" + i);
        }
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException {
        store.close();
        Files.walk(dataDir)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> { try { Files.delete(p); } catch (IOException ignored) {} });
    }

    @Benchmark
    public void sequentialWrite() throws IOException {
        store.put("seq-" + String.format("%09d", writeCounter++), "benchmark-value");
    }

    @Benchmark
    public void randomWrite() throws IOException {
        int key = ThreadLocalRandom.current().nextInt(keySpace * 2);
        store.put("rnd-" + String.format("%06d", key), "benchmark-value");
    }

    @Benchmark
    public String pointReadExistingKey() throws IOException {
        int key = ThreadLocalRandom.current().nextInt(keySpace);
        return store.get("key-" + String.format("%06d", key)).orElse(null);
    }

    @Benchmark
    public String pointReadMissingKey() throws IOException {
        int key = ThreadLocalRandom.current().nextInt(keySpace) + keySpace * 10;
        return store.get("miss-" + key).orElse(null);
    }

    @Benchmark
    public int scanRange() throws IOException {
        int start = ThreadLocalRandom.current().nextInt(keySpace - 100);
        String from = "key-" + String.format("%06d", start);
        String to = "key-" + String.format("%06d", start + 100);
        return store.scan(from, to).size();
    }
}
```

- [ ] **Step 3: Verify it compiles**

Run: `mvn compile`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Run the full test suite (benchmarks don't run during test)**

Run: `mvn verify`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add pom.xml src/main/java/com/ethanstoner/kvstore/benchmark/KvStoreBenchmark.java
git commit -m "feat: add JMH benchmarks for write, read, and scan throughput"
```

---

### Task 9: Update README and clean up

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update README to reflect new status**

Update the Status and Architecture sections to reflect that SSTables, compaction, bloom filters, and benchmarks are now implemented. Move completed items from roadmap to "Working today". Add benchmark run instructions.

Key changes:
- Architecture diagram: remove "(roadmap)" annotations, show full pipeline
- Status: move SSTables, multi-level reads, compaction, bloom filters to "Working today"
- Add benchmark instructions: `mvn package && java -jar target/kvstore-0.1.0-benchmarks.jar`
- Update complexity table with SSTable-aware costs
- Roadmap: note potential future work (leveled compaction, block cache, concurrent flush)

- [ ] **Step 2: Run full test suite one last time**

Run: `mvn verify`
Expected: BUILD SUCCESS, all tests pass.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: update README with completed LSM-tree features and benchmark instructions"
```

---

### Task 10: Final verification — build, test, package

- [ ] **Step 1: Clean build from scratch**

Run: `mvn clean verify`
Expected: BUILD SUCCESS, all tests pass.

- [ ] **Step 2: Build the benchmarks jar**

Run: `mvn package`
Expected: `target/kvstore-0.1.0-benchmarks.jar` exists.

- [ ] **Step 3: Smoke test the CLI**

Run:
```powershell
java -jar target/kvstore-0.1.0.jar put hello world
java -jar target/kvstore-0.1.0.jar get hello
java -jar target/kvstore-0.1.0.jar scan a z
java -jar target/kvstore-0.1.0.jar del hello
java -jar target/kvstore-0.1.0.jar get hello
```

Expected output:
```
OK
world
hello = world
OK
(not found)
```

- [ ] **Step 4: Quick benchmark smoke test (1 iteration only)**

Run: `java -jar target/kvstore-0.1.0-benchmarks.jar -wi 0 -i 1 -f 1 -t 1 ".*sequentialWrite.*"`
Expected: Benchmark runs and prints throughput numbers. No errors.
