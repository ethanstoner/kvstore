package com.ethanstoner.kvstore;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Immutable, sorted, on-disk table of key-value pairs.
 *
 * <p>File layout (big-endian):
 * <pre>
 * [Data Section]  — one entry per key, sorted
 *   op:       1 byte   (0 = TOMBSTONE, 1 = VALUE)
 *   keyLen:   2 bytes  (unsigned short)
 *   key:      keyLen bytes (UTF-8)
 *   valueLen: 4 bytes  (only when op = VALUE)
 *   value:    valueLen bytes (only when op = VALUE)
 *
 * [Bloom Filter Section]  — serialized by BloomFilter.writeTo
 *
 * [Index Section]  — one entry per key
 *   keyLen: 2 bytes
 *   key:    keyLen bytes (UTF-8)
 *   offset: 8 bytes (absolute file position of the data entry)
 *
 * [Footer — always exactly 24 bytes at end of file]
 *   bloomOffset: 8 bytes
 *   indexOffset: 8 bytes
 *   entryCount:  4 bytes
 *   magic:       4 bytes  (0x4B565354 = "KVST")
 * </pre>
 */
public final class SSTable implements AutoCloseable {

    // -------------------------------------------------------------------------
    // Constants
    // -------------------------------------------------------------------------

    private static final int  MAGIC        = 0x4B565354; // "KVST"
    private static final int  FOOTER_BYTES = 24;
    private static final byte OP_TOMBSTONE      = 0;
    private static final byte OP_VALUE          = 1;
    private static final byte OP_VALUE_DEFLATE  = 2;

    /** Values whose UTF-8 byte length exceeds this threshold are compressed. */
    private static final int COMPRESS_THRESHOLD_BYTES = 64;

    private static final Pattern FILENAME_PATTERN =
            Pattern.compile("(?:sst-|L(\\d+)-)(\\d+)\\.db");

    // -------------------------------------------------------------------------
    // Fields
    // -------------------------------------------------------------------------

    private final Path                    filePath;
    private final RandomAccessFile        raf;
    private final BloomFilter             bloom;
    private final TreeMap<String, Long>   index;   // key -> absolute data offset
    private final int                     entryCount;
    private final int                     level;

    /** Optional shared LRU cache; {@code null} means no caching. */
    private BlockCache cache;

    // -------------------------------------------------------------------------
    // Private constructor — use open() or write() + open()
    // -------------------------------------------------------------------------

    private SSTable(Path filePath,
                    RandomAccessFile raf,
                    BloomFilter bloom,
                    TreeMap<String, Long> index,
                    int entryCount) {
        this.filePath   = filePath;
        this.raf        = raf;
        this.bloom      = bloom;
        this.index      = index;
        this.entryCount = entryCount;
        this.level      = levelOf(filePath);
    }

    // =========================================================================
    // Static factory: write
    // =========================================================================

    /**
     * Writes {@code sortedEntries} to {@code file} in SSTable format.
     *
     * <p>Entries must already be sorted by key (ascending). Tombstones are
     * written with {@code op = 0}; normal values with {@code op = 1}.
     *
     * @param file          destination path (will be created / overwritten)
     * @param sortedEntries key-value pairs in ascending key order
     * @throws IOException on any I/O failure
     */
    public static void write(Path file,
                             Iterable<Map.Entry<String, String>> sortedEntries)
            throws IOException {

        // Collect entries so we can count them for the bloom filter.
        List<Map.Entry<String, String>> entries = new ArrayList<>();
        for (Map.Entry<String, String> e : sortedEntries) {
            entries.add(e);
        }

        // Build bloom filter over all keys.
        BloomFilter bf = new BloomFilter(Math.max(1, entries.size()), 0.01);
        for (Map.Entry<String, String> e : entries) {
            bf.add(e.getKey());
        }

        // Serialize bloom filter to a byte array so we know its size before
        // writing the data section (needed to compute section offsets).
        byte[] bloomBytes;
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            bf.writeTo(new DataOutputStream(baos));
            bloomBytes = baos.toByteArray();
        }

        // Write everything to the file.
        try (FileOutputStream fos = new FileOutputStream(file.toFile());
             DataOutputStream out = new DataOutputStream(
                     new BufferedOutputStream(fos))) {

            // --- Data section ---------------------------------------------------
            // Track offsets for the index.
            long[] dataOffsets = new long[entries.size()];
            long pos = 0;

            for (int i = 0; i < entries.size(); i++) {
                dataOffsets[i] = pos;
                Map.Entry<String, String> e = entries.get(i);
                String key   = e.getKey();
                String value = e.getValue();

                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                boolean isTombstone = Memtable.TOMBSTONE.equals(value);

                if (isTombstone) {
                    out.writeByte(OP_TOMBSTONE);
                    out.writeShort(keyBytes.length);
                    out.write(keyBytes);
                    pos += 1 + 2 + keyBytes.length;
                } else {
                    byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);
                    byte[] payload;
                    byte op;
                    if (valBytes.length > COMPRESS_THRESHOLD_BYTES) {
                        byte[] compressed = deflate(valBytes);
                        if (compressed.length < valBytes.length) {
                            op      = OP_VALUE_DEFLATE;
                            payload = compressed;
                        } else {
                            // Compression didn't help (e.g. already-random data). Store raw.
                            op      = OP_VALUE;
                            payload = valBytes;
                        }
                    } else {
                        op      = OP_VALUE;
                        payload = valBytes;
                    }
                    out.writeByte(op);
                    out.writeShort(keyBytes.length);
                    out.write(keyBytes);
                    out.writeInt(payload.length);
                    out.write(payload);
                    pos += 1 + 2 + keyBytes.length + 4 + payload.length;
                }
            }

            long bloomOffset = pos;

            // --- Bloom filter section -------------------------------------------
            out.write(bloomBytes);
            pos += bloomBytes.length;

            long indexOffset = pos;

            // --- Index section --------------------------------------------------
            for (int i = 0; i < entries.size(); i++) {
                byte[] keyBytes =
                        entries.get(i).getKey().getBytes(StandardCharsets.UTF_8);
                out.writeShort(keyBytes.length);
                out.write(keyBytes);
                out.writeLong(dataOffsets[i]);
                pos += 2 + keyBytes.length + 8;
            }

            // --- Footer (24 bytes) ----------------------------------------------
            out.writeLong(bloomOffset);
            out.writeLong(indexOffset);
            out.writeInt(entries.size());
            out.writeInt(MAGIC);

            // Flush to OS buffer before sync.
            out.flush();
            fos.getFD().sync();
        }
    }

    // =========================================================================
    // Static factory: open
    // =========================================================================

    /**
     * Opens an existing SSTable for reading.
     *
     * <p>Reads the footer to locate and load the bloom filter and the index.
     * The data section is accessed lazily via random seeks.
     *
     * @param file path to an existing SSTable file
     * @return an open {@link SSTable} (caller must close it)
     * @throws IOException if the file cannot be read or its magic is wrong
     */
    public static SSTable open(Path file) throws IOException {
        long fileSize = Files.size(file);
        if (fileSize < FOOTER_BYTES) {
            throw new IOException(
                    "File too small to be a valid SSTable: " + file);
        }

        RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r");
        try {
            // --- Read footer ----------------------------------------------------
            raf.seek(fileSize - FOOTER_BYTES);
            long bloomOffset = raf.readLong();
            long indexOffset = raf.readLong();
            int  count       = raf.readInt();
            int  magic       = raf.readInt();

            if (magic != MAGIC) {
                throw new IOException(
                        "Bad SSTable magic: 0x" + Integer.toHexString(magic)
                        + " in " + file);
            }

            // Sanity-check offsets.
            if (bloomOffset < 0 || bloomOffset >= fileSize
                    || indexOffset < 0 || indexOffset >= fileSize
                    || bloomOffset > indexOffset) {
                throw new IOException("Corrupt SSTable offsets in " + file);
            }

            // --- Read bloom filter ----------------------------------------------
            raf.seek(bloomOffset);
            int bloomLen = (int)(indexOffset - bloomOffset);
            byte[] bloomBytes = new byte[bloomLen];
            raf.readFully(bloomBytes);
            BloomFilter bf;
            try (DataInputStream dis = new DataInputStream(
                    new ByteArrayInputStream(bloomBytes))) {
                bf = BloomFilter.readFrom(dis);
            }

            // --- Read index -----------------------------------------------------
            raf.seek(indexOffset);
            long indexEnd = fileSize - FOOTER_BYTES;
            TreeMap<String, Long> idx = new TreeMap<>();
            while (raf.getFilePointer() < indexEnd) {
                int    kLen   = raf.readUnsignedShort();
                byte[] kBytes = new byte[kLen];
                raf.readFully(kBytes);
                long   offset = raf.readLong();
                idx.put(new String(kBytes, StandardCharsets.UTF_8), offset);
            }

            return new SSTable(file, raf, bf, idx, count);

        } catch (IOException e) {
            // Close the RAF if construction fails.
            try { raf.close(); } catch (IOException ignored) {}
            throw e;
        }
    }

    // =========================================================================
    // Public API
    // =========================================================================

    /**
     * Attaches a shared {@link BlockCache} to this SSTable. Once attached,
     * {@link #readValueAt} checks the cache before doing a disk seek, and
     * populates it on a miss.
     *
     * @param cache the shared cache, or {@code null} to detach
     */
    public void attachCache(BlockCache cache) {
        this.cache = cache;
    }

    /**
     * Point lookup.
     *
     * <p>Returns {@code Optional.empty()} when the key is definitely absent
     * (bloom filter miss or index miss). Returns {@code Optional.of(value)}
     * for a live value and {@code Optional.of(TOMBSTONE)} for a deleted key.
     */
    public Optional<String> get(String key) throws IOException {
        if (!bloom.mightContain(key)) {
            return Optional.empty();
        }
        Long offset = index.get(key);
        if (offset == null) {
            return Optional.empty();
        }
        String value = readValueAt(offset);
        return Optional.of(value);
    }

    /**
     * Range scan — inclusive {@code from}, exclusive {@code to}.
     *
     * <p>Tombstones are included in the result (callers must filter them out
     * if needed, e.g. during compaction or in {@link KvStore}).
     */
    public NavigableMap<String, String> scan(String from, String to)
            throws IOException {
        NavigableMap<String, Long> subIndex =
                index.subMap(from, true, to, false);
        TreeMap<String, String> result = new TreeMap<>();
        for (Map.Entry<String, Long> e : subIndex.entrySet()) {
            result.put(e.getKey(), readValueAt(e.getValue()));
        }
        return result;
    }

    /** @return {@code true} if the bloom filter says the key might be present. */
    public boolean mightContain(String key) {
        return bloom.mightContain(key);
    }

    /** @return {@code true} if the key is in the in-memory index. */
    public boolean containsKey(String key) {
        return index.containsKey(key);
    }

    /**
     * @return an unmodifiable, navigable view of all keys in this SSTable.
     */
    public NavigableSet<String> keySet() {
        return Collections.unmodifiableNavigableSet(index.navigableKeySet());
    }

    /**
     * @return the sequence number encoded in the filename, or -1 if the
     *         filename does not match the expected pattern.
     */
    public int sequenceNum() {
        return sequenceNumber(filePath);
    }

    /** @return the path of the underlying file. */
    public Path path() {
        return filePath;
    }

    /** @return the size of the underlying file in bytes. */
    public long fileSize() throws IOException {
        return Files.size(filePath);
    }

    /** @return the number of entries (including tombstones) stored in this SSTable. */
    public int entryCount() {
        return entryCount;
    }

    /** @return the level this SSTable belongs to (0 for L0, 1 for L1, etc.). */
    public int level() {
        return level;
    }

    // =========================================================================
    // Static helpers
    // =========================================================================

    /**
     * Extracts the sequence number from a filename like {@code "sst-000042.db"}
     * or {@code "L1-000005.db"}.
     *
     * @param file path whose {@linkplain Path#getFileName() filename} to parse
     * @return the sequence number, or -1 if the name does not match
     */
    public static int sequenceNumber(Path file) {
        String name = file.getFileName().toString();
        Matcher m = FILENAME_PATTERN.matcher(name);
        if (!m.matches()) return -1;
        // group(1) = level digits (null for legacy sst- prefix), group(2) = seq digits
        return Integer.parseInt(m.group(2));
    }

    /**
     * Extracts the level from a filename like {@code "L1-000005.db"} or
     * {@code "sst-000042.db"} (legacy = level 0).
     *
     * @param file path whose {@linkplain Path#getFileName() filename} to parse
     * @return the level, or -1 if the name does not match
     */
    public static int levelOf(Path file) {
        String name = file.getFileName().toString();
        Matcher m = FILENAME_PATTERN.matcher(name);
        if (!m.matches()) return -1;
        return m.group(1) == null ? 0 : Integer.parseInt(m.group(1));
    }

    /**
     * Generates the canonical filename for an SSTable with the given sequence
     * number, e.g. {@code sequenceNumber = 42} → {@code "sst-000042.db"}.
     *
     * <p>Kept for backward compatibility; new compaction code uses
     * {@link #fileName(Path, int, int)}.
     *
     * @param dir    directory in which the file will live
     * @param seqNum sequence number (must be &gt;= 0)
     * @return full path {@code dir/sst-NNNNNN.db}
     */
    public static Path fileName(Path dir, int seqNum) {
        return dir.resolve(String.format("sst-%06d.db", seqNum));
    }

    /**
     * Generates the leveled filename for an SSTable, e.g. level=1, seq=5 →
     * {@code "L1-000005.db"}.
     *
     * @param dir   directory in which the file will live
     * @param level the LSM level (0, 1, 2, …)
     * @param seq   sequence number (must be &gt;= 0)
     * @return full path {@code dir/L<level>-NNNNNN.db}
     */
    public static Path fileName(Path dir, int level, int seq) {
        return dir.resolve(String.format("L%d-%06d.db", level, seq));
    }

    // =========================================================================
    // AutoCloseable
    // =========================================================================

    @Override
    public void close() throws IOException {
        raf.close();
    }

    // =========================================================================
    // Private helpers
    // =========================================================================

    /** Compresses {@code data} with Deflate and returns the compressed bytes. */
    private static byte[] deflate(byte[] data) {
        java.util.zip.Deflater deflater =
                new java.util.zip.Deflater(java.util.zip.Deflater.DEFAULT_COMPRESSION);
        try {
            deflater.setInput(data);
            deflater.finish();
            ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length);
            byte[] buf = new byte[1024];
            while (!deflater.finished()) {
                int n = deflater.deflate(buf);
                baos.write(buf, 0, n);
            }
            return baos.toByteArray();
        } finally {
            deflater.end();
        }
    }

    /** Decompresses Deflate-compressed {@code compressed} and returns the original bytes. */
    private static byte[] inflate(byte[] compressed) throws IOException {
        java.util.zip.Inflater inflater = new java.util.zip.Inflater();
        try {
            inflater.setInput(compressed);
            ByteArrayOutputStream baos = new ByteArrayOutputStream(compressed.length * 2);
            byte[] buf = new byte[1024];
            while (!inflater.finished()) {
                int n;
                try {
                    n = inflater.inflate(buf);
                } catch (java.util.zip.DataFormatException e) {
                    throw new IOException("inflate failed", e);
                }
                if (n == 0) {
                    if (inflater.needsInput() || inflater.needsDictionary()) break;
                }
                baos.write(buf, 0, n);
            }
            return baos.toByteArray();
        } finally {
            inflater.end();
        }
    }

    /**
     * Reads the value (or tombstone sentinel) stored at the given absolute
     * file offset. Checks the {@link BlockCache} before seeking; populates
     * the cache on a miss. Synchronized on {@code raf} for thread safety.
     */
    private String readValueAt(long offset) throws IOException {
        if (cache != null) {
            String cached = cache.get(sequenceNum(), offset);
            if (cached != null) return cached;
        }
        String value;
        synchronized (raf) {
            raf.seek(offset);
            byte op = raf.readByte();
            int kLen = raf.readUnsignedShort();
            raf.skipBytes(kLen);
            if (op == OP_TOMBSTONE) {
                value = Memtable.TOMBSTONE;
            } else {
                int vLen = raf.readInt();
                byte[] vBytes = new byte[vLen];
                raf.readFully(vBytes);
                if (op == OP_VALUE_DEFLATE) {
                    byte[] decompressed = inflate(vBytes);
                    value = new String(decompressed, StandardCharsets.UTF_8);
                } else if (op == OP_VALUE) {
                    value = new String(vBytes, StandardCharsets.UTF_8);
                } else {
                    throw new IOException("unknown op byte: " + op);
                }
            }
        }
        if (cache != null) {
            cache.put(sequenceNum(), offset, value);
        }
        return value;
    }
}
