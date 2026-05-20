package com.ethanstoner.kvstore;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.BiConsumer;
import java.util.zip.CRC32;

/**
 * The Write-Ahead Log (WAL): durability for the {@link Memtable}.
 *
 * <p>The memtable lives in RAM, so a crash would lose every un-flushed write.
 * To prevent that, <b>every</b> mutation is appended to this log on disk
 * <i>before</i> it is applied to the memtable. After a crash we re-open the
 * store and {@link #replay} the log to rebuild the exact memtable state.
 *
 * <p>This "log first, then apply" ordering is the entire idea behind crash
 * recovery in real databases (Postgres, SQLite, RocksDB all do a version of
 * this). Get the ordering wrong and you can acknowledge a write that a crash
 * then loses.
 *
 * <p>Record format (binary, one frame per mutation):
 * <pre>
 *   int (4 bytes BE)   recordLen  — byte length of the payload that follows
 *   byte[recordLen]    payload    — op byte + writeUTF(key) [+ writeUTF(value)]
 *                                   op: 0 = DELETE, 1 = PUT
 *   int (4 bytes BE)   crc32      — CRC32 checksum of the payload bytes
 * </pre>
 *
 * <p>On replay, any truncated tail (short read) or CRC mismatch is treated as
 * corruption: replay stops at the first bad frame, preserving all records that
 * were written before the corruption.
 */
public final class WriteAheadLog implements AutoCloseable {

    private static final byte OP_DELETE = 0;
    private static final byte OP_PUT    = 1;

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

    public synchronized void appendPut(String key, String value) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeByte(OP_PUT);
        dos.writeUTF(key);
        dos.writeUTF(value);
        dos.flush();
        writeFramedRecord(baos.toByteArray());
    }

    public synchronized void appendDelete(String key) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeByte(OP_DELETE);
        dos.writeUTF(key);
        dos.flush();
        writeFramedRecord(baos.toByteArray());
    }

    /**
     * Writes a framed record: [recordLen: 4 bytes][payload: recordLen bytes][crc32: 4 bytes].
     */
    private void writeFramedRecord(byte[] payload) throws IOException {
        CRC32 crc = new CRC32();
        crc.update(payload);
        out.writeInt(payload.length);
        out.write(payload);
        out.writeInt((int) crc.getValue());
        flushToDisk();
    }

    /**
     * Replays every record in order into {@code apply}, which receives
     * {@code (key, value)} where a {@code null} value means a delete.
     * Called once on startup to rebuild the memtable.
     *
     * <p>Replay is fault-tolerant: a truncated tail or a CRC mismatch causes
     * replay to stop cleanly (the record and everything after it is discarded).
     * This correctly handles the case where the process crashed mid-write.
     */
    public void replay(BiConsumer<String, String> apply) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (DataInputStream in = new DataInputStream(new FileInputStream(path.toFile()))) {
            long offset = 0;
            while (true) {
                // --- read frame length ---
                int recordLen;
                try {
                    recordLen = in.readInt();
                } catch (EOFException eof) {
                    return; // clean end of log
                }
                offset += 4;

                // --- read payload ---
                byte[] payload = new byte[recordLen];
                int read = 0;
                while (read < recordLen) {
                    int n = in.read(payload, read, recordLen - read);
                    if (n < 0) {
                        System.err.println("WAL corruption detected at offset " + offset
                                + ": short read in payload, replay stopping");
                        return;
                    }
                    read += n;
                }
                offset += recordLen;

                // --- read stored CRC ---
                int storedCrc;
                try {
                    storedCrc = in.readInt();
                } catch (EOFException eof) {
                    System.err.println("WAL corruption detected at offset " + offset
                            + ": truncated CRC, replay stopping");
                    return;
                }
                offset += 4;

                // --- verify CRC ---
                CRC32 crc = new CRC32();
                crc.update(payload);
                int computedCrc = (int) crc.getValue();
                if (computedCrc != storedCrc) {
                    System.err.println("WAL corruption detected at offset " + offset
                            + ": CRC mismatch (expected " + Integer.toUnsignedString(storedCrc)
                            + ", got " + Integer.toUnsignedString(computedCrc) + "), replay stopping");
                    return;
                }

                // --- decode payload ---
                DataInputStream payloadIn = new DataInputStream(new ByteArrayInputStream(payload));
                byte op = payloadIn.readByte();
                String key = payloadIn.readUTF();
                if (op == OP_PUT) {
                    apply.accept(key, payloadIn.readUTF());
                } else {
                    apply.accept(key, null);
                }
            }
        }
    }

    /**
     * Flushes the output stream through the OS buffer. Without this a crash
     * could lose an "acknowledged" write that was still in a buffer.
     */
    private void flushToDisk() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}
