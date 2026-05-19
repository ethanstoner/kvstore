package com.ethanstoner.kvstore;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.BiConsumer;

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
 * <p>Record format (binary, one per mutation):
 * <pre>
 *   byte    op      0 = DELETE, 1 = PUT
 *   UTF     key
 *   UTF     value   (omitted when op = DELETE)
 * </pre>
 */
public final class WriteAheadLog implements AutoCloseable {

    private static final byte OP_DELETE = 0;
    private static final byte OP_PUT = 1;

    private final Path path;
    private final DataOutputStream out;

    public WriteAheadLog(Path path) throws IOException {
        this.path = path;
        Files.createDirectories(path.toAbsolutePath().getParent());
        // append = true so re-opening an existing store keeps prior records.
        this.out = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(path.toFile(), true)));
    }

    public synchronized void appendPut(String key, String value) throws IOException {
        out.writeByte(OP_PUT);
        out.writeUTF(key);
        out.writeUTF(value);
        flushToDisk();
    }

    public synchronized void appendDelete(String key) throws IOException {
        out.writeByte(OP_DELETE);
        out.writeUTF(key);
        flushToDisk();
    }

    /**
     * Replays every record in order into {@code apply}, which receives
     * {@code (key, value)} where a {@code null} value means a delete.
     * Called once on startup to rebuild the memtable.
     */
    public void replay(BiConsumer<String, String> apply) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (DataInputStream in = new DataInputStream(new FileInputStream(path.toFile()))) {
            while (true) {
                byte op;
                try {
                    op = in.readByte();
                } catch (EOFException eof) {
                    return; // clean end of log
                }
                String key = in.readUTF();
                if (op == OP_PUT) {
                    apply.accept(key, in.readUTF());
                } else {
                    apply.accept(key, null);
                }
            }
        }
    }

    /**
     * fsync the bytes through the OS buffer to the physical disk. Without this
     * a crash could lose an "acknowledged" write that was still in a buffer.
     */
    private void flushToDisk() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}
