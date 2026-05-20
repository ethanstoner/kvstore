package com.ethanstoner.kvstore.server.resp;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Reads RESP (Redis Serialization Protocol) frames from an {@link InputStream}.
 *
 * <p>RESP types supported on read:
 * <ul>
 *   <li>{@code *N} — array (the normal command form)</li>
 *   <li>{@code $N} — bulk string (byte-counted, {@code -1} = null)</li>
 *   <li>Inline command — a single line of space-separated tokens
 *       terminated by {@code \r\n} or {@code \n}, used by {@code redis-cli}
 *       and raw {@code nc}/{@code telnet}</li>
 * </ul>
 */
public final class RespParser {

    private static final int MAX_ARGS     = 1024 * 1024;
    private static final int MAX_BULK_LEN = 512 * 1024 * 1024;

    private RespParser() {}

    /**
     * Reads one full command from {@code in}.
     *
     * @return parsed command as {@code String[]}, or {@code null} if the
     *         stream ended cleanly between commands (EOF before any byte
     *         of the next frame was read).
     * @throws EOFException if the stream ended mid-frame
     * @throws IOException  if the stream is malformed
     */
    public static String[] readCommand(InputStream in) throws IOException {
        int first = in.read();
        if (first == -1) return null;

        if (first == '*') {
            int n = readDecimalLine(in);
            if (n < 0)        throw new IOException("array length < 0: " + n);
            if (n > MAX_ARGS) throw new IOException("array length > MAX_ARGS: " + n);
            String[] args = new String[n];
            for (int i = 0; i < n; i++) {
                int marker = in.read();
                if (marker == -1) throw new EOFException("eof reading bulk marker");
                if (marker != '$') throw new IOException("expected '$', got " + (char) marker);
                int len = readDecimalLine(in);
                if (len == -1) {
                    args[i] = null;
                } else {
                    if (len < 0)            throw new IOException("bulk len < -1: " + len);
                    if (len > MAX_BULK_LEN) throw new IOException("bulk len > MAX_BULK_LEN: " + len);
                    byte[] buf = readNBytes(in, len);
                    if (buf.length != len)  throw new EOFException("short read on bulk: " + buf.length + "/" + len);
                    args[i] = new String(buf, StandardCharsets.UTF_8);
                    consumeCrLf(in);
                }
            }
            return args;
        }

        // Inline command form
        StringBuilder line = new StringBuilder();
        line.append((char) first);
        while (true) {
            int b = in.read();
            if (b == -1) throw new EOFException("eof in inline command");
            if (b == '\r') {
                int next = in.read();
                if (next != '\n') throw new IOException("expected LF after CR in inline command");
                break;
            }
            if (b == '\n') break;
            line.append((char) b);
        }
        return line.toString().trim().split(" +");
    }

    /**
     * Read exactly {@code len} bytes from {@code in}, handling partial reads
     * from streams that return fewer bytes than requested per call.
     */
    private static byte[] readNBytes(InputStream in, int len) throws IOException {
        byte[] buf = new byte[len];
        int offset = 0;
        while (offset < len) {
            int read = in.read(buf, offset, len - offset);
            if (read == -1) break;
            offset += read;
        }
        if (offset < len) {
            // Return truncated array so caller can detect short read
            byte[] partial = new byte[offset];
            System.arraycopy(buf, 0, partial, 0, offset);
            return partial;
        }
        return buf;
    }

    private static int readDecimalLine(InputStream in) throws IOException {
        StringBuilder s = new StringBuilder();
        while (true) {
            int b = in.read();
            if (b == -1) throw new EOFException("eof reading decimal");
            if (b == '\r') {
                int next = in.read();
                if (next != '\n') throw new IOException("expected LF after CR");
                try {
                    return Integer.parseInt(s.toString());
                } catch (NumberFormatException e) {
                    throw new IOException("bad number: " + s, e);
                }
            }
            s.append((char) b);
        }
    }

    private static void consumeCrLf(InputStream in) throws IOException {
        int cr = in.read();
        if (cr == -1) throw new EOFException("eof while expecting CRLF");
        int lf = in.read();
        if (lf == -1) throw new EOFException("eof after CR while expecting LF");
        if (cr != '\r' || lf != '\n') {
            throw new IOException("expected CRLF, got " + cr + "," + lf);
        }
    }
}
