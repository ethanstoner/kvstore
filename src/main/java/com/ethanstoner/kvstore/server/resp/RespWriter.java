package com.ethanstoner.kvstore.server.resp;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Emits RESP responses to an {@link OutputStream}. */
public final class RespWriter {

    private static final byte[] CRLF = {'\r', '\n'};
    private static final byte[] NULL_BULK = "$-1\r\n".getBytes(StandardCharsets.UTF_8);

    private RespWriter() {}

    public static void writeSimpleString(OutputStream out, String s) throws IOException {
        out.write('+');
        out.write(s.getBytes(StandardCharsets.UTF_8));
        out.write(CRLF);
    }

    public static void writeError(OutputStream out, String message) throws IOException {
        out.write('-');
        out.write(message.getBytes(StandardCharsets.UTF_8));
        out.write(CRLF);
    }

    public static void writeInteger(OutputStream out, long value) throws IOException {
        out.write(':');
        out.write(Long.toString(value).getBytes(StandardCharsets.UTF_8));
        out.write(CRLF);
    }

    public static void writeBulkString(OutputStream out, String s) throws IOException {
        if (s == null) {
            out.write(NULL_BULK);
            return;
        }
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        out.write('$');
        out.write(Integer.toString(bytes.length).getBytes(StandardCharsets.UTF_8));
        out.write(CRLF);
        out.write(bytes);
        out.write(CRLF);
    }

    public static void writeArray(OutputStream out, List<String> items) throws IOException {
        out.write('*');
        out.write(Integer.toString(items.size()).getBytes(StandardCharsets.UTF_8));
        out.write(CRLF);
        for (String item : items) {
            writeBulkString(out, item);
        }
    }
}
