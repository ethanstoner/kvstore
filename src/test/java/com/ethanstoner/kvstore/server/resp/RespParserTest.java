package com.ethanstoner.kvstore.server.resp;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class RespParserTest {

    private static InputStream wrap(String s) {
        return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
    }

    private static InputStream wrap(byte[] b) {
        return new ByteArrayInputStream(b);
    }

    @Test
    void parsesArrayOfBulkStrings() throws IOException {
        String input = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
        String[] cmd = RespParser.readCommand(wrap(input));
        assertArrayEquals(new String[]{"SET", "k", "v"}, cmd);
    }

    @Test
    void parsesEmptyArray() throws IOException {
        String[] cmd = RespParser.readCommand(wrap("*0\r\n"));
        assertArrayEquals(new String[0], cmd);
    }

    @Test
    void parsesNullBulkStringInArray() throws IOException {
        String input = "*2\r\n$3\r\nGET\r\n$-1\r\n";
        String[] cmd = RespParser.readCommand(wrap(input));
        assertArrayEquals(new String[]{"GET", null}, cmd);
    }

    @Test
    void parsesEmptyBulkString() throws IOException {
        String input = "*2\r\n$3\r\nSET\r\n$0\r\n\r\n";
        String[] cmd = RespParser.readCommand(wrap(input));
        assertArrayEquals(new String[]{"SET", ""}, cmd);
    }

    @Test
    void bulkStringIsBytecountedNotLineTerminated() throws IOException {
        String input = "*2\r\n$3\r\nSET\r\n$6\r\nab\r\ncd\r\n";
        String[] cmd = RespParser.readCommand(wrap(input));
        assertArrayEquals(new String[]{"SET", "ab\r\ncd"}, cmd);
    }

    @Test
    void parsesInlineCommand() throws IOException {
        String[] cmd = RespParser.readCommand(wrap("PING\r\n"));
        assertArrayEquals(new String[]{"PING"}, cmd);
    }

    @Test
    void parsesInlineCommandWithArgs() throws IOException {
        String[] cmd = RespParser.readCommand(wrap("SET key value\r\n"));
        assertArrayEquals(new String[]{"SET", "key", "value"}, cmd);
    }

    @Test
    void parsesInlineCommandWithLfOnly() throws IOException {
        String[] cmd = RespParser.readCommand(wrap("PING\n"));
        assertArrayEquals(new String[]{"PING"}, cmd);
    }

    @Test
    void eofBetweenCommandsReturnsNull() throws IOException {
        assertNull(RespParser.readCommand(wrap("")));
    }

    @Test
    void eofMidCommandThrowsEofException() {
        assertThrows(EOFException.class,
                () -> RespParser.readCommand(wrap("*3\r\n$3\r\nSET")));
    }

    @Test
    void badArrayLengthThrows() {
        assertThrows(IOException.class,
                () -> RespParser.readCommand(wrap("*-5\r\n")));
    }

    @Test
    void missingCrlfThrows() {
        assertThrows(IOException.class,
                () -> RespParser.readCommand(wrap("*1\r$3\r\nFOO\r\n")));
    }

    @Test
    void partialReadStillParses() throws IOException {
        String input = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
        InputStream slow = new InputStream() {
            int idx = 0;
            @Override public int read() { return idx < bytes.length ? bytes[idx++] & 0xff : -1; }
            @Override public int read(byte[] buf, int off, int len) {
                if (idx >= bytes.length) return -1;
                buf[off] = bytes[idx++];
                return 1;
            }
        };
        String[] cmd = RespParser.readCommand(slow);
        assertArrayEquals(new String[]{"SET", "k", "v"}, cmd);
    }

    @Test
    void multiByteUtf8InBulkString() throws IOException {
        String value = "héllo wörld";
        byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);
        String header = "*2\r\n$3\r\nSET\r\n$" + valBytes.length + "\r\n";
        byte[] headerBytes = header.getBytes(StandardCharsets.UTF_8);
        byte[] full = new byte[headerBytes.length + valBytes.length + 2];
        System.arraycopy(headerBytes, 0, full, 0, headerBytes.length);
        System.arraycopy(valBytes, 0, full, headerBytes.length, valBytes.length);
        full[full.length - 2] = '\r';
        full[full.length - 1] = '\n';
        String[] cmd = RespParser.readCommand(wrap(full));
        assertArrayEquals(new String[]{"SET", value}, cmd);
    }

    @Test
    void arityExceedingLimitThrows() {
        String input = "*1048577\r\n";
        assertThrows(IOException.class, () -> RespParser.readCommand(wrap(input)));
    }
}
