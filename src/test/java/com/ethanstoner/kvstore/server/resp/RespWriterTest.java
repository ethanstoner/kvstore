package com.ethanstoner.kvstore.server.resp;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RespWriterTest {

    @Test
    void writesSimpleString() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RespWriter.writeSimpleString(out, "OK");
        assertEquals("+OK\r\n", out.toString("UTF-8"));
    }

    @Test
    void writesError() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RespWriter.writeError(out, "ERR bad");
        assertEquals("-ERR bad\r\n", out.toString("UTF-8"));
    }

    @Test
    void writesInteger() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RespWriter.writeInteger(out, 42);
        assertEquals(":42\r\n", out.toString("UTF-8"));
    }

    @Test
    void writesBulkString() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RespWriter.writeBulkString(out, "hello");
        assertEquals("$5\r\nhello\r\n", out.toString("UTF-8"));
    }

    @Test
    void writesNullBulkString() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RespWriter.writeBulkString(out, null);
        assertEquals("$-1\r\n", out.toString("UTF-8"));
    }

    @Test
    void writesEmptyBulkString() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RespWriter.writeBulkString(out, "");
        assertEquals("$0\r\n\r\n", out.toString("UTF-8"));
    }

    @Test
    void writesArrayWithNulls() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RespWriter.writeArray(out, Arrays.asList("a", null, "b"));
        assertEquals("*3\r\n$1\r\na\r\n$-1\r\n$1\r\nb\r\n", out.toString("UTF-8"));
    }

    @Test
    void writesEmptyArray() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RespWriter.writeArray(out, List.of());
        assertEquals("*0\r\n", out.toString("UTF-8"));
    }

    @Test
    void bulkRoundTripsThroughParser() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RespWriter.writeArray(out, List.of("héllo wörld\r\nwith\r\nbreaks"));
        String[] parsed = RespParser.readCommand(
                new ByteArrayInputStream(out.toByteArray()));
        assertArrayEquals(new String[]{"héllo wörld\r\nwith\r\nbreaks"}, parsed);
    }
}
