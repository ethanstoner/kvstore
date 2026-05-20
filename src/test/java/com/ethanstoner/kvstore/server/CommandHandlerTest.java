package com.ethanstoner.kvstore.server;

import com.ethanstoner.kvstore.KvStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class CommandHandlerTest {

    KvStore store;
    CommandHandler handler;

    @BeforeEach
    void setUp(@TempDir Path dir) throws IOException {
        store = new KvStore(dir);
        handler = new CommandHandler(store, new KvServer(store, 0));
    }

    @AfterEach
    void tearDown() throws IOException {
        store.close();
    }

    private String run(String... args) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        handler.handle(args, out);
        return out.toString("UTF-8");
    }

    @Test
    void pingReturnsPong() throws IOException {
        assertEquals("+PONG\r\n", run("PING"));
    }

    @Test
    void pingEchoesArgument() throws IOException {
        assertEquals("$5\r\nhello\r\n", run("PING", "hello"));
    }

    @Test
    void setAndGet() throws IOException {
        assertEquals("+OK\r\n", run("SET", "k", "v"));
        assertEquals("$1\r\nv\r\n", run("GET", "k"));
    }

    @Test
    void getMissingReturnsNullBulk() throws IOException {
        assertEquals("$-1\r\n", run("GET", "nope"));
    }

    @Test
    void delReturnsCountOfActuallyDeleted() throws IOException {
        run("SET", "a", "1");
        run("SET", "b", "2");
        assertEquals(":2\r\n", run("DEL", "a", "b", "c"));
        assertEquals("$-1\r\n", run("GET", "a"));
    }

    @Test
    void existsCountsPresentKeys() throws IOException {
        run("SET", "a", "1");
        run("SET", "b", "2");
        assertEquals(":2\r\n", run("EXISTS", "a", "b", "missing"));
    }

    @Test
    void mgetReturnsArrayWithNulls() throws IOException {
        run("SET", "a", "1");
        run("SET", "c", "3");
        assertEquals("*3\r\n$1\r\n1\r\n$-1\r\n$1\r\n3\r\n",
                run("MGET", "a", "b", "c"));
    }

    @Test
    void msetWritesAllPairs() throws IOException {
        assertEquals("+OK\r\n", run("MSET", "a", "1", "b", "2"));
        assertEquals("$1\r\n1\r\n", run("GET", "a"));
        assertEquals("$1\r\n2\r\n", run("GET", "b"));
    }

    @Test
    void msetWithOddNumberOfKvArgsErrors() throws IOException {
        String result = run("MSET", "a", "1", "b");
        assertTrue(result.startsWith("-ERR"), result);
    }

    @Test
    void scanRangeReturnsAlternatingKeysAndValues() throws IOException {
        run("SET", "k1", "v1");
        run("SET", "k2", "v2");
        run("SET", "k3", "v3");
        assertEquals("*4\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n$2\r\nv2\r\n",
                run("SCAN", "k1", "k3"));
    }

    @Test
    void scanCursorStyleReturnsError() throws IOException {
        String result = run("SCAN", "0");
        assertTrue(result.startsWith("-ERR"), result);
        assertTrue(result.contains("range scan"), result);
    }

    @Test
    void dbsizeReturnsApproximate() throws IOException {
        run("SET", "a", "1");
        run("SET", "b", "2");
        String result = run("DBSIZE");
        assertTrue(result.startsWith(":"));
        assertTrue(result.endsWith("\r\n"));
    }

    @Test
    void infoReturnsBulkString() throws IOException {
        String result = run("INFO");
        assertTrue(result.startsWith("$"), result);
        assertTrue(result.contains("# server"), result);
        assertTrue(result.contains("kvstore_version:0.1.0"), result);
    }

    @Test
    void commandReturnsEmptyArray() throws IOException {
        assertEquals("*0\r\n", run("COMMAND"));
    }

    @Test
    void commandDocsReturnsEmptyArray() throws IOException {
        assertEquals("*0\r\n", run("COMMAND", "DOCS"));
        assertEquals("*0\r\n", run("COMMAND", "DOCS", "SET", "GET"));
    }

    @Test
    void quitReturnsOk() throws IOException {
        assertEquals("+OK\r\n", run("QUIT"));
    }

    @Test
    void unknownCommandErrors() throws IOException {
        String result = run("FROBNICATE");
        assertTrue(result.startsWith("-ERR"), result);
        assertTrue(result.contains("FROBNICATE"), result);
    }

    @Test
    void wrongArityErrors() throws IOException {
        String result = run("GET");
        assertTrue(result.startsWith("-ERR"), result);
    }

    @Test
    void caseInsensitiveCommands() throws IOException {
        run("set", "k", "v");
        assertEquals("$1\r\nv\r\n", run("get", "k"));
    }
}
