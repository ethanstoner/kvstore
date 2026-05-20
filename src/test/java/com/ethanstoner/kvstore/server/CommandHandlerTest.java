package com.ethanstoner.kvstore.server;

import com.ethanstoner.kvstore.KvStore;
import com.ethanstoner.kvstore.server.UserStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class CommandHandlerTest {

    /** Mutable auth stub used by tests; starts authenticated so existing tests are unaffected. */
    static class AuthStub implements AuthState {
        boolean authed = true;
        @Override public boolean isAuthenticated() { return authed; }
        @Override public void markAuthenticated() { authed = true; }
    }

    KvStore store;
    CommandHandler handler;
    AuthStub auth = new AuthStub();

    @BeforeEach
    void setUp(@TempDir Path dir) throws IOException {
        store = new KvStore(dir);
        handler = new CommandHandler(store, new KvServer(store, 0));
        auth = new AuthStub(); // reset per test
    }

    @AfterEach
    void tearDown() throws IOException {
        store.close();
    }

    private String run(String... args) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        handler.handle(args, out, auth);
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

    // ── AUTH tests ──────────────────────────────────────────────────────────

    @Test
    void authCommandWithoutPasswordConfigReturnsError() throws IOException {
        // Server constructed without password — AUTH should error
        String result = run("AUTH", "anything");
        assertTrue(result.startsWith("-ERR"), result);
        assertTrue(result.contains("no password"), result);
    }

    @Test
    void noauthBlocksCommandsWhenAuthRequired(@TempDir Path dir) throws IOException {
        try (KvStore s = new KvStore(dir)) {
            KvServer authServer = new KvServer(s, 0, "secret");
            CommandHandler authHandler = new CommandHandler(s, authServer);
            AuthStub unauth = new AuthStub();
            unauth.authed = false;

            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            authHandler.handle(new String[]{"SET", "k", "v"}, out, unauth);
            String reply = out.toString("UTF-8");
            assertTrue(reply.startsWith("-NOAUTH"), reply);
        }
    }

    @Test
    void successfulAuthUnlocksCommands(@TempDir Path dir) throws IOException {
        try (KvStore s = new KvStore(dir)) {
            KvServer authServer = new KvServer(s, 0, "secret");
            CommandHandler authHandler = new CommandHandler(s, authServer);
            AuthStub unauth = new AuthStub();
            unauth.authed = false;

            // AUTH first
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            authHandler.handle(new String[]{"AUTH", "secret"}, out, unauth);
            assertEquals("+OK\r\n", out.toString("UTF-8"));
            assertTrue(unauth.authed, "connection should now be authenticated");

            // Now SET works
            out.reset();
            authHandler.handle(new String[]{"SET", "k", "v"}, out, unauth);
            assertEquals("+OK\r\n", out.toString("UTF-8"));
        }
    }

    @Test
    void wrongPasswordIsRejected(@TempDir Path dir) throws IOException {
        try (KvStore s = new KvStore(dir)) {
            KvServer authServer = new KvServer(s, 0, "secret");
            CommandHandler authHandler = new CommandHandler(s, authServer);
            AuthStub unauth = new AuthStub();
            unauth.authed = false;

            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            authHandler.handle(new String[]{"AUTH", "wrong"}, out, unauth);
            String reply = out.toString("UTF-8");
            assertTrue(reply.startsWith("-WRONGPASS"), reply);
            assertFalse(unauth.authed, "connection should still be unauthenticated");
        }
    }

    @Test
    void pingAllowedBeforeAuth(@TempDir Path dir) throws IOException {
        try (KvStore s = new KvStore(dir)) {
            KvServer authServer = new KvServer(s, 0, "secret");
            CommandHandler authHandler = new CommandHandler(s, authServer);
            AuthStub unauth = new AuthStub();
            unauth.authed = false;

            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            authHandler.handle(new String[]{"PING"}, out, unauth);
            assertEquals("+PONG\r\n", out.toString("UTF-8"));
        }
    }

    // ── Multi-user AUTH tests ────────────────────────────────────────────────

    @Test
    void authWithUsernameAndPasswordWorks(@TempDir Path dir) throws IOException {
        try (KvStore s = new KvStore(dir)) {
            UserStore users = UserStore.builder()
                    .addUser("alice", "wonderland")
                    .addUser("bob", "builder")
                    .build();
            KvServer authServer = new KvServer(s, 0, users, null);
            CommandHandler authHandler = new CommandHandler(s, authServer);
            AuthStub unauth = new AuthStub(); unauth.authed = false;

            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            authHandler.handle(new String[]{"AUTH", "alice", "wonderland"}, out, unauth);
            assertEquals("+OK\r\n", out.toString("UTF-8"));
            assertTrue(unauth.authed);
        }
    }

    @Test
    void authWithWrongUserRejected(@TempDir Path dir) throws IOException {
        try (KvStore s = new KvStore(dir)) {
            UserStore users = UserStore.builder().addUser("alice", "secret").build();
            KvServer authServer = new KvServer(s, 0, users, null);
            CommandHandler authHandler = new CommandHandler(s, authServer);
            AuthStub unauth = new AuthStub(); unauth.authed = false;

            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            authHandler.handle(new String[]{"AUTH", "bob", "secret"}, out, unauth);
            assertTrue(out.toString("UTF-8").startsWith("-WRONGPASS"));
            assertFalse(unauth.authed);
        }
    }

    @Test
    void singlePasswordModeAcceptsBothAuthForms(@TempDir Path dir) throws IOException {
        try (KvStore s = new KvStore(dir)) {
            // --requirepass mode: creates a "default" user
            KvServer authServer = new KvServer(s, 0, "topsecret");
            CommandHandler authHandler = new CommandHandler(s, authServer);

            // Form 1: AUTH <password>
            AuthStub a1 = new AuthStub(); a1.authed = false;
            java.io.ByteArrayOutputStream out1 = new java.io.ByteArrayOutputStream();
            authHandler.handle(new String[]{"AUTH", "topsecret"}, out1, a1);
            assertEquals("+OK\r\n", out1.toString("UTF-8"));

            // Form 2: AUTH default <password>
            AuthStub a2 = new AuthStub(); a2.authed = false;
            java.io.ByteArrayOutputStream out2 = new java.io.ByteArrayOutputStream();
            authHandler.handle(new String[]{"AUTH", "default", "topsecret"}, out2, a2);
            assertEquals("+OK\r\n", out2.toString("UTF-8"));
        }
    }

    @Test
    void multiUserNoDefaultRejectsPasswordOnlyAuth(@TempDir Path dir) throws IOException {
        try (KvStore s = new KvStore(dir)) {
            UserStore users = UserStore.builder().addUser("alice", "wonderland").build();
            KvServer authServer = new KvServer(s, 0, users, null);
            CommandHandler authHandler = new CommandHandler(s, authServer);
            AuthStub unauth = new AuthStub(); unauth.authed = false;

            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            // AUTH <password> with no "default" user configured
            authHandler.handle(new String[]{"AUTH", "wonderland"}, out, unauth);
            assertTrue(out.toString("UTF-8").startsWith("-WRONGPASS"));
            assertFalse(unauth.authed);
        }
    }

    @Test
    void authArityErrors(@TempDir Path dir) throws IOException {
        try (KvStore s = new KvStore(dir)) {
            KvServer authServer = new KvServer(s, 0, "x");
            CommandHandler authHandler = new CommandHandler(s, authServer);
            AuthStub unauth = new AuthStub(); unauth.authed = false;

            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            authHandler.handle(new String[]{"AUTH"}, out, unauth);   // no args
            assertTrue(out.toString("UTF-8").startsWith("-ERR"));

            out.reset();
            authHandler.handle(new String[]{"AUTH", "a", "b", "c"}, out, unauth);  // too many
            assertTrue(out.toString("UTF-8").startsWith("-ERR"));
        }
    }

    @Test
    void wrongPasswordInSinglePasswordModeRejected(@TempDir Path dir) throws IOException {
        try (KvStore s = new KvStore(dir)) {
            KvServer authServer = new KvServer(s, 0, "secret");
            CommandHandler authHandler = new CommandHandler(s, authServer);
            AuthStub unauth = new AuthStub(); unauth.authed = false;

            // AUTH default wrong — should fail
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            authHandler.handle(new String[]{"AUTH", "default", "wrong"}, out, unauth);
            assertTrue(out.toString("UTF-8").startsWith("-WRONGPASS"));
            assertFalse(unauth.authed);

            // AUTH bob anything — no such user, should fail
            out.reset();
            authHandler.handle(new String[]{"AUTH", "bob", "secret"}, out, unauth);
            assertTrue(out.toString("UTF-8").startsWith("-WRONGPASS"));
            assertFalse(unauth.authed);
        }
    }

    // ── INCR / DECR / INCRBY / DECRBY tests ────────────────────────────────

    @Test
    void incrCreatesAndIncrements() throws IOException {
        assertEquals(":1\r\n", run("INCR", "counter"));
        assertEquals(":2\r\n", run("INCR", "counter"));
        assertEquals(":3\r\n", run("INCR", "counter"));
    }

    @Test
    void decrCreatesAndDecrements() throws IOException {
        assertEquals(":-1\r\n", run("DECR", "counter"));
        assertEquals(":-2\r\n", run("DECR", "counter"));
    }

    @Test
    void incrbyAndDecrbyWorkWithExplicitDelta() throws IOException {
        assertEquals(":10\r\n", run("INCRBY", "x", "10"));
        assertEquals(":25\r\n", run("INCRBY", "x", "15"));
        assertEquals(":20\r\n", run("DECRBY", "x", "5"));
    }

    @Test
    void incrOnNonIntegerErrors() throws IOException {
        run("SET", "name", "alice");
        String result = run("INCR", "name");
        assertTrue(result.startsWith("-ERR"), result);
        assertTrue(result.contains("integer"), result);
    }

    @Test
    void incrbyWithNonIntegerDeltaErrors() throws IOException {
        String result = run("INCRBY", "x", "notanumber");
        assertTrue(result.startsWith("-ERR"), result);
    }

    @Test
    void incrArityErrors() throws IOException {
        String r1 = run("INCR");
        assertTrue(r1.startsWith("-ERR"), r1);
        String r2 = run("INCRBY", "x");
        assertTrue(r2.startsWith("-ERR"), r2);
    }

    // ── TTL / expiration tests ───────────────────────────────────────────────

    @Test
    void setWithExSetsTtl() throws IOException {
        assertEquals("+OK\r\n", run("SET", "k", "v", "EX", "60"));
        String ttlReply = run("TTL", "k");
        assertTrue(ttlReply.startsWith(":"), ttlReply);
        int seconds = Integer.parseInt(ttlReply.substring(1, ttlReply.indexOf("\r")));
        assertTrue(seconds > 0, "TTL should be positive, got " + seconds);
    }

    @Test
    void setWithPxSetsMillisTtl() throws IOException {
        assertEquals("+OK\r\n", run("SET", "k", "v", "PX", "60000"));
        String reply = run("PTTL", "k");
        assertTrue(reply.startsWith(":"), reply);
        long ms = Long.parseLong(reply.substring(1, reply.indexOf("\r")));
        assertTrue(ms > 0, "PTTL should be positive, got " + ms);
    }

    @Test
    void expireCommandWorks() throws IOException {
        run("SET", "k", "v");
        assertEquals(":1\r\n", run("EXPIRE", "k", "60"));
        assertEquals(":0\r\n", run("EXPIRE", "missing", "60"));
    }

    @Test
    void persistCommandWorks() throws IOException {
        run("SET", "k", "v", "EX", "60");
        assertEquals(":1\r\n", run("PERSIST", "k"));
        assertEquals(":-1\r\n", run("TTL", "k"));
    }

    @Test
    void ttlReturnsMinusTwoForMissing() throws IOException {
        assertEquals(":-2\r\n", run("TTL", "nope"));
    }

    @Test
    void ttlReturnsMinusOneForKeyWithoutExpiry() throws IOException {
        run("SET", "k", "v");
        assertEquals(":-1\r\n", run("TTL", "k"));
    }

    @Test
    void pexpireCommandWorks() throws IOException {
        run("SET", "k", "v");
        assertEquals(":1\r\n", run("PEXPIRE", "k", "60000"));
        String reply = run("PTTL", "k");
        assertTrue(reply.startsWith(":"), reply);
        long ms = Long.parseLong(reply.substring(1, reply.indexOf("\r")));
        assertTrue(ms > 0, "PTTL should be positive after PEXPIRE");
    }

    @Test
    void setWithInvalidExOptionErrors() throws IOException {
        String result = run("SET", "k", "v", "EX", "notanumber");
        assertTrue(result.startsWith("-ERR"), result);
    }

    @Test
    void setWithUnknownOptionErrors() throws IOException {
        String result = run("SET", "k", "v", "XX");
        assertTrue(result.startsWith("-ERR"), result);
    }

    // ── Snapshot command tests ───────────────────────────────────────────────

    @Test
    void saveCommand() throws IOException {
        run("SET", "a", "1");
        assertEquals("+OK\r\n", run("SAVE"));
    }

    @Test
    void bgsaveCommand() throws IOException {
        run("SET", "a", "1");
        String reply = run("BGSAVE");
        assertEquals("+Background saving started\r\n", reply);
    }

    @Test
    void lastsaveReturnsZeroBeforeAnySnapshot() throws IOException {
        assertEquals(":0\r\n", run("LASTSAVE"));
    }

    @Test
    void lastsaveAdvancesAfterSave() throws IOException {
        run("SET", "k", "v");
        run("SAVE");
        String reply = run("LASTSAVE");
        // Returns :<epoch>\r\n
        assertTrue(reply.startsWith(":"), reply);
        assertTrue(reply.endsWith("\r\n"), reply);
        long ts = Long.parseLong(reply.substring(1, reply.indexOf("\r")));
        assertTrue(ts > 0, "expected positive epoch, got " + ts);
    }
}
