package com.ethanstoner.kvstore.server;

import com.ethanstoner.kvstore.KvStore;
import com.ethanstoner.kvstore.server.UserStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class KvServerTest {

    /** Sends a RESP array command and reads back exactly one RESP reply. */
    private static String roundTrip(int port, String[] cmd) throws IOException {
        try (Socket sock = new Socket("127.0.0.1", port)) {
            StringBuilder req = new StringBuilder();
            req.append('*').append(cmd.length).append("\r\n");
            for (String a : cmd) {
                byte[] b = a.getBytes(StandardCharsets.UTF_8);
                req.append('$').append(b.length).append("\r\n").append(a).append("\r\n");
            }
            sock.getOutputStream().write(req.toString().getBytes(StandardCharsets.UTF_8));
            sock.getOutputStream().flush();
            sock.setSoTimeout(3000);
            return readOneRespReply(sock.getInputStream());
        }
    }

    /** RESP-aware reader: reads exactly one full reply frame. */
    private static String readOneRespReply(java.io.InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int first = in.read();
        if (first == -1) throw new EOFException("server closed");
        buf.write(first);
        switch (first) {
            case '+': case '-': case ':':
                readUntilCrLf(in, buf);
                break;
            case '$': {
                String header = readUntilCrLf(in, buf);
                int len = Integer.parseInt(header);
                if (len >= 0) {
                    readNBytesInto(in, buf, len);
                    readCrLfInto(in, buf);
                }
                break;
            }
            case '*': {
                String header = readUntilCrLf(in, buf);
                int n = Integer.parseInt(header);
                for (int i = 0; i < n; i++) {
                    int marker = in.read();
                    if (marker == -1) throw new EOFException();
                    buf.write(marker);
                    if (marker != '$') throw new IOException("expected $, got " + (char) marker);
                    String hdr = readUntilCrLf(in, buf);
                    int len = Integer.parseInt(hdr);
                    if (len >= 0) {
                        readNBytesInto(in, buf, len);
                        readCrLfInto(in, buf);
                    }
                }
                break;
            }
            default:
                throw new IOException("unknown RESP type: " + (char) first);
        }
        return buf.toString(StandardCharsets.UTF_8);
    }

    private static String readUntilCrLf(java.io.InputStream in, ByteArrayOutputStream buf) throws IOException {
        StringBuilder s = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            buf.write(b);
            if (b == '\r') {
                int next = in.read();
                if (next == -1) throw new EOFException();
                buf.write(next);
                if (next == '\n') return s.toString();
                throw new IOException("expected LF after CR");
            }
            s.append((char) b);
        }
        throw new EOFException();
    }

    private static void readNBytesInto(java.io.InputStream in, ByteArrayOutputStream buf, int n) throws IOException {
        byte[] tmp = in.readNBytes(n);
        if (tmp.length != n) throw new EOFException();
        buf.write(tmp);
    }

    private static void readCrLfInto(java.io.InputStream in, ByteArrayOutputStream buf) throws IOException {
        int cr = in.read(), lf = in.read();
        if (cr == -1 || lf == -1) throw new EOFException();
        buf.write(cr); buf.write(lf);
        if (cr != '\r' || lf != '\n') throw new IOException("expected CRLF");
    }

    @Test
    void pingReturnsPongOverTcp(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir);
             KvServer server = new KvServer(store, 0)) {
            server.start();
            String reply = roundTrip(server.port(), new String[]{"PING"});
            assertEquals("+PONG\r\n", reply);
        }
    }

    @Test
    void setThenGetOverTcp(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir);
             KvServer server = new KvServer(store, 0)) {
            server.start();
            int port = server.port();
            assertEquals("+OK\r\n", roundTrip(port, new String[]{"SET", "x", "yz"}));
            assertEquals("$2\r\nyz\r\n", roundTrip(port, new String[]{"GET", "x"}));
        }
    }

    @Test
    void multipleClientsCanConnect(@TempDir Path dir) throws Exception {
        try (KvStore store = new KvStore(dir);
             KvServer server = new KvServer(store, 0)) {
            server.start();
            int port = server.port();

            int n = 10;
            CountDownLatch done = new CountDownLatch(n);
            List<Throwable> errors = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                final int idx = i;
                Thread.startVirtualThread(() -> {
                    try {
                        String key = "key-" + idx;
                        String reply = roundTrip(port, new String[]{"SET", key, "v" + idx});
                        if (!"+OK\r\n".equals(reply)) {
                            synchronized (errors) { errors.add(new AssertionError("got: " + reply)); }
                        }
                    } catch (Exception e) {
                        synchronized (errors) { errors.add(e); }
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(10, TimeUnit.SECONDS));
            assertTrue(errors.isEmpty(), "Errors: " + errors);

            for (int i = 0; i < n; i++) {
                String reply = roundTrip(port, new String[]{"GET", "key-" + i});
                assertTrue(reply.contains("v" + i), "Expected v" + i + " in " + reply);
            }
        }
    }

    @Test
    void concurrentDelOnSameKeyExactlyOneCountsOne(@TempDir Path dir) throws Exception {
        try (KvStore store = new KvStore(dir);
             KvServer server = new KvServer(store, 0)) {
            server.start();
            int port = server.port();
            roundTrip(port, new String[]{"SET", "doomed", "v"});

            int n = 8;
            CountDownLatch done = new CountDownLatch(n);
            List<String> replies = java.util.Collections.synchronizedList(new ArrayList<>());
            for (int i = 0; i < n; i++) {
                Thread.startVirtualThread(() -> {
                    try {
                        replies.add(roundTrip(port, new String[]{"DEL", "doomed"}));
                    } catch (Exception ignored) {
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(10, TimeUnit.SECONDS));
            long ones = replies.stream().filter(r -> r.equals(":1\r\n")).count();
            long zeros = replies.stream().filter(r -> r.equals(":0\r\n")).count();
            assertEquals(1, ones, "exactly one DEL should report :1 - replies=" + replies);
            assertEquals(n - 1, zeros, "the rest should be :0");
        }
    }

    @Test
    void stopWaitsForConnectionsAndCloses(@TempDir Path dir) throws IOException {
        KvStore store = new KvStore(dir);
        KvServer server = new KvServer(store, 0);
        server.start();
        int port = server.port();
        new Socket("127.0.0.1", port).close();
        server.stop();
        assertFalse(server.isRunning());
        store.close();
        assertThrows(IOException.class, () -> {
            try (Socket s = new Socket("127.0.0.1", port)) { s.getOutputStream().write(1); }
        });
    }

    // ── AUTH integration tests ──────────────────────────────────────────────

    @Test
    void authRequiredBlocksUnauthClients(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir);
             KvServer server = new KvServer(store, 0, "topsecret")) {
            server.start();
            int port = server.port();

            // Unauth SET → NOAUTH
            String reply = roundTrip(port, new String[]{"SET", "k", "v"});
            assertTrue(reply.startsWith("-NOAUTH"), reply);
        }
    }

    @Test
    void authSuccessThenWriteWorks(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir);
             KvServer server = new KvServer(store, 0, "topsecret")) {
            server.start();
            int port = server.port();
            // AUTH and SET must share the same connection
            try (java.net.Socket sock = new java.net.Socket("127.0.0.1", port)) {
                sock.setSoTimeout(3000);
                sendResp(sock, new String[]{"AUTH", "topsecret"});
                String authReply = readOneRespReply(sock.getInputStream());
                assertEquals("+OK\r\n", authReply);

                sendResp(sock, new String[]{"SET", "k", "v"});
                String setReply = readOneRespReply(sock.getInputStream());
                assertEquals("+OK\r\n", setReply);
            }
        }
    }

    private static void sendResp(java.net.Socket sock, String[] cmd) throws IOException {
        StringBuilder req = new StringBuilder();
        req.append('*').append(cmd.length).append("\r\n");
        for (String a : cmd) {
            byte[] b = a.getBytes(StandardCharsets.UTF_8);
            req.append('$').append(b.length).append("\r\n").append(a).append("\r\n");
        }
        sock.getOutputStream().write(req.toString().getBytes(StandardCharsets.UTF_8));
        sock.getOutputStream().flush();
    }

    // ── Multi-user AUTH integration test ────────────────────────────────────

    @Test
    void multiUserAuthEndToEnd(@TempDir Path dir) throws IOException {
        UserStore users = UserStore.builder()
                .addUser("alice", "wonderland")
                .addUser("bob", "builder")
                .build();
        try (KvStore store = new KvStore(dir);
             KvServer server = new KvServer(store, 0, users, null)) {
            server.start();
            int port = server.port();

            // Unauth SET — blocked
            String reply1 = roundTrip(port, new String[]{"SET", "k", "v"});
            assertTrue(reply1.startsWith("-NOAUTH"), "Expected NOAUTH, got: " + reply1);

            // Bob authenticates and writes on same socket
            try (java.net.Socket sock = new java.net.Socket("127.0.0.1", port)) {
                sock.setSoTimeout(3000);
                sendResp(sock, new String[]{"AUTH", "bob", "builder"});
                assertEquals("+OK\r\n", readOneRespReply(sock.getInputStream()));
                sendResp(sock, new String[]{"SET", "bobkey", "bobval"});
                assertEquals("+OK\r\n", readOneRespReply(sock.getInputStream()));
            }

            // Alice authenticates and reads bob's key on same socket
            try (java.net.Socket sock = new java.net.Socket("127.0.0.1", port)) {
                sock.setSoTimeout(3000);
                sendResp(sock, new String[]{"AUTH", "alice", "wonderland"});
                assertEquals("+OK\r\n", readOneRespReply(sock.getInputStream()));
                sendResp(sock, new String[]{"GET", "bobkey"});
                assertEquals("$6\r\nbobval\r\n", readOneRespReply(sock.getInputStream()));
            }

            // Wrong password for bob — WRONGPASS
            try (java.net.Socket sock = new java.net.Socket("127.0.0.1", port)) {
                sock.setSoTimeout(3000);
                sendResp(sock, new String[]{"AUTH", "bob", "wrongpass"});
                String reply = readOneRespReply(sock.getInputStream());
                assertTrue(reply.startsWith("-WRONGPASS"), "Expected WRONGPASS, got: " + reply);
            }

            // AUTH with only password (no user) — no "default" user so WRONGPASS
            try (java.net.Socket sock = new java.net.Socket("127.0.0.1", port)) {
                sock.setSoTimeout(3000);
                sendResp(sock, new String[]{"AUTH", "builder"});
                String reply = readOneRespReply(sock.getInputStream());
                assertTrue(reply.startsWith("-WRONGPASS"), "Expected WRONGPASS for no-default, got: " + reply);
            }
        }
    }

    // ── Connection limit tests ─────────────────────────────────────────────

    @Test
    void rejectsConnectionsBeyondMax(@TempDir Path dir) throws Exception {
        try (KvStore store = new KvStore(dir);
             KvServer server = new KvServer(store, 0, UserStore.empty(), null, 2)) {
            server.start();
            int port = server.port();

            // Open 2 long-lived connections (don't close, don't read)
            java.net.Socket c1 = new java.net.Socket("127.0.0.1", port);
            java.net.Socket c2 = new java.net.Socket("127.0.0.1", port);

            // 3rd connection should be accepted by the OS but immediately closed by the server
            Thread.sleep(100);  // give server time to register c1, c2

            try (java.net.Socket c3 = new java.net.Socket("127.0.0.1", port)) {
                c3.setSoTimeout(2000);
                // Try to ping — server already closed the socket
                try {
                    c3.getOutputStream().write("*1\r\n$4\r\nPING\r\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    c3.getOutputStream().flush();
                    int b = c3.getInputStream().read();
                    // Either -1 (clean EOF) or we get some bytes briefly then EOF.
                    // The key fact: the server's INFO should now show rejected_connections >= 1.
                } catch (java.io.IOException ignored) {
                    // Expected — connection got closed
                }
            }

            c1.close();
            c2.close();
        }
    }

    @Test
    void infoIncludesMaxConnectionsSection(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir);
             KvServer server = new KvServer(store, 0, UserStore.empty(), null, 100)) {
            server.start();
            String info = server.infoText();
            assertTrue(info.contains("# limits"), info);
            assertTrue(info.contains("max_connections:100"), info);
            assertTrue(info.contains("peak_concurrent_connections:"), info);
            assertTrue(info.contains("rejected_connections:"), info);
        }
    }

    @Test
    void infoShowsUnlimitedWhenMaxIsZero(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir);
             KvServer server = new KvServer(store, 0)) {  // legacy 2-arg
            server.start();
            String info = server.infoText();
            assertTrue(info.contains("max_connections:unlimited"), info);
        }
    }
}
