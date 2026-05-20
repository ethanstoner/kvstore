package com.ethanstoner.kvstore.server;

import com.ethanstoner.kvstore.KvStore;
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
}
