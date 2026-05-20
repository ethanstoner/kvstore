package com.ethanstoner.kvstore.server;

import com.ethanstoner.kvstore.KvStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class PubSubTest {

    private static void sendCmd(Socket sock, String... cmd) throws IOException {
        StringBuilder req = new StringBuilder();
        req.append('*').append(cmd.length).append("\r\n");
        for (String a : cmd) {
            byte[] b = a.getBytes(StandardCharsets.UTF_8);
            req.append('$').append(b.length).append("\r\n").append(a).append("\r\n");
        }
        sock.getOutputStream().write(req.toString().getBytes(StandardCharsets.UTF_8));
        sock.getOutputStream().flush();
    }

    @Test
    void publishToNoSubscribersReturnsZero(@TempDir Path dir) throws IOException {
        try (KvStore store = new KvStore(dir);
             KvServer server = new KvServer(store, 0)) {
            server.start();
            try (Socket sock = new Socket("127.0.0.1", server.port())) {
                sock.setSoTimeout(3000);
                sendCmd(sock, "PUBLISH", "no-one", "hello");
                // Expect :0\r\n
                byte[] buf = new byte[16];
                int n = sock.getInputStream().read(buf);
                String reply = new String(buf, 0, n, StandardCharsets.UTF_8);
                assertEquals(":0\r\n", reply);
            }
        }
    }

    @Test
    void subscribeAndReceiveOneMessage(@TempDir Path dir) throws Exception {
        try (KvStore store = new KvStore(dir);
             KvServer server = new KvServer(store, 0)) {
            server.start();
            int port = server.port();

            // Subscriber socket
            Socket sub = new Socket("127.0.0.1", port);
            sub.setSoTimeout(5000);
            sendCmd(sub, "SUBSCRIBE", "news");
            // Skip the subscribe ack
            String ack = readOneReply(sub.getInputStream());
            assertTrue(ack.contains("subscribe"), ack);
            assertTrue(ack.contains("news"), ack);

            // Give the subscription a moment to register (rare race)
            Thread.sleep(50);

            // Publisher socket
            try (Socket pub = new Socket("127.0.0.1", port)) {
                pub.setSoTimeout(3000);
                sendCmd(pub, "PUBLISH", "news", "hello world");
                String pubReply = readOneReply(pub.getInputStream());
                assertEquals(":1\r\n", pubReply);
            }

            // Subscriber should now have the message
            String msg = readOneReply(sub.getInputStream());
            assertTrue(msg.contains("message"), msg);
            assertTrue(msg.contains("news"), msg);
            assertTrue(msg.contains("hello world"), msg);

            sub.close();
        }
    }

    @Test
    void nonPubSubCommandsBlockedInSubscribeMode(@TempDir Path dir) throws Exception {
        try (KvStore store = new KvStore(dir);
             KvServer server = new KvServer(store, 0)) {
            server.start();
            try (Socket sock = new Socket("127.0.0.1", server.port())) {
                sock.setSoTimeout(3000);
                sendCmd(sock, "SUBSCRIBE", "x");
                readOneReply(sock.getInputStream());  // ack

                sendCmd(sock, "SET", "k", "v");
                String reply = readOneReply(sock.getInputStream());
                assertTrue(reply.startsWith("-ERR"), reply);
                assertTrue(reply.contains("subscribe context"), reply);
            }
        }
    }

    @Test
    void patternSubscribeMatches(@TempDir Path dir) throws Exception {
        try (KvStore store = new KvStore(dir);
             KvServer server = new KvServer(store, 0)) {
            server.start();
            int port = server.port();

            Socket sub = new Socket("127.0.0.1", port);
            sub.setSoTimeout(5000);
            sendCmd(sub, "PSUBSCRIBE", "news.*");
            readOneReply(sub.getInputStream());  // ack
            Thread.sleep(50);

            try (Socket pub = new Socket("127.0.0.1", port)) {
                pub.setSoTimeout(3000);
                sendCmd(pub, "PUBLISH", "news.sports", "go team");
                readOneReply(pub.getInputStream());  // :1\r\n
            }

            String msg = readOneReply(sub.getInputStream());
            assertTrue(msg.contains("pmessage"), msg);
            assertTrue(msg.contains("news.*"), msg);
            assertTrue(msg.contains("news.sports"), msg);
            assertTrue(msg.contains("go team"), msg);

            sub.close();
        }
    }

    @Test
    void hubGlobMatcherUnitTests() {
        assertTrue(PubSubHub.matches("*", "anything"));
        assertTrue(PubSubHub.matches("news.*", "news.sports"));
        assertTrue(PubSubHub.matches("user.?", "user.1"));
        assertFalse(PubSubHub.matches("news.*", "weather.cold"));
        assertFalse(PubSubHub.matches("user.?", "user.42"));
    }

    /** Reads exactly one RESP reply (one frame, recursively for arrays). */
    private static String readOneReply(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int first = in.read();
        if (first == -1) throw new EOFException();
        buf.write(first);
        switch (first) {
            case '+', '-', ':' -> readLine(in, buf);
            case '$' -> {
                String header = readLine(in, buf);
                int len = Integer.parseInt(header);
                if (len >= 0) {
                    readN(in, buf, len);
                    readCrLf(in, buf);
                }
            }
            case '*' -> {
                String header = readLine(in, buf);
                int n = Integer.parseInt(header);
                for (int i = 0; i < n; i++) {
                    int m = in.read();
                    if (m == -1) throw new EOFException();
                    buf.write(m);
                    if (m == '$') {
                        String h = readLine(in, buf);
                        int l = Integer.parseInt(h);
                        if (l >= 0) { readN(in, buf, l); readCrLf(in, buf); }
                    } else if (m == ':') {
                        readLine(in, buf);
                    } else if (m == '+' || m == '-') {
                        readLine(in, buf);
                    }
                }
            }
            default -> throw new IOException("bad type byte: " + (char) first);
        }
        return buf.toString(StandardCharsets.UTF_8);
    }

    private static String readLine(InputStream in, ByteArrayOutputStream buf) throws IOException {
        StringBuilder s = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            buf.write(b);
            if (b == '\r') {
                int next = in.read();
                if (next != '\n') throw new IOException("expected LF");
                buf.write(next);
                return s.toString();
            }
            s.append((char) b);
        }
        throw new EOFException();
    }

    private static void readN(InputStream in, ByteArrayOutputStream buf, int n) throws IOException {
        byte[] tmp = in.readNBytes(n);
        if (tmp.length != n) throw new EOFException();
        buf.write(tmp);
    }

    private static void readCrLf(InputStream in, ByteArrayOutputStream buf) throws IOException {
        int cr = in.read(), lf = in.read();
        if (cr == -1 || lf == -1) throw new EOFException();
        buf.write(cr); buf.write(lf);
        if (cr != '\r' || lf != '\n') throw new IOException("expected CRLF");
    }
}
