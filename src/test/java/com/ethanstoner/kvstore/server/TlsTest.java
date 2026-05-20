package com.ethanstoner.kvstore.server;

import com.ethanstoner.kvstore.KvStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Assumptions;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class TlsTest {

    // ── helpers ─────────────────────────────────────────────────────────────

    /**
     * Invokes `keytool -genkeypair` as a subprocess to generate a self-signed
     * RSA keypair and write it into a JKS keystore at {@code dir/test.jks}.
     */
    private static Path createTempKeystore(Path dir, char[] password) throws Exception {
        Path ksPath = dir.resolve("test.jks");
        String[] cmd = {
            "keytool", "-genkeypair",
            "-alias", "kvstore",
            "-keyalg", "RSA",
            "-keysize", "2048",
            "-validity", "1",
            "-dname", "CN=localhost",
            "-keystore", ksPath.toString(),
            "-storepass", new String(password),
            "-keypass",   new String(password),
            "-storetype", "JKS"
        };
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        if (!p.waitFor(30, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            throw new IOException("keytool timed out");
        }
        if (p.exitValue() != 0) {
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            throw new IOException("keytool failed (exit " + p.exitValue() + "): " + out);
        }
        return ksPath;
    }

    /** Returns true if `keytool` is reachable on PATH. */
    private static boolean keytoolAvailable() {
        try {
            Process p = new ProcessBuilder("keytool", "-help")
                    .redirectErrorStream(true).start();
            return p.waitFor(5, TimeUnit.SECONDS) && p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Builds a client-side {@link SSLContext} that trusts exactly the certificate
     * stored in {@code keystorePath}.  This is needed because the test uses a
     * self-signed cert that no system trust store knows about.
     */
    private static SSLContext clientContextTrustingKeystore(Path keystorePath, char[] password)
            throws Exception {
        KeyStore ks = KeyStore.getInstance("JKS");
        try (FileInputStream in = new FileInputStream(keystorePath.toFile())) {
            ks.load(in, password);
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        SSLContext ctx = SSLContext.getInstance("TLSv1.3");
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
    }

    // ── tests ────────────────────────────────────────────────────────────────

    /**
     * A TLS-enabled server must successfully complete a TLS handshake and respond
     * to a PING with +PONG.
     */
    @Test
    void tlsHandshakeSucceeds(@TempDir Path dir) throws Exception {
        Assumptions.assumeTrue(keytoolAvailable(), "keytool not on PATH — skipping TLS test");

        char[] pass = "testpass".toCharArray();
        Path keystore = createTempKeystore(dir, pass);

        try (KvStore store = new KvStore(dir.resolve("data"));
             KvServer server = new KvServer(store, 0, null,
                     TlsConfig.fromKeystore(keystore, pass))) {
            server.start();
            int port = server.port();

            SSLContext clientCtx = clientContextTrustingKeystore(keystore, pass);
            try (Socket sock = clientCtx.getSocketFactory().createSocket("127.0.0.1", port)) {
                sock.setSoTimeout(5000);
                // Send RESP PING
                String req = "*1\r\n$4\r\nPING\r\n";
                sock.getOutputStream().write(req.getBytes(StandardCharsets.UTF_8));
                sock.getOutputStream().flush();

                byte[] buf = new byte[64];
                int n = sock.getInputStream().read(buf);
                assertTrue(n > 0, "server should have sent a reply");
                String reply = new String(buf, 0, n, StandardCharsets.UTF_8);
                assertEquals("+PONG\r\n", reply);
            }
        }
    }

    /**
     * A plain-TCP connection to a TLS server must fail — the client should see
     * an IOException (typically an SSLHandshakeException or an abrupt disconnect)
     * because the server will try to do a TLS handshake and the plain data the
     * client sends is not a valid ClientHello.
     */
    @Test
    void plainConnectionToTlsServerFails(@TempDir Path dir) throws Exception {
        Assumptions.assumeTrue(keytoolAvailable(), "keytool not on PATH — skipping TLS test");

        char[] pass = "testpass".toCharArray();
        Path keystore = createTempKeystore(dir, pass);

        try (KvStore store = new KvStore(dir.resolve("data2"));
             KvServer server = new KvServer(store, 0, null,
                     TlsConfig.fromKeystore(keystore, pass))) {
            server.start();
            int port = server.port();

            // Attempt a plain-text PING over a raw (non-TLS) socket.
            // The server will begin a TLS handshake; our plaintext bytes are
            // not a valid ClientHello, so the connection must fail.
            assertThrows(IOException.class, () -> {
                try (Socket sock = new Socket("127.0.0.1", port)) {
                    sock.setSoTimeout(5000);
                    String req = "*1\r\n$4\r\nPING\r\n";
                    sock.getOutputStream().write(req.getBytes(StandardCharsets.UTF_8));
                    sock.getOutputStream().flush();

                    // Either read() raises an exception, or it returns -1 (EOF),
                    // which we treat as a protocol-level error.
                    byte[] buf = new byte[256];
                    int n = sock.getInputStream().read(buf);
                    if (n == -1) {
                        throw new IOException("server closed connection (expected for TLS mismatch)");
                    }
                    // If we somehow got data back, check it is NOT a valid RESP +PONG.
                    String reply = new String(buf, 0, n, StandardCharsets.UTF_8);
                    if ("+PONG\r\n".equals(reply)) {
                        throw new AssertionError("plain TCP should not get PONG from TLS server");
                    }
                    // Any other data means the handshake didn't fail immediately;
                    // reading more should eventually produce an error.
                    throw new IOException("unexpected data from TLS server on plain socket: " + reply);
                }
            });
        }
    }

    /**
     * The no-TLS constructor path still works (regression guard).
     */
    @Test
    void noTlsConstructorStillWorks(@TempDir Path dir) throws Exception {
        try (KvStore store = new KvStore(dir);
             KvServer server = new KvServer(store, 0)) {
            server.start();
            int port = server.port();
            try (Socket sock = new Socket("127.0.0.1", port)) {
                sock.setSoTimeout(3000);
                String req = "*1\r\n$4\r\nPING\r\n";
                sock.getOutputStream().write(req.getBytes(StandardCharsets.UTF_8));
                sock.getOutputStream().flush();
                byte[] buf = new byte[64];
                int n = sock.getInputStream().read(buf);
                String reply = new String(buf, 0, n, StandardCharsets.UTF_8);
                assertEquals("+PONG\r\n", reply);
            }
        }
    }
}
