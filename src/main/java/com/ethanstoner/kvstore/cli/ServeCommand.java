package com.ethanstoner.kvstore.cli;

import com.ethanstoner.kvstore.KvStore;
import com.ethanstoner.kvstore.server.KvServer;
import com.ethanstoner.kvstore.server.TlsConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.concurrent.CountDownLatch;

/** Handles `serve [--port N] [--data DIR] [--tls-keystore PATH] [--tls-keystore-pass PASS]`. */
public final class ServeCommand {

    public static void run(String[] args) throws IOException, InterruptedException {
        int port = 6379;
        Path dataDir = Path.of("kvdata");
        String password = null;
        Path tlsKeystorePath = null;
        String tlsKeystorePass = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> {
                    if (++i >= args.length) { usage(); return; }
                    port = Integer.parseInt(args[i]);
                }
                case "--data" -> {
                    if (++i >= args.length) { usage(); return; }
                    dataDir = Path.of(args[i]);
                }
                case "--requirepass" -> {
                    if (++i >= args.length) { usage(); return; }
                    password = args[i];
                }
                case "--tls-keystore" -> {
                    if (++i >= args.length) { usage(); return; }
                    tlsKeystorePath = Path.of(args[i]);
                }
                case "--tls-keystore-pass" -> {
                    if (++i >= args.length) { usage(); return; }
                    tlsKeystorePass = args[i];
                }
                case "--help", "-h" -> { usage(); return; }
                default -> {
                    System.err.println("Unknown arg: " + args[i]);
                    usage();
                    return;
                }
            }
        }

        TlsConfig tls = null;
        if (tlsKeystorePath != null) {
            char[] ksPass = tlsKeystorePass != null ? tlsKeystorePass.toCharArray() : new char[0];
            try {
                tls = TlsConfig.fromKeystore(tlsKeystorePath, ksPass);
            } catch (GeneralSecurityException e) {
                System.err.println("Failed to load TLS keystore: " + e.getMessage());
                System.exit(1);
            }
        }

        KvStore store = new KvStore(dataDir);
        KvServer server = new KvServer(store, port, password, tls);
        server.start();
        System.out.println("kvstore server listening on " + server.port()
                + (tls != null ? " [TLS]" : "")
                + " (data dir: " + dataDir.toAbsolutePath() + ")");

        CountDownLatch done = new CountDownLatch(1);
        final KvStore storeRef = store;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { server.stop(); } catch (IOException ignored) {}
            try { storeRef.close(); } catch (IOException ignored) {}
            done.countDown();
        }, "kvstore-shutdown-hook"));
        done.await();
    }

    private static void usage() {
        System.out.println("""
                kvstore serve [--port PORT] [--data DIR] [--requirepass PASSWORD]
                              [--tls-keystore PATH] [--tls-keystore-pass PASS]
                  --port               port to listen on (default 6379)
                  --data               data directory (default ./kvdata)
                  --requirepass        require AUTH <password> before commands (default: no auth)
                  --tls-keystore       path to a JKS keystore for TLS (enables TLS mode)
                  --tls-keystore-pass  password for the JKS keystore (default: empty)

                TLS mode: when --tls-keystore is provided the server accepts only TLS
                connections on the configured port. Use redis-cli --tls to connect.

                To convert PEM files to a JKS keystore:
                  openssl pkcs12 -export -in cert.pem -inkey key.pem -out server.p12 -name kvstore
                  keytool -importkeystore -srckeystore server.p12 -srcstoretype PKCS12 \\
                          -destkeystore server.jks -deststoretype JKS -deststorepass <pass>
                """);
    }
}
