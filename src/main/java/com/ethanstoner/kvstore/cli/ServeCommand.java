package com.ethanstoner.kvstore.cli;

import com.ethanstoner.kvstore.KvStore;
import com.ethanstoner.kvstore.server.KvServer;
import com.ethanstoner.kvstore.server.TlsConfig;
import com.ethanstoner.kvstore.server.UserStore;

import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.concurrent.CountDownLatch;

/** Handles `serve [--port N] [--data DIR] [--tls-keystore PATH] [--tls-keystore-pass PASS]`. */
public final class ServeCommand {

    public static void run(String[] args) throws IOException, InterruptedException {
        int port = 6379;
        Path dataDir = Path.of("kvdata");
        String singlePassword = null;
        UserStore.Builder userBuilder = UserStore.builder();
        boolean anyUsers = false;
        Path tlsKeystorePath = null;
        String tlsKeystorePass = null;
        int maxConnections = 0;

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
                    singlePassword = args[i];
                }
                case "--user" -> {
                    if (++i >= args.length) { usage(); return; }
                    String spec = args[i];
                    int colon = spec.indexOf(':');
                    if (colon < 1 || colon == spec.length() - 1) {
                        System.err.println("--user must be in form name:password");
                        return;
                    }
                    userBuilder.addUser(spec.substring(0, colon), spec.substring(colon + 1));
                    anyUsers = true;
                }
                case "--tls-keystore" -> {
                    if (++i >= args.length) { usage(); return; }
                    tlsKeystorePath = Path.of(args[i]);
                }
                case "--tls-keystore-pass" -> {
                    if (++i >= args.length) { usage(); return; }
                    tlsKeystorePass = args[i];
                }
                case "--max-connections" -> {
                    if (++i >= args.length) { usage(); return; }
                    try {
                        maxConnections = Integer.parseInt(args[i]);
                    } catch (NumberFormatException e) {
                        System.err.println("--max-connections must be an integer");
                        return;
                    }
                }
                case "--help", "-h" -> { usage(); return; }
                default -> {
                    System.err.println("Unknown arg: " + args[i]);
                    usage();
                    return;
                }
            }
        }

        if (singlePassword != null) {
            userBuilder.addUser("default", singlePassword);
            anyUsers = true;
        }
        UserStore users = anyUsers ? userBuilder.build() : UserStore.empty();

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
        KvServer server = new KvServer(store, port, users, tls, maxConnections);
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
                              [--user name:password] [--tls-keystore PATH] [--tls-keystore-pass PASS]
                              [--max-connections N]
                  --port               port to listen on (default 6379)
                  --data               data directory (default ./kvdata)
                  --requirepass        single-password mode: require AUTH <password> (the "default" user)
                  --user name:pass     multi-user mode: add a named user (repeatable)
                                       e.g. --user alice:secret1 --user bob:secret2
                                       --requirepass and --user can be combined; --requirepass sets "default"
                  --tls-keystore       path to a JKS keystore for TLS (enables TLS mode)
                  --tls-keystore-pass  password for the JKS keystore (default: empty)
                  --max-connections    maximum concurrent connections allowed (0 = unlimited, default 0)

                TLS mode: when --tls-keystore is provided the server accepts only TLS
                connections on the configured port. Use redis-cli --tls to connect.

                To convert PEM files to a JKS keystore:
                  openssl pkcs12 -export -in cert.pem -inkey key.pem -out server.p12 -name kvstore
                  keytool -importkeystore -srckeystore server.p12 -srcstoretype PKCS12 \\
                          -destkeystore server.jks -deststoretype JKS -deststorepass <pass>
                """);
    }
}
