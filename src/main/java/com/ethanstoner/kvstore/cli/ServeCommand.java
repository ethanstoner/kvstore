package com.ethanstoner.kvstore.cli;

import com.ethanstoner.kvstore.KvStore;
import com.ethanstoner.kvstore.server.KvServer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

/** Handles `serve [--port N] [--data DIR]`. */
public final class ServeCommand {

    public static void run(String[] args) throws IOException, InterruptedException {
        int port = 6379;
        Path dataDir = Path.of("kvdata");
        String password = null;

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
                case "--help", "-h" -> { usage(); return; }
                default -> {
                    System.err.println("Unknown arg: " + args[i]);
                    usage();
                    return;
                }
            }
        }

        KvStore store = new KvStore(dataDir);
        KvServer server = new KvServer(store, port, password);
        server.start();
        System.out.println("kvstore server listening on " + server.port()
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
                  --port          port to listen on (default 6379)
                  --data          data directory (default ./kvdata)
                  --requirepass   require AUTH <password> before commands (default: no auth)
                """);
    }
}
