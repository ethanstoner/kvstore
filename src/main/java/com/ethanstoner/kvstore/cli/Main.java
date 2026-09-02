package com.ethanstoner.kvstore.cli;

import com.ethanstoner.kvstore.KvStore;

import java.nio.file.Path;
import java.util.Map;

/**
 * Tiny command-line front end so the store is demoable without writing code.
 *
 * <pre>
 *   java -jar kvstore.jar put user:42 Ethan
 *   java -jar kvstore.jar get user:42
 *   java -jar kvstore.jar del user:42
 *   java -jar kvstore.jar scan user: user;     # [from, to)
 * </pre>
 *
 * Data persists in {@code ./kvdata/} between invocations &mdash; run a {@code put}
 * then a separate {@code get} to see WAL recovery working for real.
 */
public final class Main {

    private static final Path DATA_DIR = Path.of("kvdata");

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            return;
        }
        String cmd = args[0].toLowerCase();
        try (KvStore store = new KvStore(DATA_DIR)) {
            switch (cmd) {
                case "put" -> {
                    expect(args, 3, "put <key> <value>");
                    store.put(args[1], args[2]);
                    System.out.println("OK");
                }
                case "get" -> {
                    expect(args, 2, "get <key>");
                    System.out.println(store.get(args[1]).orElse("(not found)"));
                }
                case "del" -> {
                    expect(args, 2, "del <key>");
                    store.delete(args[1]);
                    System.out.println("OK");
                }
                case "scan" -> {
                    expect(args, 3, "scan <fromInclusive> <toExclusive>");
                    Map<String, String> rows = store.scan(args[1], args[2]);
                    if (rows.isEmpty()) {
                        System.out.println("(empty range)");
                    } else {
                        rows.forEach((k, v) -> System.out.println(k + " = " + v));
                    }
                }
                default -> usage();
            }
        }
    }

    private static void expect(String[] args, int n, String form) {
        if (args.length < n) {
            throw new IllegalArgumentException("usage: " + form);
        }
    }

    private static void usage() {
        System.out.println("""
                kvstore - commands:
                  put   <key> <value>
                  get   <key>
                  del   <key>
                  scan  <fromInclusive> <toExclusive>
                  serve [--port PORT] [--data DIR] ...   start the Redis-compatible server
                                                         (run `serve --help` for all flags)
                """);
    }
}
