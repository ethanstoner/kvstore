package com.ethanstoner.kvstore.server;

import com.ethanstoner.kvstore.KvStore;
import com.ethanstoner.kvstore.server.resp.RespWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Dispatches parsed RESP commands to {@link KvStore} and writes responses.
 */
public final class CommandHandler {

    private final KvStore store;
    private final KvServer server;

    public CommandHandler(KvStore store, KvServer server) {
        this.store = store;
        this.server = server;
    }

    public void handle(String[] args, OutputStream out) throws IOException {
        if (args.length == 0) {
            RespWriter.writeError(out, "ERR empty command");
            return;
        }
        String cmd = args[0].toUpperCase(Locale.ROOT);
        try {
            switch (cmd) {
                case "PING"     -> ping(args, out);
                case "SET"      -> set(args, out);
                case "GET"      -> get(args, out);
                case "DEL"      -> del(args, out);
                case "EXISTS"   -> exists(args, out);
                case "MGET"     -> mget(args, out);
                case "MSET"     -> mset(args, out);
                case "SCAN"     -> scan(args, out);
                case "DBSIZE"   -> dbsize(args, out);
                case "INFO"     -> info(args, out);
                case "COMMAND"  -> RespWriter.writeArray(out, List.of());
                case "QUIT"     -> RespWriter.writeSimpleString(out, "OK");
                case "SHUTDOWN" -> shutdown(out);
                default         -> RespWriter.writeError(out,
                        "ERR unknown command '" + args[0] + "'");
            }
        } catch (IOException e) {
            if (e instanceof java.net.SocketException) throw e;
            RespWriter.writeError(out, "ERR internal: " + e.getMessage());
        }
    }

    private void ping(String[] args, OutputStream out) throws IOException {
        if (args.length == 1)      RespWriter.writeSimpleString(out, "PONG");
        else if (args.length == 2) RespWriter.writeBulkString(out, args[1]);
        else wrongArity(out, "PING");
    }

    private void set(String[] args, OutputStream out) throws IOException {
        if (args.length != 3) { wrongArity(out, "SET"); return; }
        store.put(args[1], args[2]);
        RespWriter.writeSimpleString(out, "OK");
    }

    private void get(String[] args, OutputStream out) throws IOException {
        if (args.length != 2) { wrongArity(out, "GET"); return; }
        Optional<String> v = store.get(args[1]);
        RespWriter.writeBulkString(out, v.orElse(null));
    }

    private void del(String[] args, OutputStream out) throws IOException {
        if (args.length < 2) { wrongArity(out, "DEL"); return; }
        long count = 0;
        for (int i = 1; i < args.length; i++) {
            if (store.deleteIfPresent(args[i])) count++;
        }
        RespWriter.writeInteger(out, count);
    }

    private void exists(String[] args, OutputStream out) throws IOException {
        if (args.length < 2) { wrongArity(out, "EXISTS"); return; }
        long count = 0;
        for (int i = 1; i < args.length; i++) {
            if (store.get(args[i]).isPresent()) count++;
        }
        RespWriter.writeInteger(out, count);
    }

    private void mget(String[] args, OutputStream out) throws IOException {
        if (args.length < 2) { wrongArity(out, "MGET"); return; }
        List<String> results = new ArrayList<>(args.length - 1);
        for (int i = 1; i < args.length; i++) {
            results.add(store.get(args[i]).orElse(null));
        }
        RespWriter.writeArray(out, results);
    }

    private void mset(String[] args, OutputStream out) throws IOException {
        if (args.length < 3 || (args.length % 2) != 1) {
            RespWriter.writeError(out, "ERR wrong number of arguments for 'MSET'");
            return;
        }
        for (int i = 1; i < args.length; i += 2) {
            store.put(args[i], args[i + 1]);
        }
        RespWriter.writeSimpleString(out, "OK");
    }

    private void scan(String[] args, OutputStream out) throws IOException {
        if (args.length != 3) {
            RespWriter.writeError(out,
                    "ERR This server uses SCAN <from> <to> (range scan), not cursor-based SCAN");
            return;
        }
        Map<String, String> range = store.scan(args[1], args[2]);
        List<String> flat = new ArrayList<>(range.size() * 2);
        for (Map.Entry<String, String> e : range.entrySet()) {
            flat.add(e.getKey());
            flat.add(e.getValue());
        }
        RespWriter.writeArray(out, flat);
    }

    private void dbsize(String[] args, OutputStream out) throws IOException {
        if (args.length != 1) { wrongArity(out, "DBSIZE"); return; }
        RespWriter.writeInteger(out, server.approximateKeyCount());
    }

    private void info(String[] args, OutputStream out) throws IOException {
        if (args.length > 2) { wrongArity(out, "INFO"); return; }
        String body = server.infoText();
        RespWriter.writeBulkString(out, body);
    }

    private void shutdown(OutputStream out) {
        new Thread(() -> {
            try { server.stop(); } catch (IOException ignored) {}
        }, "kvstore-shutdown").start();
    }

    private static void wrongArity(OutputStream out, String cmd) throws IOException {
        RespWriter.writeError(out,
                "ERR wrong number of arguments for '" + cmd + "'");
    }
}
