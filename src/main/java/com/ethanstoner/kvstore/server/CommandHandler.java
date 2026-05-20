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

    /** Test-only convenience: handle a command on an already-authenticated stub connection. */
    void handle(String[] args, OutputStream out) throws IOException {
        handle(args, out, ALWAYS_AUTHED);
    }

    public void handle(String[] args, OutputStream out, AuthState auth) throws IOException {
        if (args.length == 0) {
            RespWriter.writeError(out, "ERR empty command");
            return;
        }
        String cmd = args[0].toUpperCase(Locale.ROOT);

        // Pre-auth gate
        if (!auth.isAuthenticated()) {
            if (cmd.equals("AUTH")) {
                auth(args, out, auth);
                return;
            }
            if (!cmd.equals("PING") && !cmd.equals("QUIT") && !cmd.equals("COMMAND")) {
                RespWriter.writeError(out, "NOAUTH Authentication required.");
                return;
            }
        }

        try {
            switch (cmd) {
                case "AUTH"     -> auth(args, out, auth);
                case "PING"     -> ping(args, out);
                case "SET"      -> set(args, out);
                case "GET"      -> get(args, out);
                case "DEL"      -> del(args, out);
                case "EXISTS"   -> exists(args, out);
                case "MGET"     -> mget(args, out);
                case "MSET"     -> mset(args, out);
                case "SCAN"     -> scan(args, out);
                case "INCR"     -> incrBy(args, out, 1, "INCR", 2);
                case "DECR"     -> incrBy(args, out, -1, "DECR", 2);
                case "INCRBY"   -> incrByWithArg(args, out, "INCRBY", false);
                case "DECRBY"   -> incrByWithArg(args, out, "DECRBY", true);
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

    private static final AuthState ALWAYS_AUTHED = new AuthState() {
        @Override public boolean isAuthenticated() { return true; }
        @Override public void markAuthenticated() {}
    };

    private void auth(String[] args, OutputStream out, AuthState conn) throws IOException {
        if (args.length < 2 || args.length > 3) {
            wrongArity(out, "AUTH");
            return;
        }
        if (!server.isAuthRequired()) {
            RespWriter.writeError(out,
                    "ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?");
            return;
        }

        String username;
        String password;
        if (args.length == 2) {
            // Implicit "default" user — backward-compatible single-password mode
            username = "default";
            password = args[1];
        } else {
            username = args[1];
            password = args[2];
        }

        if (server.checkAuth(username, password)) {
            conn.markAuthenticated();
            RespWriter.writeSimpleString(out, "OK");
        } else {
            RespWriter.writeError(out,
                    "WRONGPASS invalid username-password pair or user is disabled.");
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

    private void incrBy(String[] args, OutputStream out, long delta, String cmd, int expectedArity) throws IOException {
        if (args.length != expectedArity) { wrongArity(out, cmd); return; }
        try {
            long result = store.incrementBy(args[1], delta);
            RespWriter.writeInteger(out, result);
        } catch (NumberFormatException e) {
            RespWriter.writeError(out, "ERR value is not an integer or out of range");
        } catch (ArithmeticException e) {
            RespWriter.writeError(out, "ERR increment or decrement would overflow");
        }
    }

    private void incrByWithArg(String[] args, OutputStream out, String cmd, boolean negate) throws IOException {
        if (args.length != 3) { wrongArity(out, cmd); return; }
        long delta;
        try {
            delta = Long.parseLong(args[2]);
        } catch (NumberFormatException e) {
            RespWriter.writeError(out, "ERR value is not an integer or out of range");
            return;
        }
        if (negate) delta = -delta;  // for DECRBY
        try {
            long result = store.incrementBy(args[1], delta);
            RespWriter.writeInteger(out, result);
        } catch (NumberFormatException e) {
            RespWriter.writeError(out, "ERR value is not an integer or out of range");
        } catch (ArithmeticException e) {
            RespWriter.writeError(out, "ERR increment or decrement would overflow");
        }
    }

    private static void wrongArity(OutputStream out, String cmd) throws IOException {
        RespWriter.writeError(out,
                "ERR wrong number of arguments for '" + cmd + "'");
    }
}
