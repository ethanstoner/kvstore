package com.ethanstoner.kvstore.server;

import com.ethanstoner.kvstore.KvStore;
import com.ethanstoner.kvstore.ValueEntry;
import com.ethanstoner.kvstore.server.resp.RespWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
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

        // Subscribed-mode gate: only a subset of commands allowed when subscribed
        if (auth instanceof ClientConnection cc && cc.isInSubscribeMode()) {
            switch (cmd) {
                case "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE",
                     "PING", "QUIT" -> { /* allowed — fall through to main switch */ }
                default -> {
                    RespWriter.writeError(out,
                            "ERR Can't execute '" + cmd + "' in subscribe context, only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed");
                    return;
                }
            }
        }

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

        // Permission check — only when auth is required and command is not in the pre-auth allowlist.
        if (server.isAuthRequired()
                && !cmd.equals("AUTH") && !cmd.equals("PING")
                && !cmd.equals("QUIT") && !cmd.equals("COMMAND")) {
            String user = auth.authenticatedUsername();
            if (user == null) {
                // Defensive: auth gate above should have blocked unauthenticated requests.
                RespWriter.writeError(out, "NOAUTH Authentication required.");
                return;
            }
            if (!server.userStore().canRun(user, cmd)) {
                RespWriter.writeError(out,
                        "NOPERM this user has no permissions to run the '" + cmd + "' command");
                return;
            }
        }

        try {
            switch (cmd) {
                case "AUTH"     -> auth(args, out, auth);
                case "PING"     -> ping(args, out);
                case "SET"      -> set(args, out);
                case "SETEX"    -> setex(args, out, false);
                case "PSETEX"   -> setex(args, out, true);
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
                case "EXPIRE"   -> expire(args, out, false);
                case "PEXPIRE"  -> expire(args, out, true);
                case "TTL"      -> ttl(args, out, false);
                case "PTTL"     -> ttl(args, out, true);
                case "PERSIST"  -> persist(args, out);
                case "SAVE"         -> save(args, out);
                case "BGSAVE"       -> bgsave(args, out);
                case "LASTSAVE"     -> lastsave(args, out);
                case "COMMAND"      -> RespWriter.writeArray(out, List.of());
                case "QUIT"         -> RespWriter.writeSimpleString(out, "OK");
                case "SHUTDOWN"     -> shutdown(out);
                case "SUBSCRIBE"    -> subscribe(args, out, auth, false);
                case "UNSUBSCRIBE"  -> unsubscribe(args, out, auth, false);
                case "PSUBSCRIBE"   -> subscribe(args, out, auth, true);
                case "PUNSUBSCRIBE" -> unsubscribe(args, out, auth, true);
                case "PUBLISH"      -> publish(args, out);
                default             -> RespWriter.writeError(out,
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
            username = "default";
            password = args[1];
        } else {
            username = args[1];
            password = args[2];
        }

        if (server.checkAuth(username, password)) {
            conn.markAuthenticated(username);
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
        if (args.length < 3) { wrongArity(out, "SET"); return; }
        long expiresAtMs = ValueEntry.NEVER;
        // Parse optional EX / PX options
        for (int i = 3; i < args.length; i++) {
            String opt = args[i].toUpperCase(Locale.ROOT);
            if ((opt.equals("EX") || opt.equals("PX")) && i + 1 < args.length) {
                long n;
                try { n = Long.parseLong(args[i + 1]); }
                catch (NumberFormatException e) {
                    RespWriter.writeError(out, "ERR value is not an integer or out of range");
                    return;
                }
                long millis = opt.equals("EX") ? n * 1000L : n;
                expiresAtMs = System.currentTimeMillis() + millis;
                i++; // skip the argument value
            } else {
                RespWriter.writeError(out, "ERR syntax error");
                return;
            }
        }
        store.put(args[1], args[2], expiresAtMs);
        RespWriter.writeSimpleString(out, "OK");
    }

    /** {@code SETEX key seconds value} / {@code PSETEX key milliseconds value}. */
    private void setex(String[] args, OutputStream out, boolean millis) throws IOException {
        String name = millis ? "PSETEX" : "SETEX";
        if (args.length != 4) { wrongArity(out, name); return; }
        long n;
        try { n = Long.parseLong(args[2]); }
        catch (NumberFormatException e) {
            RespWriter.writeError(out, "ERR value is not an integer or out of range");
            return;
        }
        if (n <= 0) {
            RespWriter.writeError(out, "ERR invalid expire time in '"
                    + name.toLowerCase(Locale.ROOT) + "' command");
            return;
        }
        store.put(args[1], args[3], System.currentTimeMillis() + (millis ? n : n * 1000L));
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

    private void expire(String[] args, OutputStream out, boolean millis) throws IOException {
        if (args.length != 3) { wrongArity(out, millis ? "PEXPIRE" : "EXPIRE"); return; }
        long n;
        try { n = Long.parseLong(args[2]); }
        catch (NumberFormatException e) {
            RespWriter.writeError(out, "ERR value is not an integer or out of range");
            return;
        }
        long expiresAtMs = System.currentTimeMillis() + (millis ? n : n * 1000L);
        boolean ok = store.expire(args[1], expiresAtMs);
        RespWriter.writeInteger(out, ok ? 1 : 0);
    }

    private void ttl(String[] args, OutputStream out, boolean millis) throws IOException {
        if (args.length != 2) { wrongArity(out, millis ? "PTTL" : "TTL"); return; }
        long remaining = store.ttl(args[1]);
        if (remaining < 0) { RespWriter.writeInteger(out, remaining); return; }
        RespWriter.writeInteger(out, millis ? remaining : remaining / 1000L);
    }

    private void persist(String[] args, OutputStream out) throws IOException {
        if (args.length != 2) { wrongArity(out, "PERSIST"); return; }
        boolean removed = store.persist(args[1]);
        RespWriter.writeInteger(out, removed ? 1 : 0);
    }

    // ── Pub/Sub commands ─────────────────────────────────────────────────────

    private void subscribe(String[] args, OutputStream out, AuthState auth, boolean isPattern) throws IOException {
        if (args.length < 2) { wrongArity(out, isPattern ? "PSUBSCRIBE" : "SUBSCRIBE"); return; }
        if (!(auth instanceof ClientConnection cc)) {
            RespWriter.writeError(out, "ERR pub/sub requires a real client connection");
            return;
        }
        PubSubHub hub = server.pubSubHub();
        String tag = isPattern ? "psubscribe" : "subscribe";
        for (int i = 1; i < args.length; i++) {
            String target = args[i];
            if (isPattern) { hub.psubscribe(target, cc); cc.addPattern(target); }
            else           { hub.subscribe(target, cc);  cc.addChannel(target); }
            // Write 3-element array with integer count as 3rd element
            writePubSubReply(out, tag, target, cc.subscribedCount());
        }
    }

    private void unsubscribe(String[] args, OutputStream out, AuthState auth, boolean isPattern) throws IOException {
        if (!(auth instanceof ClientConnection cc)) {
            RespWriter.writeError(out, "ERR pub/sub requires a real client connection");
            return;
        }
        PubSubHub hub = server.pubSubHub();
        String tag = isPattern ? "punsubscribe" : "unsubscribe";

        List<String> targets = new ArrayList<>();
        if (args.length >= 2) {
            for (int i = 1; i < args.length; i++) targets.add(args[i]);
        } else {
            // No args: unsubscribe from all
            targets.addAll(isPattern ? cc.patterns() : cc.channels());
        }

        if (targets.isEmpty()) {
            // Special case: nothing to unsubscribe from — reply with null channel
            out.write('*');
            out.write("3\r\n".getBytes(StandardCharsets.UTF_8));
            RespWriter.writeBulkString(out, tag);
            RespWriter.writeBulkString(out, null);
            RespWriter.writeInteger(out, 0);
            return;
        }

        for (String target : targets) {
            if (isPattern) { hub.punsubscribe(target, cc); cc.removePattern(target); }
            else           { hub.unsubscribe(target, cc);  cc.removeChannel(target); }
            writePubSubReply(out, tag, target, cc.subscribedCount());
        }
    }

    private void publish(String[] args, OutputStream out) throws IOException {
        if (args.length != 3) { wrongArity(out, "PUBLISH"); return; }
        int n = server.pubSubHub().publish(args[1], args[2]);
        RespWriter.writeInteger(out, n);
    }

    /** Writes a 3-element pub/sub reply array: [tag, target, intCount]. */
    private static void writePubSubReply(OutputStream out, String tag, String target, int count) throws IOException {
        out.write('*');
        out.write("3\r\n".getBytes(StandardCharsets.UTF_8));
        RespWriter.writeBulkString(out, tag);
        RespWriter.writeBulkString(out, target);
        RespWriter.writeInteger(out, count);
    }

    // ── Snapshot commands ────────────────────────────────────────────────────

    private void save(String[] args, OutputStream out) throws IOException {
        if (args.length != 1) { wrongArity(out, "SAVE"); return; }
        try {
            store.snapshot();
            RespWriter.writeSimpleString(out, "OK");
        } catch (IOException e) {
            RespWriter.writeError(out, "ERR snapshot failed: " + e.getMessage());
        }
    }

    private void bgsave(String[] args, OutputStream out) throws IOException {
        if (args.length > 2) { wrongArity(out, "BGSAVE"); return; }
        // Redis accepts an optional SCHEDULE flag; snapshots here are always backgrounded.
        if (args.length == 2 && !args[1].equalsIgnoreCase("SCHEDULE")) {
            RespWriter.writeError(out, "ERR syntax error");
            return;
        }
        store.bgsnapshot();
        RespWriter.writeSimpleString(out, "Background saving started");
    }

    private void lastsave(String[] args, OutputStream out) throws IOException {
        if (args.length != 1) { wrongArity(out, "LASTSAVE"); return; }
        RespWriter.writeInteger(out, store.lastSnapshotEpochSec());
    }

    private static void wrongArity(OutputStream out, String cmd) throws IOException {
        RespWriter.writeError(out,
                "ERR wrong number of arguments for '" + cmd + "'");
    }
}
