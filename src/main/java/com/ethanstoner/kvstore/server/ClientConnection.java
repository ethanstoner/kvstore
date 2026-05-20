package com.ethanstoner.kvstore.server;

import com.ethanstoner.kvstore.server.resp.RespWriter;
import com.ethanstoner.kvstore.server.resp.RespParser;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** One per accepted TCP connection. Runs in its own virtual thread. */
final class ClientConnection implements Runnable, AuthState {

    private final Socket socket;
    private final KvServer server;
    private final CommandHandler handler;
    private boolean authenticated;

    // Pub/Sub state
    private final Set<String> subscribedChannels = ConcurrentHashMap.newKeySet();
    private final Set<String> subscribedPatterns = ConcurrentHashMap.newKeySet();

    // Guards all writes to out, so async deliverMessage calls don't interleave
    private final Object writeLock = new Object();
    private volatile OutputStream out;

    ClientConnection(Socket socket, KvServer server, CommandHandler handler) {
        this.socket = socket;
        this.server = server;
        this.handler = handler;
        this.authenticated = !server.isAuthRequired();
    }

    @Override
    public boolean isAuthenticated() { return authenticated; }

    @Override
    public void markAuthenticated() { this.authenticated = true; }

    // ── Pub/Sub accessors ────────────────────────────────────────────────────

    public boolean isInSubscribeMode() {
        return !subscribedChannels.isEmpty() || !subscribedPatterns.isEmpty();
    }

    public int subscribedCount() {
        return subscribedChannels.size() + subscribedPatterns.size();
    }

    public void addChannel(String channel)  { subscribedChannels.add(channel); }
    public void removeChannel(String channel) { subscribedChannels.remove(channel); }
    public void addPattern(String pattern)  { subscribedPatterns.add(pattern); }
    public void removePattern(String pattern) { subscribedPatterns.remove(pattern); }

    public Set<String> channels() { return Collections.unmodifiableSet(subscribedChannels); }
    public Set<String> patterns() { return Collections.unmodifiableSet(subscribedPatterns); }

    /**
     * Called by PubSubHub on another thread to push a direct-channel message.
     * Synchronizes on writeLock to prevent interleaving with the read loop.
     */
    void deliverMessage(String channel, String message) {
        synchronized (writeLock) {
            try {
                OutputStream o = out;
                if (o == null) return;
                RespWriter.writeArray(o, List.of("message", channel, message));
                o.flush();
            } catch (IOException ignored) {
                // Subscriber's socket is gone; their run() loop will clean up.
            }
        }
    }

    /**
     * Called by PubSubHub on another thread to push a pattern-matched message.
     */
    void deliverPMessage(String pattern, String channel, String message) {
        synchronized (writeLock) {
            try {
                OutputStream o = out;
                if (o == null) return;
                RespWriter.writeArray(o, List.of("pmessage", pattern, channel, message));
                o.flush();
            } catch (IOException ignored) {}
        }
    }

    // ── Main run loop ────────────────────────────────────────────────────────

    @Override
    public void run() {
        server.registerConnection(this);
        try (InputStream in = new BufferedInputStream(socket.getInputStream());
             OutputStream rawOut = new BufferedOutputStream(socket.getOutputStream())) {

            this.out = rawOut;

            while (true) {
                String[] cmd;
                try {
                    cmd = RespParser.readCommand(in);
                } catch (EOFException | SocketException eof) {
                    return;
                }
                if (cmd == null) return;

                synchronized (writeLock) {
                    handler.handle(cmd, rawOut, this);
                    rawOut.flush();
                }
                server.recordCommand();

                if (cmd.length > 0 && "QUIT".equalsIgnoreCase(cmd[0])) {
                    return;
                }
            }
        } catch (IOException ignored) {
        } finally {
            this.out = null;
            closeQuietly();
            server.pubSubHub().removeConnection(this);
            server.unregisterConnection(this);
        }
    }

    void requestStop() {
        closeQuietly();
    }

    private void closeQuietly() {
        try { socket.close(); } catch (IOException ignored) {}
    }
}
