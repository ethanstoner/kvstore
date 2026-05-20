package com.ethanstoner.kvstore.server;

import com.ethanstoner.kvstore.KvStore;
import com.ethanstoner.kvstore.SSTable;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TCP server that fronts a {@link KvStore} with the RESP protocol.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>{@link #start()} — binds the socket, spawns the accept loop on
 *       a platform thread, returns immediately.</li>
 *   <li>Each accepted socket is handed to a virtual thread running a
 *       {@link ClientConnection}.</li>
 *   <li>{@link #stop()} — closes the {@code ServerSocket}, asks each
 *       connection to stop, waits (with timeout) for the active set
 *       to drain.</li>
 * </ol>
 *
 * <p>Does NOT close the underlying {@link KvStore} — the caller owns
 * the store's lifecycle (avoids double-close in try-with-resources).
 */
public final class KvServer implements AutoCloseable {

    private final KvStore store;
    private final int requestedPort;
    private final String requirePassword;

    private final Set<ClientConnection> activeConnections =
            Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ExecutorService connExecutor =
            Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicLong commandsProcessed = new AtomicLong();
    private final AtomicLong connectionsReceived = new AtomicLong();
    private final long startNanos = System.nanoTime();

    private volatile ServerSocket serverSocket;
    private volatile Thread acceptThread;
    private volatile boolean running;

    public KvServer(KvStore store, int port) {
        this(store, port, null);
    }

    public KvServer(KvStore store, int port, String requirePassword) {
        this.store = store;
        this.requestedPort = port;
        this.requirePassword = requirePassword;
    }

    public boolean isAuthRequired() { return requirePassword != null; }

    /**
     * Constant-time password check. Returns false if no password is configured
     * (callers should check isAuthRequired() first if they need to distinguish).
     */
    public boolean checkPassword(String attempt) {
        if (requirePassword == null || attempt == null) return false;
        byte[] expected = requirePassword.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] actual   = attempt.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return java.security.MessageDigest.isEqual(expected, actual);
    }

    public synchronized void start() throws IOException {
        if (running) throw new IllegalStateException("already running");
        this.serverSocket = new ServerSocket(requestedPort);
        this.running = true;
        CommandHandler handler = new CommandHandler(store, this);
        this.acceptThread = new Thread(() -> acceptLoop(handler), "kvstore-accept");
        this.acceptThread.setDaemon(false);
        this.acceptThread.start();
    }

    private void acceptLoop(CommandHandler handler) {
        while (running) {
            Socket client;
            try {
                client = serverSocket.accept();
            } catch (SocketException e) {
                return; // serverSocket closed during stop()
            } catch (IOException e) {
                if (!running) return;
                continue;
            }
            connectionsReceived.incrementAndGet();
            connExecutor.submit(new ClientConnection(client, this, handler));
        }
    }

    public synchronized void stop() throws IOException {
        if (!running) return;
        running = false;

        try { serverSocket.close(); } catch (IOException ignored) {}

        for (ClientConnection c : activeConnections) {
            c.requestStop();
        }

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!activeConnections.isEmpty() && System.nanoTime() < deadline) {
            try { Thread.sleep(50); } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        connExecutor.shutdown();
        try { connExecutor.awaitTermination(2, TimeUnit.SECONDS); }
        catch (InterruptedException ie) { Thread.currentThread().interrupt(); }

        // NOTE: we do NOT close the KvStore here. Caller owns its lifecycle.
    }

    @Override
    public void close() throws IOException { stop(); }

    public int port() {
        return serverSocket == null ? -1 : serverSocket.getLocalPort();
    }

    public boolean isRunning() { return running; }

    void registerConnection(ClientConnection c) { activeConnections.add(c); }
    void unregisterConnection(ClientConnection c) { activeConnections.remove(c); }
    void recordCommand() { commandsProcessed.incrementAndGet(); }

    public long approximateKeyCount() {
        long count = 0;
        count += store.sstableSnapshot().stream()
                      .mapToLong(SSTable::entryCount).sum();
        count += Math.max(0, store.memtableApproximateBytes() / 50);
        return count;
    }

    public String infoText() {
        long uptimeSec = (System.nanoTime() - startNanos) / 1_000_000_000L;
        StringBuilder s = new StringBuilder();
        s.append("# server\n");
        s.append("kvstore_version:0.1.0\n");
        s.append("uptime_in_seconds:").append(uptimeSec).append('\n');
        s.append("process_id:").append(ProcessHandle.current().pid()).append('\n');
        s.append('\n');
        s.append("# stats\n");
        s.append("total_commands_processed:").append(commandsProcessed.get()).append('\n');
        s.append("total_connections_received:").append(connectionsReceived.get()).append('\n');
        s.append("connected_clients:").append(activeConnections.size()).append('\n');
        s.append('\n');
        s.append("# storage\n");
        s.append("memtable_bytes:").append(store.memtableApproximateBytes()).append('\n');
        var ssts = store.sstableSnapshot();
        s.append("sstable_count:").append(ssts.size()).append('\n');
        long totalBytes = 0;
        for (SSTable sst : ssts) {
            try { totalBytes += sst.fileSize(); } catch (IOException ignored) {}
        }
        s.append("sstable_total_bytes:").append(totalBytes).append('\n');
        s.append("dbsize_approx:").append(approximateKeyCount()).append('\n');
        return s.toString();
    }
}
