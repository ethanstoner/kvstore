package com.ethanstoner.kvstore.server;

import com.ethanstoner.kvstore.KvStore;

import java.io.IOException;

/** Stub — full implementation in Task 6. Provides the signatures CommandHandler needs. */
public final class KvServer implements AutoCloseable {

    private final KvStore store;
    private final int port;

    public KvServer(KvStore store, int port) {
        this.store = store;
        this.port = port;
    }

    public long approximateKeyCount() { return 0; }

    public String infoText() {
        return "# server\nkvstore_version:0.1.0\n";
    }

    public void stop() throws IOException { /* real impl in Task 6 */ }

    @Override
    public void close() throws IOException { stop(); }
}
