package com.ethanstoner.kvstore.server;

/** Per-connection authentication state. Implemented by ClientConnection, mockable in tests. */
public interface AuthState {
    boolean isAuthenticated();
    void markAuthenticated();
}
