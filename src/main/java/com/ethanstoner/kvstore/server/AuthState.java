package com.ethanstoner.kvstore.server;

/** Per-connection authentication state. Implemented by ClientConnection, mockable in tests. */
public interface AuthState {
    boolean isAuthenticated();
    void markAuthenticated();

    /** Mark this connection authenticated for the given username. */
    default void markAuthenticated(String username) { markAuthenticated(); }

    /** Returns the authenticated username, or {@code null} if not yet authenticated. */
    default String authenticatedUsername() { return null; }
}
