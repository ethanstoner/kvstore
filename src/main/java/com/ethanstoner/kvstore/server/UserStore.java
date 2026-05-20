package com.ethanstoner.kvstore.server;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Stores credentials for one or more users. Username "default" is the
 * conventional Redis name for the single-password fallback.
 *
 * <p>Verification is constant-time via {@link MessageDigest#isEqual}.
 */
public final class UserStore {

    private final Map<String, byte[]> passwords; // username -> UTF-8 password bytes

    private UserStore(Map<String, byte[]> passwords) {
        this.passwords = passwords;
    }

    /** @return {@code true} if any users are configured (i.e., auth is required). */
    public boolean isAuthRequired() {
        return !passwords.isEmpty();
    }

    /** Verify in constant time. Returns {@code true} iff the user exists and the password matches. */
    public boolean check(String username, String attempt) {
        if (username == null) {
            // Still do a dummy compare to keep timing roughly uniform.
            byte[] dummy = "x".getBytes(StandardCharsets.UTF_8);
            byte[] attemptBytes = attempt == null ? new byte[0] : attempt.getBytes(StandardCharsets.UTF_8);
            MessageDigest.isEqual(dummy, attemptBytes);
            return false;
        }
        byte[] expected = passwords.get(username);
        if (expected == null || attempt == null) {
            // Still do a dummy compare to keep timing roughly uniform.
            byte[] dummy = "x".getBytes(StandardCharsets.UTF_8);
            byte[] attemptBytes = attempt == null ? new byte[0] : attempt.getBytes(StandardCharsets.UTF_8);
            MessageDigest.isEqual(dummy, attemptBytes);
            return false;
        }
        byte[] actual = attempt.getBytes(StandardCharsets.UTF_8);
        return MessageDigest.isEqual(expected, actual);
    }

    public boolean hasUser(String username) {
        return passwords.containsKey(username);
    }

    /** Builder for incremental construction from CLI flags. */
    public static final class Builder {
        private final Map<String, byte[]> map = new HashMap<>();

        public Builder addUser(String username, String password) {
            map.put(username, password.getBytes(StandardCharsets.UTF_8));
            return this;
        }

        public UserStore build() {
            return new UserStore(Collections.unmodifiableMap(new HashMap<>(map)));
        }
    }

    public static Builder builder() { return new Builder(); }

    /** An empty store: no users, no auth required. */
    public static UserStore empty() { return new UserStore(Collections.emptyMap()); }

    /** Single-password backward-compatibility shortcut: "default" user with the given password. */
    public static UserStore singleDefault(String password) {
        return builder().addUser("default", password).build();
    }
}
