package com.ethanstoner.kvstore.server;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Stores credentials for one or more users. Username "default" is the
 * conventional Redis name for the single-password fallback.
 *
 * <p>Verification is constant-time via {@link MessageDigest#isEqual}.
 *
 * <p>Each user can optionally have a command allowlist. An empty allowlist
 * means all commands are permitted (default). A non-empty allowlist restricts
 * the user to only those commands (case-insensitive, Redis 6 ACL style).
 */
public final class UserStore {

    /**
     * Immutable per-user record: password bytes + optional command allowlist.
     * An empty {@code allowedCommands} set means all commands are allowed.
     */
    public record User(byte[] password, Set<String> allowedCommands) {
        /**
         * Returns {@code true} if this user is permitted to run the given command.
         * An empty allowlist means all commands are allowed.
         */
        public boolean canRun(String command) {
            return allowedCommands.isEmpty()
                || allowedCommands.contains(command.toUpperCase(Locale.ROOT));
        }
    }

    private final Map<String, User> users; // username -> User

    private UserStore(Map<String, User> users) {
        this.users = users;
    }

    /** @return {@code true} if any users are configured (i.e., auth is required). */
    public boolean isAuthRequired() {
        return !users.isEmpty();
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
        User user = users.get(username);
        if (user == null || attempt == null) {
            // Still do a dummy compare to keep timing roughly uniform.
            byte[] dummy = "x".getBytes(StandardCharsets.UTF_8);
            byte[] attemptBytes = attempt == null ? new byte[0] : attempt.getBytes(StandardCharsets.UTF_8);
            MessageDigest.isEqual(dummy, attemptBytes);
            return false;
        }
        byte[] actual = attempt.getBytes(StandardCharsets.UTF_8);
        return MessageDigest.isEqual(user.password(), actual);
    }

    public boolean hasUser(String username) {
        return users.containsKey(username);
    }

    /**
     * Returns the {@link User} record for the given username, or {@code null} if not found.
     */
    public User getUser(String username) {
        return users.get(username);
    }

    /**
     * Returns {@code true} if the given user is allowed to run the given command.
     * Returns {@code false} if the user does not exist.
     */
    public boolean canRun(String username, String command) {
        User user = users.get(username);
        if (user == null) return false;
        return user.canRun(command);
    }

    /** Builder for incremental construction from CLI flags. */
    public static final class Builder {
        private final Map<String, User> map = new HashMap<>();

        /** Add a user with no command restrictions (all commands allowed). */
        public Builder addUser(String username, String password) {
            return addUser(username, password, Set.of());
        }

        /** Add a user with an explicit command allowlist. Empty set = all allowed. */
        public Builder addUser(String username, String password, Set<String> allowedCommands) {
            Set<String> normalized = new HashSet<>();
            for (String c : allowedCommands) normalized.add(c.toUpperCase(Locale.ROOT));
            map.put(username, new User(
                    password.getBytes(StandardCharsets.UTF_8),
                    Collections.unmodifiableSet(normalized)));
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
