package com.ethanstoner.kvstore.server;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class UserStoreTest {

    @Test
    void emptyStoreHasNoAuthRequired() {
        UserStore s = UserStore.empty();
        assertFalse(s.isAuthRequired());
        assertFalse(s.check("anyone", "anything"));
    }

    @Test
    void singleDefaultUserOnlyAcceptsDefault() {
        UserStore s = UserStore.singleDefault("topsecret");
        assertTrue(s.isAuthRequired());
        assertTrue(s.check("default", "topsecret"));
        assertFalse(s.check("default", "wrong"));
        assertFalse(s.check("alice", "topsecret"));
    }

    @Test
    void multipleUsersIsolatedByName() {
        UserStore s = UserStore.builder()
                .addUser("alice", "wonderland")
                .addUser("bob", "builder")
                .build();
        assertTrue(s.check("alice", "wonderland"));
        assertTrue(s.check("bob", "builder"));
        assertFalse(s.check("alice", "builder"));
        assertFalse(s.check("bob", "wonderland"));
        assertFalse(s.check("carol", "wonderland"));
    }

    @Test
    void nullInputsRejected() {
        UserStore s = UserStore.singleDefault("x");
        assertFalse(s.check("default", null));
        assertFalse(s.check(null, "x"));
    }

    @Test
    void hasUserReturnsTrueOnlyForConfiguredUsers() {
        UserStore s = UserStore.builder()
                .addUser("alice", "pass")
                .build();
        assertTrue(s.hasUser("alice"));
        assertFalse(s.hasUser("bob"));
        assertFalse(s.hasUser("default"));
    }

    @Test
    void userWithEmptyAllowlistCanRunAnyCommand() {
        UserStore s = UserStore.builder().addUser("admin", "x").build();
        assertTrue(s.canRun("admin", "SET"));
        assertTrue(s.canRun("admin", "DEL"));
        assertTrue(s.canRun("admin", "FLUSHALL"));
    }

    @Test
    void userWithAllowlistOnlyRunsAllowedCommands() {
        UserStore s = UserStore.builder()
                .addUser("readonly", "x", java.util.Set.of("GET", "MGET", "EXISTS"))
                .build();
        assertTrue(s.canRun("readonly", "GET"));
        assertTrue(s.canRun("readonly", "MGET"));
        assertFalse(s.canRun("readonly", "SET"));
        assertFalse(s.canRun("readonly", "DEL"));
    }

    @Test
    void allowlistCheckIsCaseInsensitive() {
        UserStore s = UserStore.builder()
                .addUser("u", "p", java.util.Set.of("get", "set"))
                .build();
        assertTrue(s.canRun("u", "GET"));
        assertTrue(s.canRun("u", "get"));
        assertTrue(s.canRun("u", "Set"));
    }

    @Test
    void missingUserCannotRunAnyCommand() {
        UserStore s = UserStore.builder().addUser("u", "p", java.util.Set.of("GET")).build();
        assertFalse(s.canRun("ghost", "GET"));
    }
}
