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
}
