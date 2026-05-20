package com.ethanstoner.kvstore.server;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Routes published messages to subscribed connections.
 *
 * <p>Thread-safe: subscribe/unsubscribe and publish may race; the worst
 * case is that a just-subscribed client misses one in-flight publish
 * (acceptable per Redis semantics, which don't guarantee delivery).
 */
public final class PubSubHub {

    private final Map<String, Set<ClientConnection>> channelSubs = new ConcurrentHashMap<>();
    private final Map<String, Set<ClientConnection>> patternSubs = new ConcurrentHashMap<>();

    public void subscribe(String channel, ClientConnection conn) {
        channelSubs.computeIfAbsent(channel, k -> ConcurrentHashMap.newKeySet()).add(conn);
    }

    public void unsubscribe(String channel, ClientConnection conn) {
        Set<ClientConnection> set = channelSubs.get(channel);
        if (set != null) {
            set.remove(conn);
            if (set.isEmpty()) channelSubs.remove(channel);
        }
    }

    public void psubscribe(String pattern, ClientConnection conn) {
        patternSubs.computeIfAbsent(pattern, k -> ConcurrentHashMap.newKeySet()).add(conn);
    }

    public void punsubscribe(String pattern, ClientConnection conn) {
        Set<ClientConnection> set = patternSubs.get(pattern);
        if (set != null) {
            set.remove(conn);
            if (set.isEmpty()) patternSubs.remove(pattern);
        }
    }

    /** Drops a connection from all subscriptions (called when client disconnects). */
    public void removeConnection(ClientConnection conn) {
        channelSubs.values().forEach(set -> set.remove(conn));
        patternSubs.values().forEach(set -> set.remove(conn));
    }

    /**
     * Publishes a message and returns the number of receivers (deduplicated
     * across direct channel subscribers + pattern subscribers).
     */
    public int publish(String channel, String message) {
        Set<ClientConnection> recipients = new HashSet<>();
        Set<ClientConnection> direct = channelSubs.get(channel);
        if (direct != null) {
            for (ClientConnection c : direct) {
                if (recipients.add(c)) c.deliverMessage(channel, message);
            }
        }
        for (Map.Entry<String, Set<ClientConnection>> e : patternSubs.entrySet()) {
            if (matches(e.getKey(), channel)) {
                for (ClientConnection c : e.getValue()) {
                    if (recipients.add(c)) c.deliverPMessage(e.getKey(), channel, message);
                }
            }
        }
        return recipients.size();
    }

    /** Simple glob match: * matches anything, ? matches one char. */
    static boolean matches(String pattern, String text) {
        StringBuilder regex = new StringBuilder("^");
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            switch (c) {
                case '*' -> regex.append(".*");
                case '?' -> regex.append('.');
                case '.', '(', ')', '+', '|', '^', '$', '@', '%', '\\' -> regex.append("\\").append(c);
                default -> regex.append(c);
            }
        }
        regex.append("$");
        return text.matches(regex.toString());
    }
}
