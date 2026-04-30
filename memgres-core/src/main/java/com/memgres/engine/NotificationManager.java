package com.memgres.engine;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Central pub/sub registry for LISTEN/NOTIFY.
 * Thread-safe and shared across all connections.
 */
public class NotificationManager {

    private final Map<String, List<Session>> listeners = new ConcurrentHashMap<>();

    public void listen(Session session, String channel) {
        List<Session> sessions = listeners.computeIfAbsent(channel.toLowerCase(), k -> new CopyOnWriteArrayList<>());
        synchronized (sessions) {
            if (!sessions.contains(session)) {
                sessions.add(session);
            }
        }
    }

    public void unlisten(Session session, String channel) {
        List<Session> sessions = listeners.get(channel.toLowerCase());
        if (sessions != null) {
            sessions.remove(session);
        }
    }

    public void unlistenAll(Session session) {
        for (List<Session> sessions : listeners.values()) {
            sessions.remove(session);
        }
    }

    public List<String> getListeningChannels(Session session) {
        List<String> channels = new ArrayList<>();
        for (Map.Entry<String, List<Session>> entry : listeners.entrySet()) {
            if (entry.getValue().contains(session)) {
                channels.add(entry.getKey());
            }
        }
        Collections.sort(channels);
        return channels;
    }

    public void notify(String channel, String payload, int senderPid) {
        List<Session> sessions = listeners.get(channel.toLowerCase());
        if (sessions != null) {
            Notification notification = new Notification(senderPid, channel, payload != null ? payload : "");
            for (Session s : sessions) {
                s.addNotification(notification);
            }
        }
    }
}
