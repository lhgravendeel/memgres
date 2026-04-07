package com.memgres.engine;

/**
 * An asynchronous notification message for LISTEN/NOTIFY.
 */
public final class Notification {
    public final int pid;
    public final String channel;
    public final String payload;

    public Notification(int pid, String channel, String payload) {
        this.pid = pid;
        this.channel = channel;
        this.payload = payload;
    }

    public int pid() { return pid; }
    public String channel() { return channel; }
    public String payload() { return payload; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Notification that = (Notification) o;
        return pid == that.pid
            && java.util.Objects.equals(channel, that.channel)
            && java.util.Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(pid, channel, payload);
    }

    @Override
    public String toString() {
        return "Notification[pid=" + pid + ", " + "channel=" + channel + ", " + "payload=" + payload + "]";
    }
}
