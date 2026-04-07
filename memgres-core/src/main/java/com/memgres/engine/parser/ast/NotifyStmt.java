package com.memgres.engine.parser.ast;

/**
 * NOTIFY channel [, 'payload']
 */
public final class NotifyStmt implements Statement {
    public final String channel;
    public final String payload;

    public NotifyStmt(String channel, String payload) {
        this.channel = channel;
        this.payload = payload;
    }

    public String channel() { return channel; }
    public String payload() { return payload; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NotifyStmt that = (NotifyStmt) o;
        return java.util.Objects.equals(channel, that.channel)
            && java.util.Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(channel, payload);
    }

    @Override
    public String toString() {
        return "NotifyStmt[channel=" + channel + ", " + "payload=" + payload + "]";
    }
}
