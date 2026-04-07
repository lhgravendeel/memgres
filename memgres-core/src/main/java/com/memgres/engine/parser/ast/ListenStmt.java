package com.memgres.engine.parser.ast;

/**
 * LISTEN channel
 */
public final class ListenStmt implements Statement {
    public final String channel;

    public ListenStmt(String channel) {
        this.channel = channel;
    }

    public String channel() { return channel; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ListenStmt that = (ListenStmt) o;
        return java.util.Objects.equals(channel, that.channel);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(channel);
    }

    @Override
    public String toString() {
        return "ListenStmt[channel=" + channel + "]";
    }
}
