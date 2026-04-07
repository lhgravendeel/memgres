package com.memgres.engine.parser.ast;

/**
 * UNLISTEN channel | UNLISTEN *
 */
public final class UnlistenStmt implements Statement {
    public final String channel;

    public UnlistenStmt(String channel) {
        this.channel = channel;
    }

    public String channel() { return channel; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UnlistenStmt that = (UnlistenStmt) o;
        return java.util.Objects.equals(channel, that.channel);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(channel);
    }

    @Override
    public String toString() {
        return "UnlistenStmt[channel=" + channel + "]";
    }
}
