package com.memgres.engine.parser.ast;

/**
 * DISCARD ALL | DISCARD PLANS | DISCARD SEQUENCES | DISCARD TEMP
 */
public final class DiscardStmt implements Statement {
    public final String target;

    public DiscardStmt(String target) {
        this.target = target;
    }

    public String target() { return target; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DiscardStmt that = (DiscardStmt) o;
        return java.util.Objects.equals(target, that.target);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(target);
    }

    @Override
    public String toString() {
        return "DiscardStmt[target=" + target + "]";
    }
}
