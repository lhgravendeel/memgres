package com.memgres.engine.parser.ast;

/** DROP OWNED BY role [CASCADE|RESTRICT] */
public final class DropOwnedStmt implements Statement {
    public final String role;
    public final boolean cascade;

    public DropOwnedStmt(String role, boolean cascade) {
        this.role = role;
        this.cascade = cascade;
    }

    public String role() { return role; }
    public boolean cascade() { return cascade; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DropOwnedStmt that = (DropOwnedStmt) o;
        return java.util.Objects.equals(role, that.role)
            && cascade == that.cascade;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(role, cascade);
    }

    @Override
    public String toString() {
        return "DropOwnedStmt[role=" + role + ", " + "cascade=" + cascade + "]";
    }
}
