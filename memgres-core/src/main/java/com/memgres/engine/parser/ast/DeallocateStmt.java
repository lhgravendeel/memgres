package com.memgres.engine.parser.ast;

/**
 * DEALLOCATE [PREPARE] name | DEALLOCATE ALL
 */
public final class DeallocateStmt implements Statement {
    public final String name;
    public final boolean all;

    public DeallocateStmt(String name, boolean all) {
        this.name = name;
        this.all = all;
    }

    public String name() { return name; }
    public boolean all() { return all; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeallocateStmt that = (DeallocateStmt) o;
        return java.util.Objects.equals(name, that.name)
            && all == that.all;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, all);
    }

    @Override
    public String toString() {
        return "DeallocateStmt[name=" + name + ", " + "all=" + all + "]";
    }
}
