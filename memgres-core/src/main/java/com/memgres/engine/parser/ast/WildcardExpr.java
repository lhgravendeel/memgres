package com.memgres.engine.parser.ast;

/**
 * The * wildcard in SELECT *, or table.* in SELECT t.*
 */
public final class WildcardExpr implements Expression {
    public final String table;

    public WildcardExpr(String table) {
        this.table = table;
    }

    public WildcardExpr() {
        this(null);
    }

    public String table() { return table; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WildcardExpr that = (WildcardExpr) o;
        return java.util.Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(table);
    }

    @Override
    public String toString() {
        return "WildcardExpr[table=" + table + "]";
    }
}
