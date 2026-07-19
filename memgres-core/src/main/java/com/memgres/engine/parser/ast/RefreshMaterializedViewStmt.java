package com.memgres.engine.parser.ast;

/**
 * REFRESH MATERIALIZED VIEW [CONCURRENTLY] name [WITH [NO] DATA]
 */
public final class RefreshMaterializedViewStmt implements Statement {
    public final String name;
    /** False for REFRESH ... WITH NO DATA (leaves the view unpopulated). */
    public final boolean withData;

    public RefreshMaterializedViewStmt(String name) {
        this(name, true);
    }

    public RefreshMaterializedViewStmt(String name, boolean withData) {
        this.name = name;
        this.withData = withData;
    }

    public String name() { return name; }
    public boolean withData() { return withData; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RefreshMaterializedViewStmt that = (RefreshMaterializedViewStmt) o;
        return java.util.Objects.equals(name, that.name) && withData == that.withData;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, withData);
    }

    @Override
    public String toString() {
        return "RefreshMaterializedViewStmt[name=" + name + ", withData=" + withData + "]";
    }
}
