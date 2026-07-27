package com.memgres.engine.parser.ast;

/**
 * REFRESH MATERIALIZED VIEW [CONCURRENTLY] name [WITH [NO] DATA]
 */
public final class RefreshMaterializedViewStmt implements Statement {
    public final String name;
    /** False for REFRESH ... WITH NO DATA (leaves the view unpopulated). */
    public final boolean withData;
    /** True for REFRESH ... CONCURRENTLY, which needs a unique index to match rows up. */
    public final boolean concurrently;

    public RefreshMaterializedViewStmt(String name) {
        this(name, true, false);
    }

    public RefreshMaterializedViewStmt(String name, boolean withData) {
        this(name, withData, false);
    }

    public RefreshMaterializedViewStmt(String name, boolean withData, boolean concurrently) {
        this.name = name;
        this.withData = withData;
        this.concurrently = concurrently;
    }

    public String name() { return name; }
    public boolean withData() { return withData; }
    public boolean concurrently() { return concurrently; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RefreshMaterializedViewStmt that = (RefreshMaterializedViewStmt) o;
        return java.util.Objects.equals(name, that.name) && withData == that.withData
                && concurrently == that.concurrently;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, withData, concurrently);
    }

    @Override
    public String toString() {
        return "RefreshMaterializedViewStmt[name=" + name + ", withData=" + withData
                + ", concurrently=" + concurrently + "]";
    }
}
