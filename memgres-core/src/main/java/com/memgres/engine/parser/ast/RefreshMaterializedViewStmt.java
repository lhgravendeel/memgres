package com.memgres.engine.parser.ast;

public final class RefreshMaterializedViewStmt implements Statement {
    public final String name;

    public RefreshMaterializedViewStmt(String name) {
        this.name = name;
    }

    public String name() { return name; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RefreshMaterializedViewStmt that = (RefreshMaterializedViewStmt) o;
        return java.util.Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name);
    }

    @Override
    public String toString() {
        return "RefreshMaterializedViewStmt[name=" + name + "]";
    }
}
