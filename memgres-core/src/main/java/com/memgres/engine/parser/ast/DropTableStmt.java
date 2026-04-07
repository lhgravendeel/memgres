package com.memgres.engine.parser.ast;

import com.memgres.engine.util.Cols;

import java.util.List;

/**
 * DROP TABLE [IF EXISTS] [schema.]name [, ...] [CASCADE|RESTRICT]
 */
public final class DropTableStmt implements Statement {
    public final String schema;
    public final String name;
    public final boolean ifExists;
    public final boolean cascade;
    public final List<String> additionalTables;

    public DropTableStmt(
            String schema,
            String name,
            boolean ifExists,
            boolean cascade,
            List<String> additionalTables
    ) {
        this.schema = schema;
        this.name = name;
        this.ifExists = ifExists;
        this.cascade = cascade;
        this.additionalTables = additionalTables;
    }

    public DropTableStmt(String schema, String name, boolean ifExists, boolean cascade) {
        this(schema, name, ifExists, cascade, Cols.listOf());
    }

    public String schema() { return schema; }
    public String name() { return name; }
    public boolean ifExists() { return ifExists; }
    public boolean cascade() { return cascade; }
    public List<String> additionalTables() { return additionalTables; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DropTableStmt that = (DropTableStmt) o;
        return java.util.Objects.equals(schema, that.schema)
            && java.util.Objects.equals(name, that.name)
            && ifExists == that.ifExists
            && cascade == that.cascade
            && java.util.Objects.equals(additionalTables, that.additionalTables);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(schema, name, ifExists, cascade, additionalTables);
    }

    @Override
    public String toString() {
        return "DropTableStmt[schema=" + schema + ", " + "name=" + name + ", " + "ifExists=" + ifExists + ", " + "cascade=" + cascade + ", " + "additionalTables=" + additionalTables + "]";
    }
}
