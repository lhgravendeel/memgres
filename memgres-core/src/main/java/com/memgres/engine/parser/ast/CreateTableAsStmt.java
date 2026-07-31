package com.memgres.engine.parser.ast;

import java.util.Objects;

/**
 * CREATE TABLE name AS SELECT ... [WITH DATA | WITH NO DATA]
 */
public final class CreateTableAsStmt implements Statement {
    public final String schema;
    public final String name;
    public final boolean ifNotExists;
    public final boolean temporary;
    public final Statement query;
    public final boolean withData;
    /** The names written as CREATE TABLE t (a, b) AS ..., or null when none were. */
    public final java.util.List<String> columnNames;

    public CreateTableAsStmt(String schema, String name, boolean ifNotExists, boolean temporary,
                             Statement query, boolean withData) {
        this(schema, name, ifNotExists, temporary, query, withData, null);
    }

    public CreateTableAsStmt(String schema, String name, boolean ifNotExists, boolean temporary,
                             Statement query, boolean withData,
                             java.util.List<String> columnNames) {
        this.schema = schema;
        this.name = name;
        this.ifNotExists = ifNotExists;
        this.temporary = temporary;
        this.query = query;
        this.withData = withData;
        this.columnNames = columnNames;
    }

    public java.util.List<String> columnNames() { return columnNames; }

    public String schema() { return schema; }
    public String name() { return name; }
    public boolean ifNotExists() { return ifNotExists; }
    public boolean temporary() { return temporary; }
    public Statement query() { return query; }
    public boolean withData() { return withData; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateTableAsStmt that = (CreateTableAsStmt) o;
        return Objects.equals(schema, that.schema)
            && Objects.equals(name, that.name)
            && ifNotExists == that.ifNotExists
            && temporary == that.temporary
            && Objects.equals(query, that.query)
            && withData == that.withData;
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, name, ifNotExists, temporary, query, withData);
    }

    @Override
    public String toString() {
        return "CreateTableAsStmt[schema=" + schema + ", name=" + name + ", ifNotExists=" + ifNotExists
            + ", temporary=" + temporary + ", query=" + query + ", withData=" + withData + "]";
    }
}
