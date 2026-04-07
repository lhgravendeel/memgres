package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * CREATE [UNIQUE] INDEX [CONCURRENTLY] [IF NOT EXISTS] name ON [schema.]table [USING method] (columns...) [INCLUDE (cols)] [WHERE cond]
 */
public final class CreateIndexStmt implements Statement {
    public final String name;
    public final String schema;
    public final String table;
    public final List<String> columns;
    public final boolean unique;
    public final boolean ifNotExists;
    public final boolean concurrently;
    public final String method;
    public final List<String> includeColumns;
    public final String whereClause;

    public CreateIndexStmt(
            String name,
            String schema,
            String table,
            List<String> columns,
            boolean unique,
            boolean ifNotExists,
            boolean concurrently,
            String method,
            List<String> includeColumns,
            String whereClause
    ) {
        this.name = name;
        this.schema = schema;
        this.table = table;
        this.columns = columns;
        this.unique = unique;
        this.ifNotExists = ifNotExists;
        this.concurrently = concurrently;
        this.method = method;
        this.includeColumns = includeColumns;
        this.whereClause = whereClause;
    }

    public CreateIndexStmt(String name, String table, List<String> columns,
                           boolean unique, boolean ifNotExists, boolean concurrently) {
        this(name, null, table, columns, unique, ifNotExists, concurrently, null, null, null);
    }

    /** Constructor without schema (backwards compatible). */
    public CreateIndexStmt(String name, String table, List<String> columns,
                           boolean unique, boolean ifNotExists, boolean concurrently,
                           String method, List<String> includeColumns, String whereClause) {
        this(name, null, table, columns, unique, ifNotExists, concurrently, method, includeColumns, whereClause);
    }

    public String name() { return name; }
    public String schema() { return schema; }
    public String table() { return table; }
    public List<String> columns() { return columns; }
    public boolean unique() { return unique; }
    public boolean ifNotExists() { return ifNotExists; }
    public boolean concurrently() { return concurrently; }
    public String method() { return method; }
    public List<String> includeColumns() { return includeColumns; }
    public String whereClause() { return whereClause; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateIndexStmt that = (CreateIndexStmt) o;
        return java.util.Objects.equals(name, that.name)
            && java.util.Objects.equals(schema, that.schema)
            && java.util.Objects.equals(table, that.table)
            && java.util.Objects.equals(columns, that.columns)
            && unique == that.unique
            && ifNotExists == that.ifNotExists
            && concurrently == that.concurrently
            && java.util.Objects.equals(method, that.method)
            && java.util.Objects.equals(includeColumns, that.includeColumns)
            && java.util.Objects.equals(whereClause, that.whereClause);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, schema, table, columns, unique, ifNotExists, concurrently, method, includeColumns, whereClause);
    }

    @Override
    public String toString() {
        return "CreateIndexStmt[name=" + name + ", " + "schema=" + schema + ", " + "table=" + table + ", " + "columns=" + columns + ", " + "unique=" + unique + ", " + "ifNotExists=" + ifNotExists + ", " + "concurrently=" + concurrently + ", " + "method=" + method + ", " + "includeColumns=" + includeColumns + ", " + "whereClause=" + whereClause + "]";
    }
}
