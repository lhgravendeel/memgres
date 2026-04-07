package com.memgres.engine.parser.ast;

/**
 * A column reference, optionally qualified: column, table.column, or schema.table.column
 */
public final class ColumnRef implements Expression {
    public final String schema;
    public final String table;
    public final String column;

    public ColumnRef(String schema, String table, String column) {
        this.schema = schema;
        this.table = table;
        this.column = column;
    }

    public ColumnRef(String column) {
        this(null, null, column);
    }

    public ColumnRef(String table, String column) {
        this(null, table, column);
    }

    public String schema() { return schema; }
    public String table() { return table; }
    public String column() { return column; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnRef that = (ColumnRef) o;
        return java.util.Objects.equals(schema, that.schema)
            && java.util.Objects.equals(table, that.table)
            && java.util.Objects.equals(column, that.column);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(schema, table, column);
    }

    @Override
    public String toString() {
        return "ColumnRef[schema=" + schema + ", " + "table=" + table + ", " + "column=" + column + "]";
    }
}
