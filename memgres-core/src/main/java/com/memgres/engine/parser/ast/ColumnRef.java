package com.memgres.engine.parser.ast;

/**
 * A column reference, optionally qualified: column, table.column, schema.table.column, or
 * catalog.schema.table.column.
 *
 * <p>The catalog part exists because SQL allows it and PostgreSQL accepts it when it names the
 * database being queried -- and refuses it, as a cross-database reference, when it does not.
 * Either way it has to parse.
 */
public final class ColumnRef implements Expression {
    public final String catalog;
    // schema/table are mutable for the same reason SelectStmt.TableRef's are: ALTER TABLE ...
    // RENAME TO has to retarget the stored view ASTs that already name this relation, and a
    // SELECT * is expanded at creation into references qualified by the relation's own name.
    public String schema;
    public String table;
    public final String column;

    public ColumnRef(String catalog, String schema, String table, String column) {
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
        this.column = column;
    }

    public ColumnRef(String schema, String table, String column) {
        this(null, schema, table, column);
    }

    public ColumnRef(String column) {
        this(null, null, column);
    }

    /** Retarget this reference at the same relation under its new schema and name. */
    public void retarget(String newSchema, String newTable) {
        this.schema = newSchema;
        this.table = newTable;
    }

    public ColumnRef(String table, String column) {
        this(null, table, column);
    }

    public String catalog() { return catalog; }
    public String schema() { return schema; }
    public String table() { return table; }
    public String column() { return column; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnRef that = (ColumnRef) o;
        return java.util.Objects.equals(catalog, that.catalog)
            && java.util.Objects.equals(schema, that.schema)
            && java.util.Objects.equals(table, that.table)
            && java.util.Objects.equals(column, that.column);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(catalog, schema, table, column);
    }

    @Override
    public String toString() {
        return "ColumnRef[catalog=" + catalog + ", " + "schema=" + schema + ", " + "table=" + table + ", " + "column=" + column + "]";
    }
}
