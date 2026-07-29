package com.memgres.engine.parser.ast;

/**
 * The * wildcard in SELECT *, table.* in SELECT t.*, or schema.table.* in SELECT s.t.*
 *
 * <p>The schema is kept because it is what picks between two FROM entries of one name:
 * {@code SELECT s2.t.* FROM s1.t, s2.t} stands for the columns of s2's t and no others.
 * Dropping it expanded both, and let a star qualified by a schema that does not hold the
 * relation expand it anyway.
 */
public final class WildcardExpr implements Expression {
    public final String catalog;
    public final String schema;
    public final String table;

    public WildcardExpr(String catalog, String schema, String table) {
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
    }

    public WildcardExpr(String schema, String table) {
        this(null, schema, table);
    }

    public WildcardExpr(String table) {
        this(null, table);
    }

    public WildcardExpr() {
        this(null, null);
    }

    public String catalog() { return catalog; }
    public String schema() { return schema; }
    public String table() { return table; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WildcardExpr that = (WildcardExpr) o;
        return java.util.Objects.equals(catalog, that.catalog)
            && java.util.Objects.equals(schema, that.schema)
            && java.util.Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(catalog, schema, table);
    }

    @Override
    public String toString() {
        return "WildcardExpr[catalog=" + catalog + ", " + "schema=" + schema + ", table=" + table + "]";
    }
}
