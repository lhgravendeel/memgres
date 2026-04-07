package com.memgres.engine.parser.ast;

/**
 * Represents a composite field access: (expr).field_name
 * Used by PG JDBC metadata queries like (information_schema._pg_expandarray(i.indkey)).n
 */
public final class FieldAccessExpr implements Expression {
    public final Expression expr;
    public final String field;

    public FieldAccessExpr(Expression expr, String field) {
        this.expr = expr;
        this.field = field;
    }

    public Expression expr() { return expr; }
    public String field() { return field; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldAccessExpr that = (FieldAccessExpr) o;
        return java.util.Objects.equals(expr, that.expr)
            && java.util.Objects.equals(field, that.field);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(expr, field);
    }

    @Override
    public String toString() {
        return "FieldAccessExpr[expr=" + expr + ", " + "field=" + field + "]";
    }
}
