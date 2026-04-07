package com.memgres.engine.parser.ast;

/**
 * Composite star expansion: (expr).* expands a composite value into individual columns.
 */
public final class CompositeStarExpr implements Expression {
    public final Expression expr;

    public CompositeStarExpr(Expression expr) {
        this.expr = expr;
    }

    public Expression expr() { return expr; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompositeStarExpr that = (CompositeStarExpr) o;
        return java.util.Objects.equals(expr, that.expr);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(expr);
    }

    @Override
    public String toString() {
        return "CompositeStarExpr[expr=" + expr + "]";
    }
}
