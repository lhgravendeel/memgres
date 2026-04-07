package com.memgres.engine.parser.ast;

/**
 * expr IS [NOT] NULL
 */
public final class IsNullExpr implements Expression {
    public final Expression expr;
    public final boolean negated;

    public IsNullExpr(Expression expr, boolean negated) {
        this.expr = expr;
        this.negated = negated;
    }

    public Expression expr() { return expr; }
    public boolean negated() { return negated; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IsNullExpr that = (IsNullExpr) o;
        return java.util.Objects.equals(expr, that.expr)
            && negated == that.negated;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(expr, negated);
    }

    @Override
    public String toString() {
        return "IsNullExpr[expr=" + expr + ", " + "negated=" + negated + "]";
    }
}
