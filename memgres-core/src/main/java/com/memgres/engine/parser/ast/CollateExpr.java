package com.memgres.engine.parser.ast;

/**
 * A COLLATE expression: expr COLLATE collation_name.
 * Preserves the collation for functions that need to behave differently by collation (e.g. upper()).
 */
public final class CollateExpr implements Expression {
    public final Expression expr;
    public final String collation;

    public CollateExpr(Expression expr, String collation) {
        this.expr = expr;
        this.collation = collation;
    }

    public Expression expr() { return expr; }
    public String collation() { return collation; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CollateExpr that = (CollateExpr) o;
        return java.util.Objects.equals(expr, that.expr)
            && java.util.Objects.equals(collation, that.collation);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(expr, collation);
    }

    @Override
    public String toString() {
        return "CollateExpr[expr=" + expr + ", " + "collation=" + collation + "]";
    }
}
