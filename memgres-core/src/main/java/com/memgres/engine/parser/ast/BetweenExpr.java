package com.memgres.engine.parser.ast;

/**
 * expr [NOT] BETWEEN [SYMMETRIC] low AND high
 */
public final class BetweenExpr implements Expression {
    public final Expression expr;
    public final Expression low;
    public final Expression high;
    public final boolean negated;
    public final boolean symmetric;

    public BetweenExpr(
            Expression expr,
            Expression low,
            Expression high,
            boolean negated,
            boolean symmetric
    ) {
        this.expr = expr;
        this.low = low;
        this.high = high;
        this.negated = negated;
        this.symmetric = symmetric;
    }

    public BetweenExpr(Expression expr, Expression low, Expression high, boolean negated) {
        this(expr, low, high, negated, false);
    }

    public Expression expr() { return expr; }
    public Expression low() { return low; }
    public Expression high() { return high; }
    public boolean negated() { return negated; }
    public boolean symmetric() { return symmetric; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BetweenExpr that = (BetweenExpr) o;
        return java.util.Objects.equals(expr, that.expr)
            && java.util.Objects.equals(low, that.low)
            && java.util.Objects.equals(high, that.high)
            && negated == that.negated
            && symmetric == that.symmetric;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(expr, low, high, negated, symmetric);
    }

    @Override
    public String toString() {
        return "BetweenExpr[expr=" + expr + ", " + "low=" + low + ", " + "high=" + high + ", " + "negated=" + negated + ", " + "symmetric=" + symmetric + "]";
    }
}
