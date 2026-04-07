package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * expr [NOT] IN (values...) or expr [NOT] IN (subquery)
 */
public final class InExpr implements Expression {
    public final Expression expr;
    public final List<Expression> values;
    public final boolean negated;
    public final boolean fromAny;

    public InExpr(Expression expr, List<Expression> values, boolean negated, boolean fromAny) {
        this.expr = expr;
        this.values = values;
        this.negated = negated;
        this.fromAny = fromAny;
    }

    public InExpr(Expression expr, List<Expression> values, boolean negated) {
        this(expr, values, negated, false);
    }

    public Expression expr() { return expr; }
    public List<Expression> values() { return values; }
    public boolean negated() { return negated; }
    public boolean fromAny() { return fromAny; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InExpr that = (InExpr) o;
        return java.util.Objects.equals(expr, that.expr)
            && java.util.Objects.equals(values, that.values)
            && negated == that.negated
            && fromAny == that.fromAny;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(expr, values, negated, fromAny);
    }

    @Override
    public String toString() {
        return "InExpr[expr=" + expr + ", " + "values=" + values + ", " + "negated=" + negated + ", " + "fromAny=" + fromAny + "]";
    }
}
