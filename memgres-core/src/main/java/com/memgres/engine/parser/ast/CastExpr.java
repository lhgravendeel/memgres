package com.memgres.engine.parser.ast;

/**
 * A type cast: expr::type or CAST(expr AS type).
 */
public final class CastExpr implements Expression {
    public final Expression expr;
    public final String typeName;

    public CastExpr(Expression expr, String typeName) {
        this.expr = expr;
        this.typeName = typeName;
    }

    public Expression expr() { return expr; }
    public String typeName() { return typeName; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CastExpr that = (CastExpr) o;
        return java.util.Objects.equals(expr, that.expr)
            && java.util.Objects.equals(typeName, that.typeName);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(expr, typeName);
    }

    @Override
    public String toString() {
        return "CastExpr[expr=" + expr + ", " + "typeName=" + typeName + "]";
    }
}
