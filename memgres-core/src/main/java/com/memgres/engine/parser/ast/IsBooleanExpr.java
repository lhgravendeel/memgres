package com.memgres.engine.parser.ast;

public final class IsBooleanExpr implements Expression {
    public final Expression expr;
    public final BooleanTest test;

    public IsBooleanExpr(Expression expr, BooleanTest test) {
        this.expr = expr;
        this.test = test;
    }

    public enum BooleanTest { IS_TRUE, IS_NOT_TRUE, IS_FALSE, IS_NOT_FALSE, IS_UNKNOWN, IS_NOT_UNKNOWN, IS_DOCUMENT, IS_NOT_DOCUMENT }

    public Expression expr() { return expr; }
    public BooleanTest test() { return test; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IsBooleanExpr that = (IsBooleanExpr) o;
        return java.util.Objects.equals(expr, that.expr)
            && java.util.Objects.equals(test, that.test);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(expr, test);
    }

    @Override
    public String toString() {
        return "IsBooleanExpr[expr=" + expr + ", " + "test=" + test + "]";
    }
}
