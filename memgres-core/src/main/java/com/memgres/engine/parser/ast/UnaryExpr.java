package com.memgres.engine.parser.ast;

/**
 * A unary expression: NOT expr, -expr, etc.
 */
public final class UnaryExpr implements Expression {
    public final UnaryOp op;
    public final Expression operand;

    public UnaryExpr(UnaryOp op, Expression operand) {
        this.op = op;
        this.operand = operand;
    }

    public enum UnaryOp {
        NOT, NEGATE, POSITIVE, BIT_NOT, ABS, SQRT, CBRT, GEO_IS_HORIZONTAL, GEO_IS_VERTICAL
    }

    public UnaryOp op() { return op; }
    public Expression operand() { return operand; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UnaryExpr that = (UnaryExpr) o;
        return java.util.Objects.equals(op, that.op)
            && java.util.Objects.equals(operand, that.operand);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(op, operand);
    }

    @Override
    public String toString() {
        return "UnaryExpr[op=" + op + ", " + "operand=" + operand + "]";
    }
}
