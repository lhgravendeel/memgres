package com.memgres.engine.parser.ast;

/**
 * expr op ANY(array_expr) or expr op ALL(array_expr).
 * Unlike AnyAllExpr which takes a subquery, this takes an array expression directly.
 */
public final class AnyAllArrayExpr implements Expression {
    public final Expression left;
    public final BinaryExpr.BinOp op;
    public final Expression array;
    public final boolean isAll;

    public AnyAllArrayExpr(Expression left, BinaryExpr.BinOp op, Expression array, boolean isAll) {
        this.left = left;
        this.op = op;
        this.array = array;
        this.isAll = isAll;
    }

    public Expression left() { return left; }
    public BinaryExpr.BinOp op() { return op; }
    public Expression array() { return array; }
    public boolean isAll() { return isAll; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnyAllArrayExpr that = (AnyAllArrayExpr) o;
        return java.util.Objects.equals(left, that.left)
            && java.util.Objects.equals(op, that.op)
            && java.util.Objects.equals(array, that.array)
            && isAll == that.isAll;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(left, op, array, isAll);
    }

    @Override
    public String toString() {
        return "AnyAllArrayExpr[left=" + left + ", " + "op=" + op + ", " + "array=" + array + ", " + "isAll=" + isAll + "]";
    }
}
