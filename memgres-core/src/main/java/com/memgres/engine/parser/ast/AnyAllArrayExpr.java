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
    /** The operator as written, where it is one the reader declared rather than a built-in. */
    public final String userOp;

    public AnyAllArrayExpr(Expression left, BinaryExpr.BinOp op, Expression array, boolean isAll) {
        this(left, op, array, isAll, null);
    }

    public AnyAllArrayExpr(Expression left, BinaryExpr.BinOp op, Expression array, boolean isAll,
                           String userOp) {
        this.left = left;
        this.op = op;
        this.array = array;
        this.isAll = isAll;
        this.userOp = userOp;
    }

    public String userOp() { return userOp; }

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
            && isAll == that.isAll
            && java.util.Objects.equals(userOp, that.userOp);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(left, op, array, isAll, userOp);
    }

    @Override
    public String toString() {
        return "AnyAllArrayExpr[left=" + left + ", " + "op=" + op + ", " + "array=" + array + ", " + "isAll=" + isAll + "]";
    }
}
