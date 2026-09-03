package com.memgres.engine.parser.ast;

/**
 * Represents {@code expr op ANY (subquery)} or {@code expr op ALL (subquery)}.
 */
public final class AnyAllExpr implements Expression {
    public final Expression left;
    public final BinaryExpr.BinOp op;
    public final Statement subquery;
    public final boolean isAll;
    /** The operator as written, where it is one the reader declared rather than a built-in. */
    public final String userOp;

    public AnyAllExpr(Expression left, BinaryExpr.BinOp op, Statement subquery, boolean isAll) {
        this(left, op, subquery, isAll, null);
    }

    public AnyAllExpr(Expression left, BinaryExpr.BinOp op, Statement subquery, boolean isAll,
                      String userOp) {
        this.left = left;
        this.op = op;
        this.subquery = subquery;
        this.isAll = isAll;
        this.userOp = userOp;
    }

    public String userOp() { return userOp; }

    public Expression left() { return left; }
    public BinaryExpr.BinOp op() { return op; }
    public Statement subquery() { return subquery; }
    public boolean isAll() { return isAll; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnyAllExpr that = (AnyAllExpr) o;
        return java.util.Objects.equals(left, that.left)
            && java.util.Objects.equals(op, that.op)
            && java.util.Objects.equals(subquery, that.subquery)
            && isAll == that.isAll
            && java.util.Objects.equals(userOp, that.userOp);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(left, op, subquery, isAll, userOp);
    }

    @Override
    public String toString() {
        return "AnyAllExpr[left=" + left + ", " + "op=" + op + ", " + "subquery=" + subquery + ", " + "isAll=" + isAll + "]";
    }
}
