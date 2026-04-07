package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * UNION, INTERSECT, or EXCEPT between two SELECT statements.
 * SELECT ... UNION [ALL] SELECT ...
 */
public final class SetOpStmt implements Statement {
    public final Statement left;
    public final SetOpType op;
    public final boolean all;
    public final Statement right;
    public final List<SelectStmt.OrderByItem> orderBy;
    public final Expression limit;
    public final Expression offset;

    public SetOpStmt(
            Statement left,
            SetOpType op,
            boolean all,
            Statement right,
            List<SelectStmt.OrderByItem> orderBy,
            Expression limit,
            Expression offset
    ) {
        this.left = left;
        this.op = op;
        this.all = all;
        this.right = right;
        this.orderBy = orderBy;
        this.limit = limit;
        this.offset = offset;
    }

    public enum SetOpType {
        UNION, INTERSECT, EXCEPT
    }

    public Statement left() { return left; }
    public SetOpType op() { return op; }
    public boolean all() { return all; }
    public Statement right() { return right; }
    public List<SelectStmt.OrderByItem> orderBy() { return orderBy; }
    public Expression limit() { return limit; }
    public Expression offset() { return offset; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SetOpStmt that = (SetOpStmt) o;
        return java.util.Objects.equals(left, that.left)
            && java.util.Objects.equals(op, that.op)
            && all == that.all
            && java.util.Objects.equals(right, that.right)
            && java.util.Objects.equals(orderBy, that.orderBy)
            && java.util.Objects.equals(limit, that.limit)
            && java.util.Objects.equals(offset, that.offset);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(left, op, all, right, orderBy, limit, offset);
    }

    @Override
    public String toString() {
        return "SetOpStmt[left=" + left + ", " + "op=" + op + ", " + "all=" + all + ", " + "right=" + right + ", " + "orderBy=" + orderBy + ", " + "limit=" + limit + ", " + "offset=" + offset + "]";
    }
}
