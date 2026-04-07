package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * A function call: name(args...), optionally with DISTINCT, *, or ORDER BY (for aggregates).
 */
public final class FunctionCallExpr implements Expression {
    public final String name;
    public final List<Expression> args;
    public final boolean distinct;
    public final boolean star;
    public final List<SelectStmt.OrderByItem> orderBy;
    public final Expression filter;

    public FunctionCallExpr(
            String name,
            List<Expression> args,
            boolean distinct,
            boolean star,
            List<SelectStmt.OrderByItem> orderBy,
            Expression filter
    ) {
        this.name = name;
        this.args = args;
        this.distinct = distinct;
        this.star = star;
        this.orderBy = orderBy;
        this.filter = filter;
    }

    public FunctionCallExpr(String name, List<Expression> args) {
        this(name, args, false, false, null, null);
    }

    public FunctionCallExpr(String name, List<Expression> args, boolean distinct, boolean star) {
        this(name, args, distinct, star, null, null);
    }

    public FunctionCallExpr(String name, List<Expression> args, boolean distinct, boolean star,
                             List<SelectStmt.OrderByItem> orderBy) {
        this(name, args, distinct, star, orderBy, null);
    }

    public String name() { return name; }
    public List<Expression> args() { return args; }
    public boolean distinct() { return distinct; }
    public boolean star() { return star; }
    public List<SelectStmt.OrderByItem> orderBy() { return orderBy; }
    public Expression filter() { return filter; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FunctionCallExpr that = (FunctionCallExpr) o;
        return java.util.Objects.equals(name, that.name)
            && java.util.Objects.equals(args, that.args)
            && distinct == that.distinct
            && star == that.star
            && java.util.Objects.equals(orderBy, that.orderBy)
            && java.util.Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, args, distinct, star, orderBy, filter);
    }

    @Override
    public String toString() {
        return "FunctionCallExpr[name=" + name + ", " + "args=" + args + ", " + "distinct=" + distinct + ", " + "star=" + star + ", " + "orderBy=" + orderBy + ", " + "filter=" + filter + "]";
    }
}
