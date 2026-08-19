package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * Ordered-set aggregate expression: funcname(args) WITHIN GROUP (ORDER BY ...) [FILTER (WHERE ...)]
 * Examples: percentile_disc(0.5) WITHIN GROUP (ORDER BY val)
 *           mode() WITHIN GROUP (ORDER BY val)
 *           rank(10) WITHIN GROUP (ORDER BY val)
 */
public final class OrderedSetAggExpr implements Expression {
    public final String funcName;
    public final List<Expression> args;
    public final List<SelectStmt.OrderByItem> withinGroupOrderBy;
    /** The FILTER predicate, which the grammar writes after WITHIN GROUP; null where none is. */
    public final Expression filter;

    public OrderedSetAggExpr(String funcName, List<Expression> args, List<SelectStmt.OrderByItem> withinGroupOrderBy) {
        this(funcName, args, withinGroupOrderBy, null);
    }

    public OrderedSetAggExpr(String funcName, List<Expression> args,
                             List<SelectStmt.OrderByItem> withinGroupOrderBy, Expression filter) {
        this.funcName = funcName;
        this.args = args;
        this.withinGroupOrderBy = withinGroupOrderBy;
        this.filter = filter;
    }

    public String funcName() { return funcName; }
    public List<Expression> args() { return args; }
    public List<SelectStmt.OrderByItem> withinGroupOrderBy() { return withinGroupOrderBy; }
    public Expression filter() { return filter; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderedSetAggExpr that = (OrderedSetAggExpr) o;
        return java.util.Objects.equals(funcName, that.funcName)
            && java.util.Objects.equals(args, that.args)
            && java.util.Objects.equals(withinGroupOrderBy, that.withinGroupOrderBy)
            && java.util.Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(funcName, args, withinGroupOrderBy, filter);
    }

    @Override
    public String toString() {
        return "OrderedSetAggExpr[funcName=" + funcName + ", " + "args=" + args + ", "
                + "withinGroupOrderBy=" + withinGroupOrderBy + ", filter=" + filter + "]";
    }
}
