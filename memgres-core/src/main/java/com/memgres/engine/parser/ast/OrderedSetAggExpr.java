package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * Ordered-set aggregate expression: funcname(args) WITHIN GROUP (ORDER BY ...)
 * Examples: percentile_disc(0.5) WITHIN GROUP (ORDER BY val)
 *           mode() WITHIN GROUP (ORDER BY val)
 *           rank(10) WITHIN GROUP (ORDER BY val)
 */
public final class OrderedSetAggExpr implements Expression {
    public final String funcName;
    public final List<Expression> args;
    public final List<SelectStmt.OrderByItem> withinGroupOrderBy;

    public OrderedSetAggExpr(String funcName, List<Expression> args, List<SelectStmt.OrderByItem> withinGroupOrderBy) {
        this.funcName = funcName;
        this.args = args;
        this.withinGroupOrderBy = withinGroupOrderBy;
    }

    public String funcName() { return funcName; }
    public List<Expression> args() { return args; }
    public List<SelectStmt.OrderByItem> withinGroupOrderBy() { return withinGroupOrderBy; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderedSetAggExpr that = (OrderedSetAggExpr) o;
        return java.util.Objects.equals(funcName, that.funcName)
            && java.util.Objects.equals(args, that.args)
            && java.util.Objects.equals(withinGroupOrderBy, that.withinGroupOrderBy);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(funcName, args, withinGroupOrderBy);
    }

    @Override
    public String toString() {
        return "OrderedSetAggExpr[funcName=" + funcName + ", " + "args=" + args + ", " + "withinGroupOrderBy=" + withinGroupOrderBy + "]";
    }
}
