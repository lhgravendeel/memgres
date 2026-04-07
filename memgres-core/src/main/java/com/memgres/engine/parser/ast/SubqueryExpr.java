package com.memgres.engine.parser.ast;

/**
 * A scalar subquery used as an expression: (SELECT ... [UNION/INTERSECT/EXCEPT ...])
 */
public final class SubqueryExpr implements Expression {
    public final Statement subquery;

    public SubqueryExpr(Statement subquery) {
        this.subquery = subquery;
    }

    public Statement subquery() { return subquery; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubqueryExpr that = (SubqueryExpr) o;
        return java.util.Objects.equals(subquery, that.subquery);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(subquery);
    }

    @Override
    public String toString() {
        return "SubqueryExpr[subquery=" + subquery + "]";
    }
}
