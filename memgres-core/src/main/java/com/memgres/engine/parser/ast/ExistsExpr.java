package com.memgres.engine.parser.ast;

/**
 * EXISTS (SELECT ... [UNION/INTERSECT/EXCEPT ...])
 */
public final class ExistsExpr implements Expression {
    public final Statement subquery;

    public ExistsExpr(Statement subquery) {
        this.subquery = subquery;
    }

    public Statement subquery() { return subquery; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExistsExpr that = (ExistsExpr) o;
        return java.util.Objects.equals(subquery, that.subquery);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(subquery);
    }

    @Override
    public String toString() {
        return "ExistsExpr[subquery=" + subquery + "]";
    }
}
