package com.memgres.engine.parser.ast;

/**
 * ARRAY(SELECT ... [UNION/INTERSECT/EXCEPT ...]): collects the first column into an array.
 */
public final class ArraySubqueryExpr implements Expression {
    public final Statement subquery;

    public ArraySubqueryExpr(Statement subquery) {
        this.subquery = subquery;
    }

    public Statement subquery() { return subquery; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ArraySubqueryExpr that = (ArraySubqueryExpr) o;
        return java.util.Objects.equals(subquery, that.subquery);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(subquery);
    }

    @Override
    public String toString() {
        return "ArraySubqueryExpr[subquery=" + subquery + "]";
    }
}
