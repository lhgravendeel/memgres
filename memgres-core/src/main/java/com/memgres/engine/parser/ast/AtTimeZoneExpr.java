package com.memgres.engine.parser.ast;

/**
 * Represents the SQL expression: expr AT TIME ZONE zone
 */
public final class AtTimeZoneExpr implements Expression {
    public final Expression expr;
    public final Expression zone;

    public AtTimeZoneExpr(Expression expr, Expression zone) {
        this.expr = expr;
        this.zone = zone;
    }

    public Expression expr() { return expr; }
    public Expression zone() { return zone; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AtTimeZoneExpr that = (AtTimeZoneExpr) o;
        return java.util.Objects.equals(expr, that.expr)
            && java.util.Objects.equals(zone, that.zone);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(expr, zone);
    }

    @Override
    public String toString() {
        return "AtTimeZoneExpr[expr=" + expr + ", " + "zone=" + zone + "]";
    }
}
