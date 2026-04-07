package com.memgres.engine.parser.ast;

/**
 * expr [NOT] LIKE pattern [ESCAPE 'char']
 * Also handles ILIKE.
 */
public final class LikeExpr implements Expression {
    public final Expression left;
    public final Expression pattern;
    public final String escape;
    public final boolean caseInsensitive;
    public final boolean negated;

    public LikeExpr(
            Expression left,
            Expression pattern,
            String escape,
            boolean caseInsensitive,
            boolean negated
    ) {
        this.left = left;
        this.pattern = pattern;
        this.escape = escape;
        this.caseInsensitive = caseInsensitive;
        this.negated = negated;
    }

    public Expression left() { return left; }
    public Expression pattern() { return pattern; }
    public String escape() { return escape; }
    public boolean caseInsensitive() { return caseInsensitive; }
    public boolean negated() { return negated; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LikeExpr that = (LikeExpr) o;
        return java.util.Objects.equals(left, that.left)
            && java.util.Objects.equals(pattern, that.pattern)
            && java.util.Objects.equals(escape, that.escape)
            && caseInsensitive == that.caseInsensitive
            && negated == that.negated;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(left, pattern, escape, caseInsensitive, negated);
    }

    @Override
    public String toString() {
        return "LikeExpr[left=" + left + ", " + "pattern=" + pattern + ", " + "escape=" + escape + ", " + "caseInsensitive=" + caseInsensitive + ", " + "negated=" + negated + "]";
    }
}
