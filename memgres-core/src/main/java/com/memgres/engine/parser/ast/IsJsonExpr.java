package com.memgres.engine.parser.ast;

/**
 * IS [NOT] JSON [OBJECT | ARRAY | SCALAR] [WITH UNIQUE KEYS]
 */
public final class IsJsonExpr implements Expression {
    public final Expression expr;
    public final boolean negated;
    public final JsonType jsonType; // null = any JSON
    public final boolean uniqueKeys;

    public IsJsonExpr(Expression expr, boolean negated, JsonType jsonType, boolean uniqueKeys) {
        this.expr = expr;
        this.negated = negated;
        this.jsonType = jsonType;
        this.uniqueKeys = uniqueKeys;
    }

    public enum JsonType { VALUE, OBJECT, ARRAY, SCALAR }

    public Expression expr() { return expr; }
    public boolean negated() { return negated; }
    public JsonType jsonType() { return jsonType; }
    public boolean uniqueKeys() { return uniqueKeys; }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IsJsonExpr that = (IsJsonExpr) o;
        return negated == that.negated && uniqueKeys == that.uniqueKeys
                && java.util.Objects.equals(expr, that.expr) && jsonType == that.jsonType;
    }
    @Override public int hashCode() { return java.util.Objects.hash(expr, negated, jsonType, uniqueKeys); }
    @Override public String toString() { return "IsJsonExpr[expr=" + expr + ", negated=" + negated + ", jsonType=" + jsonType + ", uniqueKeys=" + uniqueKeys + "]"; }
}
