package com.memgres.engine.parser.ast;

/**
 * Represents a user-defined operator expression, either binary (left op right) or unary prefix (op right).
 * Used for operators created via CREATE OPERATOR that don't map to built-in BinaryExpr.BinOp values.
 * Can originate from:
 *   - Infix qualified syntax:  expr OPERATOR(schema.op) expr
 *   - Prefix qualified syntax: OPERATOR(schema.op)(arg)
 *   - Infix custom token:      expr +++ expr  (multi-char custom operator)
 */
public final class CustomOperatorExpr implements Expression {
    private final String schema;      // may be null (resolved via search_path)
    private final String opSymbol;    // operator symbol, e.g. "+++", "<=>"
    private final Expression left;    // null for unary prefix
    private final Expression right;

    public CustomOperatorExpr(String schema, String opSymbol, Expression left, Expression right) {
        this.schema = schema;
        this.opSymbol = opSymbol;
        this.left = left;
        this.right = right;
    }

    public String schema() { return schema; }
    public String opSymbol() { return opSymbol; }
    public Expression left() { return left; }
    public Expression right() { return right; }

    /** True if this is a unary prefix operator (left is null). */
    public boolean isUnary() { return left == null; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomOperatorExpr that = (CustomOperatorExpr) o;
        return java.util.Objects.equals(schema, that.schema)
            && java.util.Objects.equals(opSymbol, that.opSymbol)
            && java.util.Objects.equals(left, that.left)
            && java.util.Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(schema, opSymbol, left, right);
    }

    @Override
    public String toString() {
        return "CustomOperatorExpr[schema=" + schema + ", op=" + opSymbol
            + ", left=" + left + ", right=" + right + "]";
    }
}
