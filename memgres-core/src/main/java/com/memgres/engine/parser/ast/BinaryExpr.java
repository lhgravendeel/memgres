package com.memgres.engine.parser.ast;

/**
 * A binary expression: left OP right.
 */
public final class BinaryExpr implements Expression {
    public final Expression left;
    public final BinOp op;
    public final Expression right;

    public BinaryExpr(Expression left, BinOp op, Expression right) {
        this.left = left;
        this.op = op;
        this.right = right;
    }

    public enum BinOp {
        // Arithmetic
        ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULO, POWER,
        // Bitwise
        BIT_AND, BIT_OR, BIT_XOR, SHIFT_LEFT, SHIFT_RIGHT,
        // Comparison
        EQUAL, NOT_EQUAL, LESS_THAN, GREATER_THAN, LESS_EQUAL, GREATER_EQUAL,
        // Logical
        AND, OR,
        // String
        CONCAT, LIKE, ILIKE, SIMILAR_TO,
        // JSON
        JSON_ARROW, JSON_ARROW_TEXT, JSON_HASH_ARROW, JSON_HASH_ARROW_TEXT,
        // Array
        CONTAINS, CONTAINED_BY, OVERLAP,
        // Full-text search
        TS_MATCH,
        // JSONB key existence
        JSONB_EXISTS, JSONB_EXISTS_ANY, JSONB_EXISTS_ALL,
        // JSONB path exists (@?)
        JSONB_PATH_EXISTS_OP,
        // JSONB path deletion
        JSON_DELETE_PATH,
        // Inet containment
        INET_CONTAINS_EQUALS, INET_CONTAINED_BY_EQUALS,
        // Regex match
        REGEX_MATCH, REGEX_IMATCH, NOT_REGEX_MATCH, NOT_REGEX_IMATCH,
        // Geometric
        DISTANCE, APPROX_EQUAL, GEO_BELOW, GEO_ABOVE,
        GEO_NOT_EXTEND_RIGHT, GEO_NOT_EXTEND_LEFT, GEO_NOT_EXTEND_ABOVE, GEO_NOT_EXTEND_BELOW,
        GEO_INTERSECTS, GEO_CLOSEST_POINT, GEO_PARALLEL, GEO_PERPENDICULAR,
        // Distinct comparison (NULL-safe)
        IS_DISTINCT_FROM, IS_NOT_DISTINCT_FROM,
        // Range
        RANGE_ADJACENT
    }

    public Expression left() { return left; }
    public BinOp op() { return op; }
    public Expression right() { return right; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BinaryExpr that = (BinaryExpr) o;
        return java.util.Objects.equals(left, that.left)
            && java.util.Objects.equals(op, that.op)
            && java.util.Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(left, op, right);
    }

    @Override
    public String toString() {
        return "BinaryExpr[left=" + left + ", " + "op=" + op + ", " + "right=" + right + "]";
    }
}
