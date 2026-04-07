package com.memgres.engine.parser.ast;

import java.util.Objects;

/**
 * Array slice expression: arr[lower:upper]
 */
public final class ArraySliceExpr implements Expression {
    public final Expression array;
    public final Expression lower;
    public final Expression upper;

    public ArraySliceExpr(Expression array, Expression lower, Expression upper) {
        this.array = array;
        this.lower = lower;
        this.upper = upper;
    }

    public Expression array() { return array; }
    public Expression lower() { return lower; }
    public Expression upper() { return upper; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ArraySliceExpr that = (ArraySliceExpr) o;
        return Objects.equals(array, that.array)
            && Objects.equals(lower, that.lower)
            && Objects.equals(upper, that.upper);
    }

    @Override
    public int hashCode() {
        return Objects.hash(array, lower, upper);
    }

    @Override
    public String toString() {
        return "ArraySliceExpr[array=" + array + ", lower=" + lower + ", upper=" + upper + "]";
    }
}
