package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * ARRAY[expr, expr, ...] or '{1,2,3}' or ROW(expr, expr, ...)
 */
public final class ArrayExpr implements Expression {
    public final List<Expression> elements;
    public final boolean isRow;

    public ArrayExpr(List<Expression> elements, boolean isRow) {
        this.elements = elements;
        this.isRow = isRow;
    }

    /** Convenience constructor for non-ROW arrays. */
    public ArrayExpr(List<Expression> elements) {
        this(elements, false);
    }

    public List<Expression> elements() { return elements; }
    public boolean isRow() { return isRow; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ArrayExpr that = (ArrayExpr) o;
        return java.util.Objects.equals(elements, that.elements)
            && isRow == that.isRow;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(elements, isRow);
    }

    @Override
    public String toString() {
        return "ArrayExpr[elements=" + elements + ", " + "isRow=" + isRow + "]";
    }
}
