package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * ARRAY[expr, expr, ...] or '{1,2,3}' or ROW(expr, expr, ...)
 */
public final class ArrayExpr implements Expression {
    public final List<Expression> elements;
    public final boolean isRow;
    /**
     * Whether the row was written with the ROW keyword rather than as a bare parenthesised list.
     *
     * <p>The two build the same value, and nothing that evaluates a row has to tell them apart.
     * One rule does: a bare {@code (a, b)} written as a GROUP BY item is a list of two items and
     * groups by each of them, while {@code ROW(a, b)} is a single expression of row type and
     * groups by the row -- which licenses neither {@code a} nor {@code b} on its own.
     *
     * <p>Deliberately outside {@link #equals}: having grouped by one spelling, the other one
     * still names the same expression, so the two compare equal wherever expressions are
     * compared. Only how the item was written differs, and only the GROUP BY list reads that.
     */
    public final boolean rowKeyword;

    public ArrayExpr(List<Expression> elements, boolean isRow) {
        this(elements, isRow, false);
    }

    public ArrayExpr(List<Expression> elements, boolean isRow, boolean rowKeyword) {
        this.elements = elements;
        this.isRow = isRow;
        this.rowKeyword = rowKeyword;
    }

    /** Convenience constructor for non-ROW arrays. */
    public ArrayExpr(List<Expression> elements) {
        this(elements, false);
    }

    public List<Expression> elements() { return elements; }
    public boolean isRow() { return isRow; }
    public boolean rowKeyword() { return rowKeyword; }

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
