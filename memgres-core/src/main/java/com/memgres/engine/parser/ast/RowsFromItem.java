package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * One element of a {@code ROWS FROM (...)} list: the function call, and the column definition
 * list written after it as {@code f(...) AS (name type, ...)}.
 *
 * <p>A ROWS FROM list gives each of its functions its own description, which is why the
 * definition list cannot be the FROM item's own column alias list -- that one renames the
 * columns of every function at once. Each entry is held as {@code "name type"}, the same shape
 * a column alias list uses elsewhere in this parser.
 */
public final class RowsFromItem implements Expression {
    public final FunctionCallExpr call;
    public final List<String> columnDefs;

    public RowsFromItem(FunctionCallExpr call, List<String> columnDefs) {
        this.call = call;
        this.columnDefs = columnDefs;
    }

    public FunctionCallExpr call() { return call; }
    public List<String> columnDefs() { return columnDefs; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RowsFromItem that = (RowsFromItem) o;
        return java.util.Objects.equals(call, that.call)
            && java.util.Objects.equals(columnDefs, that.columnDefs);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(call, columnDefs);
    }

    @Override
    public String toString() {
        return "RowsFromItem[call=" + call + ", columnDefs=" + columnDefs + "]";
    }
}
