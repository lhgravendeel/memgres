package com.memgres.engine.parser.ast;

/**
 * One column of the row a multi-column UPDATE assignment takes its values from:
 * {@code UPDATE t SET (a, b) = (SELECT x, y FROM ...)}.
 *
 * <p>The assignment is written once and names several columns, but the engine stores an
 * assignment per column -- so each column carries this node, and they share the one source node.
 * Sharing it is what makes the source read once per updated row rather than once per column: the
 * first of them evaluates it and binds the result to the source node in the row's context, and
 * the rest read that binding.
 */
public final class RowElementExpr implements Expression {
    public final Expression source;
    public final int index;
    public final int width;

    public RowElementExpr(Expression source, int index, int width) {
        this.source = source;
        this.index = index;
        this.width = width;
    }

    public Expression source() { return source; }

    public int index() { return index; }

    /** How many columns the assignment names, which is how wide the source has to be. */
    public int width() { return width; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RowElementExpr that = (RowElementExpr) o;
        return index == that.index && width == that.width
                && java.util.Objects.equals(source, that.source);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(source, index, width);
    }

    @Override
    public String toString() {
        return "RowElementExpr[source=" + source + ", index=" + index + ", width=" + width + "]";
    }
}
