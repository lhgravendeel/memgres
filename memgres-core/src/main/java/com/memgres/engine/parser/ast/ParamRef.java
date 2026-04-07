package com.memgres.engine.parser.ast;

/**
 * A positional parameter reference: $1, $2, etc.
 */
public final class ParamRef implements Expression {
    public final int index;

    public ParamRef(int index) {
        this.index = index;
    }

    public int index() { return index; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ParamRef that = (ParamRef) o;
        return index == that.index;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(index);
    }

    @Override
    public String toString() {
        return "ParamRef[index=" + index + "]";
    }
}
