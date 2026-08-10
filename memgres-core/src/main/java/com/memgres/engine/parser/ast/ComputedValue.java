package com.memgres.engine.parser.ast;

/**
 * A value that has already been computed, standing where an expression stands.
 *
 * <p>{@code INSERT ... SELECT} runs its query and then has to hand each result value to the insert
 * path, which takes expressions. It used to rebuild one by writing the value out with Java's
 * {@code toString()} and reading the text back as a literal — so a bytea became the text of its
 * identity hash, an array became a Java list, and both were stored as what they had been printed
 * as. A value that is already the value does not need a spelling to survive the journey.
 */
public final class ComputedValue implements Expression {

    private final Object value;

    public ComputedValue(Object value) {
        this.value = value;
    }

    public Object value() {
        return value;
    }

    @Override
    public String toString() {
        return value == null ? "NULL" : value.toString();
    }
}
