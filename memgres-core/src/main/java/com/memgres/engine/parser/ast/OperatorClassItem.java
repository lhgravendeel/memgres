package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * One entry of an operator class or family: {@code OPERATOR 3 =} or {@code FUNCTION 1 f(int,int)}.
 *
 * <p>The AS clause was read by skipping to the semicolon, so an operator class could name a
 * strategy number the access method does not have, an operator that does not exist and a support
 * function that was never written, and the statement reported success.
 */
public final class OperatorClassItem {

    public enum Kind { OPERATOR, FUNCTION }

    public final Kind kind;
    public final int number;
    /** The operator symbol, or the routine's name. */
    public final String name;
    /** The argument types written in parentheses after it, empty when none were. */
    public final List<String> argTypes;

    public OperatorClassItem(Kind kind, int number, String name, List<String> argTypes) {
        this.kind = kind;
        this.number = number;
        this.name = name;
        this.argTypes = argTypes;
    }

    public Kind kind() { return kind; }
    public int number() { return number; }
    public String name() { return name; }
    public List<String> argTypes() { return argTypes; }

    @Override
    public String toString() {
        return kind + " " + number + " " + name + argTypes;
    }
}
