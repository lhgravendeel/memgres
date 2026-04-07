package com.memgres.engine.parser.ast;

/**
 * A named argument expression: name => expr (used in function calls like make_interval).
 */
public final class NamedArgExpr implements Expression {
    public final String name;
    public final Expression value;

    public NamedArgExpr(String name, Expression value) {
        this.name = name;
        this.value = value;
    }

    public String name() { return name; }
    public Expression value() { return value; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NamedArgExpr that = (NamedArgExpr) o;
        return java.util.Objects.equals(name, that.name)
            && java.util.Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, value);
    }

    @Override
    public String toString() {
        return "NamedArgExpr[name=" + name + ", " + "value=" + value + "]";
    }
}
