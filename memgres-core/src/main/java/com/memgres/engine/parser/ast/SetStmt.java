package com.memgres.engine.parser.ast;

/**
 * SET [SESSION|LOCAL] name = value | SET name TO value
 */
public final class SetStmt implements Statement {
    public final String name;
    public final String value;
    public final boolean isLocal;

    public SetStmt(String name, String value, boolean isLocal) {
        this.name = name;
        this.value = value;
        this.isLocal = isLocal;
    }

    /** Convenience constructor for non-LOCAL SET (backward compat). */
    public SetStmt(String name, String value) {
        this(name, value, false);
    }

    public String name() { return name; }
    public String value() { return value; }
    public boolean isLocal() { return isLocal; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SetStmt that = (SetStmt) o;
        return java.util.Objects.equals(name, that.name)
            && java.util.Objects.equals(value, that.value)
            && isLocal == that.isLocal;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, value, isLocal);
    }

    @Override
    public String toString() {
        return "SetStmt[name=" + name + ", " + "value=" + value + ", " + "isLocal=" + isLocal + "]";
    }
}
