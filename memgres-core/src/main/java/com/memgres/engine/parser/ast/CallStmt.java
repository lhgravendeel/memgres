package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * CALL procedure_name(args...)
 */
public final class CallStmt implements Statement {
    public final String name;
    public final List<Expression> args;

    public CallStmt(String name, List<Expression> args) {
        this.name = name;
        this.args = args;
    }

    public String name() { return name; }
    public List<Expression> args() { return args; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CallStmt that = (CallStmt) o;
        return java.util.Objects.equals(name, that.name)
            && java.util.Objects.equals(args, that.args);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, args);
    }

    @Override
    public String toString() {
        return "CallStmt[name=" + name + ", " + "args=" + args + "]";
    }
}
