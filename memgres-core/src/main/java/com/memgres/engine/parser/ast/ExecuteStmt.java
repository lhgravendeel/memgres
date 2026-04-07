package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * EXECUTE name [(param, ...)]
 */
public final class ExecuteStmt implements Statement {
    public final String name;
    public final List<Expression> params;

    public ExecuteStmt(String name, List<Expression> params) {
        this.name = name;
        this.params = params;
    }

    public String name() { return name; }
    public List<Expression> params() { return params; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExecuteStmt that = (ExecuteStmt) o;
        return java.util.Objects.equals(name, that.name)
            && java.util.Objects.equals(params, that.params);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, params);
    }

    @Override
    public String toString() {
        return "ExecuteStmt[name=" + name + ", " + "params=" + params + "]";
    }
}
