package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * PREPARE name [(type, ...)] AS statement
 */
public final class PrepareStmt implements Statement {
    public final String name;
    public final List<String> paramTypes;
    public final Statement body;

    public PrepareStmt(String name, List<String> paramTypes, Statement body) {
        this.name = name;
        this.paramTypes = paramTypes;
        this.body = body;
    }

    public String name() { return name; }
    public List<String> paramTypes() { return paramTypes; }
    public Statement body() { return body; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PrepareStmt that = (PrepareStmt) o;
        return java.util.Objects.equals(name, that.name)
            && java.util.Objects.equals(paramTypes, that.paramTypes)
            && java.util.Objects.equals(body, that.body);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, paramTypes, body);
    }

    @Override
    public String toString() {
        return "PrepareStmt[name=" + name + ", " + "paramTypes=" + paramTypes + ", " + "body=" + body + "]";
    }
}
