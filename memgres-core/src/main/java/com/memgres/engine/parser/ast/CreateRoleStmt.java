package com.memgres.engine.parser.ast;

import java.util.Map;
import java.util.Objects;

/**
 * CREATE ROLE|USER name [WITH options]
 */
public final class CreateRoleStmt implements Statement {
    public final String name;
    public final boolean isUser;
    public final Map<String, String> options;

    public CreateRoleStmt(String name, boolean isUser, Map<String, String> options) {
        this.name = name;
        this.isUser = isUser;
        this.options = options;
    }

    public String name() { return name; }
    public boolean isUser() { return isUser; }
    public Map<String, String> options() { return options; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateRoleStmt that = (CreateRoleStmt) o;
        return Objects.equals(name, that.name)
            && isUser == that.isUser
            && Objects.equals(options, that.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, isUser, options);
    }

    @Override
    public String toString() {
        return "CreateRoleStmt[name=" + name + ", isUser=" + isUser + ", options=" + options + "]";
    }
}
