package com.memgres.engine.parser.ast;

import com.memgres.engine.util.Cols;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * CREATE ROLE|USER name [WITH options] [IN ROLE role, ...]
 */
public final class CreateRoleStmt implements Statement {
    public final String name;
    public final boolean isUser;
    public final Map<String, String> options;
    public final List<String> inRoles;

    public CreateRoleStmt(String name, boolean isUser, Map<String, String> options) {
        this(name, isUser, options, Cols.listOf());
    }

    public CreateRoleStmt(String name, boolean isUser, Map<String, String> options, List<String> inRoles) {
        this.name = name;
        this.isUser = isUser;
        this.options = options;
        this.inRoles = inRoles;
    }

    public String name() { return name; }
    public boolean isUser() { return isUser; }
    public Map<String, String> options() { return options; }
    public List<String> inRoles() { return inRoles; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateRoleStmt that = (CreateRoleStmt) o;
        return Objects.equals(name, that.name)
            && isUser == that.isUser
            && Objects.equals(options, that.options)
            && Objects.equals(inRoles, that.inRoles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, isUser, options, inRoles);
    }

    @Override
    public String toString() {
        return "CreateRoleStmt[name=" + name + ", isUser=" + isUser + ", options=" + options + ", inRoles=" + inRoles + "]";
    }
}
