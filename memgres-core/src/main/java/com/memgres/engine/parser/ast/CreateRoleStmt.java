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
    /**
     * The roles this one is granted to — the ROLE clause makes them members of the new role,
     * where IN ROLE makes the new role a member of them. The two run in opposite directions and
     * reading only the first left half of what the statement said unrecorded.
     */
    public final List<String> memberRoles;
    /** The members the ADMIN clause names, which hold their membership with admin option. */
    public final List<String> adminRoles;

    public CreateRoleStmt(String name, boolean isUser, Map<String, String> options) {
        this(name, isUser, options, Cols.listOf());
    }

    public CreateRoleStmt(String name, boolean isUser, Map<String, String> options,
                          List<String> inRoles) {
        this(name, isUser, options, inRoles, java.util.Collections.<String>emptyList(),
                java.util.Collections.<String>emptyList());
    }

    public CreateRoleStmt(String name, boolean isUser, Map<String, String> options,
                          List<String> inRoles, List<String> memberRoles,
                          List<String> adminRoles) {
        this.memberRoles = memberRoles;
        this.adminRoles = adminRoles;
        this.name = name;
        this.isUser = isUser;
        this.options = options;
        this.inRoles = inRoles;
    }

    public String name() { return name; }
    public boolean isUser() { return isUser; }
    public Map<String, String> options() { return options; }
    public List<String> inRoles() { return inRoles; }
    public List<String> memberRoles() { return memberRoles; }
    public List<String> adminRoles() { return adminRoles; }

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
