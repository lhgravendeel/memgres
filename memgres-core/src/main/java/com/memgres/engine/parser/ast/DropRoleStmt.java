package com.memgres.engine.parser.ast;

import java.util.Collections;
import java.util.List;

/**
 * DROP ROLE [IF EXISTS] name [, ...]
 *
 * <p>One statement may name several roles, and it drops all of them. Read as one name with the
 * rest advanced over and thrown away, {@code DROP ROLE a, b, c} dropped a alone and reported
 * success, leaving b and c behind for whoever read the report to believe were gone.
 */
public final class DropRoleStmt implements Statement {
    public final String name;
    public final boolean ifExists;
    private final List<String> names;

    public DropRoleStmt(String name, boolean ifExists) {
        this(Collections.singletonList(name), ifExists);
    }

    public DropRoleStmt(List<String> names, boolean ifExists) {
        this.names = names;
        this.name = names.isEmpty() ? null : names.get(0);
        this.ifExists = ifExists;
    }

    public String name() { return name; }

    /** Every role the statement named, in the order they were written. */
    public List<String> names() { return names; }

    public boolean ifExists() { return ifExists; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DropRoleStmt that = (DropRoleStmt) o;
        return java.util.Objects.equals(names, that.names)
            && ifExists == that.ifExists;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(names, ifExists);
    }

    @Override
    public String toString() {
        return "DropRoleStmt[names=" + names + ", " + "ifExists=" + ifExists + "]";
    }
}
