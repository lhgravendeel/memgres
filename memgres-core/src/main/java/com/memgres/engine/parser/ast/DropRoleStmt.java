package com.memgres.engine.parser.ast;

/**
 * DROP ROLE [IF EXISTS] name [, ...]
 */
public final class DropRoleStmt implements Statement {
    public final String name;
    public final boolean ifExists;

    public DropRoleStmt(String name, boolean ifExists) {
        this.name = name;
        this.ifExists = ifExists;
    }

    public String name() { return name; }
    public boolean ifExists() { return ifExists; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DropRoleStmt that = (DropRoleStmt) o;
        return java.util.Objects.equals(name, that.name)
            && ifExists == that.ifExists;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, ifExists);
    }

    @Override
    public String toString() {
        return "DropRoleStmt[name=" + name + ", " + "ifExists=" + ifExists + "]";
    }
}
