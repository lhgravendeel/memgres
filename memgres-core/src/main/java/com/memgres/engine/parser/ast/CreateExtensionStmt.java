package com.memgres.engine.parser.ast;

/**
 * CREATE EXTENSION [IF NOT EXISTS] name
 */
public final class CreateExtensionStmt implements Statement {
    public final String name;
    public final boolean ifNotExists;

    public CreateExtensionStmt(String name, boolean ifNotExists) {
        this.name = name;
        this.ifNotExists = ifNotExists;
    }

    public String name() { return name; }
    public boolean ifNotExists() { return ifNotExists; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateExtensionStmt that = (CreateExtensionStmt) o;
        return java.util.Objects.equals(name, that.name)
            && ifNotExists == that.ifNotExists;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, ifNotExists);
    }

    @Override
    public String toString() {
        return "CreateExtensionStmt[name=" + name + ", " + "ifNotExists=" + ifNotExists + "]";
    }
}
