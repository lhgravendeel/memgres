package com.memgres.engine.parser.ast;

/**
 * CREATE SCHEMA [IF NOT EXISTS] name [AUTHORIZATION owner]
 */
public final class CreateSchemaStmt implements Statement {
    public final String name;
    public final boolean ifNotExists;
    public final String authorization;

    public CreateSchemaStmt(String name, boolean ifNotExists, String authorization) {
        this.name = name;
        this.ifNotExists = ifNotExists;
        this.authorization = authorization;
    }

    public String name() { return name; }
    public boolean ifNotExists() { return ifNotExists; }
    public String authorization() { return authorization; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateSchemaStmt that = (CreateSchemaStmt) o;
        return java.util.Objects.equals(name, that.name)
            && ifNotExists == that.ifNotExists
            && java.util.Objects.equals(authorization, that.authorization);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, ifNotExists, authorization);
    }

    @Override
    public String toString() {
        return "CreateSchemaStmt[name=" + name + ", " + "ifNotExists=" + ifNotExists + ", " + "authorization=" + authorization + "]";
    }
}
