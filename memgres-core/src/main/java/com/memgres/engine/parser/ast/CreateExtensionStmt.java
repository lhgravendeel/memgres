package com.memgres.engine.parser.ast;

/**
 * CREATE EXTENSION [IF NOT EXISTS] name [WITH] [SCHEMA schema] [VERSION version] [CASCADE]
 */
public final class CreateExtensionStmt implements Statement {
    public final String name;
    public final boolean ifNotExists;
    private final String schema;
    private final String version;
    private final boolean cascade;

    public CreateExtensionStmt(String name, boolean ifNotExists) {
        this(name, ifNotExists, null, null, false);
    }

    public CreateExtensionStmt(String name, boolean ifNotExists, String schema, String version, boolean cascade) {
        this.name = name;
        this.ifNotExists = ifNotExists;
        this.schema = schema;
        this.version = version;
        this.cascade = cascade;
    }

    public String name() { return name; }
    public boolean ifNotExists() { return ifNotExists; }
    public String schema() { return schema; }
    public String version() { return version; }
    public boolean cascade() { return cascade; }

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
