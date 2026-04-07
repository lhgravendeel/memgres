package com.memgres.engine.parser.ast;

/**
 * DROP {FUNCTION|TRIGGER|TYPE|INDEX|VIEW|SEQUENCE|SCHEMA|EXTENSION|RULE|...} [IF EXISTS] name [ON table] [CASCADE]
 */
public final class DropStmt implements Statement {
    public final ObjectType objectType;
    public final String name;
    public final String onTable;
    public final boolean ifExists;
    public final boolean cascade;

    public DropStmt(
            ObjectType objectType,
            String name,
            String onTable,
            boolean ifExists,
            boolean cascade
    ) {
        this.objectType = objectType;
        this.name = name;
        this.onTable = onTable;
        this.ifExists = ifExists;
        this.cascade = cascade;
    }

    public enum ObjectType {
        FUNCTION, TRIGGER, TYPE, INDEX, VIEW, SEQUENCE, SCHEMA, DOMAIN,
        EXTENSION, RULE, COLLATION, CAST, CONVERSION, AGGREGATE,
        OPERATOR, OPERATOR_CLASS, OPERATOR_FAMILY, POLICY
    }

    public ObjectType objectType() { return objectType; }
    public String name() { return name; }
    public String onTable() { return onTable; }
    public boolean ifExists() { return ifExists; }
    public boolean cascade() { return cascade; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DropStmt that = (DropStmt) o;
        return java.util.Objects.equals(objectType, that.objectType)
            && java.util.Objects.equals(name, that.name)
            && java.util.Objects.equals(onTable, that.onTable)
            && java.util.Objects.equals(ifExists, that.ifExists)
            && cascade == that.cascade;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(objectType, name, onTable, ifExists, cascade);
    }

    @Override
    public String toString() {
        return "DropStmt[objectType=" + objectType + ", " + "name=" + name + ", " + "onTable=" + onTable + ", " + "ifExists=" + ifExists + ", " + "cascade=" + cascade + "]";
    }
}
