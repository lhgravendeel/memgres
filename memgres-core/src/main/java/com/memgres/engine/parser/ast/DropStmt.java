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
    public final java.util.List<String> paramTypes;
    /** Explicit schema qualifier, when the statement gave one; null means "search the path". */
    public final String schema;
    /**
     * The further objects the same DROP names, each a drop of its own kind, ifExists and
     * behaviour. Never null; empty when the statement named a single object.
     */
    public final java.util.List<DropStmt> more;

    public DropStmt(
            ObjectType objectType,
            String name,
            String onTable,
            boolean ifExists,
            boolean cascade
    ) {
        this(objectType, name, onTable, ifExists, cascade, null);
    }

    public DropStmt(
            ObjectType objectType,
            String name,
            String onTable,
            boolean ifExists,
            boolean cascade,
            java.util.List<String> paramTypes
    ) {
        this(objectType, name, onTable, ifExists, cascade, paramTypes, null);
    }

    public DropStmt(
            ObjectType objectType,
            String name,
            String onTable,
            boolean ifExists,
            boolean cascade,
            java.util.List<String> paramTypes,
            String schema
    ) {
        this(objectType, name, onTable, ifExists, cascade, paramTypes, schema, null);
    }

    public DropStmt(
            ObjectType objectType,
            String name,
            String onTable,
            boolean ifExists,
            boolean cascade,
            java.util.List<String> paramTypes,
            String schema,
            java.util.List<DropStmt> more
    ) {
        this.objectType = objectType;
        this.name = name;
        this.onTable = onTable;
        this.ifExists = ifExists;
        this.cascade = cascade;
        this.paramTypes = paramTypes;
        this.schema = schema;
        this.more = more == null
                ? java.util.Collections.<DropStmt>emptyList()
                : java.util.Collections.unmodifiableList(new java.util.ArrayList<DropStmt>(more));
    }

    public enum ObjectType {
        // ROUTINE is FUNCTION and PROCEDURE together: it names a routine of either kind, where
        // each of the other two names a routine of one kind and refuses the other.
        FUNCTION, PROCEDURE, ROUTINE,
        TRIGGER, TYPE, INDEX, VIEW, MATERIALIZED_VIEW, SEQUENCE, SCHEMA, DOMAIN,
        EXTENSION, RULE, COLLATION, CAST, CONVERSION, AGGREGATE,
        OPERATOR, OPERATOR_CLASS, OPERATOR_FAMILY, POLICY
    }

    public ObjectType objectType() { return objectType; }
    public String name() { return name; }
    public String onTable() { return onTable; }
    public boolean ifExists() { return ifExists; }
    public boolean cascade() { return cascade; }
    public java.util.List<String> paramTypes() { return paramTypes; }
    public String schema() { return schema; }
    public java.util.List<DropStmt> more() { return more; }

    /** This drop and the further ones the same statement names, in the order they were written. */
    public static java.util.List<DropStmt> allOf(DropStmt first) {
        java.util.List<DropStmt> all = new java.util.ArrayList<DropStmt>();
        all.add(first);
        if (first.more() != null) all.addAll(first.more());
        return all;
    }

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
