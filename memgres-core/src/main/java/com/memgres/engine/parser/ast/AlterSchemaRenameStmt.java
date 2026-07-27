package com.memgres.engine.parser.ast;

/**
 * ALTER SCHEMA name RENAME TO newName
 */
public final class AlterSchemaRenameStmt implements Statement {
    private final String name;
    private final String newName;

    public AlterSchemaRenameStmt(String name, String newName) {
        this.name = name;
        this.newName = newName;
    }

    public String name() { return name; }
    public String newName() { return newName; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlterSchemaRenameStmt that = (AlterSchemaRenameStmt) o;
        return java.util.Objects.equals(name, that.name)
                && java.util.Objects.equals(newName, that.newName);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, newName);
    }

    @Override
    public String toString() {
        return "AlterSchemaRenameStmt[name=" + name + ", newName=" + newName + "]";
    }
}
