package com.memgres.engine.parser.ast;

/** ALTER SCHEMA name OWNER TO new_owner */
public final class AlterSchemaOwnerStmt implements Statement {
    public final String name;
    public final String newOwner;

    public AlterSchemaOwnerStmt(String name, String newOwner) {
        this.name = name;
        this.newOwner = newOwner;
    }

    public String name() { return name; }
    public String newOwner() { return newOwner; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlterSchemaOwnerStmt that = (AlterSchemaOwnerStmt) o;
        return java.util.Objects.equals(name, that.name)
            && java.util.Objects.equals(newOwner, that.newOwner);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, newOwner);
    }

    @Override
    public String toString() {
        return "AlterSchemaOwnerStmt[name=" + name + ", " + "newOwner=" + newOwner + "]";
    }
}
