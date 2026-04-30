package com.memgres.engine.parser.ast;

public final class AlterTypeStmt implements Statement {
    public final String typeName;
    public final Action action;
    public final String value;
    public final String newValue;
    public final boolean ifNotExists;
    public final String position;
    public final String neighbor;

    public AlterTypeStmt(
            String typeName,
            Action action,
            String value,
            String newValue,
            boolean ifNotExists,
            String position,
            String neighbor
    ) {
        this.typeName = typeName;
        this.action = action;
        this.value = value;
        this.newValue = newValue;
        this.ifNotExists = ifNotExists;
        this.position = position;
        this.neighbor = neighbor;
    }

    public enum Action { ADD_VALUE, RENAME_VALUE, RENAME_TO, SET_SCHEMA, OWNER_TO,
        ADD_ATTRIBUTE, DROP_ATTRIBUTE, ALTER_ATTRIBUTE_TYPE, RENAME_ATTRIBUTE }

    public String typeName() { return typeName; }
    public Action action() { return action; }
    public String value() { return value; }
    public String newValue() { return newValue; }
    public boolean ifNotExists() { return ifNotExists; }
    public String position() { return position; }
    public String neighbor() { return neighbor; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlterTypeStmt that = (AlterTypeStmt) o;
        return java.util.Objects.equals(typeName, that.typeName)
            && java.util.Objects.equals(action, that.action)
            && java.util.Objects.equals(value, that.value)
            && java.util.Objects.equals(newValue, that.newValue)
            && ifNotExists == that.ifNotExists
            && java.util.Objects.equals(position, that.position)
            && java.util.Objects.equals(neighbor, that.neighbor);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(typeName, action, value, newValue, ifNotExists, position, neighbor);
    }

    @Override
    public String toString() {
        return "AlterTypeStmt[typeName=" + typeName + ", " + "action=" + action + ", " + "value=" + value + ", " + "newValue=" + newValue + ", " + "ifNotExists=" + ifNotExists + ", " + "position=" + position + ", " + "neighbor=" + neighbor + "]";
    }
}
