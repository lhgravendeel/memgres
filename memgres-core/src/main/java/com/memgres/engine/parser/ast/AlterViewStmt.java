package com.memgres.engine.parser.ast;

import java.util.Map;

/**
 * ALTER VIEW [IF EXISTS] name RENAME TO new_name
 * Also handles ALTER VIEW ... SET SCHEMA, ALTER VIEW ... OWNER TO, etc.
 */
public final class AlterViewStmt implements Statement {
    public final String name;
    public final String newName;
    public final boolean ifExists;
    public final Action action;
    public final Map<String, String> setOptions;

    public AlterViewStmt(String name, String newName, boolean ifExists, Action action) {
        this(name, newName, ifExists, action, null);
    }

    public AlterViewStmt(String name, String newName, boolean ifExists, Action action, Map<String, String> setOptions) {
        this.name = name;
        this.newName = newName;
        this.ifExists = ifExists;
        this.action = action;
        this.setOptions = setOptions;
    }

    public enum Action {
        RENAME_TO,
        OWNER_TO,
        SET_OPTIONS,
        NO_OP    // SET SCHEMA, etc., accepted but ignored
    }

    /** Convenience constructor for RENAME TO. */
    public AlterViewStmt(String name, String newName, boolean ifExists) {
        this(name, newName, ifExists, Action.RENAME_TO);
    }

    public String name() { return name; }
    public String newName() { return newName; }
    public boolean ifExists() { return ifExists; }
    public Action action() { return action; }
    public Map<String, String> setOptions() { return setOptions; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlterViewStmt that = (AlterViewStmt) o;
        return java.util.Objects.equals(name, that.name)
            && java.util.Objects.equals(newName, that.newName)
            && ifExists == that.ifExists
            && java.util.Objects.equals(action, that.action);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, newName, ifExists, action);
    }

    @Override
    public String toString() {
        return "AlterViewStmt[name=" + name + ", " + "newName=" + newName + ", " + "ifExists=" + ifExists + ", " + "action=" + action + "]";
    }
}
