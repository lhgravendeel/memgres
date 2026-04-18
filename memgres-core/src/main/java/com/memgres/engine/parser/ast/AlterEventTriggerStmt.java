package com.memgres.engine.parser.ast;

/**
 * ALTER EVENT TRIGGER name {DISABLE | ENABLE | ENABLE REPLICA | ENABLE ALWAYS | RENAME TO newname | OWNER TO newowner}
 */
public final class AlterEventTriggerStmt implements Statement {
    public enum Action { DISABLE, ENABLE, ENABLE_REPLICA, ENABLE_ALWAYS, RENAME, OWNER }

    public final String name;
    public final Action action;
    public final String newName; // for RENAME TO

    public AlterEventTriggerStmt(String name, Action action, String newName) {
        this.name = name;
        this.action = action;
        this.newName = newName;
    }

    public String name() { return name; }
    public Action action() { return action; }
    public String newName() { return newName; }
}
