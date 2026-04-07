package com.memgres.engine.parser.ast;

import java.util.Objects;

/**
 * CREATE RULE name AS ON event TO table DO [INSTEAD|ALSO] action
 */
public final class CreateRuleStmt implements Statement {
    public final String name;
    public final String event;
    public final String table;
    public final String action;
    public final String command;

    public CreateRuleStmt(String name, String event, String table, String action, String command) {
        this.name = name;
        this.event = event;
        this.table = table;
        this.action = action;
        this.command = command;
    }

    public String name() { return name; }
    public String event() { return event; }
    public String table() { return table; }
    public String action() { return action; }
    public String command() { return command; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateRuleStmt that = (CreateRuleStmt) o;
        return Objects.equals(name, that.name)
            && Objects.equals(event, that.event)
            && Objects.equals(table, that.table)
            && Objects.equals(action, that.action)
            && Objects.equals(command, that.command);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, event, table, action, command);
    }

    @Override
    public String toString() {
        return "CreateRuleStmt[name=" + name + ", event=" + event + ", table=" + table
            + ", action=" + action + ", command=" + command + "]";
    }
}
