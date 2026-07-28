package com.memgres.engine.parser.ast;

import java.util.List;
import java.util.Objects;

/**
 * CREATE [OR REPLACE] RULE name AS ON event TO table [WHERE cond] DO [INSTEAD|ALSO] action
 *
 * <p>A rule may carry several actions, written as a parenthesised semicolon-separated list, so
 * {@link #commands} rather than {@link #command} is what the executor stores; {@code command}
 * remains the first action for the single-action case that every other caller assumes.
 */
public final class CreateRuleStmt implements Statement {
    public final String name;
    public final String event;
    public final String table;
    public final String action;
    public final String command;
    public final List<String> commands;
    public final String whereClause;
    public final boolean orReplace;

    public CreateRuleStmt(String name, String event, String table, String action, String command) {
        this(name, event, table, action,
                command == null || command.isEmpty() ? java.util.Collections.<String>emptyList()
                        : java.util.Collections.singletonList(command),
                null, false);
    }

    public CreateRuleStmt(String name, String event, String table, String action,
                          List<String> commands, String whereClause, boolean orReplace) {
        this.name = name;
        this.event = event;
        this.table = table;
        this.action = action;
        this.commands = commands == null ? java.util.Collections.<String>emptyList() : commands;
        this.command = this.commands.isEmpty() ? "" : this.commands.get(0);
        this.whereClause = whereClause;
        this.orReplace = orReplace;
    }

    public String name() { return name; }
    public String event() { return event; }
    public String table() { return table; }
    public String action() { return action; }
    public String command() { return command; }
    public List<String> commands() { return commands; }
    public String whereClause() { return whereClause; }
    public boolean orReplace() { return orReplace; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateRuleStmt that = (CreateRuleStmt) o;
        return Objects.equals(name, that.name)
            && Objects.equals(event, that.event)
            && Objects.equals(table, that.table)
            && Objects.equals(action, that.action)
            && Objects.equals(commands, that.commands)
            && Objects.equals(whereClause, that.whereClause)
            && orReplace == that.orReplace;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, event, table, action, commands, whereClause, orReplace);
    }

    @Override
    public String toString() {
        return "CreateRuleStmt[name=" + name + ", event=" + event + ", table=" + table
            + ", action=" + action + ", commands=" + commands + "]";
    }
}
