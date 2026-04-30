package com.memgres.engine.parser.ast;

/**
 * DROP EVENT TRIGGER [IF EXISTS] name [CASCADE | RESTRICT]
 */
public final class DropEventTriggerStmt implements Statement {
    public final String name;
    public final boolean ifExists;

    public DropEventTriggerStmt(String name, boolean ifExists) {
        this.name = name;
        this.ifExists = ifExists;
    }

    public String name() { return name; }
    public boolean ifExists() { return ifExists; }
}
