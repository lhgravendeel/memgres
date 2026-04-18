package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * CREATE EVENT TRIGGER name ON event [WHEN TAG IN (...)] EXECUTE FUNCTION funcname()
 */
public final class CreateEventTriggerStmt implements Statement {
    public final String name;
    public final String event;
    public final List<String> tags;
    public final String functionName;

    public CreateEventTriggerStmt(String name, String event, List<String> tags, String functionName) {
        this.name = name;
        this.event = event;
        this.tags = tags;
        this.functionName = functionName;
    }

    public String name() { return name; }
    public String event() { return event; }
    public List<String> tags() { return tags; }
    public String functionName() { return functionName; }
}
