package com.memgres.engine;

import java.util.List;

/**
 * Represents a PostgreSQL event trigger definition stored in-memory.
 */
public class PgEventTrigger {

    private String name;
    private final String event; // ddl_command_start, ddl_command_end, sql_drop, table_rewrite
    private final String functionName;
    private final List<String> tags; // nullable: WHEN TAG IN (...)
    private char enabled; // 'O' = origin/enabled, 'D' = disabled, 'R' = replica, 'A' = always

    public PgEventTrigger(String name, String event, String functionName, List<String> tags) {
        this.name = name;
        this.event = event;
        this.functionName = functionName;
        this.tags = tags;
        this.enabled = 'O'; // enabled by default
    }

    public String getName() { return name; }
    public String getEvent() { return event; }
    public String getFunctionName() { return functionName; }
    public List<String> getTags() { return tags; }
    public char getEnabled() { return enabled; }

    public void setEnabled(char enabled) { this.enabled = enabled; }
    public void setName(String name) { this.name = name; }
}
