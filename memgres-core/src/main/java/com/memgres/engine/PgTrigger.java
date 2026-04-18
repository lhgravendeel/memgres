package com.memgres.engine;

/**
 * Represents a PostgreSQL trigger definition.
 */
public class PgTrigger {

    public enum Timing { BEFORE, AFTER, INSTEAD_OF }
    public enum Event { INSERT, UPDATE, DELETE, TRUNCATE }

    private final String name;
    private final Timing timing;
    private final Event event;
    private final String tableName;
    private final String functionName;
    private final java.util.List<String> updateColumns; // for UPDATE OF col1, col2
    private final String newTransitionTable; // REFERENCING NEW TABLE AS name
    private final String oldTransitionTable; // REFERENCING OLD TABLE AS name
    private final boolean forEachStatement; // FOR EACH STATEMENT (vs FOR EACH ROW)
    private final String whenClause; // WHEN (condition) clause text
    private final boolean deferrable; // CONSTRAINT TRIGGER ... DEFERRABLE
    private final boolean initiallyDeferred; // DEFERRABLE INITIALLY DEFERRED
    private String schemaName; // schema where the trigger's table lives
    private boolean disabled; // true when ALTER TABLE ... DISABLE TRIGGER sets this

    public PgTrigger(String name, Timing timing, Event event, String tableName, String functionName) {
        this(name, timing, event, tableName, functionName, null, null, null, false, null, false, false);
    }

    public PgTrigger(String name, Timing timing, Event event, String tableName, String functionName,
                     java.util.List<String> updateColumns, String newTransitionTable, String oldTransitionTable,
                     boolean forEachStatement) {
        this(name, timing, event, tableName, functionName, updateColumns, newTransitionTable, oldTransitionTable,
                forEachStatement, null, false, false);
    }

    public PgTrigger(String name, Timing timing, Event event, String tableName, String functionName,
                     java.util.List<String> updateColumns, String newTransitionTable, String oldTransitionTable,
                     boolean forEachStatement, String whenClause, boolean deferrable, boolean initiallyDeferred) {
        this.name = name;
        this.timing = timing;
        this.event = event;
        this.tableName = tableName;
        this.functionName = functionName;
        this.updateColumns = updateColumns;
        this.newTransitionTable = newTransitionTable;
        this.oldTransitionTable = oldTransitionTable;
        this.forEachStatement = forEachStatement;
        this.whenClause = whenClause;
        this.deferrable = deferrable;
        this.initiallyDeferred = initiallyDeferred;
    }

    public String getName() {
        return name;
    }

    public Timing getTiming() {
        return timing;
    }

    public Event getEvent() {
        return event;
    }

    public String getTableName() {
        return tableName;
    }

    public String getFunctionName() {
        return functionName;
    }

    public java.util.List<String> getUpdateColumns() {
        return updateColumns;
    }

    public String getNewTransitionTable() {
        return newTransitionTable;
    }

    public String getOldTransitionTable() {
        return oldTransitionTable;
    }

    public boolean isForEachStatement() {
        return forEachStatement;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }

    public String getWhenClause() {
        return whenClause;
    }

    public boolean isDeferrable() {
        return deferrable;
    }

    public boolean isInitiallyDeferred() {
        return initiallyDeferred;
    }
}
