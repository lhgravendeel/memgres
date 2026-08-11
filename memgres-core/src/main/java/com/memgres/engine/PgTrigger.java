package com.memgres.engine;

/**
 * Represents a PostgreSQL trigger definition.
 */
public class PgTrigger {

    public enum Timing { BEFORE, AFTER, INSTEAD_OF }
    public enum Event { INSERT, UPDATE, DELETE, TRUNCATE }

    private String name;
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
    private final java.util.List<String> args; // function arguments from EXECUTE FUNCTION func(arg1, arg2, ...)
    private String schemaName; // schema where the trigger's table lives
    /**
     * The firing mode, as {@code pg_trigger.tgenabled} spells it: {@code O} origin (the default),
     * {@code D} disabled, {@code R} replica, {@code A} always. Only {@code O} and {@code A} fire in
     * an ordinary session — a replica trigger waits for a session in replica mode.
     */
    private String enabledState = "O";
    /** Set when CREATE CONSTRAINT TRIGGER was the statement, rather than CREATE TRIGGER. */
    private boolean constraintTrigger;
    /** The relation a constraint trigger's FROM clause named, or null when none was written. */
    private String constraintRelation;
    /**
     * The partitioned relation this trigger was cloned from, or null when it was written on the
     * relation it sits on. PostgreSQL clones a partitioned table's FOR EACH ROW triggers onto
     * every partition and points each copy's {@code tgparentid} at the trigger it came from: the
     * copy is a catalog row of its own, it goes when the original does, and it may not be dropped
     * on its own because the original requires it.
     */
    private String clonedFromTable;

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
        this(name, timing, event, tableName, functionName, updateColumns, newTransitionTable, oldTransitionTable,
                forEachStatement, whenClause, deferrable, initiallyDeferred, null);
    }

    public PgTrigger(String name, Timing timing, Event event, String tableName, String functionName,
                     java.util.List<String> updateColumns, String newTransitionTable, String oldTransitionTable,
                     boolean forEachStatement, String whenClause, boolean deferrable, boolean initiallyDeferred,
                     java.util.List<String> args) {
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
        this.args = args;
    }

    public String getName() {
        return name;
    }

    /** ALTER TRIGGER ... RENAME TO: the trigger keeps its definition under a new name. */
    public void setName(String name) {
        this.name = name;
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
        return !("O".equals(enabledState) || "A".equals(enabledState));
    }

    /**
     * Whether this trigger fires while the session is in the given replication role. PostgreSQL
     * decides it per trigger: {@code tgenabled} 'O' fires only in origin or local, 'R' only in
     * replica, 'A' in both and 'D' in neither. Deciding it once for the whole statement left an
     * ENABLE REPLICA trigger with no mode at all in which it ran.
     */
    public boolean firesUnderReplicationRole(boolean replicaRole) {
        if ("A".equals(enabledState)) return true;
        if ("D".equals(enabledState)) return false;
        if ("R".equals(enabledState)) return replicaRole;
        return !replicaRole;
    }

    public void setDisabled(boolean disabled) {
        this.enabledState = disabled ? "D" : "O";
    }

    /** The {@code pg_trigger.tgenabled} code this trigger carries. */
    public String getEnabledState() {
        return enabledState;
    }

    public void setEnabledState(String state) {
        this.enabledState = state;
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

    public java.util.List<String> getArgs() {
        return args;
    }

    /**
     * Whether this was created as a CONSTRAINT TRIGGER. PostgreSQL records one in pg_constraint
     * as well as pg_trigger, and pg_get_triggerdef prints CREATE CONSTRAINT TRIGGER along with
     * the deferrability that is the point of the form; a definition without those words restores
     * an ordinary trigger that fires immediately.
     */
    public boolean isConstraintTrigger() {
        return constraintTrigger;
    }

    public void setConstraintTrigger(boolean constraintTrigger) {
        this.constraintTrigger = constraintTrigger;
    }

    /** The relation a constraint trigger's FROM clause named, which tgconstrrelid points at. */
    public String getConstraintRelation() {
        return constraintRelation;
    }

    public void setConstraintRelation(String constraintRelation) {
        this.constraintRelation = constraintRelation;
    }

    /** The partitioned relation this is a copy of, or null when it was written where it sits. */
    public String getClonedFromTable() {
        return clonedFromTable;
    }

    public void setClonedFromTable(String clonedFromTable) {
        this.clonedFromTable = clonedFromTable;
    }
}
