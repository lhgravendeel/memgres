package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * CREATE TRIGGER name {BEFORE|AFTER} {INSERT|UPDATE|DELETE} [OR ...]
 * ON [schema.]table FOR EACH ROW [WHEN (condition)] EXECUTE {FUNCTION|PROCEDURE} funcname()
 */
public final class CreateTriggerStmt implements Statement {
    public final String name;
    public final String timing;
    public final List<String> events;
    public final String table;
    public final String schema;
    public final String functionName;
    public final boolean orReplace;
    public final String whenClause;
    public final List<String> updateOfColumns;
    public final String newTransitionTable;
    public final String oldTransitionTable;
    public final boolean forEachStatement;
    public final boolean deferrable;
    public final boolean initiallyDeferred;

    public CreateTriggerStmt(
            String name,
            String timing,
            List<String> events,
            String table,
            String schema,
            String functionName,
            boolean orReplace,
            String whenClause,
            List<String> updateOfColumns,
            String newTransitionTable,
            String oldTransitionTable,
            boolean forEachStatement
    ) {
        this(name, timing, events, table, schema, functionName, orReplace, whenClause,
                updateOfColumns, newTransitionTable, oldTransitionTable, forEachStatement, false, false);
    }

    public CreateTriggerStmt(
            String name,
            String timing,
            List<String> events,
            String table,
            String schema,
            String functionName,
            boolean orReplace,
            String whenClause,
            List<String> updateOfColumns,
            String newTransitionTable,
            String oldTransitionTable,
            boolean forEachStatement,
            boolean deferrable,
            boolean initiallyDeferred
    ) {
        this.name = name;
        this.timing = timing;
        this.events = events;
        this.table = table;
        this.schema = schema;
        this.functionName = functionName;
        this.orReplace = orReplace;
        this.whenClause = whenClause;
        this.updateOfColumns = updateOfColumns;
        this.newTransitionTable = newTransitionTable;
        this.oldTransitionTable = oldTransitionTable;
        this.forEachStatement = forEachStatement;
        this.deferrable = deferrable;
        this.initiallyDeferred = initiallyDeferred;
    }

    /** Backward-compatible constructor without whenClause. */
    public CreateTriggerStmt(String name, String timing, List<String> events,
                             String table, String schema, String functionName, boolean orReplace) {
        this(name, timing, events, table, schema, functionName, orReplace, null, null, null, null, false);
    }

    /** Backward-compatible constructor with whenClause but without new fields. */
    public CreateTriggerStmt(String name, String timing, List<String> events,
                             String table, String schema, String functionName, boolean orReplace,
                             String whenClause) {
        this(name, timing, events, table, schema, functionName, orReplace, whenClause, null, null, null, false);
    }

    public String name() { return name; }
    public String timing() { return timing; }
    public List<String> events() { return events; }
    public String table() { return table; }
    public String schema() { return schema; }
    public String functionName() { return functionName; }
    public boolean orReplace() { return orReplace; }
    public String whenClause() { return whenClause; }
    public List<String> updateOfColumns() { return updateOfColumns; }
    public String newTransitionTable() { return newTransitionTable; }
    public String oldTransitionTable() { return oldTransitionTable; }
    public boolean forEachStatement() { return forEachStatement; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateTriggerStmt that = (CreateTriggerStmt) o;
        return java.util.Objects.equals(name, that.name)
            && java.util.Objects.equals(timing, that.timing)
            && java.util.Objects.equals(events, that.events)
            && java.util.Objects.equals(table, that.table)
            && java.util.Objects.equals(schema, that.schema)
            && java.util.Objects.equals(functionName, that.functionName)
            && orReplace == that.orReplace
            && java.util.Objects.equals(whenClause, that.whenClause)
            && java.util.Objects.equals(updateOfColumns, that.updateOfColumns)
            && java.util.Objects.equals(newTransitionTable, that.newTransitionTable)
            && java.util.Objects.equals(oldTransitionTable, that.oldTransitionTable)
            && java.util.Objects.equals(forEachStatement, that.forEachStatement);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, timing, events, table, schema, functionName, orReplace, whenClause, updateOfColumns, newTransitionTable, oldTransitionTable, forEachStatement);
    }

    @Override
    public String toString() {
        return "CreateTriggerStmt[name=" + name + ", " + "timing=" + timing + ", " + "events=" + events + ", " + "table=" + table + ", " + "schema=" + schema + ", " + "functionName=" + functionName + ", " + "orReplace=" + orReplace + ", " + "whenClause=" + whenClause + ", " + "updateOfColumns=" + updateOfColumns + ", " + "newTransitionTable=" + newTransitionTable + ", " + "oldTransitionTable=" + oldTransitionTable + ", " + "forEachStatement=" + forEachStatement + "]";
    }
}
