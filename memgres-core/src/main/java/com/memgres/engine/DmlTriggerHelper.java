package com.memgres.engine;

import com.memgres.engine.plpgsql.PlpgsqlExecutor;

import java.util.*;

/**
 * Trigger execution helpers for DML operations.
 * Extracted from DmlExecutor to separate trigger concerns.
 */
class DmlTriggerHelper {

    private final AstExecutor executor;

    DmlTriggerHelper(AstExecutor executor) {
        this.executor = executor;
    }

    Object[] executeTriggers(List<PgTrigger> triggers, PgTrigger.Timing timing,
                             PgTrigger.Event event, Object[] newRow, Object[] oldRow, Table table) {
        return executeTriggers(triggers, timing, event, newRow, oldRow, table, null);
    }

    Object[] executeTriggers(List<PgTrigger> triggers, PgTrigger.Timing timing,
                             PgTrigger.Event event, Object[] newRow, Object[] oldRow, Table table,
                             Set<String> updatedColumns) {
        for (PgTrigger trigger : triggers) {
            if (trigger.getTiming() == timing && trigger.getEvent() == event && !trigger.isForEachStatement()) {
                // For UPDATE OF triggers, check if any of the updated columns match
                if (event == PgTrigger.Event.UPDATE && trigger.getUpdateColumns() != null
                        && updatedColumns != null) {
                    boolean matches = false;
                    for (String col : trigger.getUpdateColumns()) {
                        if (updatedColumns.contains(col.toLowerCase())) {
                            matches = true;
                            break;
                        }
                    }
                    if (!matches) continue;
                }
                PgFunction function = executor.database.getFunction(trigger.getFunctionName());
                if (function != null) {
                    PlpgsqlExecutor plExec = new PlpgsqlExecutor(executor, executor.database, executor.session);
                    newRow = plExec.executeTriggerFunction(function, newRow, oldRow, table, trigger);
                }
            }
        }
        return newRow;
    }

    /**
     * Fire statement-level triggers. Creates temporary transition tables if defined.
     */
    void fireStatementTriggers(List<PgTrigger> triggers, PgTrigger.Timing timing,
                               PgTrigger.Event event, Table table,
                               List<Object[]> newRows, List<Object[]> oldRows) {
        for (PgTrigger trigger : triggers) {
            if (trigger.getTiming() == timing && trigger.getEvent() == event && trigger.isForEachStatement()) {
                PgFunction function = executor.database.getFunction(trigger.getFunctionName());
                if (function == null) continue;

                // Create transition tables if specified
                String newTransName = trigger.getNewTransitionTable();
                String oldTransName = trigger.getOldTransitionTable();
                String schemaName = executor.defaultSchema();
                try {
                    if (newTransName != null && newRows != null) {
                        createTransitionTable(newTransName, schemaName, table, newRows);
                    }
                    if (oldTransName != null && oldRows != null) {
                        createTransitionTable(oldTransName, schemaName, table, oldRows);
                    }
                    PlpgsqlExecutor plExec = new PlpgsqlExecutor(executor, executor.database, executor.session);
                    plExec.executeTriggerFunction(function, null, null, table, trigger);
                } finally {
                    // Clean up transition tables
                    if (newTransName != null) {
                        Schema schema = executor.database.getSchema(schemaName);
                        if (schema != null) schema.removeTable(newTransName);
                    }
                    if (oldTransName != null) {
                        Schema schema = executor.database.getSchema(schemaName);
                        if (schema != null) schema.removeTable(oldTransName);
                    }
                }
            }
        }
    }

    private void createTransitionTable(String name, String schemaName, Table sourceTable, List<Object[]> rows) {
        List<Column> cols = new ArrayList<>();
        for (Column c : sourceTable.getColumns()) {
            cols.add(new Column(c.getName(), c.getType(), c.isNullable(), c.isPrimaryKey(), null,
                    c.getEnumTypeName(), c.getPrecision(), c.getScale()));
        }
        Table transTable = new Table(name, cols);
        for (Object[] row : rows) {
            transTable.insertRow(Arrays.copyOf(row, row.length));
        }
        Schema schema = executor.database.getSchema(schemaName);
        if (schema != null) {
            schema.addTable(transTable);
        }
    }
}
