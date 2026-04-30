package com.memgres.engine;

import com.memgres.engine.plpgsql.PlpgsqlExecutor;

import java.util.*;
import com.memgres.engine.parser.ast.Expression;

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
            if (trigger.isDisabled()) continue;
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
                // Evaluate WHEN clause if present
                if (trigger.getWhenClause() != null && !trigger.getWhenClause().isEmpty()) {
                    try {
                        com.memgres.engine.parser.ast.Expression whenExpr =
                                com.memgres.engine.parser.Parser.parseExpression(trigger.getWhenClause());
                        // Build a row context with NEW/OLD references
                        RowContext ctx = newRow != null ? new RowContext(table, "new", newRow) : null;
                        if (oldRow != null && ctx != null) {
                            ctx = ctx.merge(new RowContext(table, "old", oldRow));
                        } else if (oldRow != null) {
                            ctx = new RowContext(table, "old", oldRow);
                        }
                        if (ctx == null) ctx = new RowContext(table, table.getName(), new Object[table.getColumns().size()]);
                        Object result = executor.evalExpr(whenExpr, ctx);
                        if (!executor.isTruthy(result)) continue;
                    } catch (Exception e) {
                        // If WHEN evaluation fails, skip this trigger
                        continue;
                    }
                }
                // Deferred constraint triggers: defer to commit
                if (trigger.isInitiallyDeferred() && timing == PgTrigger.Timing.AFTER
                        && executor.session != null && executor.session.isInTransaction()) {
                    final Object[] capturedNew = newRow != null ? Arrays.copyOf(newRow, newRow.length) : null;
                    final Object[] capturedOld = oldRow != null ? Arrays.copyOf(oldRow, oldRow.length) : null;
                    final PgTrigger capturedTrigger = trigger;
                    executor.session.addDeferredTrigger(() -> {
                        PgFunction function = executor.database.getFunction(capturedTrigger.getFunctionName());
                        if (function != null) {
                            PlpgsqlExecutor plExec = new PlpgsqlExecutor(executor, executor.database, executor.session);
                            plExec.executeTriggerFunction(function, capturedNew, capturedOld, table, capturedTrigger);
                        }
                    });
                    continue;
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
            if (trigger.isDisabled()) continue;
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
