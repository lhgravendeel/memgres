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

    /** True while the session is replaying replicated changes rather than originating them. */
    static boolean inReplicaRole(AstExecutor executor) {
        if (executor.session == null) return false;
        String role = executor.session.getGucSettings().get("session_replication_role");
        return role != null && role.equalsIgnoreCase("replica");
    }

    Object[] executeTriggers(List<PgTrigger> triggers, PgTrigger.Timing timing,
                             PgTrigger.Event event, Object[] newRow, Object[] oldRow, Table table) {
        return executeTriggers(triggers, timing, event, newRow, oldRow, table, null);
    }

    Object[] executeTriggers(List<PgTrigger> triggers, PgTrigger.Timing timing,
                             PgTrigger.Event event, Object[] newRow, Object[] oldRow, Table table,
                             Set<String> updatedColumns) {
        boolean replicaRole = inReplicaRole(executor);
        for (PgTrigger trigger : triggers) {
            if (!trigger.firesUnderReplicationRole(replicaRole)) continue;
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
                    com.memgres.engine.parser.ast.Expression whenExpr;
                    try {
                        whenExpr = com.memgres.engine.parser.Parser.parseExpression(trigger.getWhenClause());
                    } catch (RuntimeException unparsable) {
                        continue; // nothing to evaluate; the definition is reported elsewhere
                    }
                    // Build a row context with NEW/OLD references
                    RowContext ctx = newRow != null ? new RowContext(table, "new", newRow) : null;
                    if (oldRow != null && ctx != null) {
                        ctx = ctx.merge(new RowContext(table, "old", oldRow));
                    } else if (oldRow != null) {
                        ctx = new RowContext(table, "old", oldRow);
                    }
                    if (ctx == null) ctx = new RowContext(table, table.getName(), new Object[table.getColumns().size()]);
                    // An error raised while deciding whether the trigger fires belongs to the
                    // statement that raised it. Swallowing it silently skips the trigger and
                    // lets a row through that a division by zero should have stopped.
                    Object result = executor.evalExpr(whenExpr, ctx);
                    if (!executor.isTruthy(result)) continue;
                }
                // A deferred constraint trigger fires at the end of the transaction, and outside an
                // explicit one that is the end of the statement's own implicit transaction -- so it
                // still runs after every immediate AFTER trigger of the same statement rather than
                // where it happens to be registered. See Session.runEndOfStatementDeferredChecks.
                if (trigger.isInitiallyDeferred() && timing == PgTrigger.Timing.AFTER
                        && executor.session != null) {
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
                    Object[] result = plExec.executeTriggerFunction(function, newRow, oldRow, table, trigger);
                    if (result == null) {
                        // BEFORE/INSTEAD OF row trigger returned NULL: skip the operation on
                        // this row and suppress any remaining triggers (PostgreSQL semantics)
                        return null;
                    }
                    // A DELETE has no NEW row to hand on to the next trigger; what the function
                    // returned says only whether the row still goes.
                    if (event != PgTrigger.Event.DELETE) newRow = result;
                }
            }
        }
        // A DELETE answers with the row it was asked about: there is no NEW row for it to report,
        // and returning null would read as "skip this row".
        return event == PgTrigger.Event.DELETE ? oldRow : newRow;
    }

    /**
     * What a referential action owes one referencing table for the statement now running.
     *
     * <p>PostgreSQL carries out ON DELETE / ON UPDATE CASCADE, SET NULL and SET DEFAULT as a
     * statement against the referencing table, so that table's FOR EACH STATEMENT triggers fire
     * for it: its BEFORE ones when the action is first reached and its AFTER ones once the whole
     * statement is over. They fire once however many parent rows the action was reached for, and
     * they fire even when no row of the referencing table matched -- which is why the record is
     * the statement's and not the parent row's.
     */
    static final class ReferentialStatement {
        private final boolean collecting;
        final List<Object[]> newRows = new ArrayList<Object[]>();
        final List<Object[]> oldRows = new ArrayList<Object[]>();

        ReferentialStatement(boolean collecting) {
            this.collecting = collecting;
        }

        /**
         * Note a row the action wrote. Only a transition table reads these back, so nothing is
         * copied when no AFTER trigger of this statement declared one.
         */
        void wrote(Object[] newRow, Object[] oldRow) {
            if (!collecting) return;
            if (newRow != null) newRows.add(Arrays.copyOf(newRow, newRow.length));
            if (oldRow != null) oldRows.add(Arrays.copyOf(oldRow, oldRow.length));
        }

        /** The same for the rows the statement wrote to that relation itself: one transition table
         * holds every row the statement left behind on it, whatever wrote them. */
        void alsoWrote(List<Object[]> written, List<Object[]> replaced) {
            if (!collecting) return;
            if (written != null) {
                for (Object[] row : written) newRows.add(Arrays.copyOf(row, row.length));
            }
            if (replaced != null) {
                for (Object[] row : replaced) oldRows.add(Arrays.copyOf(row, row.length));
            }
        }
    }

    /**
     * Fire the referencing table's BEFORE statement-level triggers for a referential action and
     * queue its AFTER ones for the end of the statement, the first time this statement reaches
     * that table for that event.
     *
     * @return the record to note the written rows on, or null when the statement has already
     *     fired that relation's statement-level triggers itself
     */
    ReferentialStatement referentialStatement(Table child, PgTrigger.Event event) {
        final Session session = executor.session;
        if (session == null || child == null) return null;
        if (session.statementTriggersFired(child, event)) {
            return session.referentialStatement(child, event);
        }
        final String named = child.getName();
        List<PgTrigger> triggers = executor.database.getTriggersForTable(named);
        boolean anyAfter = false;
        boolean collecting = false;
        for (PgTrigger trigger : triggers) {
            if (!trigger.isForEachStatement() || trigger.getEvent() != event) continue;
            if (trigger.getTiming() != PgTrigger.Timing.AFTER) continue;
            anyAfter = true;
            if (trigger.getNewTransitionTable() != null || trigger.getOldTransitionTable() != null) {
                collecting = true;
            }
        }
        final ReferentialStatement acting = new ReferentialStatement(collecting);
        session.recordStatementTriggers(child, event, acting);
        runStatementTriggers(triggers, PgTrigger.Timing.BEFORE, event, child, null, null);
        if (anyAfter) {
            session.addEndOfStatementTrigger(() -> runStatementTriggers(
                    executor.database.getTriggersForTable(named), PgTrigger.Timing.AFTER, event,
                    child, event == PgTrigger.Event.DELETE ? null : acting.newRows,
                    event == PgTrigger.Event.INSERT ? null : acting.oldRows));
        }
        return acting;
    }

    /**
     * Fire statement-level triggers. Creates temporary transition tables if defined.
     *
     * <p>A relation's FOR EACH STATEMENT triggers fire once for the statement. When a referential
     * action has already fired them -- which is what a table referencing itself with ON DELETE
     * CASCADE arranges -- the statement's own target does not fire them a second time, and the
     * AFTER half is the one the action queued for the end of the statement.
     */
    void fireStatementTriggers(List<PgTrigger> triggers, PgTrigger.Timing timing,
                               PgTrigger.Event event, Table table,
                               List<Object[]> newRows, List<Object[]> oldRows) {
        if (executor.session != null && table != null) {
            ReferentialStatement acting = executor.session.referentialStatement(table, event);
            if (acting != null) {
                if (timing == PgTrigger.Timing.AFTER) acting.alsoWrote(newRows, oldRows);
                return;
            }
            if (timing == PgTrigger.Timing.BEFORE) {
                executor.session.recordStatementTriggers(table, event, null);
            }
        }
        runStatementTriggers(triggers, timing, event, table, newRows, oldRows);
    }

    private void runStatementTriggers(List<PgTrigger> triggers, PgTrigger.Timing timing,
                                      PgTrigger.Event event, Table table,
                                      List<Object[]> newRows, List<Object[]> oldRows) {
        boolean replicaRole = inReplicaRole(executor);
        for (PgTrigger trigger : triggers) {
            if (!trigger.firesUnderReplicationRole(replicaRole)) continue;
            if (trigger.getTiming() == timing && trigger.getEvent() == event && trigger.isForEachStatement()) {
                PgFunction function = executor.database.getFunction(trigger.getFunctionName());
                if (function == null) continue;

                // Create transition tables if specified
                String newTransName = trigger.getNewTransitionTable();
                String oldTransName = trigger.getOldTransitionTable();
                String schemaName = executor.defaultSchema();
                // A transition name may collide with a real table. PG scopes transition
                // tables to the statement, so shadow the real one and put it back after —
                // never destroy it.
                Schema transScope = executor.database.getSchema(schemaName);
                Table shadowedNew = null;
                Table shadowedOld = null;
                try {
                    if (newTransName != null && newRows != null) {
                        if (transScope != null) shadowedNew = transScope.getTable(newTransName);
                        createTransitionTable(newTransName, schemaName, table, newRows);
                    }
                    if (oldTransName != null && oldRows != null) {
                        if (transScope != null) shadowedOld = transScope.getTable(oldTransName);
                        createTransitionTable(oldTransName, schemaName, table, oldRows);
                    }
                    PlpgsqlExecutor plExec = new PlpgsqlExecutor(executor, executor.database, executor.session);
                    plExec.executeTriggerFunction(function, null, null, table, trigger);
                } finally {
                    // Clean up transition tables and their ownership records, restoring any
                    // real table the transition name shadowed.
                    if (newTransName != null) {
                        Schema schema = executor.database.getSchema(schemaName);
                        if (schema != null) {
                            schema.removeTable(newTransName);
                            if (shadowedNew != null) schema.addTable(shadowedNew);
                        }
                        if (shadowedNew == null) {
                            executor.database.removeObjectOwner("table:" + schemaName.toLowerCase() + "." + newTransName.toLowerCase());
                        }
                    }
                    if (oldTransName != null) {
                        Schema schema = executor.database.getSchema(schemaName);
                        if (schema != null) {
                            schema.removeTable(oldTransName);
                            if (shadowedOld != null) schema.addTable(shadowedOld);
                        }
                        if (shadowedOld == null) {
                            executor.database.removeObjectOwner("table:" + schemaName.toLowerCase() + "." + oldTransName.toLowerCase());
                        }
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
            // Register ownership so privilege checks pass when trigger function queries the transition table
            String ownerKey = "table:" + schemaName.toLowerCase() + "." + name.toLowerCase();
            String role = executor.currentRole();
            if (role == null) role = executor.sessionUser();
            if (role != null) executor.database.setObjectOwner(ownerKey, role);
        }
    }
}
