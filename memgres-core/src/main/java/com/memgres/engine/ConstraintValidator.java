package com.memgres.engine;

import com.memgres.engine.parser.ast.*;
import com.memgres.engine.util.Strs;

import java.util.*;

/**
 * Handles constraint validation (PK, UNIQUE, CHECK, FK, EXCLUDE) and FK cascade actions.
 * Extracted from AstExecutor to separate constraint concerns from DML execution.
 */
class ConstraintValidator {

    private final AstExecutor executor;
    private final DmlTriggerHelper triggerHelper;

    ConstraintValidator(AstExecutor executor) {
        this.executor = executor;
        this.triggerHelper = new DmlTriggerHelper(executor);
    }

    /** Find the schema name that contains the given table. */
    /** A conflicting row that another session has inserted but has not yet committed. */
    static final class PendingUniqueConflict {
        final Session owner;
        final Object[] row;
        final String relation;

        PendingUniqueConflict(Session owner, Object[] row, String relation) {
            this.owner = owner;
            this.row = row;
            this.relation = relation;
        }
    }

    /**
     * Look for a row that would collide on a unique or primary key and that another session has
     * inserted without committing. Whether such a row is really a duplicate is not yet decided —
     * it depends on that transaction committing — so the caller waits rather than reporting.
     *
     * <p>Only plain column constraints are considered. A partial or expression constraint falls
     * through to the ordinary check, which reports immediately as before; that is the older
     * behaviour rather than a new failure.
     *
     * @return the pending conflict, or null if there is none to wait for
     */
    PendingUniqueConflict findUncommittedUniqueConflict(Table table, Object[] newRow, Object[] excludeRow) {
        if (executor.session == null || executor.database == null) return null;
        String schema = findSchemaName(table);
        String key = (schema != null ? schema : "public") + "." + table.getName();
        Map<Object[], Session> pending = new java.util.IdentityHashMap<>();
        for (Session other : executor.database.getActiveSessions()) {
            if (other == executor.session) continue;
            // A transaction that has already failed will never make its row permanent, so there
            // is nothing to wait for. Waiting anyway is how two sessions that broke each other's
            // statement end up waiting for one another with no way out.
            if (other.isDoomed()) continue;
            java.util.Set<Object[]> theirs = other.getUncommittedInserts(key);
            if (theirs == null) continue;
            for (Object[] r : theirs) {
                pending.put(r, other);
            }
        }
        if (pending.isEmpty()) return null;

        for (StoredConstraint sc : table.getConstraints()) {
            if (sc.isNotEnforced()) continue;
            if (sc.getType() == StoredConstraint.Type.EXCLUDE) {
                // An exclusion constraint is broken by a pair of rows just as a unique one is, so
                // a row another session has written and not committed is no more decidable here
                // than there: whether it conflicts depends on that transaction committing.
                PendingUniqueConflict excluded =
                        pendingExcludeConflict(table, newRow, excludeRow, sc, pending);
                if (excluded != null) return excluded;
                continue;
            }
            if (sc.getType() != StoredConstraint.Type.PRIMARY_KEY
                    && sc.getType() != StoredConstraint.Type.UNIQUE) {
                continue;
            }
            if (sc.getWhereExpr() != null) continue; // partial: left to the ordinary check
            List<String> columns = sc.getColumns();
            if (columns == null || columns.isEmpty()) continue;
            int[] indices = new int[columns.size()];
            Object[] newVals = new Object[columns.size()];
            boolean usable = true;
            for (int i = 0; i < columns.size(); i++) {
                indices[i] = table.getColumnIndex(columns.get(i));
                if (indices[i] < 0) { usable = false; break; }
                newVals[i] = newRow[indices[i]];
            }
            if (!usable) continue;
            // NULLs are distinct unless the constraint says otherwise, so they never collide.
            if (!sc.isNullsNotDistinct()) {
                boolean anyNull = false;
                for (Object v : newVals) { if (v == null) { anyNull = true; break; } }
                if (anyNull) continue;
            }
            for (Map.Entry<Object[], Session> entry : pending.entrySet()) {
                Object[] candidate = entry.getKey();
                if (candidate == excludeRow) continue;
                boolean allMatch = true;
                for (int i = 0; i < indices.length; i++) {
                    if (indices[i] >= candidate.length
                            || !valuesEqual(newVals[i], candidate[indices[i]])) {
                        allMatch = false;
                        break;
                    }
                }
                if (allMatch) {
                    return new PendingUniqueConflict(entry.getValue(), candidate, table.getName());
                }
            }
        }
        return null;
    }

    /**
     * The same question for an exclusion constraint: is one of the rows another session has
     * written and not committed in conflict with the row being written?
     *
     * <p>The operators are the constraint's own rather than plain equality, because that is what
     * decides an exclusion conflict; a NULL on either side takes part in nothing, exactly as it
     * does in the ordinary scan.
     */
    private PendingUniqueConflict pendingExcludeConflict(Table table, Object[] newRow,
                                                         Object[] excludeRow, StoredConstraint sc,
                                                         Map<Object[], Session> pending) {
        List<StoredConstraint.ExcludeElement> elements = sc.getExcludeElements();
        if (elements == null || elements.isEmpty()) return null;
        int[] colIndices = new int[elements.size()];
        for (int i = 0; i < elements.size(); i++) {
            colIndices[i] = table.getColumnIndex(elements.get(i).column());
            if (colIndices[i] < 0) return null;
        }
        for (Map.Entry<Object[], Session> entry : pending.entrySet()) {
            Object[] candidate = entry.getKey();
            if (candidate == excludeRow) continue;
            boolean allMatch = true;
            for (int i = 0; i < elements.size(); i++) {
                Object newVal = colIndices[i] < newRow.length ? newRow[colIndices[i]] : null;
                Object existVal = colIndices[i] < candidate.length ? candidate[colIndices[i]] : null;
                if (newVal == null || existVal == null) { allMatch = false; break; }
                if (!excludeOpMatches(elements.get(i).operator(), newVal, existVal)) {
                    allMatch = false;
                    break;
                }
            }
            if (allMatch) {
                return new PendingUniqueConflict(entry.getValue(), candidate, table.getName());
            }
        }
        return null;
    }

    /**
     * Rows another session inserted inside a transaction that has already failed.
     *
     * <p>PostgreSQL marks such a row dead the instant the statement errors, so its key is free
     * again: an INSERT that would collide with it succeeds rather than reporting 23505 for a row
     * nobody will ever be able to read. Returning them here lets the uniqueness scan skip them.
     * The set is empty in every ordinary case, and is only built when a failed transaction with
     * uncommitted rows really is open, so the common path pays a session walk and nothing else.
     */
    java.util.Set<Object[]> deadUncommittedRows(Table table) {
        if (executor.session == null || executor.database == null) {
            return java.util.Collections.emptySet();
        }
        java.util.Set<Object[]> dead = null;
        String key = null;
        for (Session other : executor.database.getActiveSessions()) {
            if (other == executor.session || !other.isDoomed()) continue;
            if (key == null) key = uncommittedKey(table);
            java.util.Set<Object[]> theirs = other.getUncommittedInserts(key);
            if (theirs == null || theirs.isEmpty()) continue;
            if (dead == null) {
                dead = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Object[], Boolean>());
            }
            dead.addAll(theirs);
        }
        return dead == null ? java.util.Collections.<Object[]>emptySet() : dead;
    }

    /** True while the row is still an uncommitted insert belonging to that session. */
    static boolean isStillPending(PendingUniqueConflict conflict, String schemaTable) {
        java.util.Set<Object[]> theirs = conflict.owner.getUncommittedInserts(schemaTable);
        if (theirs == null) return false;
        for (Object[] candidate : theirs) {
            if (candidate == conflict.row) return true;
        }
        return false;
    }

    /** The schema-qualified key under which a table's uncommitted rows are recorded. */
    String uncommittedKey(Table table) {
        String schema = findSchemaName(table);
        return (schema != null ? schema : "public") + "." + table.getName();
    }

    private String findSchemaName(Table table) {
        if (executor.database == null) return null;
        for (Map.Entry<String, Schema> entry : executor.database.getSchemas().entrySet()) {
            if (entry.getValue().getTable(table.getName()) == table) {
                return entry.getKey();
            }
        }
        return null;
    }

    /** Check if a FK constraint's referenced table matches the given parent table (name + schema). */
    private boolean fkReferencesTable(StoredConstraint sc, Table parentTable, String parentSchemaName) {
        if (!sc.getReferencesTable().equalsIgnoreCase(parentTable.getName())) return false;
        if (sc.getReferencesSchema() != null) {
            // Schema-qualified FK — must match the specific schema
            return sc.getReferencesSchema().equalsIgnoreCase(parentSchemaName);
        }
        return true; // unqualified FK — name match is sufficient
    }

    void validateConstraints(Table table, Object[] row, Object[] excludeRow) {
        validateConstraints(table, row, excludeRow, null);
    }

    /**
     * @param decided a constraint the caller has already settled for this row, or null. A
     *     referential action checks its own key against the referenced table as that table will be
     *     once the statement is over; checking it again here, without knowing which rows are going
     *     away, reports a key that nothing was going to reference.
     */
    void validateConstraints(Table table, Object[] row, Object[] excludeRow, StoredConstraint decided) {
        // 1. NOT NULL enforcement
        for (int i = 0; i < table.getColumns().size(); i++) {
            Column col = table.getColumns().get(i);
            if (!col.isNullable() && row[i] == null) {
                // PostgreSQL names the relation as well as the column: the same column name
                // appears on a parent and its children, and only the relation tells them apart.
                MemgresException ex = new MemgresException(
                        "null value in column \"" + col.getName() + "\" of relation \""
                        + table.getName() + "\" violates not-null constraint",
                        "23502");
                ex.setColumn(col.getName());
                ex.setTable(table.getName());
                String schema = findSchemaName(table);
                if (schema != null) ex.setSchema(schema);
                ex.setDetail("Failing row contains (" + formatRow(table, row) + ").");
                throw ex;
            }
        }

        // 2. Constraint checks
        for (StoredConstraint sc : table.getConstraints()) {
            // PG 18: NOT ENFORCED constraints are stored but not validated
            if (sc.isNotEnforced()) continue;
            if (sc == decided) continue;

            // Deferred constraint handling: defer to COMMIT if currently deferred
            boolean shouldDefer = checkIsCurrentlyDeferred(sc);

            switch (sc.getType()) {
                case PRIMARY_KEY:
                    if (shouldDefer) {
                        executor.session.addDeferredCheck(table, row, sc);
                    } else {
                        validateUniqueness(table, row, sc.getColumns(), excludeRow, true, sc.getName(), false, null, null);
                    }
                    break;
                case UNIQUE:
                    if (shouldDefer) {
                        executor.session.addDeferredCheck(table, row, sc);
                    } else {
                        validateUniqueness(table, row, sc.getColumns(), excludeRow, false, sc.getName(), sc.isNullsNotDistinct(), sc.getWhereExpr(), sc.getExpressionColumns());
                    }
                    break;
                case CHECK:
                    if (shouldDefer) {
                        executor.session.addDeferredCheck(table, row, sc);
                    } else {
                        validateCheck(table, row, sc);
                    }
                    break;
                case FOREIGN_KEY: {
                    if (shouldDefer) {
                        executor.session.addDeferredCheck(table, row, sc);
                    } else {
                        validateForeignKey(table, row, sc);
                    }
                    break;
                }
                case EXCLUDE:
                    if (shouldDefer) {
                        executor.session.addDeferredCheck(table, row, sc);
                    } else {
                        validateExclude(table, row, sc, excludeRow);
                    }
                    break;
            }
        }
    }

    private void validateUniqueness(Table table, Object[] newRow, List<String> columns,
                                    Object[] excludeRow, boolean isPK, String constraintName,
                                    boolean nullsNotDistinct, com.memgres.engine.parser.ast.Expression whereExpr,
                                    java.util.List<com.memgres.engine.parser.ast.Expression> exprColumns) {
        // Compute virtual column values so uniqueness checks work on virtual columns (lenient: suppress errors)
        boolean hasVirtual = executor.dmlExecutor.hasVirtualColumns(table);
        if (hasVirtual) {
            newRow = executor.dmlExecutor.computeVirtualColumns(table, newRow, false);
        }
        // Rows a failed transaction wrote are dead: they hold no key any more.
        final java.util.Set<Object[]> dead = deadUncommittedRows(table);
        // For partial unique indexes, check if the new row satisfies the WHERE predicate
        if (whereExpr != null) {
            RowContext newCtx = new RowContext(table, null, newRow);
            Object whereResult = executor.evalExpr(whereExpr, newCtx);
            if (!(whereResult instanceof Boolean && ((Boolean) whereResult))) {
                return; // New row doesn't satisfy predicate, no uniqueness check needed
            }
        }

        // For expression-based indexes, evaluate expressions instead of looking up column indices
        if (exprColumns != null && !exprColumns.isEmpty()) {
            RowContext newCtx = new RowContext(table, null, newRow);
            Object[] newVals = new Object[exprColumns.size()];
            for (int i = 0; i < exprColumns.size(); i++) {
                newVals[i] = executor.evalExpr(exprColumns.get(i), newCtx);
            }

            // NULL handling
            if (!isPK && !nullsNotDistinct) {
                for (Object val : newVals) {
                    if (val == null) return;
                }
            }

            for (Object[] existingRow : table.getRows()) {
                if (excludeRow != null && existingRow == excludeRow) continue;
                if (dead.contains(existingRow)) continue;

                Object[] evalExisting = hasVirtual ? executor.dmlExecutor.computeVirtualColumns(table, existingRow, false) : existingRow;

                if (whereExpr != null) {
                    RowContext existingCtx = new RowContext(table, null, evalExisting);
                    Object existingResult = executor.evalExpr(whereExpr, existingCtx);
                    if (!(existingResult instanceof Boolean && ((Boolean) existingResult))) {
                        continue;
                    }
                }

                RowContext existingCtx = new RowContext(table, null, evalExisting);
                boolean allMatch = true;
                for (int i = 0; i < exprColumns.size(); i++) {
                    Object existingVal = executor.evalExpr(exprColumns.get(i), existingCtx);
                    if (!valuesEqual(newVals[i], existingVal)) {
                        allMatch = false;
                        break;
                    }
                }
                if (allMatch) {
                    MemgresException ex = new MemgresException(
                            "duplicate key value violates unique constraint \"" + constraintName + "\"",
                            "23505");
                    ex.setConstraint(constraintName);
                    ex.setTable(table.getName());
                    String schema = findSchemaName(table);
                    if (schema != null) ex.setSchema(schema);
                    ex.setDetail(buildKeyDetail(table, columns, newVals));
                    throw ex;
                }
            }
            return;
        }

        int[] colIndices = new int[columns.size()];
        Object[] newVals = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            colIndices[i] = table.getColumnIndex(columns.get(i));
            if (colIndices[i] < 0) return; // column not found, skip
            newVals[i] = newRow[colIndices[i]];
        }

        // PK columns must not be null
        if (isPK) {
            for (int i = 0; i < newVals.length; i++) {
                if (newVals[i] == null) {
                    throw new MemgresException(
                            "null value in column \"" + columns.get(i) + "\" of relation \""
                            + table.getName() + "\" violates not-null constraint",
                            "23502");
                }
            }
        }

        // NULL values in UNIQUE columns: by default, NULLs are distinct (don't conflict)
        // With NULLS NOT DISTINCT, NULLs are treated as equal and conflict
        if (!isPK && !nullsNotDistinct) {
            for (Object val : newVals) {
                if (val == null) return; // NULL is always unique (standard behavior)
            }
        }

        // Try O(1) index lookup for simple (non-partial, non-expression) constraints
        // Skip index fast path for NULLS NOT DISTINCT with NULL values because index treats NULLs as distinct
        boolean hasNull = false;
        for (Object val : newVals) { if (val == null) { hasNull = true; break; } }
        // The index cannot tell a dead row from a live one, so when one is present the scan below
        // does the work instead. That only happens while a failed transaction is still open.
        if (whereExpr == null && constraintName != null && !(nullsNotDistinct && hasNull) && dead.isEmpty()) {
            TableIndex idx = table.getIndex(constraintName);
            if (idx != null) {
                Object[] conflict = idx.findConflict(newRow, excludeRow);
                if (conflict != null) {
                    MemgresException ex = new MemgresException(
                            "duplicate key value violates unique constraint \"" + constraintName + "\"",
                            "23505");
                    ex.setConstraint(constraintName);
                    ex.setTable(table.getName());
                    String schema = findSchemaName(table);
                    if (schema != null) ex.setSchema(schema);
                    ex.setDetail(buildKeyDetail(table, columns, newVals));
                    throw ex;
                }
                return;
            }
        }

        for (Object[] existingRow : table.getRows()) {
            if (excludeRow != null && existingRow == excludeRow) continue;
            if (dead.contains(existingRow)) continue;

            Object[] evalExisting = hasVirtual ? executor.dmlExecutor.computeVirtualColumns(table, existingRow, false) : existingRow;

            // For partial unique indexes, only check rows that satisfy the WHERE predicate
            if (whereExpr != null) {
                RowContext existingCtx = new RowContext(table, null, evalExisting);
                Object existingResult = executor.evalExpr(whereExpr, existingCtx);
                if (!(existingResult instanceof Boolean && ((Boolean) existingResult))) {
                    continue; // Existing row doesn't satisfy predicate, skip
                }
            }

            boolean allMatch = true;
            for (int i = 0; i < colIndices.length; i++) {
                Object existingVal = evalExisting[colIndices[i]];
                if (!valuesEqual(newVals[i], existingVal)) {
                    allMatch = false;
                    break;
                }
            }
            if (allMatch) {
                MemgresException ex = new MemgresException(
                        "duplicate key value violates unique constraint \"" + constraintName + "\"",
                        "23505");
                ex.setConstraint(constraintName);
                ex.setTable(table.getName());
                String schema = findSchemaName(table);
                if (schema != null) ex.setSchema(schema);
                ex.setDetail(buildKeyDetail(table, columns, newVals));
                throw ex;
            }
        }
    }

    /**
     * The DETAIL of a duplicate key: <em>Key (col1, col2)=(val1, val2) already exists.</em> The key
     * is named as PostgreSQL names it, which is not always as it was written down -- a name needing
     * quotes gets them, and a key that is an expression is deparsed.
     */
    private String buildKeyDetail(Table table, List<String> columns, Object[] vals) {
        return IndexKeyDescription.alreadyExists(table, columns, vals);
    }

    private void validateCheck(Table table, Object[] row, StoredConstraint sc) {
        // Compute virtual column values so CHECK expressions can reference them
        Object[] evalRow = executor.dmlExecutor.hasVirtualColumns(table) ? executor.dmlExecutor.computeVirtualColumns(table, row, false) : row;
        RowContext ctx = new RowContext(table, null, evalRow);
        Object result = executor.evalExpr(sc.getCheckExpr(), ctx);
        if (result instanceof Boolean && !((Boolean) result)) {
            MemgresException ex = new MemgresException(
                    "new row for relation \"" + table.getName()
                            + "\" violates check constraint \"" + sc.getName() + "\"",
                    "23514");
            ex.setConstraint(sc.getName());
            ex.setTable(table.getName());
            String schema = findSchemaName(table);
            if (schema != null) ex.setSchema(schema);
            // The row itself is printed, as it is for a not-null violation: a statement writing
            // many rows at once otherwise says only that one of them was refused. The computed
            // row is the one shown, so a generated column appears with the value it was judged on.
            ex.setDetail("Failing row contains (" + formatRow(table, evalRow) + ").");
            throw ex;
        }
    }

    /** Package-visible for deferred constraint checking from Session.commit(). */
    void validateForeignKeyDeferred(Table table, Object[] row, StoredConstraint sc) {
        validateForeignKey(table, row, sc);
    }

    /**
     * True while this constraint's checks are being postponed for the current session.
     *
     * <p>An explicit transaction is not what makes a constraint deferred. PostgreSQL runs a
     * statement outside one in an implicit transaction of its own, and a DEFERRABLE INITIALLY
     * DEFERRED constraint is checked when that transaction commits -- once the statement is over,
     * with its AFTER triggers and its data-modifying WITH items done -- rather than as each row is
     * written. Asking for an explicit transaction here checked such a constraint row by row in
     * autocommit, so a statement that swaps two unique values was refused halfway through it.
     *
     * @see Session#runEndOfStatementDeferredChecks()
     */
    private boolean checkIsCurrentlyDeferred(StoredConstraint sc) {
        return sc.isDeferrable()
                && executor.session != null
                && executor.session.isConstraintCurrentlyDeferred(sc);
    }

    /**
     * The error a still-referenced parent row raises. RESTRICT and NO ACTION report it
     * differently: PostgreSQL calls RESTRICT out by name under its own SQLSTATE, because that
     * action refuses the write outright rather than merely finding the key unsatisfied.
     */
    private MemgresException referencedRowError(Table parentTable, Table childTable,
                                                StoredConstraint sc, List<String> refColNames,
                                                Object[] parentVals, String childSchemaName,
                                                boolean restrict) {
        // A constraint a partition inherited belongs to the partitioned table, and that is the
        // relation PostgreSQL names — not the partition the offending row happens to live in.
        // The DETAIL names it too, so the two halves of the error cannot disagree.
        String constrainedTable = sc.getInheritedFrom() != null
                ? sc.getInheritedFrom() : childTable.getName();
        StringBuilder detailSb = new StringBuilder("Key (");
        for (int i = 0; i < refColNames.size(); i++) {
            if (i > 0) detailSb.append(", ");
            detailSb.append(refColNames.get(i));
        }
        detailSb.append(")=(");
        for (int i = 0; i < parentVals.length; i++) {
            if (i > 0) detailSb.append(", ");
            detailSb.append(parentVals[i]);
        }
        detailSb.append(") is ").append(restrict ? "" : "still ")
                .append("referenced from table \"").append(constrainedTable).append("\".");
        MemgresException ex = new MemgresException(
                "update or delete on table \"" + parentTable.getName() + "\" violates "
                        + (restrict ? "RESTRICT setting of " : "")
                        + "foreign key constraint \"" + sc.getName()
                        + "\" on table \"" + constrainedTable + "\"",
                restrict ? "23001" : "23503");
        ex.setDetail(detailSb.toString());
        ex.setConstraint(sc.getName());
        ex.setTable(childTable.getName());
        if (childSchemaName != null) ex.setSchema(childSchemaName);
        return ex;
    }

    /** True when the row is still one of the table's live rows. */
    private static boolean rowIsLive(Table table, Object[] row) {
        for (Object[] candidate : table.getRows()) {
            if (candidate == row) return true;
        }
        return false;
    }

    /**
     * Re-check, at COMMIT or at SET CONSTRAINTS ... IMMEDIATE, a child row whose referenced row
     * was deleted or re-keyed earlier in the transaction. The transaction has had the chance to
     * put the key back or to move the child row, so the check is made against what the tables
     * hold now; only a child row that is still there and still points at a missing key fails.
     */
    void validateDeferredReferencedFk(Table parentTable, Table childTable, Object[] childRow,
                                      StoredConstraint sc) {
        if (!rowIsLive(childTable, childRow)) return;

        int[] childColIndices = new int[sc.getColumns().size()];
        Object[] childVals = new Object[childColIndices.length];
        for (int i = 0; i < childColIndices.length; i++) {
            childColIndices[i] = childTable.getColumnIndex(sc.getColumns().get(i));
            if (childColIndices[i] < 0) return;
            childVals[i] = childRow[childColIndices[i]];
            // A null in the key references nothing, so nothing is left dangling.
            if (childVals[i] == null) return;
        }

        try {
            validateForeignKey(childTable, childRow, sc);
            return;
        } catch (MemgresException e) {
            if (!"23503".equals(e.getSqlState())) throw e;
        }

        List<String> refColNames = sc.getReferencesColumns();
        if (refColNames.isEmpty()) {
            refColNames = findPrimaryKeyColumns(parentTable);
        }
        throw referencedRowError(parentTable, childTable, sc, refColNames, childVals,
                findSchemaName(childTable), false);
    }

    /** Validate a deferred constraint at commit time. Dispatches by constraint type. */
    void validateDeferredConstraint(Table table, Object[] row, StoredConstraint sc) {
        switch (sc.getType()) {
            case PRIMARY_KEY:
            case UNIQUE:
                // For PK/UNIQUE, validate the whole table for duplicates (handled separately via validateDeferredUniqueness)
                break;
            case CHECK:
                validateCheck(table, row, sc);
                break;
            case FOREIGN_KEY:
                validateForeignKey(table, row, sc);
                break;
            case EXCLUDE:
                validateExclude(table, row, sc, null);
                break;
        }
    }

    /**
     * Validate deferred PK/UNIQUE constraint by scanning the entire table for duplicates.
     * Called once per (table, constraint) pair at commit time.
     */
    void validateDeferredUniqueness(Table table, StoredConstraint sc) {
        boolean isPK = sc.getType() == StoredConstraint.Type.PRIMARY_KEY;
        List<String> columns = sc.getColumns();
        boolean nullsNotDistinct = sc.isNullsNotDistinct();

        int[] colIndices = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            colIndices[i] = table.getColumnIndex(columns.get(i));
            if (colIndices[i] < 0) return;
        }

        Set<TableIndex.IndexKey> seen = new HashSet<>();
        for (Object[] row : table.getRows()) {
            Object[] vals = new Object[colIndices.length];
            boolean hasNull = false;
            for (int i = 0; i < colIndices.length; i++) {
                vals[i] = TableIndex.normalize(row[colIndices[i]]);
                if (vals[i] == null) hasNull = true;
            }
            // NULLs are distinct by default (skip), unless NULLS NOT DISTINCT
            if (hasNull && !isPK && !nullsNotDistinct) continue;
            TableIndex.IndexKey key = new TableIndex.IndexKey(vals);
            if (!seen.add(key)) {
                MemgresException ex = new MemgresException(
                        "duplicate key value violates unique constraint \"" + sc.getName() + "\"",
                        "23505");
                ex.setConstraint(sc.getName());
                ex.setTable(table.getName());
                String schema = findSchemaName(table);
                if (schema != null) ex.setSchema(schema);
                ex.setDetail(buildKeyDetail(table, columns, vals));
                throw ex;
            }
        }
    }

    /**
     * The key an ON DELETE SET DEFAULT is about to write, checked against the referenced table as
     * it will be once the statement is over.
     *
     * <p>PostgreSQL has two complaints here and picks by which side is at fault. A default no row
     * ever held is the child's fault, reported as the row it would write: <em>insert or update on
     * table "c" violates ..., Key (pid)=(9) is not present in table "p"</em>. A default whose row
     * exists but is being deleted is the parent's fault -- the delete is what cannot go ahead --
     * and is reported from that side: <em>update or delete on table "p" violates ... on table "c",
     * Key (id)=(9) is still referenced from table "c"</em>.
     *
     * @param vanishing rows of the referenced table this statement is removing
     */
    private void checkDefaultKeyStillThere(Table childTable, Object[] newRow, StoredConstraint sc,
                                           java.util.Set<Object[]> vanishing, Table parentTable,
                                           List<String> refColNames) {
        // PostgreSQL names whichever side its own referential triggers reach first, and which
        // that is depends on the order the rows come up in: the same DELETE reports the child's
        // "is not present" for one arrangement of parent rows and the parent's "still referenced"
        // for another. The SQLSTATE is 23503 either way, so this keeps the child-side message
        // rather than model an ordering it cannot reproduce.
        validateForeignKey(childTable, newRow, sc, vanishing);
    }

    /** The columns a foreign key references, falling back to the referenced table's key. */
    private List<String> updateRefColNames(StoredConstraint sc, Table parentTable) {
        List<String> refColNames = sc.getReferencesColumns();
        if (refColNames == null || refColNames.isEmpty()) {
            refColNames = findPrimaryKeyColumns(parentTable);
        }
        return refColNames;
    }

    /** PostgreSQL's parent-side wording for a key a statement may not take away. */
    private MemgresException parentSideViolation(Table parentTable, Table childTable,
                                                 StoredConstraint sc, Object[] childRow,
                                                 List<String> refColNames) {
        // A partition's copy of an inherited key belongs to the partitioned table, and that is
        // the relation PostgreSQL names, in the message and in the detail alike.
        String constrainedTable = sc.getInheritedFrom() != null
                ? sc.getInheritedFrom() : childTable.getName();
        MemgresException ex = new MemgresException(
                "update or delete on table \"" + parentTable.getName()
                        + "\" violates foreign key constraint \"" + sc.getName()
                        + "\" on table \"" + constrainedTable + "\"", "23503");
        ex.setConstraint(sc.getName());
        ex.setTable(parentTable.getName());
        String schemaName = findSchemaName(parentTable);
        if (schemaName != null) ex.setSchema(schemaName);
        StringBuilder detail = new StringBuilder("Key (");
        for (int i = 0; i < refColNames.size(); i++) {
            if (i > 0) detail.append(", ");
            detail.append(refColNames.get(i));
        }
        detail.append(")=(");
        for (int i = 0; i < sc.getColumns().size(); i++) {
            if (i > 0) detail.append(", ");
            int idx = childTable.getColumnIndex(sc.getColumns().get(i));
            detail.append(idx >= 0 ? childRow[idx] : null);
        }
        detail.append(") is still referenced from table \"")
              .append(constrainedTable).append("\".");
        ex.setDetail(detail.toString());
        return ex;
    }

    private void validateForeignKey(Table table, Object[] row, StoredConstraint sc) {
        validateForeignKey(table, row, sc, null);
    }

    /**
     * @param vanishing rows of the referenced table that this statement is about to delete, and
     *     which therefore cannot satisfy the key even though they are still stored. Identity-based,
     *     and null when the caller is not deleting anything.
     */
    private void validateForeignKey(Table table, Object[] row, StoredConstraint sc,
                                    java.util.Set<Object[]> vanishing) {
        // Resolve the referenced table (schema-qualified when available)
        Table refTable;
        if (sc.getReferencesSchema() != null) {
            refTable = executor.resolveTable(sc.getReferencesSchema(), sc.getReferencesTable());
        } else {
            refTable = executor.resolveTableAnySchema(sc.getReferencesTable());
        }

        int[] fkColIndices = new int[sc.getColumns().size()];
        for (int i = 0; i < sc.getColumns().size(); i++) {
            fkColIndices[i] = table.getColumnIndex(sc.getColumns().get(i));
        }

        // Determine referenced columns; if not specified, use the PK of the referenced table
        List<String> refColNames = sc.getReferencesColumns();
        if (refColNames.isEmpty()) {
            refColNames = findPrimaryKeyColumns(refTable);
        }

        int[] refColIndices = new int[refColNames.size()];
        for (int i = 0; i < refColNames.size(); i++) {
            refColIndices[i] = refTable.getColumnIndex(refColNames.get(i));
            if (refColIndices[i] < 0) {
                throw new MemgresException("Referenced column not found: " + refColNames.get(i));
            }
        }

        // Get the FK values from the row
        Object[] fkVals = new Object[fkColIndices.length];
        for (int i = 0; i < fkColIndices.length; i++) {
            // A key column this relation does not carry describes no value in this row. Reading
            // past the row's end raised an internal error where there is simply nothing to check.
            if (fkColIndices[i] < 0 || fkColIndices[i] >= row.length) return;
            fkVals[i] = row[fkColIndices[i]];
        }

        // A NULL means "references nothing". Under the default MATCH SIMPLE one NULL is enough to
        // excuse the whole key; MATCH FULL wants all of them or none, since half a key cannot
        // identify a referenced row.
        int nullCount = 0;
        for (Object val : fkVals) {
            if (val == null) nullCount++;
        }
        if (nullCount == fkVals.length && nullCount > 0) return;
        if (nullCount > 0) {
            if (!"FULL".equalsIgnoreCase(sc.getMatchType())) return;
            MemgresException ex = new MemgresException(
                    "insert or update on table \"" + table.getName() +
                            "\" violates foreign key constraint \"" + sc.getName() + "\"",
                    "23503");
            ex.setConstraint(sc.getName());
            ex.setTable(table.getName());
            String schemaName = findSchemaName(table);
            if (schemaName != null) ex.setSchema(schemaName);
            ex.setDetail("MATCH FULL does not allow mixing of null and nonnull key values.");
            throw ex;
        }

        // Collect all tables to search (include partitions for partitioned tables)
        List<Table> searchTables = new java.util.ArrayList<>();
        if (refTable.getPartitionStrategy() != null && !refTable.getPartitions().isEmpty()) {
            DmlPartitionHelper.collectAllPartitionTables(refTable, searchTables);
        } else {
            searchTables.add(refTable);
        }

        java.util.Set<Object[]> invisible = invisibleReferencedRows(refTable, vanishing);

        if (sc.isPeriod()) {
            if (periodKeyCovered(searchTables, refColNames, sc, table, row, fkColIndices, invisible)) {
                return;
            }
            throw childSideViolation(table, sc, fkColIndices, fkVals);
        }

        // Try O(1) index lookup on referenced table's PK/UNIQUE index
        boolean found = false;
        // A row may satisfy a self-referencing key by itself: PostgreSQL checks the constraint
        // once the row is in place, so a row pointing at its own key is accepted. The check here
        // runs before the row is stored, so it has to be considered explicitly.
        if (refTable == table) {
            boolean selfMatch = true;
            for (int i = 0; i < refColIndices.length; i++) {
                if (!valuesEqual(fkVals[i], row[refColIndices[i]])) {
                    selfMatch = false;
                    break;
                }
            }
            if (selfMatch) found = true;
        }
        if (!found) found = referencedKeyPresent(searchTables, refColNames, fkVals, invisible);
        // The key may be gone only because another session is part-way through deleting the row
        // that holds it, which nothing can decide until that transaction ends. The check waits for
        // it and then looks again, rather than refusing a write that becomes valid the moment that
        // transaction rolls back.
        while (!found && awaitDeletedReferencedRow(searchTables, refColNames, fkVals)) {
            invisible = invisibleReferencedRows(refTable, vanishing);
            found = referencedKeyPresent(searchTables, refColNames, fkVals, invisible);
        }

        if (!found) {
            throw childSideViolation(table, sc, fkColIndices, fkVals);
        }
    }

    /** Whether any of the referenced tables still holds the key, counting no invisible row. */
    private boolean referencedKeyPresent(List<Table> searchTables, List<String> refColNames,
                                         Object[] fkVals, java.util.Set<Object[]> invisible) {
        for (Table searchTable : searchTables) {
            // Recompute ref column indices for each partition table (columns should match)
            int[] stRefColIndices = new int[refColNames.size()];
            boolean colsOk = true;
            for (int i = 0; i < refColNames.size(); i++) {
                stRefColIndices[i] = searchTable.getColumnIndex(refColNames.get(i));
                if (stRefColIndices[i] < 0) { colsOk = false; break; }
            }
            if (!colsOk) continue;
            TableIndex refIdx = findIndexForColumns(searchTable, refColNames);
            if (refIdx != null && (invisible == null || invisible.isEmpty())) {
                if (refIdx.containsKey(fkVals)) return true;
            } else if (refIdx != null) {
                // The key is still in the index, so ask the index which rows hold it and let a row
                // this statement is deleting, or one another session has not committed, count for
                // nothing.
                for (Object[] refRow : refIdx.findAll(fkVals)) {
                    if (!invisible.contains(refRow)) return true;
                }
            } else {
                for (Object[] refRow : searchTable.getRows()) {
                    if (invisible != null && invisible.contains(refRow)) continue;
                    boolean allMatch = true;
                    for (int i = 0; i < stRefColIndices.length; i++) {
                        if (!valuesEqual(fkVals[i], refRow[stRefColIndices[i]])) {
                            allMatch = false;
                            break;
                        }
                    }
                    if (allMatch) return true;
                }
            }
        }
        return false;
    }

    /**
     * Wait while the key a foreign key wants is missing only because another session has deleted
     * the row that holds it without committing.
     *
     * <p>PostgreSQL takes a share lock on the referenced row to check a foreign key, so a child
     * write whose parent row another transaction is in the middle of removing waits for that
     * transaction instead of deciding: whether the key is really gone depends on it. Reporting at
     * once threw away a write that is valid as soon as that transaction rolls back.
     *
     * @return true when there was something to wait for, so the caller looks for the key again
     */
    private boolean awaitDeletedReferencedRow(List<Table> searchTables, List<String> refColNames,
                                              Object[] fkVals) {
        if (executor.session == null || executor.database == null) return false;
        if (!anyOtherOpenTransaction()) return false;
        for (Table searchTable : searchTables) {
            int[] indices = new int[refColNames.size()];
            boolean colsOk = true;
            for (int i = 0; i < refColNames.size(); i++) {
                indices[i] = searchTable.getColumnIndex(refColNames.get(i));
                if (indices[i] < 0) { colsOk = false; break; }
            }
            if (!colsOk) continue;
            final String key = uncommittedKey(searchTable);
            for (Session other : executor.database.getActiveSessions()) {
                if (other == executor.session || other.isDoomed() || !other.isInTransaction()) continue;
                for (Object[] gone : other.getUncommittedDeletes(key)) {
                    if (!rowHoldsKey(gone, indices, fkVals)) continue;
                    final Session blocker = other;
                    final Object[] waitedFor = gone;
                    executor.database.awaitConcurrentWrite(executor.session, blocker,
                            () -> stillDeleting(blocker, key, waitedFor), searchTable.getName());
                    return true;
                }
            }
        }
        return false;
    }

    /** True while the row is still a delete that session has not committed. */
    private static boolean stillDeleting(Session other, String schemaTable, Object[] row) {
        for (Object[] candidate : other.getUncommittedDeletes(schemaTable)) {
            if (candidate == row) return true;
        }
        return false;
    }

    /** PostgreSQL's child-side wording for a key the referenced table does not hold. */
    private MemgresException childSideViolation(Table table, StoredConstraint sc,
                                                int[] fkColIndices, Object[] fkVals) {
        MemgresException ex = new MemgresException(
                "insert or update on table \"" + table.getName() +
                        "\" violates foreign key constraint \"" + sc.getName() + "\"",
                "23503");
        ex.setConstraint(sc.getName());
        ex.setTable(table.getName());
        String schema = findSchemaName(table);
        if (schema != null) ex.setSchema(schema);
        StringBuilder detailSb = new StringBuilder("Key (");
        for (int i = 0; i < sc.getColumns().size(); i++) {
            if (i > 0) detailSb.append(", ");
            detailSb.append(sc.getColumns().get(i));
        }
        detailSb.append(")=(");
        for (int i = 0; i < fkColIndices.length; i++) {
            if (i > 0) detailSb.append(", ");
            detailSb.append(fkVals[i]);
        }
        detailSb.append(") is not present in table \"").append(sc.getReferencesTable()).append("\".");
        ex.setDetail(detailSb.toString());
        return ex;
    }

    /**
     * Whether a temporal foreign key's row is covered by the rows it references.
     *
     * <p>A {@code PERIOD} key names a span rather than a value: the referencing row is satisfied
     * when the rows sharing its other key columns cover its whole period between them. Two
     * referenced rows meeting end to end cover a period that crosses the join, which is the point
     * of the feature — a period is not required to sit inside any one referenced row.
     *
     * <p>An empty period is covered by nothing. PostgreSQL refuses it rather than treating it as
     * a period with nothing to satisfy, and the containment test alone would have let it through.
     */
    private boolean periodKeyCovered(List<Table> searchTables, List<String> refColNames,
                                     StoredConstraint sc, Table table, Object[] row,
                                     int[] fkColIndices, java.util.Set<Object[]> vanishing) {
        int last = fkColIndices.length - 1;
        RangeOperations.PgRange wanted = asRange(row[fkColIndices[last]]);
        if (wanted == null || wanted.isEmpty()) return false;

        List<RangeOperations.PgRange> covering = new java.util.ArrayList<>();
        for (Table searchTable : searchTables) {
            int[] refIdx = new int[refColNames.size()];
            boolean colsOk = true;
            for (int i = 0; i < refColNames.size(); i++) {
                refIdx[i] = searchTable.getColumnIndex(refColNames.get(i));
                if (refIdx[i] < 0) { colsOk = false; break; }
            }
            if (!colsOk) continue;
            for (Object[] refRow : searchTable.getRows()) {
                if (vanishing != null && vanishing.contains(refRow)) continue;
                boolean sameKey = true;
                for (int i = 0; i < last; i++) {
                    if (!valuesEqual(row[fkColIndices[i]], refRow[refIdx[i]])) { sameKey = false; break; }
                }
                if (!sameKey) continue;
                RangeOperations.PgRange has = asRange(refRow[refIdx[last]]);
                if (has != null && !has.isEmpty()) covering.add(has);
            }
        }
        return coveredByUnion(wanted, covering);
    }

    /** A stored value read as a range, or null when it is not one. */
    private static RangeOperations.PgRange asRange(Object value) {
        if (value instanceof RangeOperations.PgRange) return (RangeOperations.PgRange) value;
        if (value == null) return null;
        try {
            return RangeOperations.parse(value.toString());
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * Whether one range is covered by a set of others taken together. The pieces are joined where
     * they meet or overlap, and the answer is whether any one joined piece holds the whole range.
     */
    private static boolean coveredByUnion(RangeOperations.PgRange wanted,
                                          List<RangeOperations.PgRange> pieces) {
        List<RangeOperations.PgRange> merged = new java.util.ArrayList<>();
        List<RangeOperations.PgRange> pending = new java.util.ArrayList<>(pieces);
        while (!pending.isEmpty()) {
            RangeOperations.PgRange current = pending.remove(0);
            boolean grew = true;
            while (grew) {
                grew = false;
                for (int i = 0; i < pending.size(); i++) {
                    RangeOperations.PgRange other = pending.get(i);
                    if (current.overlaps(other) || RangeOperations.areAdjacent(current, other)) {
                        current = RangeOperations.union(current, other);
                        pending.remove(i);
                        grew = true;
                        break;
                    }
                }
            }
            merged.add(current);
        }
        for (RangeOperations.PgRange piece : merged) {
            if (piece.containsRange(wanted)) return true;
        }
        return false;
    }

    private void validateExclude(Table table, Object[] newRow, StoredConstraint sc, Object[] excludeRow) {
        List<StoredConstraint.ExcludeElement> elements = sc.getExcludeElements();
        if (elements == null || elements.isEmpty()) return;

        int[] colIndices = new int[elements.size()];
        for (int i = 0; i < elements.size(); i++) {
            colIndices[i] = table.getColumnIndex(elements.get(i).column());
            if (colIndices[i] < 0) return;
        }

        // Rows a failed transaction wrote are dead: they exclude nothing any more, exactly as they
        // hold no unique key any more.
        final java.util.Set<Object[]> dead = deadUncommittedRows(table);
        for (Object[] existingRow : table.getRows()) {
            if (existingRow == excludeRow) continue;
            if (dead.contains(existingRow)) continue;
            boolean allMatch = true;
            for (int i = 0; i < elements.size(); i++) {
                Object newVal = newRow[colIndices[i]];
                Object existVal = existingRow[colIndices[i]];
                if (newVal == null || existVal == null) { allMatch = false; break; }
                String op = elements.get(i).operator();
                if (!excludeOpMatches(op, newVal, existVal)) { allMatch = false; break; }
            }
            if (allMatch) {
                MemgresException ex = new MemgresException(
                        "conflicting key value violates exclusion constraint \"" + sc.getName() + "\"",
                        "23P01");
                ex.setConstraint(sc.getName());
                ex.setDetail(excludeKeyDetail(elements, colIndices, newRow, existingRow));
                throw ex;
            }
        }
    }

    /**
     * Both keys, the way PostgreSQL prints them. An exclusion constraint is broken by a pair of
     * rows, so naming only the row being written leaves the reader to go looking for the row it
     * collided with.
     */
    private static String excludeKeyDetail(List<StoredConstraint.ExcludeElement> elements,
                                           int[] colIndices, Object[] newRow, Object[] existingRow) {
        StringBuilder cols = new StringBuilder();
        for (int i = 0; i < elements.size(); i++) {
            if (i > 0) cols.append(", ");
            cols.append(elements.get(i).column());
        }
        return "Key (" + cols + ")=(" + excludeKeyValues(colIndices, newRow)
                + ") conflicts with existing key (" + cols + ")=("
                + excludeKeyValues(colIndices, existingRow) + ").";
    }

    /** The values one row holds in the constraint's columns, in the order the constraint names them. */
    private static String excludeKeyValues(int[] colIndices, Object[] row) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < colIndices.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(row[colIndices[i]]);
        }
        return sb.toString();
    }

    private boolean excludeOpMatches(String op, Object a, Object b) {
        switch (op) {
            case "=":
                return valuesEqual(a, b);
            // The operator is what decides a conflict, so <> excludes every value unlike the one
            // already stored: a second row is refused unless it holds exactly the same value.
            case "<>":
            case "!=":
                return !valuesEqual(a, b);
            case "&&":
                return rangesOverlap(a.toString(), b.toString());
            default:
                return false;
        }
    }

    /**
     * Whether two ranges overlap. The bounds are compared as values of the element type, which is
     * how the {@code &&} operator compares them; comparing their written text put 9 after 10 and
     * let two overlapping ranges into a column that excludes them.
     */
    private boolean rangesOverlap(String r1, String r2) {
        try {
            return RangeOperations.parse(r1).overlaps(RangeOperations.parse(r2));
        } catch (MemgresException e) {
            return false;
        }
    }


    // ---- Definition-time validation of a FOREIGN KEY ----

    /**
     * Whether the referenced table has a {@code WITHOUT OVERLAPS} key over these columns.
     *
     * <p>That is the key a temporal foreign key references, and it is not an ordinary unique one:
     * the scalar columns are unique per period rather than outright, so the same id may appear on
     * as many rows as have periods that do not overlap. memgres stores it as the exclusion
     * constraint it is — equality on the scalar columns, overlap on the last — which is the shape
     * looked for here.
     */
    private static boolean hasWithoutOverlapsKeyOn(Table refTable, List<String> refCols) {
        for (StoredConstraint sc : refTable.getConstraints()) {
            if (sc.getType() != StoredConstraint.Type.EXCLUDE) continue;
            List<StoredConstraint.ExcludeElement> elems = sc.getExcludeElements();
            if (elems == null || elems.size() != refCols.size()) continue;
            boolean matches = true;
            for (int i = 0; i < elems.size(); i++) {
                String wantOp = i == elems.size() - 1 ? "&&" : "=";
                if (!wantOp.equals(elems.get(i).operator())
                        || !refCols.get(i).equalsIgnoreCase(elems.get(i).column())) {
                    matches = false;
                    break;
                }
            }
            if (matches) return true;
        }
        return false;
    }

    /**
     * The column a {@code PERIOD} names has to be a span on both sides. A key written over a
     * number cannot be covered by anything, and PostgreSQL says the constraint cannot be built
     * rather than building one that never passes.
     */
    private static void requirePeriodColumnsAreRanges(Table table, Table refTable,
                                                      StoredConstraint fk, List<String> refCols) {
        List<String> fkCols = fk.getColumns();
        Column referencing = columnOf(table, fkCols.get(fkCols.size() - 1));
        Column referenced = columnOf(refTable, refCols.get(refCols.size() - 1));
        if (isRangeColumn(referencing) && isRangeColumn(referenced)) return;
        MemgresException ex = new MemgresException("foreign key constraint \"" + fk.getName()
                + "\" cannot be implemented", "42804");
        // A PERIOD is matched against a PERIOD, so what the reader has to see is the pair that
        // could not be matched and what each side of it holds -- the same detail any foreign key
        // over incompatible types carries.
        ex.setDetail("Key columns \"" + fkCols.get(fkCols.size() - 1)
                + "\" of the referencing table and \"" + refCols.get(refCols.size() - 1)
                + "\" of the referenced table are of incompatible types: "
                + columnTypeName(referencing) + " and " + columnTypeName(referenced) + ".");
        throw ex;
    }

    /** The type a column holds, named the way PostgreSQL names it in an error. */
    private static String columnTypeName(Column column) {
        return column == null || column.getType() == null
                ? "unknown" : column.getType().toRegtypeDisplay();
    }

    private static boolean isRangeColumn(Column column) {
        if (column == null || column.getType() == null) return false;
        String name = column.getType().getPgName();
        return name != null && name.toLowerCase(java.util.Locale.ROOT).endsWith("range");
    }

    /** Names PostgreSQL reserves for system columns; a key may not be built on any of them. */    /** Names PostgreSQL reserves for system columns; a key may not be built on any of them. */
    private static final Set<String> SYSTEM_COLUMNS = new HashSet<>(Arrays.asList(
            "tableoid", "ctid", "xmin", "cmin", "xmax", "cmax"));

    /**
     * Runs the checks PostgreSQL runs when a FOREIGN KEY is declared, in PostgreSQL's own order.
     *
     * <p>A key whose referenced columns are not unique, or whose two sides hold values that can
     * never compare equal, cannot enforce anything. Storing it anyway is worse than refusing it:
     * the constraint shows up in the catalog and a reader of the schema concludes the data is
     * protected when nothing is checking it.
     *
     * @param refTable the referenced table, already resolved and known to be a table
     */
    static void validateForeignKeyDefinition(Table table, Table refTable, String refTableName,
                                             StoredConstraint fk) {
        List<String> fkCols = fk.getColumns();
        for (String col : fkCols) {
            requireKeyColumn(table, col);
        }
        // The ON DELETE SET NULL/SET DEFAULT column list is checked next, before the referenced
        // side is looked at at all — matching PostgreSQL, which reports a bad list first.
        List<String> setCols = fk.getOnDeleteSetNullColumns();
        if (setCols != null) {
            for (String col : setCols) {
                requireKeyColumn(table, col);
                if (!StoredConstraint.containsIgnoreCase(fkCols, col)) {
                    throw new MemgresException("column \"" + col
                            + "\" referenced in ON DELETE SET action must be part of foreign key", "42P10");
                }
            }
        }

        List<String> refCols = fk.getReferencesColumns();
        if (refCols == null || refCols.isEmpty()) {
            refCols = primaryKeyColumnsOf(refTable);
            if (refCols.isEmpty()) {
                throw new MemgresException("there is no primary key for referenced table \""
                        + refTableName + "\"", "42704");
            }
            rejectDeferrableReferencedKey(refTable, refCols, refTableName, true);
        } else {
            for (String col : refCols) {
                requireKeyColumn(refTable, col);
            }
            for (int i = 0; i < refCols.size(); i++) {
                for (int j = i + 1; j < refCols.size(); j++) {
                    if (refCols.get(i).equalsIgnoreCase(refCols.get(j))) {
                        throw new MemgresException(
                                "foreign key referenced-columns list must not contain duplicates", "42830");
                    }
                }
            }
            boolean keyed = fk.isPeriod()
                    ? hasWithoutOverlapsKeyOn(refTable, refCols)
                    : hasUniqueConstraintOn(refTable, refCols);
            if (!keyed) {
                throw new MemgresException("there is no unique constraint matching given keys for referenced table \""
                        + refTableName + "\"", "42830");
            }
            if (!fk.isPeriod()) rejectDeferrableReferencedKey(refTable, refCols, refTableName, false);
            if (fk.isPeriod()) requirePeriodColumnsAreRanges(table, refTable, fk, refCols);
        }
        if (fkCols.size() != refCols.size()) {
            throw new MemgresException(
                    "number of referencing and referenced columns for foreign key disagree", "42830");
        }
        for (int i = 0; i < fkCols.size(); i++) {
            Column fkCol = columnOf(table, fkCols.get(i));
            Column refCol = columnOf(refTable, refCols.get(i));
            if (fkCol == null || refCol == null || keyTypesComparable(fkCol, refCol)) continue;
            MemgresException ex = new MemgresException("foreign key constraint \"" + fk.getName()
                    + "\" cannot be implemented", "42804");
            ex.setDetail("Key columns \"" + fkCols.get(i) + "\" of the referencing table and \""
                    + refCols.get(i) + "\" of the referenced table are of incompatible types: "
                    + fkCol.getType().toRegtypeDisplay() + " and " + refCol.getType().toRegtypeDisplay() + ".");
            throw ex;
        }
    }

    /** A key column must exist and must not be a system column. */
    private static void requireKeyColumn(Table table, String column) {
        if (table.getColumnIndex(column) >= 0) return;
        if (SYSTEM_COLUMNS.contains(column.toLowerCase())) {
            throw new MemgresException("system columns cannot be used in foreign keys", "0A000");
        }
        throw new MemgresException("column \"" + column
                + "\" referenced in foreign key constraint does not exist", "42703");
    }

    private static Column columnOf(Table table, String name) {
        int idx = table.getColumnIndex(name);
        return idx < 0 ? null : table.getColumns().get(idx);
    }

    static List<String> primaryKeyColumnsOf(Table table) {
        for (StoredConstraint sc : table.getConstraints()) {
            if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY) return sc.getColumns();
        }
        List<String> pkCols = new ArrayList<>();
        for (Column col : table.getColumns()) {
            if (col.isPrimaryKey()) pkCols.add(col.getName());
        }
        return pkCols;
    }

    /**
     * A foreign key is checked as the row is written, and a deferrable key's index need not be
     * unique at that moment: PostgreSQL will not build a foreign key over one at all. How it words
     * the refusal follows the statement rather than the key -- a reference that named no columns
     * went to the primary key and is told so.
     */
    private static void rejectDeferrableReferencedKey(Table refTable, List<String> refCols,
                                                      String refTableName, boolean impliedKey) {
        for (StoredConstraint sc : refTable.getConstraints()) {
            if (sc.getType() != StoredConstraint.Type.PRIMARY_KEY
                    && sc.getType() != StoredConstraint.Type.UNIQUE) continue;
            if (!sc.isDeferrable() || sc.getWhereExpr() != null) continue;
            List<String> cols = sc.getColumns();
            if (cols == null || cols.size() != refCols.size()) continue;
            boolean allPresent = true;
            for (String want : refCols) {
                if (!StoredConstraint.containsIgnoreCase(cols, want)) { allPresent = false; break; }
            }
            if (!allPresent) continue;
            throw new MemgresException("cannot use a deferrable "
                    + (impliedKey ? "primary key" : "unique constraint")
                    + " for referenced table \"" + refTableName + "\"", "55000");
        }
    }

    /**
     * True when the referenced table has a PRIMARY KEY or UNIQUE constraint over exactly these
     * columns. PostgreSQL matches the index by column set, not by the order they were written in.
     */
    private static boolean hasUniqueConstraintOn(Table refTable, List<String> refCols) {
        for (StoredConstraint sc : refTable.getConstraints()) {
            if (sc.getType() != StoredConstraint.Type.PRIMARY_KEY
                    && sc.getType() != StoredConstraint.Type.UNIQUE) continue;
            if (sc.getWhereExpr() != null) continue; // a partial index cannot back a foreign key
            List<String> cols = sc.getColumns();
            if (cols == null || cols.size() != refCols.size()) continue;
            boolean allPresent = true;
            for (String want : refCols) {
                if (!StoredConstraint.containsIgnoreCase(cols, want)) { allPresent = false; break; }
            }
            if (allPresent) return true;
        }
        // Column-level PRIMARY KEY that never became a stored constraint
        if (refCols.size() == 1) {
            Column col = columnOf(refTable, refCols.get(0));
            if (col != null && col.isPrimaryKey()) return true;
        }
        return false;
    }

    /**
     * PostgreSQL builds a foreign key only when the two key columns have an equality operator it
     * can use: either both types sit in the same btree operator family, or the referencing type
     * casts implicitly to the referenced one. Types this engine does not classify — enums,
     * domains, composites — are left to the existing lenient behaviour rather than guessed at.
     */
    private static boolean keyTypesComparable(Column fkCol, Column refCol) {
        if (hasNamedType(fkCol) || hasNamedType(refCol)) return true;
        DataType a = fkCol.getType();
        DataType b = refCol.getType();
        if (a == b) return true;
        String fa = keyTypeFamily(a);
        String fb = keyTypeFamily(b);
        if (fa == null || fb == null) return true;
        if (fa.equals(fb)) return true;
        return castsImplicitly(fa, fb);
    }

    /** True for a column whose type carries an identity beyond its {@link DataType}. */
    private static boolean hasNamedType(Column col) {
        return col.getEnumTypeName() != null || col.getDomainTypeName() != null
                || col.getCompositeTypeName() != null || col.getType() == DataType.ENUM;
    }

    /** The btree operator family a type's default operator class belongs to, or null if unknown. */
    private static String keyTypeFamily(DataType type) {
        switch (type) {
            case SMALLINT: case INTEGER: case BIGINT:
            case SMALLSERIAL: case SERIAL: case BIGSERIAL:
                return "integer";
            case NUMERIC: return "numeric";
            case REAL: case DOUBLE_PRECISION: return "float";
            case TEXT: case VARCHAR: case CHAR: case NAME: return "text";
            case DATE: case TIMESTAMP: case TIMESTAMPTZ: return "datetime";
            case TIME: return "time";
            case TIMETZ: return "timetz";
            case INTERVAL: return "interval";
            case INET: case CIDR: return "network";
            case OID: return "oid";
            case BIT: case VARBIT: return "bit";
            case BOOLEAN: case UUID: case BYTEA: case MACADDR: case MACADDR8:
            case MONEY: case JSONB: case XID:
                return type.name();
            default:
                return null;
        }
    }

    /** The implicit casts between key-type families that PostgreSQL will fall back on. */
    private static boolean castsImplicitly(String from, String to) {
        if ("integer".equals(from)) {
            return "numeric".equals(to) || "float".equals(to) || "oid".equals(to);
        }
        if ("numeric".equals(from)) return "float".equals(to);
        if ("time".equals(from)) return "interval".equals(to);
        return false;
    }

    private List<String> findPrimaryKeyColumns(Table table) {
        // Check stored constraints for PK
        for (StoredConstraint sc : table.getConstraints()) {
            if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY) {
                return sc.getColumns();
            }
        }
        // Fall back to column-level PK flags
        List<String> pkCols = new ArrayList<>();
        for (Column col : table.getColumns()) {
            if (col.isPrimaryKey()) {
                pkCols.add(col.getName());
            }
        }
        return pkCols;
    }

    /**
     * The columns an ON DELETE SET NULL / SET DEFAULT touches: the explicit column list when the
     * constraint carries one (PG 15+), otherwise every referencing column.
     */
    private static int[] actionColumnIndices(Table childTable, List<String> actionCols, int[] fallback) {
        if (actionCols == null || actionCols.isEmpty()) return fallback;
        int[] indices = new int[actionCols.size()];
        for (int i = 0; i < actionCols.size(); i++) {
            indices[i] = childTable.getColumnIndex(actionCols.get(i));
        }
        return indices;
    }

    /** The lower-cased names of the columns an action writes, for {@code UPDATE OF} triggers. */
    private java.util.Set<String> actionColumnNames(Table childTable, int[] indices) {
        java.util.Set<String> names = new LinkedHashSet<>();
        for (int idx : indices) {
            if (idx >= 0 && idx < childTable.getColumns().size()) {
                names.add(childTable.getColumns().get(idx).getName().toLowerCase());
            }
        }
        return names;
    }

    /**
     * One row a referential action is about to rewrite: the stored row, the values it holds now,
     * and the values the action computed for it.
     *
     * <p>The rows are collected before any of them is written, so the scan over the referencing
     * table is not disturbed by the writes it leads to.
     */
    static final class ActionRow {
        final Object[] row;
        final Object[] oldValues;
        final Object[] newValues;

        ActionRow(Object[] row, Object[] oldValues, Object[] newValues) {
            this.row = row;
            this.oldValues = oldValues;
            this.newValues = newValues;
        }
    }

    /** Whether another session is inside a transaction that could still make a row permanent. */
    private boolean anyOtherOpenTransaction() {
        for (Session other : executor.database.getActiveSessions()) {
            if (other == executor.session || other.isDoomed()) continue;
            if (other.isInTransaction()) return true;
        }
        return false;
    }

    /**
     * Wait until no other session holds an uncommitted row of the referencing table under this key.
     *
     * <p>A referential action decides what becomes of the rows that reference a key, and a row
     * another transaction has written but not committed is not one it may decide anything about:
     * PostgreSQL locks the referencing rows, so the write on the parent waits for that transaction
     * to settle. Deciding instead refuses a delete over a row that may never exist, or cascades
     * into a row that transaction is still free to keep.
     */
    private void awaitPendingChildRows(Table childTable, int[] childColIndices, Object[] parentVals) {
        if (executor.session == null || executor.database == null) return;
        if (!anyOtherOpenTransaction()) return;
        final String key = uncommittedKey(childTable);
        while (true) {
            // The wait can end without the row having moved. Polling the cancel token here means a
            // statement_timeout or a client cancel still ends this loop if it does.
            StatementCancel.check();
            PendingUniqueConflict blocked = null;
            for (Session other : executor.database.getActiveSessions()) {
                if (other == executor.session || other.isDoomed() || !other.isInTransaction()) continue;
                for (Object[] candidate : other.getUncommittedInserts(key)) {
                    if (rowHoldsKey(candidate, childColIndices, parentVals)) {
                        blocked = new PendingUniqueConflict(other, candidate, childTable.getName());
                        break;
                    }
                }
                if (blocked != null) break;
            }
            if (blocked == null) return;
            final PendingUniqueConflict pending = blocked;
            executor.database.awaitConcurrentWrite(executor.session, pending.owner,
                    () -> isStillPending(pending, key), pending.relation);
        }
    }

    /** Whether a row carries the referencing values a write on that parent key would act on. */
    private boolean rowHoldsKey(Object[] row, int[] childColIndices, Object[] parentVals) {
        for (int i = 0; i < childColIndices.length; i++) {
            int idx = childColIndices[i];
            if (idx < 0 || idx >= row.length) return false;
            if (!valuesEqual(parentVals[i], row[idx])) return false;
        }
        return true;
    }

    /**
     * The rows of a referenced table that a foreign-key check may not count.
     *
     * <p>{@code vanishing} holds the rows the statement itself is taking away. To them belong the
     * rows another session has inserted and not committed: PostgreSQL decides the check under the
     * writing session's own snapshot, where another transaction's unfinished insert is simply not
     * there, so a child row may not be admitted on the strength of a parent row that may never
     * exist. The union is built only when another transaction really is open, so an ordinary check
     * pays a walk over the sessions and nothing else.
     */
    private java.util.Set<Object[]> invisibleReferencedRows(Table refTable,
                                                            java.util.Set<Object[]> vanishing) {
        if (executor.session == null || executor.database == null) return vanishing;
        if (!anyOtherOpenTransaction()) return vanishing;
        String key = uncommittedKey(refTable);
        java.util.Set<Object[]> hidden = null;
        for (Session other : executor.database.getActiveSessions()) {
            if (other == executor.session || !other.isInTransaction()) continue;
            java.util.Set<Object[]> theirs = other.getUncommittedInserts(key);
            if (theirs == null || theirs.isEmpty()) continue;
            if (hidden == null) {
                hidden = Collections.newSetFromMap(new java.util.IdentityHashMap<Object[], Boolean>());
                if (vanishing != null) hidden.addAll(vanishing);
            }
            hidden.addAll(theirs);
        }
        return hidden == null ? vanishing : hidden;
    }

    /**
     * Handle FK ON DELETE actions for all tables that reference the given table.
     */
    void handleFkOnDelete(Table parentTable, Object[] deletedRow) {
        handleFkOnDelete(parentTable, deletedRow, null);
    }

    void handleFkOnDelete(Table parentTable, Object[] deletedRow, java.util.Set<Object[]> alsoDeleting) {
        handleFkOnDelete(parentTable, deletedRow, alsoDeleting,
                Collections.newSetFromMap(new java.util.IdentityHashMap<Object[], Boolean>()));
    }

    /**
     * Two tables referencing each other with ON DELETE CASCADE is an ordinary schema, and so is a
     * table referencing itself. The walk therefore has to visit each row once: without
     * {@code cascaded} it follows the cycle back to a row it has already deleted and recurses
     * until the stack is gone.
     */
    private void handleFkOnDelete(Table parentTable, Object[] deletedRow,
                                  java.util.Set<Object[]> alsoDeleting,
                                  java.util.Set<Object[]> cascaded) {
        if (!cascaded.add(deletedRow)) return;
        String parentSchemaName = findSchemaName(parentTable);
        // Find all tables with FK constraints referencing this table
        for (Schema schema : executor.database.getSchemas().values()) {
            for (Table childTable : schema.getTables().values()) {
                for (StoredConstraint sc : childTable.getConstraints()) {
                    if (sc.getType() != StoredConstraint.Type.FOREIGN_KEY) continue;
                    if (sc.isNotEnforced()) continue;
                    if (!fkReferencesTable(sc, parentTable, parentSchemaName)) continue;

                    List<String> refColNames = sc.getReferencesColumns();
                    if (refColNames.isEmpty()) {
                        refColNames = findPrimaryKeyColumns(parentTable);
                    }

                    int[] parentColIndices = new int[refColNames.size()];
                    boolean columnMismatch = false;
                    for (int i = 0; i < refColNames.size(); i++) {
                        parentColIndices[i] = parentTable.getColumnIndex(refColNames.get(i));
                        if (parentColIndices[i] < 0) {
                            columnMismatch = true;
                            break;
                        }
                    }
                    if (columnMismatch) continue;

                    int[] childColIndices = new int[sc.getColumns().size()];
                    for (int i = 0; i < sc.getColumns().size(); i++) {
                        childColIndices[i] = childTable.getColumnIndex(sc.getColumns().get(i));
                    }

                    Object[] parentVals = new Object[parentColIndices.length];
                    for (int i = 0; i < parentColIndices.length; i++) {
                        parentVals[i] = deletedRow[parentColIndices[i]];
                    }

                    // Check replica identity for child table when it will be modified by FK cascade
                    if (sc.getOnDelete() == StoredConstraint.FkAction.SET_NULL
                            || sc.getOnDelete() == StoredConstraint.FkAction.SET_DEFAULT
                            || sc.getOnDelete() == StoredConstraint.FkAction.CASCADE) {
                        checkChildTableReplicaIdentity(childTable,
                                sc.getOnDelete() == StoredConstraint.FkAction.CASCADE ? "delete" : "update");
                    }

                    String childSchemaName = schema.getName();

                    awaitPendingChildRows(childTable, childColIndices, parentVals);

                    switch (sc.getOnDelete()) {
                        case CASCADE: {
                            // PostgreSQL runs the action as a DELETE on the referencing table, so
                            // that table's own FOR EACH STATEMENT triggers fire for it -- once for
                            // the whole statement, and even when no row of it matches.
                            final DmlTriggerHelper.ReferentialStatement acting =
                                    triggerHelper.referentialStatement(childTable, PgTrigger.Event.DELETE);
                            // Collect rows to delete, then use Table.deleteRows for proper index maintenance
                            java.util.Set<Object[]> deleteSet = Collections.newSetFromMap(new java.util.IdentityHashMap<>());
                            for (Object[] childRow : childTable.getRows()) {
                                boolean matches = true;
                                for (int i = 0; i < childColIndices.length; i++) {
                                    if (!valuesEqual(parentVals[i], childRow[childColIndices[i]])) {
                                        matches = false;
                                        break;
                                    }
                                }
                                // A row the same statement is already deleting leaves the action
                                // nothing to do: removing it again records a second undo entry and
                                // fires its triggers a second time.
                                if (matches && (alsoDeleting == null || !alsoDeleting.contains(childRow))) {
                                    deleteSet.add(childRow);
                                }
                            }
                            // PostgreSQL runs the action as a DELETE on the referencing table, so
                            // the child's own row triggers fire and a BEFORE trigger returning NULL
                            // keeps its row — leaving, as PostgreSQL leaves it, a reference to a
                            // row that has gone.
                            executor.dmlExecutor.applyReferentialDelete(childTable, childSchemaName,
                                    deleteSet, surviving -> {
                                        // The rows still standing are the ones the action takes, and
                                        // an AFTER STATEMENT trigger's OLD TABLE is made of them.
                                        if (acting != null) {
                                            for (Object[] going : surviving) acting.wrote(null, going);
                                        }
                                        // Recurse before the rows go: the rows this cascade is about
                                        // to remove are as gone, to anything referencing them, as
                                        // the ones the statement named — a SET DEFAULT further down
                                        // may not point at one of them.
                                        for (Object[] childRow : new ArrayList<>(surviving)) {
                                            handleFkOnDelete(childTable, childRow, surviving, cascaded);
                                        }
                                    });
                            break;
                        }
                        case SET_NULL: {
                            // The action is an UPDATE of the referencing table, and that table's
                            // statement-level triggers fire for it as for one a client wrote.
                            final DmlTriggerHelper.ReferentialStatement acting =
                                    triggerHelper.referentialStatement(childTable, PgTrigger.Event.UPDATE);
                            int[] nullIndices = actionColumnIndices(
                                    childTable, sc.getOnDeleteSetNullColumns(), childColIndices);
                            List<ActionRow> pending = new ArrayList<>();
                            for (Object[] childRow : childTable.getRows()) {
                                boolean matches = true;
                                for (int i = 0; i < childColIndices.length; i++) {
                                    if (!valuesEqual(parentVals[i], childRow[childColIndices[i]])) {
                                        matches = false;
                                        break;
                                    }
                                }
                                if (matches) {
                                    Object[] oldVals = Arrays.copyOf(childRow, childRow.length);
                                    Object[] newVals = Arrays.copyOf(childRow, childRow.length);
                                    for (int idx : nullIndices) {
                                        newVals[idx] = null;
                                    }
                                    pending.add(new ActionRow(childRow, oldVals, newVals));
                                }
                            }
                            for (ActionRow acted : executor.dmlExecutor.applyReferentialUpdate(
                                    childTable, childSchemaName, pending,
                                    actionColumnNames(childTable, nullIndices), null)) {
                                if (acting != null) acting.wrote(acted.row, acted.oldValues);
                                // Recurse: the child row's FK columns changed, so its dependents may need cascading
                                handleFkOnUpdate(childTable, acted.oldValues, acted.row);
                            }
                            break;
                        }
                        case SET_DEFAULT: {
                            // Also an UPDATE of the referencing table as far as its triggers go.
                            final DmlTriggerHelper.ReferentialStatement acting =
                                    triggerHelper.referentialStatement(childTable, PgTrigger.Event.UPDATE);
                            int[] defaultIndices = actionColumnIndices(
                                    childTable, sc.getOnDeleteSetNullColumns(), childColIndices);
                            List<ActionRow> pending = new ArrayList<>();
                            for (Object[] childRow : childTable.getRows()) {
                                boolean matches = true;
                                for (int i = 0; i < childColIndices.length; i++) {
                                    if (!valuesEqual(parentVals[i], childRow[childColIndices[i]])) {
                                        matches = false;
                                        break;
                                    }
                                }
                                // A child row the same statement is deleting needs no default:
                                // it will not be there to carry one. Writing one anyway is how a
                                // self-referential DELETE came to be refused over a key that
                                // nothing was going to reference.
                                if (matches && alsoDeleting != null && alsoDeleting.contains(childRow)) {
                                    continue;
                                }
                                if (matches) {
                                    Object[] oldVals = Arrays.copyOf(childRow, childRow.length);
                                    Object[] newVals = Arrays.copyOf(childRow, childRow.length);
                                    for (int idx : defaultIndices) {
                                        Column col = childTable.getColumns().get(idx);
                                        newVals[idx] = col.getDefaultValue() != null
                                                ? executor.evaluateDefault(col.getDefaultValue(), col.getType(), col.getParsedDefaultExpr())
                                                : null;
                                    }
                                    // The default is an ordinary value: nothing guarantees the
                                    // referenced table holds it, so the key is checked against the
                                    // new values before the row is rewritten, leaving the row as
                                    // it was when the check fails. A parent row this same statement
                                    // is deleting cannot satisfy it either — it is still stored
                                    // while the action runs, but it will not be there afterwards.
                                    checkDefaultKeyStillThere(childTable, newVals, sc, alsoDeleting,
                                            parentTable, refColNames);
                                    pending.add(new ActionRow(childRow, oldVals, newVals));
                                }
                            }
                            // The acting key has just been checked, and only that check knows which
                            // parent rows are going away, so the generic pass leaves it alone.
                            for (ActionRow acted : executor.dmlExecutor.applyReferentialUpdate(
                                    childTable, childSchemaName, pending,
                                    actionColumnNames(childTable, defaultIndices), sc)) {
                                if (acting != null) acting.wrote(acted.row, acted.oldValues);
                                handleFkOnUpdate(childTable, acted.oldValues, acted.row);
                            }
                            break;
                        }
                        case RESTRICT:
                        case NO_ACTION: {
                            // NO ACTION is the deferrable one: it only asks that no child is left
                            // dangling once the transaction is over, so a transaction is free to
                            // delete the parent and put it back. RESTRICT refuses the delete
                            // itself and fires at the statement even when the constraint is
                            // deferred.
                            boolean postpone = sc.getOnDelete() == StoredConstraint.FkAction.NO_ACTION
                                    && checkIsCurrentlyDeferred(sc);
                            for (Object[] childRow : childTable.getRows()) {
                                // Skip rows that are also being deleted in the same statement
                                if (alsoDeleting != null && alsoDeleting.contains(childRow)) continue;
                                if (!childRowNeedsParent(sc, childTable, childRow, childColIndices,
                                        parentVals, deletedRow, alsoDeleting)) {
                                    continue;
                                }
                                if (postpone) {
                                    executor.session.addDeferredReferencedCheck(
                                            childTable, childRow, sc, parentTable);
                                    continue;
                                }
                                throw referencedRowError(parentTable, childTable, sc, refColNames,
                                        parentVals, schema.getName(),
                                        sc.getOnDelete() == StoredConstraint.FkAction.RESTRICT);
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    /**
     * Handle FK ON UPDATE actions for all tables that reference the given table.
     */
    void handleFkOnUpdate(Table parentTable, Object[] oldRow, Object[] newRow) {
        handleFkOnUpdate(parentTable, oldRow, newRow,
                Collections.newSetFromMap(new java.util.IdentityHashMap<Object[], Boolean>()));
    }

    /** As with the delete walk, a cycle of ON UPDATE CASCADE constraints must visit each row once. */
    private void handleFkOnUpdate(Table parentTable, Object[] oldRow, Object[] newRow,
                                  java.util.Set<Object[]> cascaded) {
        if (!cascaded.add(newRow)) return;
        String parentSchemaName = findSchemaName(parentTable);
        for (Schema schema : executor.database.getSchemas().values()) {
            for (Table childTable : schema.getTables().values()) {
                for (StoredConstraint sc : childTable.getConstraints()) {
                    if (sc.getType() != StoredConstraint.Type.FOREIGN_KEY) continue;
                    if (sc.isNotEnforced()) continue;
                    if (!fkReferencesTable(sc, parentTable, parentSchemaName)) continue;

                    List<String> refColNames = sc.getReferencesColumns();
                    if (refColNames.isEmpty()) {
                        refColNames = findPrimaryKeyColumns(parentTable);
                    }

                    // Check if any referenced columns actually changed
                    int[] parentColIndices = new int[refColNames.size()];
                    boolean anyChanged = false;
                    boolean columnMismatch = false;
                    for (int i = 0; i < refColNames.size(); i++) {
                        parentColIndices[i] = parentTable.getColumnIndex(refColNames.get(i));
                        if (parentColIndices[i] < 0) {
                            // Referenced column doesn't exist on this table; FK is from a
                            // different schema referencing a different table with the same name.
                            columnMismatch = true;
                            break;
                        }
                        if (!valuesEqual(oldRow[parentColIndices[i]], newRow[parentColIndices[i]])) {
                            anyChanged = true;
                        }
                    }
                    if (columnMismatch) continue;
                    if (!anyChanged) continue;

                    int[] childColIndices = new int[sc.getColumns().size()];
                    for (int i = 0; i < sc.getColumns().size(); i++) {
                        childColIndices[i] = childTable.getColumnIndex(sc.getColumns().get(i));
                    }

                    Object[] oldParentVals = new Object[parentColIndices.length];
                    Object[] newParentVals = new Object[parentColIndices.length];
                    for (int i = 0; i < parentColIndices.length; i++) {
                        oldParentVals[i] = oldRow[parentColIndices[i]];
                        newParentVals[i] = newRow[parentColIndices[i]];
                    }

                    String childSchemaName = schema.getName();

                    awaitPendingChildRows(childTable, childColIndices, oldParentVals);

                    switch (sc.getOnUpdate()) {
                        case CASCADE: {
                            // As on the delete side, the action is a statement against the
                            // referencing table, and its statement-level triggers fire once for it.
                            final DmlTriggerHelper.ReferentialStatement acting =
                                    triggerHelper.referentialStatement(childTable, PgTrigger.Event.UPDATE);
                            List<ActionRow> pending = new ArrayList<>();
                            for (Object[] childRow : childTable.getRows()) {
                                boolean matches = true;
                                for (int i = 0; i < childColIndices.length; i++) {
                                    if (!valuesEqual(oldParentVals[i], childRow[childColIndices[i]])) {
                                        matches = false;
                                        break;
                                    }
                                }
                                if (matches) {
                                    Object[] oldVals = Arrays.copyOf(childRow, childRow.length);
                                    Object[] newVals = Arrays.copyOf(childRow, childRow.length);
                                    for (int i = 0; i < childColIndices.length; i++) {
                                        newVals[childColIndices[i]] = newParentVals[i];
                                    }
                                    pending.add(new ActionRow(childRow, oldVals, newVals));
                                }
                            }
                            for (ActionRow acted : executor.dmlExecutor.applyReferentialUpdate(
                                    childTable, childSchemaName, pending,
                                    actionColumnNames(childTable, childColIndices), null)) {
                                if (acting != null) acting.wrote(acted.row, acted.oldValues);
                                // Recurse: the child row's FK columns changed
                                handleFkOnUpdate(childTable, acted.oldValues, acted.row, cascaded);
                            }
                            break;
                        }
                        case SET_NULL: {
                            final DmlTriggerHelper.ReferentialStatement acting =
                                    triggerHelper.referentialStatement(childTable, PgTrigger.Event.UPDATE);
                            // Determine which child column indices to null
                            int[] updateNullIndices;
                            java.util.List<String> updateSetNullCols = sc.getOnUpdateSetNullColumns();
                            if (updateSetNullCols != null && !updateSetNullCols.isEmpty()) {
                                updateNullIndices = new int[updateSetNullCols.size()];
                                for (int ni = 0; ni < updateSetNullCols.size(); ni++) {
                                    updateNullIndices[ni] = childTable.getColumnIndex(updateSetNullCols.get(ni));
                                }
                            } else {
                                updateNullIndices = childColIndices;
                            }
                            List<ActionRow> pending = new ArrayList<>();
                            for (Object[] childRow : childTable.getRows()) {
                                boolean matches = true;
                                for (int i = 0; i < childColIndices.length; i++) {
                                    if (!valuesEqual(oldParentVals[i], childRow[childColIndices[i]])) {
                                        matches = false;
                                        break;
                                    }
                                }
                                if (matches) {
                                    Object[] oldVals = Arrays.copyOf(childRow, childRow.length);
                                    Object[] newVals = Arrays.copyOf(childRow, childRow.length);
                                    for (int idx : updateNullIndices) {
                                        newVals[idx] = null;
                                    }
                                    pending.add(new ActionRow(childRow, oldVals, newVals));
                                }
                            }
                            for (ActionRow acted : executor.dmlExecutor.applyReferentialUpdate(
                                    childTable, childSchemaName, pending,
                                    actionColumnNames(childTable, updateNullIndices), null)) {
                                if (acting != null) acting.wrote(acted.row, acted.oldValues);
                                handleFkOnUpdate(childTable, acted.oldValues, acted.row, cascaded);
                            }
                            break;
                        }
                        case SET_DEFAULT: {
                            final DmlTriggerHelper.ReferentialStatement acting =
                                    triggerHelper.referentialStatement(childTable, PgTrigger.Event.UPDATE);
                            List<ActionRow> pending = new ArrayList<>();
                            for (Object[] childRow : childTable.getRows()) {
                                boolean matches = true;
                                for (int i = 0; i < childColIndices.length; i++) {
                                    if (!valuesEqual(oldParentVals[i], childRow[childColIndices[i]])) {
                                        matches = false;
                                        break;
                                    }
                                }
                                if (matches) {
                                    Object[] oldVals = Arrays.copyOf(childRow, childRow.length);
                                    Object[] newVals = Arrays.copyOf(childRow, childRow.length);
                                    for (int i = 0; i < childColIndices.length; i++) {
                                        Column col = childTable.getColumns().get(childColIndices[i]);
                                        newVals[childColIndices[i]] = col.getDefaultValue() != null
                                                ? executor.evaluateDefault(col.getDefaultValue(), col.getType(), col.getParsedDefaultExpr())
                                                : null;
                                    }
                                    // The default has to name a key the referenced table still
                                    // holds. Moving a key away from under it is the update's
                                    // fault, so PostgreSQL reports it from the parent's side.
                                    try {
                                        validateForeignKey(childTable, newVals, sc, null);
                                    } catch (MemgresException notThere) {
                                        throw parentSideViolation(parentTable, childTable, sc,
                                                newVals, updateRefColNames(sc, parentTable));
                                    }
                                    pending.add(new ActionRow(childRow, oldVals, newVals));
                                }
                            }
                            // The acting key was just checked, and its failure is worded from the
                            // parent's side; the generic pass would report the child's wording for
                            // the same key, so it leaves that constraint alone.
                            for (ActionRow acted : executor.dmlExecutor.applyReferentialUpdate(
                                    childTable, childSchemaName, pending,
                                    actionColumnNames(childTable, childColIndices), sc)) {
                                if (acting != null) acting.wrote(acted.row, acted.oldValues);
                                handleFkOnUpdate(childTable, acted.oldValues, acted.row, cascaded);
                            }
                            break;
                        }
                        case RESTRICT:
                        case NO_ACTION: {
                            // As on the delete side: NO ACTION only wants the key back by the end
                            // of the transaction, RESTRICT refuses the update where it stands.
                            boolean postpone = sc.getOnUpdate() == StoredConstraint.FkAction.NO_ACTION
                                    && checkIsCurrentlyDeferred(sc);
                            for (Object[] childRow : childTable.getRows()) {
                                if (!childRowNeedsParent(sc, childTable, childRow, childColIndices,
                                        oldParentVals, oldRow, null)) {
                                    continue;
                                }
                                if (postpone) {
                                    executor.session.addDeferredReferencedCheck(
                                            childTable, childRow, sc, parentTable);
                                    continue;
                                }
                                throw referencedRowError(parentTable, childTable, sc, refColNames,
                                        oldParentVals, childSchemaName,
                                        sc.getOnUpdate() == StoredConstraint.FkAction.RESTRICT);
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    /**
     * Whether a child row is left without what it references once a parent row goes.
     *
     * <p>For an ordinary key that is the same question as whether the two keys are equal: the
     * child names one parent row, and taking that row away leaves it naming nothing. A temporal
     * key names as many parent rows as its period runs across, so taking one away only matters
     * when what is left no longer covers the period — which is why the periods themselves are
     * never compared for equality here.
     */
    private boolean childRowNeedsParent(StoredConstraint sc, Table childTable, Object[] childRow,
                                        int[] childColIndices, Object[] parentVals,
                                        Object[] parentRow, java.util.Set<Object[]> alsoGoing) {
        int scalarColumns = sc.isPeriod() ? childColIndices.length - 1 : childColIndices.length;
        for (int i = 0; i < scalarColumns; i++) {
            if (!valuesEqual(parentVals[i], childRow[childColIndices[i]])) return false;
        }
        if (!sc.isPeriod()) return true;
        java.util.Set<Object[]> without =
                Collections.newSetFromMap(new java.util.IdentityHashMap<Object[], Boolean>());
        if (alsoGoing != null) without.addAll(alsoGoing);
        if (parentRow != null) without.add(parentRow);
        try {
            validateForeignKey(childTable, childRow, sc, without);
            return false;
        } catch (MemgresException stillShort) {
            return true;
        }
    }

    /** Find an index on the given table whose columns match the provided column names (in order). */    /** Find an index on the given table whose columns match the provided column names (in order). */
    private TableIndex findIndexForColumns(Table table, List<String> columnNames) {
        int[] targetIndices = new int[columnNames.size()];
        for (int i = 0; i < columnNames.size(); i++) {
            targetIndices[i] = table.getColumnIndex(columnNames.get(i));
            if (targetIndices[i] < 0) return null;
        }
        for (TableIndex idx : table.getIndexes().values()) {
            int[] idxCols = idx.getColumnIndices();
            if (idxCols.length != targetIndices.length) continue;
            boolean match = true;
            for (int i = 0; i < idxCols.length; i++) {
                if (idxCols[i] != targetIndices[i]) { match = false; break; }
            }
            if (match) return idx;
        }
        return null;
    }

    boolean valuesEqual(Object a, Object b) {
        if (a == null || b == null) return a == b;
        return TypeCoercion.areEqual(a, b);
    }

    /**
     * Format a row as a comma-separated string for error detail messages.
     *
     * <p>A virtual generated column is shown as the word {@code virtual} rather than as what it
     * works out to. PostgreSQL prints the row as it is stored, and a virtual column is not stored:
     * it is computed again whenever it is read, so there is no value in the row to print. A stored
     * generated column is in the row like any other and prints like one.
     */
    String formatRow(Table table, Object[] row) {
        List<Column> columns = table == null ? null : table.getColumns();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < row.length; i++) {
            if (i > 0) sb.append(", ");
            if (columns != null && i < columns.size()
                    && columns.get(i).isVirtual() && columns.get(i).isGenerated()) {
                sb.append("virtual");
            } else {
                sb.append(row[i] == null ? "null" : row[i].toString());
            }
        }
        return sb.toString();
    }

    /**
     * Pre-flight validation: check WHERE clause for column=literal type mismatches
     * that PG would catch at plan time, even on empty tables.
     */
    void validateWhereTypesAgainstTable(Expression where, Table table) {
        if (where instanceof CustomOperatorExpr) {
            CustomOperatorExpr cop = (CustomOperatorExpr) where;
            if (cop.left() != null) validateWhereTypesAgainstTable(cop.left(), table);
            validateWhereTypesAgainstTable(cop.right(), table);
            return;
        }
        if (where instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) where;
            if (bin.op() == BinaryExpr.BinOp.AND || bin.op() == BinaryExpr.BinOp.OR) {
                validateWhereTypesAgainstTable(bin.left(), table);
                validateWhereTypesAgainstTable(bin.right(), table);
                return;
            }
            if (bin.op() == BinaryExpr.BinOp.EQUAL || bin.op() == BinaryExpr.BinOp.NOT_EQUAL
                    || bin.op() == BinaryExpr.BinOp.LESS_THAN || bin.op() == BinaryExpr.BinOp.GREATER_THAN
                    || bin.op() == BinaryExpr.BinOp.LESS_EQUAL || bin.op() == BinaryExpr.BinOp.GREATER_EQUAL) {
                // Check for column = literal type mismatch
                ColumnRef col = null;
                Literal lit = null;
                if (bin.left() instanceof ColumnRef && bin.right() instanceof Literal) {
                    Literal l = (Literal) bin.right();
                    ColumnRef cr = (ColumnRef) bin.left();
                    col = cr; lit = l;
                } else if (bin.right() instanceof ColumnRef && bin.left() instanceof Literal) {
                    Literal l = (Literal) bin.left();
                    ColumnRef cr = (ColumnRef) bin.right();
                    col = cr; lit = l;
                }
                if (col != null && lit != null && lit.literalType() == Literal.LiteralType.STRING) {
                    int colIdx = table.getColumnIndex(col.column());
                    if (colIdx >= 0) {
                        Column column = table.getColumns().get(colIdx);
                        DataType dt = column.getType();
                        if (dt == DataType.INTEGER || dt == DataType.BIGINT || dt == DataType.SMALLINT) {
                            String sVal = lit.value();
                            try { Long.parseLong(sVal); } catch (NumberFormatException e) {
                                throw new MemgresException(
                                    "invalid input syntax for type integer: \"" + sVal + "\"", "22P02");
                            }
                        }
                    }
                }
                // Check for numeric literal vs text/varchar column (PG rejects: operator does not exist: text = integer)
                if (col != null && lit != null && (lit.literalType() == Literal.LiteralType.INTEGER || lit.literalType() == Literal.LiteralType.FLOAT)) {
                    int colIdx = table.getColumnIndex(col.column());
                    if (colIdx >= 0) {
                        Column column = table.getColumns().get(colIdx);
                        DataType dt = column.getType();
                        if (dt == DataType.TEXT || dt == DataType.VARCHAR || dt == DataType.CHAR) {
                            String opSym = bin.op() == BinaryExpr.BinOp.EQUAL ? "=" :
                                    bin.op() == BinaryExpr.BinOp.NOT_EQUAL ? "<>" :
                                    bin.op() == BinaryExpr.BinOp.LESS_THAN ? "<" :
                                    bin.op() == BinaryExpr.BinOp.GREATER_THAN ? ">" :
                                    bin.op() == BinaryExpr.BinOp.LESS_EQUAL ? "<=" : ">=";
                            throw new MemgresException(
                                    "operator does not exist: text " + opSym + " integer", "42883");
                        }
                    }
                }
            }
        }
    }

    void validateOperatorTypes(BinaryExpr.BinOp op, Object left, Object right) {
        // Skip validation when either operand is null (SQL NULL semantics)
        if (left == null || right == null) return;

        boolean leftIsBitString = left instanceof AstExecutor.PgBitString;
        boolean rightIsBitString = right instanceof AstExecutor.PgBitString;
        boolean leftIsGeometric = left instanceof String && GeometricOperations.isGeometricString(((String) left));
        boolean rightIsGeometric = right instanceof String && GeometricOperations.isGeometricString(((String) right));

        // String - String: PG says "operator is not unique" (42725). An infinity keeps its
        // timestamp type through the literal, so subtracting one is a real timestamp operation.
        boolean leftIsInfinity = left instanceof String && isInfinityWord((String) left);
        boolean rightIsInfinity = right instanceof String && isInfinityWord((String) right);
        if (op == BinaryExpr.BinOp.SUBTRACT && left instanceof String && right instanceof String
                && !leftIsInfinity && !rightIsInfinity
                && !leftIsGeometric && !((String) left).trim().startsWith("{") && !((String) left).trim().startsWith("[")
                && !RangeOperations.isRangeString(((String) left))
                && !isLsnString(((String) left).trim())) {
            String rs = (String) right;
            String ls = (String) left;
            throw new MemgresException("operator is not unique: unknown - unknown", "42725");
        }

        // DIVIDE with range operands: no such operator (42883), but not geometric types
        if (op == BinaryExpr.BinOp.DIVIDE && left instanceof String && right instanceof String
                && !leftIsGeometric && !rightIsGeometric
                && RangeOperations.isRangeString(((String) left)) && RangeOperations.isRangeString(((String) right))) {
            String rs = (String) right;
            String ls = (String) left;
            throw new MemgresException("operator does not exist: int4range / int4range", "42883");
        }

        // MULTIPLY or DIVIDE with jsonb operand: no such operator (42883)
        if (op == BinaryExpr.BinOp.MULTIPLY || op == BinaryExpr.BinOp.DIVIDE) {
            String ls2 = left.toString().trim();
            String rs2 = right.toString().trim();
            String opSym2 = op == BinaryExpr.BinOp.MULTIPLY ? "*" : "/";
            if ((ls2.startsWith("{") || ls2.startsWith("[")) && !leftIsGeometric && !(left instanceof List<?>)
                    && !RangeOperations.isRangeString(ls2) && !RangeOperations.isMultirangeOrEmpty(ls2)) {
                throw new MemgresException("operator does not exist: jsonb " + opSym2 + " integer", "42883");
            }
            if ((rs2.startsWith("{") || rs2.startsWith("[")) && !rightIsGeometric && !(right instanceof List<?>)
                    && !RangeOperations.isRangeString(rs2) && !RangeOperations.isMultirangeOrEmpty(rs2)) {
                throw new MemgresException("operator does not exist: integer " + opSym2 + " jsonb", "42883");
            }
        }

        // CONTAINS (@>) with List operands of mismatched element types -> 42883
        if (op == BinaryExpr.BinOp.CONTAINS && left instanceof List<?> && right instanceof List<?>) {
            List<?> ll = (List<?>) left;
            List<?> rl = (List<?>) right;
            Object leftFirst = ll.stream().filter(java.util.Objects::nonNull).findFirst().orElse(null);
            Object rightFirst = rl.stream().filter(java.util.Objects::nonNull).findFirst().orElse(null);
            if (leftFirst != null && rightFirst != null) {
                boolean leftNum = leftFirst instanceof Number;
                boolean rightNum = rightFirst instanceof Number;
                if (leftNum != rightNum) {
                    throw new MemgresException(
                            "operator does not exist: integer[] @> text[]", "42883");
                }
            }
        }

        // 1. Arithmetic ops with boolean operand: integer + boolean, etc.
        if (op == BinaryExpr.BinOp.ADD || op == BinaryExpr.BinOp.SUBTRACT
                || op == BinaryExpr.BinOp.MULTIPLY || op == BinaryExpr.BinOp.DIVIDE
                || op == BinaryExpr.BinOp.MODULO) {
            if (left instanceof Boolean || right instanceof Boolean) {
                String leftType = pgTypeNameOf(left);
                String rightType = pgTypeNameOf(right);
                String opSym;
                switch (op) {
                    case ADD:
                        opSym = "+";
                        break;
                    case SUBTRACT:
                        opSym = "-";
                        break;
                    case MULTIPLY:
                        opSym = "*";
                        break;
                    case DIVIDE:
                        opSym = "/";
                        break;
                    case MODULO:
                        opSym = "%";
                        break;
                    default:
                        opSym = op.name();
                        break;
                }
                throw new MemgresException(
                        "operator does not exist: " + leftType + " " + opSym + " " + rightType, "42883");
            }
            // Bit string arithmetic: B'1010' + B'0101', not supported
            if (leftIsBitString || rightIsBitString) {
                String opSym;
                switch (op) {
                    case ADD:
                        opSym = "+";
                        break;
                    case SUBTRACT:
                        opSym = "-";
                        break;
                    case MULTIPLY:
                        opSym = "*";
                        break;
                    case DIVIDE:
                        opSym = "/";
                        break;
                    case MODULO:
                        opSym = "%";
                        break;
                    default:
                        opSym = op.name();
                        break;
                }
                throw new MemgresException(
                        "operator does not exist: bit " + opSym + " bit", "42883");
            }
            // text +/-/%/modulo number: only valid if text is a numeric string (implicit cast)
            // text + integer where text is non-numeric: 42883 (no such operator, PG plan-time error)
            if ((op == BinaryExpr.BinOp.ADD || op == BinaryExpr.BinOp.SUBTRACT
                    || op == BinaryExpr.BinOp.MODULO) && left instanceof String
                    && !leftIsGeometric && !(((String) left).trim().startsWith("{") || ((String) left).trim().startsWith("["))
                    && !RangeOperations.isRangeString(((String) left)) && right instanceof Number
                    && !isNumericString(((String) left))) {
                String ls3 = (String) left;
                String opSym;
                switch (op) {
                    case ADD:
                        opSym = "+";
                        break;
                    case SUBTRACT:
                        opSym = "-";
                        break;
                    case MODULO:
                        opSym = "%";
                        break;
                    default:
                        opSym = op.name();
                        break;
                }
                throw new MemgresException("operator does not exist: text " + opSym + " integer", "42883");
            }
        }

        // 2. boolean || boolean (string concat with booleans)
        if (op == BinaryExpr.BinOp.CONCAT && left instanceof Boolean && right instanceof Boolean) {
            throw new MemgresException("operator does not exist: boolean || boolean", "42883");
        }

        // Geometry || geometry (concat not supported for geometric types)
        if (op == BinaryExpr.BinOp.CONCAT && leftIsGeometric && rightIsGeometric) {
            throw new MemgresException("operator does not exist: point || point", "42883");
        }

        // JSONB || non-jsonb: only jsonb || jsonb is valid
        if (op == BinaryExpr.BinOp.CONCAT) {
            String ls = left.toString().trim();
            String rs = right.toString().trim();
            boolean leftIsPgArray = ls.startsWith("{") && ls.endsWith("}") && !ls.startsWith("{\"");
            boolean leftIsJson = (ls.startsWith("{") || ls.startsWith("[")) && !leftIsGeometric && !leftIsPgArray;
            // json || integer: reject
            if (leftIsJson && !(left instanceof List<?>) && !(left instanceof TsVector) && !(right instanceof TsVector)) {
                if (right instanceof Number) {
                    throw new MemgresException("operator does not exist: jsonb || integer", "42883");
                }
            }
            // Array concat type checking: two arrays with incompatible element types
            if (left instanceof List<?> && right instanceof List<?>) {
                List<?> ll = (List<?>) left;
                List<?> rl = (List<?>) right;
                Object leftFirst = ll.stream().filter(java.util.Objects::nonNull).findFirst().orElse(null);
                Object rightFirst = rl.stream().filter(java.util.Objects::nonNull).findFirst().orElse(null);
                // A sub-array as an element means the operands differ in dimension, not in element
                // type: {{1,2},{3,4}} || {5,6} is a legal concatenation and gains a row.
                boolean sameDimension = !(leftFirst instanceof List<?>) && !(rightFirst instanceof List<?>);
                if (sameDimension && leftFirst != null && rightFirst != null) {
                    boolean leftNum = leftFirst instanceof Number;
                    boolean rightNum = rightFirst instanceof Number;
                    if (leftNum && !rightNum) {
                        // Check if right element is parseable as number
                        boolean rightParseable = rightFirst instanceof String && isNumericString(((String) rightFirst));
                        if (!rightParseable) {
                            throw new MemgresException(
                                    "operator does not exist: integer[] || text[]", "42883");
                        }
                    } else if (!leftNum && rightNum) {
                        boolean leftParseable = leftFirst instanceof String && isNumericString(((String) leftFirst));
                        if (!leftParseable) {
                            throw new MemgresException(
                                    "operator does not exist: text[] || integer[]", "42883");
                        }
                    }
                }
            }
            // Array || scalar type checking
            if (left instanceof List<?> && !(right instanceof List<?>) && !(right instanceof TsVector)) {
                List<?> ll = (List<?>) left;
                // ARRAY || geometry: reject
                if (rightIsGeometric) {
                    throw new MemgresException("operator does not exist: integer[] || point", "42883");
                }
                if (!ll.isEmpty() && right instanceof String) {
                    String s = (String) right;
                    Object firstElem = ll.stream().filter(java.util.Objects::nonNull).findFirst().orElse(null);
                    if (firstElem instanceof Number) {
                        try { Long.parseLong(s); } catch (NumberFormatException e) {
                            try { Double.parseDouble(s); } catch (NumberFormatException e2) {
                                throw new MemgresException(
                                        "invalid input syntax for type integer: \"" + s + "\"", "22P02");
                            }
                        }
                    }
                }
            }
        }

        // point @> integer: containment requires compatible geometry types
        if (op == BinaryExpr.BinOp.CONTAINS) {
            if (leftIsGeometric && right instanceof Number) {
                throw new MemgresException("operator does not exist: point @> integer", "42883");
            }
        }

        // Bit string = integer, not comparable
        if (op == BinaryExpr.BinOp.EQUAL) {
            if (leftIsBitString && !(right instanceof AstExecutor.PgBitString)) {
                throw new MemgresException("operator does not exist: bit = " + pgTypeNameOf(right), "42883");
            }
            if (rightIsBitString && !(left instanceof AstExecutor.PgBitString)) {
                throw new MemgresException("operator does not exist: " + pgTypeNameOf(left) + " = bit", "42883");
            }
        }

        // 5. Array arithmetic: ARRAY + ARRAY, ARRAY - ARRAY
        if ((op == BinaryExpr.BinOp.ADD || op == BinaryExpr.BinOp.SUBTRACT)
                && left instanceof List<?> && right instanceof List<?>) {
            String opSym = op == BinaryExpr.BinOp.ADD ? "+" : "-";
            throw new MemgresException(
                    "operator does not exist: integer[] " + opSym + " integer[]", "42883");
        }
        // Array + scalar: not supported
        if ((op == BinaryExpr.BinOp.ADD || op == BinaryExpr.BinOp.SUBTRACT
                || op == BinaryExpr.BinOp.MULTIPLY || op == BinaryExpr.BinOp.DIVIDE)
                && left instanceof List<?> && right instanceof Number) {
            String opSym;
            switch (op) {
                case ADD:
                    opSym = "+";
                    break;
                case SUBTRACT:
                    opSym = "-";
                    break;
                case MULTIPLY:
                    opSym = "*";
                    break;
                case DIVIDE:
                    opSym = "/";
                    break;
                default:
                    opSym = op.name();
                    break;
            }
            throw new MemgresException("operator does not exist: integer[] " + opSym + " integer", "42883");
        }
        if ((op == BinaryExpr.BinOp.ADD || op == BinaryExpr.BinOp.SUBTRACT
                || op == BinaryExpr.BinOp.MULTIPLY || op == BinaryExpr.BinOp.DIVIDE)
                && left instanceof Number && right instanceof List<?>) {
            String opSym;
            switch (op) {
                case ADD:
                    opSym = "+";
                    break;
                case SUBTRACT:
                    opSym = "-";
                    break;
                case MULTIPLY:
                    opSym = "*";
                    break;
                case DIVIDE:
                    opSym = "/";
                    break;
                default:
                    opSym = op.name();
                    break;
            }
            throw new MemgresException("operator does not exist: integer " + opSym + " integer[]", "42883");
        }

        // Array * Array arithmetic: not supported
        if (op == BinaryExpr.BinOp.MULTIPLY && left instanceof List<?> && right instanceof List<?>) {
            throw new MemgresException("operator does not exist: integer[] * integer[]", "42883");
        }

        // Text arithmetic (* or /): not supported when text is not a valid number
        if ((op == BinaryExpr.BinOp.MULTIPLY || op == BinaryExpr.BinOp.DIVIDE) && left instanceof String
                && !leftIsGeometric && !(((String) left).trim().startsWith("{") || ((String) left).trim().startsWith("["))
                && !RangeOperations.isRangeString(((String) left))) {
            String ls2 = (String) left;
            String opSym = op == BinaryExpr.BinOp.MULTIPLY ? "*" : "/";
            if (right instanceof Number) {
                // text / integer: only valid if text is a numeric string (implicit cast)
                if (!isNumericString(ls2)) {
                    throw new MemgresException("operator does not exist: text " + opSym + " integer", "42883");
                }
            } else if (!(right instanceof String && GeometricOperations.isGeometricString(((String) right)))) {
                // text / text, not supported
                throw new MemgresException("operator does not exist: text " + opSym + " text", "42883");
            }
        }

        // Date * Date: not supported
        if (op == BinaryExpr.BinOp.MULTIPLY
                && left instanceof java.time.LocalDate && right instanceof java.time.LocalDate) {
            throw new MemgresException("operator does not exist: date * date", "42883");
        }

        // Timestamp + Timestamp: not supported
        if (op == BinaryExpr.BinOp.ADD
                && (left instanceof java.time.LocalDateTime || left instanceof java.time.OffsetDateTime)
                && (right instanceof java.time.LocalDateTime || right instanceof java.time.OffsetDateTime)) {
            throw new MemgresException("operator does not exist: timestamp + timestamp", "42883");
        }

        // Interval * Interval: not supported
        if (op == BinaryExpr.BinOp.MULTIPLY && left instanceof PgInterval && right instanceof PgInterval) {
            throw new MemgresException("operator does not exist: interval * interval", "42883");
        }

        // UUID + UUID: not supported
        if (op == BinaryExpr.BinOp.ADD && left instanceof java.util.UUID && right instanceof java.util.UUID) {
            throw new MemgresException("operator does not exist: uuid + uuid", "42883");
        }

        // inet * inet: not supported (but inet - inet and inet + integer are valid)
        if (op == BinaryExpr.BinOp.MULTIPLY && left instanceof String && right instanceof String
                && ((String) left).contains(".") && ((String) right).contains(".")) {
            String rs3 = (String) right;
            String ls3 = (String) left;
            throw new MemgresException("operator does not exist: inet * inet", "42883");
        }

        // integer || integer: not supported (only text/array/jsonb can use ||)
        if (op == BinaryExpr.BinOp.CONCAT && left instanceof Number && right instanceof Number
                && !(left instanceof Boolean) && !(right instanceof Boolean)) {
            throw new MemgresException("operator does not exist: integer || integer", "42883");
        }

        // Array overlap (&&) type mismatch
        if (op == BinaryExpr.BinOp.OVERLAP && left instanceof List<?> && right instanceof List<?>) {
            List<?> ll = (List<?>) left;
            List<?> rl = (List<?>) right;
            Object leftFirst = ll.stream().filter(java.util.Objects::nonNull).findFirst().orElse(null);
            Object rightFirst = rl.stream().filter(java.util.Objects::nonNull).findFirst().orElse(null);
            if (leftFirst != null && rightFirst != null) {
                boolean leftNum = leftFirst instanceof Number;
                boolean rightNum = rightFirst instanceof Number;
                if (leftNum != rightNum) {
                    throw new MemgresException(
                            "operator does not exist: integer[] && text[]", "42883");
                }
            }
        }
    }

    /** Check if a string looks like a valid numeric value (for implicit text→numeric cast). */
    private static boolean isNumericString(String s) {
        if (s == null || Strs.isBlank(s)) return false;
        String t = s.trim();
        if (t.equalsIgnoreCase("infinity") || t.equalsIgnoreCase("-infinity") || t.equalsIgnoreCase("nan")) return true;
        try { Double.parseDouble(t); return true; } catch (NumberFormatException e) { return false; }
    }

    /**
     * Return the PG type name for a runtime Java value (for use in error messages).
     */
    static String pgTypeNameOf(Object value) {
        if (value == null) return "unknown";
        if (value instanceof Boolean) return "boolean";
        if (value instanceof Integer) return "integer";
        if (value instanceof Long) return "bigint";
        if (value instanceof Short) return "smallint";
        if (value instanceof Float) return "real";
        if (value instanceof Double) return "double precision";
        if (value instanceof java.math.BigDecimal) return "numeric";
        if (value instanceof java.time.LocalDate) return "date";
        if (value instanceof java.time.LocalTime) return "time";
        if (value instanceof java.time.LocalDateTime) return "timestamp";
        if (value instanceof java.time.OffsetDateTime) return "timestamp with time zone";
        if (value instanceof PgInterval) return "interval";
        if (value instanceof java.util.UUID) return "uuid";
        if (value instanceof AstExecutor.PgBitString) return "bit";
        if (value instanceof CidrValue) return "cidr";
        if (value instanceof InetValue) return "inet";
        if (value instanceof MacaddrValue) return "macaddr";
        if (value instanceof Macaddr8Value) return "macaddr8";
        if (value instanceof List) return "integer[]";
        return "text";
    }

    /** Check if a string matches the pg_lsn format: hex/hex (e.g., "0/4000000"). */
    /** True for the timestamp infinity words, which carry a temporal type despite being text. */
    private static boolean isInfinityWord(String s) {
        String t = s.trim();
        return t.equalsIgnoreCase("infinity") || t.equalsIgnoreCase("-infinity");
    }

    private static boolean isLsnString(String s) {
        return s.matches("[0-9a-fA-F]+/[0-9a-fA-F]+");
    }

    /** Record an undo entry for cascaded child row deletes so ROLLBACK can restore them. */
    void recordCascadeDeleteUndo(String schemaName, String tableName, List<Object[]> rows) {
        if (rows.isEmpty()) return;
        executor.recordUndo(new Session.DeleteUndo(schemaName, tableName, rows));
        if (executor.session != null) {
            executor.session.trackUncommittedDelete(schemaName + "." + tableName, rows);
        }
    }

    /** Record an undo entry for a cascaded child row update so ROLLBACK can restore it. */
    void recordCascadeUpdateUndo(String schemaName, String tableName, Object[] row, Object[] oldValues) {
        executor.recordUndo(new Session.UpdateUndo(schemaName, tableName, row, oldValues));
        if (executor.session != null) {
            executor.session.trackUncommittedUpdate(schemaName + "." + tableName, row, oldValues);
        }
    }

    /** Check if a child table affected by FK cascade is published and needs replica identity. */
    private void checkChildTableReplicaIdentity(Table childTable, String dmlVerb) {
        if (executor.database.getPublications().isEmpty()) return;
        String tableName = childTable.getName();
        boolean published = false;
        for (Database.PubDef pub : executor.database.getPublications().values()) {
            if (pub.allTables) { published = true; break; }
            for (String t : pub.tables) {
                if (t.equalsIgnoreCase(tableName)) { published = true; break; }
            }
            if (published) break;
        }
        if (published && !childTable.hasUsableReplicaIdentity()) {
            // Worded as PostgreSQL words it for a write named directly: a DELETE is "delete from",
            // and the advice names the write that was refused.
            boolean updating = "update".equals(dmlVerb);
            MemgresException ex = new MemgresException(
                    "cannot " + (updating ? "update" : "delete from") + " table \"" + tableName
                            + "\" because it does not have a replica identity and publishes "
                            + (updating ? "updates" : "deletes"),
                    "55000");
            ex.setHint("To enable " + (updating ? "updating" : "deleting from")
                    + " the table, set REPLICA IDENTITY using ALTER TABLE.");
            throw ex;
        }
    }

}
