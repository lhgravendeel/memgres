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

    ConstraintValidator(AstExecutor executor) {
        this.executor = executor;
    }

    void validateConstraints(Table table, Object[] row, Object[] excludeRow) {
        // 1. NOT NULL enforcement
        for (int i = 0; i < table.getColumns().size(); i++) {
            Column col = table.getColumns().get(i);
            if (!col.isNullable() && row[i] == null) {
                throw new MemgresException(
                        "null value in column \"" + col.getName() + "\" violates not-null constraint",
                        "23502");
            }
        }

        // 2. Constraint checks
        for (StoredConstraint sc : table.getConstraints()) {
            // PG 18: NOT ENFORCED constraints are stored but not validated
            if (sc.isNotEnforced()) continue;

            // Deferred constraint handling: defer to COMMIT if currently deferred
            boolean shouldDefer = sc.isDeferrable() && executor.session != null && executor.session.isInTransaction()
                    && executor.session.isConstraintCurrentlyDeferred(sc);

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
        // Compute virtual column values so uniqueness checks work on virtual columns
        boolean hasVirtual = executor.dmlExecutor.hasVirtualColumns(table);
        if (hasVirtual) {
            newRow = executor.dmlExecutor.computeVirtualColumns(table, newRow);
        }
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

                Object[] evalExisting = hasVirtual ? executor.dmlExecutor.computeVirtualColumns(table, existingRow) : existingRow;

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
                    throw new MemgresException(
                            "duplicate key value violates unique constraint \"" + constraintName + "\"",
                            "23505");
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
                            "null value in column \"" + columns.get(i) + "\" violates not-null constraint",
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
        if (whereExpr == null && constraintName != null && !(nullsNotDistinct && hasNull)) {
            TableIndex idx = table.getIndex(constraintName);
            if (idx != null) {
                Object[] conflict = idx.findConflict(newRow, excludeRow);
                if (conflict != null) {
                    throw new MemgresException(
                            "duplicate key value violates unique constraint \"" + constraintName + "\"",
                            "23505");
                }
                return;
            }
        }

        for (Object[] existingRow : table.getRows()) {
            if (excludeRow != null && existingRow == excludeRow) continue;

            Object[] evalExisting = hasVirtual ? executor.dmlExecutor.computeVirtualColumns(table, existingRow) : existingRow;

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
                throw new MemgresException(
                        "duplicate key value violates unique constraint \"" + constraintName + "\"",
                        "23505");
            }
        }
    }

    private void validateCheck(Table table, Object[] row, StoredConstraint sc) {
        // Compute virtual column values so CHECK expressions can reference them
        Object[] evalRow = executor.dmlExecutor.hasVirtualColumns(table) ? executor.dmlExecutor.computeVirtualColumns(table, row) : row;
        RowContext ctx = new RowContext(table, null, evalRow);
        Object result = executor.evalExpr(sc.getCheckExpr(), ctx);
        if (result instanceof Boolean && !((Boolean) result)) {
            Boolean b = (Boolean) result;
            throw new MemgresException(
                    "new row violates check constraint \"" + sc.getName() + "\"",
                    "23514");
        }
    }

    /** Package-visible for deferred constraint checking from Session.commit(). */
    void validateForeignKeyDeferred(Table table, Object[] row, StoredConstraint sc) {
        validateForeignKey(table, row, sc);
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
                throw new MemgresException(
                        "duplicate key value violates unique constraint \"" + sc.getName() + "\"",
                        "23505");
            }
        }
    }

    private void validateForeignKey(Table table, Object[] row, StoredConstraint sc) {
        // Resolve the referenced table
        Table refTable = executor.resolveTableAnySchema(sc.getReferencesTable());

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
            fkVals[i] = row[fkColIndices[i]];
        }

        // NULL FK values are always valid
        for (Object val : fkVals) {
            if (val == null) return;
        }

        // Try O(1) index lookup on referenced table's PK/UNIQUE index
        boolean found = false;
        TableIndex refIdx = findIndexForColumns(refTable, refColNames);
        if (refIdx != null) {
            found = refIdx.containsKey(fkVals);
        } else {
            for (Object[] refRow : refTable.getRows()) {
                boolean allMatch = true;
                for (int i = 0; i < refColIndices.length; i++) {
                    if (!valuesEqual(fkVals[i], refRow[refColIndices[i]])) {
                        allMatch = false;
                        break;
                    }
                }
                if (allMatch) {
                    found = true;
                    break;
                }
            }
        }

        if (!found) {
            throw new MemgresException(
                    "insert or update on table \"" + table.getName() +
                            "\" violates foreign key constraint \"" + sc.getName() + "\"",
                    "23503");
        }
    }

    private void validateExclude(Table table, Object[] newRow, StoredConstraint sc, Object[] excludeRow) {
        List<StoredConstraint.ExcludeElement> elements = sc.getExcludeElements();
        if (elements == null || elements.isEmpty()) return;

        int[] colIndices = new int[elements.size()];
        for (int i = 0; i < elements.size(); i++) {
            colIndices[i] = table.getColumnIndex(elements.get(i).column());
            if (colIndices[i] < 0) return;
        }

        for (Object[] existingRow : table.getRows()) {
            if (existingRow == excludeRow) continue;
            boolean allMatch = true;
            for (int i = 0; i < elements.size(); i++) {
                Object newVal = newRow[colIndices[i]];
                Object existVal = existingRow[colIndices[i]];
                if (newVal == null || existVal == null) { allMatch = false; break; }
                String op = elements.get(i).operator();
                if (!excludeOpMatches(op, newVal, existVal)) { allMatch = false; break; }
            }
            if (allMatch) {
                throw new MemgresException(
                        "conflicting key value violates exclusion constraint \"" + sc.getName() + "\"",
                        "23P01");
            }
        }
    }

    private boolean excludeOpMatches(String op, Object a, Object b) {
        switch (op) {
            case "=":
                return valuesEqual(a, b);
            case "&&":
                return rangesOverlap(a.toString(), b.toString());
            default:
                return false;
        }
    }

    /** Check if two PostgreSQL range literals overlap. */
    private boolean rangesOverlap(String r1, String r2) {
        Object[] p1 = parseRange(r1);
        Object[] p2 = parseRange(r2);
        if (p1 == null || p2 == null) return false;
        String lower1 = (String) p1[0], upper1 = (String) p1[1];
        boolean lowerInc1 = (boolean) p1[2], upperInc1 = (boolean) p1[3];
        String lower2 = (String) p2[0], upper2 = (String) p2[1];
        boolean lowerInc2 = (boolean) p2[2], upperInc2 = (boolean) p2[3];

        // Two ranges overlap if neither is strictly before the other
        // r1 is before r2 if upper1 < lower2 (or <= if either bound is exclusive)
        if (!upper1.isEmpty() && !lower2.isEmpty()) {
            int cmp = upper1.compareTo(lower2);
            if (cmp < 0 || (cmp == 0 && (!upperInc1 || !lowerInc2))) return false;
        }
        if (!upper2.isEmpty() && !lower1.isEmpty()) {
            int cmp = upper2.compareTo(lower1);
            if (cmp < 0 || (cmp == 0 && (!upperInc2 || !lowerInc1))) return false;
        }
        return true;
    }

    /** Parse a range literal like '[2024-01-01,2024-01-05)' into [lower, upper, lowerInc, upperInc]. */
    private Object[] parseRange(String range) {
        if (range == null || range.length() < 3) return null;
        boolean lowerInc = range.charAt(0) == '[';
        boolean upperInc = range.charAt(range.length() - 1) == ']';
        String inner = range.substring(1, range.length() - 1);
        int commaIdx = inner.indexOf(',');
        if (commaIdx < 0) return null;
        String lower = inner.substring(0, commaIdx).trim();
        String upper = inner.substring(commaIdx + 1).trim();
        return new Object[]{lower, upper, lowerInc, upperInc};
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
     * Handle FK ON DELETE actions for all tables that reference the given table.
     */
    void handleFkOnDelete(Table parentTable, Object[] deletedRow) {
        // Find all tables with FK constraints referencing this table
        for (Schema schema : executor.database.getSchemas().values()) {
            for (Table childTable : schema.getTables().values()) {
                for (StoredConstraint sc : childTable.getConstraints()) {
                    if (sc.getType() != StoredConstraint.Type.FOREIGN_KEY) continue;
                    if (sc.isNotEnforced()) continue;
                    if (!sc.getReferencesTable().equalsIgnoreCase(parentTable.getName())) continue;

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

                    switch (sc.getOnDelete()) {
                        case CASCADE: {
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
                                if (matches) deleteSet.add(childRow);
                            }
                            if (!deleteSet.isEmpty()) {
                                childTable.deleteRows(deleteSet);
                            }
                            break;
                        }
                        case SET_NULL: {
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
                                    for (int idx : childColIndices) {
                                        newVals[idx] = null;
                                    }
                                    childTable.updateRowInPlace(childRow, oldVals, newVals);
                                }
                            }
                            break;
                        }
                        case SET_DEFAULT: {
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
                                    for (int i = 0; i < childColIndices.length; i++) {
                                        Column col = childTable.getColumns().get(childColIndices[i]);
                                        newVals[childColIndices[i]] = col.getDefaultValue() != null
                                                ? executor.evaluateDefault(col.getDefaultValue(), col.getType(), col.getParsedDefaultExpr())
                                                : null;
                                    }
                                    childTable.updateRowInPlace(childRow, oldVals, newVals);
                                }
                            }
                            break;
                        }
                        case RESTRICT:
                        case NO_ACTION: {
                            // Check if any child rows reference this parent row
                            for (Object[] childRow : childTable.getRows()) {
                                boolean matches = true;
                                for (int i = 0; i < childColIndices.length; i++) {
                                    if (!valuesEqual(parentVals[i], childRow[childColIndices[i]])) {
                                        matches = false;
                                        break;
                                    }
                                }
                                if (matches) {
                                    throw new MemgresException(
                                            "update or delete on table \"" + parentTable.getName() +
                                                    "\" violates foreign key constraint \"" + sc.getName() +
                                                    "\" on table \"" + childTable.getName() + "\"",
                                            "23503");
                                }
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
        for (Schema schema : executor.database.getSchemas().values()) {
            for (Table childTable : schema.getTables().values()) {
                for (StoredConstraint sc : childTable.getConstraints()) {
                    if (sc.getType() != StoredConstraint.Type.FOREIGN_KEY) continue;
                    if (sc.isNotEnforced()) continue;
                    if (!sc.getReferencesTable().equalsIgnoreCase(parentTable.getName())) continue;

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

                    switch (sc.getOnUpdate()) {
                        case CASCADE: {
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
                                    childTable.updateRowInPlace(childRow, oldVals, newVals);
                                }
                            }
                            break;
                        }
                        case SET_NULL: {
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
                                    for (int idx : childColIndices) {
                                        newVals[idx] = null;
                                    }
                                    childTable.updateRowInPlace(childRow, oldVals, newVals);
                                }
                            }
                            break;
                        }
                        case SET_DEFAULT: {
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
                                    childTable.updateRowInPlace(childRow, oldVals, newVals);
                                }
                            }
                            break;
                        }
                        case RESTRICT:
                        case NO_ACTION: {
                            for (Object[] childRow : childTable.getRows()) {
                                boolean matches = true;
                                for (int i = 0; i < childColIndices.length; i++) {
                                    if (!valuesEqual(oldParentVals[i], childRow[childColIndices[i]])) {
                                        matches = false;
                                        break;
                                    }
                                }
                                if (matches) {
                                    throw new MemgresException(
                                            "update or delete on table \"" + parentTable.getName() +
                                                    "\" violates foreign key constraint \"" + sc.getName() +
                                                    "\" on table \"" + childTable.getName() + "\"",
                                            "23503");
                                }
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    /** Find an index on the given table whose columns match the provided column names (in order). */
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

        // String - String: PG says "operator is not unique" (42725)
        if (op == BinaryExpr.BinOp.SUBTRACT && left instanceof String && right instanceof String
                && !leftIsGeometric && !((String) left).trim().startsWith("{") && !((String) left).trim().startsWith("[")
                && !RangeOperations.isRangeString(((String) left))) {
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
                    && !RangeOperations.isRangeString(ls2)) {
                throw new MemgresException("operator does not exist: jsonb " + opSym2 + " integer", "42883");
            }
            if ((rs2.startsWith("{") || rs2.startsWith("[")) && !rightIsGeometric && !(right instanceof List<?>)
                    && !RangeOperations.isRangeString(rs2)) {
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
                if (leftFirst != null && rightFirst != null) {
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
        if (value instanceof List) return "integer[]";
        return "text";
    }
}
