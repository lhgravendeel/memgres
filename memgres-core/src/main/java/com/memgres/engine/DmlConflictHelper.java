package com.memgres.engine;

import com.memgres.engine.parser.ast.*;

import java.util.*;

/**
 * ON CONFLICT / upsert helpers for DML execution.
 * Extracted from DmlExecutor to separate conflict-resolution concerns.
 */
class DmlConflictHelper {

    private final AstExecutor executor;

    DmlConflictHelper(AstExecutor executor) {
        this.executor = executor;
    }

    /**
     * Hold each operator class written in a conflict target against the key it was written after.
     *
     * <p>A target names the keys of an index, and a key is only that key under the class it is
     * compared with: {@code ON CONFLICT (i text_pattern_ops)} over an integer column names a key
     * no index has. A class the server does not ship is not a mismatch but a name that reaches
     * nothing, which is a complaint of its own.
     */
    private void requireArbiterOpclasses(Table table, List<String> columns, List<String> opclasses) {
        if (opclasses == null) return;
        for (int i = 0; i < columns.size() && i < opclasses.size(); i++) {
            String written = opclasses.get(i);
            if (written == null) continue;
            String name = written;
            int dot = written.lastIndexOf('.');
            if (dot > 0) {
                String schema = written.substring(0, dot);
                if (!"pg_catalog".equalsIgnoreCase(schema)
                        && executor.database.getSchema(schema.toLowerCase(Locale.ROOT)) == null) {
                    throw new MemgresException("schema \"" + schema + "\" does not exist", "3F000");
                }
                name = written.substring(dot + 1);
            }
            if (!CatalogTypeSystemBuilder.shippedClassExists("btree", name)
                    && !executor.database.getUserOperatorClasses().containsKey(name)) {
                throw new MemgresException("operator class \"" + name + "\" does not exist"
                        + " for access method \"btree\"", "42704");
            }
            int index = table.getColumnIndex(columns.get(i));
            if (index < 0) continue;
            String indexed = CatalogTypeSystemBuilder.defaultClassFor("btree",
                    table.getColumns().get(index).getType().getOid());
            if (indexed != null && !indexed.equals(name)) {
                throw new MemgresException("there is no unique or exclusion constraint matching"
                        + " the ON CONFLICT specification", "42P10");
            }
        }
    }

    /** Find an existing row that conflicts with the proposed row on the specified conflict target. */
    Object[] findConflictingRow(Table table, Object[] proposedRow, InsertStmt.OnConflict onConflict) {
        // Handle expression-based conflict targets (e.g. ON CONFLICT ((lower(email))))
        List<String> conflictExprs = onConflict.conflictExpressions();
        if (conflictExprs != null && !conflictExprs.isEmpty()) {
            return findConflictingRowByExpression(table, proposedRow, onConflict);
        }

        List<String> conflictCols = onConflict.columns();

        // If no explicit conflict columns, find PK/UNIQUE constraints
        if (conflictCols == null || conflictCols.isEmpty()) {
            if (onConflict.constraint() != null) {
                // ON CONFLICT ON CONSTRAINT name: find columns from constraint
                for (StoredConstraint sc : table.getConstraints()) {
                    if (sc.getName().equalsIgnoreCase(onConflict.constraint())) {
                        conflictCols = sc.getColumns();
                        break;
                    }
                }
            }
            if (conflictCols == null || conflictCols.isEmpty()) {
                // No explicit target: check ALL unique constraints (PK + UNIQUE).
                // PG's targetless ON CONFLICT DO NOTHING suppresses any unique violation.
                List<List<String>> allUniqueSets = new ArrayList<>();
                for (StoredConstraint sc : table.getConstraints()) {
                    if ((sc.getType() == StoredConstraint.Type.PRIMARY_KEY || sc.getType() == StoredConstraint.Type.UNIQUE)
                            && sc.getColumns() != null && !sc.getColumns().isEmpty()) {
                        allUniqueSets.add(sc.getColumns());
                    }
                }
                if (!allUniqueSets.isEmpty()) {
                    for (List<String> cols : allUniqueSets) {
                        Object[] hit = findRowByColumns(table, proposedRow, cols);
                        if (hit != null) return hit;
                    }
                    return null;
                }
            }
        }

        if (conflictCols == null || conflictCols.isEmpty()) return null;

        // Validate conflict columns exist
        List<String> explicitCols = onConflict.columns();
        if (explicitCols != null && !explicitCols.isEmpty()) {
            for (String col : explicitCols) {
                if (table.getColumnIndex(col) < 0) {
                    throw new MemgresException("column \"" + col + "\" of relation \"" + table.getName() + "\" does not exist", "42703");
                }
            }
            requireArbiterOpclasses(table, explicitCols, onConflict.opclasses());
            // Validate that the specified conflict columns match a unique constraint or primary key
            boolean hasMatchingUnique = false;
            for (StoredConstraint sc : table.getConstraints()) {
                if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY || sc.getType() == StoredConstraint.Type.UNIQUE) {
                    List<String> constraintCols = sc.getColumns();
                    if (constraintCols.size() == explicitCols.size()) {
                        boolean allMatch = true;
                        for (String ec : explicitCols) {
                            if (constraintCols.stream().noneMatch(cc -> cc.equalsIgnoreCase(ec))) {
                                allMatch = false;
                                break;
                            }
                        }
                        if (allMatch) { hasMatchingUnique = true; break; }
                    }
                }
            }
            if (!hasMatchingUnique) {
                throw new MemgresException(
                    "there is no unique or exclusion constraint matching the ON CONFLICT specification", "42P10");
            }
        }

        int[] colIndices = new int[conflictCols.size()];
        Object[] proposedVals = new Object[conflictCols.size()];
        for (int i = 0; i < conflictCols.size(); i++) {
            colIndices[i] = table.getColumnIndex(conflictCols.get(i));
            if (colIndices[i] < 0) return null;
            proposedVals[i] = proposedRow[colIndices[i]];
        }

        // Try O(1) index lookup to find a matching constraint index
        for (StoredConstraint sc : table.getConstraints()) {
            if (sc.getType() != StoredConstraint.Type.PRIMARY_KEY && sc.getType() != StoredConstraint.Type.UNIQUE) continue;
            if (sc.getName() == null) continue;
            List<String> scCols = sc.getColumns();
            if (scCols.size() != conflictCols.size()) continue;
            boolean colMatch = true;
            for (int i = 0; i < conflictCols.size(); i++) {
                if (!conflictCols.get(i).equalsIgnoreCase(scCols.get(i))) { colMatch = false; break; }
            }
            if (!colMatch) continue;
            // A partial index holds only the rows its predicate admits, so a row it would not hold
            // collides with nothing there and a row it does not hold is nothing to collide with.
            // The predicate that decides this is the index's own: the one written beside the target
            // is read to settle which index arbitrates and says nothing about which rows it holds.
            if (sc.getWhereExpr() != null) {
                if (!indexHolds(table, sc.getWhereExpr(), proposedRow)) return null;
                for (Object[] existingRow : table.getRows()) {
                    if (!indexHolds(table, sc.getWhereExpr(), existingRow)) continue;
                    if (sameKey(existingRow, colIndices, proposedVals)) return existingRow;
                }
                return null;
            }
            // The index treats every null key as unlike every other, which is what a key means
            // unless it was declared NULLS NOT DISTINCT: there the rows are compared instead, and
            // one null is the same key as another.
            if (sc.isNullsNotDistinct() && anyNull(proposedVals)) break;
            TableIndex idx = table.getIndex(sc.getName());
            if (idx != null) {
                Object[] conflict = idx.findConflict(proposedRow, null);
                return conflict; // null if no conflict, else the conflicting row
            }
        }

        for (Object[] existingRow : table.getRows()) {
            boolean allMatch = true;
            for (int i = 0; i < colIndices.length; i++) {
                if (!executor.valuesEqual(proposedVals[i], existingRow[colIndices[i]])) {
                    allMatch = false;
                    break;
                }
            }
            if (allMatch) return existingRow;
        }
        return null;
    }

    private static boolean anyNull(Object[] values) {
        for (Object v : values) {
            if (v == null) return true;
        }
        return false;
    }

    /** Whether a partial index holds a row, which is what its own predicate decides. */
    private boolean indexHolds(Table table, Expression predicate, Object[] row) {
        return executor.isTruthy(executor.evalExpr(predicate, new RowContext(table, null, row)));
    }

    /** Whether an existing row carries the same key as the row being written. */
    /** Whether a key over exactly these columns was declared to treat nulls as equal. */
    private boolean keyOverTheseColumnsTreatsNullsAsEqual(Table table, List<String> cols) {
        for (StoredConstraint sc : table.getConstraints()) {
            if (sc.getType() != StoredConstraint.Type.PRIMARY_KEY
                    && sc.getType() != StoredConstraint.Type.UNIQUE) {
                continue;
            }
            if (!sc.isNullsNotDistinct()) continue;
            List<String> scCols = sc.getColumns();
            if (scCols == null || scCols.size() != cols.size()) continue;
            boolean same = true;
            for (int i = 0; i < cols.size(); i++) {
                if (!cols.get(i).equalsIgnoreCase(scCols.get(i))) { same = false; break; }
            }
            if (same) return true;
        }
        return false;
    }

    private boolean sameKey(Object[] existingRow, int[] colIndices, Object[] proposedVals) {
        for (int i = 0; i < colIndices.length; i++) {
            if (!executor.valuesEqual(proposedVals[i], existingRow[colIndices[i]])) return false;
        }
        return true;
    }

    /** Find an existing row that conflicts on the given column set. */
    private Object[] findRowByColumns(Table table, Object[] proposedRow, List<String> cols) {
        int[] colIndices = new int[cols.size()];
        Object[] proposedVals = new Object[cols.size()];
        // A null is unlike every other value and so conflicts with nothing — unless the key it
        // is part of was declared NULLS NOT DISTINCT, which is the whole of what that clause
        // says. Refusing to look made the insert fail on the key it was told how to handle.
        boolean nullsCollide = keyOverTheseColumnsTreatsNullsAsEqual(table, cols);
        for (int i = 0; i < cols.size(); i++) {
            colIndices[i] = table.getColumnIndex(cols.get(i));
            if (colIndices[i] < 0) return null;
            proposedVals[i] = proposedRow[colIndices[i]];
            if (proposedVals[i] == null && !nullsCollide) return null;
        }
        // Try index lookup
        for (StoredConstraint sc : table.getConstraints()) {
            if (sc.getType() != StoredConstraint.Type.PRIMARY_KEY && sc.getType() != StoredConstraint.Type.UNIQUE) continue;
            if (sc.getName() == null) continue;
            List<String> scCols = sc.getColumns();
            if (scCols.size() != cols.size()) continue;
            boolean colMatch = true;
            for (int i = 0; i < cols.size(); i++) {
                if (!cols.get(i).equalsIgnoreCase(scCols.get(i))) { colMatch = false; break; }
            }
            if (!colMatch) continue;
            // As above: a key declared NULLS NOT DISTINCT is compared row by row, because the
            // index has no entry for a null key to find.
            if (sc.isNullsNotDistinct() && anyNull(proposedVals)) break;
            TableIndex idx = table.getIndex(sc.getName());
            if (idx != null) {
                return idx.findConflict(proposedRow, null);
            }
        }
        // Fall back to sequential scan
        for (Object[] existingRow : table.getRows()) {
            boolean allMatch = true;
            for (int i = 0; i < colIndices.length; i++) {
                if (!executor.valuesEqual(proposedVals[i], existingRow[colIndices[i]])) {
                    allMatch = false;
                    break;
                }
            }
            if (allMatch) return existingRow;
        }
        return null;
    }

    /** Find a conflicting row using expression-based conflict targets (e.g. lower(email)). */
    Object[] findConflictingRowByExpression(Table table, Object[] proposedRow, InsertStmt.OnConflict onConflict) {
        List<String> targetExprs = onConflict.conflictExpressions();
        Expression conflictWhere = onConflict.whereClause();

        // Find the matching stored constraint with expression columns
        StoredConstraint matchedConstraint = null;
        boolean matchedAsPlainColumns = false;
        for (StoredConstraint sc : table.getConstraints()) {
            if (sc.getType() != StoredConstraint.Type.PRIMARY_KEY && sc.getType() != StoredConstraint.Type.UNIQUE) continue;

            if (sc.getExpressionColumns() != null && sc.getExpressionColumns().size() == targetExprs.size()) {
                boolean allMatch = true;
                for (int i = 0; i < targetExprs.size(); i++) {
                    String targetExpr = targetExprs.get(i).toLowerCase(java.util.Locale.ROOT).replaceAll("\\s+", "");
                    String idxExpr = sc.getColumns().get(i).toLowerCase(java.util.Locale.ROOT).replaceAll("\\s+", "");
                    if (!targetExpr.equals(idxExpr)) {
                        allMatch = false;
                        break;
                    }
                }
                if (allMatch && !ArbiterPredicate.infers(table, conflictWhere, sc.getWhereExpr())) continue;
                if (allMatch) {
                    matchedConstraint = sc;
                    break;
                }
            }
            // Also match bare column name expressions against regular column constraints
            // e.g., ON CONFLICT ((id)) where expression is "id" matches PRIMARY KEY (id)
            if (sc.getColumns() != null && sc.getColumns().size() == targetExprs.size()) {
                boolean allMatch = true;
                for (int i = 0; i < targetExprs.size(); i++) {
                    String targetExpr = targetExprs.get(i).toLowerCase(java.util.Locale.ROOT).replaceAll("\\s+", "");
                    String colName = sc.getColumns().get(i).toLowerCase(java.util.Locale.ROOT);
                    if (!targetExpr.equals(colName)) {
                        allMatch = false;
                        break;
                    }
                }
                if (allMatch && !ArbiterPredicate.infers(table, conflictWhere, sc.getWhereExpr())) continue;
                if (allMatch) {
                    matchedConstraint = sc;
                    matchedAsPlainColumns = true;
                    break;
                }
            }
        }

        if (matchedConstraint == null) return null;

        // If matched as plain columns, delegate to the regular column-based conflict finder
        if (matchedAsPlainColumns) {
            List<String> cols = matchedConstraint.getColumns();
            InsertStmt.OnConflict colBased = new InsertStmt.OnConflict(
                    cols, onConflict.constraint(), onConflict.doNothing(), onConflict.doUpdate(),
                    onConflict.whereClause(), null, onConflict.doUpdateWhereClause());
            return findConflictingRow(table, proposedRow, colBased);
        }

        List<Expression> exprCols = matchedConstraint.getExpressionColumns();
        Expression whereExpr = matchedConstraint.getWhereExpr();

        // Evaluate expressions on the proposed row
        RowContext proposedCtx = new RowContext(table, null, proposedRow);

        // Check if proposed row satisfies partial index predicate
        if (whereExpr != null) {
            Object whereResult = executor.evalExpr(whereExpr, proposedCtx);
            if (!(whereResult instanceof Boolean && ((Boolean) whereResult))) {
                return null; // Proposed row doesn't match predicate, so no conflict possible
            }
        }

        Object[] proposedVals = new Object[exprCols.size()];
        for (int i = 0; i < exprCols.size(); i++) {
            proposedVals[i] = executor.evalExpr(exprCols.get(i), proposedCtx);
        }

        // Compare against all existing rows
        for (Object[] existingRow : table.getRows()) {
            if (whereExpr != null) {
                RowContext existingCtx = new RowContext(table, null, existingRow);
                Object existingResult = executor.evalExpr(whereExpr, existingCtx);
                if (!(existingResult instanceof Boolean && ((Boolean) existingResult))) continue;
            }

            RowContext existingCtx = new RowContext(table, null, existingRow);
            boolean allMatch = true;
            for (int i = 0; i < exprCols.size(); i++) {
                Object existingVal = executor.evalExpr(exprCols.get(i), existingCtx);
                if (!executor.valuesEqual(proposedVals[i], existingVal)) {
                    allMatch = false;
                    break;
                }
            }
            if (allMatch) return existingRow;
        }
        return null;
    }

}
