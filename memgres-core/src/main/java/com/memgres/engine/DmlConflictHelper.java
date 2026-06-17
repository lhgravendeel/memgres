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
                // Default: use PRIMARY KEY columns
                for (StoredConstraint sc : table.getConstraints()) {
                    if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY) {
                        conflictCols = sc.getColumns();
                        break;
                    }
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
                    String targetExpr = targetExprs.get(i).toLowerCase().replaceAll("\\s+", "");
                    String idxExpr = sc.getColumns().get(i).toLowerCase().replaceAll("\\s+", "");
                    if (!targetExpr.equals(idxExpr)) {
                        allMatch = false;
                        break;
                    }
                }
                if (allMatch && sc.getWhereExpr() != null && conflictWhere == null) continue;
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
                    String targetExpr = targetExprs.get(i).toLowerCase().replaceAll("\\s+", "");
                    String colName = sc.getColumns().get(i).toLowerCase();
                    if (!targetExpr.equals(colName)) {
                        allMatch = false;
                        break;
                    }
                }
                if (allMatch && sc.getWhereExpr() != null && conflictWhere == null) continue;
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
