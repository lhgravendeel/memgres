package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Handles DML statement execution: INSERT, UPDATE, DELETE, MERGE, COPY.
 * Extracted from AstExecutor to separate DML concerns from expression evaluation.
 * Delegates to: DmlConflictHelper, DmlPartitionHelper, DmlValidationHelper, DmlTriggerHelper.
 */
class DmlExecutor {

    private final AstExecutor executor;
    private final DmlConflictHelper conflictHelper;
    private final DmlPartitionHelper partitionHelper;
    private final DmlValidationHelper validationHelper;
    private final DmlTriggerHelper triggerHelper;

    DmlExecutor(AstExecutor executor) {
        this.executor = executor;
        this.conflictHelper = new DmlConflictHelper(executor);
        this.partitionHelper = new DmlPartitionHelper(executor);
        this.validationHelper = new DmlValidationHelper(executor);
        this.triggerHelper = new DmlTriggerHelper(executor);
    }

    /** Returns true when session_replication_role suppresses user triggers. */
    private boolean triggersDisabled() {
        if (executor.session == null) return false;
        String role = executor.session.getGucSettings().get("session_replication_role");
        return role != null && role.equalsIgnoreCase("replica");
    }

    /** Push CTE scope, execute action, pop CTE scope. DRYs INSERT/UPDATE/DELETE CTE handling. */
    private <T> T withCteScope(List<SelectStmt.CommonTableExpr> withClauses, java.util.function.Supplier<T> action) {
        boolean pushed = false;
        if (withClauses != null && !withClauses.isEmpty()) {
            Map<String, SelectStmt.CommonTableExpr> cteMap = new LinkedHashMap<>();
            for (SelectStmt.CommonTableExpr cte : withClauses) {
                cteMap.put(cte.name().toLowerCase(), cte);
            }
            executor.cteStack.push(cteMap);
            for (String cteName : cteMap.keySet()) executor.cteResultCache.remove(cteName);
            pushed = true;
        }
        try {
            return action.get();
        } finally {
            if (pushed) executor.cteStack.pop();
        }
    }

    // ---- INSERT ----

    QueryResult executeInsert(InsertStmt stmt) {
        return withCteScope(stmt.withClauses(), () -> executeInsertInner(stmt));
    }

    private QueryResult executeInsertInner(InsertStmt stmt) {
        // Check read-only transaction
        checkReadOnly("INSERT");
        String schemaName = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        // Collect WITH CHECK OPTION constraints from views we're inserting through
        List<Expression> viewCheckExprs = validationHelper.collectViewCheckExprs(stmt.table());
        Table table = executor.resolveTable(schemaName, stmt.table());

        // Check for INSTEAD rules
        String ruleVal = executor.database.getRule(stmt.table(), "INSERT");
        if ("INSTEAD_NOTHING".equals(ruleVal)) {
            return QueryResult.command(QueryResult.Type.INSERT, 0);
        }
        if (ruleVal != null && ruleVal.startsWith("INSTEAD:")) {
            String ruleSql = ruleVal.substring("INSTEAD:".length());
            List<List<Expression>> ruleValueRows = stmt.values();
            if (ruleValueRows != null) {
                for (List<Expression> valueRow : ruleValueRows) {
                    String sql = ruleSql;
                    List<String> colNames = stmt.columns();
                    if (colNames == null) {
                        colNames = new ArrayList<>();
                        for (Column c : table.getColumns()) colNames.add(c.getName());
                    }
                    for (int ci = 0; ci < Math.min(colNames.size(), valueRow.size()); ci++) {
                        Object val = executor.evalExpr(valueRow.get(ci), null);
                        String colName = colNames.get(ci);
                        String replacement = val == null ? "NULL"
                                : val instanceof Number ? val.toString()
                                : "'" + val.toString().replace("'", "''") + "'";
                        sql = sql.replaceAll("(?i)NEW\\s*\\.\\s*" + colName, replacement);
                    }
                    executor.execute(sql, Cols.listOf());
                }
            }
            return QueryResult.command(QueryResult.Type.INSERT, ruleValueRows != null ? ruleValueRows.size() : 0);
        }

        // Validate RETURNING columns exist before processing rows
        validateReturning(stmt.returning(), table);

        List<PgTrigger> triggers = triggersDisabled() ? Cols.listOf() : executor.database.getTriggersForTable(stmt.table());
        // Check for INSTEAD OF triggers (on views)
        boolean hasInsteadOfInsert = triggers.stream().anyMatch(
                t -> t.getTiming() == PgTrigger.Timing.INSTEAD_OF && t.getEvent() == PgTrigger.Event.INSERT);
        int inserted = 0;
        List<Object[]> returningRows = new ArrayList<>();

        // Determine source rows: VALUES list or SELECT subquery
        List<List<Expression>> valueRows;
        if (stmt.selectStmt() != null) {
            // INSERT ... SELECT [UNION/INTERSECT/EXCEPT ...]; execute and convert to value expressions
            QueryResult subResult = executor.executeStatement(stmt.selectStmt());
            valueRows = new ArrayList<>();
            for (Object[] subRow : subResult.getRows()) {
                List<Expression> exprRow = new ArrayList<>();
                for (Object val : subRow) {
                    if (val == null) {
                        exprRow.add(Literal.ofNull());
                    } else if (val instanceof Integer || val instanceof Long) {
                        exprRow.add(Literal.ofInt(val.toString()));
                    } else if (val instanceof Double || val instanceof Float || val instanceof java.math.BigDecimal) {
                        exprRow.add(Literal.ofFloat(val.toString()));
                    } else if (val instanceof Boolean) {
                        Boolean b = (Boolean) val;
                        exprRow.add(Literal.ofBoolean(b));
                    } else {
                        exprRow.add(Literal.ofString(val.toString()));
                    }
                }
                valueRows.add(exprRow);
            }
        } else {
            valueRows = stmt.values();
        }

        List<Object[]> insertedRows = new ArrayList<>(); // for transition tables in statement triggers
        for (List<Expression> valueRow : valueRows) {
            Object[] row = new Object[table.getColumns().size()];

            // Validate column count matches value count FIRST
            if (stmt.columns() != null && stmt.columns().size() != valueRow.size()) {
                throw new MemgresException("INSERT has more " +
                        (stmt.columns().size() > valueRow.size() ? "target columns than expressions" : "expressions than target columns"),
                        "42601");
            } else if (stmt.columns() == null && valueRow.size() > table.getColumns().size()) {
                throw new MemgresException("INSERT has more expressions than target columns", "42601");
            }

            // Fill provided values FIRST (with type coercion); validates before consuming serials
            Set<Integer> filledCols = new HashSet<>();
            if (stmt.columns() != null) {
                for (int i = 0; i < stmt.columns().size(); i++) {
                    // Skip DEFAULT keyword; let the serial/default logic handle it
                    if (valueRow.get(i) instanceof Literal && ((Literal) valueRow.get(i)).literalType() == Literal.LiteralType.DEFAULT) continue;
                    int colIdx = table.getColumnIndex(stmt.columns().get(i));
                    if (colIdx < 0) {
                        throw new MemgresException("Column not found: " + stmt.columns().get(i));
                    }
                    // Reject explicit writes to GENERATED ALWAYS AS ... STORED columns
                    Column genCol = table.getColumns().get(colIdx);
                    if (genCol.isGenerated()) {
                        throw new MemgresException("cannot insert a non-DEFAULT value into column \"" + genCol.getName() + "\"\n  Detail: Column \"" + genCol.getName() + "\" is a generated column.", "428C9");
                    }
                    Object val = executor.evalExpr(valueRow.get(i), null);
                    row[colIdx] = TypeCoercion.coerceForStorage(val, table.getColumns().get(colIdx));
                    filledCols.add(colIdx);
                }
            } else {
                for (int i = 0; i < valueRow.size() && i < row.length; i++) {
                    if (valueRow.get(i) instanceof Literal && ((Literal) valueRow.get(i)).literalType() == Literal.LiteralType.DEFAULT) continue;
                    // Reject explicit writes to GENERATED ALWAYS AS ... STORED columns
                    Column genCol = table.getColumns().get(i);
                    if (genCol.isGenerated()) {
                        throw new MemgresException("cannot insert a non-DEFAULT value into column \"" + genCol.getName() + "\"\n  Detail: Column \"" + genCol.getName() + "\" is a generated column.", "428C9");
                    }
                    Object val = executor.evalExpr(valueRow.get(i), null);
                    row[i] = TypeCoercion.coerceForStorage(val, table.getColumns().get(i));
                    filledCols.add(i);
                }
            }

            // OVERRIDING USER VALUE: ignore explicit values for identity columns, use sequence default
            if (stmt.overridingUserValue()) {
                for (int colIdx : new HashSet<>(filledCols)) {
                    Column col = table.getColumns().get(colIdx);
                    if (col.getDefaultValue() != null && (col.getDefaultValue().contains("__identity__:always")
                            || col.getDefaultValue().contains("__identity__:bydefault"))) {
                        row[colIdx] = null; // clear explicit value, will be filled by default path below
                        filledCols.remove(colIdx);
                    }
                }
            }

            // Check for GENERATED ALWAYS AS IDENTITY columns and reject explicit values
            if (!stmt.overridingSystemValue()) {
                for (int colIdx : filledCols) {
                    Column col = table.getColumns().get(colIdx);
                    if (col.getDefaultValue() != null && col.getDefaultValue().contains("__identity__:always")) {
                        throw new MemgresException("cannot insert a non-DEFAULT value into column \"" + col.getName() + "\"", "428C9");
                    }
                }
            } // end overridingSystemValue check

            // Validate enum values and wrap as PgEnum for correct ordering
            for (int colIdx : filledCols) {
                Column col = table.getColumns().get(colIdx);
                if (col.getType() == DataType.ENUM && row[colIdx] != null) {
                    // Skip validation for enum array columns; elements were already validated during ARRAY expression evaluation
                    if (col.getArrayElementType() != null || (row[colIdx] instanceof String && ((String) row[colIdx]).startsWith("{"))) {
                        String s = (String) row[colIdx];
                        continue;
                    }
                    String enumTypeName = col.getEnumTypeName();
                    if (enumTypeName != null) {
                        CustomEnum customEnum = executor.database.getCustomEnum(enumTypeName);
                        if (customEnum != null) {
                            String label = row[colIdx] instanceof AstExecutor.PgEnum ? ((AstExecutor.PgEnum) row[colIdx]).label() : row[colIdx].toString();
                            if (!customEnum.isValidLabel(label)) {
                                throw new MemgresException(
                                        "invalid input value for enum " + enumTypeName + ": \"" + label + "\"",
                                        "22P02");
                            }
                            row[colIdx] = new AstExecutor.PgEnum(label, enumTypeName, customEnum.ordinal(label));
                        }
                    }
                }
            }

            // Fill serial columns and defaults AFTER explicit values validated
            for (int i = 0; i < table.getColumns().size(); i++) {
                if (filledCols.contains(i)) continue;
                Column col = table.getColumns().get(i);
                if (col.getType() == DataType.SERIAL || col.getType() == DataType.BIGSERIAL || col.getType() == DataType.SMALLSERIAL) {
                    row[i] = table.nextSerial();
                } else if (col.getDefaultValue() != null) {
                    Object defVal = executor.evaluateDefault(col.getDefaultValue(), col.getType(), col.getParsedDefaultExpr());
                    row[i] = TypeCoercion.coerceForStorage(defVal, col);
                } else if (col.getDomainTypeName() != null) {
                    DomainType domain = executor.database.getDomain(col.getDomainTypeName());
                    if (domain != null && domain.getDefaultValue() != null) {
                        row[i] = executor.evaluateDefault(domain.getDefaultValue(), domain.getBaseType());
                    }
                }
            }

            // Apply citext lowercasing for columns with citext-based domains
            validationHelper.applyCitextFolding(table, row);

            // Compute generated columns
            computeGeneratedColumns(table, row);

            // INSTEAD OF INSERT triggers (on views): trigger handles the insert, skip normal path
            if (hasInsteadOfInsert) {
                triggerHelper.executeTriggers(triggers, PgTrigger.Timing.INSTEAD_OF, PgTrigger.Event.INSERT, row, null, table);
                inserted++;
                if (stmt.returning() != null && !stmt.returning().isEmpty()) {
                    returningRows.add(evalReturning(stmt.returning(), table, stmt.alias(), row, null, row));
                }
                continue;
            }

            // BEFORE INSERT triggers
            row = triggerHelper.executeTriggers(triggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.INSERT, row, null, table);

            // Validate enum values
            validationHelper.validateEnumValues(row, table);

            // Validate domain CHECK constraints
            validationHelper.validateDomainChecks(row, table);

            // ON CONFLICT handling
            if (stmt.onConflict() != null) {
                // DO UPDATE requires a conflict target (columns, constraint, or expressions)
                if (stmt.onConflict().doUpdate() != null
                        && (stmt.onConflict().columns() == null || stmt.onConflict().columns().isEmpty())
                        && stmt.onConflict().constraint() == null
                        && (stmt.onConflict().conflictExpressions() == null || stmt.onConflict().conflictExpressions().isEmpty())) {
                    throw new MemgresException("ON CONFLICT DO UPDATE requires inference specification or constraint name\n  "
                            + "Hint: For example, ON CONFLICT (column_name).", "42601");
                }
                // When ON CONFLICT has a WHERE clause, verify a matching partial unique index exists
                if (stmt.onConflict().whereClause() != null && stmt.onConflict().columns() != null) {
                    boolean hasMatchingPartialIndex = false;
                    for (StoredConstraint sc : table.getConstraints()) {
                        if ((sc.getType() == StoredConstraint.Type.UNIQUE || sc.getType() == StoredConstraint.Type.PRIMARY_KEY)
                                && sc.getColumns() != null
                                && sc.getColumns().equals(stmt.onConflict().columns())
                                && sc.getWhereExpr() != null) {
                            hasMatchingPartialIndex = true;
                            break;
                        }
                    }
                    if (!hasMatchingPartialIndex) {
                        throw new MemgresException("there is no unique or exclusion constraint matching the ON CONFLICT specification", "42P10");
                    }
                }
                // When ON CONFLICT uses expression targets, verify a matching unique index exists
                if (stmt.onConflict().conflictExpressions() != null && !stmt.onConflict().conflictExpressions().isEmpty()) {
                    boolean hasMatchingExprIndex = false;
                    List<String> targetExprs = stmt.onConflict().conflictExpressions();
                    for (StoredConstraint sc : table.getConstraints()) {
                        if ((sc.getType() == StoredConstraint.Type.UNIQUE || sc.getType() == StoredConstraint.Type.PRIMARY_KEY)
                                && sc.getExpressionColumns() != null
                                && sc.getExpressionColumns().size() == targetExprs.size()) {
                            // Check that all expression columns match
                            boolean allMatch = true;
                            for (int ei = 0; ei < targetExprs.size(); ei++) {
                                String targetExpr = targetExprs.get(ei).toLowerCase().replaceAll("\\s+", "");
                                String idxExpr = sc.getColumns().get(ei).toLowerCase().replaceAll("\\s+", "");
                                if (!targetExpr.equals(idxExpr)) {
                                    allMatch = false;
                                    break;
                                }
                            }
                            if (allMatch) {
                                // Also check WHERE predicate compatibility:
                                // If the index has a WHERE predicate, ON CONFLICT must also have one
                                if (sc.getWhereExpr() != null && stmt.onConflict().whereClause() == null) {
                                    continue; // partial index requires WHERE in ON CONFLICT
                                }
                                hasMatchingExprIndex = true;
                                break;
                            }
                        }
                    }
                    if (!hasMatchingExprIndex) {
                        throw new MemgresException("there is no unique or exclusion constraint matching the ON CONFLICT specification", "42P10");
                    }
                }
                Object[] conflictRow = conflictHelper.findConflictingRow(table, row, stmt.onConflict());
                if (conflictRow != null) {
                    if (stmt.onConflict().doNothing()) {
                        // DO NOTHING: skip this row
                        continue;
                    } else if (stmt.onConflict().doUpdate() != null) {
                        // DO UPDATE: update the conflicting row
                        Object[] oldRow = Arrays.copyOf(conflictRow, conflictRow.length);
                        table.beforeRowUpdate(conflictRow, oldRow);
                        try {
                            RowContext conflictCtx = new RowContext(table, null, conflictRow);
                            for (InsertStmt.SetClause set : stmt.onConflict().doUpdate()) {
                                int colIdx = table.getColumnIndex(set.column());
                                if (colIdx < 0) {
                                    throw new MemgresException("Column not found: " + set.column());
                                }
                                // In DO UPDATE SET, "excluded" refers to the proposed row
                                // We handle this by making the proposed values available
                                Object val = conflictHelper.evalExprWithExcluded(set.value(), conflictCtx, table, row);
                                conflictRow[colIdx] = TypeCoercion.coerceForStorage(val, table.getColumns().get(colIdx));
                            }
                            table.afterRowUpdate(conflictRow);
                        } catch (Exception e) {
                            // Restore old values and re-add to index on failure
                            System.arraycopy(oldRow, 0, conflictRow, 0, oldRow.length);
                            table.afterRowUpdate(conflictRow);
                            throw e;
                        }
                        executor.constraintValidator.validateConstraints(table, conflictRow, conflictRow);
                        recordUpdateUndo(stmt.schema(), stmt.table(), conflictRow, oldRow);
                        if (stmt.returning() != null && !stmt.returning().isEmpty()) {
                            returningRows.add(evalReturning(stmt.returning(), table, stmt.alias(), conflictRow, oldRow, conflictRow));
                        }
                        inserted++;
                        continue;
                    }
                }
            }

            // Validate WITH CHECK OPTION
            validationHelper.enforceViewCheckOption(viewCheckExprs, table, row);

            // Enforce RLS WITH CHECK policies for INSERT
            if (table.isRlsEnabled()) {
                // Determine the current effective role
                String currentRole = null;
                if (executor.session != null) {
                    currentRole = executor.session.getGucSettings().get("role");
                }
                boolean isSuperuser = currentRole == null
                        || "test".equalsIgnoreCase(currentRole)
                        || "postgres".equalsIgnoreCase(currentRole)
                        || "memgres".equalsIgnoreCase(currentRole);
                if (!isSuperuser || table.isRlsForced()) {
                    String effectiveRole = currentRole != null ? currentRole : "test";
                    boolean anyPolicyMatched = false;
                    for (RlsPolicy policy : table.getRlsPolicies()) {
                        if (policy.appliesTo("INSERT") && policy.getWithCheckExpr() != null
                                && policy.appliesToRole(effectiveRole)) {
                            anyPolicyMatched = true;
                            RowContext rlsCtx = new RowContext(table, null, row);
                            try {
                                Object result = executor.evalExpr(policy.getWithCheckExpr(), rlsCtx);
                                if (!Boolean.TRUE.equals(result)) {
                                    throw new MemgresException(
                                        "new row violates row-level security policy for table \"" + table.getName() + "\"", "42501");
                                }
                            } catch (MemgresException e) {
                                throw e;
                            } catch (Exception e) {
                                throw new MemgresException(
                                    "new row violates row-level security policy for table \"" + table.getName() + "\"", "42501");
                            }
                        }
                    }
                    // If RLS is enabled but no INSERT policies match this role, deny the insert
                    if (!anyPolicyMatched) {
                        throw new MemgresException(
                            "new row violates row-level security policy for table \"" + table.getName() + "\"", "42501");
                    }
                }
            }

            // Validate constraints and insert atomically under table write lock
            // to prevent concurrent INSERTs from both passing unique checks.
            Table targetTable = partitionHelper.routeToPartition(table, row);
            targetTable.getWriteLock().lock();
            try {
                executor.constraintValidator.validateConstraints(table, row, null);
                targetTable.insertRow(row);
            } finally {
                targetTable.getWriteLock().unlock();
            }
            recordInsertUndo(stmt.schema(), targetTable.getName(), row);
            inserted++;
            insertedRows.add(Arrays.copyOf(row, row.length));

            // AFTER INSERT triggers (row-level)
            triggerHelper.executeTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.INSERT, row, null, table);

            // Collect RETURNING row
            if (stmt.returning() != null && !stmt.returning().isEmpty()) {
                returningRows.add(evalReturning(stmt.returning(), table, stmt.alias(), row, null, row));
            }
        }

        // Fire statement-level AFTER triggers with transition tables
        triggerHelper.fireStatementTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.INSERT, table, insertedRows, null);

        if (stmt.returning() != null && !stmt.returning().isEmpty()) {
            List<Column> retCols = buildReturningColumns(stmt.returning(), table);
            return QueryResult.returning(QueryResult.Type.INSERT, retCols, returningRows, inserted);
        }
        return QueryResult.command(QueryResult.Type.INSERT, inserted);
    }

    // ---- COPY ----

    QueryResult executeCopy(CopyStmt stmt) {
        // Validate direction: COPY TO requires STDOUT, COPY FROM requires STDIN
        if (!stmt.isFrom() && "STDIN".equalsIgnoreCase(stmt.source()) && stmt.subquery() == null) {
            throw new MemgresException("COPY TO STDIN is not valid; use STDOUT", "42601");
        }
        if (stmt.isFrom() && "STDOUT".equalsIgnoreCase(stmt.source())) {
            throw new MemgresException("COPY FROM STDOUT is not valid; use STDIN", "42601");
        }

        // Handle PROGRAM: deny it (requires superuser)
        if ("PROGRAM".equalsIgnoreCase(stmt.source())) {
            throw new MemgresException("must be superuser to COPY to or from an external program", "42501");
        }

        // Deny server-side file access (PG 18: must_be_superuser for file I/O)
        if (stmt.source() != null && !"STDIN".equalsIgnoreCase(stmt.source()) && !"STDOUT".equalsIgnoreCase(stmt.source())) {
            throw new MemgresException("must be superuser to COPY to or from a file", "42501");
        }

        // Handle COPY (subquery) TO form
        if (stmt.subquery() != null) {
            // Execute the subquery (validates table references, producing 42P01 for missing tables)
            QueryResult subResult = executor.executeStatement(stmt.subquery());
            return QueryResult.copyOut(subResult.getColumns(), subResult.getRows(), stmt);
        }

        if (!stmt.isFrom()) {
            // Check if target is a view
            Database.ViewDef viewDef = executor.database.getView(stmt.table());
            if (viewDef != null) {
                // Execute the view query to get filtered results
                String colList = (stmt.columns() != null && !stmt.columns().isEmpty())
                        ? String.join(", ", stmt.columns())
                        : "*";
                String sql = "SELECT " + colList + " FROM " + stmt.table();
                if (stmt.whereClause() != null && !stmt.whereClause().isEmpty()) {
                    sql += " WHERE " + stmt.whereClause();
                }
                QueryResult viewResult = executor.execute(sql, Cols.listOf());
                return QueryResult.copyOut(viewResult.getColumns(), viewResult.getRows(), stmt);
            }
            // COPY TO STDOUT: if WHERE clause present, use SELECT to filter
            if (stmt.whereClause() != null && !stmt.whereClause().isEmpty()) {
                String colList = (stmt.columns() != null && !stmt.columns().isEmpty())
                        ? String.join(", ", stmt.columns())
                        : "*";
                String sql = "SELECT " + colList + " FROM " + stmt.table() + " WHERE " + stmt.whereClause();
                QueryResult selectResult = executor.execute(sql, Cols.listOf());
                return QueryResult.copyOut(selectResult.getColumns(), selectResult.getRows(), stmt);
            }
            String copySchema = "public";
            String copyTableName = stmt.table();
            if (copyTableName.contains(".")) {
                int dot = copyTableName.indexOf('.');
                copySchema = copyTableName.substring(0, dot);
                copyTableName = copyTableName.substring(dot + 1);
            }
            Table table = executor.resolveTable(copySchema, copyTableName);
            List<Column> columns;
            List<Integer> colIndices = new ArrayList<>();
            if (stmt.columns() != null && !stmt.columns().isEmpty()) {
                columns = new ArrayList<>();
                for (String colName : stmt.columns()) {
                    int idx = table.getColumnIndex(colName);
                    if (idx < 0) throw new MemgresException(
                        "column \"" + colName + "\" of relation \"" + stmt.table() + "\" does not exist", "42703");
                    colIndices.add(idx);
                    columns.add(table.getColumns().get(idx));
                }
            } else {
                columns = table.getColumns();
                for (int i = 0; i < columns.size(); i++) colIndices.add(i);
            }
            boolean copyHasVirtual = hasVirtualColumns(table);
            List<Object[]> rows = new ArrayList<>();
            for (Object[] tableRow : table.getRows()) {
                Object[] srcRow = copyHasVirtual ? computeVirtualColumns(table, tableRow) : tableRow;
                Object[] row = new Object[colIndices.size()];
                for (int i = 0; i < colIndices.size(); i++) {
                    row[i] = srcRow[colIndices.get(i)];
                }
                rows.add(row);
            }
            return QueryResult.copyOut(columns, rows, stmt);
        }

        // COPY FROM STDIN: validate table/columns, then return COPY_IN for PgWireHandler
        String fromSchema = "public";
        String fromTableName = stmt.table();
        if (fromTableName.contains(".")) {
            int dot = fromTableName.indexOf('.');
            fromSchema = fromTableName.substring(0, dot);
            fromTableName = fromTableName.substring(dot + 1);
        }
        Table table = executor.resolveTable(fromSchema, fromTableName);
        if (stmt.columns() != null && !stmt.columns().isEmpty()) {
            Set<String> seen = new java.util.HashSet<>();
            for (String colName : stmt.columns()) {
                int idx = table.getColumnIndex(colName);
                if (idx < 0) throw new MemgresException(
                    "column \"" + colName + "\" of relation \"" + stmt.table() + "\" does not exist", "42703");
                if (!seen.add(colName.toLowerCase())) {
                    throw new MemgresException(
                        "column \"" + colName + "\" specified more than once", "42701");
                }
            }
        }

        // If inline data is present (non-wire COPY FROM), handle it directly
        if (stmt.inlineData() != null) {
            int inserted = 0;
            for (List<String> dataRow : stmt.inlineData()) {
                Object[] row = new Object[table.getColumns().size()];
                fillDefaults(table, row);
                List<Integer> colIndices = resolveColumnIndices(stmt, table);
                for (int i = 0; i < dataRow.size() && i < colIndices.size(); i++) {
                    String val = dataRow.get(i);
                    if (val.equals(stmt.nullString())) {
                        row[colIndices.get(i)] = null;
                    } else {
                        row[colIndices.get(i)] = TypeCoercion.coerceForStorage(val, table.getColumns().get(colIndices.get(i)));
                    }
                }
                executor.constraintValidator.validateConstraints(table, row, null);
                table.insertRow(row);
                recordInsertUndo(null, stmt.table(), row);
                inserted++;
            }
            return QueryResult.command(QueryResult.Type.INSERT, inserted);
        }

        return QueryResult.copyIn(stmt);
    }

    /** Insert a single row during COPY FROM, called from PgWireHandler.
     *  Values list: null entries mean NULL, non-null entries are data values.
     *  Returns the inserted row Object[] (used for atomicity rollback). */
    Object[] executeCopyFromRow(CopyStmt stmt, List<String> values) {
        String rowSchema = "public";
        String rowTableName = stmt.table();
        if (rowTableName.contains(".")) {
            int dot = rowTableName.indexOf('.');
            rowSchema = rowTableName.substring(0, dot);
            rowTableName = rowTableName.substring(dot + 1);
        }
        Table table = executor.resolveTable(rowSchema, rowTableName);
        Object[] row = new Object[table.getColumns().size()];
        fillDefaults(table, row);
        List<Integer> colIndices = resolveColumnIndices(stmt, table);

        // Validate column count
        if (values.size() > colIndices.size()) {
            throw new MemgresException("extra data after last expected column", "22P04");
        }
        if (values.size() < colIndices.size()) {
            throw new MemgresException("missing data for column \"" +
                table.getColumns().get(colIndices.get(values.size())).getName() + "\"", "22P04");
        }

        // Check for GENERATED ALWAYS identity columns and reject explicit non-null values
        for (int i = 0; i < values.size(); i++) {
            int colIdx = colIndices.get(i);
            Column col = table.getColumns().get(colIdx);
            if (col.getDefaultValue() != null && col.getDefaultValue().contains("__identity__:always")) {
                if (values.get(i) != null) {
                    throw new MemgresException("cannot insert a non-DEFAULT value into column \"" +
                        col.getName() + "\"\n  Detail: Column \"" + col.getName() +
                        "\" is an identity column defined as GENERATED ALWAYS.", "428C9");
                }
            }
        }

        // FORCE_NOT_NULL: for specified columns, convert null to empty string
        List<String> forceNotNull = stmt.forceNotNull();

        for (int i = 0; i < values.size() && i < colIndices.size(); i++) {
            String val = values.get(i);
            int colIdx = colIndices.get(i);
            Column col = table.getColumns().get(colIdx);

            // DEFAULT option: replace marker string with column default
            if (val != null && stmt.defaultString() != null && val.equals(stmt.defaultString())) {
                // Leave the default value that fillDefaults already set
                continue;
            }

            // FORCE_NOT_NULL: if this column is in the list and value is null, use empty string
            if (val == null && forceNotNull != null) {
                String colName = col.getName();
                if (forceNotNull.contains("*") || forceNotNull.stream().anyMatch(c -> c.equalsIgnoreCase(colName))) {
                    val = "";
                }
            }

            if (val == null) {
                row[colIdx] = null;
            } else {
                row[colIdx] = TypeCoercion.coerceForStorage(val, col);
            }
        }

        // Compute generated columns
        computeGeneratedColumns(table, row);

        // Fire BEFORE INSERT triggers
        List<PgTrigger> triggers = triggersDisabled() ? Cols.listOf() : executor.database.getTriggersForTable(stmt.table());
        if (triggers != null && !triggers.isEmpty()) {
            Object[] modified = triggerHelper.executeTriggers(triggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.INSERT, row, null, table);
            if (modified == null) return null; // BEFORE trigger returned null = skip row
            row = modified;
        }

        executor.constraintValidator.validateConstraints(table, row, null);
        table.insertRow(row);
        recordInsertUndo(null, stmt.table(), row);

        // Fire AFTER INSERT triggers
        if (triggers != null && !triggers.isEmpty()) {
            triggerHelper.executeTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.INSERT, row, null, table);
        }

        return row;
    }

    private void fillDefaults(Table table, Object[] row) {
        for (int i = 0; i < table.getColumns().size(); i++) {
            Column col = table.getColumns().get(i);
            if (col.getType() == DataType.SERIAL || col.getType() == DataType.BIGSERIAL || col.getType() == DataType.SMALLSERIAL) {
                row[i] = table.nextSerial();
            } else if (col.getDefaultValue() != null) {
                Object defVal = executor.evaluateDefault(col.getDefaultValue(), col.getType(), col.getParsedDefaultExpr());
                row[i] = TypeCoercion.coerceForStorage(defVal, col);
            }
        }
    }

    List<Integer> resolveColumnIndices(CopyStmt stmt, Table table) {
        List<Integer> colIndices = new ArrayList<>();
        if (stmt.columns() != null && !stmt.columns().isEmpty()) {
            for (String colName : stmt.columns()) {
                int idx = table.getColumnIndex(colName);
                Column col = table.getColumns().get(idx);
                // Reject explicit generated columns in COPY FROM column list
                if (stmt.isFrom() && col.isGenerated()) {
                    throw new MemgresException("column \"" + colName + "\" is a generated column\n" +
                        "  Detail: Generated columns cannot be used in COPY.", "42601");
                }
                colIndices.add(idx);
            }
        } else {
            for (int i = 0; i < table.getColumns().size(); i++) {
                Column col = table.getColumns().get(i);
                // For COPY FROM, skip generated columns when no explicit column list
                if (stmt.isFrom() && col.isGenerated()) continue;
                colIndices.add(i);
            }
        }
        return colIndices;
    }

    // ---- UPDATE ----

    QueryResult executeUpdate(UpdateStmt stmt) {
        return withCteScope(stmt.withClauses(), () -> executeUpdateInner(stmt));
    }

    private QueryResult executeUpdateInner(UpdateStmt stmt) {
        // Check read-only transaction
        checkReadOnly("UPDATE");
        String schemaName = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        // Collect WITH CHECK OPTION constraints from views we're updating through
        List<Expression> viewCheckExprs = validationHelper.collectViewCheckExprs(stmt.table());
        Table table = executor.resolveTable(schemaName, stmt.table());
        // Validate RETURNING columns exist before processing rows
        validateReturning(stmt.returning(), table);
        List<PgTrigger> triggers = triggersDisabled() ? Cols.listOf() : executor.database.getTriggersForTable(stmt.table());
        List<Object[]> returningRows = new ArrayList<>();

        // Validate: FROM table alias must not conflict with target table alias
        if (stmt.from() != null && !stmt.from().isEmpty()) {
            String targetAlias = (stmt.alias() != null ? stmt.alias() : stmt.table()).toLowerCase();
            for (SelectStmt.FromItem fi : stmt.from()) {
                String fromAlias = partitionHelper.extractFromItemAlias(fi);
                if (fromAlias != null && fromAlias.equalsIgnoreCase(targetAlias)) {
                    throw new MemgresException("table reference \"" + targetAlias + "\" is ambiguous", "42712");
                }
            }
        }

        // Resolve FROM clause tables (for multi-table UPDATE)
        List<RowContext> fromContexts = null;
        if (stmt.from() != null && !stmt.from().isEmpty()) {
            fromContexts = executor.fromResolver.resolveFromClause(stmt.from());
        }

        // Pre-flight type validation of WHERE clause (PG checks at plan time, even on empty tables)
        if (stmt.where() != null) {
            executor.constraintValidator.validateWhereTypesAgainstTable(stmt.where(), table);
        }

        // Pre-flight: reject writes to GENERATED ALWAYS columns (even on empty tables, PG errors at plan time)
        for (InsertStmt.SetClause set : stmt.setClauses()) {
            int colIdx = table.getColumnIndex(set.column());
            if (colIdx >= 0) {
                Column genCol = table.getColumns().get(colIdx);
                if (genCol.isGenerated() && !(set.value() instanceof Literal && ((Literal) set.value()).literalType() == Literal.LiteralType.DEFAULT)) {
                    Literal lit = (Literal) set.value();
                    throw new MemgresException("column \"" + genCol.getName() + "\" can only be updated to DEFAULT\n  Detail: Column \"" + genCol.getName() + "\" is a generated column.", "428C9");
                }
                if (genCol.getDefaultValue() != null && genCol.getDefaultValue().contains("__identity__:always")
                        && !(set.value() instanceof Literal && ((Literal) set.value()).literalType() == Literal.LiteralType.DEFAULT)) {
                    Literal lit2 = (Literal) set.value();
                    throw new MemgresException("column \"" + genCol.getName() + "\" can only be updated to DEFAULT\n  Detail: Column \"" + genCol.getName() + "\" is an identity column defined as GENERATED ALWAYS.", "428C9");
                }
            }
        }

        // Compute set of updated column names (for UPDATE OF trigger filtering)
        Set<String> updatedColumnNames = new HashSet<>();
        for (InsertStmt.SetClause set : stmt.setClauses()) {
            updatedColumnNames.add(set.column().toLowerCase());
        }

        // Collect rows from all partitions for partitioned tables
        List<Object[]> rows = new ArrayList<>();
        if (table.getPartitionStrategy() != null && !table.getPartitions().isEmpty()) {
            List<Table> allTables = new ArrayList<>();
            DmlPartitionHelper.collectAllPartitionTables(table, allTables);
            for (Table t : allTables) {
                rows.addAll(t.getRows());
            }
        } else {
            rows.addAll(table.getRows());
        }

        boolean fromUpdateHasVirtual = hasVirtualColumns(table);
        if (fromContexts != null) {
            // Multi-table UPDATE: join main table with FROM tables
            List<Object[]> matchedRows = new ArrayList<>();
            List<RowContext> matchedContexts = new ArrayList<>();
            for (Object[] row : rows) {
                for (RowContext fromCtx : fromContexts) {
                    Object[] evalRow = fromUpdateHasVirtual ? computeVirtualColumns(table, row) : row;
                    RowContext mainCtx = new RowContext(table, stmt.alias(), evalRow);
                    RowContext combined = mainCtx.merge(fromCtx);
                    if (stmt.where() == null || executor.isTruthy(executor.evalExpr(stmt.where(), combined))) {
                        matchedRows.add(row);
                        matchedContexts.add(combined);
                    }
                }
            }
            // Process matched rows with their FROM context
            Set<Object[]> updated = Collections.newSetFromMap(new IdentityHashMap<>());
            for (int i = 0; i < matchedRows.size(); i++) {
                Object[] row = matchedRows.get(i);
                if (updated.contains(row)) continue; // Each row updated at most once
                updated.add(row);
                Object[] oldRow = Arrays.copyOf(row, row.length);
                Object[] newRow = triggerHelper.executeTriggers(triggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.UPDATE, row, oldRow, table, updatedColumnNames);
                RowContext ctx = matchedContexts.get(i);
                applySetClauses(stmt.setClauses(), table, newRow, ctx);
                computeGeneratedColumns(table, newRow);
                executor.constraintValidator.validateConstraints(table, newRow, row);
                validationHelper.validateDomainChecks(newRow, table);
                recordUpdateUndo(stmt.schema(), stmt.table(), row, oldRow);
                table.beforeRowUpdate(row, oldRow);
                System.arraycopy(newRow, 0, row, 0, row.length);
                table.afterRowUpdate(row);
                executor.constraintValidator.handleFkOnUpdate(table, oldRow, row);
                triggerHelper.executeTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.UPDATE, row, oldRow, table, updatedColumnNames);
                if (stmt.returning() != null && !stmt.returning().isEmpty()) {
                    returningRows.add(evalReturning(stmt.returning(), table, stmt.alias(), row, oldRow, row));
                }
            }
            int count = updated.size();
            // Fire statement-level AFTER UPDATE triggers
            triggerHelper.fireStatementTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.UPDATE, table, null, null);
            if (stmt.returning() != null && !stmt.returning().isEmpty()) {
                return QueryResult.returning(QueryResult.Type.UPDATE,
                        buildReturningColumns(stmt.returning(), table), returningRows, count);
            }
            return QueryResult.command(QueryResult.Type.UPDATE, count);
        }

        // Simple UPDATE (no FROM clause)
        String updateAlias = stmt.alias();
        boolean updateHasVirtual = hasVirtualColumns(table);
        if (stmt.where() != null) {
            rows = rows.stream()
                    .filter(row -> {
                        Object[] evalRow = updateHasVirtual ? computeVirtualColumns(table, row) : row;
                        return executor.isTruthy(executor.evalExpr(stmt.where(), new RowContext(table, updateAlias, evalRow)));
                    })
                    .collect(Collectors.toList());
        }

        for (Object[] row : rows) {
            Object[] oldRow = Arrays.copyOf(row, row.length);

            // Build new row values on a COPY; don't modify live data until validated
            Object[] newRow = Arrays.copyOf(row, row.length);

            // BEFORE UPDATE triggers
            newRow = triggerHelper.executeTriggers(triggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.UPDATE, newRow, oldRow, table, updatedColumnNames);

            Object[] evalRow = updateHasVirtual ? computeVirtualColumns(table, row) : row;
            applySetClauses(stmt.setClauses(), table, newRow, new RowContext(table, updateAlias, evalRow));

            // Recompute generated columns after setting new values
            computeGeneratedColumns(table, newRow);

            // Validate WITH CHECK OPTION
            validationHelper.enforceViewCheckOption(viewCheckExprs, table, newRow);

            // Validate constraints (pass original row reference to exclude self from uniqueness check)
            executor.constraintValidator.validateConstraints(table, newRow, row);

            // Validate domain CHECK constraints
            validationHelper.validateDomainChecks(newRow, table);

            // Record undo before applying the update
            recordUpdateUndo(stmt.schema(), stmt.table(), row, oldRow);

            table.beforeRowUpdate(row, oldRow);
            System.arraycopy(newRow, 0, row, 0, row.length);
            table.afterRowUpdate(row);

            // Handle FK ON UPDATE cascades
            executor.constraintValidator.handleFkOnUpdate(table, oldRow, row);

            // AFTER UPDATE triggers
            triggerHelper.executeTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.UPDATE, row, oldRow, table, updatedColumnNames);

            // Collect RETURNING row
            if (stmt.returning() != null && !stmt.returning().isEmpty()) {
                returningRows.add(evalReturning(stmt.returning(), table, updateAlias, row, oldRow, row));
            }
        }

        // Fire statement-level AFTER UPDATE triggers
        triggerHelper.fireStatementTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.UPDATE, table, null, null);

        if (stmt.returning() != null && !stmt.returning().isEmpty()) {
            return QueryResult.returning(QueryResult.Type.UPDATE,
                    buildReturningColumns(stmt.returning(), table), returningRows, rows.size());
        }
        return QueryResult.command(QueryResult.Type.UPDATE, rows.size());
    }

    /** Apply SET clauses to a row. DRYs the duplicate logic between multi-table and simple UPDATE paths. */
    private void applySetClauses(List<InsertStmt.SetClause> setClauses, Table table, Object[] newRow, RowContext ctx) {
        for (InsertStmt.SetClause set : setClauses) {
            int colIdx = table.getColumnIndex(set.column());
            if (colIdx < 0) {
                throw new MemgresException("Column not found: " + set.column());
            }
            Column genCol = table.getColumns().get(colIdx);
            // Reject explicit writes to GENERATED ALWAYS AS ... STORED columns
            if (genCol.isGenerated() && !(set.value() instanceof Literal && ((Literal) set.value()).literalType() == Literal.LiteralType.DEFAULT)) {
                Literal lit = (Literal) set.value();
                throw new MemgresException("column \"" + genCol.getName() + "\" can only be updated to DEFAULT\n  Detail: Column \"" + genCol.getName() + "\" is a generated column.", "428C9");
            }
            if (genCol.isGenerated()) continue; // DEFAULT, skip (will be recomputed)
            // Reject explicit writes to GENERATED ALWAYS AS IDENTITY columns
            if (genCol.getDefaultValue() != null && genCol.getDefaultValue().contains("__identity__:always")
                    && !(set.value() instanceof Literal && ((Literal) set.value()).literalType() == Literal.LiteralType.DEFAULT)) {
                Literal lit2 = (Literal) set.value();
                throw new MemgresException("column \"" + genCol.getName() + "\" can only be updated to DEFAULT\n  Detail: Column \"" + genCol.getName() + "\" is an identity column defined as GENERATED ALWAYS.", "428C9");
            }
            // For UPDATE SET col = DEFAULT, apply the column's default value
            if (set.value() instanceof Literal && ((Literal) set.value()).literalType() == Literal.LiteralType.DEFAULT) {
                Literal lit = (Literal) set.value();
                Column col = table.getColumns().get(colIdx);
                if (col.getDefaultValue() != null) {
                    newRow[colIdx] = TypeCoercion.coerceForStorage(
                            executor.evaluateDefault(col.getDefaultValue(), col.getType(), col.getParsedDefaultExpr()), col);
                } else {
                    newRow[colIdx] = null;
                }
                continue;
            }
            Object val = executor.evalExpr(set.value(), ctx);
            newRow[colIdx] = TypeCoercion.coerceForStorage(val, table.getColumns().get(colIdx));
        }
    }

    // ---- DELETE ----

    QueryResult executeDelete(DeleteStmt stmt) {
        return withCteScope(stmt.withClauses(), () -> executeDeleteInner(stmt));
    }

    private QueryResult executeDeleteInner(DeleteStmt stmt) {
        // Check read-only transaction
        checkReadOnly("DELETE");
        String schemaName = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        Table table = executor.resolveTable(schemaName, stmt.table());
        // Validate RETURNING columns exist before processing rows
        validateReturning(stmt.returning(), table);
        boolean hasReturning = stmt.returning() != null && !stmt.returning().isEmpty();

        // Validate: USING table alias must not conflict with target table alias
        if (stmt.using() != null && !stmt.using().isEmpty()) {
            String targetAlias = (stmt.alias() != null ? stmt.alias() : stmt.table()).toLowerCase();
            for (SelectStmt.FromItem fi : stmt.using()) {
                String usingAlias = partitionHelper.extractFromItemAlias(fi);
                if (usingAlias != null && usingAlias.equalsIgnoreCase(targetAlias)) {
                    throw new MemgresException("table reference \"" + targetAlias + "\" is ambiguous", "42712");
                }
            }
        }

        if (stmt.where() == null) {
            // Collect all tables (including partitions) for DELETE ALL
            List<Table> allTables = new ArrayList<>();
            DmlPartitionHelper.collectAllPartitionTables(table, allTables);

            List<Object[]> allRowsCopy = new ArrayList<>();
            for (Table t : allTables) {
                allRowsCopy.addAll(t.getRows());
            }
            for (Object[] row : allRowsCopy) {
                executor.constraintValidator.handleFkOnDelete(table, row);
            }
            // Collect RETURNING before deleting
            List<Object[]> returningRows = new ArrayList<>();
            if (hasReturning) {
                for (Object[] row : allRowsCopy) {
                    returningRows.add(evalReturning(stmt.returning(), table, stmt.alias(), row, row, null));
                }
            }
            recordDeleteUndo(stmt.schema(), stmt.table(), allRowsCopy);
            int count = 0;
            for (Table t : allTables) {
                count += t.deleteAll();
            }
            if (hasReturning) {
                return QueryResult.returning(QueryResult.Type.DELETE,
                        buildReturningColumns(stmt.returning(), table), returningRows, count);
            }
            return QueryResult.command(QueryResult.Type.DELETE, count);
        }

        // Collect rows from all partitions (for partitioned tables) or just this table
        List<Table> tablesToScan = new ArrayList<>();
        DmlPartitionHelper.collectAllPartitionTables(table, tablesToScan);

        // Build list of (owningTable, row) pairs
        List<Object[]> allRows = new ArrayList<>();
        Map<Object[], Table> rowOwner = new IdentityHashMap<>();
        for (Table t : tablesToScan) {
            for (Object[] row : t.getRows()) {
                allRows.add(row);
                rowOwner.put(row, t);
            }
        }

        Set<Object[]> toDelete = Collections.newSetFromMap(new IdentityHashMap<>());

        boolean deleteHasVirtual = hasVirtualColumns(table);
        if (stmt.using() != null && !stmt.using().isEmpty()) {
            // DELETE ... USING: join main table with USING tables, delete matching main rows
            List<RowContext> usingContexts = executor.fromResolver.resolveFromClause(stmt.using());
            for (Object[] row : allRows) {
                Object[] evalRow = deleteHasVirtual ? computeVirtualColumns(table, row) : row;
                RowContext mainCtx = new RowContext(table, stmt.alias(), evalRow);
                for (RowContext usingCtx : usingContexts) {
                    // Merge bindings: main table + using table
                    RowContext merged = mainCtx.merge(usingCtx);
                    if (stmt.where() == null || executor.isTruthy(executor.evalExpr(stmt.where(), merged))) {
                        toDelete.add(row);
                        break;
                    }
                }
            }
        } else {
            for (Object[] row : allRows) {
                Object[] evalRow = deleteHasVirtual ? computeVirtualColumns(table, row) : row;
                if (executor.isTruthy(executor.evalExpr(stmt.where(), new RowContext(table, stmt.alias(), evalRow)))) {
                    toDelete.add(row);
                }
            }
        }

        // Validate FK references before deleting (handle CASCADE/RESTRICT/SET NULL/SET DEFAULT)
        for (Object[] row : allRows) {
            if (toDelete.contains(row)) {
                executor.constraintValidator.handleFkOnDelete(table, row);
            }
        }

        // Fire BEFORE DELETE triggers (for DELETE, OLD = deleted row, NEW = null)
        List<PgTrigger> triggers = triggersDisabled() ? Cols.listOf() : executor.database.getTriggersForTable(table.getName());
        if (!triggers.isEmpty()) {
            Set<Object[]> skipRows = Collections.newSetFromMap(new IdentityHashMap<>());
            for (Object[] row : new ArrayList<>(toDelete)) {
                // For DELETE triggers, pass row as OLD; the trigger function should RETURN OLD
                Object[] result = triggerHelper.executeTriggers(triggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.DELETE, row, row, table);
                if (result == null) skipRows.add(row);
            }
            toDelete.removeAll(skipRows);
        }

        List<Object[]> deletedRows = new ArrayList<>();
        List<Object[]> returningRows = new ArrayList<>();
        // Collect returning data before deleting
        if (hasReturning) {
            for (Object[] row : toDelete) {
                returningRows.add(evalReturning(stmt.returning(), table, stmt.alias(), row, row, null));
            }
        }
        deletedRows.addAll(toDelete);
        // Remove matching rows atomically from each owning table
        for (Table t : tablesToScan) {
            t.deleteRows(toDelete);
        }
        int deleted = deletedRows.size();
        recordDeleteUndo(stmt.schema(), stmt.table(), deletedRows);

        // Fire AFTER DELETE triggers (for DELETE, OLD = deleted row)
        if (!triggers.isEmpty()) {
            for (Object[] row : deletedRows) {
                triggerHelper.executeTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.DELETE, row, row, table);
            }
        }

        // Fire statement-level AFTER DELETE triggers
        triggerHelper.fireStatementTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.DELETE, table, null, null);

        if (hasReturning) {
            return QueryResult.returning(QueryResult.Type.DELETE,
                    buildReturningColumns(stmt.returning(), table), returningRows, deleted);
        }
        return QueryResult.command(QueryResult.Type.DELETE, deleted);
    }

    // ---- MERGE ----

    QueryResult executeMerge(MergeStmt stmt) {
        return withCteScope(stmt.withClauses(), () -> executeMergeInner(stmt));
    }

    private QueryResult executeMergeInner(MergeStmt stmt) {
        String schemaName = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        Table targetTable = executor.resolveTable(schemaName, stmt.targetTable());
        String targetAlias = stmt.targetAlias() != null ? stmt.targetAlias() : stmt.targetTable();

        // Validate: source cannot be the same unaliased table as target
        if (stmt.source() instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef src = (SelectStmt.TableRef) stmt.source();
            String srcName = src.table();
            if (srcName.equalsIgnoreCase(stmt.targetTable()) && src.alias() == null && stmt.targetAlias() == null) {
                throw new MemgresException("name \"" + srcName + "\" specified more than once", "42712");
            }
        }

        // Validate: cannot have more than one unconditional WHEN MATCHED clause
        int unconditionalMatched = 0;
        int unconditionalNotMatched = 0;
        int unconditionalNotMatchedBySource = 0;
        for (MergeStmt.WhenClause clause : stmt.whenClauses()) {
            if (clause instanceof MergeStmt.WhenMatched) {
                MergeStmt.WhenMatched wm = (MergeStmt.WhenMatched) clause;
                if (wm.andCondition() == null) unconditionalMatched++;
            } else if (clause instanceof MergeStmt.WhenNotMatched) {
                MergeStmt.WhenNotMatched wnm = (MergeStmt.WhenNotMatched) clause;
                if (wnm.andCondition() == null) unconditionalNotMatched++;
            } else if (clause instanceof MergeStmt.WhenNotMatchedBySource) {
                MergeStmt.WhenNotMatchedBySource wnmbs = (MergeStmt.WhenNotMatchedBySource) clause;
                if (wnmbs.andCondition() == null) unconditionalNotMatchedBySource++;
            }
        }
        if (unconditionalMatched > 1) {
            throw new MemgresException("MERGE command cannot have more than one unconditional WHEN MATCHED clause", "42601");
        }
        if (unconditionalNotMatched > 1) {
            throw new MemgresException("MERGE command cannot have more than one unconditional WHEN NOT MATCHED clause", "42601");
        }
        if (unconditionalNotMatchedBySource > 1) {
            throw new MemgresException("MERGE command cannot have more than one unconditional WHEN NOT MATCHED BY SOURCE clause", "42601");
        }

        // Validate UPDATE SET columns exist in target table before executing
        for (MergeStmt.WhenClause clause : stmt.whenClauses()) {
            List<InsertStmt.SetClause> setsToValidate = null;
            if (clause instanceof MergeStmt.WhenMatched && !((MergeStmt.WhenMatched) clause).isDelete()) {
                setsToValidate = ((MergeStmt.WhenMatched) clause).setClauses();
            } else if (clause instanceof MergeStmt.WhenNotMatchedBySource && !((MergeStmt.WhenNotMatchedBySource) clause).isDelete()) {
                setsToValidate = ((MergeStmt.WhenNotMatchedBySource) clause).setClauses();
            }
            if (setsToValidate != null) {
                for (InsertStmt.SetClause set : setsToValidate) {
                    int colIdx = targetTable.getColumnIndex(set.column());
                    if (colIdx < 0) {
                        throw new MemgresException("column \"" + set.column() + "\" of relation \"" + stmt.targetTable() + "\" does not exist", "42703");
                    }
                }
            }
        }

        // Validate RETURNING columns
        validateReturning(stmt.returning(), targetTable);

        // Resolve source rows
        List<RowContext> sourceRows = executor.fromResolver.resolveFromItem(stmt.source());

        // Snapshot target rows before MERGE so we can roll back on failure
        List<Object[]> snapshotRows = new ArrayList<>();
        for (Object[] row : targetTable.getRows()) {
            snapshotRows.add(Arrays.copyOf(row, row.length));
        }

        int mergeCount = 0;
        boolean hasReturning = stmt.returning() != null && !stmt.returning().isEmpty();
        List<Object[]> returningRows = hasReturning ? new ArrayList<>() : null;
        List<PgTrigger> triggers = triggersDisabled() ? Cols.listOf() : executor.database.getTriggersForTable(stmt.targetTable());

        // Track rows to delete (we must not modify the row list while iterating)
        Set<Object[]> rowsToDelete = Collections.newSetFromMap(new IdentityHashMap<>());
        // Track rows already processed (each target row should be updated/deleted at most once)
        Set<Object[]> processedTargetRows = Collections.newSetFromMap(new IdentityHashMap<>());
        // Collect new rows to insert
        List<Object[]> rowsToInsert = new ArrayList<>();

        // Use snapshot rows for ON-condition matching (PG matches against pre-MERGE state)
        List<Object[]> originalTargetRows = new ArrayList<>(targetTable.getRows());

        try {
        for (RowContext sourceCtx : sourceRows) {
            // Find matching target rows for this source row using the original snapshot
            List<Object[]> matchedTargetRows = new ArrayList<>();
            for (Object[] targetRow : originalTargetRows) {
                if (processedTargetRows.contains(targetRow)) continue;
                RowContext targetCtx = new RowContext(targetTable, targetAlias, targetRow);
                RowContext combined = targetCtx.merge(sourceCtx);
                if (executor.isTruthy(executor.evalExpr(stmt.onCondition(), combined))) {
                    matchedTargetRows.add(targetRow);
                }
            }

            if (!matchedTargetRows.isEmpty()) {
                // WHEN MATCHED clauses
                for (Object[] targetRow : matchedTargetRows) {
                    if (processedTargetRows.contains(targetRow)) continue;
                    RowContext targetCtx = new RowContext(targetTable, targetAlias, targetRow);
                    RowContext combined = targetCtx.merge(sourceCtx);

                    for (MergeStmt.WhenClause clause : stmt.whenClauses()) {
                        if (clause instanceof MergeStmt.WhenMatched) {
                            MergeStmt.WhenMatched wm = (MergeStmt.WhenMatched) clause;
                            // Check optional AND condition
                            if (wm.andCondition() != null && !executor.isTruthy(executor.evalExpr(wm.andCondition(), combined))) {
                                continue;
                            }
                            if (wm.isDelete()) {
                                // DELETE — collect RETURNING before marking for deletion
                                if (hasReturning) {
                                    executor.currentMergeAction = "DELETE";
                                    returningRows.add(evalReturning(stmt.returning(), targetTable, targetAlias, targetRow, targetRow, null));
                                }
                                executor.constraintValidator.handleFkOnDelete(targetTable, targetRow);
                                rowsToDelete.add(targetRow);
                                processedTargetRows.add(targetRow);
                                mergeCount++;
                            } else if (wm.setClauses() != null && !wm.setClauses().isEmpty()) {
                                // UPDATE
                                Object[] oldRow = Arrays.copyOf(targetRow, targetRow.length);
                                for (InsertStmt.SetClause set : wm.setClauses()) {
                                    int colIdx = targetTable.getColumnIndex(set.column());
                                    if (colIdx < 0) {
                                        throw new MemgresException("Column not found: " + set.column());
                                    }
                                    Object val = executor.evalExpr(set.value(), combined);
                                    targetRow[colIdx] = TypeCoercion.coerceForStorage(val, targetTable.getColumns().get(colIdx));
                                }
                                executor.constraintValidator.validateConstraints(targetTable, targetRow, targetRow);
                                recordUpdateUndo(stmt.schema(), stmt.targetTable(), targetRow, oldRow);
                                executor.constraintValidator.handleFkOnUpdate(targetTable, oldRow, targetRow);
                                // Fire AFTER UPDATE triggers for MERGE
                                triggerHelper.executeTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.UPDATE, targetRow, oldRow, targetTable);
                                // Collect RETURNING after update (uses new values)
                                if (hasReturning) {
                                    executor.currentMergeAction = "UPDATE";
                                    returningRows.add(evalReturning(stmt.returning(), targetTable, targetAlias, targetRow, oldRow, targetRow));
                                }
                                processedTargetRows.add(targetRow);
                                mergeCount++;
                            } else {
                                // DO NOTHING — no action, no RETURNING, no count
                                processedTargetRows.add(targetRow);
                            }
                            break; // first matching WHEN clause wins
                        }
                    }
                }
            } else {
                // WHEN NOT MATCHED clauses
                for (MergeStmt.WhenClause clause : stmt.whenClauses()) {
                    if (clause instanceof MergeStmt.WhenNotMatched) {
                        MergeStmt.WhenNotMatched wnm = (MergeStmt.WhenNotMatched) clause;
                        // Check optional AND condition
                        if (wnm.andCondition() != null && !executor.isTruthy(executor.evalExpr(wnm.andCondition(), sourceCtx))) {
                            continue;
                        }
                        if (wnm.doNothing()) {
                            // DO NOTHING
                            break;
                        }
                        // INSERT
                        Object[] newRow = new Object[targetTable.getColumns().size()];
                        // Fill defaults and serial columns
                        fillDefaults(targetTable, newRow);
                        // Fill provided values
                        if (wnm.columns() != null) {
                            for (int i = 0; i < wnm.columns().size(); i++) {
                                int colIdx = targetTable.getColumnIndex(wnm.columns().get(i));
                                if (colIdx < 0) {
                                    throw new MemgresException("Column not found: " + wnm.columns().get(i));
                                }
                                // Reject explicit writes to GENERATED ALWAYS AS ... STORED columns
                                Column genCol = targetTable.getColumns().get(colIdx);
                                if (genCol.isGenerated()) {
                                    throw new MemgresException("cannot insert a non-DEFAULT value into column \"" + genCol.getName() + "\"\n  Detail: Column \"" + genCol.getName() + "\" is a generated column.", "428C9");
                                }
                                // Reject explicit writes to GENERATED ALWAYS AS IDENTITY columns
                                if (genCol.getDefaultValue() != null && genCol.getDefaultValue().contains("__identity__:always")) {
                                    throw new MemgresException("cannot insert a non-DEFAULT value into column \"" + genCol.getName() + "\"", "428C9");
                                }
                                Object val = executor.evalExpr(wnm.values().get(i), sourceCtx);
                                newRow[colIdx] = TypeCoercion.coerceForStorage(val, targetTable.getColumns().get(colIdx));
                            }
                        } else if (wnm.values() != null) {
                            for (int i = 0; i < wnm.values().size() && i < newRow.length; i++) {
                                Column genCol = targetTable.getColumns().get(i);
                                if (genCol.isGenerated()) {
                                    throw new MemgresException("cannot insert a non-DEFAULT value into column \"" + genCol.getName() + "\"\n  Detail: Column \"" + genCol.getName() + "\" is a generated column.", "428C9");
                                }
                                if (genCol.getDefaultValue() != null && genCol.getDefaultValue().contains("__identity__:always")) {
                                    throw new MemgresException("cannot insert a non-DEFAULT value into column \"" + genCol.getName() + "\"", "428C9");
                                }
                                Object val = executor.evalExpr(wnm.values().get(i), sourceCtx);
                                newRow[i] = TypeCoercion.coerceForStorage(val, targetTable.getColumns().get(i));
                            }
                        }
                        computeGeneratedColumns(targetTable, newRow);
                        validationHelper.validateEnumValues(newRow, targetTable);
                        executor.constraintValidator.validateConstraints(targetTable, newRow, null);
                        // Eagerly insert and check PK so we fail early like PG does,
                        // preventing subsequent source rows from advancing identity sequences
                        Table routedTable = partitionHelper.routeToPartition(targetTable, newRow);
                        routedTable.insertRow(newRow);
                        try {
                            executor.constraintValidator.validateConstraints(routedTable, newRow, newRow);
                        } catch (MemgresException e) {
                            routedTable.getRows().remove(newRow);
                            throw e;
                        }
                        rowsToInsert.add(newRow);
                        // Fire AFTER INSERT triggers for MERGE
                        triggerHelper.executeTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.INSERT, newRow, null, targetTable);
                        // Collect RETURNING after insert
                        if (hasReturning) {
                            executor.currentMergeAction = "INSERT";
                            returningRows.add(evalReturning(stmt.returning(), targetTable, targetAlias, newRow, null, newRow));
                        }
                        mergeCount++;
                        break; // first matching WHEN clause wins
                    }
                }
            }
        }

            // WHEN NOT MATCHED BY SOURCE: process target rows that had no source match
            boolean hasNotMatchedBySource = stmt.whenClauses().stream()
                    .anyMatch(c -> c instanceof MergeStmt.WhenNotMatchedBySource);
            if (hasNotMatchedBySource) {
                for (Object[] targetRow : originalTargetRows) {
                    if (processedTargetRows.contains(targetRow)) continue;
                    if (rowsToDelete.contains(targetRow)) continue;
                    RowContext targetCtx = new RowContext(targetTable, targetAlias, targetRow);
                    for (MergeStmt.WhenClause clause : stmt.whenClauses()) {
                        if (clause instanceof MergeStmt.WhenNotMatchedBySource) {
                            MergeStmt.WhenNotMatchedBySource wnmbs = (MergeStmt.WhenNotMatchedBySource) clause;
                            if (wnmbs.andCondition() != null && !executor.isTruthy(executor.evalExpr(wnmbs.andCondition(), targetCtx))) {
                                continue;
                            }
                            if (wnmbs.isDelete()) {
                                if (hasReturning) {
                                    executor.currentMergeAction = "DELETE";
                                    returningRows.add(evalReturning(stmt.returning(), targetTable, targetAlias, targetRow, targetRow, null));
                                }
                                executor.constraintValidator.handleFkOnDelete(targetTable, targetRow);
                                rowsToDelete.add(targetRow);
                            } else if (wnmbs.setClauses() != null && !wnmbs.setClauses().isEmpty()) {
                                Object[] oldRow = Arrays.copyOf(targetRow, targetRow.length);
                                for (InsertStmt.SetClause set : wnmbs.setClauses()) {
                                    int colIdx = targetTable.getColumnIndex(set.column());
                                    if (colIdx < 0) {
                                        throw new MemgresException("Column not found: " + set.column());
                                    }
                                    Object val = executor.evalExpr(set.value(), targetCtx);
                                    targetRow[colIdx] = TypeCoercion.coerceForStorage(val, targetTable.getColumns().get(colIdx));
                                }
                                executor.constraintValidator.validateConstraints(targetTable, targetRow, targetRow);
                                recordUpdateUndo(stmt.schema(), stmt.targetTable(), targetRow, oldRow);
                                executor.constraintValidator.handleFkOnUpdate(targetTable, oldRow, targetRow);
                                triggerHelper.executeTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.UPDATE, targetRow, oldRow, targetTable);
                                if (hasReturning) {
                                    executor.currentMergeAction = "UPDATE";
                                    returningRows.add(evalReturning(stmt.returning(), targetTable, targetAlias, targetRow, oldRow, targetRow));
                                }
                            }
                            // DO NOTHING: empty setClauses, no action
                            processedTargetRows.add(targetRow);
                            mergeCount++;
                            break;
                        }
                    }
                }
            }

            // Apply deletes
            if (!rowsToDelete.isEmpty()) {
                List<Object[]> allRows = new ArrayList<>(targetTable.getRows());
                List<Object[]> deletedRows = new ArrayList<>();
                targetTable.deleteAll();
                for (Object[] row : allRows) {
                    if (rowsToDelete.contains(row)) {
                        deletedRows.add(row);
                    } else {
                        targetTable.insertRow(row);
                    }
                }
                recordDeleteUndo(stmt.schema(), stmt.targetTable(), deletedRows);
            }

            // Validate uniqueness for already-inserted rows (they were inserted eagerly during iteration)
            for (Object[] newRow : rowsToInsert) {
                Table routedTable = partitionHelper.routeToPartition(targetTable, newRow);
                // Row is already in the table, so just validate constraints
                try {
                    executor.constraintValidator.validateConstraints(routedTable, newRow, newRow);
                } catch (MemgresException e) {
                    // Roll back the row we just inserted before re-throwing
                    routedTable.getRows().remove(newRow);
                    throw e;
                }
                recordInsertUndo(stmt.schema(), routedTable.getName(), newRow);
            }
        } catch (MemgresException e) {
            // MERGE is atomic; roll back all changes (updates, deletes, inserts) on failure
            executor.currentMergeAction = null;
            targetTable.deleteAll();
            for (Object[] origRow : snapshotRows) {
                targetTable.insertRow(origRow);
            }
            throw e;
        }

        executor.currentMergeAction = null;
        if (hasReturning) {
            List<Column> retCols = buildReturningColumns(stmt.returning(), targetTable);
            return QueryResult.returning(QueryResult.Type.MERGE, retCols, returningRows, mergeCount);
        }
        return QueryResult.command(QueryResult.Type.MERGE, mergeCount);
    }

    // ---- Helper: compute generated columns ----

    private void computeGeneratedColumns(Table table, Object[] row) {
        for (int i = 0; i < table.getColumns().size(); i++) {
            Column col = table.getColumns().get(i);
            if (col.isGenerated() && !col.isVirtual()) {
                row[i] = evalGeneratedColumn(table, row, col);
            }
        }
    }

    Object evalGeneratedColumn(Table table, Object[] row, Column col) {
        // Substitute column names with their literal values
        String sql = col.getGeneratedExpr();
        for (int j = 0; j < table.getColumns().size(); j++) {
            Column c = table.getColumns().get(j);
            if (c.isGenerated()) continue;
            Object val = row[j];
            String replacement = val == null ? "NULL"
                    : (val instanceof Number || val instanceof Boolean) ? val.toString()
                    : "'" + val.toString().replace("'", "''") + "'";
            sql = sql.replaceAll("(?i)\\b" + java.util.regex.Pattern.quote(c.getName()) + "\\b", replacement);
        }
        try {
            QueryResult result = executor.execute("SELECT " + sql);
            if (!result.getRows().isEmpty() && result.getRows().get(0).length > 0) {
                return TypeCoercion.coerceForStorage(result.getRows().get(0)[0], col);
            }
        } catch (Exception e) { /* ignore */ }
        return null;
    }

    // ---- Helper: compute virtual generated columns on read ----

    /** Check if a table has any VIRTUAL generated columns. */
    boolean hasVirtualColumns(Table table) {
        for (Column col : table.getColumns()) {
            if (col.isVirtual()) return true;
        }
        return false;
    }

    /**
     * Clone a row and fill in VIRTUAL generated column values.
     * The original row is not modified (virtual columns are not stored).
     */
    Object[] computeVirtualColumns(Table table, Object[] row) {
        Object[] result = row.clone();
        for (int i = 0; i < table.getColumns().size(); i++) {
            Column col = table.getColumns().get(i);
            if (col.isVirtual()) {
                result[i] = evalGeneratedColumn(table, result, col);
            }
        }
        return result;
    }

    // ---- Read-only transaction check ----

    private void checkReadOnly(String command) {
        if (executor.session != null && executor.session.isReadOnly()) {
            throw new MemgresException("cannot execute " + command + " in a read-only transaction", "25006");
        }
    }

    // ---- Transaction undo recording ----

    private void recordInsertUndo(String schema, String table, Object[] row) {
        String schemaName = schema != null ? schema : executor.defaultSchema();
        executor.recordUndo(new Session.InsertUndo(schemaName, table, row));
        // Track for MVCC visibility
        if (executor.session != null) {
            executor.session.trackUncommittedInsert(schemaName + "." + table, row);
        }
    }

    private void recordDeleteUndo(String schema, String table, List<Object[]> rows) {
        if (rows.isEmpty()) return;
        String schemaName = schema != null ? schema : executor.defaultSchema();
        executor.recordUndo(new Session.DeleteUndo(schemaName, table, rows));
        // Track for MVCC visibility
        if (executor.session != null) {
            executor.session.trackUncommittedDelete(schemaName + "." + table, rows);
        }
    }

    private void recordUpdateUndo(String schema, String table, Object[] row, Object[] oldValues) {
        String schemaName = schema != null ? schema : executor.defaultSchema();
        executor.recordUndo(new Session.UpdateUndo(schemaName, table, row, oldValues));
        // Track for MVCC visibility
        if (executor.session != null) {
            executor.session.trackUncommittedUpdate(schemaName + "." + table, row, oldValues);
        }
    }

    // ---- RETURNING helpers ----

    /** Evaluate RETURNING expressions for a single row. */
    private Object[] evalReturning(List<SelectStmt.SelectTarget> returning, Table table, Object[] row) {
        return evalReturning(returning, table, null, row);
    }

    private Object[] evalReturning(List<SelectStmt.SelectTarget> returning, Table table, String alias, Object[] row) {
        return evalReturning(returning, table, alias, row, null, null);
    }

    /**
     * Evaluate RETURNING expressions with OLD/NEW support (PG 18).
     * @param oldRow pre-modification row (null for INSERT)
     * @param newRow post-modification row (null for DELETE); when both null, uses 'row' as current
     */
    private Object[] evalReturning(List<SelectStmt.SelectTarget> returning, Table table, String alias,
                                    Object[] row, Object[] oldRow, Object[] newRow) {
        // Compute virtual generated column values for RETURNING evaluation
        if (hasVirtualColumns(table)) {
            row = computeVirtualColumns(table, row);
            if (oldRow != null) oldRow = computeVirtualColumns(table, oldRow);
            if (newRow != null) newRow = computeVirtualColumns(table, newRow);
        }
        // Check if RETURNING references OLD or NEW (qualified column refs or wildcards)
        boolean usesOldNew = false;
        for (SelectStmt.SelectTarget target : returning) {
            if (target.expr() instanceof WildcardExpr) {
                WildcardExpr we = (WildcardExpr) target.expr();
                if (we.table() != null && (we.table().equalsIgnoreCase("OLD") || we.table().equalsIgnoreCase("NEW"))) {
                    usesOldNew = true;
                    break;
                }
            } else if (target.expr() instanceof ColumnRef) {
                ColumnRef cr = (ColumnRef) target.expr();
                if (cr.table() != null && (cr.table().equalsIgnoreCase("old") || cr.table().equalsIgnoreCase("new"))) {
                    usesOldNew = true;
                    break;
                }
            }
            // Also check nested expressions (e.g., NEW.val - OLD.val)
            if (!usesOldNew) {
                usesOldNew = exprReferencesOldNew(target.expr());
            }
        }

        // Build context with OLD/NEW bindings only when needed (avoids ambiguity for unqualified refs)
        List<RowContext.TableBinding> bindings = new ArrayList<>();
        // Primary binding (alias or table name) points to current row (backward compat: unqualified = NEW)
        bindings.add(new RowContext.TableBinding(table, alias, row));
        if (usesOldNew) {
            // OLD binding
            if (oldRow != null) {
                bindings.add(new RowContext.TableBinding(table, "old", oldRow));
            } else {
                bindings.add(new RowContext.TableBinding(table, "old", new Object[table.getColumns().size()]));
            }
            // NEW binding
            if (newRow != null) {
                bindings.add(new RowContext.TableBinding(table, "new", newRow));
            } else if (oldRow != null && row == oldRow) {
                bindings.add(new RowContext.TableBinding(table, "new", new Object[table.getColumns().size()]));
            } else {
                bindings.add(new RowContext.TableBinding(table, "new", row));
            }
        }
        RowContext ctx = new RowContext(bindings);
        if (usesOldNew) {
            // Mark all table columns as "using" columns to suppress ambiguity
            // between primary binding and OLD/NEW bindings for unqualified refs
            Set<String> allCols = new java.util.HashSet<>();
            for (Column c : table.getColumns()) allCols.add(c.getName().toLowerCase());
            ctx.setUsingColumns(allCols);
        }
        List<Object> values = new ArrayList<>();
        for (SelectStmt.SelectTarget target : returning) {
            if (target.expr() instanceof WildcardExpr) {
                WildcardExpr we = (WildcardExpr) target.expr();
                if (we.table() != null && we.table().equalsIgnoreCase("OLD")) {
                    Object[] src = oldRow != null ? oldRow : new Object[table.getColumns().size()];
                    for (Object val : src) values.add(val);
                } else if (we.table() != null && we.table().equalsIgnoreCase("NEW")) {
                    // For DELETE (oldRow == row && newRow == null): NEW is all NULLs
                    Object[] src;
                    if (newRow != null) {
                        src = newRow;
                    } else if (oldRow != null && row == oldRow) {
                        src = new Object[table.getColumns().size()];
                    } else {
                        src = row;
                    }
                    for (Object val : src) values.add(val);
                } else {
                    // Bare * or table.* — return current row (NEW behavior, backward compat)
                    for (Object val : row) values.add(val);
                }
            } else {
                values.add(executor.evalExpr(target.expr(), ctx));
            }
        }
        return values.toArray();
    }

    /** Check if an expression tree contains OLD.col or NEW.col references. */
    private boolean exprReferencesOldNew(Expression expr) {
        if (expr == null) return false;
        if (expr instanceof ColumnRef) {
            ColumnRef cr = (ColumnRef) expr;
            return cr.table() != null && (cr.table().equalsIgnoreCase("old") || cr.table().equalsIgnoreCase("new"));
        }
        if (expr instanceof FunctionCallExpr) {
            for (Expression arg : ((FunctionCallExpr) expr).args()) {
                if (exprReferencesOldNew(arg)) return true;
            }
        }
        if (expr instanceof BinaryExpr) {
            BinaryExpr be = (BinaryExpr) expr;
            return exprReferencesOldNew(be.left()) || exprReferencesOldNew(be.right());
        }
        if (expr instanceof CaseExpr) {
            CaseExpr ce = (CaseExpr) expr;
            if (exprReferencesOldNew(ce.operand())) return true;
            for (CaseExpr.WhenClause wc : ce.whenClauses()) {
                if (exprReferencesOldNew(wc.condition()) || exprReferencesOldNew(wc.result())) return true;
            }
            return exprReferencesOldNew(ce.elseExpr());
        }
        if (expr instanceof IsNullExpr) {
            return exprReferencesOldNew(((IsNullExpr) expr).expr());
        }
        if (expr instanceof CastExpr) {
            return exprReferencesOldNew(((CastExpr) expr).expr());
        }
        if (expr instanceof UnaryExpr) {
            return exprReferencesOldNew(((UnaryExpr) expr).operand());
        }
        if (expr instanceof InExpr) {
            InExpr ie = (InExpr) expr;
            if (exprReferencesOldNew(ie.expr())) return true;
            if (ie.values() != null) {
                for (Expression v : ie.values()) {
                    if (exprReferencesOldNew(v)) return true;
                }
            }
        }
        if (expr instanceof BetweenExpr) {
            BetweenExpr be = (BetweenExpr) expr;
            return exprReferencesOldNew(be.expr()) || exprReferencesOldNew(be.low()) || exprReferencesOldNew(be.high());
        }
        return false;
    }

    /** Validate that all column references in RETURNING exist in the table. */
    void validateReturning(List<SelectStmt.SelectTarget> returning, Table table) {
        if (returning == null) return;
        for (SelectStmt.SelectTarget target : returning) {
            if (target.expr() instanceof WildcardExpr) continue;
            if (target.expr() instanceof ColumnRef) {
                ColumnRef cr = (ColumnRef) target.expr();
                // OLD.col and NEW.col reference the same table's columns
                boolean isOldNew = cr.table() != null &&
                        (cr.table().equalsIgnoreCase("old") || cr.table().equalsIgnoreCase("new"));
                if (cr.table() == null || cr.table().equalsIgnoreCase(table.getName()) || isOldNew) {
                    int idx = table.getColumnIndex(cr.column());
                    if (idx < 0) {
                        String qualifier = cr.table() != null ? cr.table() + "." : "";
                        throw new MemgresException("column " + qualifier + cr.column() + " does not exist", "42703");
                    }
                }
            }
        }
    }

    /** Build Column metadata for RETURNING clause. */
    private List<Column> buildReturningColumns(List<SelectStmt.SelectTarget> returning, Table table) {
        List<Column> cols = new ArrayList<>();
        for (SelectStmt.SelectTarget target : returning) {
            if (target.expr() instanceof WildcardExpr) {
                cols.addAll(table.getColumns());
            } else if (target.alias() != null) {
                cols.add(new Column(target.alias(), DataType.TEXT, true, false, null));
            } else if (target.expr() instanceof ColumnRef) {
                ColumnRef cr = (ColumnRef) target.expr();
                String colName = cr.column();
                int idx = table.getColumnIndex(colName);
                if (idx >= 0) {
                    cols.add(table.getColumns().get(idx));
                } else {
                    cols.add(new Column(colName, DataType.TEXT, true, false, null));
                }
            } else {
                cols.add(new Column(executor.exprToAlias(target.expr()), DataType.TEXT, true, false, null));
            }
        }
        return cols;
    }
}
