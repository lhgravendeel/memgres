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

    // Captured view→base column mapping for the DML statement currently being executed.
    // Captured immediately after resolveTable so that later resolveTable calls (FROM clauses,
    // constraint validation) cannot clobber the shared AstExecutor.lastViewColumnMapping field.
    private Map<String, String> activeViewColMap;
    // Ordered base-column names, one per view-column position, for positional INSERT remapping.
    private List<String> activeViewColOrder;
    // View columns computed from an expression: named by a write, they are refused rather than
    // reported missing, because the column exists and only the place to write it does not.
    private Set<String> activeViewExprCols;

    /** Translate a view column name to the base table column name using the active mapping. */
    private String mapViewColumn(String colName) {
        Map<String, String> mapping = activeViewColMap;
        if (mapping == null || colName == null) return colName;
        String mapped = mapping.get(colName.toLowerCase());
        return mapped != null ? mapped : colName;
    }

    /** Build a single-table RowContext that also resolves renamed view column names (if any). */
    private RowContext viewAwareCtx(Table table, String alias, Object[] row) {
        RowContext ctx = new RowContext(table, alias, row);
        if (activeViewColMap != null) ctx.setColumnAliases(activeViewColMap);
        return ctx;
    }

    /** Returns true when session_replication_role suppresses user triggers. */
    private boolean triggersDisabled() {
        if (executor.session == null) return false;
        String role = executor.session.getGucSettings().get("session_replication_role");
        return role != null && role.equalsIgnoreCase("replica");
    }

    /** Record row metadata (xmin, cmin) for system column support. */
    void recordRowMeta(String schema, Table table, Object[] row) {
        if (executor.session != null && executor.database != null) {
            String tableKey = resolveTableSchemaKey(schema, table);
            long xmin = executor.session.getTransactionId();
            long cmin = executor.session.getCommandId();
            executor.database.setRowInsertMeta(tableKey, row, xmin, cmin);
        }
    }

    /** Record row metadata update (new ctid after UPDATE). */
    void recordRowUpdateMeta(String schema, Table table, Object[] row) {
        if (executor.session != null && executor.database != null) {
            String tableKey = resolveTableSchemaKey(schema, table);
            long xmin = executor.session.getTransactionId();
            long cmin = executor.session.getCommandId();
            executor.database.setRowUpdateMeta(tableKey, row, xmin, cmin);
        }
    }

    /** Resolve the schema-qualified key for a table's row metadata.
     *  When schema is null, find the actual schema by scanning database schemas. */
    private String resolveTableSchemaKey(String schema, Table table) {
        if (schema != null) return schema + "." + table.getName();
        // Find the actual schema containing this table instance
        for (Map.Entry<String, Schema> e : executor.database.getSchemas().entrySet()) {
            if (e.getValue().getTable(table.getName()) == table) {
                return e.getKey() + "." + table.getName();
            }
        }
        return "public." + table.getName();
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
            // PG evaluates every data-modifying CTE before the main statement acts, so a
            // WITH d AS (DELETE ...) INSERT cannot have the delete wipe the inserted row.
            // Referenced CTEs are cached, so this does not run them twice.
            if (withClauses != null) {
                executePendingDmlCtes(withClauses);
            }
            return action.get();
        } finally {
            if (pushed) executor.cteStack.pop();
        }
    }

    /** Execute any DML CTEs that haven't been executed yet (not in cache). */
    private void executePendingDmlCtes(List<SelectStmt.CommonTableExpr> withClauses) {
        for (SelectStmt.CommonTableExpr cte : withClauses) {
            if (isDmlStatement(cte.query())) {
                String key = cte.name().toLowerCase();
                if (!executor.cteResultCache.containsKey(key)) {
                    executor.selectExecutor.executeCte(cte);
                }
            }
        }
    }

    private static boolean isDmlStatement(Statement stmt) {
        return stmt instanceof InsertStmt || stmt instanceof UpdateStmt || stmt instanceof DeleteStmt;
    }

    // ---- INSERT ----

    /**
     * A materialized view holds the stored result of its query, so writing to it would be
     * discarded by the next refresh. PostgreSQL refuses the write rather than accept one that
     * cannot last.
     */
    private void rejectMaterializedViewWrite(String tableName) {
        if (tableName == null) return;
        String bare = tableName.contains(".")
                ? tableName.substring(tableName.lastIndexOf('.') + 1) : tableName;
        Database.ViewDef view = executor.database.getView(bare);
        if (view != null && view.materialized()) {
            throw new MemgresException("cannot change materialized view \"" + bare + "\"", "42809");
        }
    }

    /**
     * Validate and insert with the table lock held, so two concurrent inserts of the same key
     * cannot both pass. When the only thing standing in the way is another session's uncommitted
     * insert, release the lock and wait for that transaction to settle, then start again — the
     * wait cannot happen under the lock, because rolling the other transaction back needs it.
     *
     * @param checkTable the relation whose constraints describe the row as written
     * @param targetTable where the row is actually stored, which differs for a partition
     */
    private void validateAndInsertWaiting(Table checkTable, Object[] row,
                                          Table targetTable, Object[] storedRow) {
        while (true) {
            // The wait below can end without the row having moved. Polling the cancel token here
            // means a statement_timeout or a client cancel still ends this loop if it does.
            StatementCancel.check();
            ConstraintValidator.PendingUniqueConflict pending;
            targetTable.getWriteLock().lock();
            try {
                pending = executor.constraintValidator
                        .findUncommittedUniqueConflict(targetTable, storedRow, null);
                if (pending == null) {
                    executor.constraintValidator.validateConstraints(checkTable, row, null);
                    if (targetTable != checkTable) {
                        executor.constraintValidator.validateConstraints(targetTable, storedRow, null);
                    }
                    targetTable.insertRow(storedRow);
                    return;
                }
            } finally {
                targetTable.getWriteLock().unlock();
            }
            awaitPendingInsert(targetTable, pending);
        }
    }

    private void awaitPendingInsert(Table table, ConstraintValidator.PendingUniqueConflict pending) {
        final String key = executor.constraintValidator.uncommittedKey(table);
        executor.database.awaitConcurrentWrite(executor.session, pending.owner,
                () -> ConstraintValidator.isStillPending(pending, key), pending.relation);
    }

    /**
     * Check the ON CONFLICT clause against the target relation before any row is processed.
     * PostgreSQL settles all of this while planning, so a bad column or constraint name is
     * reported even when the clause would never have fired.
     */
    private void validateOnConflictTarget(InsertStmt stmt, Table table) {
        InsertStmt.OnConflict oc = stmt.onConflict();
        if (oc == null) return;
        // The conflict target names an index, and an index is built from one row at a time; the
        // action it triggers is an UPDATE of that one row. Neither has a group to aggregate or a
        // result to be numbered against, so PostgreSQL names the clause the same way it does for
        // a CREATE INDEX or a plain UPDATE — and it settles all of this before it touches a row.
        // The arbiter columns are read first, as PostgreSQL reads them: a target that names no
        // column of the relation is that error, not whatever the action would also have been.
        if (oc.columns() != null) {
            for (String col : oc.columns()) {
                if (table.getColumnIndex(col) < 0) {
                    throw new MemgresException("column \"" + col + "\" of relation \""
                            + table.getName() + "\" does not exist", "42703");
                }
            }
        }
        PlacementCheck placement = executor.selectExecutor.placementCheck;
        if (oc.conflictExpressionAsts() != null) {
            for (Expression target : oc.conflictExpressionAsts()) {
                placement.reject(target, "index expressions");
            }
        }
        placement.reject(oc.whereClause(), "index predicates");
        if (oc.doUpdate() != null) {
            for (InsertStmt.SetClause set : oc.doUpdate()) {
                placement.reject(set.value(), "UPDATE");
            }
        }
        placement.reject(oc.doUpdateWhereClause(), "WHERE");
        // The WHERE of a DO UPDATE decides whether to overwrite the one conflicting row, so it is
        // an UPDATE's WHERE and takes no set either -- the SET list beside it already refuses one.
        executor.selectExecutor.rejectSrfIn(oc.doUpdateWhereClause(), "WHERE");
        // Both the target row and EXCLUDED are in scope here, so an unqualified column name is
        // ambiguous rather than a column of the target: only what the clause writes down is typed.
        BooleanContext.check(oc.doUpdateWhereClause(), "WHERE", BooleanContext.Types.none());
        if (oc.constraint() != null) {
            StoredConstraint named = null;
            for (StoredConstraint sc : table.getConstraints()) {
                if (sc.getName() != null && sc.getName().equalsIgnoreCase(oc.constraint())) {
                    named = sc;
                    break;
                }
            }
            if (named == null) {
                throw new MemgresException("constraint \"" + oc.constraint()
                        + "\" for table \"" + table.getName() + "\" does not exist", "42704");
            }
            // Arbitration needs a unique index to decide which row was hit; a CHECK or foreign
            // key has none, so there is nothing to conflict against.
            if (named.getType() != StoredConstraint.Type.PRIMARY_KEY
                    && named.getType() != StoredConstraint.Type.UNIQUE) {
                throw new MemgresException(
                        "constraint in ON CONFLICT clause has no associated index", "42809");
            }
        }
        if (oc.doUpdate() != null) {
            for (InsertStmt.SetClause set : oc.doUpdate()) {
                if (table.getColumnIndex(set.column()) < 0) {
                    throw new MemgresException("column \"" + set.column() + "\" of relation \""
                            + table.getName() + "\" does not exist", "42703");
                }
            }
        }
    }

    /**
     * The columns the ON CONFLICT clause arbitrates on, or null when they cannot be determined
     * from the statement alone (an expression target, for instance).
     */
    private List<String> conflictArbiterColumns(InsertStmt.OnConflict oc, Table table) {
        if (oc.columns() != null && !oc.columns().isEmpty()) return oc.columns();
        if (oc.constraint() != null) {
            for (StoredConstraint sc : table.getConstraints()) {
                if (sc.getName() != null && sc.getName().equalsIgnoreCase(oc.constraint())) {
                    return sc.getColumns();
                }
            }
        }
        return null;
    }

    /**
     * A key already acted on by DO UPDATE within this statement cannot be acted on again: the
     * second update would overwrite what the first just wrote, so the result would depend on the
     * order the rows happened to be processed.
     */
    private void rejectSecondUpdateOfSameKey(InsertStmt.OnConflict oc, Table table,
                                             Object[] row, java.util.Set<String> seenKeys) {
        if (oc.doUpdate() == null) return; // DO NOTHING may skip the same key repeatedly
        List<String> cols = conflictArbiterColumns(oc, table);
        if (cols == null || cols.isEmpty()) return;
        StringBuilder key = new StringBuilder();
        for (String col : cols) {
            int idx = table.getColumnIndex(col);
            if (idx < 0 || idx >= row.length) return;
            Object v = row[idx];
            if (v == null) return; // a null arbiter value never matches anything
            // Length-prefixed so two values cannot run together into one key.
            key.append(v.toString().length()).append(':').append(v).append(';');
        }
        if (!seenKeys.add(key.toString())) {
            throw new MemgresException(
                    "ON CONFLICT DO UPDATE command cannot affect row a second time", "21000");
        }
    }

    /**
     * Undo steps recorded while a single statement runs, so that a statement which fails part way
     * leaves nothing behind. The session's undo log only records inside an explicit transaction,
     * and in autocommit PostgreSQL still treats each statement as all-or-nothing — without this a
     * multi-row INSERT that failed on its last row kept every row before it.
     */
    private static final class StatementUndo {
        private final List<Runnable> steps = new ArrayList<>();

        void record(Runnable step) { steps.add(step); }

        void revert() {
            for (int i = steps.size() - 1; i >= 0; i--) {
                try {
                    steps.get(i).run();
                } catch (RuntimeException ignored) {
                    // Keep unwinding: a step that cannot be undone must not strand the rest.
                }
            }
            steps.clear();
        }
    }

    /**
     * What PostgreSQL settles about a data-modifying statement before it judges any clause of it.
     *
     * <p>PostgreSQL builds the range table first — putting the relation the statement writes into
     * it ahead of the ones it reads — and validates the written column list against that relation
     * while it is still analysing the statement. A statement that both names something that is not
     * there and misuses a clause therefore reports the name. A SELECT already gets this from
     * {@link FromResolver#checkRelationNamesExist}; INSERT, UPDATE, DELETE and MERGE resolve their
     * target inside the methods below instead, which is why a clause-level refusal used to win.
     *
     * <p>This is the same resolution those methods perform, by name, reading no rows: it can only
     * report what they would have reported, one step earlier. Where it cannot settle the answer
     * without reading — a view, a relation it cannot describe — it says nothing and leaves the
     * later check where it was.
     */
    void checkTargetsResolvable(Statement stmt) {
        executor.fromResolver.checkRelationNamesExist(stmt);
        SelectStmt.TableRef target = FromResolver.writtenRelationOf(stmt);
        if (target == null) return;
        Table table = describeWrittenRelation(target);
        if (table == null) return;
        // The column list of an INSERT is validated against the target the moment the target is
        // known, before the VALUES or the SELECT behind them is looked at.
        if (stmt instanceof InsertStmt) {
            requireTargetColumns(table, target.table, ((InsertStmt) stmt).columns());
        }
        // An UPDATE's FROM and a DELETE's USING bring other relations into scope; without one the
        // target is the whole scope, and PostgreSQL resolves the WHERE against it before it
        // reaches the assignments or anything they carry.
        if (stmt instanceof UpdateStmt) {
            UpdateStmt u = (UpdateStmt) stmt;
            if (u.from() == null || u.from().isEmpty()) {
                requireReadableColumns(table, u.where(), u.alias(), u.table());
            }
        }
        if (stmt instanceof DeleteStmt) {
            DeleteStmt d = (DeleteStmt) stmt;
            if (d.using() == null || d.using().isEmpty()) {
                requireReadableColumns(table, d.where(), d.alias(), d.table());
            }
        }
    }

    /**
     * The stored relation a data-modifying statement writes, described rather than read.
     *
     * <p>A view is left alone: it has columns of its own and a rewrite behind it, and the write
     * path resolves both and has its own words for what is wrong with them.
     */
    private Table describeWrittenRelation(SelectStmt.TableRef target) {
        if (target.table == null) return null;
        Database.ViewDef view = target.schema != null
                ? executor.database.getView(target.schema, target.table)
                : executor.database.getView(target.table);
        if (view != null) return null;
        try {
            return executor.resolveTable(
                    target.schema != null ? target.schema : executor.defaultSchema(),
                    target.table, target.schema != null);
        } catch (RuntimeException e) {
            // Anything this cannot resolve is left to the write path, which resolves it again and
            // reports whatever it finds in its own words.
            return null;
        }
    }

    QueryResult executeInsert(InsertStmt stmt) {
        return withCteScope(stmt.withClauses(), () -> executeInsertInner(stmt));
    }

    private QueryResult executeInsertInner(InsertStmt stmt) {
        rejectDuplicateInsertColumns(stmt.columns());
        // Check read-only transaction
        checkReadOnly("INSERT");
        rejectMaterializedViewWrite(stmt.table());
        String schemaName = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        // Collect WITH CHECK OPTION constraints from views we're inserting through
        List<DmlValidationHelper.ViewCheck> viewCheckExprs = validationHelper.collectViewCheckExprs(stmt.table());
        // H35: honor explicit schema qualifier so a same-named temp table cannot shadow it
        executor.viewDmlVerb = "insert into";
        Table table = executor.resolveTable(schemaName, stmt.table(), stmt.schema() != null);
        // A VALUES row is written out in full; it is not read from any relation, so there is
        // nothing for an aggregate to aggregate or for a window call to be numbered against.
        if (stmt.values() != null) {
            for (List<Expression> valueRow : stmt.values()) {
                for (Expression value : valueRow) {
                    executor.selectExecutor.placementCheck.reject(value, "VALUES");
                    // A single VALUES row is projected like a select list and a set in it expands
                    // into rows. Two or more rows are a scan of a constant table, which has
                    // nowhere to expand a set, and PostgreSQL refuses it -- measured: the same
                    // call in a one-row VALUES is accepted and writes one row per element.
                    if (stmt.values().size() > 1) executor.selectExecutor.rejectSrfIn(value, "VALUES");
                }
            }
        }
        // ON CONFLICT DO UPDATE assigns to one row, the same as an UPDATE, and is named so.
        if (stmt.onConflict() != null && stmt.onConflict().doUpdate() != null) {
            for (InsertStmt.SetClause set : stmt.onConflict().doUpdate()) {
                executor.selectExecutor.rejectSrfIn(set.value(), "UPDATE");
            }
        }
        // Capture view column mapping/order before any further resolveTable calls clobber them.
        this.activeViewColMap = executor.lastViewColumnMapping;
        this.activeViewColOrder = executor.lastViewColumnOrder;
        this.activeViewExprCols = executor.lastViewExpressionColumns;
        // C6: Enforce INSERT privilege
        executor.checkTablePrivilege("INSERT", schemaName, stmt.table());
        // Check table-level locks (blocks if ACCESS EXCLUSIVE held by another session)
        executor.database.checkTableLockForDml(schemaName + "." + stmt.table(), executor.session);

        // Check for INSTEAD / ALSO rules. Writing through an updatable view is rewritten to a
        // write on the base table, so the base table's rules apply as well — a rule of the
        // view's own takes precedence, as it does in PG, because it replaces the rewrite.
        String ruleRelation = stmt.table();
        String ruleVal = executor.database.getRule(ruleRelation, "INSERT");
        if (ruleVal == null && table != null && table.getName() != null
                && !table.getName().equalsIgnoreCase(stmt.table())) {
            ruleRelation = table.getName();
            ruleVal = executor.database.getRule(ruleRelation, "INSERT");
        }
        // A rule that writes back to its own table re-enters itself; PG detects that while
        // rewriting the statement and never runs any of it.
        if (ruleVal != null && executor.isRuleExpanding(ruleRelation, "INSERT")) {
            throw PgErrors.infiniteRecursionInRules(ruleRelation);
        }
        // Check for DO ALSO rule - will be applied after normal insert
        String alsoRuleSql = null;
        if (ruleVal != null && ruleVal.startsWith("ALSO:")) {
            alsoRuleSql = ruleVal.substring("ALSO:".length());
        }
        if ("INSTEAD_NOTHING".equals(ruleVal)) {
            return QueryResult.command(QueryResult.Type.INSERT, 0);
        }
        if (ruleVal != null && ruleVal.startsWith("INSTEAD:")) {
            int ruleCount = runInsertRuleActions(ruleVal.substring("INSTEAD:".length()), stmt, table, ruleRelation);
            return QueryResult.command(QueryResult.Type.INSERT, ruleCount);
        }

        // A view column computed from an expression is a real column of the view — it is returned
        // by SELECT — but there is nothing behind it to write to. PG says so in those words, and
        // saying "column does not exist" instead would send a caller looking for a typo.
        if (activeViewExprCols != null && stmt.columns() != null) {
            for (String col : stmt.columns()) {
                if (activeViewExprCols.contains(col.toLowerCase())) {
                    MemgresException ex = new MemgresException("cannot insert into column \"" + col
                            + "\" of view \"" + stmt.table() + "\"", "0A000");
                    ex.setDetail(ViewUpdatability.DETAIL_NOT_COLUMN);
                    throw ex;
                }
            }
        }
        // Every name the statement writes to is resolved against the relation before any row is
        // looked at, so an INSERT into a column that is not there is refused the same way whether
        // the VALUES list is empty, the SELECT behind it returns nothing, or the table is empty.
        requireTargetColumns(table, stmt.table(), stmt.columns());

        // Validate RETURNING columns exist before processing rows
        validateReturning(stmt.returning(), table);

        // PG 18: RETURNING OLD/NEW is supported with ON CONFLICT DO NOTHING
        // Non-conflicting rows return NEW.*, conflicting (skipped) rows return nothing

        List<PgTrigger> triggers = triggersDisabled() ? Cols.listOf() : executor.database.getTriggersForTable(stmt.table());
        // Check for INSTEAD OF triggers (on views)
        boolean hasInsteadOfInsert = triggers.stream().anyMatch(
                t -> t.getTiming() == PgTrigger.Timing.INSTEAD_OF && t.getEvent() == PgTrigger.Event.INSERT);
        int inserted = 0;
        List<Object[]> returningRows = new ArrayList<>();
        // The ON CONFLICT clause describes the relation, not any one row, so it is checked once.
        validateOnConflictTarget(stmt, table);
        java.util.Set<String> conflictKeysActedOn = new java.util.HashSet<>();
        StatementUndo statementUndo = new StatementUndo();

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
                    } else if (val instanceof Double || val instanceof Float) {
                        // Keep the float width: a plain numeric literal would make an overflow on
                        // the way into a real column read as an input error rather than the
                        // narrowing PG reports, and would leave NaN with no numeric to become.
                        exprRow.add(new CastExpr(Literal.ofFloat(val.toString()),
                                val instanceof Float ? "float4" : "float8"));
                    } else if (val instanceof java.math.BigDecimal) {
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

        // A VALUES row holding a set-returning call is as many rows as the call produces, so the
        // expansion happens before anything counts the rows or writes one.
        List<List<Expression>> srfExpandedRows = new ArrayList<>();
        List<RowContext> valueRowContexts = expandSrfValueRows(valueRows, srfExpandedRows);
        if (valueRowContexts != null) valueRows = srfExpandedRows;

        // Pre-validate all rows' column counts before inserting any rows (atomicity)
        for (List<Expression> valueRow : valueRows) {
            if (stmt.columns() != null && stmt.columns().size() != valueRow.size()) {
                throw new MemgresException("INSERT has more " +
                        (stmt.columns().size() > valueRow.size() ? "target columns than expressions" : "expressions than target columns"),
                        "42601");
            } else if (stmt.columns() == null && valueRow.size() > table.getColumns().size()) {
                throw new MemgresException("INSERT has more expressions than target columns", "42601");
            }
        }

        // Fire BEFORE STATEMENT triggers
        triggerHelper.fireStatementTriggers(triggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.INSERT, table, null, null);

        // A statement either happens or it does not: if a later row is refused, the rows already
        // written by this same statement have to go with it.
        try {
        List<Object[]> insertedRows = new ArrayList<>(); // for transition tables in statement triggers
        List<Object[]> afterRowTriggerNewRows = new ArrayList<>(); // queue AFTER ROW triggers
        List<Table> afterRowTriggerTables = new ArrayList<>(); // corresponding target tables for AFTER ROW
        // Positional INSERT through a reordered/renamed/subset view: the VALUES are supplied in
        // VIEW-column order, so remap them onto the base table using the view's ordered base-column
        // list. Named INSERT keeps stmt.columns() (mapViewColumn resolves any renamed view names).
        List<String> insertColumns = stmt.columns();
        if (insertColumns == null && activeViewColOrder != null) {
            insertColumns = activeViewColOrder;
        }
        // When the table has row triggers, snapshot target rows so a trigger raising
        // mid-statement rolls the whole INSERT back (PostgreSQL statement atomicity).
        Map<Table, List<Object[]>> insSnapshot = triggers.isEmpty() ? null
                : snapshotTargetTables(collectTargetTables(table));
        try {
        int valueRowIdx = -1;
        for (List<Expression> valueRow : valueRows) {
            valueRowIdx++;
            // Non-null only for a VALUES row a set-returning call expanded, and then it holds the
            // element of each call that belongs to this row.
            RowContext valueCtx = valueRowContexts == null ? null : valueRowContexts.get(valueRowIdx);
            Object[] row = new Object[table.getColumns().size()];

            // Fill provided values FIRST (with type coercion); validates before consuming serials
            Set<Integer> filledCols = new HashSet<>();
            if (insertColumns != null) {
                for (int i = 0; i < insertColumns.size() && i < valueRow.size(); i++) {
                    // Skip DEFAULT keyword; let the serial/default logic handle it
                    if (valueRow.get(i) instanceof Literal && ((Literal) valueRow.get(i)).literalType() == Literal.LiteralType.DEFAULT) continue;
                    String colName = mapViewColumn(insertColumns.get(i));
                    int colIdx = table.getColumnIndex(colName);
                    if (colIdx < 0) {
                        throw new MemgresException("Column not found: " + insertColumns.get(i));
                    }
                    // Reject explicit writes to GENERATED ALWAYS AS ... STORED columns
                    Column genCol = table.getColumns().get(colIdx);
                    if (genCol.isGenerated()) {
                        throw new MemgresException("cannot insert a non-DEFAULT value into column \"" + genCol.getName() + "\"\n  Detail: Column \"" + genCol.getName() + "\" is a generated column.", "428C9");
                    }
                    Object val = executor.evalExpr(valueRow.get(i), valueCtx);
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
                    Object val = executor.evalExpr(valueRow.get(i), valueCtx);
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
                row[colIdx] = wrapEnumValue(table.getColumns().get(colIdx), row[colIdx]);
            }

            // Fill serial columns and defaults AFTER explicit values validated
            for (int i = 0; i < table.getColumns().size(); i++) {
                if (filledCols.contains(i)) continue;
                Column col = table.getColumns().get(i);
                if (col.getDefaultValue() != null && col.getDefaultValue().startsWith("__identity__")) {
                    row[i] = resolveIdentityNextVal(table, col);
                } else if ((col.getType() == DataType.SERIAL || col.getType() == DataType.BIGSERIAL || col.getType() == DataType.SMALLSERIAL)
                        && col.getDefaultValue() != null && col.getDefaultValue().contains("nextval(")) {
                    row[i] = resolveSerialNextVal(table, col);
                } else if ((col.getType() == DataType.SERIAL || col.getType() == DataType.BIGSERIAL || col.getType() == DataType.SMALLSERIAL)
                        && col.getDefaultValue() == null) {
                    // Default was removed (e.g. DROP SEQUENCE CASCADE) — no auto-value
                } else if (col.getType() == DataType.SERIAL || col.getType() == DataType.BIGSERIAL || col.getType() == DataType.SMALLSERIAL) {
                    row[i] = table.nextSerial();
                } else if (col.getDefaultValue() != null) {
                    Object defVal = executor.evaluateDefault(col.getDefaultValue(), col.getType(), col.getParsedDefaultExpr());
                    // wrapEnumValue mirrors the filledCols loop above: a DEFAULT-populated ENUM
                    // column previously stayed a raw Java String forever (TypeCoercion.coerce
                    // has no ENUM case), so ordering comparisons (<, <=, ORDER BY, ...) silently
                    // fell back to lexicographic string comparison instead of PG's declaration
                    // order. This is the actual root cause behind the "ON CONFLICT expression
                    // arbiter looks like it doesn't match" symptom (mtask-8 Group 6): the
                    // partial-index/ON-CONFLICT WHERE predicate `state < 'active'` silently
                    // evaluated false for a DEFAULT-valued job_state column ('created' > 'active'
                    // lexicographically), so the conflict search bailed out early and the insert
                    // proceeded, silently violating the unique index -- the arbiter/index
                    // matching itself was already correct.
                    row[i] = wrapEnumValue(col, TypeCoercion.coerceForStorage(defVal, col));
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
                Object[] insteadRow = triggerHelper.executeTriggers(triggers, PgTrigger.Timing.INSTEAD_OF, PgTrigger.Event.INSERT, row, null, table);
                if (insteadRow == null) {
                    // INSTEAD OF trigger returned NULL: row skipped (not counted, no RETURNING)
                    continue;
                }
                inserted++;
                if (stmt.returning() != null && !stmt.returning().isEmpty()) {
                    returningRows.add(evalReturning(stmt.returning(), table, stmt.alias(), row, null, row));
                }
                continue;
            }

            // BEFORE INSERT triggers (use leaf partition for correct TG_TABLE_NAME)
            Table beforeTrigTable = table;
            try { beforeTrigTable = partitionHelper.routeToPartition(table, row); } catch (Exception ignored) {}
            row = triggerHelper.executeTriggers(triggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.INSERT, row, null, beforeTrigTable);
            if (row == null) {
                // BEFORE trigger returned NULL: skip this row (not inserted, not counted, no RETURNING)
                continue;
            }

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
                        // Also match bare column names in expression syntax against regular column constraints
                        // e.g., ON CONFLICT ((id)) where (id) is parsed as expression "id" should match PRIMARY KEY (id)
                        if (!hasMatchingExprIndex
                                && (sc.getType() == StoredConstraint.Type.UNIQUE || sc.getType() == StoredConstraint.Type.PRIMARY_KEY)
                                && sc.getColumns() != null
                                && sc.getColumns().size() == targetExprs.size()) {
                            boolean allMatch = true;
                            for (int ei = 0; ei < targetExprs.size(); ei++) {
                                String targetExpr = targetExprs.get(ei).toLowerCase().replaceAll("\\s+", "");
                                String colName = sc.getColumns().get(ei).toLowerCase();
                                if (!targetExpr.equals(colName)) {
                                    allMatch = false;
                                    break;
                                }
                            }
                            if (allMatch) {
                                if (sc.getWhereExpr() != null && stmt.onConflict().whereClause() == null) {
                                    continue;
                                }
                                hasMatchingExprIndex = true;
                            }
                        }
                    }
                    if (!hasMatchingExprIndex) {
                        throw new MemgresException("there is no unique or exclusion constraint matching the ON CONFLICT specification", "42P10");
                    }
                }
                // For partitioned tables, actual row storage lives on the leaf partition, not
                // the parent: route the conflict search there so it can see (and update) rows
                // that were previously routed to that same partition. The constraint-target
                // validation above intentionally stays against the parent's declared
                // constraints; the partition carries its own copy (see createPartitionOfTable).
                Table conflictTable = partitionHelper.routeToPartition(table, row);
                rejectSecondUpdateOfSameKey(stmt.onConflict(), conflictTable, row, conflictKeysActedOn);
                Object[] conflictRow = conflictHelper.findConflictingRow(conflictTable, row, stmt.onConflict());
                if (conflictRow != null) {
                    if (stmt.onConflict().doNothing()) {
                        // DO NOTHING: skip this row
                        continue;
                    } else if (stmt.onConflict().doUpdate() != null) {
                        // DO UPDATE: update the conflicting row
                        // Build RowContext with "excluded" binding so the standard expression
                        // evaluator resolves EXCLUDED.col references for all expression types
                        Object[] evalConflict = hasVirtualColumns(conflictTable) ? computeVirtualColumns(conflictTable, conflictRow) : conflictRow;
                        List<RowContext.TableBinding> conflictBindings = new ArrayList<>();
                        conflictBindings.add(new RowContext.TableBinding(conflictTable, stmt.alias(), evalConflict));
                        conflictBindings.add(new RowContext.TableBinding(conflictTable, "excluded", row));
                        RowContext conflictCtx = new RowContext(conflictBindings);
                        Set<String> allCols = new HashSet<>();
                        for (Column c : conflictTable.getColumns()) allCols.add(c.getName().toLowerCase());
                        conflictCtx.setUsingColumns(allCols);

                        // First evaluate the DO UPDATE WHERE clause if present
                        if (stmt.onConflict().doUpdateWhereClause() != null) {
                            Object whereResult = executor.evalExpr(stmt.onConflict().doUpdateWhereClause(), conflictCtx);
                            // Read as a condition, like every other WHERE: an unadorned literal is
                            // of type unknown and boolean's input function reads it, so WHERE 't'
                            // is the true this clause was skipping the update for.
                            if (!executor.isTruthy(whereResult)) {
                                // WHERE clause evaluated to false/null: skip entirely — no update, no count, no RETURNING
                                continue;
                            }
                        }
                        Object[] oldRow = Arrays.copyOf(conflictRow, conflictRow.length);
                        Object[] newRow = Arrays.copyOf(conflictRow, conflictRow.length);
                        Set<String> conflictUpdCols = new HashSet<>();
                        for (InsertStmt.SetClause set : stmt.onConflict().doUpdate()) {
                            int colIdx = conflictTable.getColumnIndex(set.column());
                            if (colIdx < 0) {
                                throw new MemgresException("Column not found: " + set.column());
                            }
                            conflictUpdCols.add(set.column().toLowerCase());
                            Object val = executor.evalExpr(set.value(), conflictCtx);
                            newRow[colIdx] = TypeCoercion.coerceForStorage(val, conflictTable.getColumns().get(colIdx));
                        }
                        // The conflict path is an UPDATE, so it fires UPDATE row triggers.
                        newRow = triggerHelper.executeTriggers(triggers, PgTrigger.Timing.BEFORE,
                                PgTrigger.Event.UPDATE, newRow, oldRow, conflictTable, conflictUpdCols);
                        if (newRow == null) continue;   // BEFORE trigger suppressed the row
                        computeGeneratedColumns(conflictTable, newRow);
                        // Validate constraints BEFORE mutating the row to avoid index corruption
                        executor.constraintValidator.validateConstraints(conflictTable, newRow, conflictRow);
                        try {
                            conflictTable.updateRowInPlace(conflictRow, oldRow, newRow);
                            {
                                final Table undoTbl = conflictTable;
                                final Object[] undoTarget = conflictRow;
                                final Object[] undoBefore = Arrays.copyOf(oldRow, oldRow.length);
                                final Object[] undoAfter = Arrays.copyOf(newRow, newRow.length);
                                statementUndo.record(() ->
                                        undoTbl.updateRowInPlace(undoTarget, undoAfter, undoBefore));
                            }
                        } catch (Exception e) {
                            conflictTable.updateRowInPlace(conflictRow, newRow, oldRow);
                            throw e;
                        }
                        recordUpdateUndo(stmt.schema(), conflictTable.getName(), conflictRow, oldRow);
                        triggerHelper.executeTriggers(triggers, PgTrigger.Timing.AFTER,
                                PgTrigger.Event.UPDATE, conflictRow, oldRow, conflictTable, conflictUpdCols);
                        if (stmt.returning() != null && !stmt.returning().isEmpty()) {
                            returningRows.add(evalReturning(stmt.returning(), conflictTable, stmt.alias(), conflictRow, oldRow, conflictRow));
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
                String insertRlsSchema = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
                if (!executor.shouldBypassRls(table, insertRlsSchema)) {
                    enforceRlsWithCheck(table, row, "INSERT");
                }
            }

            // Validate constraints and insert atomically under table write lock
            // to prevent concurrent INSERTs from both passing unique checks.
            Table targetTable = partitionHelper.routeToPartition(table, row);
            // An ATTACHed partition may order its columns differently from the parent.
            Object[] storedRow = targetTable == table ? row : targetTable.rowFromParent(row);
            validateAndInsertWaiting(table, row, targetTable, storedRow);
            final Table undoTable = targetTable;
            final Object[] undoRow = storedRow;
            statementUndo.record(() -> undoTable.removeRow(undoRow));
            recordInsertUndo(stmt.schema(), targetTable.getName(), storedRow);
            // C10: If routed to a child partition, also sync parent's RR snapshot
            if (targetTable != table && executor.session != null) {
                String parentKey = (stmt.schema() != null ? stmt.schema() : executor.defaultSchema()) + "." + table.getName();
                executor.session.syncParentSnapshotOnInsert(parentKey, row);
            }
            recordRowMeta(stmt.schema(), targetTable, row);
            inserted++;
            insertedRows.add(Arrays.copyOf(row, row.length));

            // Queue AFTER INSERT row triggers (PG fires all AFTER ROW triggers after all rows are processed)
            afterRowTriggerNewRows.add(Arrays.copyOf(row, row.length));
            afterRowTriggerTables.add(targetTable);

            // Collect RETURNING row
            if (stmt.returning() != null && !stmt.returning().isEmpty()) {
                returningRows.add(evalReturning(stmt.returning(), table, stmt.alias(), row, null, row));
            }
        }

        // Fire queued AFTER ROW triggers (use leaf partition table for correct TG_TABLE_NAME)
        for (int i = 0; i < afterRowTriggerNewRows.size(); i++) {
            Table trigTable = afterRowTriggerTables.get(i);
            triggerHelper.executeTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.INSERT, afterRowTriggerNewRows.get(i), null, trigTable);
        }

        // Fire statement-level AFTER triggers with transition tables
        triggerHelper.fireStatementTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.INSERT, table, insertedRows, null);
        } catch (MemgresException e) {
            // Statement is atomic: undo any rows already inserted/updated before the failure.
            if (insSnapshot != null) {
                restoreTargetTables(insSnapshot);
                if (executor.session != null) executor.session.discardUndoForCurrentStatement();
            }
            throw e;
        }

        // Execute DO ALSO rule if present
        if (alsoRuleSql != null) {
            runInsertRuleActions(alsoRuleSql, stmt, table, ruleRelation);
        }

        // Track DML statistics
        if (inserted > 0) table.incrementTupInserted(inserted);

        if (stmt.returning() != null && !stmt.returning().isEmpty()) {
            List<Column> retCols = buildReturningColumns(stmt.returning(), table);
            return QueryResult.returning(QueryResult.Type.INSERT, retCols, returningRows, inserted);
        }
        return QueryResult.command(QueryResult.Type.INSERT, inserted);
        } catch (RuntimeException e) {
            statementUndo.revert();
            throw e;
        }
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
            // H35: honor explicit schema qualifier so a same-named temp table cannot shadow it
            Table table = executor.resolveTable(copySchema, copyTableName, stmt.table().contains("."));
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
        // H35: honor explicit schema qualifier so a same-named temp table cannot shadow it
        Table table = executor.resolveTable(fromSchema, fromTableName, stmt.table().contains("."));
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
            Expression inlineWhereExpr = null;
            if (stmt.whereClause() != null && !stmt.whereClause().isEmpty()) {
                inlineWhereExpr = com.memgres.engine.parser.Parser.parseExpression(stmt.whereClause());
            }
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
                // Apply WHERE clause filtering (COPY FROM ... WHERE)
                if (inlineWhereExpr != null) {
                    RowContext ctx = new RowContext(table, null, row);
                    if (!executor.isTruthy(executor.evalExpr(inlineWhereExpr, ctx))) {
                        continue; // Row does not match WHERE clause; skip it
                    }
                }
                validateAndInsertWaiting(table, row, table, row);
                recordInsertUndo(null, stmt.table(), row);
                recordRowMeta(null, table, row);
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
        // H35: honor explicit schema qualifier so a same-named temp table cannot shadow it
        Table table = executor.resolveTable(rowSchema, rowTableName, stmt.table().contains("."));
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

        // A GENERATED ALWAYS identity refuses a written value from INSERT and UPDATE, but not
        // from COPY: PostgreSQL lets COPY write the column as given, which is what carries the
        // identity values across a dump and restore. Refusing it here meant pg_dump could write
        // a dump of this server that this server would not read back.

        // FORCE_NOT_NULL: for specified columns, do not match against the null string
        List<String> forceNotNull = stmt.forceNotNull();
        // FORCE_NULL: for specified columns, match the (possibly quoted) value against the null string
        List<String> forceNull = stmt.forceNull();
        String effectiveNullStr = stmt.nullString() != null ? stmt.nullString() : "";

        for (int i = 0; i < values.size() && i < colIndices.size(); i++) {
            String val = values.get(i);
            int colIdx = colIndices.get(i);
            Column col = table.getColumns().get(colIdx);

            // DEFAULT option: replace marker string with column default
            if (val != null && stmt.defaultString() != null && val.equals(stmt.defaultString())) {
                // Leave the default value that fillDefaults already set
                continue;
            }

            // FORCE_NOT_NULL: if this column is in the list and the field matched the
            // null string, keep it as the literal null-string data instead of NULL
            if (val == null && forceNotNull != null) {
                String colName = col.getName();
                if (forceNotNull.contains("*") || forceNotNull.stream().anyMatch(c -> c.equalsIgnoreCase(colName))) {
                    val = effectiveNullStr;
                }
            }

            // FORCE_NULL: if this column is in the list and the (possibly quoted) value
            // equals the null string, convert to null
            if (val != null && val.equals(effectiveNullStr) && forceNull != null) {
                String colName = col.getName();
                if (forceNull.contains("*") || forceNull.stream().anyMatch(c -> c.equalsIgnoreCase(colName))) {
                    val = null;
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

        // Apply WHERE clause filtering (COPY FROM ... WHERE)
        if (stmt.whereClause() != null && !stmt.whereClause().isEmpty()) {
            Expression whereExpr = com.memgres.engine.parser.Parser.parseExpression(stmt.whereClause());
            RowContext ctx = new RowContext(table, null, row);
            if (!executor.isTruthy(executor.evalExpr(whereExpr, ctx))) {
                return null; // Row does not match WHERE clause; skip it
            }
        }

        // Fire BEFORE INSERT triggers
        List<PgTrigger> triggers = triggersDisabled() ? Cols.listOf() : executor.database.getTriggersForTable(stmt.table());
        if (triggers != null && !triggers.isEmpty()) {
            Object[] modified = triggerHelper.executeTriggers(triggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.INSERT, row, null, table);
            if (modified == null) return null; // BEFORE trigger returned null = skip row
            row = modified;
        }

        validateAndInsertWaiting(table, row, table, row);
        recordInsertUndo(null, stmt.table(), row);
        recordRowMeta(null, table, row);

        // Fire AFTER INSERT triggers
        if (triggers != null && !triggers.isEmpty()) {
            triggerHelper.executeTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.INSERT, row, null, table);
        }

        return row;
    }

    private void fillDefaults(Table table, Object[] row) {
        for (int i = 0; i < table.getColumns().size(); i++) {
            Column col = table.getColumns().get(i);
            if (col.getDefaultValue() != null && col.getDefaultValue().startsWith("__identity__")) {
                row[i] = resolveIdentityNextVal(table, col);
            } else if ((col.getType() == DataType.SERIAL || col.getType() == DataType.BIGSERIAL || col.getType() == DataType.SMALLSERIAL)
                    && col.getDefaultValue() != null && col.getDefaultValue().contains("nextval(")) {
                row[i] = resolveSerialNextVal(table, col);
            } else if ((col.getType() == DataType.SERIAL || col.getType() == DataType.BIGSERIAL || col.getType() == DataType.SMALLSERIAL)
                    && col.getDefaultValue() == null) {
                // Default was removed (e.g. DROP SEQUENCE CASCADE) — no auto-value
            } else if (col.getType() == DataType.SERIAL || col.getType() == DataType.BIGSERIAL || col.getType() == DataType.SMALLSERIAL) {
                row[i] = table.nextSerial();
            } else if (col.getDefaultValue() != null) {
                Object defVal = executor.evaluateDefault(col.getDefaultValue(), col.getType(), col.getParsedDefaultExpr());
                row[i] = TypeCoercion.coerceForStorage(defVal, col);
            }
        }
    }

    /**
     * Validates an ENUM column's raw label and wraps it in an {@link AstExecutor.PgEnum} carrying
     * its declared ordinal position, so downstream comparisons ({@code <}, {@code <=}, {@code >},
     * {@code >=}, {@code ORDER BY}, simple {@code CASE}, ...) use PG's declaration-order semantics
     * instead of silently falling back to lexicographic string comparison
     * ({@code TypeCoercion.compare}/{@code coerce} have no ENUM case). Returns the value unchanged
     * for non-ENUM columns, {@code null}, enum array columns, and array-literal text (whose
     * elements are validated separately during ARRAY expression evaluation).
     */
    private Object wrapEnumValue(Column col, Object rawValue) {
        if (col.getType() != DataType.ENUM || rawValue == null) return rawValue;
        if (col.getArrayElementType() != null || (rawValue instanceof String && ((String) rawValue).startsWith("{"))) {
            return rawValue;
        }
        String enumTypeName = col.getEnumTypeName();
        if (enumTypeName == null) return rawValue;
        CustomEnum customEnum = executor.database.getCustomEnum(enumTypeName);
        if (customEnum == null) return rawValue;
        String label = rawValue instanceof AstExecutor.PgEnum ? ((AstExecutor.PgEnum) rawValue).label() : rawValue.toString();
        if (!customEnum.isValidLabel(label)) {
            // Named the way PostgreSQL names it here: bare when the search path finds it,
            // qualified when the type is one the reader would otherwise not know which of.
            throw new MemgresException("invalid input value for enum "
                    + TypeNamespace.display(executor.database, executor.session, enumTypeName)
                    + ": \"" + label + "\"", "22P02");
        }
        return new AstExecutor.PgEnum(label, enumTypeName, customEnum.ordinal(label));
    }

    private Object resolveSerialNextVal(Table table, Column col) {
        String def = col.getDefaultValue();
        // Extract sequence name from nextval('seqname'::regclass). A bare name in a default means
        // the sequence in this table's own schema: two schemas may each hold a t_id_seq, and
        // drawing from whichever one is found first makes the two tables share a counter.
        int q1 = def.indexOf('\'');
        int q2 = def.indexOf('\'', q1 + 1);
        if (q1 >= 0 && q2 > q1) {
            String seqName = def.substring(q1 + 1, q2);
            Sequence seq = executor.database.getSequenceFor(table.getSchemaName(), seqName);
            if (seq != null) {
                return drawFromSequence(seq);
            }
        }
        return null;
    }

    /**
     * Draw the next value and record it as this session's, so a serial or identity column read
     * back with currval reports the key this connection generated rather than another's.
     */
    private long drawFromSequence(Sequence seq) {
        long value = seq.nextVal();
        executor.lastSequenceValue = value;
        executor.sessionSequenceValues.put(seq.qualifiedName().toLowerCase(), value);
        return value;
    }

    private Object resolveIdentityNextVal(Table table, Column col) {
        String def = col.getDefaultValue();
        if (def != null && def.contains(":seq:")) {
            String seqName = def.substring(def.indexOf(":seq:") + 5);
            Sequence seq = executor.database.getSequenceFor(table.getSchemaName(), seqName);
            if (seq != null) {
                return drawFromSequence(seq);
            }
        }
        return table.nextSerial();
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
        rejectMaterializedViewWrite(stmt.table());
        // A rule rewrites the statement before anything else looks at the table, so an
        // INSTEAD NOTHING rule means no update happens and none of the checks below apply.
        QueryResult ruled = applyInsteadRule(stmt.table(), "UPDATE", QueryResult.Type.UPDATE,
                stmt.where(), stmt.setClauses());
        if (ruled != null) return ruled;
        // A DO ALSO rule is added to the statement, so its actions run against the rows as they
        // are now and the statement then goes on to do its own work.
        applyAlsoRule(stmt.table(), "UPDATE", stmt.where(), stmt.setClauses(),
                stmt.alias(), stmt.from());
        String schemaName = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        // Collect WITH CHECK OPTION constraints from views we're updating through
        List<DmlValidationHelper.ViewCheck> viewCheckExprs = validationHelper.collectViewCheckExprs(stmt.table());
        // H35: honor explicit schema qualifier so a same-named temp table cannot shadow it
        executor.viewDmlVerb = "update";
        Table table = executor.resolveTable(schemaName, stmt.table(), stmt.schema() != null);
        // An UPDATE names one row at a time; there is no group behind it to aggregate and no
        // result to number a window against, in either the assignments or the WHERE.
        checkUpdatePlacement(stmt, table);
        // Capture view column mapping before further resolveTable calls clobber it (renamed-column views).
        this.activeViewColMap = executor.lastViewColumnMapping;
        this.activeViewColOrder = executor.lastViewColumnOrder;
        // A view column computed from an expression has nothing to assign back to.
        Set<String> viewExprCols = executor.lastViewExpressionColumns;
        if (viewExprCols != null) {
            for (InsertStmt.SetClause set : stmt.setClauses()) {
                if (viewExprCols.contains(set.column().toLowerCase())) {
                    MemgresException ex = new MemgresException("cannot update column \"" + set.column()
                            + "\" of view \"" + stmt.table() + "\"", "0A000");
                    ex.setDetail(ViewUpdatability.DETAIL_NOT_COLUMN);
                    throw ex;
                }
            }
        }
        // An assignment names a column of the relation, and PostgreSQL resolves it while it is
        // still rewriting the statement. Leaving it to the per-row apply meant an UPDATE over a
        // table with no rows — or one whose WHERE matched none — quietly reported success.
        List<String> setTargets = new ArrayList<>();
        for (InsertStmt.SetClause set : stmt.setClauses()) setTargets.add(set.column());
        requireTargetColumns(table, stmt.table(), setTargets);
        // The rest of the statement is resolved against the same relation, so a name in the
        // WHERE or on the right of an assignment is refused before the scan rather than by
        // whichever row happened to reach the evaluator first.
        if (stmt.from() == null || stmt.from().isEmpty()) {
            requireReadableColumns(table, stmt.where(), stmt.alias(), stmt.table());
            for (InsertStmt.SetClause set : stmt.setClauses()) {
                requireReadableColumns(table, set.value(), stmt.alias(), stmt.table());
            }
        }
        // C6: Enforce UPDATE privilege
        executor.checkTablePrivilege("UPDATE", schemaName, stmt.table());
        // Check for attempts to assign to system columns (PG error 0A000, before replica identity check)
        for (InsertStmt.SetClause set : stmt.setClauses()) {
            String col = set.column().toLowerCase();
            if (col.equals("ctid") || col.equals("xmin") || col.equals("xmax")
                    || col.equals("cmin") || col.equals("cmax") || col.equals("tableoid")) {
                throw new MemgresException("cannot assign to system column \"" + set.column() + "\"", "0A000");
            }
        }
        checkReplicaIdentity(table, stmt.table(), "update");
        // Validate RETURNING columns exist before processing rows
        validateReturning(stmt.returning(), table);
        List<PgTrigger> triggers = triggersDisabled() ? Cols.listOf() : executor.database.getTriggersForTable(stmt.table());
        // INSTEAD OF UPDATE triggers on a view: the trigger performs the actual work; the virtual
        // view table's rows are only used to evaluate WHERE and populate OLD/NEW for the trigger.
        boolean hasInsteadOfUpdate = triggers.stream().anyMatch(
                t -> t.getTiming() == PgTrigger.Timing.INSTEAD_OF && t.getEvent() == PgTrigger.Event.UPDATE);
        List<Object[]> returningRows = new ArrayList<>();

        // Validate: FROM table alias must not conflict with target table alias
        if (stmt.from() != null && !stmt.from().isEmpty()) {
            String targetAlias = (stmt.alias() != null ? stmt.alias() : stmt.table()).toLowerCase();
            for (SelectStmt.FromItem fi : stmt.from()) {
                String fromAlias = partitionHelper.extractFromItemAlias(fi);
                if (fromAlias != null && fromAlias.equalsIgnoreCase(targetAlias)) {
                    // The name is given twice, which is what PostgreSQL says: the target and
                    // the FROM item answer to one name and neither can be reached. "ambiguous"
                    // is a different complaint with a different message under the same SQLSTATE.
                    throw new MemgresException(
                            "table name \"" + targetAlias + "\" specified more than once", "42712");
                }
            }
        }

        // Resolve FROM clause tables (for multi-table UPDATE)
        List<RowContext> fromContexts = null;
        if (stmt.from() != null && !stmt.from().isEmpty()) {
            // The WHERE goes with the FROM: it is the qual above whatever joins the FROM holds,
            // and a full join below it is planned as an inner one when it rejects padded rows.
            fromContexts = executor.fromResolver.resolveWrittenFromClause(stmt.from());
        }

        // Pre-flight type validation of WHERE clause (PG checks at plan time, even on empty tables)
        if (stmt.where() != null) {
            executor.constraintValidator.validateWhereTypesAgainstTable(stmt.where(), table);
        }

        // Pre-flight: reject writes to GENERATED ALWAYS columns (even on empty tables, PG errors at plan time)
        for (InsertStmt.SetClause set : stmt.setClauses()) {
            int colIdx = table.getColumnIndex(mapViewColumn(set.column()));
            if (colIdx >= 0) {
                Column genCol = table.getColumns().get(colIdx);
                if (genCol.isGenerated() && !(set.value() instanceof Literal && ((Literal) set.value()).literalType() == Literal.LiteralType.DEFAULT)) {
                    // The cast that used to stand here read the value as a Literal purely to
                    // discard it, so assigning an expression threw ClassCastException instead of
                    // reaching the refusal on the very next line.
                    throw new MemgresException("column \"" + genCol.getName() + "\" can only be updated to DEFAULT\n  Detail: Column \"" + genCol.getName() + "\" is a generated column.", "428C9");
                }
                if (genCol.getDefaultValue() != null && genCol.getDefaultValue().contains("__identity__:always")
                        && !(set.value() instanceof Literal && ((Literal) set.value()).literalType() == Literal.LiteralType.DEFAULT)) {
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

        // Fire BEFORE STATEMENT triggers
        triggerHelper.fireStatementTriggers(triggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.UPDATE, table, null, null);

        // Check if RLS applies to UPDATE
        boolean rlsUpdateActive = false;
        if (table.isRlsEnabled()) {
            String rlsSchema = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
            if (!executor.shouldBypassRls(table, rlsSchema)) {
                rlsUpdateActive = true;
            }
        }

        // When the table has row triggers, snapshot target rows so a trigger raising
        // mid-statement rolls the whole UPDATE back (PostgreSQL statement atomicity).
        Map<Table, List<Object[]>> updSnapshot = triggers.isEmpty() ? null
                : snapshotTargetTables(collectTargetTables(table));
        try {
        boolean fromUpdateHasVirtual = hasVirtualColumns(table);
        if (fromContexts != null) {
            // Multi-table UPDATE: join main table with FROM tables
            List<Object[]> matchedRows = new ArrayList<>();
            List<RowContext> matchedContexts = new ArrayList<>();
            List<RowContext> matchedFromContexts = new ArrayList<>();
            for (Object[] row : rows) {
                for (RowContext fromCtx : fromContexts) {
                    Object[] evalRow = fromUpdateHasVirtual ? computeVirtualColumns(table, row) : row;
                    RowContext mainCtx = viewAwareCtx(table, stmt.alias(), evalRow);
                    RowContext combined = mainCtx.merge(fromCtx);
                    if (stmt.where() == null || executor.isTruthy(executor.evalExpr(stmt.where(), combined))) {
                        matchedRows.add(row);
                        matchedContexts.add(combined);
                        matchedFromContexts.add(fromCtx);
                    }
                }
            }
            // Process matched rows with their FROM context
            Set<Object[]> updated = Collections.newSetFromMap(new IdentityHashMap<>());
            int updatedCount = 0;
            List<Object[]> fromOldRows = new ArrayList<>();
            List<Object[]> fromNewRows = new ArrayList<>();
            List<Object[]> fromAfterOld = new ArrayList<>();
            List<Object[]> fromAfterNew = new ArrayList<>();
            for (int i = 0; i < matchedRows.size(); i++) {
                Object[] row = matchedRows.get(i);
                if (updated.contains(row)) continue; // Each row updated at most once
                updated.add(row);
                Object[] oldRow = Arrays.copyOf(row, row.length);
                Object[] newRow = triggerHelper.executeTriggers(triggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.UPDATE, row, oldRow, table, updatedColumnNames);
                if (newRow == null) {
                    continue;
                }
                // RLS USING filter: skip rows not visible under UPDATE policy
                if (rlsUpdateActive) {
                    List<Object[]> rlsCheck = filterRowsByRlsUsing(table, Collections.singletonList(row), "UPDATE", stmt.alias());
                    if (rlsCheck.isEmpty()) continue;
                }
                RowContext ctx = matchedContexts.get(i);
                applySetClauses(stmt.setClauses(), table, newRow, ctx);
                // RLS WITH CHECK on the new row
                if (rlsUpdateActive) {
                    enforceRlsWithCheck(table, newRow, "UPDATE");
                }
                computeGeneratedColumns(table, newRow);
                executor.constraintValidator.validateConstraints(table, newRow, row);
                validationHelper.validateDomainChecks(newRow, table);
                recordUpdateUndo(stmt.schema(), stmt.table(), row, oldRow);
                table.updateRowInPlace(row, oldRow, newRow);
                recordRowUpdateMeta(stmt.schema(), table, row);
                updatedCount++;
                executor.constraintValidator.handleFkOnUpdate(table, oldRow, row);
                fromOldRows.add(oldRow);
                fromNewRows.add(Arrays.copyOf(row, row.length));
                fromAfterOld.add(oldRow);
                fromAfterNew.add(Arrays.copyOf(row, row.length));
                if (stmt.returning() != null && !stmt.returning().isEmpty()) {
                    returningRows.add(evalReturning(stmt.returning(), table, stmt.alias(), row, oldRow, row, matchedFromContexts.get(i)));
                }
            }
            // Fire queued AFTER ROW triggers
            for (int i = 0; i < fromAfterNew.size(); i++) {
                triggerHelper.executeTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.UPDATE, fromAfterNew.get(i), fromAfterOld.get(i), table, updatedColumnNames);
            }
            int count = updatedCount;
            // Fire statement-level AFTER UPDATE triggers with transition tables
            triggerHelper.fireStatementTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.UPDATE, table, fromNewRows, fromOldRows);
            // Track DML statistics
            if (count > 0) table.incrementTupUpdated(count);
            if (stmt.returning() != null && !stmt.returning().isEmpty()) {
                return QueryResult.returning(QueryResult.Type.UPDATE,
                        buildReturningColumns(stmt.returning(), table), returningRows, count);
            }
            return QueryResult.command(QueryResult.Type.UPDATE, count);
        }

        // Simple UPDATE (no FROM clause)
        String updateAlias = stmt.alias();
        boolean updateHasVirtual = hasVirtualColumns(table);
        if (stmt.where() instanceof com.memgres.engine.parser.ast.CurrentOfExpr) {
            com.memgres.engine.parser.ast.CurrentOfExpr cof = (com.memgres.engine.parser.ast.CurrentOfExpr) stmt.where();
            rows = filterByCurrentOf(cof, table, rows);
        } else if (stmt.where() != null) {
            final List<Object[]> scanned = rows;
            final Table updateTable = table;
            rows = matchAgainstCommittedRows(table, scanned, row -> {
                Object[] evalRow = updateHasVirtual ? computeVirtualColumns(table, row) : row;
                return executor.isTruthy(executor.evalExpr(stmt.where(), viewAwareCtx(table, updateAlias, evalRow)));
            }, () -> rescanTable(updateTable));
        }

        // RLS USING filter for UPDATE: restrict which rows can be updated
        if (rlsUpdateActive) {
            rows = filterRowsByRlsUsing(table, rows, "UPDATE", updateAlias);
        }

        // PG's UPDATE takes a FOR UPDATE lock on every row it touches; a concurrent
        // FOR UPDATE NOWAIT must see it, or queue workers double-process the same row.
        lockRowsForDml(table, rows);

        int updatedCount = 0;
        List<Object[]> simpleOldRows = new ArrayList<>();
        List<Object[]> simpleNewRows = new ArrayList<>();
        List<Object[]> simpleAfterOld = new ArrayList<>();
        List<Object[]> simpleAfterNew = new ArrayList<>();
        for (Object[] row : rows) {
            Object[] oldRow = Arrays.copyOf(row, row.length);

            // Build new row values on a COPY; don't modify live data until validated
            Object[] newRow = Arrays.copyOf(row, row.length);

            // Apply SET clauses first, then fire BEFORE UPDATE triggers so they see NEW with proposed values
            Object[] evalRow = updateHasVirtual ? computeVirtualColumns(table, row) : row;
            applySetClauses(stmt.setClauses(), table, newRow, viewAwareCtx(table, updateAlias, evalRow));

            // INSTEAD OF UPDATE trigger on a view: the trigger does the real work (against base
            // tables), so fire it with OLD/NEW and skip the normal storage update entirely.
            if (hasInsteadOfUpdate) {
                Object[] res = triggerHelper.executeTriggers(triggers, PgTrigger.Timing.INSTEAD_OF, PgTrigger.Event.UPDATE, newRow, oldRow, table, updatedColumnNames);
                if (res != null) {
                    updatedCount++;
                    if (stmt.returning() != null && !stmt.returning().isEmpty()) {
                        returningRows.add(evalReturning(stmt.returning(), table, updateAlias, res, oldRow, res));
                    }
                }
                continue;
            }

            // RLS WITH CHECK on the new row after SET clauses
            if (rlsUpdateActive) {
                enforceRlsWithCheck(table, newRow, "UPDATE");
            }

            // BEFORE UPDATE triggers (see NEW with SET-applied values, can further modify NEW)
            newRow = triggerHelper.executeTriggers(triggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.UPDATE, newRow, oldRow, table, updatedColumnNames);
            if (newRow == null) {
                continue;
            }

            computeGeneratedColumns(table, newRow);
            validationHelper.enforceViewCheckOption(viewCheckExprs, table, newRow);
            executor.constraintValidator.validateConstraints(table, newRow, row);
            validationHelper.validateDomainChecks(newRow, table);
            recordUpdateUndo(stmt.schema(), stmt.table(), row, oldRow);

            // Check if partition key changed and row needs to move between partitions
            if (table.getPartitionStrategy() != null && !table.getPartitions().isEmpty()) {
                Table newTarget = partitionHelper.routeToPartition(table, newRow);
                Table currentPartition = null;
                List<Table> allParts = new ArrayList<>();
                DmlPartitionHelper.collectAllPartitionTables(table, allParts);
                for (Table pt : allParts) {
                    if (pt.getRows().contains(row)) {
                        currentPartition = pt;
                        break;
                    }
                }
                if (currentPartition != null && currentPartition != newTarget) {
                    currentPartition.deleteRow(row);
                    Object[] movedRow = Arrays.copyOf(newRow, newRow.length);
                    newTarget.insertRow(movedRow);
                } else if (currentPartition != null) {
                    currentPartition.updateRowInPlace(row, oldRow, newRow);
                    recordRowUpdateMeta(stmt.schema(), table, row);
                } else {
                    table.updateRowInPlace(row, oldRow, newRow);
                    recordRowUpdateMeta(stmt.schema(), table, row);
                }
            } else {
                table.updateRowInPlace(row, oldRow, newRow);
                recordRowUpdateMeta(stmt.schema(), table, row);
            }
            updatedCount++;
            executor.constraintValidator.handleFkOnUpdate(table, oldRow, row);

            // Queue AFTER ROW triggers
            simpleOldRows.add(oldRow);
            simpleNewRows.add(Arrays.copyOf(row, row.length));
            simpleAfterOld.add(oldRow);
            simpleAfterNew.add(Arrays.copyOf(row, row.length));

            if (stmt.returning() != null && !stmt.returning().isEmpty()) {
                returningRows.add(evalReturning(stmt.returning(), table, updateAlias, row, oldRow, row));
            }
        }

        // Fire queued AFTER ROW triggers
        for (int i = 0; i < simpleAfterNew.size(); i++) {
            triggerHelper.executeTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.UPDATE, simpleAfterNew.get(i), simpleAfterOld.get(i), table, updatedColumnNames);
        }

        // Fire statement-level AFTER UPDATE triggers with transition tables
        triggerHelper.fireStatementTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.UPDATE, table, simpleNewRows, simpleOldRows);

        // Track DML statistics
        if (updatedCount > 0) table.incrementTupUpdated(updatedCount);

        if (stmt.returning() != null && !stmt.returning().isEmpty()) {
            return QueryResult.returning(QueryResult.Type.UPDATE,
                    buildReturningColumns(stmt.returning(), table), returningRows, updatedCount);
        }
        return QueryResult.command(QueryResult.Type.UPDATE, updatedCount);
        } catch (MemgresException e) {
            // Statement is atomic: undo any rows already updated before the failure.
            if (updSnapshot != null) {
                restoreTargetTables(updSnapshot);
                if (executor.session != null) executor.session.discardUndoForCurrentStatement();
            }
            throw e;
        }
    }

    /**
     * Enforce RLS WITH CHECK policies for INSERT/UPDATE.
     * Throws 42501 if the new row doesn't satisfy all applicable WITH CHECK policies,
     * or if no policies apply (default-deny).
     */
    private void enforceRlsWithCheck(Table table, Object[] row, String command) {
        String effectiveRole = executor.currentRole();
        List<RlsPolicy> permissive = new ArrayList<>();
        List<RlsPolicy> restrictive = new ArrayList<>();
        for (RlsPolicy policy : table.getRlsPolicies()) {
            if (!policy.appliesTo(command) || !policy.appliesToRole(effectiveRole)) continue;
            // WITH CHECK falls back to USING when the policy did not write one.
            if (policy.getWithCheckExpr() == null && policy.getUsingExpr() == null) continue;
            if (policy.isRestrictive()) restrictive.add(policy); else permissive.add(policy);
        }
        RowContext rlsCtx = new RowContext(table, null, row);
        // A PERMISSIVE policy grants: writing a second one widens what may be written, so the
        // set is OR-ed. AND-ing them would make each new policy narrow the rule set instead,
        // and would refuse rows the first policy plainly allows.
        boolean granted = false;
        for (RlsPolicy policy : permissive) {
            if (rlsCheckPasses(policy, rlsCtx)) { granted = true; break; }
        }
        if (!granted) {
            throw new MemgresException(
                "new row violates row-level security policy for table \"" + table.getName() + "\"", "42501");
        }
        // A RESTRICTIVE policy takes away: every one of them must also pass, and the one that
        // refused is named.
        for (RlsPolicy policy : restrictive) {
            if (!rlsCheckPasses(policy, rlsCtx)) {
                throw new MemgresException("new row violates row-level security policy \""
                        + policy.getName() + "\" for table \"" + table.getName() + "\"", "42501");
            }
        }
    }

    /** Evaluate one policy's WITH CHECK expression (falling back to USING) against a new row. */
    private boolean rlsCheckPasses(RlsPolicy policy, RowContext ctx) {
        Expression checkExpr = policy.getWithCheckExpr();
        if (checkExpr == null) checkExpr = policy.getUsingExpr();
        if (checkExpr == null) return true;
        try {
            return Boolean.TRUE.equals(executor.evalExpr(checkExpr, ctx));
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Filter rows by RLS USING policies for UPDATE/DELETE.
     * Returns only the rows that pass applicable USING policies.
     */
    private List<Object[]> filterRowsByRlsUsing(Table table, List<Object[]> rows, String command, String alias) {
        String effectiveRole = executor.currentRole();
        List<RlsPolicy> permissivePolicies = new ArrayList<>();
        List<RlsPolicy> restrictivePolicies = new ArrayList<>();
        for (RlsPolicy p : table.getRlsPolicies()) {
            if (p.appliesTo(command) && p.getUsingExpr() != null && p.appliesToRole(effectiveRole)) {
                if (p.isRestrictive()) {
                    restrictivePolicies.add(p);
                } else {
                    permissivePolicies.add(p);
                }
            }
        }
        // Default-deny: no applicable policies → 0 rows
        if (permissivePolicies.isEmpty() && restrictivePolicies.isEmpty()) {
            return new ArrayList<>();
        }
        List<Object[]> filtered = new ArrayList<>();
        for (Object[] row : rows) {
            RowContext ctx = new RowContext(table, alias, row);
            boolean passesPermissive = permissivePolicies.isEmpty() ? false : false;
            if (!permissivePolicies.isEmpty()) {
                for (RlsPolicy policy : permissivePolicies) {
                    try {
                        Object result = executor.evalExpr(policy.getUsingExpr(), ctx);
                        if (Boolean.TRUE.equals(result)) { passesPermissive = true; break; }
                    } catch (Exception e) { /* row doesn't pass */ }
                }
            }
            boolean passesRestrictive = true;
            for (RlsPolicy policy : restrictivePolicies) {
                try {
                    Object result = executor.evalExpr(policy.getUsingExpr(), ctx);
                    if (!Boolean.TRUE.equals(result)) { passesRestrictive = false; break; }
                } catch (Exception e) { passesRestrictive = false; break; }
            }
            if (passesPermissive && passesRestrictive) {
                filtered.add(row);
            }
        }
        return filtered;
    }

    /**
     * Take PG's implicit FOR UPDATE row locks for a plain UPDATE or DELETE, so that a
     * concurrent FOR UPDATE / FOR UPDATE NOWAIT blocks or reports 55P03 the way it does
     * against a real server. Waits (with deadlock detection) exactly like FOR UPDATE.
     */
    /**
     * Choose the rows an UPDATE or DELETE acts on, judging each against the values another
     * session has actually committed.
     *
     * <p>An in-flight change from another transaction must not decide this statement's answer.
     * Matching against the row as it stands lets an uncommitted UPDATE steer a row out of the
     * WHERE clause — the statement quietly touches nothing, and if that transaction rolls back it
     * touched nothing for a change that never happened. So the qual is evaluated against the last
     * committed image, and a row that matches but is being written by someone else is waited for,
     * then judged again on the outcome. Rows another transaction has inserted but not committed
     * are not there to act on at all, and a row it has deleted without committing is still there:
     * the statement waits for it rather than reporting that it deleted nothing.
     *
     * <p>A transaction that has already failed is not waited for. Its rows can never become
     * permanent, so PostgreSQL treats them as dead the instant the statement errored; waiting is
     * how two sessions that broke each other's statement wait for one another with no way out.
     */
    private List<Object[]> matchAgainstCommittedRows(Table table, List<Object[]> rows,
                                                     java.util.function.Predicate<Object[]> matches) {
        return matchAgainstCommittedRows(table, rows, matches, null);
    }

    /** Read a table's rows again, following its partitions, after a wait has ended. */
    private List<Object[]> rescanTable(Table table) {
        List<Object[]> fresh = new ArrayList<>();
        if (table.getPartitionStrategy() != null && !table.getPartitions().isEmpty()) {
            List<Table> allTables = new ArrayList<>();
            DmlPartitionHelper.collectAllPartitionTables(table, allTables);
            for (Table t : allTables) fresh.addAll(t.getRows());
        } else {
            fresh.addAll(table.getRows());
        }
        return fresh;
    }

    /**
     * As above, with a {@code rescan} that re-reads the table after a wait ends. Without one the
     * statement can only decide on the rows it read before waiting, which misses a row the other
     * transaction had deleted and then put back.
     */
    private List<Object[]> matchAgainstCommittedRows(Table table, List<Object[]> rows,
                                                     java.util.function.Predicate<Object[]> matches,
                                                     java.util.function.Supplier<List<Object[]>> rescan) {
        Session me = executor.session;
        List<Object[]> plain = new ArrayList<>();
        if (me == null) {
            for (Object[] row : rows) {
                if (matches.test(row)) plain.add(row);
            }
            return plain;
        }
        final String key = executor.constraintValidator.uncommittedKey(table);
        List<Object[]> scan = rows;
        while (true) {
            // The wait below can end without the row having moved. Polling the cancel token here
            // means a statement_timeout or a client cancel still ends this loop if it does.
            StatementCancel.check();
            Set<Object[]> notCommitted = Collections.newSetFromMap(new IdentityHashMap<Object[], Boolean>());
            Map<Object[], Object[]> otherOld = new IdentityHashMap<>();
            Map<Object[], Session> owner = new IdentityHashMap<>();
            List<Object[]> hiddenByOther = new ArrayList<>();
            for (Session other : executor.database.getActiveSessions()) {
                if (other == me || !other.isInTransaction()) continue;
                boolean doomed = other.isDoomed();
                for (Object[] r : other.getUncommittedInserts(key)) {
                    notCommitted.add(r);
                    if (!doomed) owner.put(r, other);
                }
                for (Map.Entry<Object[], Object[]> e : other.getUncommittedUpdates(key).entrySet()) {
                    otherOld.put(e.getKey(), e.getValue());
                    if (!doomed) owner.put(e.getKey(), other);
                }
                if (doomed) continue;
                for (Object[] r : other.getUncommittedDeletes(key)) {
                    owner.put(r, other);
                    hiddenByOther.add(r);
                }
            }
            List<Object[]> result = new ArrayList<>();
            Session blocker = null;
            Set<Object[]> seen = Collections.newSetFromMap(new IdentityHashMap<Object[], Boolean>());
            for (Object[] row : scan) {
                seen.add(row);
                if (notCommitted.contains(row)) continue;
                Object[] committed = otherOld.containsKey(row) ? otherOld.get(row) : row;
                if (committed == null || !matches.test(committed)) continue;
                Session own = owner.get(row);
                if (own != null) { blocker = own; break; }
                result.add(row);
            }
            // A row another transaction has deleted without committing is gone from the scan but
            // is still committed-live: this statement has to wait for the outcome before it can
            // say whether it deleted it.
            if (blocker == null) {
                for (Object[] row : hiddenByOther) {
                    if (seen.contains(row)) continue;
                    if (matches.test(row)) { blocker = owner.get(row); break; }
                }
            }
            if (blocker == null) return result;
            final Session waitFor = blocker;
            executor.database.awaitConcurrentWrite(me, waitFor,
                    () -> waitFor.isInTransaction() && !waitFor.isDoomed()
                            && waitFor.hasUncommittedWork(key), table.getName());
            if (rescan != null) scan = rescan.get();
        }
    }

    private void lockRowsForDml(Table table, List<Object[]> rows) {
        if (executor.session == null || table == null || rows == null || rows.isEmpty()) return;
        String tName = table.getName();
        for (Object[] row : rows) {
            executor.database.lockRowWaiting(tName, row, executor.session, "UPDATE");
        }
    }

    /** Apply SET clauses to a row. DRYs the duplicate logic between multi-table and simple UPDATE paths. */
    private void applySetClauses(List<InsertStmt.SetClause> setClauses, Table table, Object[] newRow, RowContext ctx) {
        for (InsertStmt.SetClause set : setClauses) {
            int colIdx = table.getColumnIndex(mapViewColumn(set.column()));
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
            // Validate array element type for subscript assignments (e.g. vals[1] = 'not_an_int')
            // The parser transforms array subscript SET into jsonb_set calls. For non-JSONB array
            // columns (INT[], TEXT[], etc.), validate that the assigned value matches the element type.
            if (set.value() instanceof FunctionCallExpr
                    && "jsonb_set".equals(((FunctionCallExpr) set.value()).name())
                    && genCol.getType() == DataType.INT4_ARRAY) {
                // Extract the new element value from the jsonb_set call's 3rd arg (to_jsonb(val))
                FunctionCallExpr jsonbSetCall = (FunctionCallExpr) set.value();
                if (jsonbSetCall.args().size() >= 3) {
                    Expression innerValExpr = jsonbSetCall.args().get(2);
                    Object innerVal = executor.evalExpr(innerValExpr, ctx);
                    if (innerVal instanceof String) {
                        String sv = innerVal.toString().replace("\"", "").trim();
                        try { Long.parseLong(sv); } catch (NumberFormatException e) {
                            throw new MemgresException(
                                    "invalid input syntax for type integer: \"" + sv + "\"", "22P02");
                        }
                    }
                }
            }
            // Handle composite field update: SET col.field = value
            if (set.subField() != null) {
                String compositeTypeName = genCol.getCompositeTypeName();
                if (compositeTypeName != null) {
                    Object currentVal = newRow[colIdx];
                    newRow[colIdx] = updateCompositeField(currentVal, compositeTypeName, set.subField(), val);
                }
            } else {
                newRow[colIdx] = TypeCoercion.coerceForStorage(val, table.getColumns().get(colIdx));
            }
        }
    }

    /** Update a single field within a composite-type value, returning the new composite value. */
    private Object updateCompositeField(Object currentVal, String typeName, String fieldName, Object newFieldVal) {
        java.util.List<com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField> fields =
                executor.database.getCompositeType(typeName);
        if (fields == null) return currentVal;

        // Find the field index
        int fieldIdx = -1;
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equalsIgnoreCase(fieldName)) {
                fieldIdx = i;
                break;
            }
        }
        if (fieldIdx < 0) return currentVal;

        // Parse current composite value into parts
        java.util.List<Object> values = new java.util.ArrayList<>();
        if (currentVal instanceof AstExecutor.PgRow) {
            AstExecutor.PgRow row = (AstExecutor.PgRow) currentVal;
            values.addAll(row.values());
        } else if (currentVal instanceof String) {
            String s = (String) currentVal;
            if (s.startsWith("(") && s.endsWith(")")) {
                String[] parts = executor.compositeTypeHandler.splitCompositeString(s.substring(1, s.length() - 1));
                for (int i = 0; i < fields.size(); i++) {
                    if (i < parts.length) {
                        String part = parts[i].trim();
                        if (part.isEmpty()) {
                            values.add(null);
                        } else {
                            values.add(executor.compositeTypeHandler.coerceFieldValue(part, fields.get(i).typeName()));
                        }
                    } else {
                        values.add(null);
                    }
                }
            }
        } else if (currentVal == null) {
            // Initialize all fields to null
            for (int i = 0; i < fields.size(); i++) {
                values.add(null);
            }
        }

        // Ensure values list is large enough
        while (values.size() <= fieldIdx) {
            values.add(null);
        }

        // Coerce the new field value to the field's type
        if (newFieldVal != null) {
            Object coerced = executor.compositeTypeHandler.coerceFieldValue(
                    newFieldVal.toString(), fields.get(fieldIdx).typeName());
            values.set(fieldIdx, coerced);
        } else {
            values.set(fieldIdx, null);
        }

        // Reconstruct as string representation
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(",");
            if (values.get(i) != null) sb.append(values.get(i));
        }
        sb.append(")");
        return sb.toString();
    }

    // ---- DELETE ----

    QueryResult executeDelete(DeleteStmt stmt) {
        return withCteScope(stmt.withClauses(), () -> executeDeleteInner(stmt));
    }

    private QueryResult executeDeleteInner(DeleteStmt stmt) {
        // Check read-only transaction
        checkReadOnly("DELETE");
        rejectMaterializedViewWrite(stmt.table());
        QueryResult ruledDelete = applyInsteadRule(stmt.table(), "DELETE", QueryResult.Type.DELETE,
                stmt.where(), null);
        if (ruledDelete != null) return ruledDelete;
        // As for UPDATE: a DO ALSO rule runs beside the statement, over the rows about to go.
        applyAlsoRule(stmt.table(), "DELETE", stmt.where(), null, stmt.alias(), stmt.using());
        String schemaName = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        // H35: honor explicit schema qualifier so a same-named temp table cannot shadow it
        executor.viewDmlVerb = "delete from";
        Table table = executor.resolveTable(schemaName, stmt.table(), stmt.schema() != null);
        // As for UPDATE: a DELETE's WHERE picks rows one at a time, so nothing in it may need a
        // group or a finished result to have a value.
        PlacementCheck placement = executor.selectExecutor.placementCheck;
        placement.reject(stmt.where(), "WHERE");
        // A WHERE decides one row at a time whether to delete it, so a set written in one has
        // nowhere to expand: PostgreSQL refuses it here for the same reason it refuses it in an
        // UPDATE's WHERE, which this already did.
        executor.selectExecutor.rejectSrfIn(stmt.where(), "WHERE");
        placement.rejectOuterLevelAggregate(stmt.where(), "WHERE", table,
                stmt.alias() != null ? stmt.alias() : stmt.table());
        BooleanContext.check(stmt.where(), "WHERE", dmlScope(table,
                stmt.alias() != null ? stmt.alias() : stmt.table(), stmt.using()));
        // Capture view column mapping before further resolveTable calls clobber it (renamed-column views).
        this.activeViewColMap = executor.lastViewColumnMapping;
        this.activeViewColOrder = executor.lastViewColumnOrder;
        // The WHERE is resolved against the relation before the scan, so a name that is not a
        // column of it is refused whether or not there is a row for the evaluator to trip over.
        if (stmt.using() == null || stmt.using().isEmpty()) {
            requireReadableColumns(table, stmt.where(), stmt.alias(), stmt.table());
        }
        // INSTEAD OF DELETE triggers on a view: the trigger performs the actual work; the virtual
        // view table's rows are only used to match WHERE and populate OLD for the trigger.
        List<PgTrigger> deleteTriggersEarly = triggersDisabled() ? Cols.listOf()
                : executor.database.getTriggersForTable(stmt.table());
        boolean hasInsteadOfDelete = deleteTriggersEarly.stream().anyMatch(
                t -> t.getTiming() == PgTrigger.Timing.INSTEAD_OF && t.getEvent() == PgTrigger.Event.DELETE);
        // C6: Enforce DELETE privilege
        executor.checkTablePrivilege("DELETE", schemaName, stmt.table());
        checkReplicaIdentity(table, stmt.table(), "delete");
        // Validate RETURNING columns exist before processing rows
        validateReturning(stmt.returning(), table);
        boolean hasReturning = stmt.returning() != null && !stmt.returning().isEmpty();

        // INSTEAD OF DELETE on a view: fire the trigger for each matching view row (the trigger
        // performs the real deletion against base tables); the virtual table is never modified.
        if (hasInsteadOfDelete) {
            List<Object[]> matched = new ArrayList<>();
            boolean idHasVirtual = hasVirtualColumns(table);
            for (Object[] row : table.getRows()) {
                Object[] evalRow = idHasVirtual ? computeVirtualColumns(table, row) : row;
                if (stmt.where() == null
                        || executor.isTruthy(executor.evalExpr(stmt.where(), viewAwareCtx(table, stmt.alias(), evalRow)))) {
                    matched.add(row);
                }
            }
            int cnt = 0;
            List<Object[]> insteadReturning = new ArrayList<>();
            for (Object[] row : matched) {
                Object[] res = triggerHelper.executeTriggers(deleteTriggersEarly,
                        PgTrigger.Timing.INSTEAD_OF, PgTrigger.Event.DELETE, row, row, table);
                if (res != null) {
                    cnt++;
                    if (hasReturning) {
                        insteadReturning.add(evalReturning(stmt.returning(), table, stmt.alias(), row, row, null));
                    }
                }
            }
            if (hasReturning) {
                return QueryResult.returning(QueryResult.Type.DELETE,
                        buildReturningColumns(stmt.returning(), table), insteadReturning, cnt);
            }
            return QueryResult.command(QueryResult.Type.DELETE, cnt);
        }

        // Validate: USING table alias must not conflict with target table alias
        if (stmt.using() != null && !stmt.using().isEmpty()) {
            String targetAlias = (stmt.alias() != null ? stmt.alias() : stmt.table()).toLowerCase();
            for (SelectStmt.FromItem fi : stmt.using()) {
                String usingAlias = partitionHelper.extractFromItemAlias(fi);
                if (usingAlias != null && usingAlias.equalsIgnoreCase(targetAlias)) {
                    // The name is given twice, which is what PostgreSQL says: the target and
                    // the FROM item answer to one name and neither can be reached. "ambiguous"
                    // is a different complaint with a different message under the same SQLSTATE.
                    throw new MemgresException(
                            "table name \"" + targetAlias + "\" specified more than once", "42712");
                }
            }
        }

        // Check if RLS applies to DELETE
        boolean rlsDeleteActive = false;
        if (table.isRlsEnabled() && !executor.shouldBypassRls(table, schemaName)) {
            rlsDeleteActive = true;
        }

        if (stmt.where() == null) {
            // Collect all tables (including partitions) for DELETE ALL
            List<Table> allTables = new ArrayList<>();
            DmlPartitionHelper.collectAllPartitionTables(table, allTables);

            List<Object[]> allRowsCopy = new ArrayList<>();
            for (Table t : allTables) {
                allRowsCopy.addAll(t.getRows());
            }
            // RLS USING filter for DELETE: restrict which rows can be deleted
            if (rlsDeleteActive) {
                allRowsCopy = filterRowsByRlsUsing(table, allRowsCopy, "DELETE", stmt.alias());
            }
            java.util.Set<Object[]> deleteSet = Collections.newSetFromMap(new IdentityHashMap<>());
            deleteSet.addAll(allRowsCopy);
            for (Object[] row : allRowsCopy) {
                executor.constraintValidator.handleFkOnDelete(table, row, deleteSet);
            }
            // Collect RETURNING before deleting
            List<Object[]> returningRows = new ArrayList<>();
            if (hasReturning) {
                for (Object[] row : allRowsCopy) {
                    returningRows.add(evalReturning(stmt.returning(), table, stmt.alias(), row, row, null));
                }
            }
            recordDeleteUndo(stmt.schema(), stmt.table(), allRowsCopy);
            int count;
            if (rlsDeleteActive) {
                // RLS filtered: delete specific rows, not all
                count = allRowsCopy.size();
                for (Table t : allTables) {
                    t.deleteRows(deleteSet);
                }
            } else {
                count = 0;
                for (Table t : allTables) {
                    count += t.deleteAll();
                }
            }
            // Track DML statistics
            if (count > 0) table.incrementTupDeleted(count);
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
        List<Object[]> deleteOrder = new ArrayList<>();

        boolean deleteHasVirtual = hasVirtualColumns(table);
        Map<Object[], RowContext> deleteUsingCtxMap = new IdentityHashMap<>();
        if (stmt.using() != null && !stmt.using().isEmpty()) {
            // DELETE ... USING: join main table with USING tables, delete matching main rows
            List<RowContext> usingContexts =
                    executor.fromResolver.resolveWrittenFromClause(stmt.using());
            for (Object[] row : allRows) {
                Object[] evalRow = deleteHasVirtual ? computeVirtualColumns(table, row) : row;
                RowContext mainCtx = viewAwareCtx(table, stmt.alias(), evalRow);
                for (RowContext usingCtx : usingContexts) {
                    RowContext merged = mainCtx.merge(usingCtx);
                    if (stmt.where() == null || executor.isTruthy(executor.evalExpr(stmt.where(), merged))) {
                        toDelete.add(row);
                        deleteOrder.add(row);
                        deleteUsingCtxMap.put(row, usingCtx);
                        break;
                    }
                }
            }
            // Sort by join key to match PG's merge-join RETURNING order
            if (!deleteOrder.isEmpty() && stmt.where() instanceof BinaryExpr) {
                BinaryExpr whereExpr = (BinaryExpr) stmt.where();
                if (whereExpr.op() == BinaryExpr.BinOp.EQUAL && whereExpr.left() instanceof ColumnRef) {
                    ColumnRef joinCol = (ColumnRef) whereExpr.left();
                    int joinColIdx = table.getColumnIndex(joinCol.column());
                    if (joinColIdx >= 0) {
                        deleteOrder.sort((a, b) -> {
                            Object av = a[joinColIdx], bv = b[joinColIdx];
                            if (av == null && bv == null) return 0;
                            if (av == null) return -1;
                            if (bv == null) return 1;
                            return av.toString().compareTo(bv.toString());
                        });
                    }
                }
            }
        } else if (stmt.where() instanceof com.memgres.engine.parser.ast.CurrentOfExpr) {
            com.memgres.engine.parser.ast.CurrentOfExpr cof = (com.memgres.engine.parser.ast.CurrentOfExpr) stmt.where();
            List<Object[]> matched = filterByCurrentOf(cof, table, allRows);
            toDelete.addAll(matched);
        } else {
            final List<Object[]> deleteScan = allRows;
            final Map<Object[], Table> deleteOwner = rowOwner;
            final List<Table> deleteTables = tablesToScan;
            toDelete.addAll(matchAgainstCommittedRows(table, allRows, row -> {
                Object[] evalRow = deleteHasVirtual ? computeVirtualColumns(table, row) : row;
                return executor.isTruthy(executor.evalExpr(stmt.where(), viewAwareCtx(table, stmt.alias(), evalRow)));
            }, () -> {
                // A wait has ended, so the table may hold rows that were not there when it began.
                // Both the scan and the row-to-partition map have to be rebuilt from it.
                deleteScan.clear();
                deleteOwner.clear();
                for (Table t : deleteTables) {
                    for (Object[] r : t.getRows()) {
                        deleteScan.add(r);
                        deleteOwner.put(r, t);
                    }
                }
                return deleteScan;
            }));
        }

        // RLS USING filter for DELETE: remove rows that don't pass DELETE policies
        if (rlsDeleteActive) {
            List<Object[]> rlsAllowed = filterRowsByRlsUsing(table, new ArrayList<>(toDelete), "DELETE", stmt.alias());
            Set<Object[]> rlsAllowedSet = Collections.newSetFromMap(new IdentityHashMap<>());
            rlsAllowedSet.addAll(rlsAllowed);
            toDelete.retainAll(rlsAllowedSet);
            deleteOrder.removeIf(r -> !rlsAllowedSet.contains(r));
        }

        // DELETE takes the same FOR UPDATE lock on its target rows as PG does
        lockRowsForDml(table, new ArrayList<>(toDelete));

        // Validate FK references before deleting (handle CASCADE/RESTRICT/SET NULL/SET DEFAULT)
        for (Object[] row : allRows) {
            if (toDelete.contains(row)) {
                executor.constraintValidator.handleFkOnDelete(table, row, toDelete);
            }
        }

        // Fire BEFORE DELETE triggers (for DELETE, OLD = deleted row, NEW = null)
        List<PgTrigger> triggers = triggersDisabled() ? Cols.listOf() : executor.database.getTriggersForTable(table.getName());

        // Fire BEFORE STATEMENT triggers
        triggerHelper.fireStatementTriggers(triggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.DELETE, table, null, null);

        if (!triggers.isEmpty()) {
            Set<Object[]> skipRows = Collections.newSetFromMap(new IdentityHashMap<>());
            for (Object[] row : new ArrayList<>(toDelete)) {
                Object[] result = triggerHelper.executeTriggers(triggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.DELETE, row, row, table);
                if (result == null) skipRows.add(row);
            }
            toDelete.removeAll(skipRows);
        }

        List<Object[]> deletedRows = new ArrayList<>();
        List<Object[]> returningRows = new ArrayList<>();
        List<Object[]> orderedDelete;
        if (!deleteOrder.isEmpty()) {
            orderedDelete = deleteOrder;
        } else {
            orderedDelete = new ArrayList<>();
            for (Object[] row : allRows) {
                if (toDelete.contains(row)) orderedDelete.add(row);
            }
        }
        if (hasReturning) {
            for (Object[] row : orderedDelete) {
                RowContext usingCtx = deleteUsingCtxMap.get(row);
                returningRows.add(evalReturning(stmt.returning(), table, stmt.alias(), row, row, null, usingCtx));
            }
        }
        // Capture old rows for transition tables before deletion
        List<Object[]> oldRowsForTransition = new ArrayList<>();
        for (Object[] row : toDelete) {
            oldRowsForTransition.add(Arrays.copyOf(row, row.length));
        }
        deletedRows.addAll(toDelete);
        // Record undo (and run the RR write-write conflict check) before the physical
        // delete, so an aborted statement leaves no unrecorded mutation behind.
        recordDeleteUndo(stmt.schema(), stmt.table(), deletedRows);
        // When the table has row triggers, snapshot target rows so an AFTER-trigger raising
        // mid-statement rolls the whole DELETE back (PostgreSQL statement atomicity).
        Map<Table, List<Object[]>> delSnapshot = triggers.isEmpty() ? null
                : snapshotTargetTables(tablesToScan);
        int deleted;
        try {
        // Remove matching rows atomically from each owning table
        for (Table t : tablesToScan) {
            t.deleteRows(toDelete);
        }
        deleted = deletedRows.size();

        // Fire queued AFTER DELETE row triggers
        if (!triggers.isEmpty()) {
            for (Object[] row : oldRowsForTransition) {
                triggerHelper.executeTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.DELETE, row, row, table);
            }
        }

        // Fire statement-level AFTER DELETE triggers with transition tables
        triggerHelper.fireStatementTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.DELETE, table, null, oldRowsForTransition);
        } catch (MemgresException e) {
            if (delSnapshot != null) {
                restoreTargetTables(delSnapshot);
                if (executor.session != null) executor.session.discardUndoForCurrentStatement();
            }
            throw e;
        }

        // Track DML statistics
        if (deleted > 0) table.incrementTupDeleted(deleted);

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

    /** PostgreSQL's wording for a WHEN clause that follows an unconditional one; it carries no position. */
    private static MemgresException unreachableWhenClause() {
        return new MemgresException(
                "unreachable WHEN clause specified after unconditional WHEN clause", "42601")
                .suppressPosition();
    }

    /**
     * The verb PostgreSQL blames when a MERGE names a view it cannot write through: the action of
     * the first WHEN clause, which for a single-arm MERGE is the only action it would perform.
     */
    private static String mergeViewDmlVerb(MergeStmt stmt) {
        for (MergeStmt.WhenClause clause : stmt.whenClauses()) {
            if (clause instanceof MergeStmt.WhenNotMatched) return "insert into";
            if (clause instanceof MergeStmt.WhenMatched) {
                return ((MergeStmt.WhenMatched) clause).isDelete() ? "delete from" : "update";
            }
            if (clause instanceof MergeStmt.WhenNotMatchedBySource) {
                return ((MergeStmt.WhenNotMatchedBySource) clause).isDelete() ? "delete from" : "update";
            }
        }
        return "insert into";
    }

    private QueryResult executeMergeInner(MergeStmt stmt) {
        String schemaName = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        rejectMaterializedViewWrite(stmt.targetTable());
        // A MERGE onto a view that cannot take it is refused by the action it wanted to perform,
        // not by INSERT: a MERGE whose only arm is an UPDATE reports "cannot update view".
        executor.viewDmlVerb = mergeViewDmlVerb(stmt);
        // H35: honor explicit schema qualifier so a same-named temp table cannot shadow it
        Table targetTable = executor.resolveTable(schemaName, stmt.targetTable(), stmt.schema() != null);
        String targetAlias = stmt.targetAlias() != null ? stmt.targetAlias() : stmt.targetTable();
        checkMergePlacement(stmt, targetTable, targetAlias);

        // Validate: source cannot be the same unaliased table as target
        if (stmt.source() instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef src = (SelectStmt.TableRef) stmt.source();
            String srcName = src.table();
            if (srcName.equalsIgnoreCase(stmt.targetTable()) && src.alias() == null && stmt.targetAlias() == null) {
                throw new MemgresException("name \"" + srcName + "\" specified more than once", "42712");
            }
        }

        checkMergeWhenClauses(stmt, targetAlias);

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

        // Extract source table for MERGE RETURNING * (includes source columns)
        Table mergeSourceTable = null;
        if (!sourceRows.isEmpty()) {
            List<RowContext.TableBinding> srcBindings = sourceRows.get(0).getBindings();
            if (!srcBindings.isEmpty()) {
                mergeSourceTable = srcBindings.get(0).table();
            }
        }

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
        // Rows an arm has already acted on, so a later source row does not re-run it.
        Set<Object[]> processedTargetRows = Collections.newSetFromMap(new IdentityHashMap<>());
        // Rows actually modified. Only these raise 21000: PG's "affect a row a second time"
        // counts real UPDATE/DELETE actions, not DO NOTHING arms or non-firing conditions.
        Set<Object[]> affectedTargetRows = Collections.newSetFromMap(new IdentityHashMap<>());
        // Rows the ON condition paired with some source row. WHEN NOT MATCHED BY SOURCE asks
        // about the join and nothing else: a target row that a source row matched is matched
        // whether or not any WHEN MATCHED arm went on to act on it -- and with no WHEN MATCHED
        // arm at all, every row of the table used to look unmatched.
        Set<Object[]> matchedBySourceRows = Collections.newSetFromMap(new IdentityHashMap<>());
        // Collect new rows to insert
        List<Object[]> rowsToInsert = new ArrayList<>();

        // Use snapshot rows for ON-condition matching (PG matches against pre-MERGE state)
        List<Object[]> originalTargetRows = new ArrayList<>(targetTable.getRows());

        boolean mergeTargetHasVirtual = hasVirtualColumns(targetTable);
        // Collect unmatched source rows for deferred NOT MATCHED BY TARGET processing
        List<RowContext> unmatchedSourceRows = new ArrayList<>();
        try {
        for (RowContext sourceCtx : sourceRows) {
            // Find matching target rows for this source row using the original snapshot
            List<Object[]> matchedTargetRows = new ArrayList<>();
            for (Object[] targetRow : originalTargetRows) {
                Object[] evalRow = mergeTargetHasVirtual ? computeVirtualColumns(targetTable, targetRow) : targetRow;
                RowContext targetCtx = new RowContext(targetTable, targetAlias, evalRow);
                RowContext combined = targetCtx.merge(sourceCtx);
                if (executor.isTruthy(executor.evalExpr(stmt.onCondition(), combined))) {
                    matchedBySourceRows.add(targetRow);
                    // PG 21000: only a second real modification of the row is an error
                    if (affectedTargetRows.contains(targetRow)) {
                        throw new MemgresException(
                                "MERGE command cannot affect row a second time", "21000");
                    }
                    matchedTargetRows.add(targetRow);
                }
            }

            if (!matchedTargetRows.isEmpty()) {
                // WHEN MATCHED clauses
                for (Object[] targetRow : matchedTargetRows) {
                    if (processedTargetRows.contains(targetRow)) continue;
                    Object[] evalRow = mergeTargetHasVirtual ? computeVirtualColumns(targetTable, targetRow) : targetRow;
                    RowContext targetCtx = new RowContext(targetTable, targetAlias, evalRow);
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
                                    returningRows.add(evalReturning(stmt.returning(), targetTable, targetAlias, targetRow, targetRow, null, sourceCtx));
                                }
                                executor.constraintValidator.handleFkOnDelete(targetTable, targetRow);
                                rowsToDelete.add(targetRow);
                                processedTargetRows.add(targetRow);
                                affectedTargetRows.add(targetRow);
                                mergeCount++;
                            } else if (wm.setClauses() != null && !wm.setClauses().isEmpty()) {
                                // UPDATE — evaluate all SET RHS against original row snapshot onto a
                                // working copy, then let BEFORE triggers see/modify it before committing.
                                Object[] oldRow = Arrays.copyOf(targetRow, targetRow.length);
                                Object[] newRow = Arrays.copyOf(targetRow, targetRow.length);
                                Set<String> updCols = new HashSet<>();
                                for (int si = 0; si < wm.setClauses().size(); si++) {
                                    InsertStmt.SetClause set = wm.setClauses().get(si);
                                    int colIdx = targetTable.getColumnIndex(set.column());
                                    if (colIdx < 0) {
                                        throw new MemgresException("Column not found: " + set.column());
                                    }
                                    updCols.add(set.column().toLowerCase());
                                    newRow[colIdx] = TypeCoercion.coerceForStorage(
                                            executor.evalExpr(set.value(), combined), targetTable.getColumns().get(colIdx));
                                }
                                // Fire BEFORE UPDATE row triggers: PG fires them per matched row.
                                // They may modify NEW or return NULL to skip the row entirely.
                                newRow = triggerHelper.executeTriggers(triggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.UPDATE, newRow, oldRow, targetTable, updCols);
                                if (newRow == null) {
                                    // BEFORE trigger suppressed this row: no update, not counted.
                                    processedTargetRows.add(targetRow);
                                    break;
                                }
                                // Commit the (possibly trigger-modified) new values onto the live row.
                                System.arraycopy(newRow, 0, targetRow, 0, targetRow.length);
                                computeGeneratedColumns(targetTable, targetRow);
                                executor.constraintValidator.validateConstraints(targetTable, targetRow, targetRow);
                                recordUpdateUndo(stmt.schema(), stmt.targetTable(), targetRow, oldRow);
                                executor.constraintValidator.handleFkOnUpdate(targetTable, oldRow, targetRow);
                                // Fire AFTER UPDATE triggers for MERGE
                                triggerHelper.executeTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.UPDATE, targetRow, oldRow, targetTable, updCols);
                                // Collect RETURNING after update (uses new values)
                                if (hasReturning) {
                                    executor.currentMergeAction = "UPDATE";
                                    returningRows.add(evalReturning(stmt.returning(), targetTable, targetAlias, targetRow, oldRow, targetRow, sourceCtx));
                                }
                                processedTargetRows.add(targetRow);
                                affectedTargetRows.add(targetRow);
                                mergeCount++;
                            } else {
                                // DO NOTHING — the row is untouched, so a later source row may
                                // still act on it and it never counts towards the 21000 guard.
                            }
                            break; // first matching WHEN clause wins
                        }
                    }
                }
            } else {
                // Defer NOT MATCHED BY TARGET inserts until after NOT MATCHED BY SOURCE
                unmatchedSourceRows.add(sourceCtx);
            }
        }

            // WHEN NOT MATCHED BY SOURCE: process target rows that had no source match
            boolean hasNotMatchedBySource = stmt.whenClauses().stream()
                    .anyMatch(c -> c instanceof MergeStmt.WhenNotMatchedBySource);
            if (hasNotMatchedBySource) {
                for (Object[] targetRow : originalTargetRows) {
                    if (matchedBySourceRows.contains(targetRow)) continue;
                    if (processedTargetRows.contains(targetRow)) continue;
                    if (rowsToDelete.contains(targetRow)) continue;
                    Object[] evalRow = mergeTargetHasVirtual ? computeVirtualColumns(targetTable, targetRow) : targetRow;
                    RowContext targetCtx = new RowContext(targetTable, targetAlias, evalRow);
                    for (MergeStmt.WhenClause clause : stmt.whenClauses()) {
                        if (clause instanceof MergeStmt.WhenNotMatchedBySource) {
                            MergeStmt.WhenNotMatchedBySource wnmbs = (MergeStmt.WhenNotMatchedBySource) clause;
                            if (wnmbs.andCondition() != null && !executor.isTruthy(executor.evalExpr(wnmbs.andCondition(), targetCtx))) {
                                continue;
                            }
                            // Build null-padded source context for RETURNING * (NOT MATCHED BY SOURCE has no source row)
                            RowContext nullSourceCtx = null;
                            if (mergeSourceTable != null) {
                                Object[] nullSourceRow = new Object[mergeSourceTable.getColumns().size()];
                                nullSourceCtx = new RowContext(mergeSourceTable, null, nullSourceRow);
                            }
                            if (wnmbs.isDelete()) {
                                if (hasReturning) {
                                    executor.currentMergeAction = "DELETE";
                                    returningRows.add(evalReturning(stmt.returning(), targetTable, targetAlias, targetRow, targetRow, null, nullSourceCtx));
                                }
                                executor.constraintValidator.handleFkOnDelete(targetTable, targetRow);
                                rowsToDelete.add(targetRow);
                                mergeCount++;
                            } else if (wnmbs.setClauses() != null && !wnmbs.setClauses().isEmpty()) {
                                Object[] oldRow = Arrays.copyOf(targetRow, targetRow.length);
                                Object[] newRow = Arrays.copyOf(targetRow, targetRow.length);
                                Set<String> updCols = new HashSet<>();
                                for (InsertStmt.SetClause set : wnmbs.setClauses()) {
                                    int colIdx = targetTable.getColumnIndex(set.column());
                                    if (colIdx < 0) {
                                        throw new MemgresException("Column not found: " + set.column());
                                    }
                                    updCols.add(set.column().toLowerCase());
                                    Object val = executor.evalExpr(set.value(), targetCtx);
                                    newRow[colIdx] = TypeCoercion.coerceForStorage(val, targetTable.getColumns().get(colIdx));
                                }
                                // Fire BEFORE UPDATE row triggers (may modify NEW or skip via NULL).
                                newRow = triggerHelper.executeTriggers(triggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.UPDATE, newRow, oldRow, targetTable, updCols);
                                if (newRow == null) {
                                    // BEFORE trigger suppressed this row: no update, not counted.
                                    processedTargetRows.add(targetRow);
                                    break;
                                }
                                System.arraycopy(newRow, 0, targetRow, 0, targetRow.length);
                                computeGeneratedColumns(targetTable, targetRow);
                                executor.constraintValidator.validateConstraints(targetTable, targetRow, targetRow);
                                recordUpdateUndo(stmt.schema(), stmt.targetTable(), targetRow, oldRow);
                                executor.constraintValidator.handleFkOnUpdate(targetTable, oldRow, targetRow);
                                triggerHelper.executeTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.UPDATE, targetRow, oldRow, targetTable, updCols);
                                if (hasReturning) {
                                    executor.currentMergeAction = "UPDATE";
                                    returningRows.add(evalReturning(stmt.returning(), targetTable, targetAlias, targetRow, oldRow, targetRow, nullSourceCtx));
                                }
                                mergeCount++;
                            }
                            // DO NOTHING: empty setClauses, no action and nothing to count
                            processedTargetRows.add(targetRow);
                            break;
                        }
                    }
                }
            }

            // WHEN NOT MATCHED BY TARGET: process deferred inserts for unmatched source rows
            for (RowContext sourceCtx : unmatchedSourceRows) {
                for (MergeStmt.WhenClause clause : stmt.whenClauses()) {
                    if (clause instanceof MergeStmt.WhenNotMatched) {
                        MergeStmt.WhenNotMatched wnm = (MergeStmt.WhenNotMatched) clause;
                        if (wnm.andCondition() != null && !executor.isTruthy(executor.evalExpr(wnm.andCondition(), sourceCtx))) {
                            continue;
                        }
                        if (wnm.doNothing()) {
                            break;
                        }
                        // INSERT
                        Object[] newRow = new Object[targetTable.getColumns().size()];
                        fillDefaults(targetTable, newRow);
                        if (wnm.columns() != null) {
                            for (int i = 0; i < wnm.columns().size(); i++) {
                                int colIdx = targetTable.getColumnIndex(wnm.columns().get(i));
                                if (colIdx < 0) {
                                    throw new MemgresException("Column not found: " + wnm.columns().get(i));
                                }
                                Column genCol = targetTable.getColumns().get(colIdx);
                                if (genCol.isGenerated()) {
                                    throw new MemgresException("cannot insert a non-DEFAULT value into column \"" + genCol.getName() + "\"\n  Detail: Column \"" + genCol.getName() + "\" is a generated column.", "428C9");
                                }
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
                        // Fire BEFORE INSERT row triggers: PG fires them per inserted row.
                        // They may modify NEW or return NULL to skip the row entirely.
                        newRow = triggerHelper.executeTriggers(triggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.INSERT, newRow, null, targetTable);
                        if (newRow == null) {
                            // BEFORE trigger suppressed this row: not inserted, not counted.
                            break;
                        }
                        validationHelper.validateEnumValues(newRow, targetTable);
                        Table routedTable = partitionHelper.routeToPartition(targetTable, newRow);
                        routedTable.getWriteLock().lock();
                        try {
                            executor.constraintValidator.validateConstraints(targetTable, newRow, null);
                            routedTable.insertRow(newRow);
                            try {
                                executor.constraintValidator.validateConstraints(routedTable, newRow, newRow);
                            } catch (MemgresException e) {
                                routedTable.removeRow(newRow);
                                throw e;
                            }
                        } finally {
                            routedTable.getWriteLock().unlock();
                        }
                        rowsToInsert.add(newRow);
                        triggerHelper.executeTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.INSERT, newRow, null, targetTable);
                        if (hasReturning) {
                            executor.currentMergeAction = "INSERT";
                            returningRows.add(evalReturning(stmt.returning(), targetTable, targetAlias, newRow, null, newRow, sourceCtx));
                        }
                        mergeCount++;
                        break;
                    }
                }
            }

            // Apply deletes atomically under write lock so concurrent readers
            // never see the intermediate empty-table state.
            if (!rowsToDelete.isEmpty()) {
                targetTable.getWriteLock().lock();
                try {
                    List<Object[]> allRows = new ArrayList<>(targetTable.getRows());
                    List<Object[]> deletedRows = new ArrayList<>();
                    for (Object[] row : allRows) {
                        if (rowsToDelete.contains(row)) {
                            deletedRows.add(row);
                        }
                    }
                    // Record undo (and run the RR conflict check) before mutating.
                    recordDeleteUndo(stmt.schema(), stmt.targetTable(), deletedRows);
                    targetTable.deleteAll();
                    for (Object[] row : allRows) {
                        if (!rowsToDelete.contains(row)) {
                            targetTable.insertRow(row);
                        }
                    }
                } finally {
                    targetTable.getWriteLock().unlock();
                }
            }

            // Validate uniqueness for already-inserted rows (they were inserted eagerly during iteration)
            for (Object[] newRow : rowsToInsert) {
                Table routedTable = partitionHelper.routeToPartition(targetTable, newRow);
                // Row is already in the table, so just validate constraints
                try {
                    executor.constraintValidator.validateConstraints(routedTable, newRow, newRow);
                } catch (MemgresException e) {
                    // Roll back the row we just inserted before re-throwing
                    routedTable.removeRow(newRow);
                    throw e;
                }
                recordInsertUndo(stmt.schema(), routedTable.getName(), newRow);
            }
        } catch (MemgresException e) {
            // MERGE is atomic; roll back all changes (updates, deletes, inserts) on failure
            executor.currentMergeAction = null;
            targetTable.getWriteLock().lock();
            try {
                targetTable.deleteAll();
                for (Object[] origRow : snapshotRows) {
                    targetTable.insertRow(origRow);
                }
            } finally {
                targetTable.getWriteLock().unlock();
            }
            // The table is back as it was, so the statement's undo entries describe changes that
            // have already been reversed; replaying them would write the same rows twice.
            if (executor.session != null) executor.session.discardUndoForCurrentStatement();
            throw e;
        }

        executor.currentMergeAction = null;
        if (hasReturning) {
            List<Column> retCols = buildReturningColumns(stmt.returning(), targetTable, mergeSourceTable);
            return QueryResult.returning(QueryResult.Type.MERGE, retCols, returningRows, mergeCount);
        }
        return QueryResult.command(QueryResult.Type.MERGE, mergeCount);
    }

    // ---- Statement atomicity: snapshot / restore target-table rows ----
    //
    // PostgreSQL treats each INSERT/UPDATE/DELETE/MERGE as an atomic statement: if a row
    // trigger (or constraint) raises partway through, every side effect the statement had
    // already applied is rolled back. Autocommit statements have no transaction undo log to
    // lean on (Session.recordUndo only records inside a transaction), so we snapshot the
    // affected table(s) up front and, on failure, restore them. Mirrors the MERGE executor.

    /** Collect the target table plus all of its leaf partitions (rows can live on either). */
    private List<Table> collectTargetTables(Table table) {
        List<Table> tables = new ArrayList<>();
        if (table.getPartitionStrategy() != null && !table.getPartitions().isEmpty()) {
            DmlPartitionHelper.collectAllPartitionTables(table, tables);
        } else {
            tables.add(table);
        }
        return tables;
    }

    /** Deep-copy every row of each given table so the statement can be rolled back. */
    private Map<Table, List<Object[]>> snapshotTargetTables(List<Table> tables) {
        Map<Table, List<Object[]>> snapshot = new IdentityHashMap<>();
        for (Table t : tables) {
            List<Object[]> copy = new ArrayList<>();
            for (Object[] r : t.getRows()) {
                copy.add(Arrays.copyOf(r, r.length));
            }
            snapshot.put(t, copy);
        }
        return snapshot;
    }

    /** Restore each snapshotted table to its captured state (used on mid-statement failure). */
    private void restoreTargetTables(Map<Table, List<Object[]>> snapshot) {
        for (Map.Entry<Table, List<Object[]>> e : snapshot.entrySet()) {
            Table t = e.getKey();
            t.getWriteLock().lock();
            try {
                t.deleteAll();
                for (Object[] r : e.getValue()) {
                    t.insertRow(r);
                }
            } finally {
                t.getWriteLock().unlock();
            }
        }
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
            String replacement;
            if (val == null) {
                replacement = "NULL";
            } else if (val instanceof Number || val instanceof Boolean) {
                replacement = val.toString();
            } else if (val instanceof HstoreValue) {
                replacement = "'" + val.toString().replace("'", "''") + "'::hstore";
            } else {
                replacement = "'" + val.toString().replace("'", "''") + "'";
            }
            sql = sql.replaceAll("(?i)\\b" + java.util.regex.Pattern.quote(c.getName()) + "\\b", replacement);
        }
        try {
            QueryResult result = executor.execute("SELECT " + sql);
            if (!result.getRows().isEmpty() && result.getRows().get(0).length > 0) {
                return TypeCoercion.coerceForStorage(result.getRows().get(0)[0], col);
            }
        } catch (MemgresException e) {
            throw e; // Propagate errors like division by zero
        } catch (Exception e) { /* ignore non-Memgres exceptions */ }
        return null;
    }

    // ---- Helper: WHERE CURRENT OF ----

    /**
     * Filter table rows to the single row positioned by the named cursor.
     * Matches by comparing column values from the cursor's current row against the table row values.
     */
    private List<Object[]> filterByCurrentOf(com.memgres.engine.parser.ast.CurrentOfExpr cof,
                                              Table table, List<Object[]> candidateRows) {
        Session.CursorState cursor = executor.session.getCursor(cof.cursorName());
        if (cursor == null) throw new MemgresException("cursor \"" + cof.cursorName() + "\" does not exist", "34000");
        int pos = cursor.getPosition();
        // A cursor that has not fetched yet, or has run past the end, is not on a row: PG says
        // so rather than quietly matching nothing
        if (pos < 0 || pos >= cursor.getRowCount()) {
            throw new MemgresException(
                    "cursor \"" + cof.cursorName() + "\" is not positioned on a row", "24000");
        }
        Object[] cursorRow = cursor.getRow(pos);
        // Map cursor columns to table column indices
        List<Column> cursorCols = cursor.getColumns();
        int[] tableColIdx = new int[cursorCols.size()];
        for (int i = 0; i < cursorCols.size(); i++) {
            tableColIdx[i] = table.getColumnIndex(cursorCols.get(i).getName());
        }
        // Find the table row matching all cursor column values
        List<Object[]> result = new ArrayList<>();
        for (Object[] row : candidateRows) {
            boolean match = true;
            for (int i = 0; i < cursorCols.size(); i++) {
                if (tableColIdx[i] < 0) continue; // cursor column not in table (e.g., computed)
                Object tableVal = row[tableColIdx[i]];
                Object cursorVal = cursorRow[i];
                if (!java.util.Objects.equals(tableVal, cursorVal)) {
                    match = false;
                    break;
                }
            }
            if (match) {
                result.add(row);
                break; // Only one row should match
            }
        }
        return result;
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
        return computeVirtualColumns(table, row, true);
    }

    Object[] computeVirtualColumns(Table table, Object[] row, boolean strict) {
        Object[] result = row.clone();
        for (int i = 0; i < table.getColumns().size(); i++) {
            Column col = table.getColumns().get(i);
            if (col.isVirtual()) {
                if (strict) {
                    // PG 18: virtual columns cannot use user-defined functions; reject at read time
                    checkVirtualColumnUdfAtRead(col);
                    result[i] = evalGeneratedColumn(table, result, col);
                } else {
                    try {
                        result[i] = evalGeneratedColumn(table, result, col);
                    } catch (Exception e) {
                        result[i] = null;
                    }
                }
            }
        }
        return result;
    }

    /**
     * Check if a virtual column's generation expression references a user-defined function.
     * PG 18 rejects UDFs in virtual columns; throw "does not exist" to match PG behavior.
     */
    private void checkVirtualColumnUdfAtRead(Column col) {
        String genExpr = col.getGeneratedExpr();
        if (genExpr == null) return;
        try {
            com.memgres.engine.parser.ast.Expression parsed =
                    com.memgres.engine.parser.Parser.parseExpression(genExpr);
            checkExprForUdf(parsed);
        } catch (MemgresException e) {
            throw e;
        } catch (Exception ignored) {}
    }

    private void checkExprForUdf(com.memgres.engine.parser.ast.Expression expr) {
        if (expr == null) return;
        if (expr instanceof com.memgres.engine.parser.ast.FunctionCallExpr) {
            com.memgres.engine.parser.ast.FunctionCallExpr fn = (com.memgres.engine.parser.ast.FunctionCallExpr) expr;
            PgFunction pgFunc = executor.database.getFunction(fn.name());
            if (pgFunc != null) {
                throw new MemgresException("function " + fn.name() + " does not exist in virtual column context", "0A000");
            }
            if (fn.args() != null) {
                for (com.memgres.engine.parser.ast.Expression arg : fn.args()) checkExprForUdf(arg);
            }
        } else if (expr instanceof com.memgres.engine.parser.ast.BinaryExpr) {
            com.memgres.engine.parser.ast.BinaryExpr bin = (com.memgres.engine.parser.ast.BinaryExpr) expr;
            checkExprForUdf(bin.left());
            checkExprForUdf(bin.right());
        } else if (expr instanceof com.memgres.engine.parser.ast.CastExpr) {
            checkExprForUdf(((com.memgres.engine.parser.ast.CastExpr) expr).expr());
        }
    }

    // ---- Read-only transaction check ----

    /**
     * Apply an INSTEAD rule for an UPDATE or DELETE. INSTEAD NOTHING reports the command as having
     * touched no rows; INSTEAD &lt;command&gt; runs that command in its place. Returns null when no
     * such rule applies and the statement should run normally.
     */
    private QueryResult applyInsteadRule(String tableName, String event, QueryResult.Type type,
                                         Expression where, List<InsertStmt.SetClause> setClauses) {
        String ruleVal = executor.database.getRule(tableName, event);
        if (ruleVal == null) return null;
        // A rule whose command lands back on its own relation would re-enter itself forever.
        if (executor.isRuleExpanding(tableName, event)) {
            throw PgErrors.infiniteRecursionInRules(tableName);
        }
        if ("INSTEAD_NOTHING".equals(ruleVal)) {
            return QueryResult.command(type, 0);
        }
        if (!ruleVal.startsWith("INSTEAD:")) return null;

        // The rule's command speaks of OLD and NEW, which only mean something against a row the
        // statement would have touched. PG rewrites the query to say the same thing; here the
        // rows are read back through the relation and the command runs once per row.
        String ruleSql = ruleVal.substring("INSTEAD:".length());
        // The statement the client sent never runs, so its row count is not the answer. PostgreSQL
        // lets the last action that is the same kind of command as the original speak for it, and
        // reports nothing at all when no action is: a DELETE replaced by an INSERT deleted nothing.
        int count = runRowRuleActions(tableName, event, ruleSql, where, setClauses, type, null);
        return QueryResult.command(type, count);
    }

    /**
     * Run the actions of a DO ALSO rule on an UPDATE or DELETE. PostgreSQL adds them to the
     * statement rather than in place of it, so they run against the rows as they stand before the
     * statement writes and the statement itself goes on to run and report its own row count.
     * Only the INSERT event ever reached its rules here, so an ON UPDATE or ON DELETE rule
     * declared DO ALSO never fired at all.
     */
    private void applyAlsoRule(String tableName, String event, Expression where,
                               List<InsertStmt.SetClause> setClauses) {
        applyAlsoRule(tableName, event, where, setClauses, null, null);
    }

    private void applyAlsoRule(String tableName, String event, Expression where,
                               List<InsertStmt.SetClause> setClauses, String alias,
                               List<SelectStmt.FromItem> extraFrom) {
        String ruleVal = executor.database.getRule(tableName, event);
        if (ruleVal == null || !ruleVal.startsWith("ALSO:")) return;
        if (executor.isRuleExpanding(tableName, event)) {
            throw PgErrors.infiniteRecursionInRules(tableName);
        }
        runRowRuleActions(tableName, event, ruleVal.substring("ALSO:".length()), where,
                setClauses, null, executor.database.getRuleQualification(tableName, event),
                alias, extraFrom);
    }

    /**
     * Run each action of a rule once for every row the statement acts on, with {@code OLD.col}
     * standing for the row as it is and {@code NEW.col} for the row the assignments would make.
     *
     * @param tagType    the command kind whose row count the caller reports, or null when the
     *                   caller reports its own
     * @param qualification the rule's WHERE, evaluated per row against OLD/NEW, or null
     * @return the row count of the last action of {@code tagType}, or 0
     */
    private int runRowRuleActions(String tableName, String event, String ruleSql, Expression where,
                                  List<InsertStmt.SetClause> setClauses, QueryResult.Type tagType,
                                  String qualification) {
        return runRowRuleActions(tableName, event, ruleSql, where, setClauses, tagType,
                qualification, null, null);
    }

    private int runRowRuleActions(String tableName, String event, String ruleSql, Expression where,
                                  List<InsertStmt.SetClause> setClauses, QueryResult.Type tagType,
                                  String qualification, String alias,
                                  List<SelectStmt.FromItem> extraFrom) {
        QueryResult affected = selectAffectedRows(tableName, alias, extraFrom, where, setClauses);
        // The assignments were projected after the relation's own columns, so the row carries the
        // old values first and what each assignment would make after them.
        int setCount = setClauses == null ? 0 : setClauses.size();
        List<Column> cols = affected.getColumns().subList(0, affected.getColumns().size() - setCount);
        Table rowShape = new Table(tableName, cols);
        String[] actions = Database.ruleActions(ruleSql);
        int[] actionCounts = new int[actions.length];
        boolean[] actionSetsTag = new boolean[actions.length];
        // An action that never says OLD or NEW is the same command for every row, and PostgreSQL
        // runs it once for the whole statement rather than once per row — but not at all when the
        // statement touched nothing. Measured: two rows updated writes one log row, none writes
        // none, while the same action reading NEW.b writes one per row.
        boolean[] perRow = new boolean[actions.length];
        for (int a = 0; a < actions.length; a++) {
            perRow[a] = mentionsRowAlias(actions[a]);
        }
        boolean[] ranOnce = new boolean[actions.length];
        executor.enterRuleExpansion(tableName, event);
        try {
            for (Object[] row : affected.getRows()) {
                RowContext rowCtx = new RowContext(rowShape, null, row);
                // Substitute once per row: the values do not change between the actions.
                String[] substituted = new String[actions.length];
                for (int a = 0; a < actions.length; a++) substituted[a] = actions[a];
                String qual = qualification;
                for (int i = 0; i < cols.size(); i++) {
                    String colName = cols.get(i).getName();
                    Object oldVal = row[i];
                    Object newVal = oldVal;
                    if (setClauses != null) {
                        for (int sc = 0; sc < setClauses.size(); sc++) {
                            if (setClauses.get(sc).column().equalsIgnoreCase(colName)) {
                                newVal = row[cols.size() + sc];
                                break;
                            }
                        }
                    }
                    for (int a = 0; a < actions.length; a++) {
                        substituted[a] = substituteRowAlias(substituted[a], "NEW", colName, newVal);
                        substituted[a] = substituteRowAlias(substituted[a], "OLD", colName, oldVal);
                    }
                    if (qual != null) {
                        qual = substituteRowAlias(qual, "NEW", colName, newVal);
                        qual = substituteRowAlias(qual, "OLD", colName, oldVal);
                    }
                }
                // A qualified rule fires only for the rows its WHERE holds for.
                if (qual != null && !ruleQualificationHolds(qual)) continue;
                for (int a = 0; a < actions.length; a++) {
                    if (!perRow[a] && ranOnce[a]) continue;
                    ranOnce[a] = true;
                    QueryResult actionResult = executor.execute(substituted[a], Cols.listOf());
                    if (tagType != null && actionResult != null && actionResult.getType() == tagType) {
                        actionSetsTag[a] = true;
                        actionCounts[a] += actionResult.getAffectedRows();
                    }
                }
            }
        } finally {
            executor.exitRuleExpansion(tableName, event);
        }
        for (int a = actions.length - 1; a >= 0; a--) {
            if (actionSetsTag[a]) return actionCounts[a];
        }
        return 0;
    }

    /** Whether a rule action says anything about the row it fires for. */
    private static boolean mentionsRowAlias(String sql) {
        return sql != null && java.util.regex.Pattern
                .compile("(?i)\\b(OLD|NEW)\\s*\\.").matcher(sql).find();
    }

    /** Evaluate a rule's qualification once OLD and NEW have been replaced by the row's values. */
    private boolean ruleQualificationHolds(String qualification) {
        try {
            QueryResult r = executor.execute("SELECT (" + qualification + ")", Cols.listOf());
            if (r == null || r.getRows().isEmpty()) return true;
            Object v = r.getRows().get(0)[0];
            return Boolean.TRUE.equals(v) || "t".equals(v) || "true".equals(String.valueOf(v));
        } catch (RuntimeException e) {
            // A qualification this engine cannot evaluate must not swallow the rule.
            return true;
        }
    }

    /**
     * Run each action of an INSERT rule once per inserted row, with {@code NEW.col} replaced by
     * the value the statement supplied for that column.
     */
    private int runInsertRuleActions(String storedBody, InsertStmt stmt, Table table,
                                      String ruleRelation) {
        if (stmt.values() == null) return 0;
        // A rule whose action writes back to the same relation would expand forever.
        executor.enterRuleExpansion(ruleRelation, "INSERT");
        try {
            return runInsertRuleActionRows(storedBody, stmt, table);
        } finally {
            executor.exitRuleExpansion(ruleRelation, "INSERT");
        }
    }

    /** Returns the row count of the last action that is itself an INSERT, or 0 when none is. */
    private int runInsertRuleActionRows(String storedBody, InsertStmt stmt, Table table) {
        String[] allActions = Database.ruleActions(storedBody);
        int[] actionCounts = new int[allActions.length];
        boolean[] actionSetsTag = new boolean[allActions.length];
        for (List<Expression> valueRow : stmt.values()) {
            // Values arrive in the order the statement names them, and through a view that is
            // the view's own column order mapped onto the base table.
            List<String> colNames = stmt.columns();
            if (colNames == null) colNames = activeViewColOrder;
            if (colNames == null) {
                colNames = new ArrayList<>();
                for (Column c : table.getColumns()) colNames.add(c.getName());
            }
            for (int a = 0; a < allActions.length; a++) {
                String sql = allActions[a];
                for (int ci = 0; ci < Math.min(colNames.size(), valueRow.size()); ci++) {
                    Object val = executor.evalExpr(valueRow.get(ci), null);
                    String colName = colNames.get(ci);
                    String replacement = val == null ? "NULL"
                            : val instanceof Number ? val.toString()
                            : "'" + val.toString().replace("'", "''") + "'";
                    sql = sql.replaceAll("(?i)NEW\\s*\\.\\s*" + colName, replacement);
                }
                // NEW carries every column of the row being inserted, so one the statement did
                // not supply is null there — not a name left standing in the rule's own SQL.
                for (Column c : table.getColumns()) {
                    sql = sql.replaceAll("(?i)NEW\\s*\\.\\s*" + c.getName(), "NULL");
                }
                QueryResult actionResult = executor.execute(sql, Cols.listOf());
                if (actionResult != null && actionResult.getType() == QueryResult.Type.INSERT) {
                    actionSetsTag[a] = true;
                    actionCounts[a] += actionResult.getAffectedRows();
                }
            }
        }
        for (int a = allActions.length - 1; a >= 0; a--) {
            if (actionSetsTag[a]) return actionCounts[a];
        }
        return 0;
    }

    /** The rows the statement would have acted on, read through the relation it names. */
    /**
     * The rows the statement acts on, which are the rows its rule fires for.
     *
     * <p>The WHERE is the statement's own, so it may name the relation by an alias and may name
     * the other relations an UPDATE ... FROM or a DELETE ... USING brought in. Rebuilding the
     * query from the table name alone left those unbound, and an ordinary write turned into
     * 42P01 "missing FROM-clause entry" as soon as the relation carried a rule.
     */
    private QueryResult selectAffectedRows(String tableName, String alias,
                                           List<SelectStmt.FromItem> extraFrom, Expression where,
                                           List<InsertStmt.SetClause> setClauses) {
        List<SelectStmt.SelectTarget> targets = new ArrayList<>();
        targets.add(new SelectStmt.SelectTarget(
                new WildcardExpr(null, null, alias != null ? alias : tableName), null));
        // Each assignment is worked out by the same query that finds the rows, so NEW.col is the
        // value the statement would write even when the expression reads one of the other
        // relations the statement brought in. Evaluating it per row against the target's columns
        // alone could not see those, and an UPDATE ... FROM under a rule failed with 42P01.
        if (setClauses != null) {
            for (InsertStmt.SetClause set : setClauses) {
                targets.add(new SelectStmt.SelectTarget(set.value(), null));
            }
        }
        List<SelectStmt.FromItem> from = new ArrayList<>();
        from.add(new SelectStmt.TableRef(null, tableName, alias));
        if (extraFrom != null) from.addAll(extraFrom);
        SelectStmt sel = new SelectStmt(false, null, targets, from, where,
                null, null, null, null, null, null, null, null, null, false);
        return executor.executeStatement(sel);
    }

    /** Replace one {@code OLD.col} or {@code NEW.col} reference with the value it stands for. */
    private String substituteRowAlias(String sql, String alias, String column, Object value) {
        String replacement = java.util.regex.Matcher.quoteReplacement(sqlLiteral(value));
        return sql.replaceAll("(?i)\\b" + alias + "\\s*\\.\\s*" + java.util.regex.Pattern.quote(column)
                + "\\b", replacement);
    }

    private String sqlLiteral(Object value) {
        if (value == null) return "NULL";
        if (value instanceof Number) return value.toString();
        if (value instanceof Boolean) return ((Boolean) value) ? "TRUE" : "FALSE";
        return "'" + value.toString().replace("'", "''") + "'";
    }

    private void checkReadOnly(String command) {
        if (executor.session != null && executor.session.isReadOnly()) {
            throw new MemgresException("cannot execute " + command + " in a read-only transaction", "25006");
        }
    }

    /**
     * PG enforces that tables covered by a publication must have a usable replica
     * identity before UPDATE or DELETE is allowed (error 55000).  A table is
     * "published" if any publication with FOR ALL TABLES exists, or if the table
     * is explicitly listed in a publication.
     */
    private void checkReplicaIdentity(Table table, String tableName, String dmlVerb) {
        // Views are never part of a publication's row-change stream (INSTEAD OF triggers do the
        // real work against base tables). PG applies FOR ALL TABLES only to real tables, so a
        // DML through a view must not be blocked by the publisher replica-identity requirement.
        if (table.isViewProjection()) return;
        if (executor.database.getPublications().isEmpty()) return;
        boolean published = false;
        for (Database.PubDef pub : executor.database.getPublications().values()) {
            if (pub.allTables) { published = true; break; }
            for (String t : pub.tables) {
                if (t.equalsIgnoreCase(tableName)) { published = true; break; }
            }
            if (published) break;
        }
        if (published && !table.hasUsableReplicaIdentity()) {
            String verb = "update".equals(dmlVerb) ? "updates" : "deletes";
            MemgresException ex = new MemgresException(
                    "cannot " + dmlVerb + " table \"" + tableName
                            + "\" because it does not have a replica identity and publishes " + verb,
                    "55000");
            ex.setHint("To enable updating the table, set REPLICA IDENTITY using ALTER TABLE.");
            throw ex;
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
        // M7: RR write-write conflict detection — if a row we're about to delete was
        // modified by a concurrent committed transaction after our snapshot, raise 40001.
        // Mirrors the check on the UPDATE path (recordUpdateUndo). Must run before the
        // undo is recorded (callers invoke this before the physical delete).
        if (executor.session != null) {
            for (Object[] row : rows) {
                executor.session.checkRRWriteConflict(schemaName + "." + table, row);
            }
        }
        executor.recordUndo(new Session.DeleteUndo(schemaName, table, rows));
        // Track for MVCC visibility
        if (executor.session != null) {
            executor.session.trackUncommittedDelete(schemaName + "." + table, rows);
        }
    }

    private void recordUpdateUndo(String schema, String table, Object[] row, Object[] oldValues) {
        String schemaName = schema != null ? schema : executor.defaultSchema();
        // M7: RR write-write conflict detection — if the row was modified by a committed
        // transaction after our snapshot, raise 40001
        if (executor.session != null) {
            executor.session.checkRRWriteConflict(schemaName + "." + table, oldValues);
        }
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
        return evalReturning(returning, table, alias, row, oldRow, newRow, null);
    }

    /**
     * Evaluate RETURNING expressions with OLD/NEW and MERGE source support.
     * @param sourceCtx source row context for MERGE or FROM/USING (null for simple DML)
     */
    private Object[] evalReturning(List<SelectStmt.SelectTarget> returning, Table table, String alias,
                                    Object[] row, Object[] oldRow, Object[] newRow,
                                    RowContext sourceCtx) {
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
        // Resolve renamed view column names in RETURNING against the base row.
        if (activeViewColMap != null) ctx.setColumnAliases(activeViewColMap);
        // Merge source context (FROM/USING/MERGE source tables) so RETURNING can reference them
        if (sourceCtx != null) {
            ctx = ctx.merge(sourceCtx);
        }
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
                    // For MERGE RETURNING *, also include source row columns
                    if (we.table() == null && sourceCtx != null) {
                        for (RowContext.TableBinding b : sourceCtx.getBindings()) {
                            for (Object val : b.row()) values.add(val);
                        }
                    }
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
        if (expr instanceof WildcardExpr) {
            WildcardExpr we = (WildcardExpr) expr;
            return we.table() != null && (we.table().equalsIgnoreCase("old") || we.table().equalsIgnoreCase("new"));
        }
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
        if (expr instanceof CustomOperatorExpr) {
            CustomOperatorExpr c = (CustomOperatorExpr) expr;
            return (c.left() != null && exprReferencesOldNew(c.left())) || exprReferencesOldNew(c.right());
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

    /**
     * The rules a MERGE's WHEN list has to satisfy before any row is read.
     *
     * <p>Each of the three kinds of WHEN clause — MATCHED, NOT MATCHED BY SOURCE and NOT MATCHED
     * BY TARGET — is tried in written order against the rows that reach it, so a clause written
     * after an unconditional clause of the same kind can never fire. The kinds are independent:
     * an unconditional MATCHED clause says nothing about a later NOT MATCHED clause.
     *
     * <p>The other two rules come from what each kind of clause can see. A NOT MATCHED BY SOURCE
     * clause fires for a target row that no source row paired with, so there is no source row to
     * read; a NOT MATCHED BY TARGET clause fires for a source row that paired with no target row,
     * so there is no target row to read. Naming the absent relation is a reference to a FROM entry
     * that exists but is out of reach here, which is a different complaint from naming a relation
     * that is not in the query at all.
     */
    private void checkMergeWhenClauses(MergeStmt stmt, String targetAlias) {
        // WITH RECURSIVE is refused outright, self-referencing or not.
        if (stmt.withClauses() != null) {
            for (SelectStmt.CommonTableExpr cte : stmt.withClauses()) {
                if (cte.recursive()) {
                    throw new MemgresException("WITH RECURSIVE is not supported for MERGE statement", "42601");
                }
            }
        }

        String sourceAlias = mergeSourceAlias(stmt.source());
        boolean terminalMatched = false;
        boolean terminalNotMatchedBySource = false;
        boolean terminalNotMatchedByTarget = false;

        for (MergeStmt.WhenClause clause : stmt.whenClauses()) {
            if (clause instanceof MergeStmt.WhenMatched) {
                MergeStmt.WhenMatched wm = (MergeStmt.WhenMatched) clause;
                if (terminalMatched) throw unreachableWhenClause();
                if (wm.andCondition() == null) terminalMatched = true;
            } else if (clause instanceof MergeStmt.WhenNotMatchedBySource) {
                MergeStmt.WhenNotMatchedBySource ws = (MergeStmt.WhenNotMatchedBySource) clause;
                if (terminalNotMatchedBySource) throw unreachableWhenClause();
                if (ws.andCondition() == null) terminalNotMatchedBySource = true;
                if (sourceAlias != null) {
                    rejectOutOfReachAlias(ws.andCondition(), sourceAlias);
                    if (ws.setClauses() != null) {
                        for (InsertStmt.SetClause set : ws.setClauses()) {
                            rejectOutOfReachAlias(set.value(), sourceAlias);
                        }
                    }
                }
            } else if (clause instanceof MergeStmt.WhenNotMatched) {
                MergeStmt.WhenNotMatched wn = (MergeStmt.WhenNotMatched) clause;
                if (terminalNotMatchedByTarget) throw unreachableWhenClause();
                if (wn.andCondition() == null) terminalNotMatchedByTarget = true;
                if (targetAlias != null) {
                    rejectOutOfReachAlias(wn.andCondition(), targetAlias);
                    if (wn.values() != null) {
                        for (Expression value : wn.values()) rejectOutOfReachAlias(value, targetAlias);
                    }
                }
                rejectDuplicateInsertColumns(wn.columns());
            }
        }
    }

    /** The name a MERGE's source is known by inside the WHEN clauses, or null when it has none. */
    private static String mergeSourceAlias(SelectStmt.FromItem source) {
        if (source instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef ref = (SelectStmt.TableRef) source;
            return ref.alias() != null ? ref.alias() : ref.table;
        }
        if (source instanceof SelectStmt.SubqueryFrom) return ((SelectStmt.SubqueryFrom) source).alias();
        if (source instanceof SelectStmt.FunctionFrom) return ((SelectStmt.FunctionFrom) source).alias();
        return null;
    }

    /**
     * Refuse a qualified reference to a relation that this part of the MERGE cannot see. An
     * expression holding a subquery is left alone: the subquery brings its own FROM list, so the
     * same name inside it may well be a relation of its own.
     */
    private void rejectOutOfReachAlias(Expression expr, String alias) {
        if (expr == null) return;
        if (AstWalk.anyMatch(expr, n -> n instanceof SelectStmt || n instanceof SetOpStmt)) return;
        Object hit = AstWalk.findFirst(expr, n -> n instanceof ColumnRef
                && ((ColumnRef) n).table() != null
                && ((ColumnRef) n).table().equalsIgnoreCase(alias));
        if (hit != null) {
            throw new MemgresException("invalid reference to FROM-clause entry for table \"" + alias + "\""
                    + "\n  Detail: There is an entry for table \"" + alias
                    + "\", but it cannot be referenced from this part of the query.", "42P01");
        }
    }

    /** A target column may be written once per INSERT, whether the INSERT stands alone or is a MERGE arm. */
    static void rejectDuplicateInsertColumns(List<String> columns) {
        if (columns == null || columns.size() < 2) return;
        Set<String> seen = new HashSet<>();
        for (String col : columns) {
            if (col == null) continue;
            if (!seen.add(col.toLowerCase())) {
                throw new MemgresException("column \"" + col + "\" specified more than once", "42701");
            }
        }
    }

    /**
     * Every clause of a MERGE that is read one row at a time: the condition that pairs a source
     * row with a target row, the condition that chooses which action fires, and the action itself.
     * PostgreSQL names the ON clause a JOIN condition, the WHEN condition its own thing, and an
     * action by the command it is — the same names it uses for a plain join, UPDATE and INSERT.
     */
    private void checkMergePlacement(MergeStmt stmt, Table targetTable, String targetAlias) {
        PlacementCheck placement = executor.selectExecutor.placementCheck;
        placement.reject(stmt.onCondition(), "JOIN conditions");
        executor.selectExecutor.rejectSrfIn(stmt.onCondition(), "JOIN conditions");
        // The ON condition is deliberately not coerced to boolean: PostgreSQL transforms a MERGE's
        // join condition without coercing it, so MERGE ... ON (1) is accepted where the same
        // condition written in a JOIN is not. A WHEN condition is coerced, and both relations are
        // in scope for it.
        BooleanContext.Types types = mergeScope(stmt, targetTable, targetAlias);
        for (MergeStmt.WhenClause clause : stmt.whenClauses()) {
            Expression when = null;
            List<InsertStmt.SetClause> sets = null;
            List<Expression> values = null;
            if (clause instanceof MergeStmt.WhenMatched) {
                MergeStmt.WhenMatched wm = (MergeStmt.WhenMatched) clause;
                when = wm.andCondition();
                sets = wm.setClauses();
            } else if (clause instanceof MergeStmt.WhenNotMatchedBySource) {
                MergeStmt.WhenNotMatchedBySource ws = (MergeStmt.WhenNotMatchedBySource) clause;
                when = ws.andCondition();
                sets = ws.setClauses();
            } else if (clause instanceof MergeStmt.WhenNotMatched) {
                MergeStmt.WhenNotMatched wn = (MergeStmt.WhenNotMatched) clause;
                when = wn.andCondition();
                values = wn.values();
            }
            placement.reject(when, "MERGE WHEN conditions");
            executor.selectExecutor.rejectSrfIn(when, "MERGE WHEN conditions");
            BooleanContext.check(when, "WHEN", types);
            if (sets != null) {
                for (InsertStmt.SetClause set : sets) {
                    placement.reject(set.value(), "UPDATE");
                    executor.selectExecutor.rejectSrfIn(set.value(), "UPDATE");
                }
            }
            if (values != null) {
                for (Expression value : values) {
                    placement.reject(value, "VALUES");
                    // A MERGE's INSERT is not a VALUES list of its own: it writes one row for the
                    // source row that reached it, so a set has no rows to expand into and
                    // PostgreSQL says what the value is rather than which clause holds it. The
                    // one-row VALUES of a plain INSERT, which does expand, is a different path.
                    rejectSetValuedCall(value);
                }
            }
        }
    }

    /**
     * The refusal PostgreSQL raises where an expression is evaluated for one row and answers a
     * set. It names the kind of value rather than the clause, because every clause that says this
     * is one that had a row already.
     */
    private void rejectSetValuedCall(Expression expr) {
        List<FunctionCallExpr> found = executor.selectExecutor.collectSrfCalls(expr);
        if (found.isEmpty()) return;
        MemgresException e = new MemgresException(
                "set-valued function called in context that cannot accept a set", "0A000");
        e.setPositionToken(found.get(0).name());
        throw e;
    }

    /**
     * An UPDATE's assignments and WHERE, both of which PostgreSQL reads per row.
     *
     * <p>The two clauses carry different names in the message: an aggregate in the SET list is
     * reported as being "in UPDATE", the one in WHERE as being "in WHERE".
     */
    private void checkUpdatePlacement(UpdateStmt stmt, Table table) {
        PlacementCheck placement = executor.selectExecutor.placementCheck;
        String targetName = stmt.alias() != null ? stmt.alias() : stmt.table();
        for (InsertStmt.SetClause set : stmt.setClauses()) {
            placement.reject(set.value(), "UPDATE");
            // An assignment writes one value into one row's column. A set has no single value to
            // write and no rows of its own to multiply an UPDATE by, so PostgreSQL refuses it
            // whether or not the WHERE finds a row to try it on.
            executor.selectExecutor.rejectSrfIn(set.value(), "UPDATE");
            placement.rejectOuterLevelAggregate(set.value(), "UPDATE", table, targetName);
        }
        placement.reject(stmt.where(), "WHERE");
        executor.selectExecutor.rejectSrfIn(stmt.where(), "WHERE");
        placement.rejectOuterLevelAggregate(stmt.where(), "WHERE", table, targetName);
        BooleanContext.Types types = dmlScope(table, targetName, stmt.from());
        for (InsertStmt.SetClause set : stmt.setClauses()) BooleanContext.scan(set.value(), types);
        BooleanContext.check(stmt.where(), "WHERE", types);
    }

    /**
     * What an UPDATE or a DELETE has in scope: its target, and the relations its FROM or USING
     * clause adds. A clause item this cannot look up leaves the target's columns untyped too,
     * because then there is no telling which relation a bare name belongs to.
     */
    private BooleanContext.Types dmlScope(Table table, String alias,
                                          List<SelectStmt.FromItem> extra) {
        if (table == null) return BooleanContext.Types.none();
        if (extra == null || extra.isEmpty()) return BooleanContext.Types.of(table);
        List<RowContext.TableBinding> bindings = new ArrayList<>();
        bindings.add(new RowContext.TableBinding(table, alias, null));
        for (SelectStmt.FromItem item : extra) {
            if (!(item instanceof SelectStmt.TableRef)) return BooleanContext.Types.none();
            SelectStmt.TableRef ref = (SelectStmt.TableRef) item;
            Table joined = executor.selectExecutor.lookupRelationOrNull(ref.schema(), ref.table());
            if (joined == null) return BooleanContext.Types.none();
            bindings.add(new RowContext.TableBinding(joined,
                    ref.alias() != null ? ref.alias() : ref.table(), null));
        }
        return BooleanContext.Types.of(executor, bindings);
    }

    /**
     * What the two relations of a MERGE supply, for typing the conditions written against them.
     * Both are described rather than read; a source this cannot look up leaves the target's
     * columns untyped as well, because then there is no telling which of the two a bare name is.
     */
    private BooleanContext.Types mergeScope(MergeStmt stmt, Table targetTable, String targetAlias) {
        if (!(stmt.source() instanceof SelectStmt.TableRef)) return BooleanContext.Types.none();
        SelectStmt.TableRef src = (SelectStmt.TableRef) stmt.source();
        Table source = executor.selectExecutor.lookupRelationOrNull(src.schema(), src.table());
        if (source == null || targetTable == null) return BooleanContext.Types.none();
        List<RowContext.TableBinding> bindings = new ArrayList<>();
        bindings.add(new RowContext.TableBinding(targetTable, targetAlias, null));
        bindings.add(new RowContext.TableBinding(source,
                src.alias() != null ? src.alias() : src.table(), null));
        return BooleanContext.Types.of(executor, bindings);
    }

    /**
     * The rows a VALUES list actually writes, once its set-returning calls have been expanded.
     *
     * <p>{@code INSERT INTO t VALUES (generate_series(5,6))} writes two rows in PostgreSQL, not
     * one holding an array: a VALUES list is a query, and a set in one of its columns multiplies
     * the row exactly as it does in a select list. The calls of one row run side by side, so a row
     * written with two of them is as long as the longer and the shorter reads NULL past its end.
     *
     * <p>Returns null when nothing in the list produces a set, which leaves every row evaluated
     * with the null context it was evaluated with before. Otherwise {@code expandedOut} is filled
     * with one entry per output row — the same expression list each time — and the returned list
     * holds the context that binds each call to its element for that row.
     */
    private List<RowContext> expandSrfValueRows(List<List<Expression>> valueRows,
                                                List<List<Expression>> expandedOut) {
        boolean anySrf = false;
        for (List<Expression> valueRow : valueRows) {
            for (Expression value : valueRow) {
                if (!executor.selectExecutor.collectSrfCalls(value).isEmpty()) { anySrf = true; break; }
            }
            if (anySrf) break;
        }
        if (!anySrf) return null;
        List<RowContext> contexts = new ArrayList<>();
        for (List<Expression> valueRow : valueRows) {
            List<RowContext> one = new ArrayList<>();
            one.add(new RowContext(Cols.<RowContext.TableBinding>listOf()));
            for (RowContext ctx : executor.selectExecutor.expandContextsForSrfs(valueRow, one)) {
                expandedOut.add(valueRow);
                contexts.add(ctx);
            }
        }
        return contexts;
    }

    /**
     * The columns every relation has without declaring them. They are not in the column list, so
     * a check for "is this a column of the relation" has to let them past: PostgreSQL has its own
     * complaint about assigning to one, and reads them like any other name.
     */
    private static final Set<String> SYSTEM_COLUMNS = Cols.setOf(
            "ctid", "xmin", "xmax", "cmin", "cmax", "tableoid", "oid");

    /**
     * Every column a statement writes to has to be a column of the relation it names. PostgreSQL
     * resolves the target list while it is still analysing the statement, so it says the same
     * thing whether the relation holds rows or not — and it names the relation the statement
     * wrote, which is the view when the write goes through one.
     */
    private void requireTargetColumns(Table table, String relationName, List<String> columnNames) {
        if (table == null || columnNames == null) return;
        for (String name : columnNames) {
            if (name == null || SYSTEM_COLUMNS.contains(name.toLowerCase())) continue;
            if (table.getColumnIndex(mapViewColumn(name)) < 0) {
                throw new MemgresException("column \"" + name + "\" of relation \""
                        + relationName + "\" does not exist", "42703");
            }
        }
    }

    /**
     * The names an UPDATE reads — in its WHERE and on the right of each assignment — are resolved
     * against the one relation it names, so a name that is not a column of it is refused before
     * the scan starts rather than by the first row to reach the evaluator. Only the shapes where
     * the target relation is demonstrably the whole scope are checked: a FROM clause, a subquery
     * or a set-returning call brings other columns into scope, and those are left to run.
     */
    private void requireReadableColumns(Table table, Expression expr, String alias, String relationName) {
        if (table == null || expr == null) return;
        if (AstWalk.anyMatch(expr, n -> n instanceof SubqueryExpr || n instanceof ExistsExpr
                || n instanceof ArraySubqueryExpr || n instanceof SelectStmt)) {
            return;
        }
        final String self = alias != null ? alias : relationName;
        AstWalk.forEach(expr, node -> {
            if (!(node instanceof ColumnRef)) return;
            ColumnRef cr = (ColumnRef) node;
            String qualifier = cr.table();
            if (qualifier != null && !qualifier.equalsIgnoreCase(self)
                    && !qualifier.equalsIgnoreCase(relationName)) {
                return;   // another relation's name: not this scope's to judge
            }
            if (SYSTEM_COLUMNS.contains(cr.column().toLowerCase())) return;
            if (table.getColumnIndex(mapViewColumn(cr.column())) < 0) {
                // PostgreSQL quotes a bare name and leaves a qualified one as written.
                throw new MemgresException("column " + (qualifier == null
                        ? "\"" + cr.column() + "\"" : qualifier + "." + cr.column())
                        + " does not exist", "42703");
            }
        });
    }

    /** Validate that all column references in RETURNING exist in the table. */
    void validateReturning(List<SelectStmt.SelectTarget> returning, Table table) {
        if (returning == null) return;
        for (SelectStmt.SelectTarget target : returning) {
            // RETURNING reports the rows the statement touched one at a time; there is no
            // group and no window frame for an aggregate or a window function to run over, and
            // no room for a set to expand into either -- RETURNING answers one row per row
            // written, so a call producing two values has nowhere to put the second.
            executor.selectExecutor.placementCheck.reject(target.expr(), "RETURNING");
            executor.selectExecutor.rejectSrfIn(target.expr(), "RETURNING");
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
        return buildReturningColumns(returning, table, null);
    }

    /** Build Column metadata for RETURNING clause, with optional source table for MERGE. */
    private List<Column> buildReturningColumns(List<SelectStmt.SelectTarget> returning, Table table, Table sourceTable) {
        List<Column> cols = new ArrayList<>();
        for (SelectStmt.SelectTarget target : returning) {
            if (target.expr() instanceof WildcardExpr) {
                WildcardExpr we = (WildcardExpr) target.expr();
                if (we.table() == null) {
                    // Bare * — for MERGE, include target + source columns
                    cols.addAll(table.getColumns());
                    if (sourceTable != null) {
                        cols.addAll(sourceTable.getColumns());
                    }
                } else {
                    cols.addAll(table.getColumns());
                }
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
