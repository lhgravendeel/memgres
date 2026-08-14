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
    // The view's own column names by position, including the computed ones. A positional write
    // names nothing, so this is the only list that can say which view column a value was aimed at.
    private List<String> activeViewColNames;
    // The view the statement named, or null when it named a relation that stores its own rows.
    // What DEFAULT stands for is settled against the relation the statement wrote, and by the time
    // an assignment is worked out that name has been replaced by the relation underneath it.
    private Database.ViewDef activeViewWriteTarget;

    /**
     * Take the view the statement's target resolved through, before resolving anything else can
     * change it. They are taken together: one left standing from an earlier statement describes a
     * view this one is not writing through. The view itself is not among them — it is found by the
     * name the statement wrote, which only the caller holds — so it is cleared here and set again
     * by each write that has a name to look it up by.
     */
    private void captureViewTarget() {
        this.activeViewWriteTarget = null;
        this.activeViewColMap = executor.lastViewColumnMapping;
        this.activeViewColOrder = executor.lastViewColumnOrder;
        this.activeViewExprCols = executor.lastViewExpressionColumns;
        this.activeViewColNames = executor.lastViewColumnNames;
    }

    /**
     * The columns the relation the statement named exposes, in order, or null when it named a
     * table. PostgreSQL rewrites a write on an auto-updatable view onto the base relation but
     * resolves the statement's own lists against the relation as written, so a view's names are
     * the names RETURNING may use and a base column the view leaves out is not a column at all.
     * A view whose definition selects everything is answered as null: its columns are the base
     * relation's own, which is what the base relation already says.
     */
    private List<String> targetViewColumns() {
        List<String> names = activeViewColNames;
        if (names == null || names.isEmpty()) return null;
        for (String name : names) {
            if (name == null) return null;
        }
        return names;
    }

    /** Whether RETURNING may name this column: the view's list decides it when there is one. */
    private boolean returningColumnExists(String column, Table table) {
        List<String> viewCols = targetViewColumns();
        if (viewCols == null) return table.getColumnIndex(column) >= 0;
        for (String viewCol : viewCols) {
            if (viewCol.equalsIgnoreCase(column)) return true;
        }
        return false;
    }

    /**
     * Which of the base relation's columns stand behind the view's, one per view column, so that
     * RETURNING * answers with the view's row rather than the row it was rewritten onto. Null
     * unless every view column is a column of the base relation under a name of its own.
     */
    private int[] viewProjection(Table table) {
        List<String> names = targetViewColumns();
        List<String> order = activeViewColOrder;
        if (names == null || order == null || names.size() != order.size()) return null;
        int[] indexes = new int[order.size()];
        for (int i = 0; i < order.size(); i++) {
            indexes[i] = table.getColumnIndex(order.get(i));
            if (indexes[i] < 0) return null;
        }
        return indexes;
    }

    /** Translate a view column name to the base table column name using the active mapping. */
    private String mapViewColumn(String colName) {
        Map<String, String> mapping = activeViewColMap;
        if (mapping == null || colName == null) return colName;
        String mapped = mapping.get(colName.toLowerCase());
        return mapped != null ? mapped : colName;
    }

    /**
     * The default the view this write is going through gives a base relation column, or null.
     *
     * <p>PostgreSQL keeps a view's column defaults on the view itself and substitutes them while
     * it rewrites the write onto the base relation, so a write through the view takes them and one
     * naming the relation directly does not. They are looked up here, on the name the statement
     * wrote, rather than being copied onto the base relation, which would give that relation a
     * default nobody declared on it.
     */
    private String viewColumnDefault(String schemaName, String written, String baseColumn) {
        if (written == null || baseColumn == null) return null;
        Database.ViewDef view = executor.database.getView(schemaName, written);
        if (view == null || view.cachedColumns() == null) return null;
        for (Column viewCol : view.cachedColumns()) {
            if (viewCol.getDefaultValue() == null) continue;
            String base = mapViewColumn(viewCol.getName());
            if (base != null && base.equalsIgnoreCase(baseColumn)) return viewCol.getDefaultValue();
        }
        return null;
    }

    /** Build a single-table RowContext that also resolves renamed view column names (if any). */
    private RowContext viewAwareCtx(Table table, String alias, Object[] row) {
        RowContext ctx = new RowContext(table, alias, row);
        if (activeViewColMap != null) ctx.setColumnAliases(activeViewColMap);
        return ctx;
    }

    /**
     * The same context for a row of the relation that is stored somewhere else.
     *
     * <p>A statement written against a partitioned table or an inheritance parent acts on rows its
     * partitions and its children hold. Which relation such a row belongs to and where in it it
     * lives are properties of that relation, so a qualification reading tableoid or ctid is
     * answered from there -- as PostgreSQL answers it, naming the child and the child's tuple.
     */
    private RowContext viewAwareCtx(Table table, String alias, Object[] row, Table storage) {
        if (storage == null || storage == table) return viewAwareCtx(table, alias, row);
        RowContext ctx = new RowContext(Cols.listOf(
                new RowContext.TableBinding(table, alias, row, storage)));
        if (activeViewColMap != null) ctx.setColumnAliases(activeViewColMap);
        return ctx;
    }

    /**
     * The triggers of a list this statement may fire. PostgreSQL decides it one trigger at a time
     * rather than once for the statement: tgenabled 'O' fires only while session_replication_role
     * is origin or local, 'R' only while it is replica, 'A' always and 'D' never. Reading the
     * setting for the statement suppressed every trigger in replica mode, which left an ENABLE
     * REPLICA trigger with no mode at all in which it ran.
     */
    private List<PgTrigger> enabledTriggers(List<PgTrigger> all) {
        if (all == null || all.isEmpty()) return Cols.listOf();
        boolean replicaRole = DmlTriggerHelper.inReplicaRole(executor);
        List<PgTrigger> kept = new ArrayList<PgTrigger>();
        for (PgTrigger candidate : all) {
            if (candidate.firesUnderReplicationRole(replicaRole)) kept.add(candidate);
        }
        return kept;
    }

    /** Record row metadata (xmin, cmin) for system column support. */
    void recordRowMeta(String schema, Table table, Object[] row) {
        if (executor.session != null && executor.database != null) {
            String tableKey = resolveTableSchemaKey(schema, table);
            long xmin = executor.session.getTransactionId();
            long cmin = executor.session.getCommandId();
            executor.database.setRowInsertMeta(tableKey, table, row, xmin, cmin);
        }
    }

    /** Record row metadata update (new ctid after UPDATE). */
    void recordRowUpdateMeta(String schema, Table table, Object[] row) {
        if (executor.session != null && executor.database != null) {
            String tableKey = resolveTableSchemaKey(schema, table);
            long xmin = executor.session.getTransactionId();
            long cmin = executor.session.getCommandId();
            executor.database.setRowUpdateMeta(tableKey, table, row, xmin, cmin);
        }
        // The row now holds what this statement wrote, so a relation reading it through columns of
        // its own has to be shown the same values.
        if (executor.session != null) executor.session.rowWasUpdatedInPlace(row);
    }

    /** Resolve the schema-qualified key for a table's row metadata.
     *  When schema is null, find the actual schema by scanning database schemas. */
    /**
     * A catalogue view cannot be written to.
     *
     * <p>Nothing looked for one, so a statement naming pg_cursors or pg_settings was told the
     * relation did not exist — which is not what it is. PostgreSQL says the view is not one it
     * can write through, and names the verb that was tried.
     */
    private void rejectCatalogViewWrite(String tableName, String verb) {
        if (tableName == null) return;
        String bare = tableName.contains(".")
                ? tableName.substring(tableName.lastIndexOf('.') + 1) : tableName;
        if (!bare.toLowerCase().startsWith("pg_")) return;
        if (executor.systemCatalog.resolve(null, bare, executor.session) == null) return;
        if (!"v".equals(PgCatalogRelations.relkind(bare.toLowerCase()))) return;
        // A catalogue view is assembled from more than one relation, which is the same reason
        // PostgreSQL gives for refusing a write to any other view of that shape, and it names the
        // trigger and the rule that would take the write instead.
        throw ViewUpdatability.cannotWrite(verb, bare,
                ViewUpdatability.DETAIL_NOT_SINGLE_RELATION, executor.viewDmlByMerge);
    }

    /**
     * Note the lock this statement takes on the relation it writes to, so pg_locks can report it.
     */
    private void recordRelationLock(String schema, String tableName, String mode) {
        if (executor.session == null || tableName == null) return;
        String bare = tableName;
        String schemaName = schema;
        if (schemaName == null && bare.contains(".")) {
            schemaName = bare.substring(0, bare.indexOf('.'));
            bare = bare.substring(bare.indexOf('.') + 1);
        }
        if (schemaName == null) schemaName = executor.defaultSchema();
        executor.session.recordRelationLock(schemaName.toLowerCase() + "." + bare.toLowerCase(), mode);
    }

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
                    // The bound the row has to pass is the one of the relation it is stored in,
                    // and it carries every bound above that relation with it. Testing the relation
                    // the statement named instead left the bounds between it and the partition the
                    // row was routed to untested, and named a relation the row was only passing
                    // through when the row failed one of the bounds they share.
                    partitionHelper.checkPartitionConstraint(targetTable, storedRow);
                    try {
                        if (targetTable != checkTable) {
                            // The row is stored in the partition and the partition carries a copy
                            // of every constraint it inherited, so PostgreSQL names the partition
                            // rather than the partitioned table the statement wrote to. Checking
                            // the leaf first is what makes its copy the one that raises; the
                            // parent is checked after it, for anything the leaf has no copy of.
                            executor.constraintValidator.validateConstraints(targetTable, storedRow, null);
                        }
                        executor.constraintValidator.validateConstraints(checkTable, row, null);
                    } catch (MemgresException refused) {
                        spendTupleIdOnFailedWrite(targetTable, refused);
                        throw refused;
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

    /**
     * The line pointer a statement that failed after writing its row has already spent.
     *
     * <p>PostgreSQL writes the row into the relation before it maintains that relation's indexes
     * and before it asks whether a referenced row exists, so a duplicate key, a conflicting
     * exclusion key or a missing parent is found with the tuple already in place. The line
     * pointer it took is not handed back: the next row written into that relation gets the one
     * after it. Everything settled before the write -- a NOT NULL, a CHECK, a partition bound, a
     * value that will not convert -- costs the relation nothing.
     */
    private void spendTupleIdOnFailedWrite(Table storage, MemgresException failure) {
        if (storage == null) return;
        String state = failure.getSqlState();
        if ("23505".equals(state) || "23503".equals(state) || "23P01".equals(state)) {
            storage.nextTupleId();
        }
    }

    /**
     * Write the deleting transaction's id into the xmax of every row this statement takes.
     *
     * <p>A row read through a partitioned table or an inheritance parent belongs to the relation
     * that stores it, and that is where its system columns are kept, so the mark goes on under
     * that relation's name rather than under the one the statement wrote.
     */
    private void markRowsDeletedBy(Table table, Map<Object[], Table> rowOwner,
                                   Collection<Object[]> rows) {
        if (executor.session == null || executor.database == null || rows.isEmpty()) return;
        long xmax = executor.session.getTransactionId();
        Map<Table, String> keys = new IdentityHashMap<Table, String>();
        for (Object[] row : rows) {
            Table storage = rowOwner == null ? null : rowOwner.get(row);
            if (storage == null) storage = table;
            String key = keys.get(storage);
            if (key == null) {
                key = resolveTableSchemaKey(null, storage);
                keys.put(storage, key);
            }
            executor.database.setRowDeleteMeta(key, row, xmax);
        }
    }

    private void awaitPendingInsert(Table table, ConstraintValidator.PendingUniqueConflict pending) {
        final String key = executor.constraintValidator.uncommittedKey(table);
        executor.database.awaitConcurrentWrite(executor.session, pending.owner,
                () -> ConstraintValidator.isStillPending(pending, key), pending.relation);
    }

    /**
     * The row already in the relation and the row being written both stand in the scope of an
     * ON CONFLICT DO UPDATE, and EXCLUDED holds every column the relation holds, so a column of
     * it written without a relation name answers to both. PostgreSQL refuses to choose between
     * them rather than pick one, and it does so while planning: before it has looked for an index
     * to arbitrate on, and whether or not any row conflicts.
     *
     * <p>The assignments are read before the WHERE beside them, which is the order PostgreSQL
     * reports the two in. The column a SET writes to is the relation's own and is not ambiguous;
     * only what stands on the right of it is. A sub-select brings relations of its own, so a name
     * it reads is judged against those and is left alone here.
     */
    private static void rejectAmbiguousBesideExcluded(InsertStmt.OnConflict oc, Table table) {
        if (oc.doUpdate() == null) return;
        for (InsertStmt.SetClause set : oc.doUpdate()) {
            rejectAmbiguousColumn(set.value(), table);
        }
        rejectAmbiguousColumn(oc.doUpdateWhereClause(), table);
    }

    private static void rejectAmbiguousColumn(Object node, Table table) {
        if (node == null || node instanceof Statement) return;
        if (node instanceof ColumnRef) {
            ColumnRef ref = (ColumnRef) node;
            if (ref.table() == null && ref.column() != null
                    && table.getColumnIndex(ref.column()) >= 0) {
                MemgresException e = new MemgresException(
                        "column reference \"" + ref.column() + "\" is ambiguous", "42702");
                e.setPositionToken(ref.column());
                throw e;
            }
            return;
        }
        AstWalk.forEachChild(node, child -> rejectAmbiguousColumn(child, table));
    }

    /**
     * The names a second relation brings into the scope a RETURNING clause is read in.
     *
     * <p>A relation named outright is looked up and asked for its columns, which is what settles
     * the question before a row has been read -- an empty relation supplies its names just as a
     * full one does. Anything else is described by the scan that resolved it, which knows what it
     * exposes as soon as it has produced a row. Where neither can answer, nothing is known and the
     * statement is left alone, for the reason mergeScope gives: with no telling what the second
     * relation supplies there is no telling which of the two a bare name is.
     */
    private Set<String> namesBesideTarget(List<SelectStmt.FromItem> extra, RowContext resolved) {
        if (extra == null || extra.isEmpty()) return null;
        Set<String> names = new HashSet<>();
        for (SelectStmt.FromItem item : extra) {
            if (!(item instanceof SelectStmt.TableRef)) {
                names = null;
                break;
            }
            SelectStmt.TableRef ref = (SelectStmt.TableRef) item;
            Table joined = executor.selectExecutor.lookupRelationOrNull(ref.schema(), ref.table());
            if (joined == null) {
                names = null;
                break;
            }
            List<String> renamed = ref.columnAliases();
            for (int i = 0; i < joined.getColumns().size(); i++) {
                String name = renamed != null && i < renamed.size()
                        ? renamed.get(i) : joined.getColumns().get(i).getName();
                names.add(name.toLowerCase());
            }
        }
        if (names == null && resolved != null) {
            names = new HashSet<>();
            for (RowContext.OutCol out : resolved.outputColumnsOrDefault()) {
                names.add(out.name.toLowerCase());
            }
        }
        return names;
    }

    /**
     * A RETURNING clause of a write that brings in a second relation stands in the scope of both,
     * so a column name they both hold answers to neither. PostgreSQL refuses to choose between
     * them rather than pick one, and it does so while analysing the statement: whether or not any
     * row is written, and whichever arm of a MERGE would have written it.
     */
    private void rejectAmbiguousReturning(List<SelectStmt.SelectTarget> returning, Table table,
                                          Set<String> beside) {
        if (returning == null || table == null || beside == null || beside.isEmpty()) return;
        for (SelectStmt.SelectTarget target : returning) {
            rejectAmbiguousBeside(target.expr(), table, beside);
        }
    }

    private static void rejectAmbiguousBeside(Object node, Table table, Set<String> beside) {
        // A sub-select brings relations of its own, so a name it reads is judged against those and
        // is left alone here.
        if (node == null || node instanceof Statement) return;
        if (node instanceof ColumnRef) {
            ColumnRef ref = (ColumnRef) node;
            if (ref.table() == null && ref.column() != null
                    && beside.contains(ref.column().toLowerCase())
                    && table.getColumnIndex(ref.column()) >= 0) {
                MemgresException e = new MemgresException(
                        "column reference \"" + ref.column() + "\" is ambiguous", "42702");
                e.setPositionToken(ref.column());
                throw e;
            }
            return;
        }
        AstWalk.forEachChild(node, child -> rejectAmbiguousBeside(child, table, beside));
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
                    // An arbiter names a column of an index rather than a column being written,
                    // so PostgreSQL reports it as a name nothing answers to rather than as a
                    // column the relation has not got -- which is what a SET list is told.
                    throw new MemgresException(
                            "column \"" + col + "\" does not exist", "42703");
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
        // An index predicate is judged against one row with no query around it, so there is nowhere
        // for a nested query to run; and the names it does hold are resolved against the relation
        // being written while the statement is planned, whether or not any index is ever reached.
        placement.rejectSubquery(oc.whereClause(), "index predicate");
        ArbiterPredicate.checkNames(executor.ddlExecutor, oc.whereClause(), table, stmt.table(),
                stmt.alias());
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
        StoredConstraint named = null;
        if (oc.constraint() != null) {
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
        }
        // What the action writes is read before the index it would arbitrate on, so a column the
        // action cannot settle is reported whether or not anything could have arbitrated.
        rejectAmbiguousBesideExcluded(oc, table);
        // Arbitration needs a unique index to decide which row was hit; a CHECK or foreign
        // key has none, so there is nothing to conflict against.
        if (named != null && named.getType() != StoredConstraint.Type.PRIMARY_KEY
                && named.getType() != StoredConstraint.Type.UNIQUE) {
            throw new MemgresException(
                    "constraint in ON CONFLICT clause has no associated index", "42809");
        }
        if (oc.doUpdate() != null) {
            for (InsertStmt.SetClause set : oc.doUpdate()) {
                int colIdx = table.getColumnIndex(set.column());
                if (colIdx < 0) {
                    throw new MemgresException("column \"" + set.column() + "\" of relation \""
                            + table.getName() + "\" does not exist", "42703");
                }
                // What a generated column holds is the relation's to compute, so DEFAULT is the
                // only thing that may be written to one. This belongs here rather than in the row
                // loop because PostgreSQL settles the SET list while planning: the statement is
                // refused even when nothing conflicts and the action would never have run, so no
                // row goes in that PostgreSQL would not have let in.
                Column target = table.getColumns().get(colIdx);
                boolean toDefault = isDefaultLiteral(set.value());
                if (target.isGenerated() && !toDefault) {
                    throw new MemgresException("column \"" + target.getName()
                            + "\" can only be updated to DEFAULT\n  Detail: Column \""
                            + target.getName() + "\" is a generated column.", "428C9");
                }
                if (!toDefault && target.getDefaultValue() != null
                        && target.getDefaultValue().contains("__identity__:always")) {
                    throw new MemgresException("column \"" + target.getName()
                            + "\" can only be updated to DEFAULT\n  Detail: Column \""
                            + target.getName() + "\" is an identity column defined as GENERATED"
                            + " ALWAYS.", "428C9");
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
            // PostgreSQL points at the rows the statement itself proposed, which is where the
            // duplicate came from — the table had nothing to do with it.
            throw new MemgresException(
                    "ON CONFLICT DO UPDATE command cannot affect row a second time"
                            + "\n  Hint: Ensure that no rows proposed for insertion within the"
                            + " same command have duplicate constrained values.", "21000");
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
        // The names below are the ones this statement's own target exposes. A renaming captured
        // for an earlier statement describes a view this one is not writing through, and reading
        // it here turned an ordinary column of an ordinary table into a column that is not there.
        captureViewTarget();
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
        executor.viewDmlByMerge = false;
        recordRelationLock(stmt.schema(), stmt.table(), "RowExclusiveLock");
        rejectCatalogViewWrite(stmt.table(), "insert into");
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
        captureViewTarget();
        activeViewWriteTarget = executor.database.getView(schemaName, stmt.table());
        // C6: Enforce INSERT privilege
        executor.checkTablePrivilege("INSERT", schemaName, stmt.table());
        // Check table-level locks (blocks if ACCESS EXCLUSIVE held by another session)
        executor.database.checkTableLockForDml(schemaName + "." + stmt.table(), executor.session);

        // Check for INSTEAD / ALSO rules. Writing through an updatable view is rewritten to a
        // write on the base table, so the base table's rules apply as well — a rule of the
        // view's own takes precedence, as it does in PG, because it replaces the rewrite.
        String ruleRelation = stmt.table();
        String ruleSchema = executor.relationSchemaOf(stmt.schema(), stmt.table());
        List<Database.StoredRule> insertRules =
                executor.database.getRules(ruleSchema, ruleRelation, "INSERT");
        if (insertRules.isEmpty() && table != null && table.getName() != null
                && !table.getName().equalsIgnoreCase(stmt.table())) {
            ruleRelation = table.getName();
            ruleSchema = table.getSchemaName();
            insertRules = executor.database.getRules(ruleSchema, ruleRelation, "INSERT");
        }
        rejectConditionalInsteadOnView(ruleSchema, ruleRelation, "INSERT", insertRules);
        // A rule that writes back to its own table re-enters itself; PG detects that while
        // rewriting the statement and never runs any of it.
        if (!insertRules.isEmpty() && executor.isRuleExpanding(ruleRelation, "INSERT")) {
            throw PgErrors.infiniteRecursionInRules(ruleRelation);
        }
        // ON CONFLICT and a rule both rewrite the same INSERT, and PostgreSQL has no defined order
        // for the two, so it refuses the combination outright rather than running them beside each
        // other.
        if (stmt.onConflict() != null
                && (rulesForbidOnConflict(executor.relationSchemaOf(stmt.schema(), stmt.table()),
                        stmt.table())
                    || (table != null
                        && rulesForbidOnConflict(table.getSchemaName(), table.getName())))) {
            throw new MemgresException("INSERT with ON CONFLICT clause cannot be used with table"
                    + " that has INSERT or UPDATE rules", "0A000");
        }
        // An INSTEAD rule stands in for the statement, so the statement has no rows of its own to
        // report and RETURNING is refused before any of it runs.
        rejectReturningThroughInsteadRule(stmt.returning(), "INSERT", stmt.table(), insertRules);
        // An INSTEAD rule written without a WHERE replaces the statement outright: nothing reaches
        // the relation and only the rules' actions run, with the last action that is itself an
        // INSERT speaking for the statement. A DO INSTEAD NOTHING of that kind leaves no action to
        // run and so reports no rows. A rule that does carry a WHERE only claims the rows it holds
        // for, which is decided per row below.
        boolean insteadWholeInsert = false;
        for (Database.StoredRule rule : insertRules) {
            if (rule.isInstead() && rule.getQualification() == null) {
                insteadWholeInsert = true;
                break;
            }
        }
        if (insteadWholeInsert) {
            boolean answersReturning = stmt.returning() != null && !stmt.returning().isEmpty();
            // The relation the statement named is the shape its answer comes back in, and it has
            // to be taken before the rule's actions run: what they resolve leaves nothing of this
            // statement's view behind.
            Table ruledShape = answersReturning ? ruledRelationShape(stmt.table(), table) : null;
            RuleAnswer answer = answersReturning ? new RuleAnswer() : null;
            // The rows the statement offered are what the rule fires for, whether they were
            // written out or read by a query: PostgreSQL runs the source query and rewrites the
            // rule's actions over its rows, so an INSERT ... SELECT under an unconditional INSTEAD
            // rule wrote nothing at all while only a VALUES list was looked for.
            int ruleCount = runInsertRuleActions(insertRules, insertSourceRows(stmt), null, stmt,
                    table, ruleRelation, answer);
            if (answersReturning) {
                return insteadRuleReturning(stmt.returning(), ruledShape, stmt.alias(),
                        QueryResult.Type.INSERT, ruleCount, answer.rows);
            }
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
        // A VALUES list with no column list names nothing, so the loop above sees nothing to
        // check -- but the values still land on the view's columns from the left, and one of the
        // positions they reach may be a computed column. Without this the value was written
        // straight into whatever base column sat behind that position. Only the positions the row
        // actually reaches count: PostgreSQL accepts a short VALUES list that stops before the
        // computed column.
        if (activeViewExprCols != null && stmt.columns() == null && activeViewColNames != null
                && stmt.values() != null) {
            int widest = 0;
            for (List<Expression> valueRow : stmt.values()) {
                if (valueRow.size() > widest) widest = valueRow.size();
            }
            for (int i = 0; i < activeViewColNames.size() && i < widest; i++) {
                String viewCol = activeViewColNames.get(i);
                if (viewCol == null || !activeViewExprCols.contains(viewCol.toLowerCase())) continue;
                MemgresException ex = new MemgresException("cannot insert into column \"" + viewCol
                        + "\" of view \"" + stmt.table() + "\"", "0A000");
                ex.setDetail(ViewUpdatability.DETAIL_NOT_COLUMN);
                throw ex;
            }
        }
        // Every name the statement writes to is resolved against the relation before any row is
        // looked at, so an INSERT into a column that is not there is refused the same way whether
        // the VALUES list is empty, the SELECT behind it returns nothing, or the table is empty.
        requireTargetColumns(table, stmt.table(), stmt.columns());

        // Validate RETURNING columns exist before processing rows
        validateReturning(stmt.returning(), table);
        rejectSystemColumnsInRoutedInsert(stmt.returning(), table, stmt.alias());

        // PG 18: RETURNING OLD/NEW is supported with ON CONFLICT DO NOTHING
        // Non-conflicting rows return NEW.*, conflicting (skipped) rows return nothing

        List<PgTrigger> triggers = enabledTriggers(rowTriggersFor(table, stmt.table()));
        Map<Table, List<PgTrigger>> insertRowTriggers = rowTriggersByRelation(table, triggers);
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
        List<List<Expression>> valueRows = insertSourceRows(stmt);

        // A VALUES row holding a set-returning call is as many rows as the call produces, so the
        // expansion happens before anything counts the rows or writes one.
        List<List<Expression>> srfExpandedRows = new ArrayList<>();
        List<RowContext> valueRowContexts = expandSrfValueRows(valueRows, srfExpandedRows);
        if (valueRowContexts != null) valueRows = srfExpandedRows;

        // A qualified INSTEAD rule replaces the statement only for the rows its WHERE holds for.
        // PostgreSQL writes the rest itself and reports their count, so which rows those are is
        // settled before anything is written.
        Set<Integer> ruleSuppressedRows = insteadClaimedRows(insertRules, valueRows,
                valueRowContexts, stmt, table);

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
        Map<Table, List<RowImage>> insSnapshot = !anyRowTriggers(insertRowTriggers) ? null
                : snapshotTargetTables(collectTargetTables(table));
        try {
        int valueRowIdx = -1;
        for (List<Expression> valueRow : valueRows) {
            valueRowIdx++;
            // A row an INSTEAD rule claimed is the rule's to deal with, not the statement's.
            if (ruleSuppressedRows.contains(valueRowIdx)) continue;
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
                    // A column named with brackets is written through them: the row starts with
                    // nothing in that column, and the assignment builds the value around it.
                    List<SubscriptExpr.Subscript> subscripts = stmt.columnSubscripts() == null
                            || i >= stmt.columnSubscripts().size()
                            ? null : stmt.columnSubscripts().get(i);
                    if (subscripts != null) {
                        row[colIdx] = TypeCoercion.coerceForStorage(
                                executor.subscriptAssign.assign(row[colIdx], genCol, subscripts,
                                        valueRow.get(i), valueCtx),
                                table.getColumns().get(colIdx));
                        filledCols.add(colIdx);
                        continue;
                    }
                    Object val = executor.evalExpr(valueRow.get(i), valueCtx);
                    row[colIdx] = validationHelper.storedValue(val, table.getColumns().get(colIdx));
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
                    row[i] = validationHelper.storedValue(val, table.getColumns().get(i));
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
                        // PostgreSQL says what closed the column and what would open it, the same
                        // way it does for a generated column just above.
                        throw new MemgresException("cannot insert a non-DEFAULT value into column \""
                                + col.getName() + "\"\n  Detail: Column \"" + col.getName()
                                + "\" is an identity column defined as GENERATED ALWAYS."
                                + "\n  Hint: Use OVERRIDING SYSTEM VALUE to override.", "428C9");
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
                // A default the view carries stands in for the base relation's, because that is
                // the default the statement's own relation declares.
                String throughView = viewColumnDefault(schemaName, stmt.table(), col.getName());
                if (throughView != null) {
                    row[i] = wrapEnumValue(col, TypeCoercion.coerceForStorage(
                            executor.evaluateDefault(throughView, col.getType()), col));
                } else if (col.getDefaultValue() != null && col.getDefaultValue().startsWith("__identity__")) {
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

            // A row trigger is a trigger of the relation the row is stored in, so the partition
            // the row routes to is the one whose triggers fire and the one they are told about.
            // Routing comes first for the other reason too: a row no partition will take is
            // refused where PostgreSQL refuses it, before any trigger has been given the chance to
            // rewrite the key into one that fits.
            Table beforeTrigTable = partitionHelper.routeForInsert(table, row);
            row = triggerHelper.executeTriggers(
                    rowTriggersIn(insertRowTriggers, beforeTrigTable, triggers),
                    PgTrigger.Timing.BEFORE, PgTrigger.Event.INSERT, row, null, beforeTrigTable);
            if (row == null) {
                // BEFORE trigger returned NULL: skip this row (not inserted, not counted, no RETURNING)
                continue;
            }

            // A generated column is computed from the row that is about to be stored, which is the
            // row the BEFORE triggers have finished with. Computing it from the row as written
            // stored a value derived from a column the triggers then changed.
            computeGeneratedColumns(table, row);

            // Validate enum values
            validationHelper.validateEnumValues(row, table);

            // Validate domain CHECK constraints
            validationHelper.validateDomainChecks(row, table);

            // PostgreSQL asks the row-level policies whether the row may be written at all before
            // it judges the row against anything else, so a row no policy admits is refused even
            // when the arbiter would have skipped it for conflicting with one already there.
            if (table.isRlsEnabled()) {
                String insertRlsSchema = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
                if (!executor.shouldBypassRls(table, insertRlsSchema)) {
                    enforceRlsWithCheck(table, row, "INSERT");
                    // RETURNING reads the new row back, so PostgreSQL requires it to pass the SELECT
                    // policies too and refuses the statement when it does not. The check runs before
                    // the write, so there is nothing to undo.
                    if (stmt.returning() != null && !stmt.returning().isEmpty()) {
                        enforceRlsWithCheck(table, row, "SELECT");
                    }
                }
            }

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
                // An index over the target's columns arbitrates only if the predicate written beside
                // them reaches it: an index with no predicate of its own is reached by any of them
                // and by none at all, and a partial index only by one that entails its predicate.
                if (stmt.onConflict().columns() != null && !stmt.onConflict().columns().isEmpty()) {
                    boolean sawTargetColumns = false;
                    boolean inferred = false;
                    for (StoredConstraint sc : table.getConstraints()) {
                        if ((sc.getType() != StoredConstraint.Type.UNIQUE
                                && sc.getType() != StoredConstraint.Type.PRIMARY_KEY)
                                || !ArbiterPredicate.sameColumns(sc.getColumns(), stmt.onConflict().columns())) {
                            continue;
                        }
                        sawTargetColumns = true;
                        if (ArbiterPredicate.infers(stmt.onConflict().whereClause(), sc.getWhereExpr())) {
                            inferred = true;
                            break;
                        }
                    }
                    // Columns no unique index covers at all are reported where the conflict is
                    // searched for, which is where a column the relation has not got is reported.
                    if (sawTargetColumns && !inferred) {
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
                                // A partial index is reached only by a predicate that entails its
                                // own, and one that is not partial by any predicate at all.
                                if (!ArbiterPredicate.infers(stmt.onConflict().whereClause(), sc.getWhereExpr())) {
                                    continue;
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
                                if (!ArbiterPredicate.infers(stmt.onConflict().whereClause(), sc.getWhereExpr())) {
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
                Table conflictTable = partitionHelper.settledPartition(table, beforeTrigTable, row);
                // A row another session has inserted but not committed may yet be rolled back, so
                // there is no telling whether it is a conflict: PostgreSQL makes the second
                // inserter wait for that transaction to end and only then decides between the
                // insert and the ON CONFLICT action. Acting on the uncommitted row instead wrote
                // through it, and both writes went when its transaction rolled back.
                Object[] conflictProbe = conflictTable == table ? row : conflictTable.rowFromParent(row);
                // PostgreSQL judges the proposed row against the columns that may not be null and
                // against the relation's CHECK constraints before it asks any index whether the
                // row conflicts, so a row the arbiter would have skipped is refused for what it
                // holds all the same. The partition it is stored in answers first, as it does on
                // the ordinary write path, because the partition carries its own copy.
                if (conflictTable != table) {
                    executor.constraintValidator
                            .validateBeforeConflictArbitration(conflictTable, conflictProbe);
                }
                executor.constraintValidator.validateBeforeConflictArbitration(table, row);
                while (true) {
                    StatementCancel.check();
                    ConstraintValidator.PendingUniqueConflict pendingInsert = executor.constraintValidator
                            .findUncommittedUniqueConflict(conflictTable, conflictProbe, null);
                    if (pendingInsert == null) break;
                    awaitPendingInsert(conflictTable, pendingInsert);
                }
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
                        Object[] evalConflict = conflictRow;
                        if (hasVirtualColumns(conflictTable)) {
                            // A VIRTUAL generated column is not stored: PostgreSQL rewrites a
                            // reference to one into its generation expression, so the expression is
                            // evaluated where the reference stood and nowhere else. A conflict
                            // clause that never names the column never evaluates it.
                            evalConflict = computeVirtualColumns(conflictTable, conflictRow,
                                    columnsNamed(conflictTable, stmt.alias(), stmt.onConflict().doUpdate(),
                                            stmt.onConflict().doUpdateWhereClause(), stmt.returning()));
                        }
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
                            Column setCol = conflictTable.getColumns().get(colIdx);
                            // DO UPDATE is an UPDATE, and SET col = DEFAULT asks for the column's
                            // own default there as it does anywhere else. A stored generated column
                            // is left alone: its value is computed again from the row this leaves,
                            // which is the only value DEFAULT can mean for it.
                            if (isDefaultLiteral(set.value())) {
                                if (setCol.isGenerated()) continue;
                                newRow[colIdx] = assignedDefault(conflictTable, setCol);
                                continue;
                            }
                            Object val = executor.evalExpr(set.value(), conflictCtx);
                            newRow[colIdx] = validationHelper.storedValue(val, setCol);
                        }
                        // The conflict path is an UPDATE, so it fires the UPDATE row triggers of
                        // the relation the row it conflicted with is stored in.
                        newRow = triggerHelper.executeTriggers(
                                rowTriggersIn(insertRowTriggers, conflictTable, triggers),
                                PgTrigger.Timing.BEFORE,
                                PgTrigger.Event.UPDATE, newRow, oldRow, conflictTable, conflictUpdCols);
                        if (newRow == null) continue;   // BEFORE trigger suppressed the row
                        computeGeneratedColumns(conflictTable, newRow);
                        // Validate constraints BEFORE mutating the row to avoid index corruption
                        partitionHelper.checkPartitionConstraint(conflictTable, newRow);
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
                        // The row the conflict clause leaves behind is a row of the view as much as
                        // the one an insert would have left, so the view's check option is asked of
                        // it too -- after it is written, as PostgreSQL asks it.
                        validationHelper.enforceViewCheckOption(viewCheckExprs, conflictTable, newRow);
                        recordUpdateUndo(stmt.schema(), conflictTable.getName(), conflictRow, oldRow);
                        // The conflict path is an UPDATE, and PostgreSQL puts the version it
                        // writes somewhere else in the relation: the row RETURNING reads back
                        // lives at a new ctid, and the row written after it takes the next one.
                        recordRowUpdateMeta(conflictTable == table ? stmt.schema() : null,
                                conflictTable, conflictRow);
                        triggerHelper.executeTriggers(
                                rowTriggersIn(insertRowTriggers, conflictTable, triggers),
                                PgTrigger.Timing.AFTER,
                                PgTrigger.Event.UPDATE, conflictRow, oldRow, conflictTable, conflictUpdCols);
                        if (stmt.returning() != null && !stmt.returning().isEmpty()) {
                            returningRows.add(evalReturning(stmt.returning(), conflictTable, stmt.alias(), conflictRow, oldRow, conflictRow));
                        }
                        inserted++;
                        continue;
                    }
                }
            }

            // Validate constraints and insert atomically under table write lock
            // to prevent concurrent INSERTs from both passing unique checks.
            Table targetTable = partitionHelper.settledPartition(table, beforeTrigTable, row);
            // An ATTACHed partition may order its columns differently from the parent.
            Object[] storedRow = targetTable == table ? row : targetTable.rowFromParent(row);
            validateAndInsertWaiting(table, row, targetTable, storedRow);
            final Table undoTable = targetTable;
            final Object[] undoRow = storedRow;
            statementUndo.record(() -> undoTable.removeRow(undoRow));
            // A view's check option is the last thing PostgreSQL asks, once the row is stored and
            // its indexes have been made: a row a unique index refuses is refused for the key it
            // duplicates, and a row the arbiter skipped is never offered to the view at all.
            validationHelper.enforceViewCheckOption(viewCheckExprs, table, row);
            recordInsertUndo(stmt.schema(), targetTable.getName(), storedRow);
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
            triggerHelper.executeTriggers(rowTriggersIn(insertRowTriggers, trigTable, triggers),
                    PgTrigger.Timing.AFTER, PgTrigger.Event.INSERT, afterRowTriggerNewRows.get(i), null, trigTable);
        }

        // Fire statement-level AFTER triggers with transition tables
        triggerHelper.fireStatementTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.INSERT, table, insertedRows, null);
        } catch (MemgresException e) {
            // Statement is atomic: undo any rows already inserted/updated before the failure.
            if (insSnapshot != null) {
                restoreTargetTables(insSnapshot);
                if (executor.session != null) {
                    executor.session.discardUndoForRelations(insSnapshot.keySet());
                }
            }
            throw e;
        }

        // Every rule the event carries runs beside the statement, in rule-name order, for the rows
        // its own WHERE holds for. The rows an INSTEAD rule claimed were kept out of the write above.
        if (!insertRules.isEmpty()) {
            runInsertRuleActions(insertRules, valueRows, valueRowContexts, stmt, table, ruleRelation,
                    null);
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
        // STDIN and STDOUT are one thing to PostgreSQL's grammar -- the absence of a file name --
        // and the direction is read from the FROM or the TO alone. COPY ... FROM STDOUT therefore
        // reads from the client and COPY ... TO STDIN writes to it, neither being an error.

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
            // There is no relation to open here, so the options are the first thing PostgreSQL
            // settles -- a bad option is reported even when the query reads a missing table.
            raiseCopyOptionError(stmt);
            // Execute the subquery (validates table references, producing 42P01 for missing tables)
            QueryResult subResult = executor.executeStatement(stmt.subquery());
            return QueryResult.copyOut(subResult.getColumns(), subResult.getRows(), stmt);
        }

        String copySchema = "public";
        String copyTableName = stmt.table();
        if (copyTableName.contains(".")) {
            int dot = copyTableName.indexOf('.');
            copySchema = copyTableName.substring(0, dot);
            copyTableName = copyTableName.substring(dot + 1);
        }
        String copyKind = rejectCopyRelationKind(stmt, copySchema, copyTableName);

        if (!stmt.isFrom()) {
            // A materialized view holds rows of its own and PostgreSQL reads them out. They are
            // reached through a query because only a table has a row list to walk here.
            if (RelationNamespace.MATVIEW.equals(copyKind)) {
                String colList = (stmt.columns() != null && !stmt.columns().isEmpty())
                        ? String.join(", ", stmt.columns())
                        : "*";
                QueryResult mvResult = executor.execute(
                        "SELECT " + colList + " FROM " + stmt.table(), Cols.listOf());
                raiseCopyOptionError(stmt);
                return QueryResult.copyOut(mvResult.getColumns(), mvResult.getRows(), stmt);
            }
            // H35: honor explicit schema qualifier so a same-named temp table cannot shadow it
            Table table = executor.resolveTable(copySchema, copyTableName, stmt.table().contains("."));
            List<Column> columns = new ArrayList<>();
            List<Integer> colIndices = new ArrayList<>();
            if (stmt.columns() != null && !stmt.columns().isEmpty()) {
                for (String colName : stmt.columns()) {
                    int idx = table.getColumnIndex(colName);
                    if (idx < 0) throw new MemgresException(
                        "column \"" + colName + "\" of relation \"" + stmt.table() + "\" does not exist", "42703");
                    rejectGeneratedCopyColumn(table.getColumns().get(idx));
                    colIndices.add(idx);
                    columns.add(table.getColumns().get(idx));
                }
            } else {
                // A generated column is left out of the data in both directions: its value is a
                // function of the row, and COPY FROM refuses to read one back — so writing it out
                // produced a dump this server could not restore.
                for (int i = 0; i < table.getColumns().size(); i++) {
                    Column col = table.getColumns().get(i);
                    if (col.isGenerated()) continue;
                    colIndices.add(i);
                    columns.add(col);
                }
            }
            // The relation and the columns the statement named are settled; now the options are.
            raiseCopyOptionError(stmt);
            checkCopyOptionColumns(table, stmt.forceQuote());
            checkCopyOptionColumns(table, stmt.forceNotNull());
            checkCopyOptionColumns(table, stmt.forceNull());
            List<Object[]> rows = new ArrayList<>();
            for (Object[] tableRow : table.getRows()) {
                Object[] row = new Object[colIndices.size()];
                for (int i = 0; i < colIndices.size(); i++) {
                    row[i] = tableRow[colIndices.get(i)];
                }
                rows.add(row);
            }
            return QueryResult.copyOut(columns, rows, stmt);
        }

        // A relation COPY FROM cannot write to is refused only once the copy has been opened, so
        // everything the statement can be judged on by itself is settled first and the refusal
        // travels back with the result for the wire to raise after its CopyInResponse.
        MemgresException intoRefusal = copyIntoRefusal(copyKind, copyTableName);
        if (intoRefusal != null) {
            raiseCopyOptionError(stmt);
            parseCopyWhere(stmt);
            return QueryResult.copyInRefused(stmt, intoRefusal);
        }

        // COPY FROM STDIN: validate table/columns, then return COPY_IN for PgWireHandler
        // H35: honor explicit schema qualifier so a same-named temp table cannot shadow it
        Table table = executor.resolveTable(copySchema, copyTableName, stmt.table().contains("."));
        if (stmt.columns() != null && !stmt.columns().isEmpty()) {
            Set<String> seen = new java.util.HashSet<>();
            for (String colName : stmt.columns()) {
                int idx = table.getColumnIndex(colName);
                if (idx < 0) throw new MemgresException(
                    "column \"" + colName + "\" of relation \"" + stmt.table() + "\" does not exist", "42703");
                // Refused now rather than when the first data row arrives: PostgreSQL never sends
                // a CopyInResponse for a statement it has already decided against.
                rejectGeneratedCopyColumn(table.getColumns().get(idx));
                if (!seen.add(colName.toLowerCase())) {
                    throw new MemgresException(
                        "column \"" + colName + "\" specified more than once", "42701");
                }
            }
        }
        // The relation and the columns the statement named are settled; now the options are.
        raiseCopyOptionError(stmt);
        checkCopyOptionColumns(table, stmt.forceNotNull());
        checkCopyOptionColumns(table, stmt.forceNull());
        // COPY FREEZE writes rows visible to everyone at once, which is only safe while nobody
        // else can have seen the relation, so PostgreSQL allows it only for a table this
        // subtransaction created or truncated.
        if (stmt.freeze() && !executor.database.wasCreatedBy(table, executor.session)) {
            throw new MemgresException("cannot perform COPY FREEZE because the table was not "
                    + "created or truncated in the current subtransaction", "55000");
        }

        parseCopyWhere(stmt);

        // The data of a COPY FROM STDIN never reaches the server inside the statement: the lines
        // that follow it in a script are the client's own reading, and a server given them as SQL
        // answers for the first of them with a syntax error. So there is nothing to store here,
        // and the rows arrive one CopyData message at a time.
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

        // A GENERATED ALWAYS identity refuses a written value from INSERT and UPDATE, but not
        // from COPY: PostgreSQL lets COPY write the column as given, which is what carries the
        // identity values across a dump and restore. Refusing it here meant pg_dump could write
        // a dump of this server that this server would not read back.

        // FORCE_NOT_NULL: for specified columns, do not match against the null string
        List<String> forceNotNull = stmt.forceNotNull();
        // FORCE_NULL: for specified columns, match the (possibly quoted) value against the null string
        List<String> forceNull = stmt.forceNull();
        String effectiveNullStr = stmt.nullString() != null ? stmt.nullString() : "";

        for (int i = 0; i < colIndices.size(); i++) {
            int colIdx = colIndices.get(i);
            Column col = table.getColumns().get(colIdx);
            // PostgreSQL walks the target columns and reads each one's field as it goes, so a
            // line runs out of fields only after the fields it does carry have been converted:
            // an empty line on a two-column table fails in the first column's input function,
            // not on the column that has no field at all.
            if (i >= values.size()) {
                throw new MemgresException(
                        "missing data for column \"" + col.getName() + "\"", "22P04");
            }
            String val = values.get(i);

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
                row[colIdx] = copyFieldValue(stmt, val, col);
            }
        }

        // Checked where PostgreSQL checks it: once the target columns have all been read, so a
        // line with too many fields is reported after the fields that belong have been converted.
        if (values.size() > colIndices.size()) {
            throw new MemgresException("extra data after last expected column", "22P04");
        }

        // Compute generated columns
        computeGeneratedColumns(table, row);

        // Apply WHERE clause filtering (COPY FROM ... WHERE)
        Expression whereExpr = stmt.parsedWhere();
        if (whereExpr == null && stmt.whereClause() != null && !stmt.whereClause().isEmpty()) {
            whereExpr = com.memgres.engine.parser.Parser.parseExpression(stmt.whereClause());
            stmt.setParsedWhere(whereExpr);
        }
        if (whereExpr != null) {
            RowContext ctx = new RowContext(table, null, row);
            if (!executor.isTruthy(executor.evalExpr(whereExpr, ctx))) {
                return null; // Row does not match WHERE clause; skip it
            }
        }

        // A row trigger is a trigger of the relation the row is stored in, so a COPY into a
        // partitioned table fires the triggers of the partition the row routes to -- the copies it
        // was given of the parent's and whatever was written on the partition itself -- and tells
        // them the partition's name. Where the row is bound is settled here and settled for good:
        // a trigger that rewrites the key does not send the row to another partition, and a row no
        // partition will take is refused before any trigger runs.
        Table beforeCopyTable = partitionHelper.routeForInsert(table, row);
        // Triggers are held under the bare relation name, so the relation's own name is what finds
        // them: a COPY that wrote the schema out looked for them under "schema.relation" and found
        // none, and every trigger of a relation named that way was silently skipped.
        List<PgTrigger> triggers = enabledTriggers(
                executor.database.getTriggersForTable(beforeCopyTable.getName()));
        if (!triggers.isEmpty()) {
            Object[] modified = triggerHelper.executeTriggers(triggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.INSERT, row, null, beforeCopyTable);
            if (modified == null) return null; // BEFORE trigger returned null = skip row
            row = modified;
        }

        // A partitioned table stores nothing of its own: every row belongs to one of its leaves,
        // and one that belongs to none is refused rather than left sitting on the parent, where
        // nothing but a per-partition query would ever show it had gone astray.
        Table targetTable = partitionHelper.settledPartition(table, beforeCopyTable, row);
        // An ATTACHed partition may order its columns differently from the parent.
        Object[] storedRow = targetTable == table ? row : targetTable.rowFromParent(row);
        validateAndInsertWaiting(table, row, targetTable, storedRow);
        recordCopyInsert(stmt, table, targetTable, row, storedRow);

        // PostgreSQL holds every AFTER row trigger of a COPY back until the statement has stored
        // its last row, so what this row owes is queued rather than run here -- against the
        // partition it really went to, because a BEFORE trigger may have rewritten the key and sent
        // it somewhere else. Running each row's AFTER as the row went let that trigger read a
        // relation the statement had not finished writing.
        List<PgTrigger> afterCopyTriggers = targetTable == beforeCopyTable ? triggers
                : enabledTriggers(executor.database.getTriggersForTable(targetTable.getName()));
        if (!afterCopyTriggers.isEmpty()) {
            copyAfterRowTriggers.add(afterCopyTriggers);
            copyAfterRowTables.add(targetTable);
            copyAfterRowNewRows.add(Arrays.copyOf(row, row.length));
        }
        // A transition table of the statement's AFTER trigger holds every row the copy wrote.
        if (copyCollectsRows) copyWrittenRows.add(Arrays.copyOf(row, row.length));

        // The rollback set matches rows by identity, so what is handed back has to be the array
        // that was actually stored — for a routed row that is the leaf's copy, not the parent's.
        return storedRow;
    }

    /**
     * One field of a COPY line, read by its column's own reader.
     *
     * <p>PostgreSQL reports which field it could not read rather than the line it was on: while a
     * reader is running it holds the column and the text it was given, and a failure there names
     * both. Binary data has no text a reader could be shown, so there PostgreSQL names the column
     * and stops. Either way the relation and the line number belong to the copy rather than to
     * the row, and are added where the copy is being read.
     */
    private Object copyFieldValue(CopyStmt stmt, String val, Column col) {
        try {
            return validationHelper.storedValue(val, col);
        } catch (MemgresException e) {
            if (e.getCopyField() == null) {
                e.setCopyField("binary".equalsIgnoreCase(stmt.format())
                        ? "column " + col.getName()
                        : "column " + col.getName() + ": \"" + val + "\"");
            }
            throw e;
        }
    }

    /**
     * What a COPY FROM has fired, what it still owes, and the rows it wrote.
     *
     * <p>PostgreSQL runs a COPY as one statement: the relation's FOR EACH STATEMENT triggers fire
     * once around the whole of it, and every AFTER row trigger waits for the last row rather than
     * following its own. A connection has one COPY open at a time and this is the connection's
     * executor, so the statement's own record lives here for as long as the copy does.
     */
    private Table copyStatementTable;
    private List<PgTrigger> copyStatementTriggers;
    private boolean copyCollectsRows;
    private final List<List<PgTrigger>> copyAfterRowTriggers = new ArrayList<List<PgTrigger>>();
    private final List<Table> copyAfterRowTables = new ArrayList<Table>();
    private final List<Object[]> copyAfterRowNewRows = new ArrayList<Object[]>();
    private final List<Object[]> copyWrittenRows = new ArrayList<Object[]>();

    /**
     * Open a COPY FROM: the relation's BEFORE statement triggers fire once, before any row.
     *
     * <p>PostgreSQL opens the copy first and fires them second, so one of them that refuses the
     * statement is reported after the client has already been told to send its data.
     */
    void beginCopyFrom(CopyStmt stmt) {
        discardCopyFrom();
        Table table = copyRelationOf(stmt);
        List<PgTrigger> declared = enabledTriggers(
                executor.database.getTriggersForTable(table.getName()));
        // Only a transition table reads the written rows back, so nothing is kept when no AFTER
        // statement trigger of this relation declared one.
        boolean collects = false;
        for (PgTrigger trigger : declared) {
            if (trigger.isForEachStatement()
                    && trigger.getTiming() == PgTrigger.Timing.AFTER
                    && trigger.getEvent() == PgTrigger.Event.INSERT
                    && trigger.getNewTransitionTable() != null) {
                collects = true;
            }
        }
        triggerHelper.fireStatementTriggers(declared, PgTrigger.Timing.BEFORE,
                PgTrigger.Event.INSERT, table, null, null);
        copyStatementTable = table;
        copyStatementTriggers = declared;
        copyCollectsRows = collects;
    }

    /**
     * Close it: every AFTER row trigger the copy queued, in the order the rows were read, and then
     * the relation's AFTER statement triggers over everything the copy wrote.
     */
    void finishCopyFrom() {
        Table table = copyStatementTable;
        if (table == null) return;
        List<PgTrigger> declared = copyStatementTriggers;
        List<List<PgTrigger>> owedTriggers = new ArrayList<List<PgTrigger>>(copyAfterRowTriggers);
        List<Table> owedTables = new ArrayList<Table>(copyAfterRowTables);
        List<Object[]> owedRows = new ArrayList<Object[]>(copyAfterRowNewRows);
        List<Object[]> written = new ArrayList<Object[]>(copyWrittenRows);
        discardCopyFrom();
        for (int i = 0; i < owedRows.size(); i++) {
            triggerHelper.executeTriggers(owedTriggers.get(i), PgTrigger.Timing.AFTER,
                    PgTrigger.Event.INSERT, owedRows.get(i), null, owedTables.get(i));
        }
        triggerHelper.fireStatementTriggers(declared, PgTrigger.Timing.AFTER,
                PgTrigger.Event.INSERT, table, written, null);
    }

    /** Forget a copy that ended without reaching the end of its statement. */
    void discardCopyFrom() {
        copyStatementTable = null;
        copyStatementTriggers = null;
        copyCollectsRows = false;
        copyAfterRowTriggers.clear();
        copyAfterRowTables.clear();
        copyAfterRowNewRows.clear();
        copyWrittenRows.clear();
    }

    /** The relation a COPY names, with an explicit schema qualifier honoured. */
    private Table copyRelationOf(CopyStmt stmt) {
        String copySchema = "public";
        String copyTableName = stmt.table();
        if (copyTableName.contains(".")) {
            int dot = copyTableName.indexOf('.');
            copySchema = copyTableName.substring(0, dot);
            copyTableName = copyTableName.substring(dot + 1);
        }
        return executor.resolveTable(copySchema, copyTableName, stmt.table().contains("."));
    }

    private void fillDefaults(Table table, Object[] row) {
        for (int i = 0; i < table.getColumns().size(); i++) {
            row[i] = defaultValueFor(table, table.getColumns().get(i));
        }
    }

    /**
     * The value a column takes when the statement supplies none: its DEFAULT, worked out now.
     *
     * <p>A serial or an identity column draws from its sequence, and that is a value consumed.
     * PostgreSQL evaluates a default expression once for every place the statement it rewrote
     * holds one, so a rule reading NEW of an inserted row draws a value of its own rather than
     * the one the row was given.
     */
    private Object defaultValueFor(Table table, Column col) {
        boolean serial = col.getType() == DataType.SERIAL || col.getType() == DataType.BIGSERIAL
                || col.getType() == DataType.SMALLSERIAL;
        if (col.getDefaultValue() != null && col.getDefaultValue().startsWith("__identity__")) {
            return resolveIdentityNextVal(table, col);
        } else if (serial && col.getDefaultValue() != null && col.getDefaultValue().contains("nextval(")) {
            return resolveSerialNextVal(table, col);
        } else if (serial && col.getDefaultValue() == null) {
            // Default was removed (e.g. DROP SEQUENCE CASCADE) — no auto-value
            return null;
        } else if (serial) {
            return table.nextSerial();
        } else if (col.getDefaultValue() != null) {
            Object defVal = executor.evaluateDefault(col.getDefaultValue(), col.getType(), col.getParsedDefaultExpr());
            return TypeCoercion.coerceForStorage(defVal, col);
        }
        // A column that declares no default of its own still has the one its type declares.
        // PostgreSQL asks the relation for a default first and the column's type second, so a
        // domain's DEFAULT stands behind every column of that domain — for a statement that leaves
        // the column out, for COPY, and for an assignment of the keyword alike.
        return domainDefault(col);
    }

    /** The default the column's type declares, which only a domain ever does. */
    private Object domainDefault(Column col) {
        if (col.getDomainTypeName() == null) return null;
        DomainType domain = executor.database.getDomain(col.getDomainTypeName());
        if (domain == null || domain.getDefaultValue() == null) return null;
        return executor.evaluateDefault(domain.getDefaultValue(), domain.getBaseType());
    }

    /**
     * The value an assignment of the DEFAULT keyword writes.
     *
     * <p>The relation the statement named is the one that answers. Writing through a view takes
     * the view's own column default, and where the view declares none the assignment writes what
     * the column's type declares and otherwise nothing at all: PostgreSQL substitutes the default
     * while it rewrites the write onto the relation underneath, so neither that relation's own
     * default nor its identity sequence is reached through a view. An INSERT is different and is
     * left where it is — the rewritten INSERT asks the base relation in its turn for every column
     * it still has no value for, so a base default does reach one.
     */
    private Object assignedDefault(Table table, Column col) {
        Database.ViewDef view = activeViewWriteTarget;
        if (view == null) return wrapEnumValue(col, defaultValueFor(table, col));
        String throughView = viewColumnDefault(view.schemaName(), view.name(), col.getName());
        Object value = throughView == null ? domainDefault(col)
                : executor.evaluateDefault(throughView, col.getType());
        return wrapEnumValue(col, TypeCoercion.coerceForStorage(value, col));
    }

    /**
     * What a rule's own query reads where the statement wrote DEFAULT.
     *
     * <p>PostgreSQL substitutes a column's default for the keyword while it rewrites the statement,
     * which is before any rule is added to it, so no rule's query ever holds one to read. Left
     * standing, the keyword reached the expression evaluator and every write to a relation carrying
     * a rule was refused outright. The column's own default expression stands in for it rather than
     * a value worked out once, because a default that draws from a sequence draws again for every
     * row: the rule's query and the statement it was added to take different values from it.
     */
    private Expression ruleAssignedValue(String relation, Table written, InsertStmt.SetClause set) {
        if (!isDefaultLiteral(set.value())) return set.value();
        // The relation the statement named is the one that answers, and a rule stands in place of
        // the rewrite that would have reached the relation underneath: through a view the keyword
        // is the view's own column default, and where the view declares none it is nothing at all.
        Database.ViewDef view = executor.database.getView(
                executor.relationSchemaOf(null, relation), relation);
        if (view != null) {
            String throughView = viewOwnDefault(view, set.column());
            return throughView == null ? Literal.ofNull()
                    : com.memgres.engine.parser.Parser.parseExpression(throughView);
        }
        int colIdx = written == null ? -1 : written.getColumnIndex(set.column());
        if (colIdx < 0) return Literal.ofNull();
        Column col = written.getColumns().get(colIdx);
        String def = col.getDefaultValue();
        if (def != null && !def.startsWith("__identity__") && col.getParsedDefaultExpr() != null) {
            return col.getParsedDefaultExpr();
        }
        return new ComputedValue(defaultValueFor(written, col));
    }

    /** The default a view declares for a column of its own, or null where it declares none. */
    private String viewOwnDefault(Database.ViewDef view, String column) {
        if (view.cachedColumns() == null || column == null) return null;
        for (Column viewCol : view.cachedColumns()) {
            if (viewCol.getName().equalsIgnoreCase(column)) return viewCol.getDefaultValue();
        }
        return null;
    }

    /**
     * What {@code NEW} carries for a column an INSERT supplied no value for.
     *
     * <p>PostgreSQL fills a column's default in while it rewrites the statement, and the relation
     * the statement named is the one that answers: a write through a view takes the view's own
     * column default. Where the view declares none the relation underneath is asked only if the
     * write still reaches it — a rule of the view's own stands in place of that rewrite, so NEW
     * reads nothing for the column rather than a default nobody wrote on the view.
     */
    private Object insertRuleDefault(InsertStmt stmt, Table table, Column col) {
        String schemaName = executor.relationSchemaOf(stmt.schema(), stmt.table());
        String throughView = viewColumnDefault(schemaName, stmt.table(), col.getName());
        if (throughView != null) {
            return wrapEnumValue(col, TypeCoercion.coerceForStorage(
                    executor.evaluateDefault(throughView, col.getType()), col));
        }
        Database.ViewDef view = executor.database.getView(schemaName, stmt.table());
        if (view != null
                && !executor.database.getRules(view.schemaName(), view.name(), "INSERT").isEmpty()) {
            return null;
        }
        return defaultValueFor(table, col);
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
        executor.lastSequenceInstanceId = seq.getInstanceId();
        executor.sessionSequenceValues.put(seq.getInstanceId(), value);
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
                if (stmt.isFrom()) rejectGeneratedCopyColumn(col);
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

    /**
     * A generated column may not be named in a COPY column list, in either direction: its value is
     * computed from the row rather than carried in the data. PostgreSQL refuses the statement, not
     * the row, so for COPY FROM this has to fire before the CopyInResponse goes out.
     */
    private static void rejectGeneratedCopyColumn(Column col) {
        if (!col.isGenerated()) return;
        MemgresException e = new MemgresException(
                "column \"" + col.getName() + "\" is a generated column", "42P10");
        e.setDetail("Generated columns cannot be used in COPY.");
        throw e;
    }

    /**
     * Read the COPY FROM WHERE clause, and refuse what may not stand in one.
     *
     * <p>The WHERE picks rows one at a time as they arrive: there is no group behind a row to
     * aggregate, no result to number a window call against, and no query for a nested one to run
     * in. Read as a general expression, all three parsed and evaluated like anything else.
     */
    private void parseCopyWhere(CopyStmt stmt) {
        if (stmt.whereClause() == null || stmt.whereClause().isEmpty()) return;
        Expression copyWhere = com.memgres.engine.parser.Parser.parseExpression(stmt.whereClause());
        executor.selectExecutor.placementCheck.rejectSubquery(copyWhere, "COPY FROM WHERE condition");
        executor.selectExecutor.placementCheck.reject(copyWhere, "COPY FROM WHERE conditions");
        stmt.setParsedWhere(copyWhere);
    }

    /**
     * Raise what the WITH list earned, if anything. PostgreSQL reads a COPY option list against
     * its option set only once it has the relation open and the named columns resolved, so the
     * relation that is not there and the column that is not a column are both reported ahead of
     * an option nobody recognises.
     */
    private static void raiseCopyOptionError(CopyStmt stmt) {
        RuntimeException e = stmt.optionError();
        if (e != null) throw e;
    }

    /** A column a FORCE_QUOTE, FORCE_NOT_NULL or FORCE_NULL option names has to be one. */
    private static void checkCopyOptionColumns(Table table, List<String> names) {
        if (names == null) return;
        for (String name : names) {
            if ("*".equals(name)) continue;
            if (table.getColumnIndex(name) < 0) {
                throw new MemgresException("column \"" + name + "\" of relation \""
                        + table.getName() + "\" does not exist", "42703");
            }
        }
    }

    /**
     * What PostgreSQL will not copy out of, and the kind it names when it refuses.
     *
     * <p>COPY reads and writes a heap. The kinds that hold no rows of their own are refused by
     * name rather than quietly answering with nothing: a partitioned table's rows all live in its
     * leaves, so reading one streamed an empty result. A view is read through the COPY (SELECT
     * ...) form. A materialized view holds rows of its own and is read like a table.
     *
     * @return the kind the name reached, or null when it reaches nothing
     */
    private String rejectCopyRelationKind(CopyStmt stmt, String schema, String bareName) {
        String holding = copySchemaHolding(stmt, schema, bareName);
        String kind = RelationNamespace.kindOf(executor.database, holding, bareName);
        if (kind == null || stmt.isFrom()) return kind;
        if (RelationNamespace.VIEW.equals(kind)) {
            throw copyKindRefusal("cannot copy from view \"" + bareName + "\"",
                    "Try the COPY (SELECT ...) TO variant.");
        }
        if (RelationNamespace.SEQUENCE.equals(kind)) {
            throw copyKindRefusal("cannot copy from sequence \"" + bareName + "\"", null);
        }
        if (RelationNamespace.INDEX.equals(kind)) {
            throw copyKindRefusal("cannot copy from index \"" + bareName + "\"", null);
        }
        if (RelationNamespace.TABLE.equals(kind) && isPartitionedParent(holding, bareName)) {
            throw copyKindRefusal("cannot copy from partitioned table \"" + bareName + "\"",
                    "Try the COPY (SELECT ...) TO variant.");
        }
        return kind;
    }

    /**
     * What PostgreSQL will not copy into, or null when the relation can take rows.
     *
     * <p>This one is not raised where the others are. PostgreSQL settles the option list, the
     * column list and the WHERE from the statement, opens the copy stream, and only then reaches
     * the code that stores rows — which is where it finds it has been asked to store them in
     * something that is not a table. So the CopyInResponse has already gone out when the refusal
     * arrives, and a client is left one message further along than the statement's failure alone
     * would leave it. A view is written only through an INSTEAD OF INSERT trigger, which is what
     * the hint tells the reader to provide, so a view carrying one is copied into.
     */
    private MemgresException copyIntoRefusal(String kind, String bareName) {
        if (RelationNamespace.VIEW.equals(kind)) {
            if ((ViewUpdatability.insteadOfEvents(executor.database, bareName)
                    & ViewUpdatability.INSERT) != 0) {
                return null;
            }
            return copyKindRefusal("cannot copy to view \"" + bareName + "\"",
                    "To enable copying to a view, provide an INSTEAD OF INSERT trigger.");
        }
        if (RelationNamespace.MATVIEW.equals(kind)) {
            return copyKindRefusal("cannot copy to materialized view \"" + bareName + "\"", null);
        }
        if (RelationNamespace.SEQUENCE.equals(kind)) {
            return copyKindRefusal("cannot copy to sequence \"" + bareName + "\"", null);
        }
        if (RelationNamespace.INDEX.equals(kind)) {
            return copyKindRefusal("cannot copy to index \"" + bareName + "\"", null);
        }
        return null;
    }

    /** The schema the COPY's relation name reaches: the written one, or the one the path finds. */
    private String copySchemaHolding(CopyStmt stmt, String schema, String bareName) {
        if (stmt.table().contains(".")) return schema;
        String holding = RelationNamespace.schemaHolding(executor.database,
                executor.relationSearchPath(), bareName);
        return holding == null ? schema : holding;
    }

    private static MemgresException copyKindRefusal(String message, String hint) {
        MemgresException e = new MemgresException(message, "42809");
        if (hint != null) e.setHint(hint);
        return e;
    }

    private boolean isPartitionedParent(String schema, String bareName) {
        Schema s = executor.database.getSchema(schema == null ? "public" : schema.toLowerCase());
        Table t = s != null ? s.getTable(bareName) : null;
        return t != null && t.getPartitionStrategy() != null;
    }

    /**
     * Record the undo and the row metadata for a row a COPY stored. A row that routed to a
     * partition was stored in the leaf, so that is the relation it has to be taken out of again
     * if the COPY fails, and the parent's own snapshot has to be told the row is there.
     */
    private void recordCopyInsert(CopyStmt stmt, Table table, Table targetTable,
                                  Object[] row, Object[] storedRow) {
        if (targetTable == table) {
            recordInsertUndo(null, stmt.table(), storedRow);
            recordRowMeta(null, table, row);
            return;
        }
        recordInsertUndo(targetTable.getSchemaName(), targetTable.getName(), storedRow);
        recordRowMeta(targetTable.getSchemaName(), targetTable, row);
    }

    // ---- UPDATE ----

    QueryResult executeUpdate(UpdateStmt stmt) {
        return withCteScope(stmt.withClauses(), () -> executeUpdateInner(stmt));
    }

    private QueryResult executeUpdateInner(UpdateStmt stmt) {
        // Check read-only transaction
        checkReadOnly("UPDATE");
        rejectMaterializedViewWrite(stmt.table());
        // A rule rewrites the statement before anything else looks at the table, so an unqualified
        // INSTEAD NOTHING rule means no update happens and none of the checks below apply. A rule
        // with a WHERE takes only the rows it holds for out of the statement.
        List<Expression> insteadSuppress = new ArrayList<>();
        QueryResult ruled = applyInsteadRule(executor.relationSchemaOf(stmt.schema(), stmt.table()),
                stmt.table(), "UPDATE", QueryResult.Type.UPDATE,
                stmt.where(), stmt.setClauses(), stmt.alias(), stmt.from(), stmt.returning(),
                insteadSuppress);
        if (ruled != null) return ruled;
        // A DO ALSO rule is added to the statement, so its actions run against the rows as they
        // are now and the statement then goes on to do its own work.
        applyAlsoRule(executor.relationSchemaOf(stmt.schema(), stmt.table()),
                stmt.table(), "UPDATE", stmt.where(), stmt.setClauses(),
                stmt.alias(), stmt.from());
        String schemaName = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        // Collect WITH CHECK OPTION constraints from views we're updating through
        List<DmlValidationHelper.ViewCheck> viewCheckExprs = validationHelper.collectViewCheckExprs(stmt.table());
        // H35: honor explicit schema qualifier so a same-named temp table cannot shadow it
        executor.viewDmlVerb = "update";
        executor.viewDmlByMerge = false;
        recordRelationLock(stmt.schema(), stmt.table(), "RowExclusiveLock");
        rejectCatalogViewWrite(stmt.table(), "update");
        Table table = executor.resolveTable(schemaName, stmt.table(), stmt.schema() != null);
        // An UPDATE names one row at a time; there is no group behind it to aggregate and no
        // result to number a window against, in either the assignments or the WHERE.
        checkUpdatePlacement(stmt, table);
        // Capture view column mapping before further resolveTable calls clobber it (renamed-column views).
        captureViewTarget();
        activeViewWriteTarget = executor.database.getView(schemaName, stmt.table());
        // The view's own WHERE goes with them: it is part of what this statement may reach.
        List<AstExecutor.ViewQual> viewQuals = executor.lastViewQuals;
        // A view column computed from an expression has nothing to assign back to.
        Set<String> viewExprCols = activeViewExprCols;
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
        // "SET t.c = ..." reads as an assignment to a field of a column named t, so what goes
        // missing is a column of the relation's own name. Nothing in that complaint shows how it
        // came to be asked for, which is why PostgreSQL adds what the writer actually did.
        for (InsertStmt.SetClause set : stmt.setClauses()) {
            if (set.subField() == null || !set.column().equalsIgnoreCase(stmt.table())
                    || table.getColumnIndex(set.column()) >= 0) {
                continue;
            }
            throw new MemgresException("column \"" + set.column() + "\" of relation \""
                    + stmt.table() + "\" does not exist"
                    + "\n  Hint: SET target columns cannot be qualified with the relation name.",
                    "42703");
        }
        requireTargetColumns(table, stmt.table(), setTargets, true);
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
        validateReturning(stmt.returning(), table, stmt.from());
        rejectAmbiguousReturning(stmt.returning(), table, namesBesideTarget(stmt.from(), null));
        List<PgTrigger> triggers = enabledTriggers(rowTriggersFor(table, stmt.table()));
        Map<Table, List<PgTrigger>> updateRowTriggers = rowTriggersByRelation(table, triggers);
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
            fromContexts = readExtraRelation(
                    () -> executor.fromResolver.resolveWrittenFromClause(stmt.from(),
                            stmt.where()),
                    new Object[]{stmt.setClauses(), stmt.where(), stmt.returning(), stmt.from()},
                    new Object[]{stmt.where(), stmt.from()}, stmt.where());
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

        // Every relation that stores rows for this one: its partitions and its inheritance
        // children. ONLY keeps the statement to the named relation's own storage, which for a
        // partitioned table is nothing at all. Each row is remembered against the relation that
        // holds it, because that is the one whose indexes have to follow the change.
        List<Table> updateTargets = new ArrayList<>();
        if (stmt.only()) {
            updateTargets.add(table);
        } else {
            DmlPartitionHelper.collectRelationAndDescendants(table, updateTargets);
        }
        Map<Object[], Table> updateRowOwner = new IdentityHashMap<>();
        List<Object[]> rows = new ArrayList<>();
        for (Table t : updateTargets) {
            for (Object[] r : t.getRows()) {
                rows.add(r);
                updateRowOwner.put(r, t);
            }
        }
        // What the statement may reach before its own WHERE has said a word. An UPDATE through an
        // auto-updatable view reaches only the rows the view shows, because PostgreSQL rewrites it
        // onto the base relation with the view's qualification added. A transaction reading from a
        // snapshot reaches only rows that existed when it was taken -- a row another session has
        // inserted since is one it is not shown, and PostgreSQL passes over it rather than
        // reporting a conflict about it -- while a row it was shown that another transaction has
        // deleted and committed ends it, there being no version of that row left to write. The
        // snapshot pairs rows with the relation they were read from, so a relation whose rows live
        // in partitions or children is left to the check that follows.
        rows = executor.filterByViewQuals(viewQuals, table, rows);
        if (executor.session != null && updateTargets.size() == 1) {
            String snapshotKey = schemaName + "." + stmt.table();
            executor.session.checkRRConcurrentDelete(snapshotKey, table, image ->
                    stmt.where() == null || executor.isTruthy(
                            executor.evalExpr(stmt.where(), viewAwareCtx(table, stmt.alias(), image))));
            List<Object[]> visible = new ArrayList<>(rows.size());
            for (Object[] r : rows) {
                if (executor.session.isVisibleInRRSnapshot(snapshotKey, r)) visible.add(r);
            }
            rows = visible;
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
        Map<Table, List<RowImage>> updSnapshot = !anyRowTriggers(updateRowTriggers) ? null
                : snapshotTargetTables(collectTargetTables(table));
        try {
        boolean fromUpdateHasVirtual = hasVirtualColumns(table);
        // A VIRTUAL generated column is not stored: PostgreSQL rewrites a reference to one into its
        // generation expression, so what the statement names is what is evaluated and an UPDATE
        // that never names the column never evaluates it. One context serves both the qualification
        // and the assignments here, so it carries everything the statement names.
        Set<String> fromUpdateReads = fromUpdateHasVirtual
                ? columnsNamed(table, stmt.alias(), stmt.setClauses(), stmt.where(), stmt.returning())
                : null;
        if (fromContexts != null) {
            // Multi-table UPDATE: join main table with FROM tables. Which rows the statement acts
            // on is settled the way it is for any other UPDATE -- against the values the other
            // transactions have actually committed, waiting for one that is part-way through
            // writing a row this statement wants. A row chosen out of another session's
            // uncommitted values is a row PostgreSQL never offered, and writing it leaves that
            // session holding a change its ROLLBACK can no longer take back.
            final List<RowContext> fromRows = fromContexts;
            // Which row of the FROM a target row joined to decides what its assignments read.
            // PostgreSQL uses one of them however many matched, so the first is the one kept.
            final Map<Object[], RowContext> joinedTo = new IdentityHashMap<>();
            // A scan a statement writes through reads its qualification the way any other scan
            // does: the parts cheapest first, and a sub-select among them under all of them.
            final Expression fromQualification = SelectExecutor.readCheapestFirst(stmt.where());
            final List<Object[]> targetScan = rows;
            List<Object[]> targetRows = readQualified(stmt.where(),
                    writtenName(stmt.alias(), table), () ->
                    matchAgainstCommittedRows(table, targetScan, row -> {
                Object[] evalRow = fromUpdateHasVirtual
                        ? computeVirtualColumns(table, row, fromUpdateReads) : row;
                RowContext mainCtx = viewAwareCtx(table, stmt.alias(), evalRow,
                        updateRowOwner.get(row));
                for (RowContext fromCtx : fromRows) {
                    if (stmt.where() == null || executor.isTruthy(
                            executor.evalExpr(fromQualification, mainCtx.merge(fromCtx)))) {
                        joinedTo.put(row, fromCtx);
                        return true;
                    }
                }
                return false;
            }, () -> executor.filterByViewQuals(viewQuals, table,
                    rescanTargets(updateTargets, updateRowOwner))));
            List<Object[]> matchedRows = new ArrayList<>();
            List<RowContext> matchedContexts = new ArrayList<>();
            List<RowContext> matchedFromContexts = new ArrayList<>();
            // What an assignment reads is worked out for every row the join answered with, not only
            // for the one each target row takes its new values from: PostgreSQL builds the new row
            // for every pair the join made and settles only afterwards that a target row it has
            // already written is written once. So a pair whose generation expression raises reaches
            // the statement however many other pairs that target row also stands in.
            final List<RowContext> pairedFrom =
                    holdsVirtualRelation(fromRows) ? new ArrayList<RowContext>() : null;
            for (Object[] row : targetRows) {
                Object[] evalRow = fromUpdateHasVirtual
                        ? computeVirtualColumns(table, row, fromUpdateReads) : row;
                RowContext mainCtx = viewAwareCtx(table, stmt.alias(), evalRow,
                        updateRowOwner.get(row));
                RowContext fromCtx = joinedTo.get(row);
                if (fromCtx == null) {
                    // The row was judged on the version another transaction left behind, which is
                    // a different array from the one the relation stores.
                    for (RowContext candidate : fromRows) {
                        if (stmt.where() == null || executor.isTruthy(
                                executor.evalExpr(fromQualification, mainCtx.merge(candidate)))) {
                            fromCtx = candidate;
                            break;
                        }
                    }
                }
                if (fromCtx == null) continue;
                matchedRows.add(row);
                matchedContexts.add(mainCtx.merge(fromCtx));
                matchedFromContexts.add(fromCtx);
                if (pairedFrom != null) {
                    for (RowContext candidate : fromRows) {
                        if (candidate == fromCtx || stmt.where() == null || executor.isTruthy(
                                executor.evalExpr(fromQualification, mainCtx.merge(candidate)))) {
                            pairedFrom.add(candidate);
                        }
                    }
                }
            }
            // The rows the join kept are the ones an assignment or a RETURNING item reads the FROM
            // clause through, so its VIRTUAL generated columns are worked out here rather than as
            // the relation was scanned. The merged contexts stand in front of the same rows, so
            // filling these fills those.
            fillJoinedVirtuals(pairedFrom != null ? pairedFrom : matchedFromContexts,
                    stmt.setClauses(), stmt.where(), stmt.returning(), stmt.from());
            // PostgreSQL's UPDATE takes a FOR UPDATE lock on every row it touches, whether or not
            // it reached them through a join.
            lockRowsForDml(table, matchedRows);
            // Process matched rows with their FROM context
            Set<Object[]> updated = Collections.newSetFromMap(new IdentityHashMap<>());
            int updatedCount = 0;
            List<Object[]> fromOldRows = new ArrayList<>();
            List<Object[]> fromNewRows = new ArrayList<>();
            List<PendingAfterRow> fromAfterRows = new ArrayList<PendingAfterRow>();
            for (int i = 0; i < matchedRows.size(); i++) {
                Object[] row = matchedRows.get(i);
                if (updated.contains(row)) continue; // Each row updated at most once
                // A qualified INSTEAD rule has already spoken for the rows its WHERE holds for.
                if (!insteadSuppress.isEmpty()) {
                    Object[] proposed = Arrays.copyOf(row, row.length);
                    applySetClauses(stmt.setClauses(), table, proposed, matchedContexts.get(i));
                    if (ruleSuppressesRow(insteadSuppress, table, row, proposed)) continue;
                }
                updated.add(row);
                Object[] oldRow = Arrays.copyOf(row, row.length);
                // A row trigger is a trigger of the relation the row is stored in, so a write
                // through a partitioned table fires the partition's own triggers beside the copies
                // it was given of the parent's, and tells them the partition's name.
                Table fromOwner = updateRowOwner.get(row);
                Table fromFiresOn = fromOwner != null ? fromOwner : table;
                List<PgTrigger> fromRowTriggers = rowTriggersIn(updateRowTriggers, fromOwner, triggers);
                Object[] newRow = triggerHelper.executeTriggers(fromRowTriggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.UPDATE, row, oldRow, fromFiresOn, updatedColumnNames);
                if (newRow == null) {
                    continue;
                }
                // RLS USING filter: skip rows not visible under the UPDATE policies, and under the
                // SELECT policies as well when the statement reads a column of the target
                if (rlsUpdateActive) {
                    List<Object[]> rlsCheck = filterRowsForWrite(table,
                            Collections.singletonList(row), "UPDATE", stmt.alias(),
                            readsTargetRelation(stmt.where(), stmt.returning(), stmt.setClauses()));
                    if (rlsCheck.isEmpty()) continue;
                }
                RowContext ctx = matchedContexts.get(i);
                applySetClauses(stmt.setClauses(), table, newRow, ctx);
                // RLS WITH CHECK on the new row
                if (rlsUpdateActive) {
                    enforceRlsWithCheck(table, newRow, "UPDATE");
                    // RETURNING reads the updated row back, so it has to pass the SELECT policies
                    // too; PostgreSQL refuses the whole statement when it does not.
                    if (stmt.returning() != null && !stmt.returning().isEmpty()) {
                        enforceRlsWithCheck(table, newRow, "SELECT");
                    }
                }
                computeGeneratedColumns(table, newRow);
                try {
                    executor.constraintValidator.validateConstraints(table, newRow, row);
                } catch (MemgresException refused) {
                    spendTupleIdOnFailedWrite(fromOwner != null ? fromOwner : table, refused);
                    throw refused;
                }
                validationHelper.validateDomainChecks(newRow, table);
                // A statement that names a partition may not move a row out of that partition's
                // bound: the row was never offered to the partitioned table, so PostgreSQL
                // refuses the update rather than re-routing it.
                partitionHelper.checkPartitionConstraint(table, newRow);
                Object[] fromWritten;
                Table fromMovedTo = partitionRowMovesTo(table, fromOwner, newRow);
                if (fromMovedTo != null) {
                    Object[] fromArriving = beforeRowChangesPartition(updateRowTriggers, triggers,
                            fromRowTriggers, fromFiresOn, fromMovedTo, oldRow, newRow);
                    if (fromArriving == PARTITION_MOVE_REFUSED) continue;
                    if (fromArriving == null) {
                        takeRowOutOfPartition(fromFiresOn, row, oldRow, false);
                        fromAfterRows.add(new PendingAfterRow(
                                PgTrigger.Event.DELETE, fromFiresOn, null, oldRow));
                        continue;
                    }
                    fromWritten = moveRowAcrossPartitions(fromOwner, fromMovedTo, row, oldRow, fromArriving);
                    fromAfterRows.add(new PendingAfterRow(
                            PgTrigger.Event.DELETE, fromFiresOn, null, oldRow));
                    fromAfterRows.add(new PendingAfterRow(PgTrigger.Event.INSERT, fromMovedTo,
                            Arrays.copyOf(fromWritten, fromWritten.length), null));
                } else {
                    recordUpdateUndo(stmt.schema(), stmt.table(), row, oldRow);
                    Table fromStorage = fromOwner != null ? fromOwner : table;
                    fromStorage.updateRowInPlace(row, oldRow, newRow);
                    recordRowUpdateMeta(fromStorage == table ? stmt.schema() : null, fromStorage, row);
                    fromWritten = row;
                    fromAfterRows.add(new PendingAfterRow(PgTrigger.Event.UPDATE, fromFiresOn,
                            Arrays.copyOf(fromWritten, fromWritten.length), oldRow));
                }
                updatedCount++;
                executor.constraintValidator.handleFkOnUpdate(table, oldRow, fromWritten);
                fromOldRows.add(oldRow);
                fromNewRows.add(Arrays.copyOf(fromWritten, fromWritten.length));
                if (stmt.returning() != null && !stmt.returning().isEmpty()) {
                    returningRows.add(evalReturning(stmt.returning(), table, stmt.alias(), fromWritten, oldRow, fromWritten, matchedFromContexts.get(i)));
                }
            }
            // Fire queued AFTER ROW triggers
            for (PendingAfterRow pending : fromAfterRows) {
                triggerHelper.executeTriggers(
                        rowTriggersIn(updateRowTriggers, pending.relation, triggers),
                        PgTrigger.Timing.AFTER, pending.event, pending.newRow, pending.oldRow,
                        pending.relation, updatedColumnNames);
            }
            int count = updatedCount;
            // Fire statement-level AFTER UPDATE triggers with transition tables
            triggerHelper.fireStatementTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.UPDATE, table, fromNewRows, fromOldRows);
            // Track DML statistics
            if (count > 0) table.incrementTupUpdated(count);
            if (stmt.returning() != null && !stmt.returning().isEmpty()) {
                return QueryResult.returning(QueryResult.Type.UPDATE,
                        buildReturningColumns(stmt.returning(), table,
                                fromRows.isEmpty() ? null : fromRows.get(0)),
                        returningRows, count);
            }
            return QueryResult.command(QueryResult.Type.UPDATE, count);
        }

        // Simple UPDATE (no FROM clause)
        String updateAlias = stmt.alias();
        boolean updateHasVirtual = hasVirtualColumns(table);
        // A VIRTUAL generated column is not stored: PostgreSQL rewrites a reference to one into its
        // generation expression, which then stands where the reference stood. The qualification is
        // read for every row scanned, so what it names is worked out there; the assignments and
        // RETURNING are read for the rows it kept, and so is what they name. Working every one out
        // for every row scanned lost the write outright: an expression that raises for a row the
        // statement passes over -- 10/a where a is zero -- ended a statement PostgreSQL completes.
        Set<String> updateFilters = updateHasVirtual
                ? columnsNamed(table, updateAlias, stmt.where()) : null;
        Set<String> updateReads = updateHasVirtual
                ? columnsNamed(table, updateAlias, stmt.setClauses(), stmt.where(), stmt.returning())
                : null;
        // The qualification's own parts are read cheapest first and the scan stops at the first one
        // that is false, so a part comparing only stored columns decides the row before any
        // generation expression is reached: WHERE a = 5 AND g = 2 never divides by the zero a.
        List<Expression> updateDecided = updateHasVirtual
                ? decidableQualification(table, updateAlias,
                        FromResolver.conjunctsOf(stmt.where()), true, null)
                : null;
        if (stmt.where() instanceof com.memgres.engine.parser.ast.CurrentOfExpr) {
            com.memgres.engine.parser.ast.CurrentOfExpr cof = (com.memgres.engine.parser.ast.CurrentOfExpr) stmt.where();
            rows = filterByCurrentOf(cof, table, rows);
        } else if (stmt.where() != null) {
            final List<Object[]> scanned = rows;
            // A scan a statement writes through reads its qualification the way any other scan
            // does: the parts cheapest first, and a sub-select among them under all of them.
            final Expression updateQualification = SelectExecutor.readCheapestFirst(stmt.where());
            rows = readQualified(stmt.where(), writtenName(updateAlias, table), () ->
                    matchAgainstCommittedRows(table, scanned, row -> {
                Object[] evalRow = updateHasVirtual
                        && !qualificationRejects(table, updateAlias, row, updateDecided)
                        ? computeVirtualColumns(table, row, updateFilters) : row;
                return executor.isTruthy(executor.evalExpr(updateQualification,
                        viewAwareCtx(table, updateAlias, evalRow, updateRowOwner.get(row))));
            }, () -> executor.filterByViewQuals(viewQuals, table,
                    rescanTargets(updateTargets, updateRowOwner))));
        }

        // A qualified INSTEAD rule has already spoken for the rows its WHERE holds for.
        if (!insteadSuppress.isEmpty()) {
            List<Object[]> keptRows = new ArrayList<>();
            for (Object[] row : rows) {
                Object[] proposed = Arrays.copyOf(row, row.length);
                Object[] setEvalRow = updateHasVirtual
                        ? computeVirtualColumns(table, row, updateReads) : row;
                applySetClauses(stmt.setClauses(), table, proposed,
                        viewAwareCtx(table, updateAlias, setEvalRow, updateRowOwner.get(row)));
                if (!ruleSuppressesRow(insteadSuppress, table, row, proposed)) keptRows.add(row);
            }
            rows = keptRows;
        }

        // RLS USING filter for UPDATE: restrict which rows can be updated, by the UPDATE policies
        // and by the SELECT policies as well when the statement reads a column of the target
        if (rlsUpdateActive) {
            rows = filterRowsForWrite(table, rows, "UPDATE", updateAlias,
                    readsTargetRelation(stmt.where(), stmt.returning(), stmt.setClauses()));
        }

        // PG's UPDATE takes a FOR UPDATE lock on every row it touches; a concurrent
        // FOR UPDATE NOWAIT must see it, or queue workers double-process the same row.
        lockRowsForDml(table, rows);

        int updatedCount = 0;
        List<Object[]> simpleOldRows = new ArrayList<>();
        List<Object[]> simpleNewRows = new ArrayList<>();
        List<PendingAfterRow> simpleAfterRows = new ArrayList<PendingAfterRow>();
        for (Object[] row : rows) {
            Object[] oldRow = Arrays.copyOf(row, row.length);

            // Build new row values on a COPY; don't modify live data until validated
            Object[] newRow = Arrays.copyOf(row, row.length);

            // Apply SET clauses first, then fire BEFORE UPDATE triggers so they see NEW with proposed values
            Object[] evalRow = updateHasVirtual ? computeVirtualColumns(table, row, updateReads) : row;
            applySetClauses(stmt.setClauses(), table, newRow,
                    viewAwareCtx(table, updateAlias, evalRow, updateRowOwner.get(row)));

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
                // RETURNING reads the updated row back, so it has to pass the SELECT policies too;
                // PostgreSQL refuses the whole statement when it does not.
                if (stmt.returning() != null && !stmt.returning().isEmpty()) {
                    enforceRlsWithCheck(table, newRow, "SELECT");
                }
            }

            // A row trigger is a trigger of the relation the row is stored in, so a write through
            // a partitioned table fires the partition's own triggers beside the copies it was given
            // of the parent's, in name order with them, and tells them the partition's name.
            Table owner = updateRowOwner.get(row);
            Table firesOn = owner != null ? owner : table;
            List<PgTrigger> rowTriggers = rowTriggersIn(updateRowTriggers, owner, triggers);
            // BEFORE UPDATE triggers (see NEW with SET-applied values, can further modify NEW)
            newRow = triggerHelper.executeTriggers(rowTriggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.UPDATE, newRow, oldRow, firesOn, updatedColumnNames);
            if (newRow == null) {
                continue;
            }

            computeGeneratedColumns(table, newRow);
            try {
                executor.constraintValidator.validateConstraints(table, newRow, row);
            } catch (MemgresException refused) {
                spendTupleIdOnFailedWrite(owner != null ? owner : table, refused);
                throw refused;
            }
            validationHelper.validateDomainChecks(newRow, table);
            // A statement that names a partition may not move a row out of that partition's
            // bound: the row was never offered to the partitioned table, so PostgreSQL refuses
            // the update rather than re-routing it.
            partitionHelper.checkPartitionConstraint(table, newRow);
            // ...and the view's check option last of all, as on the insert path: a row that
            // duplicates a key is refused for the key rather than for the view.
            validationHelper.enforceViewCheckOption(viewCheckExprs, table, newRow);

            // The row a statement leaves behind: the one it wrote where it stood, or the new one
            // the partition the values now belong in holds.
            Object[] writtenRow;
            Table movedTo = partitionRowMovesTo(table, owner, newRow);
            if (movedTo != null) {
                Object[] arriving = beforeRowChangesPartition(updateRowTriggers, triggers,
                        rowTriggers, firesOn, movedTo, oldRow, newRow);
                if (arriving == PARTITION_MOVE_REFUSED) continue;
                if (arriving == null) {
                    takeRowOutOfPartition(firesOn, row, oldRow, false);
                    simpleAfterRows.add(new PendingAfterRow(
                            PgTrigger.Event.DELETE, firesOn, null, oldRow));
                    continue;
                }
                writtenRow = moveRowAcrossPartitions(owner, movedTo, row, oldRow, arriving);
                simpleAfterRows.add(new PendingAfterRow(
                        PgTrigger.Event.DELETE, firesOn, null, oldRow));
                simpleAfterRows.add(new PendingAfterRow(PgTrigger.Event.INSERT, movedTo,
                        Arrays.copyOf(writtenRow, writtenRow.length), null));
            } else {
                recordUpdateUndo(stmt.schema(), stmt.table(), row, oldRow);
                // A row read through an inheritance parent is stored in the child, and that is
                // the relation whose indexes have to follow the change -- and the one whose page
                // the new version of the row goes on, so it is where the new ctid comes from.
                Table storage = owner != null ? owner : table;
                storage.updateRowInPlace(row, oldRow, newRow);
                recordRowUpdateMeta(storage == table ? stmt.schema() : null, storage, row);
                writtenRow = row;
                simpleAfterRows.add(new PendingAfterRow(PgTrigger.Event.UPDATE, firesOn,
                        Arrays.copyOf(writtenRow, writtenRow.length), oldRow));
            }
            updatedCount++;
            executor.constraintValidator.handleFkOnUpdate(table, oldRow, writtenRow);

            // Queue AFTER ROW triggers
            simpleOldRows.add(oldRow);
            simpleNewRows.add(Arrays.copyOf(writtenRow, writtenRow.length));

            if (stmt.returning() != null && !stmt.returning().isEmpty()) {
                returningRows.add(evalReturning(stmt.returning(), table, updateAlias,
                        writtenRow, oldRow, writtenRow));
            }
        }

        // Fire queued AFTER ROW triggers
        for (PendingAfterRow pending : simpleAfterRows) {
            triggerHelper.executeTriggers(
                    rowTriggersIn(updateRowTriggers, pending.relation, triggers),
                    PgTrigger.Timing.AFTER, pending.event, pending.newRow, pending.oldRow,
                    pending.relation, updatedColumnNames);
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
                if (executor.session != null) {
                    executor.session.discardUndoForRelations(updSnapshot.keySet());
                }
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

    /**
     * Evaluate one policy's WITH CHECK expression (falling back to USING) against a new row. An
     * error raised inside a policy is the statement's error, as it is in PostgreSQL, and not a
     * verdict on whether the row may be written: reporting a division by zero as "you are not
     * allowed" tells the writer nothing about what went wrong.
     */
    private boolean rlsCheckPasses(RlsPolicy policy, RowContext ctx) {
        Expression checkExpr = policy.getWithCheckExpr();
        if (checkExpr == null) checkExpr = policy.getUsingExpr();
        if (checkExpr == null) return true;
        return Boolean.TRUE.equals(executor.evalExpr(checkExpr, ctx));
    }

    /**
     * Filter rows by the RLS USING policies for a command, through the same gate the SELECT side
     * uses so that a query and a write answer the same way about the same row.
     */
    private List<Object[]> filterRowsByRlsUsing(Table table, List<Object[]> rows, String command, String alias) {
        List<RowContext> contexts = new ArrayList<>();
        Map<RowContext, Object[]> source = new IdentityHashMap<>();
        for (Object[] row : rows) {
            RowContext ctx = new RowContext(table, alias, row);
            contexts.add(ctx);
            source.put(ctx, row);
        }
        List<Object[]> filtered = new ArrayList<>();
        for (RowContext ctx : executor.fromResolver.filterByRlsUsing(contexts, table, command)) {
            filtered.add(source.get(ctx));
        }
        return filtered;
    }

    /**
     * The rows an UPDATE or DELETE may act on: the command's own USING policies decide, and the
     * SELECT policies decide as well whenever the statement reads a column of the relation it
     * writes. Measured against a table with an UPDATE policy but no SELECT policy, {@code SET n=99}
     * touches every row, while {@code WHERE id=1}, {@code SET n=n+1}, {@code DELETE ... WHERE id=4}
     * and any RETURNING see none — a row the user cannot read is a row the statement does not find.
     */
    private List<Object[]> filterRowsForWrite(Table table, List<Object[]> rows, String command,
                                              String alias, boolean readsTarget) {
        List<Object[]> allowed = filterRowsByRlsUsing(table, rows, command, alias);
        if (!readsTarget || allowed.isEmpty()) return allowed;
        return filterRowsByRlsUsing(table, allowed, "SELECT", alias);
    }

    /**
     * Whether the statement reads a column of the relation it writes: any column named in its WHERE
     * or on the right of an assignment, and any RETURNING list at all, which projects the row back.
     * A bare {@code WHERE true} names nothing, which is why having a WHERE is not the test.
     */
    private static boolean readsTargetRelation(Expression where, List<?> returning,
                                               List<InsertStmt.SetClause> setClauses) {
        if (returning != null && !returning.isEmpty()) return true;
        if (AstWalk.anyMatch(where, node -> node instanceof ColumnRef)) return true;
        return AstWalk.anyMatch(setValueExprs(setClauses), node -> node instanceof ColumnRef);
    }

    /** The expressions on the right of a statement's assignments. */
    private static List<Expression> setValueExprs(List<InsertStmt.SetClause> setClauses) {
        List<Expression> values = new ArrayList<>();
        if (setClauses != null) {
            for (InsertStmt.SetClause set : setClauses) values.add(set.value());
        }
        return values;
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

    /**
     * Read the statement's target relations again after a wait has ended: the relation may hold
     * rows that were not there when the scan began, so the map from row to the relation that
     * stores it is rebuilt from the same list.
     */
    private List<Object[]> rescanTargets(List<Table> targets, Map<Object[], Table> owners) {
        List<Object[]> fresh = new ArrayList<>();
        owners.clear();
        for (Table t : targets) {
            for (Object[] r : t.getRows()) {
                fresh.add(r);
                owners.put(r, t);
            }
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
        boolean waited = false;
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
            Object[] blockedRow = null;
            Set<Object[]> seen = Collections.newSetFromMap(new IdentityHashMap<Object[], Boolean>());
            for (Object[] row : scan) {
                seen.add(row);
                if (notCommitted.contains(row)) continue;
                // The version this statement may act on is the one it was shown, which for a row
                // some transaction has replaced and committed since is not what the row now holds.
                // A row another transaction is still writing has settled nothing yet, so it is
                // waited for rather than judged either way.
                boolean beingWritten = otherOld.containsKey(row);
                Object[] asShown = beingWritten
                        ? null : me.versionReplacedSinceSnapshot(key, row);
                Object[] committed = beingWritten ? otherOld.get(row)
                        : (asShown != null ? asShown : row);
                if (committed == null || !matches.test(committed)) continue;
                if (asShown != null) {
                    // PostgreSQL refuses the moment a row it means to lock turns out to have been
                    // written by a transaction that committed after this one began reading, before
                    // it looks at what the newer version holds at all. Judging the row on that
                    // newer version instead let a statement pass silently over a row it was
                    // entitled to write and report that it had written nothing.
                    throw new MemgresException(
                            "could not serialize access due to concurrent update", "40001");
                }
                Session own = owner.get(row);
                if (own != null) { blocker = own; blockedRow = row; break; }
                result.add(row);
            }
            // A row another transaction has deleted without committing is gone from the scan but
            // is still committed-live: this statement has to wait for the outcome before it can
            // say whether it deleted it. A row this statement was never shown is not one of those:
            // it was no part of the relation this transaction reads, so whatever becomes of that
            // delete changes nothing for it and PostgreSQL walks straight past.
            if (blocker == null) {
                for (Object[] row : hiddenByOther) {
                    if (seen.contains(row)) continue;
                    if (!me.isVisibleInRRSnapshot(key, row)) continue;
                    if (matches.test(row)) { blocker = owner.get(row); blockedRow = row; break; }
                }
            }
            if (blocker == null) return result;
            // Taken while the transaction being waited for still holds its write, so each image
            // holds the version this statement is entitled to read rather than the one it waits
            // for, and kept for as long as the statement runs. A qualification read again after a
            // wait is judged one part on the version the other transaction left behind and one
            // part on the relations as this statement found them: PostgreSQL substitutes the new
            // version of the row being judged and reads everything else -- a subquery in the
            // qualification, one on the right of an assignment, one in RETURNING -- under the
            // snapshot the statement started with. Nothing is taken until a wait is really about
            // to happen.
            if (!waited) me.readImagesForStatement(key, table);
            final Session waitFor = blocker;
            executor.database.awaitConcurrentWrite(me, waitFor,
                    () -> waitFor.isInTransaction() && !waitFor.isDoomed()
                            && waitFor.hasUncommittedWork(key), table.getName());
            waited = true;
            rejectIfMovedToAnotherPartition(waitFor, table, blockedRow);
            rejectIfWaitedForDeletedRow(waitFor, key, table, blockedRow);
            if (rescan != null) scan = rescan.get();
        }
    }

    /**
     * Refuse a statement whose row the transaction it waited for moved into another partition.
     *
     * <p>PostgreSQL will not follow a row across relations: the version the other transaction
     * wrote lives in a partition this statement never aimed at, so there is nothing left where the
     * lock was taken. Rather than let the statement report that it touched nothing -- silently
     * passing over a row it was entitled to act on -- PostgreSQL ends it. A transaction reading
     * from a snapshot is told this in the same words it is told about every other write it lost.
     */
    private void rejectIfMovedToAnotherPartition(Session mover, Table table, Object[] row) {
        if (row == null || mover == null || executor.session == null) return;
        if (!mover.movedRowToAnotherPartition(row)) return;
        if (relationOrDescendantHolds(table, row)) return;
        throw movedToAnotherPartition(executor.session);
    }

    /**
     * Refuse a statement whose row the transaction it waited for deleted and committed.
     *
     * <p>A transaction reading from a snapshot was shown this row and waited its turn to write
     * it, and what it waited for was the row being taken away. PostgreSQL has no version of it
     * left that belongs to that snapshot, so it ends the transaction rather than let the
     * statement report that it wrote nothing -- which is all it could otherwise say about a row
     * it was entitled to write and no longer has. A transaction that reads each statement afresh
     * stands differently: by the time it looks again the row really is gone, nothing was taken
     * from it, and there is nothing to tell it.
     */
    private void rejectIfWaitedForDeletedRow(Session deleter, String key, Table table,
                                             Object[] row) {
        if (row == null || executor.session == null) return;
        if (!executor.session.readsFromSnapshot()) return;
        // A transaction still holding its delete has taken nothing away yet: the row is there for
        // everybody else, and this wait ended for some other reason.
        if (deleter != null && deleter.isInTransaction()) return;
        // A row this transaction was never shown is not one it lost. PostgreSQL's scan would
        // never have reached it, so its going is nothing to end a statement over.
        if (!executor.session.isVisibleInRRSnapshot(key, row)) return;
        if (relationOrDescendantHolds(table, row)) return;
        throw new MemgresException(
                "could not serialize access due to concurrent delete", "40001");
    }

    /** Whether this relation, or one of the relations that store rows for it, holds a row. */
    static boolean relationOrDescendantHolds(Table relation, Object[] row) {
        if (relation.getRows().contains(row)) return true;
        for (Table partition : relation.getPartitions()) {
            if (relationOrDescendantHolds(partition, row)) return true;
        }
        for (Table child : relation.getChildren()) {
            if (relationOrDescendantHolds(child, row)) return true;
        }
        return false;
    }

    /** The refusal above, in the words the reader's isolation level earns. */
    static MemgresException movedToAnotherPartition(Session session) {
        String isolation = session.getEffectiveIsolationLevel();
        boolean fromSnapshot = "repeatable read".equals(isolation) || "serializable".equals(isolation);
        return new MemgresException(fromSnapshot
                ? "could not serialize access due to concurrent update"
                : "tuple to be locked was already moved to another partition"
                        + " due to concurrent update",
                "40001");
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
            // Reject explicit writes to GENERATED ALWAYS AS ... STORED columns. The casts that used
            // to stand in these three branches read the value as a Literal only to discard it, so
            // any caller reaching them with an expression would throw ClassCastException instead of
            // the refusal PostgreSQL gives.
            if (genCol.isGenerated() && !(set.value() instanceof Literal && ((Literal) set.value()).literalType() == Literal.LiteralType.DEFAULT)) {
                throw new MemgresException("column \"" + genCol.getName() + "\" can only be updated to DEFAULT\n  Detail: Column \"" + genCol.getName() + "\" is a generated column.", "428C9");
            }
            if (genCol.isGenerated()) continue; // DEFAULT, skip (will be recomputed)
            // Reject explicit writes to GENERATED ALWAYS AS IDENTITY columns
            if (genCol.getDefaultValue() != null && genCol.getDefaultValue().contains("__identity__:always")
                    && !(set.value() instanceof Literal && ((Literal) set.value()).literalType() == Literal.LiteralType.DEFAULT)) {
                throw new MemgresException("column \"" + genCol.getName() + "\" can only be updated to DEFAULT\n  Detail: Column \"" + genCol.getName() + "\" is an identity column defined as GENERATED ALWAYS.", "428C9");
            }
            // For UPDATE SET col = DEFAULT, apply the column's default value
            if (isDefaultLiteral(set.value())) {
                newRow[colIdx] = assignedDefault(table, table.getColumns().get(colIdx));
                continue;
            }
            // An assignment through brackets writes part of the value the column holds, which is
            // not the same as computing a new whole one.
            if (set.subscripts() != null) {
                newRow[colIdx] = TypeCoercion.coerceForStorage(
                        executor.subscriptAssign.assign(newRow[colIdx],
                                table.getColumns().get(colIdx), set.subscripts(), set.value(), ctx),
                        table.getColumns().get(colIdx));
                continue;
            }
            Object val = executor.evalExpr(set.value(), ctx);
            // Handle composite field update: SET col.field = value
            if (set.subField() != null) {
                String compositeTypeName = genCol.getCompositeTypeName();
                if (compositeTypeName != null) {
                    Object currentVal = newRow[colIdx];
                    newRow[colIdx] = updateCompositeField(currentVal, compositeTypeName, set.subField(), val);
                }
            } else {
                newRow[colIdx] = validationHelper.storedValue(val, table.getColumns().get(colIdx));
            }
        }
    }

    /**
     * Carry out a referential action as the UPDATE on the referencing table that it is.
     *
     * <p>PostgreSQL runs ON DELETE / ON UPDATE SET NULL, SET DEFAULT and CASCADE as an ordinary
     * UPDATE of the child, so the child's BEFORE triggers see it and may rewrite it, its stored
     * generated columns are computed again from the new key, and its own NOT NULL, CHECK, UNIQUE,
     * domain and foreign-key rules decide whether the write on the parent may go ahead at all.
     * Writing the row directly skipped every one of those: a NOT NULL column could be left holding
     * NULL and a CHECK left holding a value it forbids.
     *
     * @param changedColumns the columns the action writes, so an {@code UPDATE OF} trigger is
     *     selected the same way it would be for a statement the client wrote
     * @param decided the constraint the action belongs to, which the caller has already checked
     *     against the referenced table as it will be once the statement is over
     * @return the rows actually written, in the order they were written
     */
    List<ConstraintValidator.ActionRow> applyReferentialUpdate(
            Table child, String childSchema, List<ConstraintValidator.ActionRow> pending,
            Set<String> changedColumns, StoredConstraint decided) {
        List<ConstraintValidator.ActionRow> written = new ArrayList<>();
        if (pending.isEmpty()) return written;
        List<PgTrigger> triggers = enabledTriggers(rowTriggersFor(child, child.getName()));
        for (ConstraintValidator.ActionRow action : pending) {
            // Whatever the BEFORE trigger returns is what gets written, and NULL means this row is
            // left alone — the same contract an UPDATE the client wrote lives under.
            Object[] newRow = triggerHelper.executeTriggers(triggers, PgTrigger.Timing.BEFORE,
                    PgTrigger.Event.UPDATE, action.newValues, action.oldValues, child, changedColumns);
            if (newRow == null) continue;
            computeGeneratedColumns(child, newRow);
            executor.constraintValidator.validateConstraints(child, newRow, action.row, decided);
            validationHelper.validateDomainChecks(newRow, child);
            // Record undo before the write, so a later row of the same action failing still leaves
            // this one restorable.
            executor.constraintValidator.recordCascadeUpdateUndo(
                    childSchema, child.getName(), action.row, action.oldValues);
            child.updateRowInPlace(action.row, action.oldValues, newRow);
            written.add(action);
        }
        for (ConstraintValidator.ActionRow action : written) {
            triggerHelper.executeTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.UPDATE,
                    action.row, action.oldValues, child, changedColumns);
        }
        return written;
    }

    /**
     * The delete half of the same rule: ON DELETE CASCADE is a DELETE on the referencing table.
     *
     * <p>Its BEFORE DELETE triggers run first, and one that returns NULL keeps its row — PostgreSQL
     * lets the trigger win and leaves the reference dangling rather than overriding it — and its
     * AFTER DELETE triggers run once the rows are gone.
     *
     * @param cascadeFurther given the rows that survived the BEFORE triggers, so the walk into the
     *     child's own dependents happens while those rows are still there
     */
    void applyReferentialDelete(Table child, String childSchema, Set<Object[]> rows,
                                java.util.function.Consumer<Set<Object[]>> cascadeFurther) {
        if (rows.isEmpty()) return;
        List<PgTrigger> triggers = enabledTriggers(rowTriggersFor(child, child.getName()));
        if (!triggers.isEmpty()) {
            Set<Object[]> vetoed = Collections.newSetFromMap(new IdentityHashMap<Object[], Boolean>());
            for (Object[] row : new ArrayList<>(rows)) {
                if (triggerHelper.executeTriggers(triggers, PgTrigger.Timing.BEFORE,
                        PgTrigger.Event.DELETE, row, row, child) == null) {
                    vetoed.add(row);
                }
            }
            rows.removeAll(vetoed);
        }
        cascadeFurther.accept(rows);
        if (rows.isEmpty()) return;
        List<Object[]> deleted = new ArrayList<>(rows);
        List<Object[]> oldRows = new ArrayList<>();
        for (Object[] row : deleted) {
            oldRows.add(Arrays.copyOf(row, row.length));
        }
        child.deleteRows(rows);
        // Record undo so ROLLBACK can restore the deleted rows
        executor.constraintValidator.recordCascadeDeleteUndo(childSchema, child.getName(), deleted);
        for (Object[] row : oldRows) {
            triggerHelper.executeTriggers(triggers, PgTrigger.Timing.AFTER,
                    PgTrigger.Event.DELETE, row, row, child);
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
        List<Expression> insteadSuppress = new ArrayList<>();
        QueryResult ruledDelete = applyInsteadRule(
                executor.relationSchemaOf(stmt.schema(), stmt.table()),
                stmt.table(), "DELETE", QueryResult.Type.DELETE,
                stmt.where(), null, stmt.alias(), stmt.using(), stmt.returning(), insteadSuppress);
        if (ruledDelete != null) return ruledDelete;
        // As for UPDATE: a DO ALSO rule runs beside the statement, over the rows about to go.
        applyAlsoRule(executor.relationSchemaOf(stmt.schema(), stmt.table()),
                stmt.table(), "DELETE", stmt.where(), null, stmt.alias(), stmt.using());
        String schemaName = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        // H35: honor explicit schema qualifier so a same-named temp table cannot shadow it
        executor.viewDmlVerb = "delete from";
        executor.viewDmlByMerge = false;
        recordRelationLock(stmt.schema(), stmt.table(), "RowExclusiveLock");
        rejectCatalogViewWrite(stmt.table(), "delete from");
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
        captureViewTarget();
        // The view's own WHERE goes with them: it is part of what this statement may reach.
        List<AstExecutor.ViewQual> viewQuals = executor.lastViewQuals;
        // The WHERE is resolved against the relation before the scan, so a name that is not a
        // column of it is refused whether or not there is a row for the evaluator to trip over.
        if (stmt.using() == null || stmt.using().isEmpty()) {
            requireReadableColumns(table, stmt.where(), stmt.alias(), stmt.table());
        }
        // INSTEAD OF DELETE triggers on a view: the trigger performs the actual work; the virtual
        // view table's rows are only used to match WHERE and populate OLD for the trigger.
        List<PgTrigger> deleteTriggersEarly =
                enabledTriggers(executor.database.getTriggersForTable(stmt.table()));
        boolean hasInsteadOfDelete = deleteTriggersEarly.stream().anyMatch(
                t -> t.getTiming() == PgTrigger.Timing.INSTEAD_OF && t.getEvent() == PgTrigger.Event.DELETE);
        // C6: Enforce DELETE privilege
        executor.checkTablePrivilege("DELETE", schemaName, stmt.table());
        checkReplicaIdentity(table, stmt.table(), "delete");
        // Validate RETURNING columns exist before processing rows
        validateReturning(stmt.returning(), table, stmt.using());
        rejectAmbiguousReturning(stmt.returning(), table, namesBesideTarget(stmt.using(), null));
        boolean hasReturning = stmt.returning() != null && !stmt.returning().isEmpty();

        // INSTEAD OF DELETE on a view: fire the trigger for each matching view row (the trigger
        // performs the real deletion against base tables); the virtual table is never modified.
        if (hasInsteadOfDelete) {
            List<Object[]> matched = new ArrayList<>();
            boolean idHasVirtual = hasVirtualColumns(table);
            // The qualification is read for every row, so a VIRTUAL generated column is worked out
            // for one only where the qualification names it: PostgreSQL evaluates the generation
            // expression where the reference to the column stands and nowhere else.
            Set<String> idFilters = idHasVirtual
                    ? columnsNamed(table, stmt.alias(), stmt.where()) : null;
            for (Object[] row : table.getRows()) {
                Object[] evalRow = idHasVirtual ? computeVirtualColumns(table, row, idFilters) : row;
                if (stmt.where() == null
                        || executor.isTruthy(executor.evalExpr(stmt.where(), viewAwareCtx(table, stmt.alias(), evalRow)))) {
                    matched.add(row);
                }
            }
            int cnt = 0;
            List<Object[]> insteadReturning = new ArrayList<>();
            for (Object[] row : matched) {
                // A DELETE has no NEW row: PostgreSQL leaves it unassigned, so the trigger sees only
                // OLD and nothing it returns is written back over the row it was handed. Passing the
                // live row as NEW undid whatever the trigger body had written to it.
                Object[] res = triggerHelper.executeTriggers(deleteTriggersEarly,
                        PgTrigger.Timing.INSTEAD_OF, PgTrigger.Event.DELETE, null, row, table);
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

        // Every relation that stores rows for this one: its partitions and its inheritance
        // children. ONLY keeps the statement to the named relation's own storage.
        List<Table> tablesToScan = new ArrayList<>();
        if (stmt.only()) {
            tablesToScan.add(table);
        } else {
            DmlPartitionHelper.collectRelationAndDescendants(table, tablesToScan);
        }

        // Build list of (owningTable, row) pairs
        List<Object[]> allRows = new ArrayList<>();
        Map<Object[], Table> rowOwner = new IdentityHashMap<>();
        for (Table t : tablesToScan) {
            for (Object[] row : t.getRows()) {
                allRows.add(row);
                rowOwner.put(row, t);
            }
        }
        // What the statement may reach before its own WHERE has said a word: the rows the view
        // shows, and the rows this transaction's snapshot holds. A row it was shown that another
        // transaction has deleted and committed ends the transaction instead.
        allRows = executor.filterByViewQuals(viewQuals, table, allRows);
        if (executor.session != null && tablesToScan.size() == 1) {
            String snapshotKey = schemaName + "." + stmt.table();
            executor.session.checkRRConcurrentDelete(snapshotKey, table, image ->
                    stmt.where() == null || executor.isTruthy(
                            executor.evalExpr(stmt.where(), viewAwareCtx(table, stmt.alias(), image))));
            List<Object[]> visible = new ArrayList<>(allRows.size());
            for (Object[] r : allRows) {
                if (executor.session.isVisibleInRRSnapshot(snapshotKey, r)) visible.add(r);
            }
            allRows = visible;
        }

        Set<Object[]> toDelete = Collections.newSetFromMap(new IdentityHashMap<>());
        List<Object[]> deleteOrder = new ArrayList<>();

        boolean deleteHasVirtual = hasVirtualColumns(table);
        // The qualification is read for every row scanned, so only the VIRTUAL generated columns it
        // names are worked out: PostgreSQL rewrites a reference to one into its generation
        // expression, and a DELETE that never names the column never evaluates it -- which is what
        // keeps a relation whose expression raises for some row a relation rows can be deleted from.
        Set<String> deleteFilters = deleteHasVirtual
                ? columnsNamed(table, stmt.alias(), stmt.where()) : null;
        // The qualification's own parts are read cheapest first and the scan stops at the first one
        // that is false, so a part comparing only stored columns decides the row before any
        // generation expression is reached.
        List<Expression> deleteDecided = deleteHasVirtual
                ? decidableQualification(table, stmt.alias(),
                        FromResolver.conjunctsOf(stmt.where()), true, null)
                : null;
        Map<Object[], RowContext> deleteUsingCtxMap = new IdentityHashMap<>();
        if (stmt.using() != null && !stmt.using().isEmpty()) {
            // DELETE ... USING: join main table with USING tables, delete matching main rows
            List<RowContext> usingContexts = readExtraRelation(
                    () -> executor.fromResolver.resolveWrittenFromClause(stmt.using(),
                            stmt.where()),
                    new Object[]{stmt.where(), stmt.returning(), stmt.using()},
                    new Object[]{stmt.where(), stmt.using()}, stmt.where());
            // Which rows the statement acts on is settled the way it is for any other DELETE --
            // against the values the other transactions have actually committed, waiting for one
            // that is part-way through writing a row this statement wants. A row chosen out of
            // another session's uncommitted values is a row PostgreSQL never offered, and deleting
            // it leaves that session holding a change its ROLLBACK can no longer take back.
            final List<RowContext> usingRows = usingContexts;
            final Map<Object[], RowContext> joinedTo = new IdentityHashMap<>();
            final List<Object[]> usingScan = allRows;
            final Map<Object[], Table> usingOwner = rowOwner;
            final List<Table> usingTables = tablesToScan;
            // A scan a statement writes through reads its qualification the way any other scan
            // does: the parts cheapest first, and a sub-select among them under all of them.
            final Expression usingQualification = SelectExecutor.readCheapestFirst(stmt.where());
            List<Object[]> usingMatched = readQualified(stmt.where(),
                    writtenName(stmt.alias(), table), () ->
                    matchAgainstCommittedRows(table, usingScan, row -> {
                Object[] evalRow = deleteHasVirtual
                        ? computeVirtualColumns(table, row, deleteFilters) : row;
                RowContext mainCtx = viewAwareCtx(table, stmt.alias(), evalRow, rowOwner.get(row));
                for (RowContext usingCtx : usingRows) {
                    if (stmt.where() == null || executor.isTruthy(
                            executor.evalExpr(usingQualification, mainCtx.merge(usingCtx)))) {
                        joinedTo.put(row, usingCtx);
                        return true;
                    }
                }
                return false;
            }, () -> {
                // A wait has ended, so the relation may hold rows that were not there when it
                // began. Both the scan and the row-to-partition map have to be rebuilt from it,
                // and the rows the view shows are still the only ones this statement may reach.
                usingScan.clear();
                usingOwner.clear();
                for (Table t : usingTables) {
                    for (Object[] r : t.getRows()) {
                        usingScan.add(r);
                        usingOwner.put(r, t);
                    }
                }
                return executor.filterByViewQuals(viewQuals, table, usingScan);
            }));
            for (Object[] row : usingMatched) {
                toDelete.add(row);
                deleteOrder.add(row);
                RowContext usingCtx = joinedTo.get(row);
                if (usingCtx != null) deleteUsingCtxMap.put(row, usingCtx);
            }
            // The rows the join kept are the ones RETURNING reads the USING clause through.
            fillJoinedVirtuals(new ArrayList<RowContext>(deleteUsingCtxMap.values()),
                    stmt.where(), stmt.returning(), stmt.using());
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
            // The same order any other scan reads its qualification in: the parts cheapest first,
            // and a sub-select among them under all of them.
            final Expression deleteQualification = SelectExecutor.readCheapestFirst(stmt.where());
            toDelete.addAll(readQualified(stmt.where(), writtenName(stmt.alias(), table), () ->
                    matchAgainstCommittedRows(table, deleteScan, row -> {
                // A DELETE with no WHERE takes every row, and takes the same path as any other so
                // that its row and statement triggers fire, its transition table is built and a
                // BEFORE trigger's veto is honoured.
                if (stmt.where() == null) return true;
                Object[] evalRow = deleteHasVirtual
                        && !qualificationRejects(table, stmt.alias(), row, deleteDecided)
                        ? computeVirtualColumns(table, row, deleteFilters) : row;
                return executor.isTruthy(executor.evalExpr(deleteQualification,
                        viewAwareCtx(table, stmt.alias(), evalRow, rowOwner.get(row))));
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
                // The rows the view shows are still the only ones this statement may reach.
                return executor.filterByViewQuals(viewQuals, table, deleteScan);
            })));
        }

        // A qualified INSTEAD rule has already spoken for the rows its WHERE holds for; the
        // statement PostgreSQL rewrote acts on the rest.
        if (!insteadSuppress.isEmpty()) {
            Set<Object[]> spokenFor = Collections.newSetFromMap(new IdentityHashMap<>());
            for (Object[] row : toDelete) {
                if (ruleSuppressesRow(insteadSuppress, table, row, null)) spokenFor.add(row);
            }
            toDelete.removeAll(spokenFor);
            deleteOrder.removeIf(spokenFor::contains);
        }

        // RLS USING filter for DELETE: remove rows that don't pass the DELETE policies, and the
        // SELECT policies too when the statement reads a column of the target
        if (rlsDeleteActive) {
            List<Object[]> rlsAllowed = filterRowsForWrite(table, new ArrayList<>(toDelete),
                    "DELETE", stmt.alias(),
                    readsTargetRelation(stmt.where(), stmt.returning(), null));
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
        List<PgTrigger> triggers = enabledTriggers(rowTriggersFor(table, table.getName()));
        // A row trigger is a trigger of the relation the row is stored in, so a delete through a
        // partitioned table fires the partition's own triggers and tells them its name.
        Map<Table, List<PgTrigger>> deleteRowTriggers = rowTriggersByRelation(table, triggers);

        // Fire BEFORE STATEMENT triggers
        triggerHelper.fireStatementTriggers(triggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.DELETE, table, null, null);

        // The rows the scan kept, in the order it found them. PostgreSQL works through them in
        // that order and takes each one out as it reaches it, so the order has to be settled
        // before the first trigger runs rather than after the last.
        List<Object[]> orderedDelete;
        if (!deleteOrder.isEmpty()) {
            orderedDelete = deleteOrder;
        } else {
            orderedDelete = new ArrayList<>();
            for (Object[] row : allRows) {
                if (toDelete.contains(row)) orderedDelete.add(row);
            }
        }
        // The statement writes while its row triggers run, so what the relation holds now is kept
        // to put back if one of them raises (PostgreSQL statement atomicity).
        Map<Table, List<RowImage>> delSnapshot = !anyRowTriggers(deleteRowTriggers) ? null
                : snapshotTargetTables(tablesToScan);

        List<Object[]> deletedRows = new ArrayList<>();
        List<Object[]> returningRows = new ArrayList<>();
        // PostgreSQL marks each version with the transaction that deleted it and leaves it where
        // it stands, so RETURNING -- which reads the row the statement found -- answers that
        // transaction's id for xmax. The mark is made before RETURNING is read.
        if (anyRowTriggers(deleteRowTriggers)) {
            // A row goes as its own BEFORE trigger lets it go, not when the statement ends,
            // because that is where PostgreSQL takes it out: a trigger that reads the relation
            // finds the rows already taken gone, so the second row of a two-row DELETE is told
            // the first has left. Holding every removal back showed each trigger the relation as
            // the scan had found it.
            Set<Object[]> skipRows = Collections.newSetFromMap(new IdentityHashMap<>());
            try {
                for (Object[] row : orderedDelete) {
                    Table firesOn = rowOwner.get(row);
                    if (firesOn == null) firesOn = table;
                    Object[] result = triggerHelper.executeTriggers(
                            rowTriggersIn(deleteRowTriggers, rowOwner.get(row), triggers),
                            PgTrigger.Timing.BEFORE, PgTrigger.Event.DELETE, null, row, firesOn);
                    if (result == null) {
                        skipRows.add(row);
                        continue;
                    }
                    markRowsDeletedBy(table, rowOwner, Collections.singletonList(row));
                    // What RETURNING answers for the row is read where a MERGE arm reads it: off
                    // the row the statement found, and before that row goes.
                    if (hasReturning) {
                        returningRows.add(evalReturning(stmt.returning(), table, stmt.alias(),
                                row, row, null, deleteUsingCtxMap.get(row)));
                    }
                    // Record undo (and run the RR write-write conflict check) before the physical
                    // delete, so an aborted statement leaves no unrecorded mutation behind.
                    recordDeleteUndo(stmt.schema(), stmt.table(), Collections.singletonList(row));
                    firesOn.deleteRow(row);
                }
            } catch (MemgresException e) {
                restoreTargetTables(delSnapshot);
                // The relations the statement scanned are back as they were, so their entries
                // describe changes already reversed; what a trigger wrote anywhere else keeps
                // its entries, because a failed statement has to be undone there too.
                if (executor.session != null) {
                    executor.session.discardUndoForRelations(delSnapshot.keySet());
                }
                throw e;
            }
            toDelete.removeAll(skipRows);
            orderedDelete.removeAll(skipRows);
        } else {
            markRowsDeletedBy(table, rowOwner, toDelete);
            if (hasReturning) {
                for (Object[] row : orderedDelete) {
                    RowContext usingCtx = deleteUsingCtxMap.get(row);
                    returningRows.add(evalReturning(stmt.returning(), table, stmt.alias(), row, row, null, usingCtx));
                }
            }
        }
        // Capture old rows for transition tables before deletion
        List<Object[]> oldRowsForTransition = new ArrayList<>();
        // The relation each row was stored in, so its AFTER trigger fires as that relation's.
        List<Table> deletedFrom = new ArrayList<Table>();
        for (Object[] row : orderedDelete) {
            oldRowsForTransition.add(Arrays.copyOf(row, row.length));
            Table storedIn = rowOwner.get(row);
            deletedFrom.add(storedIn != null ? storedIn : table);
        }
        deletedRows.addAll(orderedDelete);
        if (!anyRowTriggers(deleteRowTriggers)) {
            // Record undo (and run the RR write-write conflict check) before the physical
            // delete, so an aborted statement leaves no unrecorded mutation behind. Where row
            // triggers ran, each row was recorded and taken as its own trigger let it go.
            recordDeleteUndo(stmt.schema(), stmt.table(), deletedRows);
        }
        int deleted;
        try {
        // Remove matching rows atomically from each owning table
        for (Table t : tablesToScan) {
            t.deleteRows(toDelete);
        }
        deleted = deletedRows.size();

        // Fire queued AFTER DELETE row triggers
        if (anyRowTriggers(deleteRowTriggers)) {
            for (int i = 0; i < oldRowsForTransition.size(); i++) {
                Table firesOn = deletedFrom.get(i);
                triggerHelper.executeTriggers(rowTriggersIn(deleteRowTriggers, firesOn, triggers),
                        PgTrigger.Timing.AFTER, PgTrigger.Event.DELETE, null,
                        oldRowsForTransition.get(i), firesOn);
            }
        }

        // Fire statement-level AFTER DELETE triggers with transition tables
        triggerHelper.fireStatementTriggers(triggers, PgTrigger.Timing.AFTER, PgTrigger.Event.DELETE, table, null, oldRowsForTransition);
        } catch (MemgresException e) {
            if (delSnapshot != null) {
                restoreTargetTables(delSnapshot);
                if (executor.session != null) {
                    executor.session.discardUndoForRelations(delSnapshot.keySet());
                }
            }
            throw e;
        }

        // Track DML statistics
        if (deleted > 0) table.incrementTupDeleted(deleted);

        if (hasReturning) {
            return QueryResult.returning(QueryResult.Type.DELETE,
                    buildReturningColumns(stmt.returning(), table,
                            deleteUsingCtxMap.isEmpty() ? null
                                    : deleteUsingCtxMap.values().iterator().next()),
                    returningRows, deleted);
        }
        return QueryResult.command(QueryResult.Type.DELETE, deleted);
    }

    // ---- MERGE ----

    QueryResult executeMerge(MergeStmt stmt) {
        return withCteScope(stmt.withClauses(), () -> executeMergeInner(stmt));
    }

    /**
     * The rows a MERGE pairs its source against, as the transactions that have committed left them.
     *
     * <p>PostgreSQL settles a MERGE's target the way it settles the target of an UPDATE or a
     * DELETE. A row another transaction has written and not committed stands at the values it held
     * before that write, a row it has inserted is not there for a source row to pair with at all,
     * and a row it has deleted without committing still is. Reading the relation as it stands
     * instead let an in-flight change decide what the arms did: a source row was inserted against a
     * key another session was about to give back, and passed over for one it had only just taken.
     *
     * <p>A row this statement would write and another transaction is part-way through writing is
     * waited for, and the scan is read again once that transaction settles, so what the arms do
     * with the row is decided on the version it left behind -- which is what PostgreSQL re-reads
     * when a wait of its own ends. Rows nobody is writing come back as the relation stores them, so
     * an arm writes where the row really lives.
     *
     * @param owners filled with the relation storing each row, which for a partitioned or an
     *     inherited target is one of the relations below the one the statement named
     * @param asCommitted filled for the rows whose committed version is not the one stored
     * @param stayUnpaired filled with the rows the join paired with nothing before a wait
     * @param writes whether the MERGE would write a target row, judged on its committed version
     * @param pairs whether any source row pairs with a target row, judged the same way
     */
    private List<Object[]> readMergeTargetAsCommitted(Table table, List<Table> scanTables,
                                                      Map<Object[], Table> owners,
                                                      Map<Object[], Object[]> asCommitted,
                                                      Set<Object[]> stayUnpaired,
                                                      java.util.function.Predicate<Object[]> writes,
                                                      java.util.function.Predicate<Object[]> pairs) {
        Session me = executor.session;
        List<Object[]> scan = rescanTargets(scanTables, owners);
        if (me == null) return scan;
        final String key = executor.constraintValidator.uncommittedKey(table);
        boolean waited = false;
        while (true) {
            // The wait below can end without the row having moved. Polling the cancel token here
            // means a statement_timeout or a client cancel still ends this loop if it does.
            StatementCancel.check();
            asCommitted.clear();
            Set<Object[]> notCommitted = Collections.newSetFromMap(new IdentityHashMap<Object[], Boolean>());
            Map<Object[], Object[]> otherOld = new IdentityHashMap<>();
            Map<Object[], Session> owner = new IdentityHashMap<>();
            List<Object[]> hiddenByOther = new ArrayList<>();
            for (Session other : executor.database.getActiveSessions()) {
                if (other == me || !other.isInTransaction()) continue;
                // A transaction that has already failed is not waited for: its rows can never
                // become permanent, so PostgreSQL treats them as dead the instant the statement
                // errored, and waiting is how two sessions that broke each other's statement wait
                // for one another with no way out.
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
            // Nobody else is part-way through writing this relation, so the scan already holds it
            // as the committed transactions have left it and no arm need be weighed against a row
            // to find out whether there is anything to wait for.
            if (notCommitted.isEmpty() && otherOld.isEmpty() && hiddenByOther.isEmpty()) return scan;
            List<Object[]> visible = new ArrayList<>();
            Set<Object[]> seen = Collections.newSetFromMap(new IdentityHashMap<Object[], Boolean>());
            for (Object[] row : scan) {
                seen.add(row);
                if (notCommitted.contains(row)) continue;
                Object[] committed = otherOld.containsKey(row) ? otherOld.get(row) : row;
                if (committed == null) continue;
                if (committed != row) asCommitted.put(row, committed);
                visible.add(row);
            }
            // A row another transaction has deleted without committing is gone from the scan and is
            // still one of the relation's rows to everybody else, so a source row pairs with it.
            for (Object[] row : hiddenByOther) {
                if (!seen.contains(row)) visible.add(row);
            }
            Session blocker = null;
            Object[] blockedRow = null;
            for (Object[] row : visible) {
                if (!writes.test(mergeRowAsCommitted(asCommitted, row))) continue;
                Session own = owner.get(row);
                if (own != null) { blocker = own; blockedRow = row; break; }
            }
            if (blocker == null) return visible;
            // PostgreSQL settles the join once, against the relation as the statement found it, and
            // when a wait ends it asks only whether the row still pairs with the source row it was
            // paired with. A row that paired with none is therefore paired with none for the rest of
            // the statement, however the transaction being waited for leaves it: the WHEN NOT
            // MATCHED BY SOURCE arm is the one that runs for it either way.
            for (Object[] row : visible) {
                if (!pairs.test(mergeRowAsCommitted(asCommitted, row))) stayUnpaired.add(row);
            }
            // Taken while the transaction being waited for still holds its write, so each image
            // holds the version this statement is entitled to read rather than the one it waits
            // for, and kept for as long as the statement runs. Nothing is taken until a wait is
            // really about to happen.
            if (!waited) me.readImagesForStatement(key, table);
            final Session waitFor = blocker;
            executor.database.awaitConcurrentWrite(me, waitFor,
                    () -> waitFor.isInTransaction() && !waitFor.isDoomed()
                            && waitFor.hasUncommittedWork(key), table.getName());
            waited = true;
            rejectIfMergeRowMoved(waitFor, table, blockedRow);
            rejectIfMergeRowDeleted(table, blockedRow);
            scan = rescanTargets(scanTables, owners);
        }
    }

    /** The version of a target row a MERGE reads: the one the committed transactions left. */
    private static Object[] mergeRowAsCommitted(Map<Object[], Object[]> asCommitted, Object[] row) {
        if (asCommitted.isEmpty()) return row;
        Object[] committed = asCommitted.get(row);
        return committed != null ? committed : row;
    }

    /**
     * Refuse a MERGE whose row the transaction it waited for moved into another partition.
     *
     * <p>PostgreSQL will not follow a row across relations: the version the other transaction wrote
     * lives in a partition this statement never aimed at, so there is nothing left where the lock
     * was taken and the statement ends rather than passing silently over a row it was entitled to
     * act on. A MERGE says so in the same words whatever it reads from, a transaction on a snapshot
     * included: what ended it is the tuple having gone somewhere it does not look, not a write it
     * lost.
     */
    private void rejectIfMergeRowMoved(Session mover, Table table, Object[] row) {
        if (row == null || mover == null || executor.session == null) return;
        if (!mover.movedRowToAnotherPartition(row)) return;
        if (relationOrDescendantHolds(table, row)) return;
        throw new MemgresException("tuple to be locked was already moved to another partition"
                + " due to concurrent update", "40001");
    }

    /**
     * Refuse a MERGE reading from a snapshot whose row the transaction it waited for deleted.
     *
     * <p>No version of that row is left for an arm to write, and PostgreSQL will not let a
     * transaction that was shown the row carry on as though it had never been there: the source
     * row it paired with would take the NOT MATCHED arm and insert a key the relation held until a
     * moment ago. A transaction that reads each statement afresh stands differently -- by the time
     * it looks again the row really is gone, and its source row is genuinely unpaired -- so this is
     * the one thing a MERGE ends over where an ordinary read of the relation would not.
     */
    private void rejectIfMergeRowDeleted(Table table, Object[] row) {
        if (row == null || executor.session == null) return;
        String isolation = executor.session.getEffectiveIsolationLevel();
        if (!"repeatable read".equals(isolation) && !"serializable".equals(isolation)) return;
        if (relationOrDescendantHolds(table, row)) return;
        throw new MemgresException(
                "could not serialize access due to concurrent delete", "40001");
    }

    /**
     * Whether a MERGE would write a target row: some source row pairs with it and the first WHEN
     * MATCHED arm that applies acts, or none pairs with it and the first WHEN NOT MATCHED BY SOURCE
     * arm that applies acts.
     *
     * <p>PostgreSQL takes no lock for a row it will not write, so a DO NOTHING arm and an arm whose
     * condition the row fails leave the row to be read and nothing more -- there is no transaction
     * to wait for on account of it.
     */
    private boolean mergeWritesTargetRow(MergeStmt stmt, Table targetTable, String targetAlias,
                                         List<RowContext> sourceRows, Object[] targetRow,
                                         boolean targetHasVirtual, Set<String> targetReads) {
        Object[] evalRow = targetHasVirtual
                ? computeVirtualColumns(targetTable, targetRow, targetReads) : targetRow;
        RowContext targetCtx = viewAwareCtx(targetTable, targetAlias, evalRow);
        for (RowContext sourceCtx : sourceRows) {
            RowContext combined = targetCtx.merge(sourceCtx);
            if (!executor.isTruthy(executor.evalExpr(stmt.onCondition(), combined))) continue;
            for (MergeStmt.WhenClause clause : stmt.whenClauses()) {
                if (!(clause instanceof MergeStmt.WhenMatched)) continue;
                MergeStmt.WhenMatched wm = (MergeStmt.WhenMatched) clause;
                if (wm.andCondition() != null
                        && !executor.isTruthy(executor.evalExpr(wm.andCondition(), combined))) {
                    continue;
                }
                return wm.isDelete() || (wm.setClauses() != null && !wm.setClauses().isEmpty());
            }
            return false;
        }
        for (MergeStmt.WhenClause clause : stmt.whenClauses()) {
            if (!(clause instanceof MergeStmt.WhenNotMatchedBySource)) continue;
            MergeStmt.WhenNotMatchedBySource ws = (MergeStmt.WhenNotMatchedBySource) clause;
            if (ws.andCondition() != null
                    && !executor.isTruthy(executor.evalExpr(ws.andCondition(), targetCtx))) {
                continue;
            }
            return ws.isDelete() || (ws.setClauses() != null && !ws.setClauses().isEmpty());
        }
        return false;
    }

    /**
     * Take the row lock a MERGE's arm holds over the row it is about to write.
     *
     * <p>PostgreSQL locks a row it writes through a MERGE exactly as it locks one it writes through
     * an UPDATE or a DELETE, so a concurrent FOR UPDATE NOWAIT reports that it could not have it. An
     * arm that writes nothing takes nothing: a DO NOTHING arm leaves the row free.
     */
    private void lockMergeRow(Table targetTable, Object[] targetRow) {
        lockRowsForDml(targetTable, Collections.singletonList(targetRow));
    }

    /** Whether any source row pairs with a target row under the MERGE's ON condition. */
    private boolean mergeSourcePairsWith(MergeStmt stmt, Table targetTable, String targetAlias,
                                         List<RowContext> sourceRows, Object[] targetRow,
                                         boolean targetHasVirtual, Set<String> targetReads) {
        Object[] evalRow = targetHasVirtual
                ? computeVirtualColumns(targetTable, targetRow, targetReads) : targetRow;
        RowContext targetCtx = viewAwareCtx(targetTable, targetAlias, evalRow);
        for (RowContext sourceCtx : sourceRows) {
            if (executor.isTruthy(executor.evalExpr(stmt.onCondition(), targetCtx.merge(sourceCtx)))) {
                return true;
            }
        }
        return false;
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
        // A MERGE writes, and PostgreSQL refuses a write in a read-only transaction before it knows
        // whether any WHEN clause would have fired: a MERGE whose every arm turns out inert is
        // refused just the same.
        checkReadOnly("MERGE");
        String schemaName = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        rejectMaterializedViewWrite(stmt.targetTable());
        // A MERGE onto a view that cannot take it is refused by the action it wanted to perform,
        // not by INSERT: a MERGE whose only arm is an UPDATE reports "cannot update view". No rule
        // can stand in for a MERGE, so the advice it is given names a trigger and nothing else.
        executor.viewDmlVerb = mergeViewDmlVerb(stmt);
        executor.viewDmlByMerge = true;
        // A WITH CHECK OPTION is written on the view, so it has to be read off the name the
        // statement gave, before resolveTable replaces it with the base relation underneath.
        List<DmlValidationHelper.ViewCheck> viewCheckExprs =
                validationHelper.collectViewCheckExprs(stmt.targetTable());
        recordRelationLock(stmt.schema(), stmt.targetTable(), "RowExclusiveLock");
        rejectCatalogViewWrite(stmt.targetTable(), executor.viewDmlVerb);
        // H35: honor explicit schema qualifier so a same-named temp table cannot shadow it
        Table targetTable = executor.resolveTable(schemaName, stmt.targetTable(), stmt.schema() != null);
        // Capture the view column mapping before a later resolveTable clobbers it, so a MERGE
        // through a view whose columns are renamed reads and writes them as an UPDATE does.
        captureViewTarget();
        activeViewWriteTarget = executor.database.getView(schemaName, stmt.targetTable());
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

        checkMergeWhenClauses(stmt, targetTable, targetAlias);
        // Every name an arm writes to, and every name it reads off the target, is resolved here --
        // where PostgreSQL resolves it, while it is still analysing the statement. Leaving it to
        // the per-row evaluator meant a MERGE whose arms never fired quietly reported success.
        checkMergeTargetNames(stmt, targetTable, targetAlias);
        // PostgreSQL does not rewrite a MERGE at all, so a relation carrying a rule cannot take
        // one: the rewriter refuses the statement outright and blames the relation as written.
        rejectMergeOnRuledRelation(stmt, targetTable);
        // The actions the WHEN clauses can perform decide both the rights the statement needs and
        // which statement-level triggers fire, so work them out once.
        List<PgTrigger.Event> mergeEvents = mergeStatementEvents(stmt);
        checkMergePrivileges(stmt, schemaName, mergeEvents);
        // Check table-level locks (blocks if ACCESS EXCLUSIVE held by another session)
        executor.database.checkTableLockForDml(schemaName + "." + stmt.targetTable(), executor.session);
        if (mergeEvents.contains(PgTrigger.Event.UPDATE)) {
            checkReplicaIdentity(targetTable, stmt.targetTable(), "update");
        } else if (mergeEvents.contains(PgTrigger.Event.DELETE)) {
            checkReplicaIdentity(targetTable, stmt.targetTable(), "delete");
        }

        // Validate RETURNING columns
        validateReturning(stmt.returning(), targetTable,
                Collections.singletonList(stmt.source()));
        if (mergeEvents.contains(PgTrigger.Event.INSERT)) {
            rejectSystemColumnsInRoutedInsert(stmt.returning(), targetTable, stmt.targetAlias());
        }

        List<PgTrigger> triggers = enabledTriggers(rowTriggersFor(targetTable, stmt.targetTable()));
        // A row trigger is a trigger of the relation the row is stored in, so a MERGE against a
        // partitioned table fires the triggers of the partition each row lives in or goes to.
        Map<Table, List<PgTrigger>> mergeRowTriggers = rowTriggersByRelation(targetTable, triggers);
        // Which columns the statement assigns is settled over the whole of it, arm by arm, because
        // that is how PostgreSQL settles it: a trigger written UPDATE OF fires for every row an arm
        // wrote as soon as any arm names one of the columns it was written for, and not only for
        // the rows written by the arm that named it.
        Set<String> mergeAssignedCols = mergeAssignedColumns(stmt);
        // PostgreSQL fires the BEFORE STATEMENT trigger of every action the MERGE could perform,
        // before a single row is read -- both the INSERT and the UPDATE trigger of a two-armed
        // MERGE, whether or not a row ends up taking each arm.
        for (PgTrigger.Event event : mergeEvents) {
            triggerHelper.fireStatementTriggers(triggers, PgTrigger.Timing.BEFORE, event, targetTable, null, null);
        }

        // Resolve source rows
        // The ON condition is what the scan of the source has to have worked out as it passes over
        // a row, because it decides whether the row is paired at all. What an arm reads of the
        // source stands above the join, so PostgreSQL puts a VIRTUAL generated column's expression
        // there and works it out for the rows that arm runs for -- see mergeMatchedReads below.
        //
        // A statement with a WHEN NOT MATCHED BY SOURCE arm that acts is the other case. Every
        // target row has to be answered then, whether or not a source row paired with it, so
        // PostgreSQL preserves the target and the source becomes the side that may be padded away
        // -- and a column generated from a source row cannot be worked out above such a join,
        // because a target row that paired with nothing carries no source row at all. Every one of
        // them is therefore worked out as the source is scanned, named by the statement or not,
        // and only what the ON condition says about the source on its own narrows that scan: an
        // equality with the target says nothing there, the two sides of an outer join not standing
        // in one class.
        // The ON condition settles whether the source is read at all, unless an arm of the
        // statement still has to answer for a source row that paired with nothing.
        final Expression mergeSourcePairing =
                mergeAnswersForUnpairedSource(stmt) ? null : stmt.onCondition();
        final java.util.function.Supplier<List<RowContext>> readMergeSource =
                () -> executor.fromResolver.resolveMergeSource(stmt.source(), mergeSourcePairing);
        List<RowContext> sourceRows = mergeSourceIsNullable(stmt)
                ? readRelationBelowJoin(readMergeSource, stmt.onCondition())
                : readExtraRelation(readMergeSource,
                        new Object[]{stmt.onCondition(), stmt.whenClauses(), stmt.returning(),
                                stmt.source()},
                        new Object[]{stmt.onCondition(), stmt.source()}, null);
        // RETURNING reads the source through the context the source produced rather than through a
        // resolved relation: a VALUES list is a source like any other and has none behind it.
        RowContext mergeSourceSample = sourceRows.isEmpty() ? null : sourceRows.get(0);
        rejectAmbiguousReturning(stmt.returning(), targetTable, namesBesideTarget(
                Collections.singletonList(stmt.source()), mergeSourceSample));
        // WHEN NOT MATCHED BY SOURCE fires for a target row no source row paired with, and
        // RETURNING still reads the source columns there -- as nulls, which is what PG answers.
        RowContext mergeNullSourceCtx = nullSourceContext(mergeSourceSample);
        String mergeSrcAlias = mergeSourceAlias(stmt.source());

        final boolean mergeTargetHasVirtual = hasVirtualColumns(targetTable);
        // A VIRTUAL generated column is not stored: PostgreSQL rewrites a reference to one into its
        // generation expression, so a MERGE works out the ones it names and no others.
        final Set<String> mergeTargetReads = mergeTargetHasVirtual
                ? columnsNamed(targetTable, targetAlias, stmt.onCondition(), stmt.whenClauses(),
                        stmt.returning())
                : null;
        // The rows of a partitioned relation live in its leaves, so the scan that pairs source rows
        // with target rows has to walk them the way UPDATE and DELETE do. Reading the parent alone
        // left every MATCHED arm inert and turned every source row into an INSERT.
        List<Table> scanTables = collectTargetTables(targetTable);
        Map<Object[], Table> mergeRowOwner = new IdentityHashMap<>();
        // Which version of each row this statement is entitled to read, where that is not the one
        // the relation stores because another transaction is part-way through writing it.
        Map<Object[], Object[]> mergeAsCommitted = new IdentityHashMap<>();
        // Target rows the join paired with nothing, kept out of it for the rest of the statement.
        Set<Object[]> mergeStayUnpaired = Collections.newSetFromMap(new IdentityHashMap<Object[], Boolean>());
        List<Object[]> originalTargetRows = readMergeTargetAsCommitted(targetTable, scanTables,
                mergeRowOwner, mergeAsCommitted, mergeStayUnpaired,
                row -> mergeWritesTargetRow(stmt, targetTable, targetAlias, sourceRows, row,
                        mergeTargetHasVirtual, mergeTargetReads),
                row -> mergeSourcePairsWith(stmt, targetTable, targetAlias, sourceRows, row,
                        mergeTargetHasVirtual, mergeTargetReads));
        // A transaction reading from a snapshot was shown a row that another transaction has since
        // deleted and committed. There is no version of it left for an arm to write, and passing
        // over it would take the arm's action for the source row instead -- inserting a key the
        // relation is about to hold again -- so PostgreSQL ends the statement.
        if (executor.session != null && scanTables.size() == 1) {
            executor.session.checkRRConcurrentDelete(schemaName + "." + stmt.targetTable(),
                    targetTable, image -> mergeWritesTargetRow(stmt, targetTable, targetAlias,
                            sourceRows, image, mergeTargetHasVirtual, mergeTargetReads));
        }

        // Row-level security decides what the join may see as much as what it may write: a row no
        // SELECT policy admits is genuinely NOT MATCHED, and the NOT MATCHED arm runs for it.
        boolean mergeRlsActive = targetTable.isRlsEnabled()
                && !executor.shouldBypassRls(targetTable, schemaName);
        if (mergeRlsActive) {
            originalTargetRows = filterRowsByRlsUsing(targetTable, originalTargetRows, "SELECT", targetAlias);
        }

        // Snapshot every table the statement can write to, so a failure part-way rolls the whole
        // MERGE back. A partitioned target holds nothing in the parent, and restoring the parent
        // alone left behind every row the INSERT arm had already routed into a leaf.
        Map<Table, List<RowImage>> mergeSnapshot = snapshotTargetTables(scanTables);

        int mergeCount = 0;
        boolean hasReturning = stmt.returning() != null && !stmt.returning().isEmpty();
        List<Object[]> returningRows = hasReturning ? new ArrayList<>() : null;

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
        // What each action actually touched: the AFTER STATEMENT triggers read these as their
        // transition tables, and the per-relation tuple counters are raised from them.
        List<Object[]> mergeUpdatedOld = new ArrayList<>();
        List<Object[]> mergeUpdatedNew = new ArrayList<>();
        List<Object[]> mergeDeletedOld = new ArrayList<>();
        // Every AFTER row trigger the arms earn, held back until the statement has finished
        // writing: PostgreSQL runs the whole of a MERGE and only then runs what it queued, so a
        // trigger that reads the target reads what the statement left there rather than what
        // stood there part-way through the scan.
        List<MergeAfterRow> mergeAfterRows = new ArrayList<MergeAfterRow>();

        // What each kind of arm reads of the source. An arm's condition, its assignments and the
        // statement's RETURNING all stand above the join, so a VIRTUAL generated column of the
        // source is worked out for the source rows that arm runs for and for no others. Working
        // every one out as the source was scanned raised, for a row no arm ever reads, an error
        // the statement never asked for.
        List<Object> mergeMatchedReads = new ArrayList<>();
        List<Object> mergeUnmatchedReads = new ArrayList<>();
        for (MergeStmt.WhenClause clause : stmt.whenClauses()) {
            if (clause instanceof MergeStmt.WhenNotMatched) mergeUnmatchedReads.add(clause);
            else if (clause instanceof MergeStmt.WhenMatched) mergeMatchedReads.add(clause);
        }
        if (!mergeMatchedReads.isEmpty()) mergeMatchedReads.add(stmt.returning());
        if (!mergeUnmatchedReads.isEmpty()) mergeUnmatchedReads.add(stmt.returning());
        // Collect unmatched source rows for deferred NOT MATCHED BY TARGET processing
        List<RowContext> unmatchedSourceRows = new ArrayList<>();
        try {
        for (RowContext sourceCtx : sourceRows) {
            // Find matching target rows for this source row using the original snapshot
            List<Object[]> matchedTargetRows = new ArrayList<>();
            for (Object[] targetRow : originalTargetRows) {
                if (mergeStayUnpaired.contains(targetRow)) continue;
                Object[] asRead = mergeRowAsCommitted(mergeAsCommitted, targetRow);
                Object[] evalRow = mergeTargetHasVirtual
                        ? computeVirtualColumns(targetTable, asRead, mergeTargetReads) : asRead;
                RowContext targetCtx = viewAwareCtx(targetTable, targetAlias, evalRow);
                RowContext combined = targetCtx.merge(sourceCtx);
                if (executor.isTruthy(executor.evalExpr(stmt.onCondition(), combined))) {
                    matchedBySourceRows.add(targetRow);
                    // PG 21000: only a second real modification of the row is an error
                    if (affectedTargetRows.contains(targetRow)) {
                        // PostgreSQL points at the source, which is where the second match came
                        // from, rather than at the row that was hit twice.
                        throw new MemgresException(
                                "MERGE command cannot affect row a second time"
                                        + "\n  Hint: Ensure that not more than one source row"
                                        + " matches any one target row.", "21000");
                    }
                    matchedTargetRows.add(targetRow);
                }
            }

            if (!matchedTargetRows.isEmpty()) {
                fillJoinedVirtuals(Collections.singletonList(sourceCtx), mergeMatchedReads);
                // WHEN MATCHED clauses
                for (Object[] targetRow : matchedTargetRows) {
                    if (processedTargetRows.contains(targetRow)) continue;
                    Object[] asRead = mergeRowAsCommitted(mergeAsCommitted, targetRow);
                    Object[] evalRow = mergeTargetHasVirtual
                            ? computeVirtualColumns(targetTable, asRead, mergeTargetReads) : asRead;
                    RowContext targetCtx = viewAwareCtx(targetTable, targetAlias, evalRow);
                    RowContext combined = targetCtx.merge(sourceCtx);

                    for (MergeStmt.WhenClause clause : stmt.whenClauses()) {
                        if (clause instanceof MergeStmt.WhenMatched) {
                            MergeStmt.WhenMatched wm = (MergeStmt.WhenMatched) clause;
                            // Check optional AND condition
                            if (wm.andCondition() != null && !executor.isTruthy(executor.evalExpr(wm.andCondition(), combined))) {
                                continue;
                            }
                            if (wm.isDelete()) {
                                requireMergeRowVisible(mergeRlsActive, targetTable, targetRow, "DELETE", targetAlias);
                                lockMergeRow(targetTable, targetRow);
                                // A row this arm deletes is deleted the way a DELETE of its own
                                // would delete it, so the DELETE row triggers of the relation that
                                // stores it fire for it. A BEFORE DELETE that answers with nothing
                                // keeps the row where it is: nothing is deleted and nothing counted.
                                Table deletedIn = mergeRowOwner.get(targetRow);
                                if (deletedIn == null) deletedIn = targetTable;
                                if (triggerHelper.executeTriggers(
                                        rowTriggersIn(mergeRowTriggers, deletedIn, triggers),
                                        PgTrigger.Timing.BEFORE, PgTrigger.Event.DELETE, null,
                                        targetRow, deletedIn) == null) {
                                    processedTargetRows.add(targetRow);
                                    break;
                                }
                                // DELETE — collect RETURNING before marking for deletion
                                if (hasReturning) {
                                    executor.currentMergeAction = "DELETE";
                                    returningRows.add(evalMergeReturning(stmt.returning(), targetTable, targetAlias,
                                            targetRow, targetRow, null, sourceCtx, mergeSrcAlias));
                                }
                                executor.constraintValidator.handleFkOnDelete(targetTable, targetRow);
                                rowsToDelete.add(targetRow);
                                takeMergeRow(stmt, deletedIn, targetRow);
                                mergeDeletedOld.add(Arrays.copyOf(targetRow, targetRow.length));
                                mergeAfterRows.add(new MergeAfterRow(PgTrigger.Event.DELETE, deletedIn,
                                        null, Arrays.copyOf(targetRow, targetRow.length), null));
                                processedTargetRows.add(targetRow);
                                affectedTargetRows.add(targetRow);
                                mergeCount++;
                            } else if (wm.setClauses() != null && !wm.setClauses().isEmpty()) {
                                requireMergeRowVisible(mergeRlsActive, targetTable, targetRow, "UPDATE", targetAlias);
                                lockMergeRow(targetTable, targetRow);
                                // UPDATE — evaluate all SET RHS against original row snapshot onto a
                                // working copy, then let BEFORE triggers see/modify it before committing.
                                Object[] oldRow = Arrays.copyOf(targetRow, targetRow.length);
                                Object[] newRow = Arrays.copyOf(targetRow, targetRow.length);
                                Set<String> updCols = mergeAssignedCols;
                                // The assignment machinery the UPDATE path uses, so a MERGE resolves
                                // a view's renamed columns and understands SET col = DEFAULT too.
                                applySetClauses(wm.setClauses(), targetTable, newRow, combined);
                                if (mergeRlsActive) {
                                    enforceRlsWithCheck(targetTable, newRow, "UPDATE");
                                }
                                // Fire BEFORE UPDATE row triggers: PG fires them per matched row,
                                // as triggers of the relation that row is stored in. They may
                                // modify NEW or return NULL to skip the row entirely.
                                Table matchedIn = mergeRowOwner.get(targetRow);
                                if (matchedIn == null) matchedIn = targetTable;
                                List<PgTrigger> matchedTriggers =
                                        rowTriggersIn(mergeRowTriggers, matchedIn, triggers);
                                newRow = triggerHelper.executeTriggers(matchedTriggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.UPDATE, newRow, oldRow, matchedIn, updCols);
                                if (newRow == null) {
                                    // BEFORE trigger suppressed this row: no update, not counted.
                                    processedTargetRows.add(targetRow);
                                    break;
                                }
                                // Commit the (possibly trigger-modified) new values onto the live row.
                                // The new values are checked before they are written, and written
                                // through the table so its indexes move with them. Copying them
                                // straight onto the live row left every index pointing at the
                                // values the row used to hold.
                                computeGeneratedColumns(targetTable, newRow);
                                validationHelper.enforceViewCheckOption(viewCheckExprs, targetTable, newRow);
                                executor.constraintValidator.validateConstraints(targetTable, newRow, targetRow);
                                validationHelper.validateDomainChecks(newRow, targetTable);
                                Object[] writtenRow = writeMergeUpdate(stmt, targetTable, mergeRowOwner,
                                        targetRow, oldRow, newRow, mergeRowTriggers, triggers,
                                        mergeAfterRows, updCols);
                                // The assignment moved the row to another partition and one of the
                                // two relations' own row triggers stopped it going, so there is
                                // nothing left to count or to report for it.
                                if (writtenRow == null || writtenRow == PARTITION_MOVE_REFUSED) {
                                    processedTargetRows.add(targetRow);
                                    break;
                                }
                                executor.constraintValidator.handleFkOnUpdate(targetTable, oldRow, writtenRow);
                                // Collect RETURNING after update (uses new values)
                                if (hasReturning) {
                                    executor.currentMergeAction = "UPDATE";
                                    returningRows.add(evalMergeReturning(stmt.returning(), targetTable, targetAlias,
                                            writtenRow, oldRow, writtenRow, sourceCtx, mergeSrcAlias));
                                }
                                mergeUpdatedOld.add(oldRow);
                                mergeUpdatedNew.add(Arrays.copyOf(writtenRow, writtenRow.length));
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
                    Object[] asRead = mergeRowAsCommitted(mergeAsCommitted, targetRow);
                    Object[] evalRow = mergeTargetHasVirtual
                            ? computeVirtualColumns(targetTable, asRead, mergeTargetReads) : asRead;
                    RowContext targetCtx = viewAwareCtx(targetTable, targetAlias, evalRow);
                    for (MergeStmt.WhenClause clause : stmt.whenClauses()) {
                        if (clause instanceof MergeStmt.WhenNotMatchedBySource) {
                            MergeStmt.WhenNotMatchedBySource wnmbs = (MergeStmt.WhenNotMatchedBySource) clause;
                            if (wnmbs.andCondition() != null && !executor.isTruthy(executor.evalExpr(wnmbs.andCondition(), targetCtx))) {
                                continue;
                            }
                            // The source row RETURNING reads here is all nulls: this arm fires
                            // precisely because no source row paired with the target row.
                            RowContext nullSourceCtx = mergeNullSourceCtx;
                            if (wnmbs.isDelete()) {
                                requireMergeRowVisible(mergeRlsActive, targetTable, targetRow, "DELETE", targetAlias);
                                lockMergeRow(targetTable, targetRow);
                                // The DELETE row triggers of the relation that stores the row, as
                                // for the arm above: a BEFORE DELETE that answers with nothing
                                // keeps the row and this arm has done nothing to it.
                                Table goneFrom = mergeRowOwner.get(targetRow);
                                if (goneFrom == null) goneFrom = targetTable;
                                if (triggerHelper.executeTriggers(
                                        rowTriggersIn(mergeRowTriggers, goneFrom, triggers),
                                        PgTrigger.Timing.BEFORE, PgTrigger.Event.DELETE, null,
                                        targetRow, goneFrom) == null) {
                                    processedTargetRows.add(targetRow);
                                    break;
                                }
                                if (hasReturning) {
                                    executor.currentMergeAction = "DELETE";
                                    returningRows.add(evalMergeReturning(stmt.returning(), targetTable, targetAlias,
                                            targetRow, targetRow, null, nullSourceCtx, mergeSrcAlias));
                                }
                                executor.constraintValidator.handleFkOnDelete(targetTable, targetRow);
                                rowsToDelete.add(targetRow);
                                takeMergeRow(stmt, goneFrom, targetRow);
                                mergeDeletedOld.add(Arrays.copyOf(targetRow, targetRow.length));
                                mergeAfterRows.add(new MergeAfterRow(PgTrigger.Event.DELETE, goneFrom,
                                        null, Arrays.copyOf(targetRow, targetRow.length), null));
                                mergeCount++;
                            } else if (wnmbs.setClauses() != null && !wnmbs.setClauses().isEmpty()) {
                                requireMergeRowVisible(mergeRlsActive, targetTable, targetRow, "UPDATE", targetAlias);
                                lockMergeRow(targetTable, targetRow);
                                Object[] oldRow = Arrays.copyOf(targetRow, targetRow.length);
                                Object[] newRow = Arrays.copyOf(targetRow, targetRow.length);
                                Set<String> updCols = mergeAssignedCols;
                                applySetClauses(wnmbs.setClauses(), targetTable, newRow, targetCtx);
                                if (mergeRlsActive) {
                                    enforceRlsWithCheck(targetTable, newRow, "UPDATE");
                                }
                                // Fire BEFORE UPDATE row triggers (may modify NEW or skip via
                                // NULL), as triggers of the relation the row is stored in.
                                Table bySourceIn = mergeRowOwner.get(targetRow);
                                if (bySourceIn == null) bySourceIn = targetTable;
                                List<PgTrigger> bySourceTriggers =
                                        rowTriggersIn(mergeRowTriggers, bySourceIn, triggers);
                                newRow = triggerHelper.executeTriggers(bySourceTriggers, PgTrigger.Timing.BEFORE, PgTrigger.Event.UPDATE, newRow, oldRow, bySourceIn, updCols);
                                if (newRow == null) {
                                    // BEFORE trigger suppressed this row: no update, not counted.
                                    processedTargetRows.add(targetRow);
                                    break;
                                }
                                // The new values are checked before they are written, and written
                                // through the table so its indexes move with them. Copying them
                                // straight onto the live row left every index pointing at the
                                // values the row used to hold.
                                computeGeneratedColumns(targetTable, newRow);
                                validationHelper.enforceViewCheckOption(viewCheckExprs, targetTable, newRow);
                                executor.constraintValidator.validateConstraints(targetTable, newRow, targetRow);
                                validationHelper.validateDomainChecks(newRow, targetTable);
                                Object[] writtenRow = writeMergeUpdate(stmt, targetTable, mergeRowOwner,
                                        targetRow, oldRow, newRow, mergeRowTriggers, triggers,
                                        mergeAfterRows, updCols);
                                if (writtenRow == null || writtenRow == PARTITION_MOVE_REFUSED) {
                                    processedTargetRows.add(targetRow);
                                    break;
                                }
                                executor.constraintValidator.handleFkOnUpdate(targetTable, oldRow, writtenRow);
                                if (hasReturning) {
                                    executor.currentMergeAction = "UPDATE";
                                    returningRows.add(evalMergeReturning(stmt.returning(), targetTable, targetAlias,
                                            writtenRow, oldRow, writtenRow, nullSourceCtx, mergeSrcAlias));
                                }
                                mergeUpdatedOld.add(oldRow);
                                mergeUpdatedNew.add(Arrays.copyOf(writtenRow, writtenRow.length));
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
            fillJoinedVirtuals(unmatchedSourceRows, mergeUnmatchedReads);
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
                                // A view names its own columns, and a MERGE through one writes to
                                // the columns of the table underneath.
                                int colIdx = targetTable.getColumnIndex(mapViewColumn(wnm.columns().get(i)));
                                if (colIdx < 0) {
                                    throw new MemgresException("column \"" + wnm.columns().get(i)
                                            + "\" of relation \"" + stmt.targetTable() + "\" does not exist", "42703");
                                }
                                // DEFAULT asks for the column's default, which fillDefaults has
                                // already put there. It is also the one value a column the system
                                // computes may be given, so nothing is evaluated for it.
                                if (isDefaultLiteral(wnm.values().get(i))) continue;
                                Object val = executor.evalExpr(wnm.values().get(i), sourceCtx);
                                newRow[colIdx] = validationHelper.storedValue(val, targetTable.getColumns().get(colIdx));
                            }
                        } else if (wnm.values() != null) {
                            for (int i = 0; i < wnm.values().size() && i < newRow.length; i++) {
                                if (isDefaultLiteral(wnm.values().get(i))) continue;
                                Object val = executor.evalExpr(wnm.values().get(i), sourceCtx);
                                newRow[i] = validationHelper.storedValue(val, targetTable.getColumns().get(i));
                            }
                        }
                        computeGeneratedColumns(targetTable, newRow);
                        // Fire BEFORE INSERT row triggers: PG fires them per inserted row, as
                        // triggers of the partition the row routes to. They may modify NEW or
                        // return NULL to skip the row entirely.
                        Table insertInto = partitionHelper.routeForInsert(targetTable, newRow);
                        newRow = triggerHelper.executeTriggers(
                                rowTriggersIn(mergeRowTriggers, insertInto, triggers),
                                PgTrigger.Timing.BEFORE, PgTrigger.Event.INSERT, newRow, null, insertInto);
                        if (newRow == null) {
                            // BEFORE trigger suppressed this row: not inserted, not counted.
                            break;
                        }
                        // A row written through a view has to satisfy the view's check option, and
                        // a row written under row-level security its WITH CHECK policies, exactly
                        // as an ordinary INSERT's does.
                        validationHelper.enforceViewCheckOption(viewCheckExprs, targetTable, newRow);
                        if (mergeRlsActive) {
                            enforceRlsWithCheck(targetTable, newRow, "INSERT");
                        }
                        validationHelper.validateEnumValues(newRow, targetTable);
                        Table routedTable =
                                partitionHelper.settledPartition(targetTable, insertInto, newRow);
                        // A key another session holds but has not committed is not taken yet and
                        // may never be: PostgreSQL waits for that transaction to settle rather than
                        // deciding against a row nobody may end up holding, and then reports the
                        // duplicate or stores the row on what it finds. The wait cannot be taken
                        // under the relation's lock, because rolling that transaction back needs it.
                        while (true) {
                            StatementCancel.check();
                            ConstraintValidator.PendingUniqueConflict pending;
                            routedTable.getWriteLock().lock();
                            try {
                                pending = executor.constraintValidator
                                        .findUncommittedUniqueConflict(routedTable, newRow, null);
                                if (pending == null) {
                                    // A partition is stored as an ordinary table whose bound nothing
                                    // else on this path consults, so a MERGE naming one is tested
                                    // against it.
                                    partitionHelper.checkPartitionConstraint(routedTable, newRow);
                                    try {
                                        executor.constraintValidator.validateConstraints(targetTable, newRow, null);
                                    } catch (MemgresException refused) {
                                        spendTupleIdOnFailedWrite(routedTable, refused);
                                        throw refused;
                                    }
                                    validationHelper.validateDomainChecks(newRow, targetTable);
                                    routedTable.insertRow(newRow);
                                    try {
                                        executor.constraintValidator.validateConstraints(routedTable, newRow, newRow);
                                    } catch (MemgresException e) {
                                        routedTable.removeRow(newRow);
                                        throw e;
                                    }
                                }
                            } finally {
                                routedTable.getWriteLock().unlock();
                            }
                            if (pending == null) break;
                            awaitPendingInsert(routedTable, pending);
                        }
                        recordRowMeta(stmt.schema(), routedTable, newRow);
                        rowsToInsert.add(newRow);
                        mergeAfterRows.add(new MergeAfterRow(PgTrigger.Event.INSERT, routedTable,
                                Arrays.copyOf(newRow, newRow.length), null, null));
                        if (hasReturning) {
                            executor.currentMergeAction = "INSERT";
                            returningRows.add(evalMergeReturning(stmt.returning(), targetTable, targetAlias,
                                    newRow, null, newRow, sourceCtx, mergeSrcAlias));
                        }
                        mergeCount++;
                        break;
                    }
                }
            }

            // Every row an arm deleted has already gone from the relation that stores it, where
            // PostgreSQL takes it out. What is left here is a sweep of the relations below a
            // partitioned or inherited target, for a row the scan could name no relation for.
            if (!rowsToDelete.isEmpty()) {
                for (Table part : scanTables) {
                    part.deleteRows(rowsToDelete);
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

            // Everything the arms queued, in the order the rows were written and with the writing
            // all done -- which is where PostgreSQL runs the AFTER row triggers of a statement.
            for (MergeAfterRow pending : mergeAfterRows) {
                triggerHelper.executeTriggers(
                        rowTriggersIn(mergeRowTriggers, pending.relation, triggers),
                        PgTrigger.Timing.AFTER, pending.event, pending.newRow, pending.oldRow,
                        pending.relation, pending.assigned);
            }

            // The AFTER STATEMENT trigger of each action the arms could perform, holding the rows
            // that action actually touched -- the same pairing the other three paths use.
            for (PgTrigger.Event event : mergeEvents) {
                if (event == PgTrigger.Event.INSERT) {
                    triggerHelper.fireStatementTriggers(triggers, PgTrigger.Timing.AFTER, event, targetTable, rowsToInsert, null);
                } else if (event == PgTrigger.Event.UPDATE) {
                    triggerHelper.fireStatementTriggers(triggers, PgTrigger.Timing.AFTER, event, targetTable, mergeUpdatedNew, mergeUpdatedOld);
                } else {
                    triggerHelper.fireStatementTriggers(triggers, PgTrigger.Timing.AFTER, event, targetTable, null, mergeDeletedOld);
                }
            }

            // Track DML statistics
            if (!rowsToInsert.isEmpty()) targetTable.incrementTupInserted(rowsToInsert.size());
            if (!mergeUpdatedNew.isEmpty()) targetTable.incrementTupUpdated(mergeUpdatedNew.size());
            if (!mergeDeletedOld.isEmpty()) targetTable.incrementTupDeleted(mergeDeletedOld.size());
        } catch (MemgresException e) {
            // MERGE is atomic; roll back all changes (updates, deletes, inserts) on failure. Every
            // leaf is restored, not only the parent: the INSERT arm routes rows into the leaves.
            executor.currentMergeAction = null;
            restoreTargetTables(mergeSnapshot);
            // The table is back as it was, so the statement's undo entries describe changes that
            // have already been reversed; replaying them would write the same rows twice. What the
            // statement wrote anywhere else keeps its entries, because a failed statement has to
            // be undone there too.
            if (executor.session != null) {
                executor.session.discardUndoForRelations(mergeSnapshot.keySet());
            }
            throw e;
        }

        executor.currentMergeAction = null;
        if (hasReturning) {
            List<Column> retCols = buildMergeReturningColumns(stmt.returning(), targetTable, targetAlias,
                    mergeSourceSample, mergeSrcAlias);
            return QueryResult.returning(QueryResult.Type.MERGE, retCols, returningRows, mergeCount);
        }
        return QueryResult.command(QueryResult.Type.MERGE, mergeCount);
    }

    /**
     * Take a row an arm has decided to delete out of the relation that stores it, now rather than
     * when the statement ends.
     *
     * <p>PostgreSQL deletes each row where the arm acts on it, so a BEFORE DELETE trigger that
     * reads the target finds the rows the arms have already taken gone -- the second row of a
     * two-row arm is told the first has left. Collecting the rows and taking them all out once the
     * arms had finished showed every trigger the relation as the scan had found it.
     */
    private void takeMergeRow(MergeStmt stmt, Table storedIn, Object[] row) {
        // What an arm deletes is not weighed against the snapshot a transaction reads from, for
        // the reason writeMergeUpdate gives.
        mergeWritesRowItMatched = true;
        try {
            recordDeleteUndo(stmt.schema(), stmt.targetTable(), Collections.singletonList(row));
        } finally {
            mergeWritesRowItMatched = false;
        }
        storedIn.deleteRow(row);
    }

    /** Whether an expression is the DEFAULT keyword, which asks for the column's own default. */
    private static boolean isDefaultLiteral(Expression expr) {
        return expr instanceof Literal && ((Literal) expr).literalType() == Literal.LiteralType.DEFAULT;
    }

    /**
     * The actions a MERGE's WHEN clauses can perform, in the order PostgreSQL considers them. A DO
     * NOTHING arm performs none: it needs no privilege and fires no statement trigger.
     */
    private List<PgTrigger.Event> mergeStatementEvents(MergeStmt stmt) {
        boolean inserts = false;
        boolean updates = false;
        boolean deletes = false;
        for (MergeStmt.WhenClause clause : stmt.whenClauses()) {
            if (clause instanceof MergeStmt.WhenMatched) {
                MergeStmt.WhenMatched wm = (MergeStmt.WhenMatched) clause;
                if (wm.isDelete()) deletes = true;
                else if (wm.setClauses() != null && !wm.setClauses().isEmpty()) updates = true;
            } else if (clause instanceof MergeStmt.WhenNotMatchedBySource) {
                MergeStmt.WhenNotMatchedBySource ws = (MergeStmt.WhenNotMatchedBySource) clause;
                if (ws.isDelete()) deletes = true;
                else if (ws.setClauses() != null && !ws.setClauses().isEmpty()) updates = true;
            } else if (clause instanceof MergeStmt.WhenNotMatched) {
                if (!((MergeStmt.WhenNotMatched) clause).doNothing()) inserts = true;
            }
        }
        List<PgTrigger.Event> events = new ArrayList<>();
        if (inserts) events.add(PgTrigger.Event.INSERT);
        if (updates) events.add(PgTrigger.Event.UPDATE);
        if (deletes) events.add(PgTrigger.Event.DELETE);
        return events;
    }

    /**
     * The rights a MERGE needs: SELECT on the target, because it reads the target to decide which
     * rows the ON condition pairs, and then one right for each kind of action its arms can perform.
     */
    private void checkMergePrivileges(MergeStmt stmt, String schemaName, List<PgTrigger.Event> events) {
        executor.checkTablePrivilege("SELECT", schemaName, stmt.targetTable());
        for (PgTrigger.Event event : events) {
            executor.checkTablePrivilege(event.name(), schemaName, stmt.targetTable());
        }
    }

    /**
     * PostgreSQL does not rewrite a MERGE, so a relation that carries a rule cannot take one: the
     * rewriter refuses the statement outright rather than try to put the rule's actions in its
     * place. It blames the relation the statement named, which is the view when the write is aimed
     * at one, and a view carrying a non-SELECT rule is refused for exactly the same reason.
     */
    private void rejectMergeOnRuledRelation(MergeStmt stmt, Table targetTable) {
        String[] events = {"INSERT", "UPDATE", "DELETE"};
        for (String event : events) {
            String rule = executor.database.getRule(
                    executor.relationSchemaOf(stmt.schema(), stmt.targetTable()),
                    stmt.targetTable(), event);
            if (rule == null && targetTable != null && targetTable.getName() != null
                    && !targetTable.getName().equalsIgnoreCase(stmt.targetTable())) {
                rule = executor.database.getRule(targetTable.getSchemaName(),
                        targetTable.getName(), event);
            }
            if (rule == null) continue;
            MemgresException ex = new MemgresException(
                    "cannot execute MERGE on relation \"" + stmt.targetTable() + "\"", "0A000");
            ex.setDetail("MERGE is not supported for relations with rules.");
            throw ex;
        }
    }

    /**
     * The names a MERGE writes on its target and reads off it, resolved before the scan.
     *
     * <p>PostgreSQL resolves them while it is still analysing the statement, so it reports a column
     * that is not there whether or not a row ever reaches the arm that named it — a MERGE over an
     * empty source used to report success. A system column is refused with its own complaint ahead
     * of the missing-column check, because it is not missing; it just cannot be assigned to. And a
     * column the system computes may only be assigned DEFAULT, which is refused here rather than
     * per row, so nothing draws a value from an identity sequence the statement will not use.
     */
    private void checkMergeTargetNames(MergeStmt stmt, Table targetTable, String targetAlias) {
        String relationName = stmt.targetTable();
        requireMergeTargetColumns(targetTable, stmt.onCondition(), targetAlias, relationName);
        for (MergeStmt.WhenClause clause : stmt.whenClauses()) {
            List<InsertStmt.SetClause> sets = null;
            Expression andCondition = null;
            if (clause instanceof MergeStmt.WhenMatched) {
                MergeStmt.WhenMatched wm = (MergeStmt.WhenMatched) clause;
                andCondition = wm.andCondition();
                if (!wm.isDelete()) sets = wm.setClauses();
            } else if (clause instanceof MergeStmt.WhenNotMatchedBySource) {
                MergeStmt.WhenNotMatchedBySource ws = (MergeStmt.WhenNotMatchedBySource) clause;
                andCondition = ws.andCondition();
                if (!ws.isDelete()) sets = ws.setClauses();
            } else if (clause instanceof MergeStmt.WhenNotMatched) {
                MergeStmt.WhenNotMatched wn = (MergeStmt.WhenNotMatched) clause;
                requireMergeTargetColumns(targetTable, wn.andCondition(), targetAlias, relationName);
                if (wn.values() != null) {
                    for (Expression value : wn.values()) {
                        requireMergeTargetColumns(targetTable, value, targetAlias, relationName);
                    }
                }
                continue;
            }
            requireMergeTargetColumns(targetTable, andCondition, targetAlias, relationName);
            if (sets == null) continue;
            for (InsertStmt.SetClause set : sets) {
                String col = set.column().toLowerCase();
                if (col.equals("ctid") || col.equals("xmin") || col.equals("xmax")
                        || col.equals("cmin") || col.equals("cmax") || col.equals("tableoid")) {
                    throw new MemgresException(
                            "cannot assign to system column \"" + set.column() + "\"", "0A000");
                }
                // A view column computed from an expression is a real column of the view — it is
                // returned by SELECT — but there is nothing behind it to write to.
                if (activeViewExprCols != null && activeViewExprCols.contains(col)) {
                    MemgresException ex = new MemgresException("cannot update column \"" + set.column()
                            + "\" of view \"" + relationName + "\"", "0A000");
                    ex.setDetail(ViewUpdatability.DETAIL_NOT_COLUMN);
                    throw ex;
                }
                int colIdx = targetTable.getColumnIndex(mapViewColumn(set.column()));
                if (colIdx < 0) {
                    throw new MemgresException("column \"" + set.column() + "\" of relation \""
                            + relationName + "\" does not exist", "42703");
                }
                Column genCol = targetTable.getColumns().get(colIdx);
                boolean toDefault = isDefaultLiteral(set.value());
                if (genCol.isGenerated() && !toDefault) {
                    throw new MemgresException("column \"" + genCol.getName()
                            + "\" can only be updated to DEFAULT\n  Detail: Column \""
                            + genCol.getName() + "\" is a generated column.", "428C9");
                }
                if (!toDefault && genCol.getDefaultValue() != null
                        && genCol.getDefaultValue().contains("__identity__:always")) {
                    throw new MemgresException("column \"" + genCol.getName()
                            + "\" can only be updated to DEFAULT\n  Detail: Column \""
                            + genCol.getName() + "\" is an identity column defined as GENERATED ALWAYS.",
                            "428C9");
                }
                requireMergeTargetColumns(targetTable, set.value(), targetAlias, relationName);
            }
        }
    }

    /**
     * Refuse a name the MERGE wrote against its target that the target does not have. Only a
     * reference the writer qualified with the target is judged: a MERGE reads two relations, so an
     * unqualified name may perfectly well be one of the source's columns, and a subquery brings a
     * scope of its own that this cannot see into.
     */
    private void requireMergeTargetColumns(Table table, Expression expr, String targetAlias,
                                           String relationName) {
        if (table == null || expr == null) return;
        if (AstWalk.anyMatch(expr, n -> n instanceof SubqueryExpr || n instanceof ExistsExpr
                || n instanceof ArraySubqueryExpr || n instanceof SelectStmt)) {
            return;
        }
        AstWalk.forEach(expr, node -> {
            if (!(node instanceof ColumnRef)) return;
            ColumnRef cr = (ColumnRef) node;
            String qualifier = cr.table();
            if (qualifier == null) return;
            if (!qualifier.equalsIgnoreCase(targetAlias) && !qualifier.equalsIgnoreCase(relationName)) {
                return;   // another relation's name: not this scope's to judge
            }
            if (SYSTEM_COLUMNS.contains(cr.column().toLowerCase())) return;
            if (table.getColumnIndex(mapViewColumn(cr.column())) < 0) {
                // PostgreSQL leaves a qualified name as it was written.
                throw new MemgresException("column " + qualifier + "." + cr.column()
                        + " does not exist", "42703");
            }
        });
    }

    /**
     * A row the ON condition paired but the policy for the action does not admit. PostgreSQL raises
     * rather than passing over it: a plain UPDATE quietly leaves such a row alone because its WHERE
     * never claimed it, but a MERGE's join already did, so it has to say it could not act.
     */
    private void requireMergeRowVisible(boolean rlsActive, Table table, Object[] row,
                                        String command, String alias) {
        if (!rlsActive) return;
        if (!filterRowsByRlsUsing(table, Collections.singletonList(row), command, alias).isEmpty()) return;
        throw new MemgresException("target row violates row-level security policy (USING expression)"
                + " for table \"" + table.getName() + "\"", "42501");
    }

    /**
     * Write a MERGE's UPDATE arm through the table that actually holds the row, and queue the AFTER
     * row trigger the write earns.
     *
     * <p>When the target is partitioned and the assignment moves the row out of its partition's
     * bounds, PostgreSQL does not carry the write out as an update at all. It deletes the row from
     * the partition that held it and inserts it into the one its values now belong to, and the row
     * triggers of the two partitions see exactly that: the source's BEFORE DELETE and then the
     * destination's BEFORE INSERT before anything is written, their AFTER halves once the statement
     * is over, and no AFTER UPDATE for the row on either relation.
     *
     * <p>A transaction reading from a snapshot writes here without being weighed against it. A row
     * another transaction has updated and committed since is one PostgreSQL's MERGE re-reads rather
     * than one it refuses: the arm runs against the version that transaction left behind. Only a
     * row that has been deleted leaves it with nothing to write, and that is the one case it ends
     * the statement over.
     *
     * @return the row as it now stands, {@code null} when the destination's BEFORE INSERT refused
     *     it and it is only deleted, or {@link #PARTITION_MOVE_REFUSED} when the source's BEFORE
     *     DELETE kept it where it was and nothing happened to it at all
     */
    private Object[] writeMergeUpdate(MergeStmt stmt, Table targetTable, Map<Object[], Table> owners,
                                      Object[] targetRow, Object[] oldRow, Object[] newRow,
                                      Map<Table, List<PgTrigger>> byRelation,
                                      List<PgTrigger> declared, List<MergeAfterRow> queued,
                                      Set<String> assigned) {
        Table owner = owners.get(targetRow);
        if (owner == null) owner = targetTable;
        Table movedTo = partitionRowMovesTo(targetTable, owner, newRow);
        Object[] arriving = null;
        if (movedTo != null) {
            arriving = beforeRowChangesPartition(byRelation, declared,
                    rowTriggersIn(byRelation, owner, declared), owner, movedTo, oldRow, newRow);
            if (arriving == PARTITION_MOVE_REFUSED) return PARTITION_MOVE_REFUSED;
        }
        mergeWritesRowItMatched = true;
        try {
            if (movedTo != null && arriving == null) {
                takeRowOutOfPartition(owner, targetRow, oldRow, false);
                queued.add(new MergeAfterRow(PgTrigger.Event.DELETE, owner, null, oldRow, null));
                return null;
            }
            if (movedTo != null) {
                Object[] movedRow = moveRowAcrossPartitions(owner, movedTo, targetRow, oldRow, arriving);
                owners.put(movedRow, movedTo);
                queued.add(new MergeAfterRow(PgTrigger.Event.DELETE, owner, null, oldRow, null));
                queued.add(new MergeAfterRow(PgTrigger.Event.INSERT, movedTo,
                        Arrays.copyOf(movedRow, movedRow.length), null, null));
                return movedRow;
            }
            recordUpdateUndo(stmt.schema(), stmt.targetTable(), targetRow, oldRow);
            owner.updateRowInPlace(targetRow, oldRow, newRow);
            recordRowUpdateMeta(owner == targetTable ? stmt.schema() : null, owner, targetRow);
            queued.add(new MergeAfterRow(PgTrigger.Event.UPDATE, owner,
                    Arrays.copyOf(targetRow, targetRow.length), oldRow, assigned));
            return targetRow;
        } finally {
            mergeWritesRowItMatched = false;
        }
    }

    /**
     * Every column any of a MERGE's arms assigns.
     *
     * <p>PostgreSQL works out the columns a statement assigns once, for the statement, and judges
     * every UPDATE OF trigger of the relation against that one set. A MERGE assigns from more than
     * one arm, and what decides whether such a trigger fires for a row is what the statement
     * assigns rather than what the arm that happened to write that row assigned.
     */
    private static Set<String> mergeAssignedColumns(MergeStmt stmt) {
        Set<String> assigned = new HashSet<String>();
        for (MergeStmt.WhenClause clause : stmt.whenClauses()) {
            List<InsertStmt.SetClause> sets = null;
            if (clause instanceof MergeStmt.WhenMatched) {
                sets = ((MergeStmt.WhenMatched) clause).setClauses();
            } else if (clause instanceof MergeStmt.WhenNotMatchedBySource) {
                sets = ((MergeStmt.WhenNotMatchedBySource) clause).setClauses();
            }
            if (sets == null) continue;
            for (InsertStmt.SetClause set : sets) assigned.add(set.column().toLowerCase());
        }
        return assigned;
    }

    /** True while a MERGE writes a row one of its arms matched. See {@link #writeMergeUpdate}. */
    private boolean mergeWritesRowItMatched;

    /**
     * One AFTER row trigger a MERGE's arm has earned, and the relation it is owed on.
     *
     * <p>PostgreSQL holds the AFTER row triggers of a statement back until the statement has
     * finished writing, so a MERGE runs all of its arms and only then runs what they queued, in the
     * order the rows were written and whichever arm wrote each of them. What the statement assigns
     * travels with the entry because that is what a trigger written UPDATE OF is judged by; a
     * delete and an insert are owed nothing of the sort and carry none.
     */
    private static final class MergeAfterRow {
        final PgTrigger.Event event;
        final Table relation;
        final Object[] newRow;
        final Object[] oldRow;
        final Set<String> assigned;

        MergeAfterRow(PgTrigger.Event event, Table relation, Object[] newRow, Object[] oldRow,
                      Set<String> assigned) {
            this.event = event;
            this.relation = relation;
            this.newRow = newRow;
            this.oldRow = oldRow;
            this.assigned = assigned;
        }
    }

    /**
     * Where an UPDATE's new values send a row, or null when they leave it where it stands.
     *
     * <p>A partitioned table stores nothing of its own: which partition holds a row is decided by
     * the row's own values, so an assignment to the partition key moves the row.
     */
    private Table partitionRowMovesTo(Table table, Table owner, Object[] newRow) {
        if (owner == null || table.getPartitionStrategy() == null
                || table.getPartitions().isEmpty()) {
            return null;
        }
        Table routed = partitionHelper.routeToPartition(table, newRow);
        return routed != null && routed != owner ? routed : null;
    }

    /**
     * Move a row into the partition its new values belong in.
     *
     * <p>PostgreSQL does not rewrite such a row where it stands: it makes the version in the
     * partition that held it dead and writes a new version into the partition the new values route
     * to. So the moved row comes back at a place of its own rather than at the one it had, a
     * session not entitled to the write goes on reading the row where it was, and an abort leaves
     * the row in its old partition rather than a copy in each. Recording the two halves as the
     * delete and the insert they really are is what lets every undo -- this transaction's and
     * every other session's -- read them.
     *
     * @return the row as its new partition stores it
     */
    private Object[] moveRowAcrossPartitions(Table from, Table to, Object[] row,
                                             Object[] oldRow, Object[] newRow) {
        return moveRowAcrossPartitions(from, to, row, oldRow, newRow, true);
    }

    /** Told apart from a refusal by identity, so a trigger returning nothing keeps its meaning. */
    private static final Object[] PARTITION_MOVE_REFUSED = new Object[0];

    /**
     * The row triggers a change of partition runs before anything is written, and the row they
     * leave for the destination to store.
     *
     * <p>PostgreSQL does not carry a row that has changed partition out as an update. It deletes
     * the row from the partition that held it and inserts it into the one its values now belong
     * to, and the row triggers of the two partitions see exactly that -- the source's BEFORE
     * DELETE and then the destination's BEFORE INSERT, with no AFTER UPDATE for the row at all.
     * A BEFORE DELETE that returns NULL leaves the row where it was and the move never happens;
     * a BEFORE INSERT that returns NULL leaves the row deleted and stored nowhere, because by
     * then the delete has been carried out.
     *
     * @return the row the destination is to store, {@code null} when the destination refused it
     *     and the row is only to be deleted, or {@link #PARTITION_MOVE_REFUSED} when the source
     *     kept it and nothing at all is to happen
     */
    private Object[] beforeRowChangesPartition(Map<Table, List<PgTrigger>> byRelation,
                                               List<PgTrigger> declared,
                                               List<PgTrigger> sourceTriggers, Table from, Table to,
                                               Object[] oldRow, Object[] newRow) {
        if (triggerHelper.executeTriggers(sourceTriggers, PgTrigger.Timing.BEFORE,
                PgTrigger.Event.DELETE, null, oldRow, from) == null) {
            return PARTITION_MOVE_REFUSED;
        }
        return triggerHelper.executeTriggers(rowTriggersIn(byRelation, to, declared),
                PgTrigger.Timing.BEFORE, PgTrigger.Event.INSERT, newRow, null, to);
    }

    /**
     * Make the version of a row that stood in the partition it is leaving dead.
     *
     * <p>The version being made dead is the one that stood there before the statement ran: that is
     * what a session which may not see the write is entitled to read, and what an abort has to
     * leave behind. One of the update paths applies the assignments to the stored row itself, so
     * the row is put back as it was before it is filed away as deleted.
     *
     * @param moved whether the row is on its way to another partition rather than simply going: a
     *     session waiting to lock a row that moved is told that it moved, while one waiting on a
     *     row a trigger refused to let the destination have finds it deleted like any other
     */
    private void takeRowOutOfPartition(Table from, Object[] row, Object[] oldRow, boolean moved) {
        System.arraycopy(oldRow, 0, row, 0, Math.min(row.length, oldRow.length));
        recordDeleteUndo(from.getSchemaName(), from.getName(), Collections.singletonList(row));
        if (moved && executor.session != null) executor.session.noteRowMovedToAnotherPartition(row);
        from.deleteRow(row);
    }

    private Object[] moveRowAcrossPartitions(Table from, Table to, Object[] row,
                                             Object[] oldRow, Object[] newRow, boolean moved) {
        // The destination is a relation with rules of its own: its constraints and its bound
        // decide whether it may hold this row, and PostgreSQL names it in the error. Both are
        // settled before the row leaves its old home, or a refused update would delete the row
        // and never store it anywhere.
        Object[] movedRow = to.rowFromParent(Arrays.copyOf(newRow, newRow.length));
        partitionHelper.checkPartitionConstraint(to, movedRow);
        executor.constraintValidator.validateConstraints(to, movedRow, null);
        takeRowOutOfPartition(from, row, oldRow, moved);
        to.insertRow(movedRow);
        recordRowMeta(to.getSchemaName(), to, movedRow);
        recordInsertUndo(to.getSchemaName(), to.getName(), movedRow);
        return movedRow;
    }

    /**
     * A source row of all nulls, shaped like the source. WHEN NOT MATCHED BY SOURCE fires for a
     * target row that no source row paired with, and RETURNING still reads the source's columns
     * there — as nulls, which is what PostgreSQL answers.
     */
    private RowContext nullSourceContext(RowContext sample) {
        if (sample == null) return null;
        List<RowContext.TableBinding> bindings = new ArrayList<>();
        for (RowContext.TableBinding b : sample.getBindings()) {
            bindings.add(new RowContext.TableBinding(b.table(), b.alias(), new Object[b.row().length]));
        }
        return new RowContext(bindings);
    }

    /** Whether a qualified reference names the MERGE's source rather than its target. */
    private boolean mergeNamesSource(String qualifier, RowContext sourceCtx, String sourceAlias) {
        if (qualifier == null) return false;
        if (qualifier.equalsIgnoreCase(sourceAlias)) return true;
        if (sourceCtx == null) return false;
        for (RowContext.TableBinding b : sourceCtx.getBindings()) {
            if (mergeBindingNamed(b, qualifier)) return true;
        }
        return false;
    }

    /** Whether one binding of the source answers to the given name. */
    private boolean mergeBindingNamed(RowContext.TableBinding b, String qualifier) {
        if (b.alias() != null && b.alias().equalsIgnoreCase(qualifier)) return true;
        return b.table() != null && b.table().getName() != null
                && b.table().getName().equalsIgnoreCase(qualifier);
    }

    /**
     * RETURNING for a MERGE, which is read over the join the statement ran rather than over the
     * target alone. PostgreSQL answers an unqualified {@code *} with every column of the source
     * followed by every column of the target — in that order — and a qualified one with the columns
     * of whichever of the two relations it names. OLD and NEW still mean the target row before and
     * after the action, and every other expression is evaluated as it is for any other write.
     */
    private Object[] evalMergeReturning(List<SelectStmt.SelectTarget> returning, Table table,
                                        String alias, Object[] row, Object[] oldRow, Object[] newRow,
                                        RowContext sourceCtx, String sourceAlias) {
        List<Object> values = new ArrayList<>();
        for (SelectStmt.SelectTarget target : returning) {
            if (target.expr() instanceof WildcardExpr) {
                WildcardExpr we = (WildcardExpr) target.expr();
                String qualifier = we.table();
                boolean oldOrNew = qualifier != null
                        && (qualifier.equalsIgnoreCase("old") || qualifier.equalsIgnoreCase("new"));
                if (!oldOrNew) {
                    boolean wantsSource = qualifier == null
                            || mergeNamesSource(qualifier, sourceCtx, sourceAlias);
                    if (wantsSource && sourceCtx != null) {
                        for (RowContext.TableBinding b : sourceCtx.getBindings()) {
                            if (qualifier != null && !qualifier.equalsIgnoreCase(sourceAlias)
                                    && !mergeBindingNamed(b, qualifier)) {
                                continue;
                            }
                            for (Object val : b.row()) values.add(val);
                        }
                    }
                    if (qualifier == null || qualifier.equalsIgnoreCase(alias)
                            || qualifier.equalsIgnoreCase(table.getName())) {
                        for (Object val : row) values.add(val);
                    }
                    continue;
                }
            }
            Object[] one = evalReturning(Cols.listOf(target), table, alias, row, oldRow, newRow, sourceCtx);
            for (Object val : one) values.add(val);
        }
        return values.toArray();
    }

    /** Column metadata for a MERGE's RETURNING, in the order {@link #evalMergeReturning} fills it. */
    private List<Column> buildMergeReturningColumns(List<SelectStmt.SelectTarget> returning, Table table,
                                                    String alias, RowContext sourceSample, String sourceAlias) {
        List<Column> cols = new ArrayList<>();
        for (SelectStmt.SelectTarget target : returning) {
            if (target.expr() instanceof WildcardExpr) {
                WildcardExpr we = (WildcardExpr) target.expr();
                String qualifier = we.table();
                boolean oldOrNew = qualifier != null
                        && (qualifier.equalsIgnoreCase("old") || qualifier.equalsIgnoreCase("new"));
                if (!oldOrNew) {
                    boolean wantsSource = qualifier == null
                            || mergeNamesSource(qualifier, sourceSample, sourceAlias);
                    if (wantsSource && sourceSample != null) {
                        for (RowContext.TableBinding b : sourceSample.getBindings()) {
                            if (qualifier != null && !qualifier.equalsIgnoreCase(sourceAlias)
                                    && !mergeBindingNamed(b, qualifier)) {
                                continue;
                            }
                            if (b.table() != null) cols.addAll(b.table().getColumns());
                        }
                    }
                    if (qualifier == null || qualifier.equalsIgnoreCase(alias)
                            || qualifier.equalsIgnoreCase(table.getName())) {
                        cols.addAll(table.getColumns());
                    }
                    continue;
                }
            }
            cols.addAll(buildReturningColumns(Cols.listOf(target), table, sourceSample));
        }
        return cols;
    }

    // ---- Statement atomicity: snapshot / restore target-table rows ----
    //
    // PostgreSQL treats each INSERT/UPDATE/DELETE/MERGE as an atomic statement: if a row
    // trigger (or constraint) raises partway through, every side effect the statement had
    // already applied is rolled back. Autocommit statements have no transaction undo log to
    // lean on (Session.recordUndo only records inside a transaction), so we snapshot the
    // affected table(s) up front and, on failure, restore them. Mirrors the MERGE executor.

    /** Collect the target relation plus every relation that stores rows for it. */
    private List<Table> collectTargetTables(Table table) {
        List<Table> tables = new ArrayList<>();
        DmlPartitionHelper.collectRelationAndDescendants(table, tables);
        return tables;
    }

    /**
     * The triggers a write to this relation fires. PostgreSQL clones a partitioned table's FOR
     * EACH ROW triggers onto every partition, and it is the partition's own copy that fires: the
     * copy is a catalogue row of its own, so reaching up to the parent for it as well fired the
     * one trigger twice for one row. A statement-level trigger is not cloned and stays with the
     * relation it was declared on, and plain inheritance children inherit no triggers at all.
     */
    private List<PgTrigger> rowTriggersFor(Table table, String named) {
        List<PgTrigger> own = executor.database.getTriggersForTable(named);
        // A write through an auto-updatable view is rewritten onto the base relation, so the base
        // relation's triggers are the ones that fire. Looking them up under the name the statement
        // wrote found only the view's, which has none — the rows were written and every BEFORE and
        // AFTER trigger on the table behind the view was silently skipped. The view's own triggers
        // stay first, so the INSTEAD OF scan reads them before anything the base relation adds.
        if (table != null && table.getName() != null && !table.getName().equalsIgnoreCase(named)) {
            List<PgTrigger> throughView = new ArrayList<PgTrigger>(own);
            throughView.addAll(executor.database.getTriggersForTable(table.getName()));
            own = throughView;
        }
        return own;
    }

    /**
     * The row triggers every relation that can store a row for this statement's target carries.
     *
     * <p>PostgreSQL fires a FOR EACH ROW trigger as a trigger of the relation the row is really
     * stored in. A partition is given a copy of every row trigger written on the partitioned table
     * above it, so what fires for a write through the parent is the copy, beside whatever was
     * written on the partition itself and in name order with it; an inheritance child is given no
     * copy at all and fires only its own. Reading the named relation's own list for every row
     * therefore never reached a partition's own triggers -- an audit, a computed column, a refusal,
     * all of them skipped for exactly the statements people write -- and named the parent inside
     * the ones it did reach. A FOR EACH STATEMENT trigger is a different matter: it belongs to the
     * relation the statement named, which is the list the caller keeps.
     */
    private Map<Table, List<PgTrigger>> rowTriggersByRelation(Table table, List<PgTrigger> declared) {
        Map<Table, List<PgTrigger>> byRelation = new IdentityHashMap<Table, List<PgTrigger>>();
        if (table == null) return byRelation;
        byRelation.put(table, declared);
        List<Table> storage = new ArrayList<Table>();
        DmlPartitionHelper.collectRelationAndDescendants(table, storage);
        for (Table relation : storage) {
            if (relation == table) continue;
            byRelation.put(relation, enabledTriggers(
                    executor.database.getTriggersForTable(relation.getName())));
        }
        return byRelation;
    }

    /** The row triggers of the relation a row is stored in, or the target's own for anything else. */
    private List<PgTrigger> rowTriggersIn(Map<Table, List<PgTrigger>> byRelation, Table storage,
                                          List<PgTrigger> declared) {
        if (storage == null) return declared;
        List<PgTrigger> here = byRelation.get(storage);
        return here == null ? declared : here;
    }

    /** Whether any relation this statement can write to carries a trigger at all. */
    private boolean anyRowTriggers(Map<Table, List<PgTrigger>> byRelation) {
        for (List<PgTrigger> list : byRelation.values()) {
            if (!list.isEmpty()) return true;
        }
        return false;
    }

    /**
     * One AFTER row trigger the statement still owes, and the relation it is owed on.
     *
     * <p>PostgreSQL holds every AFTER row trigger back until the statement has finished writing.
     * A row that changed partition is owed two of them -- the delete on the partition it left and
     * the insert on the one it reached -- and no AFTER UPDATE at all, because a move is not an
     * update on either relation.
     */
    private static final class PendingAfterRow {
        final PgTrigger.Event event;
        final Table relation;
        final Object[] newRow;
        final Object[] oldRow;

        PendingAfterRow(PgTrigger.Event event, Table relation, Object[] newRow, Object[] oldRow) {
            this.event = event;
            this.relation = relation;
            this.newRow = newRow;
            this.oldRow = oldRow;
        }
    }

    /**
     * One row as the statement found it: the array the relation holds and the values that were in
     * it.
     *
     * <p>Both are needed, because a row is more than what it holds. PostgreSQL's abort leaves the
     * version a statement found exactly where it was, and everything that has anything to say
     * about a row -- the transaction's undo log, the book that answers ctid and xmin -- says it
     * about that array and no other. Handing the relation a copy left all of it pointing at a row
     * the relation no longer held, so a ROLLBACK afterwards could not find the rows an earlier
     * statement of the same transaction had written, and they survived it.
     */
    private static final class RowImage {
        final Object[] row;
        final Object[] values;
        final long[] identity;

        RowImage(Object[] row, Map<Object[], long[]> book) {
            this.row = row;
            this.values = Arrays.copyOf(row, row.length);
            long[] was = book.get(row);
            this.identity = was == null ? null : Arrays.copyOf(was, was.length);
        }
    }

    /** Note every row of each given table so the statement can be rolled back. */
    private Map<Table, List<RowImage>> snapshotTargetTables(List<Table> tables) {
        Map<Table, List<RowImage>> snapshot = new IdentityHashMap<>();
        for (Table t : tables) {
            List<RowImage> copy = new ArrayList<>();
            Map<Object[], long[]> book = t.getRowMeta();
            for (Object[] r : t.getRows()) {
                copy.add(new RowImage(r, book));
            }
            snapshot.put(t, copy);
        }
        return snapshot;
    }

    /** Restore each snapshotted table to its captured state (used on mid-statement failure). */
    private void restoreTargetTables(Map<Table, List<RowImage>> snapshot) {
        for (Map.Entry<Table, List<RowImage>> e : snapshot.entrySet()) {
            Table t = e.getKey();
            t.getWriteLock().lock();
            try {
                t.deleteAll();
                Map<Object[], long[]> book = t.getRowMeta();
                for (RowImage image : e.getValue()) {
                    System.arraycopy(image.values, 0, image.row, 0, image.values.length);
                    // The version this statement wrote is dead and the one it found is live
                    // again, so the row answers with the place and the transaction it always
                    // had -- an abort renumbers nothing in PostgreSQL.
                    if (image.identity != null) book.put(image.row, image.identity);
                    t.insertRow(image.row);
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

    /**
     * The value a generated column takes for a row, worked out from the generation expression
     * against that row.
     *
     * <p>PostgreSQL evaluates the stored expression with the other columns as values, exactly as
     * it evaluates a CHECK constraint. Writing those values into the expression's own text and
     * reading the text back as SQL made a value that spells a column name into that column's
     * value, ordinary table data into syntax -- a text value holding a subquery ran it -- and
     * every value with no literal spelling, a bytea or a timestamp or an array, into whatever
     * Java printed it as. Whatever the expression raises is the statement's error: a value
     * PostgreSQL never computed must not be written in its place.
     */
    Object evalGeneratedColumn(Table table, Object[] row, Column col) {
        Expression expr = col.getGeneratedExprAst();
        if (expr == null) return null;
        // A FROM item's alias list renames the references to a relation's columns and not the
        // expressions stored with them, so the expression of a column reached through one is still
        // written in the names the relation underneath answers to.
        Table named = table.getColumnsRenamedFrom();
        Object value = executor.evalExpr(expr,
                new RowContext(named != null ? named : table, null, row));
        return TypeCoercion.coerceForStorage(value, col);
    }

    // ---- Helper: WHERE CURRENT OF ----

    /**
     * Filter table rows to the single row positioned by the named cursor.
     * Matches by comparing column values from the cursor's current row against the table row values.
     */
    /**
     * The one row a cursor is on, for WHERE CURRENT OF.
     *
     * <p>The row was found by comparing the cursor's columns against every row of the table and
     * taking the first that matched. A cursor's select list need not carry a key — {@code SELECT nm
     * FROM t ORDER BY id DESC} carries none — so two rows that share a value were the same row to
     * that search, and it updated whichever came first in storage rather than the one the cursor
     * had reached. The cursor remembers the stored rows it walked, so the row it is on is the row
     * it is on.
     */
    private List<Object[]> filterByCurrentOf(com.memgres.engine.parser.ast.CurrentOfExpr cof,
                                              Table table, List<Object[]> candidateRows) {
        Session.CursorState cursor = executor.session.getCursor(cof.cursorName());
        if (cursor == null) {
            throw new MemgresException("cursor \"" + cof.cursorName() + "\" does not exist", "34000");
        }
        // A cursor that has not fetched yet, or has run past the end, is not on a row: PG says
        // so rather than quietly matching nothing
        int pos = cursor.getPosition();
        if (pos < 0 || pos >= cursor.getRowCount()) {
            throw new MemgresException(
                    "cursor \"" + cof.cursorName() + "\" is not positioned on a row", "24000");
        }
        Object[] current = cursor.currentRowOf(table);
        if (current == null) {
            // A cursor that locks its rows was written to be updated through, so the complaint is
            // that it does not reach this table; one that does not was never updatable at all.
            String why = cursor.isLocking()
                    ? "\" does not have a FOR UPDATE/SHARE reference to table \""
                    : "\" is not a simply updatable scan of table \"";
            throw new MemgresException(
                    "cursor \"" + cof.cursorName() + why + table.getName() + "\"", "24000");
        }
        List<Object[]> result = new ArrayList<>();
        for (Object[] row : candidateRows) {
            if (row == current) {
                result.add(row);
                break;
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
     * Clone a row and fill in the VIRTUAL generated columns the scan reading it must have.
     * The original row is not modified (virtual columns are not stored).
     */
    Object[] computeVirtualColumns(Table table, Object[] row) {
        return computeVirtualColumns(table, null, row);
    }

    /**
     * The same, for a scan that knows the name the relation answers to in the query reading it.
     */
    Object[] computeVirtualColumns(Table table, String alias, Object[] row) {
        Object[] result = row.clone();
        Boolean rejected = null;
        // A row behind the one that settled what a sub-select was asked is never read at all, so
        // nothing of it is worked out. PostgreSQL stops reading the sub-select there.
        if (scanAlreadyAnswered()) return result;
        for (int i = 0; i < table.getColumns().size(); i++) {
            Column col = table.getColumns().get(i);
            if (!col.isVirtual()) continue;
            // A virtual column is not stored: PostgreSQL rewrites a reference to one into its
            // generation expression, so the expression is evaluated where the reference stood
            // and nowhere else. A scan reads its own qualifications for every row it passes
            // over, so those are worked out here, and everything else the query names is
            // worked out for the rows the qualifications kept. That is what leaves a relation
            // whose expression raises for one row -- 10/a where a is zero -- readable by a
            // query that filters that row out, counts the rows, or reads other columns.
            if (!scanQualificationReads(table, col)) continue;
            // The qualifications are themselves read cheapest first, and one that only compares
            // stored columns is cheaper than one holding a generation expression, so a row the
            // stored ones reject never reaches the expression at all.
            if (rejected == null) rejected = scanQualificationRejects(table, alias, result);
            if (rejected) continue;
            fillVirtualColumn(table, result, i, col);
        }
        if (rejected == null || !rejected) noteScanAnswered(table, alias, result);
        return result;
    }

    Object[] computeVirtualColumns(Table table, Object[] row, boolean strict) {
        if (strict) return computeVirtualColumns(table, null, row);
        Object[] result = row.clone();
        for (int i = 0; i < table.getColumns().size(); i++) {
            Column col = table.getColumns().get(i);
            if (!col.isVirtual()) continue;
            try {
                result[i] = evalGeneratedColumn(table, result, col);
            } catch (Exception e) {
                result[i] = null;
            }
        }
        return result;
    }

    /**
     * Clone a row and fill in the VIRTUAL generated columns named here, a null set naming them all.
     */
    Object[] computeVirtualColumns(Table table, Object[] row, Set<String> named) {
        Object[] result = row.clone();
        fillVirtualColumns(table, result, named);
        return result;
    }

    /**
     * Work out, for the rows a query kept, the VIRTUAL generated columns it reads.
     *
     * <p>PostgreSQL puts the generation expression where the reference to the column stood, so the
     * select list's is evaluated for the rows the WHERE let through and for no others. Working
     * every one out as the relation was scanned raised, for a row the query discards, an error the
     * query never asked for -- and on an UPDATE, a DELETE or a RETURNING it lost the write.
     *
     * <p>The rows are the query's own copies, made as the relation was scanned, so they are filled
     * in place: the context in front of a row carries what the FROM clause worked out about it, and
     * building a new one around a new row would drop that. One row reaches as many contexts as the
     * join it feeds found matches for it, so each is filled once.
     */
    void computeVirtualColumnsForOutput(List<RowContext> contexts,
                                        java.util.function.Supplier<Set<String>> read) {
        computeVirtualColumnsForOutput(contexts, read, Collections.<String>emptySet());
    }

    /**
     * The same, leaving these columns to the relation this query is being read as.
     *
     * <p>A relation built from a query can carry a VIRTUAL generated column of the relation
     * underneath generated still, when it exposes every column the expression reads. PostgreSQL
     * then evaluates the expression in the query reading that relation, over the rows its joins
     * and its WHERE kept, so it is not evaluated here.
     */
    void computeVirtualColumnsForOutput(List<RowContext> contexts,
                                        java.util.function.Supplier<Set<String>> read,
                                        Set<String> leftToRelation) {
        Set<String> named = null;
        Set<Object[]> filled = null;
        for (RowContext ctx : contexts) {
            for (RowContext.TableBinding binding : ctx.getBindings()) {
                Table table = binding.table();
                if (table == null || binding.row() == null || !hasVirtualColumns(table)) continue;
                if (filled == null) {
                    named = read.get();
                    filled = Collections.newSetFromMap(new IdentityHashMap<Object[], Boolean>());
                }
                if (!filled.add(binding.row())) continue;
                fillVirtualColumns(table, binding.row(), named, leftToRelation);
            }
        }
    }

    /** Work out the VIRTUAL generated columns named here into a row; a null set names them all. */
    void fillVirtualColumns(Table table, Object[] row, Set<String> named) {
        fillVirtualColumns(table, row, named, Collections.<String>emptySet());
    }

    private void fillVirtualColumns(Table table, Object[] row, Set<String> named,
                                    Set<String> leftToRelation) {
        for (int i = 0; i < table.getColumns().size(); i++) {
            Column col = table.getColumns().get(i);
            if (!col.isVirtual()) continue;
            String name = col.getName().toLowerCase();
            if (named != null && !named.contains(name)) continue;
            if (leftToRelation.contains(name)) continue;
            fillVirtualColumn(table, row, i, col);
        }
    }

    private void fillVirtualColumn(Table table, Object[] row, int index, Column col) {
        // PG 18: virtual columns cannot use user-defined functions; reject at read time
        checkVirtualColumnUdfAtRead(col);
        row[index] = evalGeneratedColumn(table, row, col);
    }

    /**
     * The columns these parts of a statement name, in lower case, or null when they name every
     * column the relation has -- which is what a {@code *} asks for, and what the relation's own
     * name read as a value asks for, a whole-row reference being all of its columns at once.
     *
     * <p>A bare {@code *} written inside a query of its own is that query's and not this
     * statement's: PostgreSQL pulls such a query up into the statement reading it, so
     * {@code USING (SELECT * FROM t) s} asks for what the statement above names of s and no more.
     * A star written with a relation before it is left alone, because the relation it names may be
     * one this statement holds a row of.
     */
    private static Set<String> columnsNamed(Table table, String alias, Object... parts) {
        final Set<String> named = new HashSet<>();
        final boolean[] everyColumn = {false};
        AstWalk.forEach(Arrays.asList(parts), node -> {
            if (node instanceof CompositeStarExpr
                    || (node instanceof WildcardExpr && ((WildcardExpr) node).table() != null)) {
                everyColumn[0] = true;
            } else if (node instanceof ColumnRef) {
                String column = ((ColumnRef) node).column();
                if (column != null) named.add(column.toLowerCase());
            }
        });
        if (everyColumn[0] || bareStarHere(Arrays.asList(parts))) return null;
        if (table != null && named.contains(table.getName().toLowerCase())) return null;
        if (alias != null && named.contains(alias.toLowerCase())) return null;
        return named;
    }

    /** Whether a bare {@code *} stands at this level rather than inside a query of its own. */
    private static boolean bareStarHere(Object root) {
        final Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
        final Deque<Object> queue = new ArrayDeque<>();
        queue.add(root);
        seen.add(root);
        while (!queue.isEmpty()) {
            Object node = queue.poll();
            if (node instanceof WildcardExpr && ((WildcardExpr) node).table() == null) return true;
            AstWalk.forEachChild(node, child -> {
                // Another statement is another query level, answering for its own columns.
                if (child instanceof Statement) return;
                if (seen.add(child)) queue.add(child);
            });
        }
        return false;
    }

    /**
     * What one query asks a relation for. Deciding it costs a walk of the statement, so it is
     * decided only if a virtual column is actually reached -- nearly no relation has one.
     */
    private static final class ColumnDemand {
        private final java.util.function.Supplier<Set<String>> read;
        private final java.util.function.Supplier<Set<String>> filtered;
        private final java.util.function.Supplier<List<Expression>> qualification;
        private final java.util.function.Supplier<Set<String>> relations;
        private final java.util.function.Supplier<Boolean> paired;
        private final java.util.function.Supplier<List<Expression>> settles;
        private List<Expression> settlingParts;
        private boolean settlingSettled;
        private boolean answered;
        private Set<String> readColumns;
        private boolean readSettled;
        private boolean readSettling;
        private Set<String> filteredColumns;
        private boolean filteredSettled;
        private List<Expression> qualificationParts;
        private Set<String> qualificationRelations;
        private boolean qualificationLone;
        private boolean qualificationSettled;
        private Table decidedFor;
        private String decidedAs;
        private List<Expression> decidable;

        ColumnDemand(java.util.function.Supplier<Set<String>> read,
                     java.util.function.Supplier<Set<String>> filtered) {
            this(read, filtered, null, null);
        }

        ColumnDemand(java.util.function.Supplier<Set<String>> read,
                     java.util.function.Supplier<Set<String>> filtered,
                     java.util.function.Supplier<List<Expression>> qualification,
                     java.util.function.Supplier<Set<String>> relations) {
            this(read, filtered, qualification, relations, null, null);
        }

        ColumnDemand(java.util.function.Supplier<Set<String>> read,
                     java.util.function.Supplier<Set<String>> filtered,
                     java.util.function.Supplier<List<Expression>> qualification,
                     java.util.function.Supplier<Set<String>> relations,
                     java.util.function.Supplier<Boolean> paired,
                     java.util.function.Supplier<List<Expression>> settles) {
            this.read = read;
            this.filtered = filtered;
            this.qualification = qualification;
            this.relations = relations;
            this.paired = paired;
            this.settles = settles;
        }

        /**
         * Whether the statement above reads this query as one join with itself, which is what makes
         * a comparison with the row beside it the condition of that join rather than a filter on
         * this query's own scan.
         */
        boolean pairedAbove() {
            return paired != null && Boolean.TRUE.equals(paired.get());
        }

        /** Whether a row already read has settled what the statement above asked this query. */
        boolean settled() { return answered; }

        /**
         * What one row has to hold for the answer to be settled by it, or null where nothing
         * settles it before every row has been read.
         */
        List<Expression> settlingParts() {
            if (!settlingSettled) {
                settlingParts = settles == null ? null : settles.get();
                settlingSettled = true;
            }
            return settlingParts;
        }

        void settle() { answered = true; }

        /**
         * The parts of the query's own qualification a scan of this relation can decide from one
         * row of it. Worked out once per relation scanned, because a scan asks it of every row.
         */
        List<Expression> decidableFor(DmlExecutor owner, Table table, String alias) {
            if (qualification == null) return Collections.emptyList();
            if (decidedFor != table || !Objects.equals(decidedAs, alias)) {
                if (!qualificationSettled) {
                    qualificationParts = qualification.get();
                    qualificationRelations = relations == null ? null : relations.get();
                    // A name written without its relation is this one's only where the query reads
                    // nothing else it could have meant.
                    qualificationLone = qualificationRelations != null
                            && qualificationRelations.size() == 1;
                    qualificationSettled = true;
                }
                decidable = owner.decidableQualification(table, alias, qualificationParts,
                        qualificationLone, qualificationRelations, pairedAbove());
                decidedFor = table;
                decidedAs = alias;
            }
            return decidable;
        }

        /** The columns the query reads anywhere, or null when it reads every column there is. */
        Set<String> readColumns() {
            if (!readSettled) {
                // A query asks what the query around it reads while working out what it reads
                // itself, and the one being worked out is not an answer to that question.
                readSettling = true;
                try {
                    readColumns = read.get();
                } finally {
                    readSettling = false;
                }
                readSettled = true;
            }
            return readColumns;
        }

        /** True while this query's own answer is still being worked out. */
        boolean settling() { return readSettling; }

        /**
         * True when the query's own qualifications read this column, and for every column when
         * they read them all.
         */
        boolean filters(String column) {
            if (!filteredSettled) {
                filteredColumns = filtered.get();
                filteredSettled = true;
            }
            return filteredColumns == null || filteredColumns.contains(column.toLowerCase());
        }
    }

    /** The queries reading a relation right now, the innermost last. */
    private final List<ColumnDemand> columnDemands = new ArrayList<>();

    /**
     * Read a relation for a query that reads these columns and whose own qualifications read those;
     * a null set names every column. What the query names is what settles whether a VIRTUAL
     * generated column's expression is evaluated at all, and where: PostgreSQL puts the expression
     * where the reference to the column stood, so a qualification's is worked out for every row
     * scanned and the rest for the rows the qualification kept.
     */
    void enterColumnDemand(java.util.function.Supplier<Set<String>> read,
                           java.util.function.Supplier<Set<String>> filtered) {
        columnDemands.add(new ColumnDemand(read, filtered));
    }

    /**
     * The same, for a query whose qualification is written out in parts the scan can read one at a
     * time, over the relations these names answer for.
     */
    void enterColumnDemand(java.util.function.Supplier<Set<String>> read,
                           java.util.function.Supplier<Set<String>> filtered,
                           java.util.function.Supplier<List<Expression>> qualification,
                           java.util.function.Supplier<Set<String>> relations) {
        columnDemands.add(new ColumnDemand(read, filtered, qualification, relations));
    }

    /**
     * The same, for a query the statement above wrote among the parts of its qualification, which
     * that statement may read as one join with itself and may stop reading at the row that settles
     * what it asked.
     */
    void enterColumnDemand(java.util.function.Supplier<Set<String>> read,
                           java.util.function.Supplier<Set<String>> filtered,
                           java.util.function.Supplier<List<Expression>> qualification,
                           java.util.function.Supplier<Set<String>> relations,
                           java.util.function.Supplier<Boolean> paired,
                           java.util.function.Supplier<List<Expression>> settles) {
        columnDemands.add(
                new ColumnDemand(read, filtered, qualification, relations, paired, settles));
    }

    void exitColumnDemand() {
        columnDemands.remove(columnDemands.size() - 1);
    }

    /**
     * Read a relation a writing statement brings in beside its target -- a MERGE's source, an
     * UPDATE's FROM clause, a DELETE's USING clause -- for a statement that names these columns of
     * it.
     *
     * <p>With nothing on the demand stack every column is demanded, which worked out a VIRTUAL
     * generated column no part of the statement mentions and lost the write to the error that
     * raised. PostgreSQL rewrites a reference to such a column into its generation expression, so
     * a column the statement never names is never evaluated at all.
     *
     * <p>The scan itself works out only what {@code qualifying} names, because that is what decides
     * whether a row is paired at all. What the statement names above the join -- an assignment, a
     * RETURNING item -- is worked out for the rows the join kept, by {@link #fillJoinedVirtuals}.
     */
    private List<RowContext> readExtraRelation(java.util.function.Supplier<List<RowContext>> read,
                                               Object[] named, Object[] qualifying,
                                               Expression restrictedBy) {
        final Object[] readParts = named;
        final Object[] qualifyingParts = qualifying;
        // The restrictions the qualification's equalities put on this relation's own scan, which
        // decide a row before anything expensive is read of it.
        final List<Expression> implied = impliedEqualities(restrictedBy);
        enterColumnDemand(() -> columnsNamed(null, null, readParts),
                () -> columnsNamed(null, null, qualifyingParts),
                () -> implied,
                null);
        try {
            return read.get();
        } finally {
            exitColumnDemand();
        }
    }

    /**
     * Read a relation whose VIRTUAL generated columns have to be worked out as it is scanned,
     * because the join above it answers rows that carry none of its rows at all.
     *
     * <p>Nothing above such a join can work the expression out, so PostgreSQL works out every one
     * of the relation's generated columns below it -- whether or not the statement names them --
     * and narrows that scan by nothing but what the join condition says about this relation on its
     * own. A comparison with the other side is not such a restriction: an outer join does not put
     * its two sides in one class, so nothing the other side is equal to is derived here.
     */
    private List<RowContext> readRelationBelowJoin(
            java.util.function.Supplier<List<RowContext>> read, Expression restrictedBy) {
        final List<Expression> restrictions = FromResolver.conjunctsOf(restrictedBy);
        enterColumnDemand(() -> null, () -> null, () -> restrictions, null);
        try {
            return read.get();
        } finally {
            exitColumnDemand();
        }
    }

    /**
     * Whether a MERGE has to answer for a source row that paired with no target row.
     *
     * <p>An arm written WHEN NOT MATCHED acts on such a row, so PostgreSQL preserves the source
     * side of the join and reads every row of it however the ON condition reads. An arm that does
     * nothing asks nothing of the join, exactly as a WHEN NOT MATCHED BY SOURCE arm that does
     * nothing asks nothing of the other side.
     */
    private static boolean mergeAnswersForUnpairedSource(MergeStmt stmt) {
        for (MergeStmt.WhenClause clause : stmt.whenClauses()) {
            if (clause instanceof MergeStmt.WhenNotMatched
                    && !((MergeStmt.WhenNotMatched) clause).doNothing()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a MERGE reads its source through a join that preserves the target.
     *
     * <p>A WHEN NOT MATCHED BY SOURCE arm acts on a target row no source row paired with, so the
     * target is the side the join keeps and the source is the side that may be padded away. An arm
     * that does nothing asks nothing of the join, and PostgreSQL leaves it an inner one.
     */
    private static boolean mergeSourceIsNullable(MergeStmt stmt) {
        for (MergeStmt.WhenClause clause : stmt.whenClauses()) {
            if (!(clause instanceof MergeStmt.WhenNotMatchedBySource)) continue;
            MergeStmt.WhenNotMatchedBySource arm = (MergeStmt.WhenNotMatchedBySource) clause;
            if (arm.isDelete() || (arm.setClauses() != null && !arm.setClauses().isEmpty())) {
                return true;
            }
        }
        return false;
    }

    /**
     * The restrictions PostgreSQL derives from a qualification's equalities.
     *
     * <p>Two columns compared for equality, and a column compared with a written constant, stand in
     * one class: {@code t.a = o.a AND o.a = 5} says {@code t.a = 5} as surely as it says either of
     * the two, and PostgreSQL puts that restriction on t's own scan. It matters here because a
     * restriction on the scan decides a row before anything costly is read of it, so a VIRTUAL
     * generated column of a row the join could never have kept is not worked out -- which is what
     * leaves {@code DELETE FROM o USING t WHERE t.a = o.a AND o.a = 5 AND t.g = 2} a statement that
     * completes over a relation whose {@code 10/a} raises where a is zero.
     *
     * <p>Only a written constant counts on the other side. Anything else may read something other
     * than the row being scanned, and deciding it here as well as where it was written would be
     * deciding two different things.
     */
    private static List<Expression> impliedEqualities(Expression qualification) {
        return qualification == null ? Collections.<Expression>emptyList()
                : impliedEqualities(FromResolver.conjunctsOf(qualification));
    }

    /** The same, for a qualification already written out in the parts that must all hold. */
    static List<Expression> impliedEqualities(List<Expression> conjuncts) {
        List<Expression> parts = readAsEqualities(conjuncts);
        Map<String, String> parent = new LinkedHashMap<>();
        Map<String, ColumnRef> written = new LinkedHashMap<>();
        for (Expression part : parts) {
            ColumnRef left = equalityRef(part, true);
            ColumnRef right = equalityRef(part, false);
            if (left == null || right == null) continue;
            written.put(refKey(left), left);
            written.put(refKey(right), right);
            unite(parent, refKey(left), refKey(right));
        }
        if (written.isEmpty()) return Collections.emptyList();
        Map<String, Literal> constants = new HashMap<>();
        for (Expression part : parts) {
            ColumnRef ref = equalityRef(part, true);
            Expression other = ref != null ? ((BinaryExpr) part).right() : null;
            if (ref == null) {
                ref = equalityRef(part, false);
                other = ref != null ? ((BinaryExpr) part).left() : null;
            }
            if (ref == null || !(other instanceof Literal)) continue;
            written.put(refKey(ref), ref);
            constants.put(classOf(parent, refKey(ref)), (Literal) other);
        }
        if (constants.isEmpty()) return Collections.emptyList();
        List<Expression> implied = new ArrayList<>();
        for (Map.Entry<String, ColumnRef> entry : written.entrySet()) {
            Literal value = constants.get(classOf(parent, entry.getKey()));
            if (value == null) continue;
            implied.add(new BinaryExpr(entry.getValue(), BinaryExpr.BinOp.EQUAL, value));
        }
        return implied;
    }

    /**
     * The parts of a qualification as PostgreSQL has them by the time it builds its classes.
     *
     * <p>Two of them are not written as the equality they are. {@code x IN (5)} is read as
     * {@code x = 5} while the statement is being read, so it puts 5 in x's class exactly as the
     * written equality does -- while a longer list is a comparison against an array, which says
     * nothing about one value, and so are NOT IN and the ANY spelling. And an OR says whatever
     * every one of its branches says, whichever branch turns out to hold: that is why {@code a = 5
     * OR a = 5} restricts a scan and {@code a = 5 OR a = 6} does not, and why {@code (a = 5 AND b =
     * 1) OR (a = 5 AND b = 2)} restricts it by the part the two have in common.
     */
    private static List<Expression> readAsEqualities(List<Expression> conjuncts) {
        List<Expression> parts = new ArrayList<>();
        for (Expression conjunct : conjuncts) addAsRead(conjunct, parts);
        return parts;
    }

    private static void addAsRead(Expression part, List<Expression> out) {
        Expression equality = writtenAsEquality(part);
        if (equality != null) {
            out.add(equality);
            return;
        }
        out.add(part);
        for (Expression common : heldByEveryBranch(part)) addAsRead(common, out);
    }

    /** The equality a one-element IN is, or null where the part is not one. */
    private static Expression writtenAsEquality(Expression part) {
        if (!(part instanceof InExpr)) return null;
        InExpr in = (InExpr) part;
        if (in.negated() || in.fromAny() || in.values() == null || in.values().size() != 1) {
            return null;
        }
        Expression only = in.values().get(0);
        // A sub-select answers with rows rather than with a value, and what a statement's
        // equalities say about the column it is joined on is a different question.
        if (only instanceof SubqueryExpr) return null;
        return new BinaryExpr(in.expr(), BinaryExpr.BinOp.EQUAL, only);
    }

    /** What an OR says whichever of its branches holds: the parts every one of them holds. */
    private static List<Expression> heldByEveryBranch(Expression part) {
        if (!(part instanceof BinaryExpr) || ((BinaryExpr) part).op() != BinaryExpr.BinOp.OR) {
            return Collections.emptyList();
        }
        List<Expression> branches = new ArrayList<>();
        collectOrBranches(part, branches);
        List<Expression> common = null;
        for (Expression branch : branches) {
            // Read as PostgreSQL has them here too, or two spellings of one comparison would look
            // like two different things and the branches would have nothing in common.
            List<Expression> held = readAsEqualities(FromResolver.conjunctsOf(branch));
            if (common == null) {
                common = held;
            } else {
                common.retainAll(held);
            }
            if (common.isEmpty()) return Collections.emptyList();
        }
        return common == null ? Collections.<Expression>emptyList() : common;
    }

    private static void collectOrBranches(Expression expr, List<Expression> out) {
        if (expr instanceof BinaryExpr && ((BinaryExpr) expr).op() == BinaryExpr.BinOp.OR) {
            collectOrBranches(((BinaryExpr) expr).left(), out);
            collectOrBranches(((BinaryExpr) expr).right(), out);
            return;
        }
        out.add(expr);
    }

    /** One side of an equality between two relation-qualified names, or of one with a constant. */
    private static ColumnRef equalityRef(Expression part, boolean left) {
        if (!(part instanceof BinaryExpr)) return null;
        BinaryExpr binary = (BinaryExpr) part;
        if (binary.op() != BinaryExpr.BinOp.EQUAL) return null;
        Expression side = left ? binary.left() : binary.right();
        if (!(side instanceof ColumnRef)) return null;
        ColumnRef ref = (ColumnRef) side;
        // A name written without its relation could be any of them, so it says nothing about one.
        return ref.table() == null || ref.column() == null ? null : ref;
    }

    private static String refKey(ColumnRef ref) {
        return ref.table().toLowerCase() + "." + ref.column().toLowerCase();
    }

    private static String classOf(Map<String, String> parent, String key) {
        String root = key;
        while (parent.containsKey(root) && !parent.get(root).equals(root)) root = parent.get(root);
        return root;
    }

    private static void unite(Map<String, String> parent, String one, String other) {
        parent.putIfAbsent(one, one);
        parent.putIfAbsent(other, other);
        String rootOne = classOf(parent, one);
        String rootOther = classOf(parent, other);
        if (!rootOne.equals(rootOther)) parent.put(rootOther, rootOne);
    }

    /**
     * Work out, for the rows a join kept, the VIRTUAL generated columns a writing statement reads
     * of the relation it brought in beside its target.
     *
     * <p>PostgreSQL puts the generation expression where the reference to the column stands, and an
     * assignment or a RETURNING item stands above the join, so the expression is evaluated for the
     * rows the join produced and for no others. Working it out as the extra relation was scanned
     * raised, for a row the join discards, an error the statement never asked for.
     */
    private void fillJoinedVirtuals(List<RowContext> joined, Object... named) {
        if (joined.isEmpty()) return;
        final Object[] parts = named;
        computeVirtualColumnsForOutput(joined, () -> columnsNamed(null, null, parts));
    }

    /** Whether any relation these rows were read from has a VIRTUAL generated column. */
    private boolean holdsVirtualRelation(List<RowContext> contexts) {
        for (RowContext ctx : contexts) {
            for (RowContext.TableBinding binding : ctx.getBindings()) {
                if (binding.table() != null && hasVirtualColumns(binding.table())) return true;
            }
        }
        return false;
    }

    /**
     * What the query around the one now running reads of the relations it holds, or null when it
     * reads every column -- which is also the answer when there is no query around it.
     *
     * <p>PostgreSQL pulls a derived table up into the query that reads it, so a {@code *} written
     * in one asks for what the query around it asks of it and no more: {@code SELECT k FROM (SELECT
     * * FROM t) s} reads k of t and nothing else.
     */
    Set<String> enclosingColumnsRead() {
        for (int i = columnDemands.size() - 1; i >= 0; i--) {
            ColumnDemand demand = columnDemands.get(i);
            // The query asking is the one whose own answer is still being worked out.
            if (demand.settling()) continue;
            return demand.readColumns();
        }
        return null;
    }

    /**
     * Whether the qualifications of whatever is reading {@code table} name this column. Anything
     * reading a row for its own reasons -- a constraint, an index, a write -- has named nothing,
     * and then every column is named. A relation under row-level security is read by its policy as
     * well as by the query, and the policy is not written in the query, so that names all of them
     * too.
     */
    private boolean scanQualificationReads(Table table, Column col) {
        if (columnDemands.isEmpty() || table.isRlsEnabled()) return true;
        return columnDemands.get(columnDemands.size() - 1).filters(col.getName());
    }

    /**
     * Whether the qualifications of whatever is reading {@code table} have already decided against
     * a row without reading any of its VIRTUAL generated columns.
     */
    private boolean scanQualificationRejects(Table table, String alias, Object[] row) {
        if (columnDemands.isEmpty() || table.isRlsEnabled()) return false;
        ColumnDemand demand = columnDemands.get(columnDemands.size() - 1);
        return qualificationRejects(table, alias, row, demand.decidableFor(this, table, alias));
    }

    /**
     * Whether a row this scan already passed over settled what the statement above asked the
     * sub-select being scanned, which is where PostgreSQL stops reading it.
     */
    private boolean scanAlreadyAnswered() {
        return !columnDemands.isEmpty()
                && columnDemands.get(columnDemands.size() - 1).settled();
    }

    /**
     * Note that this row settles what the sub-select was asked, where it holds everything the
     * question needs of one row.
     *
     * <p>A part that cannot be read against this row alone settles nothing, and neither does one
     * that raises while it is read: the scan then carries on to the rows behind it, which is what
     * it did before any of this.
     */
    private void noteScanAnswered(Table table, String alias, Object[] row) {
        if (columnDemands.isEmpty()) return;
        ColumnDemand demand = columnDemands.get(columnDemands.size() - 1);
        if (demand.settled()) return;
        List<Expression> settles = demand.settlingParts();
        if (settles == null) return;
        RowContext ctx = new RowContext(table, alias != null ? alias : table.getName(), row);
        for (Expression part : settles) {
            try {
                if (!executor.isTruthy(executor.evalExpr(part, ctx))) return;
            } catch (RuntimeException undecided) {
                return;
            }
        }
        demand.settle();
    }

    /**
     * The parts of a qualification that can be decided from one row of this relation alone and
     * that read none of its VIRTUAL generated columns.
     *
     * <p>PostgreSQL orders a scan's qualifications by what they cost to evaluate and stops at the
     * first one that is false. A qualification holding a generated column's expression costs more
     * than one that only compares a stored column, so the stored ones decide the row first and the
     * generation expression of a row they reject is never reached. That is what leaves {@code WHERE
     * a = 5 AND g = 2} readable on a relation whose {@code 10/a} raises where a is zero, whichever
     * order the two conjunctions were written in.
     *
     * <p>A part naming another relation of the same query is left out: what it says depends on
     * which row that relation paired with, and that is not settled while this one is being scanned.
     * A part naming a relation the query does not read at all is a row of the query above, which
     * that query settled before this one began -- so it reads the same here as it reads in the
     * qualification itself, and a row it rejects is a row the qualification rejects too. That is
     * what leaves the relation under {@code EXISTS (SELECT 1 FROM t2 WHERE t2.a = t1.a AND t2.g =
     * 2)} readable when {@code t2.g} raises for a row this outer row never pairs with. Anything
     * holding a query, a call or a window is left out whatever it names, because deciding one here
     * as well as where it was written would be deciding two different things.
     */
    List<Expression> decidableQualification(Table table, String alias, List<Expression> conjuncts,
                                            boolean lone, Set<String> relations) {
        return decidableQualification(table, alias, conjuncts, lone, relations, false);
    }

    /**
     * The same, for a query the statement above reads as one join with itself. What compares this
     * relation with a row that statement holds is then the condition of the join, read where the
     * two are paired and above this scan's own filter, so it settles which rows pair rather than
     * which rows there are and decides nothing here.
     */
    List<Expression> decidableQualification(Table table, String alias, List<Expression> conjuncts,
                                            boolean lone, Set<String> relations, boolean paired) {
        if (conjuncts == null || conjuncts.isEmpty()) return Collections.emptyList();
        String self = alias != null ? alias : table.getName();
        List<Expression> decidable = new ArrayList<>();
        for (Expression part : conjuncts) {
            // A part that reads false out of what is written in it alone is false for every row
            // there is, so it decides one whatever it names. PostgreSQL settles such a part while
            // it plans, and a scan it settles against never runs at all -- which is what leaves
            // WHERE false AND g = 2 OR false readable where 10/a raises for one of the rows.
            if (decidableHere(table, part, self, lone, relations, paired)
                    || executor.fromResolver.settledFalse(part)) {
                decidable.add(part);
            }
        }
        return decidable;
    }

    private static boolean decidableHere(Table table, Expression part, String self, boolean lone,
                                         Set<String> relations, boolean paired) {
        final boolean[] usable = {true};
        AstWalk.forEach(part, node -> {
            if (node instanceof Statement || node instanceof FunctionCallExpr
                    || node instanceof WindowFuncExpr || node instanceof OrderedSetAggExpr) {
                usable[0] = false;
                return;
            }
            if (!(node instanceof ColumnRef)) return;
            ColumnRef ref = (ColumnRef) node;
            if (ref.column() == null) {
                usable[0] = false;
                return;
            }
            boolean here = ref.table() == null ? lone : ref.table().equalsIgnoreCase(self);
            if (!here) {
                // A relation this query's FROM clause does not answer to is one an enclosing query
                // holds a row of, and that row stands still while this scan runs -- unless that
                // query reads this one as one join with itself, in which case what compares the two
                // is the condition of the join and is read where the rows are paired.
                if (paired || relations == null || ref.table() == null
                        || relations.contains(ref.table().toLowerCase())) {
                    usable[0] = false;
                }
                return;
            }
            int index = table.getColumnIndex(ref.column());
            if (index >= 0 && table.getColumns().get(index).isVirtual()) usable[0] = false;
        });
        return usable[0];
    }

    /** Whether such a qualification has already decided against a row. */
    boolean qualificationRejects(Table table, String alias, Object[] row,
                                 List<Expression> decidable) {
        if (decidable == null || decidable.isEmpty()) return false;
        RowContext ctx = new RowContext(table, alias != null ? alias : table.getName(), row);
        for (Expression part : decidable) {
            try {
                if (!executor.isTruthy(executor.evalExpr(part, ctx))) return true;
            } catch (RuntimeException undecided) {
                // Nothing was decided about the row, so nothing is decided against it.
            }
        }
        return false;
    }

    /**
     * Choose the rows a writing statement acts on with its qualification standing over the scan.
     *
     * <p>A sub-select written in the qualification is read as a join with the relation being
     * written, the way it is in a query that only reads, so what the rest of the qualification
     * says about the column the two are joined on restricts the sub-select's own scan.
     */
    private <T> T readQualified(Expression qualification, String lone,
                                java.util.function.Supplier<T> read) {
        executor.fromResolver.enterQualification(qualification, lone);
        try {
            return read.get();
        } finally {
            executor.fromResolver.exitQualification();
        }
    }

    /** The name a statement's qualification writes the relation it acts on under. */
    private static String writtenName(String alias, Table table) {
        String written = alias != null ? alias : table.getName();
        return written == null ? null : written.toLowerCase();
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
     * Whether a relation carries a rule that leaves ON CONFLICT nothing PostgreSQL will define.
     *
     * <p>An UPDATE rule counts wherever it stands, disabled or not, because the rewriter notices one
     * while it is looking for the rules this event fires and never asks whether that one would have
     * fired. An INSERT rule counts only when it fires and has an action to run, so a disabled one
     * and a DO INSTEAD NOTHING both leave ON CONFLICT accepted, as does a rule on any other event.
     * All four were measured against PostgreSQL.
     */
    private boolean rulesForbidOnConflict(String schema, String relation) {
        if (relation == null) return false;
        if (executor.database.getRule(schema, relation, "UPDATE") != null) return true;
        for (Database.StoredRule rule : executor.database.getRules(schema, relation, "INSERT")) {
            if (!rule.isNothing()) return true;
        }
        return false;
    }

    /**
     * A statement an INSTEAD rule stands in for never runs, so it has no rows of its own for a
     * RETURNING clause to report. PostgreSQL can answer one only from a rule action that carries a
     * RETURNING clause of its own, and refuses the statement outright otherwise — reporting the
     * count of a rule action instead left the caller a result with no columns in it. A rule with a
     * WHERE is refused as well: it takes rows out of the statement, and PostgreSQL accepts a
     * RETURNING clause only in the action of an unconditional rule, so there is nothing the rest of
     * the rows could be answered from either.
     */
    private void rejectReturningThroughInsteadRule(List<SelectStmt.SelectTarget> returning,
                                                   String verb, String relation,
                                                   List<Database.StoredRule> rules) {
        if (returning == null || returning.isEmpty()) return;
        boolean anyInstead = false;
        for (Database.StoredRule rule : rules) {
            if (!rule.isInstead()) continue;
            anyInstead = true;
            if (rule.getQualification() == null && rule.getBody() != null
                    && rule.getBody().toUpperCase().contains("RETURNING")) {
                return;
            }
        }
        if (!anyInstead) return;
        // Raised while the statement is being rewritten rather than while it is being read, so
        // there is no parse location to report and PostgreSQL sends none.
        MemgresException ex = new MemgresException("cannot perform " + verb
                + " RETURNING on relation \"" + relation + "\"", "0A000").suppressPosition();
        ex.setHint("You need an unconditional ON " + verb
                + " DO INSTEAD rule with a RETURNING clause.");
        throw ex;
    }

    /**
     * The rows an INSERT offers, written as expressions. A VALUES list stands as it is; a SELECT
     * is run first and each value carried across as it is rather than written out and read back,
     * which is what destroyed a bytea and an array on the way in.
     */
    private List<List<Expression>> insertSourceRows(InsertStmt stmt) {
        if (stmt.selectStmt() == null) return stmt.values();
        QueryResult subResult = executor.executeStatement(stmt.selectStmt());
        List<List<Expression>> valueRows = new ArrayList<>();
        for (Object[] subRow : subResult.getRows()) {
            List<Expression> exprRow = new ArrayList<>();
            for (Object val : subRow) {
                exprRow.add(val == null ? Literal.ofNull() : new ComputedValue(val));
            }
            valueRows.add(exprRow);
        }
        return valueRows;
    }

    /**
     * What a rule's actions handed back for the statement they stood in for: the shape of the
     * relation the statement named, and the rows the one action carrying a RETURNING clause
     * answered with. PostgreSQL allows a rule only one such action, so the rows are simply
     * collected in the order they came.
     */
    private static final class RuleAnswer {
        private Table shape;
        private final List<Object[]> rows = new ArrayList<>();
    }

    /** Keep what a rule action answered with, for a statement whose RETURNING it stands in for. */
    private static void collectRuleAnswer(RuleAnswer answer, QueryResult actionResult) {
        if (answer == null || actionResult == null || actionResult.getRows() == null) return;
        if (actionResult.getColumns() == null || actionResult.getColumns().isEmpty()) return;
        answer.rows.addAll(actionResult.getRows());
    }

    /**
     * The relation the statement named, as a row of it looks. Through a view that is the view's
     * own columns under their own names, because the relation the statement named is the view and
     * the write onto the base relation is a rewrite of it.
     */
    private Table ruledRelationShape(String relation, Table table) {
        List<String> viewCols = targetViewColumns();
        if (viewCols == null) return table;
        int[] projection = viewProjection(table);
        List<Column> cols = new ArrayList<>(viewCols.size());
        for (int i = 0; i < viewCols.size(); i++) {
            Column base = projection == null ? null : table.getColumns().get(projection[i]);
            cols.add(new Column(viewCols.get(i), base == null ? DataType.TEXT : base.getType(),
                    true, false, null));
        }
        return new Table(relation, cols);
    }

    /**
     * The rows an INSTEAD rule answers the statement's RETURNING clause with.
     *
     * <p>The statement itself never runs, so it has no rows of its own to report. PostgreSQL
     * answers it from the one action of the rule that carries a RETURNING clause: what that action
     * hands back is read as a row of the relation the statement named -- entry for entry, which is
     * why the action's list has to describe that relation column for column -- and the statement's
     * own RETURNING list is then worked out over it, keeping its own names, expressions and order.
     */
    private QueryResult insteadRuleReturning(List<SelectStmt.SelectTarget> returning, Table ruled,
                                             String alias, QueryResult.Type type, int count,
                                             List<Object[]> handedBack) {
        if (ruled == null) return QueryResult.command(type, count);
        // The row is already in the shape of the relation the statement named, so the mapping a
        // write through a view sets up would read it in the wrong order. Nothing else of this
        // statement is left to run: it answers from here.
        activeViewColMap = null;
        activeViewColOrder = null;
        activeViewColNames = null;
        int width = ruled.getColumns().size();
        List<Object[]> rows = new ArrayList<>();
        for (Object[] fromAction : handedBack) {
            Object[] asRuledRow = new Object[width];
            for (int i = 0; i < width && i < fromAction.length; i++) asRuledRow[i] = fromAction[i];
            rows.add(evalReturning(returning, ruled, alias, asRuledRow, null, asRuledRow));
        }
        return QueryResult.returning(type, buildReturningColumns(returning, ruled), rows, count);
    }

    /**
     * Apply the INSTEAD rules an UPDATE or DELETE carries. A rule written without a WHERE replaces
     * the statement outright, so the caller reports what the rule's actions did and the statement
     * never runs. A rule with a WHERE replaces the statement only for the rows it holds for: its
     * actions run for those, its qualification is handed back so the scan can leave them alone, and
     * the statement goes on to act on the rest and to report its own row count.
     *
     * @param returning  the statement's RETURNING list, which a rule that stands in for it has to
     *                   answer for
     * @param suppressed collects the WHERE of every rule that spoke for part of the statement
     * @return the result to report in place of the statement, or null when the statement runs
     */
    private QueryResult applyInsteadRule(String schema, String tableName, String event,
                                         QueryResult.Type type,
                                         Expression where, List<InsertStmt.SetClause> setClauses,
                                         String alias, List<SelectStmt.FromItem> extraFrom,
                                         List<SelectStmt.SelectTarget> returning,
                                         List<Expression> suppressed) {
        List<Database.StoredRule> rules = executor.database.getRules(schema, tableName, event);
        boolean anyInstead = false;
        boolean wholeStatement = false;
        for (Database.StoredRule rule : rules) {
            if (!rule.isInstead()) continue;
            anyInstead = true;
            if (rule.getQualification() == null) wholeStatement = true;
        }
        if (!anyInstead) return null;
        rejectConditionalInsteadOnView(schema, tableName, event, rules);
        rejectReturningThroughInsteadRule(returning, event, tableName, rules);
        // A rule whose command lands back on its own relation would re-enter itself forever.
        if (executor.isRuleExpanding(tableName, event)) {
            throw PgErrors.infiniteRecursionInRules(tableName);
        }
        // When a rule replaced the whole statement, the statement the client sent never runs and so
        // its row count is not the answer. PostgreSQL lets the last action that is the same kind of
        // command as the original speak for it, and reports nothing at all when no action is: a
        // DELETE replaced by an INSERT deleted nothing. A rule that claimed only some of the rows
        // leaves the statement to report its own count for the rest.
        int count = 0;
        boolean answersReturning = wholeStatement && returning != null && !returning.isEmpty();
        RuleAnswer answer = answersReturning ? new RuleAnswer() : null;
        for (Database.StoredRule rule : rules) {
            if (!rule.isInstead()) continue;
            if (!wholeStatement && rule.getQualification() != null) {
                suppressed.add(com.memgres.engine.parser.Parser.parseExpression(
                        rule.getQualification()));
            }
            if (rule.isNothing()) continue;
            // The rule's command speaks of OLD and NEW, which only mean something against a row the
            // statement would have touched. PG rewrites the query to say the same thing; here the
            // rows are read back through the relation and the command runs once per row.
            count = runRowRuleActions(tableName, event, rule.getBody(), where, setClauses,
                    wholeStatement ? type : null, rule.getQualification(), alias, extraFrom, answer);
        }
        if (answersReturning) {
            return insteadRuleReturning(returning, answer.shape, alias, type, count, answer.rows);
        }
        return wholeStatement ? QueryResult.command(type, count) : null;
    }

    /**
     * A view carrying an INSTEAD rule that speaks only for the rows its own qualification holds is
     * not one a write can be rewritten through: the rows the rule does not claim would have to be
     * written to the view itself, and a view has nothing to write to. PostgreSQL refuses the whole
     * statement rather than perform half of it, and names the trigger and the unconditional rule
     * that would take it instead. A rule written without a qualification claims the statement
     * outright, so nothing is left over and the write stands; a rule that runs beside the statement
     * rather than in place of it leaves the view as writable as it was.
     */
    private void rejectConditionalInsteadOnView(String schema, String relation, String event,
                                                List<Database.StoredRule> rules) {
        boolean conditional = false;
        for (Database.StoredRule rule : rules) {
            if (!rule.isInstead()) continue;
            if (rule.getQualification() == null) return;
            conditional = true;
        }
        if (!conditional || relation == null) return;
        String bare = RelationNamespace.bareName(relation);
        Database.ViewDef view = executor.database.getView(schema, bare);
        if (view == null || view.materialized()) return;
        String verb = "UPDATE".equals(event) ? "update"
                : "DELETE".equals(event) ? "delete from" : "insert into";
        throw ViewUpdatability.cannotWrite(verb, bare, ViewUpdatability.DETAIL_CONDITIONAL_RULE,
                false);
    }

    /**
     * Whether a qualified INSTEAD rule already spoke for this row. Its WHERE reads OLD as the row as
     * it stands and NEW as the row the assignments would make, the same way a trigger's WHEN does,
     * and PostgreSQL keeps the rows it holds for out of the statement it rewrote.
     */
    private boolean ruleSuppressesRow(List<Expression> quals, Table table, Object[] oldRow,
                                      Object[] newRow) {
        if (quals.isEmpty()) return false;
        RowContext ctx = new RowContext(table, "old", oldRow);
        if (newRow != null) ctx = ctx.merge(new RowContext(table, "new", newRow));
        for (Expression qual : quals) {
            if (executor.isTruthy(executor.evalExpr(qual, ctx))) return true;
        }
        return false;
    }

    /**
     * Run the actions of the DO ALSO rules on an UPDATE or DELETE. PostgreSQL adds them to the
     * statement rather than in place of it, so they run against the rows as they stand before the
     * statement writes and the statement itself goes on to run and report its own row count. Every
     * rule the event carries runs, in rule-name order, for the rows its own WHERE holds for.
     */
    private void applyAlsoRule(String schema, String tableName, String event, Expression where,
                               List<InsertStmt.SetClause> setClauses, String alias,
                               List<SelectStmt.FromItem> extraFrom) {
        List<Database.StoredRule> rules = executor.database.getRules(schema, tableName, event);
        boolean any = false;
        for (Database.StoredRule rule : rules) {
            if (!rule.isInstead() && !rule.isNothing()) {
                any = true;
                break;
            }
        }
        if (!any) return;
        if (executor.isRuleExpanding(tableName, event)) {
            throw PgErrors.infiniteRecursionInRules(tableName);
        }
        for (Database.StoredRule rule : rules) {
            if (rule.isInstead() || rule.isNothing()) continue;
            runRowRuleActions(tableName, event, rule.getBody(), where, setClauses, null,
                    rule.getQualification(), alias, extraFrom, null);
        }
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
                                  String qualification, String alias,
                                  List<SelectStmt.FromItem> extraFrom, RuleAnswer answer) {
        QueryResult affected = selectAffectedRows(tableName, alias, extraFrom, where, setClauses);
        // The assignments were projected after the relation's own columns, so the row carries the
        // old values first and what each assignment would make after them.
        int setCount = setClauses == null ? 0 : setClauses.size();
        List<Column> cols = affected.getColumns().subList(0, affected.getColumns().size() - setCount);
        Table rowShape = new Table(tableName, cols);
        // The relation as the statement named it, which through a view is the view's own columns:
        // that is the shape a RETURNING clause the rule answers for is worked out against.
        if (answer != null) answer.shape = rowShape;
        String[] actions = Database.ruleActions(ruleSql);
        int[] actionCounts = new int[actions.length];
        boolean[] actionSetsTag = new boolean[actions.length];
        // An action that never says OLD or NEW is the same command for every row, and PostgreSQL
        // runs it once for the whole statement rather than once per row — but not at all when the
        // statement touched nothing. Measured: two rows updated writes one log row, none writes
        // none, while the same action reading NEW.b writes one per row.
        boolean[] perRow = new boolean[actions.length];
        for (int a = 0; a < actions.length; a++) {
            perRow[a] = actionReadsRow(actions[a]);
        }
        boolean[] ranOnce = new boolean[actions.length];
        // The WHERE is read as SQL once and then judged against each row with OLD and NEW bound to
        // it, the same way the actions are. PostgreSQL keeps the qualification part of the query it
        // rewrote, where OLD and NEW are range-table entries an expression reads.
        Expression qualExpr = qualification == null ? null
                : com.memgres.engine.parser.Parser.parseExpression(qualification);
        executor.enterRuleExpansion(tableName, event);
        try {
            for (Object[] row : affected.getRows()) {
                // The row as OLD and as NEW: what an assignment would write stands where the
                // assignment names a column, and every other column is what it was.
                Object[] oldRow = new Object[cols.size()];
                Object[] newRow = new Object[cols.size()];
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
                    oldRow[i] = oldVal;
                    newRow[i] = newVal;
                }
                RowContext rowAliases = rowAliasContext(rowShape, oldRow, newRow);
                // A qualified rule fires only for the rows its WHERE holds for.
                if (qualExpr != null && !ruleQualificationHolds(qualExpr, rowAliases)) continue;
                for (int a = 0; a < actions.length; a++) {
                    if (!perRow[a] && ranOnce[a]) continue;
                    ranOnce[a] = true;
                    QueryResult actionResult = runRuleAction(actions[a], rowAliases);
                    collectRuleAnswer(answer, actionResult);
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

    /**
     * Whether a rule's qualification holds for one row, with OLD and NEW bound to it.
     *
     * <p>PostgreSQL evaluates the qualification as part of the query it rewrote, with OLD and NEW
     * as range-table entries, so the row reaches it as values an expression reads. Writing those
     * values into the qualification's text and reading the text back as SQL turned ordinary data
     * into syntax: a value that spelled {@code NEW.b} became the next substitution's target, a
     * {@code NEW.a} written inside one of the qualification's own string literals became whatever
     * the row held there, and a value with no literal spelling -- a bytea, an array -- became
     * whatever Java printed it as, so the WHERE compared something the row never held. Whatever it
     * raises is the statement's error, and what is not true does not fire: an unknown is not a yes.
     */
    private boolean ruleQualificationHolds(Expression qualification, RowContext rowAliases) {
        // A subquery inside the qualification reads OLD and NEW too, and it reaches them the way
        // any correlated reference does: through the context the row was pushed onto.
        executor.outerContextStack.push(rowAliases);
        try {
            return executor.isTruthy(executor.evalExpr(qualification, rowAliases));
        } finally {
            executor.outerContextStack.pop();
        }
    }

    /**
     * The values a row being inserted carries for each column, as {@code NEW} reads them. NEW
     * carries every column of it -- the ones the statement supplied and the ones it left to their
     * defaults — not a name left standing in the rule's own SQL.
     */
    private java.util.LinkedHashMap<String, Object> insertRuleNewValues(
            List<Expression> valueRow, RowContext valueCtx, InsertStmt stmt, Table table) {
        // Values arrive in the order the statement names them, and through a view that is
        // the view's own column order mapped onto the base table.
        List<String> colNames = stmt.columns();
        if (colNames == null) colNames = activeViewColOrder;
        if (colNames == null) {
            colNames = new ArrayList<>();
            for (Column c : table.getColumns()) colNames.add(c.getName());
        }
        java.util.LinkedHashMap<String, Object> values = new java.util.LinkedHashMap<>();
        for (Column c : table.getColumns()) values.put(c.getName(), null);
        Set<String> supplied = new HashSet<>();
        for (int ci = 0; ci < Math.min(colNames.size(), valueRow.size()); ci++) {
            // The relation's own spelling of the column, so a value supplied under another case
            // replaces the null placed for it rather than standing beside it.
            String colName = colNames.get(ci);
            int colIdx = table.getColumnIndex(colName);
            if (colIdx >= 0) colName = table.getColumns().get(colIdx).getName();
            // DEFAULT is not a value and evaluates to nothing: it asks for the column's default,
            // which is what a column the statement leaves out asks for as well.
            if (isDefaultLiteral(valueRow.get(ci))) continue;
            values.put(colName, executor.evalExpr(valueRow.get(ci), valueCtx));
            supplied.add(colName.toLowerCase());
        }
        // PostgreSQL fills in the default of every column the statement did not supply while it is
        // rewriting the statement, which is before the rules are applied, so NEW carries the
        // default rather than a null. A column the system computes is left alone: a generated
        // column is computed after the rewriter has run, and NEW reads null for one.
        for (Column c : table.getColumns()) {
            if (supplied.contains(c.getName().toLowerCase()) || c.isGenerated()) continue;
            values.put(c.getName(), insertRuleDefault(stmt, table, c));
        }
        return values;
    }

    /**
     * Whether a rule's WHERE holds for the row about to be written, with NEW standing for it.
     *
     * <p>NEW is a relation the qualification reads, not text written into it: a value that spells
     * {@code NEW.b} is a value, and the qualification's own string literals are left as they were.
     */
    private boolean insertRuleFires(Database.StoredRule rule, Table table,
                                    java.util.LinkedHashMap<String, Object> newValues) {
        if (rule.getQualification() == null) return true;
        return ruleQualificationHolds(
                com.memgres.engine.parser.Parser.parseExpression(rule.getQualification()),
                rowAliasContext(table, null, insertRuleNewRow(table, newValues)));
    }

    /**
     * The rows a qualified INSTEAD rule speaks for, by their place in the VALUES list. PostgreSQL
     * rewrites the statement to skip exactly those and to write the rest itself, so a rule with a
     * WHERE diverts the rows it matches and leaves the others where they were going.
     */
    private Set<Integer> insteadClaimedRows(List<Database.StoredRule> rules,
                                            List<List<Expression>> valueRows,
                                            List<RowContext> valueRowContexts,
                                            InsertStmt stmt, Table table) {
        Set<Integer> claimed = new HashSet<>();
        if (rules.isEmpty() || valueRows == null) return claimed;
        boolean anyQualifiedInstead = false;
        for (Database.StoredRule rule : rules) {
            if (rule.isInstead() && rule.getQualification() != null) {
                anyQualifiedInstead = true;
                break;
            }
        }
        if (!anyQualifiedInstead) return claimed;
        for (int i = 0; i < valueRows.size(); i++) {
            RowContext valueCtx = valueRowContexts == null ? null : valueRowContexts.get(i);
            java.util.LinkedHashMap<String, Object> newValues =
                    insertRuleNewValues(valueRows.get(i), valueCtx, stmt, table);
            for (Database.StoredRule rule : rules) {
                if (!rule.isInstead() || rule.getQualification() == null) continue;
                if (insertRuleFires(rule, table, newValues)) {
                    claimed.add(i);
                    break;
                }
            }
        }
        return claimed;
    }

    /**
     * Run the actions of the INSERT rules once per row the statement offered, in rule-name order,
     * with {@code NEW.col} replaced by the value that row carries and the rules whose WHERE the row
     * does not hold for left out.
     */
    private int runInsertRuleActions(List<Database.StoredRule> rules,
                                     List<List<Expression>> valueRows,
                                     List<RowContext> valueRowContexts,
                                     InsertStmt stmt, Table table, String ruleRelation,
                                     RuleAnswer answer) {
        if (valueRows == null || rules.isEmpty()) return 0;
        // A rule whose action writes back to the same relation would expand forever.
        executor.enterRuleExpansion(ruleRelation, "INSERT");
        try {
            return runInsertRuleActionRows(rules, valueRows, valueRowContexts, stmt, table, answer);
        } finally {
            executor.exitRuleExpansion(ruleRelation, "INSERT");
        }
    }

    /** Returns the row count of the last action that is itself an INSERT, or 0 when none is. */
    private int runInsertRuleActionRows(List<Database.StoredRule> rules,
                                        List<List<Expression>> valueRows,
                                        List<RowContext> valueRowContexts,
                                        InsertStmt stmt, Table table, RuleAnswer answer) {
        List<String> flatActions = new ArrayList<>();
        List<Integer> actionRule = new ArrayList<>();
        for (int r = 0; r < rules.size(); r++) {
            Database.StoredRule rule = rules.get(r);
            if (rule.isNothing()) continue;
            for (String action : Database.ruleActions(rule.getBody())) {
                flatActions.add(action);
                actionRule.add(r);
            }
        }
        int[] actionCounts = new int[flatActions.size()];
        boolean[] actionSetsTag = new boolean[flatActions.size()];
        for (int i = 0; i < valueRows.size(); i++) {
            RowContext valueCtx = valueRowContexts == null ? null : valueRowContexts.get(i);
            java.util.LinkedHashMap<String, Object> newValues =
                    insertRuleNewValues(valueRows.get(i), valueCtx, stmt, table);
            // Each rule's WHERE is judged once for the row, not once for each of its actions.
            boolean[] fires = new boolean[rules.size()];
            for (int r = 0; r < rules.size(); r++) {
                fires[r] = insertRuleFires(rules.get(r), table, newValues);
            }
            // NEW is a relation the action reads, not text written into it: a value that spells
            // NEW.b is a value, and the action's own string literals are left as they were.
            RowContext rowAliases =
                    rowAliasContext(table, null, insertRuleNewRow(table, newValues));
            for (int a = 0; a < flatActions.size(); a++) {
                if (!fires[actionRule.get(a)]) continue;
                QueryResult actionResult = runRuleAction(flatActions.get(a), rowAliases);
                collectRuleAnswer(answer, actionResult);
                if (actionResult != null && actionResult.getType() == QueryResult.Type.INSERT) {
                    actionSetsTag[a] = true;
                    actionCounts[a] += actionResult.getAffectedRows();
                }
            }
        }
        for (int a = flatActions.size() - 1; a >= 0; a--) {
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
            Table written = executor.resolveTableSafe(tableName);
            for (InsertStmt.SetClause set : setClauses) {
                targets.add(new SelectStmt.SelectTarget(ruleAssignedValue(tableName, written, set), null));
            }
        }
        List<SelectStmt.FromItem> from = new ArrayList<>();
        from.add(new SelectStmt.TableRef(null, tableName, alias));
        if (extraFrom != null) from.addAll(extraFrom);
        SelectStmt sel = new SelectStmt(false, null, targets, from, where,
                null, null, null, null, null, null, null, null, null, false);
        return executor.executeStatement(sel);
    }

    /**
     * Run one action of a rule with the row it fires for bound to OLD and NEW.
     *
     * <p>PostgreSQL rewrites the action with OLD and NEW as range-table entries and evaluates it,
     * so the row reaches the action as values that an expression reads. Writing those values into
     * the action's text instead made a value that spells {@code NEW.b} into the next
     * substitution's target, ordinary data into syntax, and a {@code NEW.a} written inside one of
     * the action's own string literals into whatever the row held there.
     */
    private QueryResult runRuleAction(String actionSql, RowContext rowAliases) {
        executor.outerContextStack.push(rowAliases);
        try {
            return executor.execute(actionSql, Cols.listOf());
        } finally {
            executor.outerContextStack.pop();
        }
    }

    /**
     * OLD and NEW as a rule's action reads them: relations of the target's shape, bound to the
     * row before and after the statement. Either may be left out, for the event that has no such
     * row -- an INSERT has no old one.
     */
    private RowContext rowAliasContext(Table shape, Object[] oldRow, Object[] newRow) {
        List<RowContext.TableBinding> bindings = new ArrayList<>();
        if (oldRow != null) bindings.add(new RowContext.TableBinding(shape, "old", oldRow));
        if (newRow != null) bindings.add(new RowContext.TableBinding(shape, "new", newRow));
        return new RowContext(bindings);
    }

    /** The values NEW carries for an inserted row, in the target's own column order. */
    private Object[] insertRuleNewRow(Table table,
                                      java.util.LinkedHashMap<String, Object> newValues) {
        Object[] newRow = new Object[table.getColumns().size()];
        for (int c = 0; c < newRow.length; c++) {
            newRow[c] = newValues.get(table.getColumns().get(c).getName());
        }
        return newRow;
    }

    /**
     * Whether a rule action reads the row it fires for, which is what decides between running it
     * once for the statement and once for every row. The answer is in the parse tree: a
     * {@code NEW.} written inside one of the action's own string literals is part of that string
     * and not a reference to anything. An action that will not parse is judged by its text, there
     * being no tree to ask.
     */
    private boolean actionReadsRow(String actionSql) {
        if (actionSql == null) return false;
        String sql = actionSql.trim();
        if (sql.endsWith(";")) sql = sql.substring(0, sql.length() - 1).trim();
        try {
            return AstWalk.anyMatch(com.memgres.engine.parser.Parser.parse(sql),
                    DmlExecutor::namesRowAlias);
        } catch (RuntimeException notParsed) {
            return mentionsRowAlias(actionSql);
        }
    }

    /** True when an AST node is a reference to OLD or NEW. */
    private static boolean namesRowAlias(Object node) {
        String qualifier = null;
        if (node instanceof ColumnRef) qualifier = ((ColumnRef) node).table();
        if (node instanceof WildcardExpr) qualifier = ((WildcardExpr) node).table();
        return qualifier != null
                && (qualifier.equalsIgnoreCase("old") || qualifier.equalsIgnoreCase("new"));
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
            // PostgreSQL names the write the way it names it everywhere else — a DELETE is
            // "delete from" — and its advice speaks of the write that was refused rather than
            // always of an update.
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
        if (executor.session != null && !mergeWritesRowItMatched) {
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
        if (executor.session != null && !mergeWritesRowItMatched) {
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
        // A VIRTUAL generated column is not stored, and PostgreSQL puts its generation expression
        // where the reference to it stood: RETURNING evaluates it for the columns it names and for
        // no others, so a write to a relation whose expression raises for the row it touched is
        // still a write that goes through.
        if (hasVirtualColumns(table)) {
            Set<String> returned = columnsNamed(table, alias, returning);
            row = computeVirtualColumns(table, row, returned);
            if (oldRow != null) oldRow = computeVirtualColumns(table, oldRow, returned);
            if (newRow != null) newRow = computeVirtualColumns(table, newRow, returned);
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
        // A row written through a partitioned table or an inheritance parent lives in the relation
        // below it, and where it lives and which relation it belongs to are that relation's to
        // answer: PostgreSQL's RETURNING names the partition the row went into, not the table the
        // statement wrote.
        Table storage = relationStoring(table, row);
        bindings.add(storage == table ? new RowContext.TableBinding(table, alias, row)
                : new RowContext.TableBinding(table, alias, row, storage));
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
                    addStarValues(values, table, src);
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
                    addStarValues(values, table, src);
                } else {
                    // Bare * or table.* — return current row (NEW behavior, backward compat)
                    addStarValues(values, table, row);
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

    /** Which of a relation and the relations below it holds a row the statement has written. */
    private Table relationStoring(Table table, Object[] row) {
        if (executor.session == null || row == null) return table;
        if (table.getPartitions().isEmpty() && table.getChildren().isEmpty()) return table;
        Table storage = executor.session.relationStoringRow(table, row);
        return storage == null ? table : storage;
    }

    /** The values a * stands for: the view's columns when the write is going through one. */
    private void addStarValues(List<Object> values, Table table, Object[] row) {
        int[] projection = viewProjection(table);
        if (projection == null) {
            for (Object val : row) values.add(val);
            return;
        }
        for (int index : projection) values.add(row[index]);
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
    private void checkMergeWhenClauses(MergeStmt stmt, Table targetTable, String targetAlias) {
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
                // A name is looked up before it is counted: PostgreSQL resolves each column of
                // the list against the target as it reads it, so a name that is not a column of
                // the target is reported as missing even when it is written twice.
                checkMergeInsertArm(wn, targetTable, stmt.targetTable());
                rejectDuplicateInsertColumns(wn.columns());
            }
        }
    }

    /**
     * What a MERGE's INSERT arm has to satisfy before any row is read — the rules an ordinary
     * INSERT is held to. A column list names columns of the target and is matched against the
     * VALUES list one for one. Without a column list the values fill the relation's columns from
     * the left, and PostgreSQL accepts fewer of them than there are columns but never more, so the
     * two complaints are not symmetric. A column the system computes may only be given DEFAULT, and
     * it is refused here rather than while the row is built, before anything has drawn a value from
     * an identity sequence the statement is not going to use.
     */
    private void checkMergeInsertArm(MergeStmt.WhenNotMatched wn, Table targetTable, String relationName) {
        if (targetTable == null || wn.doNothing()) return;
        List<Expression> values = wn.values();
        if (values == null) return;   // INSERT DEFAULT VALUES names nothing to match
        List<String> columns = wn.columns();
        if (columns != null) {
            requireTargetColumns(targetTable, relationName, columns);
            if (values.size() > columns.size()) {
                throw new MemgresException("INSERT has more expressions than target columns", "42601");
            }
            if (values.size() < columns.size()) {
                throw new MemgresException("INSERT has more target columns than expressions", "42601");
            }
        } else if (values.size() > targetTable.getColumns().size()) {
            throw new MemgresException("INSERT has more expressions than target columns", "42601");
        }
        for (int i = 0; i < values.size(); i++) {
            if (isDefaultLiteral(values.get(i))) continue;
            int colIdx = columns != null
                    ? targetTable.getColumnIndex(mapViewColumn(columns.get(i))) : i;
            if (colIdx < 0 || colIdx >= targetTable.getColumns().size()) continue;
            Column genCol = targetTable.getColumns().get(colIdx);
            if (genCol.isGenerated()) {
                throw new MemgresException("cannot insert a non-DEFAULT value into column \""
                        + genCol.getName() + "\"\n  Detail: Column \"" + genCol.getName()
                        + "\" is a generated column.", "428C9");
            }
            if (genCol.getDefaultValue() != null && genCol.getDefaultValue().contains("__identity__:always")) {
                throw new MemgresException("cannot insert a non-DEFAULT value into column \""
                        + genCol.getName() + "\"\n  Detail: Column \"" + genCol.getName()
                        + "\" is an identity column defined as GENERATED ALWAYS."
                        + "\n  Hint: Use OVERRIDING SYSTEM VALUE to override.", "428C9");
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
        requireTargetColumns(table, relationName, columnNames, false);
    }

    /**
     * The system columns an UPDATE may not assign to. PostgreSQL has a complaint of its own for
     * those — "cannot assign to system column" — so the target-column check leaves them to it.
     * There is no oid among them: a user table stopped carrying one in PostgreSQL 12, so writing
     * that name is a column that does not exist like any other.
     *
     * @param systemColumnsReported true when a later check reports an assignment to a system
     *        column in PostgreSQL's own words
     */
    private static final Set<String> UNASSIGNABLE_SYSTEM_COLUMNS = Cols.setOf(
            "ctid", "xmin", "xmax", "cmin", "cmax", "tableoid");

    private void requireTargetColumns(Table table, String relationName, List<String> columnNames,
                                      boolean systemColumnsReported) {
        if (table == null || columnNames == null) return;
        for (String name : columnNames) {
            if (name == null) continue;
            if (systemColumnsReported && UNASSIGNABLE_SYSTEM_COLUMNS.contains(name.toLowerCase())) {
                continue;
            }
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

    /**
     * The system columns RETURNING may name. PostgreSQL answers all six off the row the statement
     * wrote, so a write can say where it put the row and which relation it went into.
     */
    private static final Set<String> RETURNABLE_SYSTEM_COLUMNS = Cols.setOf(
            "ctid", "xmin", "xmax", "cmin", "cmax", "tableoid");

    /**
     * The system columns an INSERT into a partitioned table cannot report.
     *
     * <p>PostgreSQL routes such a row into a partition and hands RETURNING the values it wrote
     * rather than the tuple they were written into, so the four that describe the transaction and
     * the command behind the tuple have nothing to be read from and the statement is refused
     * before it writes anything. Where the row ended up and which relation took it are still
     * answerable, so ctid and tableoid are not among them.
     */
    private static final Set<String> UNROUTABLE_SYSTEM_COLUMNS =
            Cols.setOf("xmin", "xmax", "cmin", "cmax");

    /** Refuse a RETURNING that asks a routed insert for a system column it cannot answer. */
    private void rejectSystemColumnsInRoutedInsert(List<SelectStmt.SelectTarget> returning,
                                                   Table table, String alias) {
        if (returning == null || returning.isEmpty()) return;
        if (table.getPartitionStrategy() == null || table.getPartitions().isEmpty()) return;
        final boolean[] named = {false};
        AstWalk.forEach(returning, node -> {
            if (!(node instanceof ColumnRef)) return;
            ColumnRef ref = (ColumnRef) node;
            if (ref.column() == null
                    || !UNROUTABLE_SYSTEM_COLUMNS.contains(ref.column().toLowerCase())) {
                return;
            }
            // Only what the list reports of the written row. A sub-select reading some other
            // relation's system columns is a read like any other, and PostgreSQL answers it.
            if (namesWrittenRelation(ref.table(), table, alias)) named[0] = true;
        });
        if (named[0]) {
            throw new MemgresException("cannot retrieve a system column in this context", "0A000");
        }
    }

    /** Whether a qualifier in a RETURNING list stands for the relation the statement wrote. */
    private static boolean namesWrittenRelation(String qualifier, Table table, String alias) {
        if (qualifier == null) return true;
        return qualifier.equalsIgnoreCase(table.getName())
                || (alias != null && qualifier.equalsIgnoreCase(alias))
                || qualifier.equalsIgnoreCase("old") || qualifier.equalsIgnoreCase("new");
    }

    /** Validate that all column references in RETURNING exist in the table. */
    void validateReturning(List<SelectStmt.SelectTarget> returning, Table table) {
        validateReturning(returning, table, null);
    }

    /**
     * Validate that all column references in RETURNING exist in the table.
     *
     * <p>A write that brings in a second relation reads its RETURNING clause in the scope of both,
     * so a bare name the target does not hold is that relation's where it supplies one -- and a
     * name neither supplies is still reported, whether or not the statement writes a row, because
     * that is where PostgreSQL settles it.
     */
    void validateReturning(List<SelectStmt.SelectTarget> returning, Table table,
                           List<SelectStmt.FromItem> beside) {
        if (returning == null) return;
        Set<String> besideNames = besideReturningNames(beside);
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
                    // Every relation carries these without declaring them, and RETURNING reads
                    // them like any other name: PostgreSQL lets a write report where it put the
                    // row and which relation the row went into. There is no oid among them,
                    // because a user table stopped carrying one in PostgreSQL 12.
                    if (RETURNABLE_SYSTEM_COLUMNS.contains(cr.column().toLowerCase())) continue;
                    if (!returningColumnExists(cr.column(), table)) {
                        if (cr.table() == null
                                && besideNames.contains(cr.column().toLowerCase())) {
                            continue;
                        }
                        // PostgreSQL quotes a bare name and leaves a qualified one as written; the
                        // unquoted form also defeated the position enrichment, which finds the
                        // token in the statement text by its quotes.
                        throw new MemgresException("column " + (cr.table() == null
                                ? "\"" + cr.column() + "\"" : cr.table() + "." + cr.column())
                                + " does not exist", "42703");
                    }
                }
            }
        }
    }

    /**
     * The names a relation standing beside the write's target puts within a RETURNING clause's
     * reach, as far as the statement settles them on its own.
     *
     * <p>PostgreSQL resolves such a name against the whole range table while it is still analysing
     * the statement, so a relation named outright is asked for its columns and a derived one for
     * the names its select list gives them. What cannot be worked out of the text -- a star over
     * something this cannot enumerate, a column an alias list does not reach -- is left out, and a
     * name that stays unaccounted for is reported exactly as it was before.
     */
    private Set<String> besideReturningNames(List<SelectStmt.FromItem> beside) {
        Set<String> names = new HashSet<>();
        if (beside != null) {
            for (SelectStmt.FromItem item : beside) collectBesideNames(item, names);
        }
        return names;
    }

    private void collectBesideNames(SelectStmt.FromItem item, Set<String> names) {
        if (item instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef ref = (SelectStmt.TableRef) item;
            Table joined = executor.selectExecutor.lookupRelationOrNull(ref.schema(), ref.table());
            List<String> renamed = ref.columnAliases();
            if (joined == null) {
                if (renamed != null) addLowered(renamed, names);
                return;
            }
            for (int i = 0; i < joined.getColumns().size(); i++) {
                names.add((renamed != null && i < renamed.size()
                        ? renamed.get(i) : joined.getColumns().get(i).getName()).toLowerCase());
            }
        } else if (item instanceof SelectStmt.SubqueryFrom) {
            SelectStmt.SubqueryFrom sub = (SelectStmt.SubqueryFrom) item;
            List<String> renamed = sub.columnAliases();
            // An alias list renames the columns it reaches and leaves the rest under the names the
            // query gave them; how many of those there are is not something the text settles, so
            // only the renamed ones are counted where one is written.
            if (renamed != null && !renamed.isEmpty()) {
                addLowered(renamed, names);
                return;
            }
            collectQueryOutputNames(sub.subquery(), names);
        } else if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
            collectBesideNames(join.left(), names);
            collectBesideNames(join.right(), names);
        } else if (item instanceof SelectStmt.FunctionFrom) {
            SelectStmt.FunctionFrom call = (SelectStmt.FunctionFrom) item;
            if (call.columnAliases() != null && !call.columnAliases().isEmpty()) {
                addLowered(call.columnAliases(), names);
                return;
            }
            // A call producing one value per row exposes it under the name the FROM item answers
            // to, and one producing a row exposes that row under the same name.
            String named = call.alias() != null ? call.alias() : call.functionName();
            if (named != null) names.add(named.toLowerCase());
        }
    }

    private void collectQueryOutputNames(Statement query, Set<String> names) {
        if (query instanceof SetOpStmt) {
            // A set operation is answered under the names of its first branch.
            collectQueryOutputNames(((SetOpStmt) query).left(), names);
            return;
        }
        if (!(query instanceof SelectStmt)) return;
        SelectStmt select = (SelectStmt) query;
        if (select.targets() == null) return;
        for (SelectStmt.SelectTarget target : select.targets()) {
            if (target.alias() != null) {
                names.add(target.alias().toLowerCase());
            } else if (target.expr() instanceof ColumnRef) {
                names.add(((ColumnRef) target.expr()).column().toLowerCase());
            } else if (target.expr() instanceof WildcardExpr
                    && ((WildcardExpr) target.expr()).table() == null
                    && select.from() != null) {
                for (SelectStmt.FromItem item : select.from()) collectBesideNames(item, names);
            }
        }
    }

    private static void addLowered(List<String> written, Set<String> names) {
        for (String name : written) {
            if (name != null) names.add(name.toLowerCase());
        }
    }

    /**
     * Build Column metadata for a RETURNING clause read beside a second relation.
     *
     * <p>A name the target does not hold is the second relation's there, and it is answered with
     * that relation's own column: PostgreSQL reports the type the column has rather than the type
     * of a name it could not place. The shape is read off a row the second relation produced, which
     * is the row the value itself is read from.
     */
    private List<Column> buildReturningColumns(List<SelectStmt.SelectTarget> returning, Table table,
                                               RowContext beside) {
        if (beside == null) return buildReturningColumns(returning, table);
        List<Column> cols = new ArrayList<>();
        for (SelectStmt.SelectTarget target : returning) {
            if (target.alias() == null && target.expr() instanceof ColumnRef) {
                ColumnRef ref = (ColumnRef) target.expr();
                Column held = table.getColumnIndex(mapViewColumn(ref.column())) >= 0
                        ? null : besideColumnNamed(beside, ref.column());
                if (held != null) {
                    cols.add(held.getName().equalsIgnoreCase(ref.column()) ? held
                            : new Column(ref.column(), held.getType(), held.isNullable(),
                                    false, null));
                    continue;
                }
            }
            cols.addAll(buildReturningColumns(Cols.listOf(target), table));
        }
        return cols;
    }

    /** The column a relation standing beside the target supplies under a name, if one does. */
    private static Column besideColumnNamed(RowContext beside, String name) {
        for (RowContext.TableBinding binding : beside.getBindings()) {
            if (binding.table() == null) continue;
            int idx = binding.table().getColumnIndex(name);
            if (idx >= 0) return binding.table().getColumns().get(idx);
        }
        return null;
    }

    /** Build Column metadata for RETURNING clause. */
    private List<Column> buildReturningColumns(List<SelectStmt.SelectTarget> returning, Table table) {
        return buildReturningColumns(returning, table, (Table) null);
    }

    /**
     * What a RETURNING * answers with. Through a view that is the view's own columns under their
     * own names: the statement was rewritten onto the base relation, but the relation it named is
     * the view, and a base column the view does not show is not one of its columns.
     */
    private List<Column> returningStarColumns(Table table) {
        int[] projection = viewProjection(table);
        if (projection == null) return table.getColumns();
        List<String> names = targetViewColumns();
        List<Column> cols = new ArrayList<>(projection.length);
        for (int i = 0; i < projection.length; i++) {
            Column base = table.getColumns().get(projection[i]);
            cols.add(new Column(names.get(i), base.getType(), base.isNullable(), false, null));
        }
        return cols;
    }

    /** Build Column metadata for RETURNING clause, with optional source table for MERGE. */
    private List<Column> buildReturningColumns(List<SelectStmt.SelectTarget> returning, Table table, Table sourceTable) {
        List<Column> cols = new ArrayList<>();
        for (SelectStmt.SelectTarget target : returning) {
            if (target.expr() instanceof WildcardExpr) {
                WildcardExpr we = (WildcardExpr) target.expr();
                cols.addAll(returningStarColumns(table));
                if (we.table() == null && sourceTable != null) {
                    cols.addAll(sourceTable.getColumns());
                }
            } else if (target.alias() != null) {
                cols.add(new Column(target.alias(), DataType.TEXT, true, false, null));
            } else if (target.expr() instanceof ColumnRef) {
                ColumnRef cr = (ColumnRef) target.expr();
                String colName = cr.column();
                int idx = table.getColumnIndex(mapViewColumn(colName));
                if (idx >= 0) {
                    Column base = table.getColumns().get(idx);
                    // The answer carries the name the statement wrote, which through a view whose
                    // columns are renamed is the view's name and not the base relation's.
                    cols.add(colName.equalsIgnoreCase(base.getName()) ? base
                            : new Column(colName, base.getType(), base.isNullable(), false, null));
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
