package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Handles SELECT statement execution, aggregation, window functions, CTEs, and set operations.
 * Delegates heavy lifting to specialized evaluator classes:
 * - SelectAggregateEvaluator: GROUP BY, aggregates, GROUPING SETS
 * - SelectWindowEvaluator: window functions (OVER clauses)
 * - SelectCteExecutor: Common Table Expressions (WITH / WITH RECURSIVE)
 * - SelectSetOpExecutor: UNION / INTERSECT / EXCEPT
 */
class SelectExecutor {
    final AstExecutor executor;
    final SelectAggregateEvaluator aggregateEvaluator;
    final SelectWindowEvaluator windowEvaluator;
    final SelectCteExecutor cteExecutor;
    final SelectSetOpExecutor setOpExecutor;
    final GroupByValidator groupByValidator;
    final PlacementCheck placementCheck;

    /** Check if a column name is a PostgreSQL system column. */
    static boolean isSystemColumn(String name) {
        String lc = name.toLowerCase();
        return lc.equals("tableoid") || lc.equals("ctid") || lc.equals("xmin")
                || lc.equals("xmax") || lc.equals("cmin") || lc.equals("cmax");
    }

    static final Set<String> AGGREGATE_FUNCTIONS = Cols.setOf(
            "count", "sum", "avg", "min", "max", "string_agg", "array_agg",
            "bool_and", "bool_or", "every",
            "bit_and", "bit_or", "json_agg", "jsonb_agg", "json_object_agg", "jsonb_object_agg",
            "xmlagg", "grouping",
            "var_pop", "var_samp", "stddev_pop", "stddev_samp", "stddev", "variance",
            "bit_xor",
            "corr", "covar_pop", "covar_samp",
            "regr_slope", "regr_intercept", "regr_r2",
            "regr_count", "regr_avgx", "regr_avgy", "regr_sxx", "regr_syy", "regr_sxy",
            "json_arrayagg", "json_objectagg",
            "range_agg", "range_intersect_agg",
            "any_value"
    );

    static final Set<String> SRF_FUNCTION_NAMES = Cols.setOf("generate_series", "unnest", "regexp_matches",
            "json_array_elements", "jsonb_array_elements", "json_object_keys", "jsonb_object_keys",
            "json_array_elements_text", "jsonb_array_elements_text", "generate_subscripts",
            "json_each", "jsonb_each", "json_each_text", "jsonb_each_text",
            "jsonb_path_query", "jsonb_path_query_tz", "aclexplode", "string_to_table", "regexp_split_to_table",
            "pg_listening_channels", "pg_snapshot_xip", "txid_snapshot_xip",
            "skeys", "svals", "each", "_pg_expandarray");
    private static final Set<String> SRF_FUNCTIONS = SRF_FUNCTION_NAMES;

    SelectExecutor(AstExecutor executor) {
        this.executor = executor;
        this.aggregateEvaluator = new SelectAggregateEvaluator(this);
        this.windowEvaluator = new SelectWindowEvaluator(this);
        this.cteExecutor = new SelectCteExecutor(this);
        this.setOpExecutor = new SelectSetOpExecutor(this);
        this.groupByValidator = new GroupByValidator(this);
        this.placementCheck = new PlacementCheck(this);
    }

    /**
     * The stored relation a FROM item names, or null for anything whose columns cannot be read
     * from the catalog — a CTE, a view, a relation in another schema, a name that is not there.
     * Callers use it to decide whether a scope is knowable, so null has to mean "do not assume".
     */
    Table lookupRelationOrNull(String schemaName, String name) {
        if (name == null) return null;
        String lower = name.toLowerCase();
        for (Map<String, SelectStmt.CommonTableExpr> scope : executor.cteStack) {
            // A name mapped to null is a WITH item hidden from the body being run, so the stored
            // relation is what this name means here; anything else shadows the catalog.
            if (scope.containsKey(lower)) {
                if (scope.get(lower) != null) return null;
                break;
            }
        }
        Schema schema = executor.database.getSchema(
                schemaName != null ? schemaName : executor.defaultSchema());
        return schema == null ? null : schema.getTable(name);
    }

    /** True when the user has declared a function under this name, built-ins aside. */
    boolean hasUserFunction(String name) {
        return executor.database.getFunction(name) != null;
    }

    // ---- SELECT ----

    QueryResult executeSelect(SelectStmt stmt) {
        boolean pushedCteScope = false;
        if (stmt.withClauses() != null && !stmt.withClauses().isEmpty()) {
            Map<String, SelectStmt.CommonTableExpr> cteMap = new LinkedHashMap<>();
            for (SelectStmt.CommonTableExpr cte : stmt.withClauses()) {
                cteMap.put(cte.name().toLowerCase(), cte);
            }
            executor.cteStack.push(cteMap);
            for (String cteName : cteMap.keySet()) {
                executor.cteResultCache.remove(cteName);
            }
            pushedCteScope = true;
        }

        try {
            QueryResult result = executeSelectInner(stmt);
            // Force-execute any unreferenced DML CTEs (PG always executes data-modifying CTEs)
            if (stmt.withClauses() != null) {
                for (SelectStmt.CommonTableExpr cte : stmt.withClauses()) {
                    if (isDmlCte(cte.query())) {
                        String key = cte.name().toLowerCase();
                        if (!executor.cteResultCache.containsKey(key)) {
                            cteExecutor.executeCte(cte);
                        }
                    }
                }
            }
            return result;
        } finally {
            if (pushedCteScope) {
                executor.cteStack.pop();
            }
        }
    }

    private static boolean isDmlCte(Statement stmt) {
        return stmt instanceof InsertStmt || stmt instanceof UpdateStmt || stmt instanceof DeleteStmt;
    }

    private QueryResult executeSelectInner(SelectStmt stmt) {
        rejectMisplacedSrfs(stmt);
        rejectLockOnCollapsedRows(stmt);
        // A VALUES list is a query with no rows to read, so an aggregate or a window call in one
        // has nothing to aggregate or to be numbered against; the parser records where the SELECT
        // came from because after desugaring it is otherwise a FROM-less SELECT like any other.
        if (stmt.fromValues()) {
            for (SelectStmt.SelectTarget target : stmt.targets()) {
                placementCheck.reject(target.expr(), "VALUES");
            }
        }
        // SELECT without FROM
        if (stmt.from() == null || stmt.from().isEmpty()) {
            rejectSrfInAggregates(stmt);
            validateDistinctOn(stmt);
            windowEvaluator.validateWindowUsage(stmt, null);
            windowEvaluator.validateAfterWhere(stmt);
            boolean hasAgg = hasAggregateInTargets(stmt.targets())
                    || stmt.having() != null;
            if (hasAgg) {
                Table virtualTable = new Table("__virtual__",
                        Cols.listOf(new Column("__dummy__", DataType.INTEGER, true, false, null)));
                virtualTable.insertRow(new Object[]{1});
                RowContext virtualCtx = new RowContext(Cols.listOf(
                        new RowContext.TableBinding(virtualTable, "__virtual__", new Object[]{1})));
                List<RowContext> virtualContexts = Cols.listOf(virtualCtx);
                return aggregateEvaluator.executeAggregateSelect(stmt, virtualContexts, virtualCtx.getBindings());
            }
            return executeSelectExpressions(stmt);
        }

        List<RowContext> contexts = executor.fromResolver.resolveFromClause(stmt.from(), stmt.where());

        // PostgreSQL builds the range table before it analyses the rest of the query, so a name
        // that does not resolve is reported on its own even when the clauses are also wrong.
        rejectSrfInAggregates(stmt);
        validateDistinctOn(stmt);
        validateFromClause(stmt.from());

        List<RowContext.TableBinding> baseBindings;
        if (!contexts.isEmpty()) {
            baseBindings = contexts.get(0).getBindings();
        } else {
            baseBindings = executor.fromResolver.resolveTableBindings(stmt.from());
        }

        // The relations are resolved first: a window frame's offset is resolved against the
        // column the window is ordered by, which is one of them.
        windowEvaluator.validateWindowUsage(stmt, baseBindings);

        // Validate column references against table schema
        boolean simpleFrom = stmt.from().stream().allMatch(f -> f instanceof SelectStmt.TableRef);
        boolean hasJoins = stmt.from().stream().anyMatch(f -> f instanceof SelectStmt.JoinFrom);
        Map<String, Integer> usingMerges = new java.util.LinkedHashMap<>();
        collectUsingColumns(stmt.from(), usingMerges);
        Set<String> usingColumnsLower = new java.util.HashSet<>(usingMerges.keySet());
        if (!contexts.isEmpty()) {
            Set<String> ctxUsing = contexts.get(0).getUsingColumns();
            if (ctxUsing != null) usingColumnsLower.addAll(ctxUsing);
        }
        if ((simpleFrom || hasJoins) && !baseBindings.isEmpty()) {
            for (SelectStmt.SelectTarget target : stmt.targets()) {
                if (target.expr() instanceof ColumnRef && ((ColumnRef) target.expr()).column() != null && !"*".equals(((ColumnRef) target.expr()).column())
                        && !isSystemColumn(((ColumnRef) target.expr()).column())) {
                    ColumnRef cr = (ColumnRef) target.expr();
                    if (cr.table() == null) {
                        int matchCount = 0;
                        for (RowContext.TableBinding b : baseBindings) {
                            if (b.table().getColumnIndex(cr.column()) >= 0) matchCount++;
                        }
                        Integer merged = usingMerges.get(cr.column().toLowerCase());
                        int distinctSources = merged == null ? matchCount : matchCount - merged.intValue();
                        if (distinctSources > 1) {
                            throw new MemgresException("column reference \"" + cr.column() + "\" is ambiguous", "42702");
                        }
                        if (matchCount == 0) {
                            // A bare name matching a FROM item is a whole-row reference.
                            boolean wholeRow = false;
                            for (RowContext.TableBinding b : baseBindings) {
                                if ((b.alias() != null && b.alias().equalsIgnoreCase(cr.column()))
                                        || b.table().getName().equalsIgnoreCase(cr.column())) {
                                    wholeRow = true;
                                    break;
                                }
                            }
                            if (!wholeRow) {
                                MemgresException colEx = new MemgresException("column \"" + cr.column() + "\" does not exist", "42703");
                                // Try to generate a hint by finding the closest matching column
                                for (RowContext.TableBinding b : baseBindings) {
                                    String hint = RowContext.suggestClosestColumn(cr.column(), b.table());
                                    if (hint != null) { colEx.setHint(hint); break; }
                                }
                                throw colEx;
                            }
                        }
                    } else {
                        boolean tableFound = false;
                        boolean colFound = false;
                        boolean mayResolveViaAttributeNotation = false;
                        String hiddenByAlias = null; // alias that hides the real table name
                        for (RowContext.TableBinding b : baseBindings) {
                            // PG scoping: an alias hides the table's real name.
                            // Only match by alias; if alias differs from table name,
                            // do NOT match by raw table name.
                            boolean matchesByAlias = cr.table().equalsIgnoreCase(b.alias());
                            boolean matchesByTableName = cr.table().equalsIgnoreCase(b.table().getName());
                            if (!matchesByAlias && matchesByTableName
                                    && b.alias() != null && !b.alias().equalsIgnoreCase(b.table().getName())) {
                                // Table name is hidden by a different alias
                                hiddenByAlias = b.alias();
                                continue;
                            }
                            if (!matchesByAlias && !matchesByTableName) continue;
                            tableFound = true;
                            if (b.table().getColumnIndex(cr.column()) >= 0) { colFound = true; break; }
                            // Mirror ExprEvaluator.tryAttributeNotationFallback's guard: a
                            // single-column FROM-function (SRF) binding may resolve cr.column()
                            // at evaluation time via attribute notation (alias.name == name(alias),
                            // e.g. gs.date == date(gs)) even though it isn't a real column. Defer
                            // to evaluation time instead of rejecting here; ExprEvaluator raises
                            // the same 42703 if the fallback doesn't apply (unknown cast/function).
                            if (b.table().isFunctionResult() && b.table().getColumns().size() == 1) {
                                mayResolveViaAttributeNotation = true;
                            }
                        }
                        if (!tableFound) {
                            if (hiddenByAlias != null) {
                                MemgresException ex = new MemgresException(
                                        "invalid reference to FROM-clause entry for table \"" + cr.table() + "\"", "42P01");
                                ex.setHint("Perhaps you meant to reference the table alias \"" + hiddenByAlias + "\".");
                                throw ex;
                            }
                            throw new MemgresException("missing FROM-clause entry for table \"" + cr.table() + "\"", "42P01");
                        }
                        // A column a join merged is still readable through the relation it came
                        // from, so a qualified reference to it is valid even though the shape
                        // this check reads has already merged the column away.
                        boolean mergedAway = usingMerges.containsKey(cr.column().toLowerCase());
                        if (!colFound && !mayResolveViaAttributeNotation && !mergedAway) {
                            // A qualified reference is named in full, the way RowContext names it
                            // when the same lookup fails at evaluation time.
                            MemgresException colEx = new MemgresException(
                                    "column " + cr.table() + "." + cr.column() + " does not exist", "42703");
                            for (RowContext.TableBinding b : baseBindings) {
                                String hint = RowContext.suggestClosestColumn(cr.column(), b.table());
                                if (hint != null) { colEx.setHint(hint); break; }
                            }
                            throw colEx;
                        }
                    }
                }
            }
        }

        rejectAmbiguousQualifiedRefs(stmt, baseBindings, usingColumnsLower);

        // Validate array subscript type errors for empty tables
        if (contexts.isEmpty() && simpleFrom && !baseBindings.isEmpty()) {
            for (SelectStmt.SelectTarget target : stmt.targets()) {
                if (target.expr() instanceof BinaryExpr
                        && (((BinaryExpr) target.expr()).op() == BinaryExpr.BinOp.JSON_ARROW
                            || ((BinaryExpr) target.expr()).op() == BinaryExpr.BinOp.JSON_SUBSCRIPT)) {
                    BinaryExpr bin = (BinaryExpr) target.expr();
                    if (bin.left() instanceof ColumnRef && bin.right() instanceof Literal
                            && ((Literal) bin.right()).literalType() == Literal.LiteralType.STRING) {
                        Literal lit = (Literal) bin.right();
                        ColumnRef cr = (ColumnRef) bin.left();
                        for (RowContext.TableBinding tb : baseBindings) {
                            int colIdx = tb.table().getColumnIndex(cr.column());
                            if (colIdx >= 0) {
                                Column col = tb.table().getColumns().get(colIdx);
                                if (col.getArrayElementType() != null) {
                                    try { Integer.parseInt(lit.value()); } catch (NumberFormatException e) {
                                        throw new MemgresException("array subscript must have type integer", "42804");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // WHERE
        if (stmt.where() != null) {
            placementCheck.reject(stmt.where(), "WHERE");
            // Pre-flight type validation of WHERE clause (PG checks at plan time)
            // Only validate for simple single-table SELECTs (not CTEs/subqueries/joins)
            if (simpleFrom && baseBindings.size() == 1 && !hasJoins
                    && (stmt.withClauses() == null || stmt.withClauses().isEmpty())
                    && executor.cteStack.isEmpty()) {
                executor.validateWhereTypesAgainstTable(stmt.where(), baseBindings.get(0).table());
            }
            if (!contexts.isEmpty()) {
                Object testVal = executor.evalExpr(stmt.where(), contexts.get(0));
                if (testVal instanceof Number) {
                    throw new MemgresException("argument of WHERE must be type boolean, not type " +
                            TypeCoercion.inferType(testVal).getPgName(), "42804");
                }
            }
            contexts = contexts.stream()
                    .filter(ctx -> executor.isTruthy(executor.evalExpr(stmt.where(), ctx)))
                    .collect(Collectors.toList());
            // Track index scan: if WHERE clause is on a table that has indexes, count it
            if (!baseBindings.isEmpty()) {
                for (RowContext.TableBinding binding : baseBindings) {
                    Table srcTable = binding.sourceTable != null ? binding.sourceTable : binding.table;
                    if (srcTable != null && !srcTable.getIndexes().isEmpty()) {
                        srcTable.incrementIdxScanCount();
                        break;
                    }
                }
            }
        }

        // Everything WHERE stands in front of. PostgreSQL reads WHERE before HAVING, the window
        // definitions, ORDER BY, GROUP BY, LIMIT and OFFSET, so what it says about a query wrong
        // in two clauses is what the earlier one is wrong about.
        windowEvaluator.validateAfterWhere(stmt);

        // Check if this query uses aggregation
        boolean hasGroupBy = stmt.groupBy() != null && !stmt.groupBy().isEmpty();
        boolean hasGroupingSets = stmt.groupingSets() != null && !stmt.groupingSets().isEmpty();
        // An aggregate anywhere — select list, HAVING or ORDER BY — makes the query grouped,
        // so every other expression in it must itself be grouped or aggregated.
        boolean hasAggregates = hasAggregateInTargets(stmt.targets()) ||
                (stmt.having() != null && containsAggregate(stmt.having())) ||
                hasAggregateInOrderBy(stmt.orderBy()) ||
                hasAggregateInWindowDefs(stmt.windowDefs());

        // HAVING makes the query grouped whether or not anything in it aggregates: with no
        // GROUP BY the whole table is one group, so the query answers at most one row and its
        // select list is judged against that group. WHERE does not change that — the group is
        // there even when no row reaches it, which is why "WHERE false HAVING true" still
        // answers one row.
        if (hasGroupBy || hasGroupingSets || hasAggregates || stmt.having() != null) {
            // A star stands for the columns it expands to, and grouping is judged — and GROUP BY
            // ordinals counted — over those columns, not over the star itself.
            SelectStmt grouped = stmt;
            List<SelectStmt.SelectTarget> groupedTargets =
                    expandTargetsForOrdinals(stmt.targets(), baseBindings);
            if (groupedTargets != stmt.targets()) grouped = stmt.withTargets(groupedTargets);
            groupByValidator.validate(grouped, groupedTargets, baseBindings);
            return aggregateEvaluator.executeAggregateSelect(grouped, contexts, baseBindings);
        }

        // An ORDER BY expression is an output column the query does not print, and a set in one is
        // expanded like any other output column: PostgreSQL answers SELECT a FROM t ORDER BY
        // generate_series(1,2) with each row of t twice. Only a sort key that is not already a
        // select target is expanded here -- one that is a target is expanded by the projection,
        // and expanding it twice would square the rows.
        contexts = expandContextsForOrderBySrfs(stmt, contexts);

        // Check for window functions in targets, in a DISTINCT ON key, or ordered by without
        // being selected. A window function anywhere needs the whole partition, so the query
        // cannot be answered a row at a time.
        if (hasWindowFunctionInTargets(stmt.targets())
                || distinctOnNeedsWindowEvaluation(stmt)
                || windowEvaluator.orderByNeedsWindowEvaluation(stmt)) {
            return windowEvaluator.executeWindowSelect(stmt, contexts, baseBindings);
        }

        // Non-aggregate SELECT path
        List<Column> resultColumns = new ArrayList<>();
        List<java.util.function.Function<RowContext, Object>> projections = new ArrayList<>();

        Set<Integer> srfIndices = new HashSet<>();
        buildProjections(stmt.targets(), baseBindings, resultColumns, projections, usingColumnsLower,
                srfIndices);

        // An ordinal ORDER BY counts output columns, so a star target has to be expanded
        // first: SELECT * FROM t ORDER BY 2 means the table's second column.
        List<SelectStmt.SelectTarget> ordinalTargets = expandTargetsForOrdinals(stmt.targets(), baseBindings);
        List<SelectStmt.OrderByItem> resolvedOrderBy = resolveOrderBy(stmt.orderBy(), ordinalTargets);

        // Validate: for SELECT DISTINCT, ORDER BY expressions must appear in select list
        if (stmt.distinct() && (stmt.distinctOn() == null || stmt.distinctOn().isEmpty()) && resolvedOrderBy != null && !resolvedOrderBy.isEmpty()) {
            Set<String> targetExprs = new java.util.HashSet<>();
            for (SelectStmt.SelectTarget t : ordinalTargets) {
                if (t.alias() != null) targetExprs.add(t.alias().toLowerCase());
                targetExprs.add(t.expr().toString().toLowerCase());
                if (t.expr() instanceof ColumnRef) targetExprs.add(((ColumnRef) t.expr()).column().toLowerCase());
            }
            for (SelectStmt.OrderByItem ob : resolvedOrderBy) {
                String obStr = ob.expr().toString().toLowerCase();
                String obCol = ob.expr() instanceof ColumnRef ? ((ColumnRef) ob.expr()).column().toLowerCase() : obStr;
                if (!targetExprs.contains(obStr) && !targetExprs.contains(obCol)) {
                    throw new MemgresException("for SELECT DISTINCT, ORDER BY expressions must appear in select list", "42P10");
                }
            }
        }

        boolean hasSrf = !srfIndices.isEmpty();

        if (!hasSrf) {
            sortContexts(contexts, resolvedOrderBy);
        }

        // DISTINCT ON
        if (stmt.distinctOn() != null && !stmt.distinctOn().isEmpty()) {
            Set<String> seen = new LinkedHashSet<>();
            List<RowContext> deduped = new ArrayList<>();
            for (RowContext ctx : contexts) {
                StringBuilder keyBuilder = new StringBuilder();
                for (Expression expr : stmt.distinctOn()) {
                    Object val = executor.evalExpr(expr, ctx);
                    keyBuilder.append(val == null ? "\0NULL" : val.toString()).append('\1');
                }
                if (seen.add(keyBuilder.toString())) {
                    deduped.add(ctx);
                }
            }
            contexts = deduped;
        }

        // Row-level locking. Every base relation in the FROM tree is a lock target, or just
        // the ones named by FOR UPDATE OF; a join is not an excuse to lock nothing.
        Set<String> lockTargets = null;
        if (stmt.lockClause() != null && executor.session != null && stmt.from() != null) {
            lockTargets = new LinkedHashSet<>();
            if (!stmt.lockClause().ofTables().isEmpty()) {
                for (String of : stmt.lockClause().ofTables()) lockTargets.add(of.toLowerCase());
            } else {
                for (SelectStmt.FromItem fi : stmt.from()) collectLockTargets(fi, lockTargets);
            }
            if (lockTargets.isEmpty()) lockTargets = null;
        }
        if (lockTargets != null && stmt.lockClause() != null && stmt.lockClause().skipLocked()) {
            final Set<String> targets = lockTargets;
            int effectiveLimit = Integer.MAX_VALUE;
            int effectiveOffset = 0;
            if (stmt.offset() != null) {
                Object offVal = executor.evalExpr(stmt.offset(), contexts.isEmpty() ? null : contexts.get(0));
                if (offVal instanceof Number) {
                    effectiveOffset = clampToSize(((Number) offVal).longValue(), Integer.MAX_VALUE);
                }
            }
            if (stmt.limit() != null) {
                Object limVal = executor.evalExpr(stmt.limit(), contexts.isEmpty() ? null : contexts.get(0));
                if (limVal instanceof Number) {
                    effectiveLimit = clampToSize(((Number) limVal).longValue(), Integer.MAX_VALUE);
                }
            }
            int needed = effectiveOffset + effectiveLimit;
            List<RowContext> filtered = new ArrayList<>();
            final String lockMode = stmt.lockClause().mode();
            for (RowContext ctx : contexts) {
                boolean lockable = true;
                RowContext.TableBinding lockedBinding = null;
                for (RowContext.TableBinding b : ctx.getBindings()) {
                    if (isLockTarget(b, targets)) {
                        String tName = b.table().getName();
                        if (executor.database.isRowBeingUpdatedByOtherSession(b.row(), executor.session)) {
                            lockable = false;
                            break;
                        }
                        if (!b.table().getRows().contains(b.row())) {
                            lockable = false;
                            break;
                        }
                        if (!executor.database.tryLockRow(tName, b.row(), executor.session, lockMode)) {
                            lockable = false;
                        } else {
                            lockedBinding = b;
                        }
                        break;
                    }
                }
                if (lockable && lockedBinding != null && stmt.where() != null) {
                    RowContext freshCtx = new RowContext(lockedBinding.table(),
                            lockedBinding.alias(), lockedBinding.row());
                    if (!executor.isTruthy(executor.evalExpr(stmt.where(), freshCtx))) {
                        executor.database.unlockRow(lockedBinding.table().getName(), lockedBinding.row());
                        lockable = false;
                    }
                }
                if (lockable) {
                    filtered.add(ctx);
                    if (filtered.size() >= needed) break;
                }
            }
            contexts = filtered;
        }

        if (lockTargets != null && stmt.lockClause() != null && !stmt.lockClause().skipLocked()) {
            final Set<String> targets = lockTargets;
            SelectStmt.LockClause lc = stmt.lockClause();
            final String lockMode = lc.mode();
            for (RowContext ctx : contexts) {
                for (RowContext.TableBinding b : ctx.getBindings()) {
                    if (!isLockTarget(b, targets)) continue;
                    String tName = b.table().getName();
                    Object[] lockRow = executor.database.liveRowForSnapshotCopy(b.row(), executor.session);
                    if (lc.nowait()) {
                        if (!executor.database.tryLockRow(tName, lockRow, executor.session, lockMode)) {
                            throw new MemgresException("could not obtain lock on row in relation \"" + tName + "\"", "55P03");
                        }
                    } else {
                        executor.database.lockRowWaiting(tName, lockRow, executor.session, lockMode);
                    }
                }
            }
        }

        // Apply WITH TIES on contexts before projection (needs access to ORDER BY expressions)
        if (stmt.withTies() && resolvedOrderBy != null && !resolvedOrderBy.isEmpty()
                && stmt.limit() != null && !hasSrf) {
            // Apply OFFSET on contexts
            if (stmt.offset() != null) {
                long offRaw = limitOffsetValue(stmt.offset(), false);
                int off = clampToSize(offRaw < 0 ? 0 : offRaw, contexts.size());
                if (off > 0 && off < contexts.size()) {
                    contexts = new ArrayList<>(contexts.subList(off, contexts.size()));
                } else if (off >= contexts.size()) {
                    contexts = new ArrayList<>();
                }
            }
            // Apply LIMIT WITH TIES on contexts
            long limRaw = limitOffsetValue(stmt.limit(), true);
            int lim = clampToSize(limRaw < 0 ? Integer.MAX_VALUE : limRaw, contexts.size());
            if (limRaw >= 0 && lim < contexts.size() && !contexts.isEmpty()) {
                RowContext lastCtx = contexts.get(lim - 1);
                int end = lim;
                while (end < contexts.size()) {
                    RowContext candidateCtx = contexts.get(end);
                    boolean tied = true;
                    for (SelectStmt.OrderByItem item : resolvedOrderBy) {
                        Object va = executor.evalExpr(item.expr(), lastCtx);
                        Object vb = executor.evalExpr(item.expr(), candidateCtx);
                        if (va == null && vb == null) continue;
                        if (va == null || vb == null) { tied = false; break; }
                        if (executor.compareValues(va, vb) != 0) { tied = false; break; }
                    }
                    if (!tied) break;
                    end++;
                }
                contexts = new ArrayList<>(contexts.subList(0, end));
            } else if (lim == 0) {
                contexts = new ArrayList<>();
            }
        }

        // Project
        List<Object[]> resultRows = projectRows(contexts, projections, srfIndices);

        // For SRF queries, apply ORDER BY after SRF expansion
        if (hasSrf && resolvedOrderBy != null && !resolvedOrderBy.isEmpty()) {
            final List<SelectStmt.OrderByItem> ob = resolvedOrderBy;
            resultRows.sort((a, b) -> {
                for (SelectStmt.OrderByItem item : ob) {
                    int colIdx = resolveOrderByToColumnIndex(item.expr(), stmt.targets());
                    if (colIdx < 0) continue;
                    Object va = colIdx < a.length ? a[colIdx] : null;
                    Object vb = colIdx < b.length ? b[colIdx] : null;
                    int cmp;
                    if (va == null && vb == null) cmp = 0;
                    else if (va == null || vb == null) {
                        boolean effectiveNullsFirst = item.nullsFirst() != null ? item.nullsFirst() : item.descending();
                        if (va == null) cmp = effectiveNullsFirst ? -1 : 1;
                        else cmp = effectiveNullsFirst ? 1 : -1;
                        if (cmp != 0) return cmp; else continue;
                    } else {
                        String collation = item.expr() instanceof CollateExpr
                                ? ((CollateExpr) item.expr()).collation() : null;
                        if (collation != null && va instanceof String && vb instanceof String) {
                            cmp = TypeCoercion.compareStringsWithCollation((String) va, (String) vb, collation);
                        } else {
                            cmp = executor.compareValues(va, vb);
                        }
                    }
                    if (item.descending()) cmp = -cmp;
                    if (cmp != 0) return cmp;
                }
                return 0;
            });
        }

        resultRows = applyDistinct(stmt, resultRows);
        // Skip applyOffsetAndLimit if WITH TIES was already applied on contexts
        if (!(stmt.withTies() && resolvedOrderBy != null && !resolvedOrderBy.isEmpty()
                && stmt.limit() != null && !hasSrf)) {
            resultRows = applyOffsetAndLimit(stmt, resultRows);
        }

        return QueryResult.select(resultColumns, resultRows);
    }

    /**
     * A row lock names the rows it locks, so the query has to still have them.
     *
     * <p>{@code FOR UPDATE} locks the base-table row behind each output row. DISTINCT, GROUP BY,
     * HAVING, an aggregate and a window function each turn many input rows into one output row —
     * after them there is no longer one row to point a lock at, so PostgreSQL refuses the
     * combination outright rather than locking some arbitrary member of the group. It reports the
     * clauses in this order, and only the first one it meets.
     *
     * <p>A lock applies to the relations under it, so a sub-select in FROM is judged the same way
     * unless {@code OF} named which relations to lock instead.
     */
    private void rejectLockOnCollapsedRows(SelectStmt stmt) {
        SelectStmt.LockClause lock = stmt.lockClause();
        if (lock == null) return;
        checkLockable(stmt, lock, lock.ofTables() == null || lock.ofTables().isEmpty());
    }

    private void checkLockable(SelectStmt stmt, SelectStmt.LockClause lock, boolean descend) {
        String mode = "FOR " + (lock.mode() == null ? "UPDATE" : lock.mode());
        if (stmt.distinct()) throw lockNotAllowed(mode, "DISTINCT clause");
        if ((stmt.groupBy() != null && !stmt.groupBy().isEmpty())
                || (stmt.groupingSets() != null && !stmt.groupingSets().isEmpty())) {
            throw lockNotAllowed(mode, "GROUP BY clause");
        }
        if (stmt.having() != null) throw lockNotAllowed(mode, "HAVING clause");
        if (hasAggregateInTargets(stmt.targets()) || hasAggregateInOrderBy(stmt.orderBy())) {
            throw lockNotAllowed(mode, "aggregate functions");
        }
        if (hasWindowFunctionInTargets(stmt.targets())
                || (stmt.orderBy() != null && orderByHasWindowFunction(stmt.orderBy()))) {
            throw lockNotAllowed(mode, "window functions");
        }
        if (!descend || stmt.from() == null) return;
        for (SelectStmt.FromItem item : stmt.from()) {
            if (item instanceof SelectStmt.SubqueryFrom) {
                Statement inner = ((SelectStmt.SubqueryFrom) item).subquery();
                if (inner instanceof SelectStmt) checkLockable((SelectStmt) inner, lock, true);
            }
        }
    }

    private boolean orderByHasWindowFunction(List<SelectStmt.OrderByItem> orderBy) {
        for (SelectStmt.OrderByItem item : orderBy) {
            if (containsWindowFunction(item.expr())) return true;
        }
        return false;
    }

    private static MemgresException lockNotAllowed(String mode, String what) {
        return new MemgresException(mode + " is not allowed with " + what, "0A000");
    }

    // ---- Expression analysis helpers (shared across delegates) ----

    boolean isAggregateFunction(String name) {
        String stripped = FunctionEvaluator.stripSchemaPrefix(name.toLowerCase());
        return AGGREGATE_FUNCTIONS.contains(stripped)
                || executor.database.hasAggregate(stripped);
    }

    boolean containsAggregate(Expression expr) {
        if (expr instanceof SubqueryExpr) return false;
        if (expr instanceof ExistsExpr) return false;
        if (expr instanceof AnyAllExpr) return false;
        if (expr instanceof ArraySubqueryExpr) return false;
        if (expr instanceof OrderedSetAggExpr) return true;
        // A window function is not itself an aggregate, but what it reads may be one: it runs
        // over the grouped result, so sum(sum(v)) OVER () and rank() OVER (ORDER BY sum(v)) are
        // queries with one row per group exactly as sum(v) on its own would be.
        if (expr instanceof WindowFuncExpr) {
            WindowFuncExpr wf = (WindowFuncExpr) expr;
            for (Expression arg : wf.args()) {
                if (containsAggregate(arg)) return true;
            }
            return windowSpecContainsAggregate(wf.partitionBy(), wf.orderBy());
        }
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            if (isAggregateFunction(fn.name())) return true;
            for (Expression arg : fn.args()) {
                if (containsAggregate(arg)) return true;
            }
            return false;
        }
        if (expr instanceof BinaryExpr) return containsAggregate(((BinaryExpr) expr).left()) || containsAggregate(((BinaryExpr) expr).right());
        if (expr instanceof CustomOperatorExpr) { CustomOperatorExpr c = (CustomOperatorExpr) expr; return (c.left() != null && containsAggregate(c.left())) || containsAggregate(c.right()); }
        if (expr instanceof UnaryExpr) return containsAggregate(((UnaryExpr) expr).operand());
        if (expr instanceof CastExpr) return containsAggregate(((CastExpr) expr).expr());
        if (expr instanceof IsJsonExpr) return containsAggregate(((IsJsonExpr) expr).expr());
        if (expr instanceof IsNullExpr) return containsAggregate(((IsNullExpr) expr).expr());
        if (expr instanceof InExpr) return containsAggregate(((InExpr) expr).expr());
        if (expr instanceof LikeExpr) return containsAggregate(((LikeExpr) expr).left()) || containsAggregate(((LikeExpr) expr).pattern());
        if (expr instanceof CaseExpr) {
            CaseExpr c = (CaseExpr) expr;
            for (CaseExpr.WhenClause when : c.whenClauses()) {
                if (containsAggregate(when.condition()) || containsAggregate(when.result())) return true;
            }
            return c.elseExpr() != null && containsAggregate(c.elseExpr());
        }
        return false;
    }

    boolean hasAggregateInTargets(List<SelectStmt.SelectTarget> targets) {
        for (SelectStmt.SelectTarget target : targets) {
            if (containsAggregate(target.expr())) return true;
        }
        return false;
    }

    /**
     * A WINDOW clause entry is part of the window specification of every call that names it, so
     * an aggregate written there groups the query just as one written in an inline OVER does.
     */
    private boolean hasAggregateInWindowDefs(List<SelectStmt.WindowDef> defs) {
        if (defs == null) return false;
        for (SelectStmt.WindowDef def : defs) {
            if (windowSpecContainsAggregate(def.partitionBy(), def.orderBy())) return true;
        }
        return false;
    }

    private boolean windowSpecContainsAggregate(List<Expression> partitionBy,
                                                List<SelectStmt.OrderByItem> orderBy) {
        if (partitionBy != null) {
            for (Expression p : partitionBy) {
                if (containsAggregate(p)) return true;
            }
        }
        if (orderBy != null) {
            for (SelectStmt.OrderByItem o : orderBy) {
                if (containsAggregate(o.expr())) return true;
            }
        }
        return false;
    }

    private boolean hasAggregateInOrderBy(List<SelectStmt.OrderByItem> orderBy) {
        if (orderBy == null) return false;
        for (SelectStmt.OrderByItem ob : orderBy) {
            if (containsAggregate(ob.expr())) return true;
        }
        return false;
    }

    boolean containsWindowFunction(Expression expr) {
        if (expr instanceof SubqueryExpr) return false;
        if (expr instanceof ExistsExpr) return false;
        if (expr instanceof AnyAllExpr) return false;
        if (expr instanceof ArraySubqueryExpr) return false;
        if (expr instanceof WindowFuncExpr) return true;
        if (expr instanceof BinaryExpr) return containsWindowFunction(((BinaryExpr) expr).left()) || containsWindowFunction(((BinaryExpr) expr).right());
        if (expr instanceof CustomOperatorExpr) { CustomOperatorExpr c = (CustomOperatorExpr) expr; return (c.left() != null && containsWindowFunction(c.left())) || containsWindowFunction(c.right()); }
        if (expr instanceof UnaryExpr) return containsWindowFunction(((UnaryExpr) expr).operand());
        if (expr instanceof CastExpr) return containsWindowFunction(((CastExpr) expr).expr());
        if (expr instanceof IsNullExpr) return containsWindowFunction(((IsNullExpr) expr).expr());
        if (expr instanceof CaseExpr) {
            CaseExpr c = (CaseExpr) expr;
            for (CaseExpr.WhenClause when : c.whenClauses()) {
                if (containsWindowFunction(when.condition()) || containsWindowFunction(when.result())) return true;
            }
            return c.elseExpr() != null && containsWindowFunction(c.elseExpr());
        }
        if (expr instanceof FunctionCallExpr) {
            for (Expression arg : ((FunctionCallExpr) expr).args()) {
                if (containsWindowFunction(arg)) return true;
            }
        }
        return false;
    }

    boolean hasWindowFunctionInTargets(List<SelectStmt.SelectTarget> targets) {
        for (SelectStmt.SelectTarget target : targets) {
            if (containsWindowFunction(target.expr())) return true;
        }
        return false;
    }

    // ---- Shared SELECT helpers ----

    /**
     * Replaces star targets with one target per column the star stands for, so ORDER BY
     * ordinals and DISTINCT validation see the real output column list. Returns the
     * original list when there is no star to expand.
     */
    private List<SelectStmt.SelectTarget> expandTargetsForOrdinals(
            List<SelectStmt.SelectTarget> targets, List<RowContext.TableBinding> bindings) {
        if (targets == null || bindings == null || bindings.isEmpty()) return targets;
        boolean hasStar = false;
        for (SelectStmt.SelectTarget t : targets) {
            if (t.expr() instanceof WildcardExpr) { hasStar = true; break; }
        }
        if (!hasStar) return targets;
        List<SelectStmt.SelectTarget> out = new ArrayList<>();
        for (SelectStmt.SelectTarget t : targets) {
            if (!(t.expr() instanceof WildcardExpr)) { out.add(t); continue; }
            WildcardExpr w = (WildcardExpr) t.expr();
            for (RowContext.TableBinding b : bindings) {
                if (w.table() != null && !b.alias().equalsIgnoreCase(w.table())
                        && !b.table().getName().equalsIgnoreCase(w.table())) {
                    continue;
                }
                for (Column c : b.table().getColumns()) {
                    out.add(new SelectStmt.SelectTarget(new ColumnRef(b.alias(), c.getName()), null));
                }
            }
        }
        return out.isEmpty() ? targets : out;
    }

    /** True when a DISTINCT ON key is a window function, which only the window pass can evaluate. */
    private boolean distinctOnNeedsWindowEvaluation(SelectStmt stmt) {
        if (stmt.distinctOn() == null) return false;
        for (Expression on : stmt.distinctOn()) {
            if (containsWindowFunction(on)) return true;
        }
        return false;
    }

    List<SelectStmt.OrderByItem> resolveOrderBy(List<SelectStmt.OrderByItem> orderBy,
                                                  List<SelectStmt.SelectTarget> targets) {
        if (orderBy == null || orderBy.isEmpty()) return orderBy;

        List<SelectStmt.OrderByItem> resolved = new ArrayList<>();
        for (SelectStmt.OrderByItem item : orderBy) {
            Expression expr = item.expr();

            // A constant in ORDER BY is an output-column position and nothing else, exactly as
            // in GROUP BY: an integer outside the select list is out of range rather than a
            // value to sort every row by, and a constant that is not an integer at all names
            // no column and sorts nothing.
            Integer pos = GroupByValidator.integerConstant(expr);
            if (pos != null) {
                if (pos < 1 || pos > targets.size()) {
                    throw new MemgresException("ORDER BY position " + pos + " is not in select list", "42P10");
                }
                expr = targets.get(pos - 1).expr();
            } else if (expr instanceof Literal) {
                throw new MemgresException("non-integer constant in ORDER BY", "42601");
            }

            if (expr instanceof ColumnRef && ((ColumnRef) expr).table() == null) {
                ColumnRef ref = (ColumnRef) expr;
                for (SelectStmt.SelectTarget target : targets) {
                    if (target.alias() != null && ref.column().equalsIgnoreCase(target.alias())) {
                        expr = target.expr();
                        break;
                    }
                }
            }

            resolved.add(new SelectStmt.OrderByItem(expr, item.descending(), item.nullsFirst()));
        }
        return resolved;
    }

    int resolveOrderByToColumnIndex(Expression expr, List<SelectStmt.SelectTarget> targets) {
        if (expr instanceof Literal && ((Literal) expr).literalType() == Literal.LiteralType.INTEGER) {
            Literal lit = (Literal) expr;
            int pos = Integer.parseInt(lit.value());
            if (pos >= 1 && pos <= targets.size()) return pos - 1;
        }

        if (expr instanceof ColumnRef && ((ColumnRef) expr).table() == null) {
            ColumnRef ref = (ColumnRef) expr;
            for (int i = 0; i < targets.size(); i++) {
                SelectStmt.SelectTarget target = targets.get(i);
                if (target.alias() != null && ref.column().equalsIgnoreCase(target.alias())) {
                    return i;
                }
                if (ref.column().equalsIgnoreCase(executor.exprToAlias(target.expr()))) {
                    return i;
                }
            }
        }

        for (int i = 0; i < targets.size(); i++) {
            if (targets.get(i).expr().equals(expr)) return i;
        }

        return -1;
    }

    List<Object[]> applyDistinct(SelectStmt stmt, List<Object[]> resultRows) {
        // DISTINCT ON already deduped on its key expressions above (~line 407); it must never
        // also run this plain full-projection DISTINCT pass. The parser sets stmt.distinct() =
        // true for DISTINCT ON too (SelectParser.parseSelectBody), so without this guard two rows
        // with distinct DISTINCT ON keys but an incidentally-equal projection collapse into one
        // (mtask-8 Group 4) -- PostgreSQL keeps both.
        if (stmt.distinct() && (stmt.distinctOn() == null || stmt.distinctOn().isEmpty())) {
            Set<RowKey> seen = new LinkedHashSet<>();
            List<Object[]> deduped = new ArrayList<>();
            for (Object[] row : resultRows) {
                if (seen.add(new RowKey(row))) {
                    deduped.add(row);
                }
            }
            return deduped;
        }
        return resultRows;
    }


    /**
     * LIMIT and OFFSET are bigint in PostgreSQL. Narrowing them to int made any value above
     * 2^31 wrap negative and then fail its own sign check, reporting a negative limit for a
     * number the caller wrote as positive.
     *
     * <p>A fractional argument is cast to bigint rather than truncated, so LIMIT 1.5 is two rows
     * and OFFSET 0.5 skips one. numeric rounds half away from zero and float8 rounds half to
     * even, which is what the two casts do; and the sign is judged after the rounding, so
     * LIMIT -0.4 is a limit of zero rather than a negative one.
     */
    long limitOffsetValue(Expression expr, boolean isLimit) {
        Object raw = executor.evalExpr(expr, null);
        if (raw == null) return -1; // NULL means "no limit", as in PG
        long value;
        try {
            value = raw instanceof java.math.BigDecimal
                    ? roundToBigint((java.math.BigDecimal) raw)
                    : raw instanceof Double || raw instanceof Float
                            ? (long) Math.rint(((Number) raw).doubleValue())
                            : executor.toLong(raw);
        } catch (NumberFormatException e) {
            throw new MemgresException(
                    "invalid input syntax for type bigint: \"" + raw + "\"", "22P02");
        }
        if (value < 0) {
            throw new MemgresException((isLimit ? "LIMIT" : "OFFSET") + " must not be negative",
                    isLimit ? "2201W" : "2201X");
        }
        return value;
    }

    /** numeric to bigint, rounding half away from zero as the cast does. */
    private static long roundToBigint(java.math.BigDecimal value) {
        return value.setScale(0, java.math.RoundingMode.HALF_UP).longValue();
    }

    /** Clamp a bigint row count down to something a list index can hold. */
    private static int clampToSize(long value, int size) {
        if (value >= size) return size;
        return (int) value;
    }

    List<Object[]> applyOffsetAndLimit(SelectStmt stmt, List<Object[]> resultRows) {
        if (stmt.offset() != null) {
            long offRaw = limitOffsetValue(stmt.offset(), false);
            int off = clampToSize(offRaw, resultRows.size());
            if (offRaw >= 0) {
                if (off > 0 && off < resultRows.size()) {
                    resultRows = new ArrayList<>(resultRows.subList(off, resultRows.size()));
                } else if (off >= resultRows.size()) {
                    resultRows = Cols.listOf();
                }
            }
        }
        if (stmt.limit() != null) {
            long limRaw = limitOffsetValue(stmt.limit(), true);
            int lim = clampToSize(limRaw < 0 ? Integer.MAX_VALUE : limRaw, resultRows.size());
            if (limRaw >= 0 && lim < resultRows.size()) {
                if (stmt.withTies() && stmt.orderBy() != null && !stmt.orderBy().isEmpty() && !resultRows.isEmpty()) {
                    // WITH TIES: include additional rows tied with the last row by ORDER BY values
                    Object[] lastRow = resultRows.get(lim - 1);
                    // Resolve ORDER BY column indices
                    int[] obIndices = new int[stmt.orderBy().size()];
                    for (int oi = 0; oi < stmt.orderBy().size(); oi++) {
                        obIndices[oi] = resolveOrderByToColumnIndex(stmt.orderBy().get(oi).expr(), stmt.targets());
                    }
                    int end = lim;
                    while (end < resultRows.size()) {
                        Object[] candidate = resultRows.get(end);
                        boolean tied = true;
                        for (int obIdx : obIndices) {
                            if (obIdx < 0) continue;
                            Object va = obIdx < lastRow.length ? lastRow[obIdx] : null;
                            Object vb = obIdx < candidate.length ? candidate[obIdx] : null;
                            if (va == null && vb == null) continue;
                            if (va == null || vb == null) { tied = false; break; }
                            if (executor.compareValues(va, vb) != 0) { tied = false; break; }
                        }
                        if (!tied) break;
                        end++;
                    }
                    resultRows = new ArrayList<>(resultRows.subList(0, end));
                } else {
                    resultRows = new ArrayList<>(resultRows.subList(0, lim));
                }
            }
        }
        return resultRows;
    }

    /**
     * @param srfProjections collects the index of every projection whose value is a set to expand
     *     into rows. A star target contributes several projections for the one target, so
     *     counting targets would point the expansion at the wrong column.
     */
    private void buildProjections(List<SelectStmt.SelectTarget> targets,
                                   List<RowContext.TableBinding> baseBindings,
                                   List<Column> resultColumns,
                                   List<java.util.function.Function<RowContext, Object>> projections,
                                   Set<String> usingColumnsForDedup,
                                   Set<Integer> srfProjections) {
        for (SelectStmt.SelectTarget target : targets) {
            int projectionStart = projections.size();
            if (target.expr() instanceof WildcardExpr) {
                WildcardExpr w = (WildcardExpr) target.expr();
                if (w.table() != null) {
                    for (int bIdx = 0; bIdx < baseBindings.size(); bIdx++) {
                        RowContext.TableBinding binding = baseBindings.get(bIdx);
                        if (binding.alias().equalsIgnoreCase(w.table()) ||
                                binding.table().getName().equalsIgnoreCase(w.table())) {
                            final int bindingIdx = bIdx;
                            for (int i = 0; i < binding.table().getColumns().size(); i++) {
                                resultColumns.add(copyColumnWithOid(binding.table(), i));
                                final int colIdx = i;
                                projections.add(ctx -> ctx.getBindings().get(bindingIdx).row()[colIdx]);
                            }
                        }
                    }
                } else {
                    Set<String> emittedUsingCols = new java.util.HashSet<>();
                    for (int bIdx = 0; bIdx < baseBindings.size(); bIdx++) {
                        RowContext.TableBinding binding = baseBindings.get(bIdx);
                        final int bindingIdx = bIdx;
                        for (int i = 0; i < binding.table().getColumns().size(); i++) {
                            String colNameLower = binding.table().getColumns().get(i).getName().toLowerCase();
                            if (usingColumnsForDedup != null && usingColumnsForDedup.contains(colNameLower)
                                    && !emittedUsingCols.add(colNameLower)) {
                                continue;
                            }
                            resultColumns.add(copyColumnWithOid(binding.table(), i));
                            final int colIdx = i;
                            // For USING columns, use COALESCE(left, right) so unmatched
                            // right rows show the right side's value (PG behavior)
                            if (usingColumnsForDedup != null && usingColumnsForDedup.contains(colNameLower)) {
                                // Find the same column in other bindings for COALESCE
                                final String usingCol = colNameLower;
                                final int leftBindingIdx = bindingIdx;
                                final int leftColIdx = colIdx;
                                projections.add(ctx -> {
                                    Object val = ctx.getBindings().get(leftBindingIdx).row()[leftColIdx];
                                    if (val != null) return val;
                                    // Left is null — search other bindings for the same column
                                    for (int bi = 0; bi < ctx.getBindings().size(); bi++) {
                                        if (bi == leftBindingIdx) continue;
                                        RowContext.TableBinding ob = ctx.getBindings().get(bi);
                                        for (int ci = 0; ci < ob.table().getColumns().size(); ci++) {
                                            if (ob.table().getColumns().get(ci).getName().equalsIgnoreCase(usingCol)) {
                                                Object rval = ob.row()[ci];
                                                if (rval != null) return rval;
                                            }
                                        }
                                    }
                                    return null;
                                });
                            } else {
                                projections.add(ctx -> ctx.getBindings().get(bindingIdx).row()[colIdx]);
                            }
                        }
                    }
                }
            } else if (target.expr() instanceof CompositeStarExpr) {
                CompositeStarExpr cse = (CompositeStarExpr) target.expr();
                String typeName = resolveCompositeTypeFromBindings(cse.expr(), baseBindings);
                if (typeName == null) typeName = executor.resolveCompositeTypeNamePublic(cse.expr(), null);
                List<String> recordFields = typeName != null ? null
                        : JsonFunctions.recordFieldNames(cse.expr());
                if (recordFields != null) {
                    // A record built by the call itself, not by a declared type: the names come
                    // from the function, and one call can produce a whole set of such records.
                    for (int fi = 0; fi < recordFields.size(); fi++) {
                        resultColumns.add(new Column(recordFields.get(fi), DataType.TEXT, true, false, null));
                        final Expression innerExpr = cse.expr();
                        final int fieldIdx = fi;
                        projections.add(ctx -> recordField(executor.evalExpr(innerExpr, ctx), fieldIdx));
                    }
                } else if (typeName != null) {
                    List<CreateTypeStmt.CompositeField> fields = executor.database.getCompositeType(typeName);
                    if (fields != null) {
                        final String resolvedTypeName = typeName;
                        for (int fi = 0; fi < fields.size(); fi++) {
                            CreateTypeStmt.CompositeField field = fields.get(fi);
                            DataType fieldType = DataType.fromPgName(field.typeName());
                            resultColumns.add(new Column(field.name(), fieldType != null ? fieldType : DataType.TEXT, true, false, null));
                            final Expression innerExpr = cse.expr();
                            final String fieldName = field.name();
                            projections.add(ctx -> {
                                Object val = executor.evalExpr(innerExpr, ctx);
                                return executor.extractCompositeField(val, fieldName, resolvedTypeName);
                            });
                        }
                    }
                }
            } else {
                Expression expr = target.expr();
                String alias = target.alias();
                if (alias == null) alias = executor.exprToAlias(expr);
                Column sourceCol = null;
                String sourceTableName = null;
                String sourceSchemaName = null;
                int sourceColIdx = -1;
                if (expr instanceof ColumnRef && ((ColumnRef) expr).column() != null) {
                    ColumnRef cr = (ColumnRef) expr;
                    for (RowContext.TableBinding b : baseBindings) {
                        if (cr.table() != null && !cr.table().equalsIgnoreCase(b.alias())
                                && !cr.table().equalsIgnoreCase(b.table().getName())) continue;
                        int colIdx = b.table().getColumnIndex(cr.column());
                        if (colIdx >= 0) {
                            sourceCol = b.table().getColumns().get(colIdx);
                            sourceTableName = b.table().getName();
                            sourceColIdx = colIdx;
                            break;
                        }
                    }
                }
                if (sourceCol != null) {
                    // Carry over domainTypeName/compositeTypeName/arrayElementType too, not just
                    // enumTypeName+precision+scale -- dropping arrayElementType here made a
                    // projected "region_t[]" column indistinguishable from a scalar "region_t"
                    // column (both have type=ENUM, enumTypeName="region_t"), which caused
                    // PgWireValueFormatter.columnTypeOid to advertise the enum element's OID
                    // instead of the array's for e.g. "SELECT regions FROM sellers".
                    Column rc = new Column(alias, sourceCol.getType(), sourceCol.isNullable(), sourceCol.isPrimaryKey(), null,
                            sourceCol.getEnumTypeName(), sourceCol.getPrecision(), sourceCol.getScale(), null,
                            sourceCol.getDomainTypeName(), sourceCol.getCompositeTypeName(), sourceCol.getArrayElementType());
                    if (sourceTableName != null) {
                        String schemaKey = sourceSchemaName != null ? sourceSchemaName : "public";
                        int tblOid = executor.systemCatalog.getOid("rel:" + schemaKey + "." + sourceTableName);
                        rc.setTableOid(tblOid);
                        rc.setAttNum((short) (sourceColIdx + 1));
                    }
                    resultColumns.add(rc);
                } else {
                    resultColumns.add(buildProjectedColumn(alias, expr, baseBindings));
                }
                if (findSrfCall(expr) != null) {
                    projections.add(ctx -> evalSrfExpandedTarget(expr, ctx));
                } else {
                    projections.add(ctx -> executor.evalExpr(expr, ctx));
                }
            }
            if (srfProjections != null && isSrfCall(target.expr())) {
                for (int pi = projectionStart; pi < projections.size(); pi++) srfProjections.add(pi);
            }
        }
    }

    /**
     * One field of a record, or the same field of every record when the value is a whole set --
     * which keeps the fields of {@code (jsonb_each(x)).*} expanding in step with each other.
     */
    private static Object recordField(Object value, int fieldIdx) {
        if (value instanceof List<?>) {
            List<Object> fields = new ArrayList<>();
            for (Object element : (List<?>) value) fields.add(recordField(element, fieldIdx));
            return fields;
        }
        return value instanceof RecordValue ? ((RecordValue) value).valueAt(fieldIdx) : null;
    }

    /** Copy a column from a table, setting tableOid and attNum for RowDescription metadata. */
    private Column copyColumnWithOid(Table table, int colIdx) {
        Column src = table.getColumns().get(colIdx);
        Column rc = new Column(src.getName(), src.getType(), src.isNullable(), src.isPrimaryKey(), null,
                src.getEnumTypeName(), src.getPrecision(), src.getScale(), null,
                src.getDomainTypeName(), src.getCompositeTypeName(), src.getArrayElementType());
        String schemaKey = "public";
        int tblOid = executor.systemCatalog.getOid("rel:" + schemaKey + "." + table.getName());
        rc.setTableOid(tblOid);
        rc.setAttNum((short) (colIdx + 1));
        return rc;
    }

    private String resolveCompositeTypeFromBindings(Expression expr, List<RowContext.TableBinding> baseBindings) {
        if (expr instanceof ColumnRef) {
            ColumnRef ref = (ColumnRef) expr;
            for (RowContext.TableBinding b : baseBindings) {
                if (ref.table() != null &&
                    !ref.table().equalsIgnoreCase(b.alias()) &&
                    !ref.table().equalsIgnoreCase(b.table().getName())) continue;
                int idx = b.table().getColumnIndex(ref.column());
                if (idx >= 0) {
                    Column col = b.table().getColumns().get(idx);
                    if (col.getCompositeTypeName() != null) {
                        return col.getCompositeTypeName().toLowerCase();
                    }
                }
            }
        }
        if (expr instanceof FieldAccessExpr) {
            FieldAccessExpr fa = (FieldAccessExpr) expr;
            return resolveCompositeTypeFromBindings(fa.expr(), baseBindings);
        }
        if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            String tn = cast.typeName().toLowerCase().trim();
            if (executor.database.isCompositeType(tn)) return tn;
        }
        return null;
    }

    private void sortContexts(List<RowContext> contexts, List<SelectStmt.OrderByItem> resolvedOrderBy) {
        if (resolvedOrderBy != null && !resolvedOrderBy.isEmpty()) {
            List<CustomEnum> enumLookups = new ArrayList<>();
            List<String> collationLookups = new ArrayList<>();
            List<Boolean> arrayKeys = new ArrayList<>();
            for (SelectStmt.OrderByItem item : resolvedOrderBy) {
                CustomEnum ce = resolveEnumForExpr(item.expr(), contexts);
                enumLookups.add(ce);
                arrayKeys.add(isArrayOrderKey(item.expr(), contexts));
                // Extract explicit COLLATE collation name if present
                collationLookups.add(item.expr() instanceof CollateExpr
                        ? ((CollateExpr) item.expr()).collation() : null);
            }
            contexts.sort((ctxA, ctxB) -> {
                for (int idx = 0; idx < resolvedOrderBy.size(); idx++) {
                    SelectStmt.OrderByItem item = resolvedOrderBy.get(idx);
                    Object va = executor.evalExpr(item.expr(), ctxA);
                    Object vb = executor.evalExpr(item.expr(), ctxB);

                    if (va == null && vb == null) continue;
                    if (va == null || vb == null) {
                        boolean nullsFirst = item.nullsFirst() != null ? item.nullsFirst() : item.descending();
                        if (va == null) return nullsFirst ? -1 : 1;
                        else return nullsFirst ? 1 : -1;
                    }

                    int cmp;
                    CustomEnum ce = enumLookups.get(idx);
                    if (ce != null) {
                        cmp = Integer.compare(ce.ordinal(va.toString()), ce.ordinal(vb.toString()));
                    } else {
                        String collation = collationLookups.get(idx);
                        if (collation != null && va instanceof String && vb instanceof String) {
                            cmp = TypeCoercion.compareStringsWithCollation((String) va, (String) vb, collation);
                        } else if (arrayKeys.get(idx)) {
                            cmp = executor.compareValues(
                                    TypeCoercion.arrayForCompare(va), TypeCoercion.arrayForCompare(vb));
                        } else {
                            cmp = executor.compareValues(va, vb);
                        }
                    }
                    if (item.descending()) cmp = -cmp;
                    if (cmp != 0) return cmp;
                }
                return 0;
            });
        }
    }

    /** True when the sort key is an array column, whose values are stored as literals. */
    private boolean isArrayOrderKey(Expression expr, List<RowContext> contexts) {
        if (!(expr instanceof ColumnRef) || contexts == null || contexts.isEmpty()) return false;
        String col = ((ColumnRef) expr).column();
        for (RowContext.TableBinding b : contexts.get(0).getBindings()) {
            int i = b.table().getColumnIndex(col);
            if (i >= 0) {
                Column c = b.table().getColumns().get(i);
                return c.getArrayElementType() != null || c.getType().getPgName().startsWith("_");
            }
        }
        return false;
    }

    private CustomEnum resolveEnumForExpr(Expression expr, List<RowContext> contexts) {
        if (!(expr instanceof ColumnRef)) return null;
        ColumnRef ref = (ColumnRef) expr;
        if (contexts.isEmpty()) return null;
        RowContext sample = contexts.get(0);
        for (RowContext.TableBinding b : sample.getBindings()) {
            if (b.table() == null) continue;
            String colName = ref.column();
            String qualifier = ref.table();
            if (qualifier != null
                    && !qualifier.equalsIgnoreCase(b.table().getName())
                    && (b.alias() == null || !qualifier.equalsIgnoreCase(b.alias()))) continue;
            int idx = b.table().getColumnIndex(colName);
            if (idx >= 0) {
                Column col = b.table().getColumns().get(idx);
                if (col.getType() == DataType.ENUM && col.getEnumTypeName() != null) {
                    return executor.database.getCustomEnum(col.getEnumTypeName());
                }
            }
        }
        return null;
    }

    private List<Object[]> projectRows(List<RowContext> contexts,
                                        List<java.util.function.Function<RowContext, Object>> projections) {
        return projectRows(contexts, projections, null);
    }

    private List<Object[]> projectRows(List<RowContext> contexts,
                                        List<java.util.function.Function<RowContext, Object>> projections,
                                        Set<Integer> srfIndices) {
        List<Object[]> resultRows = new ArrayList<>();
        for (RowContext ctx : contexts) {
            Object[] projected = new Object[projections.size()];
            Map<Integer, List<?>> srfResults = new LinkedHashMap<>();
            boolean emptySrf = false;
            for (int i = 0; i < projections.size(); i++) {
                projected[i] = projections.get(i).apply(ctx);
                if (projected[i] instanceof List<?>
                        && (srfIndices == null || srfIndices.contains(i))) {
                    List<?> list = (List<?>) projected[i];
                    if (list.isEmpty()) {
                        emptySrf = true;
                    } else {
                        srfResults.put(i, list);
                    }
                }
            }
            if (emptySrf && srfResults.isEmpty()) {
                continue;
            }
            if (!srfResults.isEmpty()) {
                int maxLen = 0;
                for (List<?> sl : srfResults.values()) {
                    if (sl.size() > maxLen) maxLen = sl.size();
                }
                for (int ri = 0; ri < maxLen; ri++) {
                    Object[] expandedRow = new Object[projections.size()];
                    System.arraycopy(projected, 0, expandedRow, 0, projected.length);
                    for (Map.Entry<Integer, List<?>> entry : srfResults.entrySet()) {
                        int idx = entry.getKey();
                        List<?> sl = entry.getValue();
                        expandedRow[idx] = ri < sl.size() ? sl.get(ri) : null;
                    }
                    resultRows.add(expandedRow);
                }
            } else {
                resultRows.add(projected);
            }
        }
        return resultRows;
    }

    // ---- SELECT without FROM ----

    private QueryResult executeSelectExpressions(SelectStmt stmt) {
        // A FROM-less SELECT has one row and nothing to sort, but its ORDER BY is still analysed:
        // an ordinal outside the select list is out of range and a constant that is not an
        // integer names no column. Skipping the resolve here let SELECT 1 AS a ORDER BY NULL
        // through where every other shape of SELECT already refused it.
        resolveOrderBy(stmt.orderBy(), stmt.targets());
        if (hasWindowFunctionInTargets(stmt.targets())) {
            Table virtualTable = new Table("__virtual__",
                    Cols.listOf(new Column("__dummy__", DataType.INTEGER, true, false, null)));
            virtualTable.insertRow(new Object[]{1});
            RowContext virtualCtx = new RowContext(Cols.listOf(
                    new RowContext.TableBinding(virtualTable, "__virtual__", new Object[]{1})));
            List<RowContext> virtualContexts = Cols.listOf(virtualCtx);
            return windowEvaluator.executeWindowSelect(stmt, virtualContexts, virtualCtx.getBindings());
        }
        if (stmt.limit() != null) limitOffsetValue(stmt.limit(), true);
        if (stmt.offset() != null) limitOffsetValue(stmt.offset(), false);
        if (stmt.where() != null) {
            Object whereVal = executor.evalExpr(stmt.where(), null);
            if (whereVal instanceof Number) {
                throw new MemgresException("argument of WHERE must be type boolean, not type " +
                        TypeCoercion.inferType(whereVal).getPgName(), "42804");
            }
            if (!executor.isTruthy(whereVal)) {
                List<Column> columns = new ArrayList<>();
                for (SelectStmt.SelectTarget target : stmt.targets()) {
                    String alias = target.alias();
                    if (alias == null) alias = executor.exprToAlias(target.expr());
                    columns.add(buildProjectedColumn(alias, target.expr(), Cols.listOf()));
                }
                return QueryResult.select(columns, new ArrayList<>());
            }
        }

        List<Column> columns = new ArrayList<>();
        List<Object> valuesList = new ArrayList<>();
        int srfIndex = -1;
        List<?> srfList = null;
        Map<Integer, List<?>> srfMap = new LinkedHashMap<>();

        boolean hasCompositeStar = false;
        for (SelectStmt.SelectTarget target : stmt.targets()) {
            if (target.expr() instanceof CompositeStarExpr) { hasCompositeStar = true; break; }
        }

        if (hasCompositeStar) {
            for (SelectStmt.SelectTarget target : stmt.targets()) {
                if (target.expr() instanceof CompositeStarExpr) {
                    CompositeStarExpr cse = (CompositeStarExpr) target.expr();
                    String typeName = executor.resolveCompositeTypeNamePublic(cse.expr(), null);
                    Object val = executor.evalExpr(cse.expr(), null);
                    List<String> recordFields = typeName != null ? null
                            : JsonFunctions.recordFieldNames(cse.expr());
                    if (recordFields != null) {
                        for (int fi = 0; fi < recordFields.size(); fi++) {
                            columns.add(new Column(recordFields.get(fi), DataType.TEXT, true, false, null));
                            Object field = recordField(val, fi);
                            if (field instanceof List<?>) srfMap.put(valuesList.size(), (List<?>) field);
                            valuesList.add(field);
                        }
                    } else if (typeName != null) {
                        List<CreateTypeStmt.CompositeField> fields = executor.database.getCompositeType(typeName);
                        if (fields != null) {
                            for (CreateTypeStmt.CompositeField field : fields) {
                                DataType fieldType = DataType.fromPgName(field.typeName());
                                columns.add(new Column(field.name(), fieldType != null ? fieldType : DataType.TEXT, true, false, null));
                                valuesList.add(executor.extractCompositeField(val, field.name(), typeName));
                            }
                        }
                    }
                } else {
                    String alias = target.alias();
                    if (alias == null) alias = executor.exprToAlias(target.expr());
                    columns.add(buildProjectedColumn(alias, target.expr(), Cols.listOf()));
                    valuesList.add(executor.evalExpr(target.expr(), null));
                }
            }
            Object[] values = valuesList.toArray();
            if (!srfMap.isEmpty()) {
                return QueryResult.select(columns,
                        applyOffsetAndLimit(stmt, expandSrfRows(values, srfMap)));
            }
            List<Object[]> rows = new ArrayList<>();
            rows.add(values);
            return QueryResult.select(columns, rows);
        }

        Object[] values = new Object[stmt.targets().size()];
        // Only used to host SRF element bindings (see RowContext.setBoundValue) when a target
        // expression contains a nested set-returning function call; a no-FROM SELECT has no
        // table bindings to resolve columns against, so an empty context is safe here.
        RowContext srfHostCtx = new RowContext(Cols.<RowContext.TableBinding>listOf());
        for (int i = 0; i < stmt.targets().size(); i++) {
            SelectStmt.SelectTarget target = stmt.targets().get(i);
            String alias = target.alias();
            if (alias == null) {
                alias = executor.exprToAlias(target.expr());
            }

            DataType resultType = executor.inferExprType(target.expr());
            FunctionCallExpr srfNode = findSrfCall(target.expr());
            Object val = srfNode != null
                    ? evalSrfExpandedTarget(target.expr(), srfHostCtx)
                    : executor.evalExpr(target.expr(), null);
            if (val instanceof byte[] && resultType == DataType.TEXT) {
                resultType = DataType.BYTEA;
            }
            if (resultType == DataType.ENUM) {
                String enumTypeName = executor.resolveEnumTypeName(target.expr(), Cols.listOf());
                columns.add(enumTypeName != null
                        ? new Column(alias, DataType.ENUM, true, false, null, enumTypeName)
                        : new Column(alias, DataType.TEXT, true, false, null));
            } else {
                columns.add(new Column(alias, resultType, true, false, null));
            }
            if (val instanceof List<?> && srfNode != null) {
                List<?> list = (List<?>) val;
                if (srfIndex < 0) {
                    srfIndex = i;
                    srfList = list;
                }
                srfMap.put(i, list);
            }
            values[i] = val;
        }

        if (srfIndex >= 0 && srfList != null) {
            List<Object[]> rows = expandSrfRows(values, srfMap);
            List<SelectStmt.OrderByItem> resolvedOrderBy = resolveOrderBy(stmt.orderBy(), stmt.targets());
            if (resolvedOrderBy != null && !resolvedOrderBy.isEmpty()) {
                final List<SelectStmt.OrderByItem> ob = resolvedOrderBy;
                rows.sort((a, b) -> {
                    for (SelectStmt.OrderByItem item : ob) {
                        int colIdx = resolveOrderByToColumnIndex(item.expr(), stmt.targets());
                        if (colIdx < 0) continue;
                        Object va = colIdx < a.length ? a[colIdx] : null;
                        Object vb = colIdx < b.length ? b[colIdx] : null;
                        int cmp;
                        if (va == null && vb == null) cmp = 0;
                        else if (va == null || vb == null) {
                            // Effective nulls-first: explicit setting, or default (DESC→first, ASC→last)
                            boolean effectiveNullsFirst = item.nullsFirst() != null ? item.nullsFirst() : item.descending();
                            if (va == null) cmp = effectiveNullsFirst ? -1 : 1;
                            else cmp = effectiveNullsFirst ? 1 : -1;
                            if (cmp != 0) return cmp; else continue;
                        } else {
                            String collation = item.expr() instanceof CollateExpr
                                    ? ((CollateExpr) item.expr()).collation() : null;
                            if (collation != null && va instanceof String && vb instanceof String) {
                                cmp = TypeCoercion.compareStringsWithCollation((String) va, (String) vb, collation);
                            } else {
                                cmp = executor.compareValues(va, vb);
                            }
                        }
                        if (item.descending()) cmp = -cmp;
                        if (cmp != 0) return cmp;
                    }
                    return 0;
                });
            }
            // Apply OFFSET + LIMIT
            return QueryResult.select(columns, applyOffsetAndLimit(stmt, rows));
        }

        // A row list of one still has to honour LIMIT and OFFSET; skipping that made LIMIT 0
        // return the row it was told to exclude.
        List<Object[]> single = new ArrayList<>();
        single.add(values);
        return QueryResult.select(columns, applyOffsetAndLimit(stmt, single));
    }

    /**
     * One row per element of the largest set in the row. Sets of different sizes run to the
     * longest and the shorter ones read NULL past their end, which is what PG does since 10.
     */
    private static List<Object[]> expandSrfRows(Object[] values, Map<Integer, List<?>> srfMap) {
        int maxLen = 0;
        for (List<?> sl : srfMap.values()) {
            if (sl.size() > maxLen) maxLen = sl.size();
        }
        List<Object[]> rows = new ArrayList<>();
        for (int ri = 0; ri < maxLen; ri++) {
            Object[] row = Arrays.copyOf(values, values.length);
            for (Map.Entry<Integer, List<?>> entry : srfMap.entrySet()) {
                List<?> sl = entry.getValue();
                row[entry.getKey()] = ri < sl.size() ? sl.get(ri) : null;
            }
            rows.add(row);
        }
        return rows;
    }

    /**
     * How many times each column name is merged away by a join, counting both the columns a
     * USING clause names and the ones a NATURAL join finds for itself.
     *
     * <p>A merged column is one column of the join's output however many relations contributed
     * to it, so a name that appears in three relations under two merges is not ambiguous — while
     * the same name in three relations under one merge still is. Counting rather than
     * remembering a set is what keeps {@code t JOIN u USING (s) JOIN v ON true} ambiguous, as
     * PostgreSQL has it, while {@code t NATURAL JOIN u NATURAL JOIN v} resolves.
     */
    private void collectUsingColumns(List<SelectStmt.FromItem> fromItems, Map<String, Integer> result) {
        if (fromItems == null) return;
        for (SelectStmt.FromItem item : fromItems) {
            if (item instanceof SelectStmt.JoinFrom) {
                SelectStmt.JoinFrom jf = (SelectStmt.JoinFrom) item;
                if (jf.using() != null) {
                    for (String col : jf.using()) countMerge(result, col);
                } else if (isNaturalJoin(jf.joinType())) {
                    for (String col : FromJoinExecutor.commonColumns(
                            executor.fromResolver.resolveItemShape(jf.left()),
                            executor.fromResolver.resolveItemShape(jf.right()))) {
                        countMerge(result, col);
                    }
                }
                collectUsingColumns(Cols.listOf(jf.left()), result);
                collectUsingColumns(Cols.listOf(jf.right()), result);
            }
        }
    }

    private static void countMerge(Map<String, Integer> result, String col) {
        String key = col.toLowerCase();
        Integer prior = result.get(key);
        result.put(key, prior == null ? 1 : prior + 1);
    }

    private static boolean isNaturalJoin(SelectStmt.JoinType type) {
        return type == SelectStmt.JoinType.NATURAL || type == SelectStmt.JoinType.NATURAL_LEFT
                || type == SelectStmt.JoinType.NATURAL_RIGHT || type == SelectStmt.JoinType.NATURAL_FULL;
    }

    private boolean isSrfCall(Expression expr) {
        return findSrfCall(expr) != null;
    }

    /**
     * A set-returning function produces rows, so it may sit anywhere rows are still being
     * produced -- the FROM clause, a select-list expression, GROUP BY, ORDER BY -- and nowhere
     * that reads rows already produced. WHERE and a JOIN condition decide whether to keep a row
     * and HAVING whether to keep a group, so neither has a set to expand; LIMIT and OFFSET are
     * read once for the whole query. PG also refuses one buried in a conditional, since which
     * arm's rows it would produce is undecidable.
     */
    private static void rejectMisplacedSrfs(SelectStmt stmt) {
        rejectSrfIn(stmt.where(), "WHERE");
        rejectSrfIn(stmt.having(), "HAVING");
        rejectSrfIn(stmt.limit(), "LIMIT");
        rejectSrfIn(stmt.offset(), "OFFSET");
        rejectSrfsInJoinConditions(stmt.from());
        if (stmt.targets() != null) {
            for (SelectStmt.SelectTarget t : stmt.targets()) {
                rejectSrfWhereOneBooleanIsWanted(t.expr());
                rejectSrfInFilter(t.expr());
                MemgresException conditional = conditionalHidingSrf(t.expr());
                if (conditional != null) throw conditional;
            }
        }
    }

    /** Refuses a set-returning call written in a clause that reads rows already produced. */
    static void rejectSrfIn(Expression expr, String clause) {
        if (expr == null) return;
        List<FunctionCallExpr> found = collectSrfCalls(expr);
        if (!found.isEmpty()) throw misplacedSrf(clause, found.get(0));
    }

    /**
     * FILTER selects which rows an aggregate accumulates, so only an aggregate may carry one.
     * PostgreSQL names the function rather than the clause, because what is wrong is the call.
     */
    private static void rejectSrfInFilter(Expression expr) {
        Object found = AstWalk.findFirst(expr, node -> node instanceof FunctionCallExpr
                && ((FunctionCallExpr) node).filter() != null
                && SRF_FUNCTIONS.contains(FunctionEvaluator.stripSchemaPrefix(
                        ((FunctionCallExpr) node).name().toLowerCase())));
        if (found == null) return;
        String name = FunctionEvaluator.stripSchemaPrefix(
                ((FunctionCallExpr) found).name().toLowerCase());
        MemgresException e = new MemgresException(
                "FILTER specified, but " + name + " is not an aggregate function", "42809");
        e.setPositionToken(name);
        throw e;
    }

    /**
     * Somewhere that wants one boolean and gets a set, PostgreSQL names the kind of value rather
     * than the placement rule: a WHEN condition, and either side of AND, OR and NOT (BETWEEN
     * among them, which is an AND once written out). An SRF elsewhere in the same constructs --
     * a CASE operand or result -- is the placement rule again and reported so.
     */
    private static void rejectSrfWhereOneBooleanIsWanted(Expression expr) {
        Object found = AstWalk.findFirst(expr, node -> {
            if (node instanceof CaseExpr) {
                for (CaseExpr.WhenClause when : ((CaseExpr) node).whenClauses()) {
                    if (containsSrf(when.condition())) return true;
                }
                return false;
            }
            // IN compares one value against a list of values. A set on either side is neither,
            // and PostgreSQL says so in the same words it uses for AND and OR. A set inside a
            // sub-query on the right is ordinary -- containsSrf stops at the nested query.
            if (node instanceof InExpr) return containsSrf(node);
            if (node instanceof BetweenExpr) return containsSrf(node);
            if (node instanceof BinaryExpr) {
                BinaryExpr bin = (BinaryExpr) node;
                if (bin.op() != BinaryExpr.BinOp.AND && bin.op() != BinaryExpr.BinOp.OR) return false;
                return containsSrf(bin.left()) || containsSrf(bin.right());
            }
            if (node instanceof UnaryExpr) {
                UnaryExpr un = (UnaryExpr) node;
                return un.op() == UnaryExpr.UnaryOp.NOT && containsSrf(un.operand());
            }
            return false;
        });
        if (found == null) return;
        String construct = found instanceof CaseExpr ? "CASE/WHEN"
                : found instanceof InExpr ? "IN"
                : found instanceof BetweenExpr ? "AND"
                : found instanceof UnaryExpr ? "NOT"
                : ((BinaryExpr) found).op().name();
        MemgresException e = new MemgresException(
                "argument of " + construct + " must not return a set", "42804");
        // PostgreSQL points at where the construct starts, which is its leftmost operand.
        if (found instanceof InExpr) e.setPositionToken(leadingToken(((InExpr) found).expr()));
        throw e;
    }

    /** The word an expression starts with, as it is written -- what a position points at. */
    private static String leadingToken(Expression expr) {
        if (expr instanceof ColumnRef) {
            ColumnRef ref = (ColumnRef) expr;
            return ref.table() != null ? ref.table() : ref.column();
        }
        if (expr instanceof FunctionCallExpr) return ((FunctionCallExpr) expr).name();
        if (expr instanceof Literal) return ((Literal) expr).value();
        return null;
    }

    private static void rejectSrfsInJoinConditions(List<SelectStmt.FromItem> fromItems) {
        if (fromItems == null) return;
        for (SelectStmt.FromItem item : fromItems) {
            if (!(item instanceof SelectStmt.JoinFrom)) continue;
            SelectStmt.JoinFrom jf = (SelectStmt.JoinFrom) item;
            rejectSrfIn(jf.on(), "JOIN conditions");
            rejectSrfsInJoinConditions(Cols.listOf(jf.left(), jf.right()));
        }
    }

    /**
     * A join or subquery given an alias exposes everything it produces under that single name,
     * so two of its columns can end up sharing one. PG refuses to pick between them, because
     * which one it picked would be invisible in the query text.
     */
    private static void rejectAmbiguousQualifiedRefs(SelectStmt stmt,
                                                     List<RowContext.TableBinding> bindings,
                                                     Set<String> usingColumns) {
        if (bindings == null || bindings.isEmpty()) return;
        List<ColumnRef> refs = new ArrayList<>();
        for (SelectStmt.SelectTarget t : stmt.targets()) collectLocalColumnRefs(t.expr(), refs);
        collectLocalColumnRefs(stmt.where(), refs);
        collectLocalColumnRefs(stmt.having(), refs);
        if (stmt.groupBy() != null) {
            for (Expression g : stmt.groupBy()) collectLocalColumnRefs(g, refs);
        }
        if (stmt.orderBy() != null) {
            for (SelectStmt.OrderByItem ob : stmt.orderBy()) collectLocalColumnRefs(ob.expr(), refs);
        }
        for (ColumnRef cr : refs) {
            if (cr.table() == null || cr.column() == null || "*".equals(cr.column())) continue;
            if (usingColumns.contains(cr.column().toLowerCase())) continue;
            for (RowContext.TableBinding b : bindings) {
                String exposed = b.alias() != null ? b.alias() : b.table().getName();
                if (!cr.table().equalsIgnoreCase(exposed)) continue;
                int matches = 0;
                for (Column c : b.table().getColumns()) {
                    if (c.getName().equalsIgnoreCase(cr.column())) matches++;
                }
                if (matches > 1) {
                    throw new MemgresException(
                            "column reference \"" + cr.column() + "\" is ambiguous", "42702");
                }
            }
        }
    }

    /** Column references belonging to this query level; a nested query resolves its own. */
    private static void collectLocalColumnRefs(Object node, List<ColumnRef> out) {
        if (node == null) return;
        if (node instanceof ColumnRef) {
            out.add((ColumnRef) node);
            return;
        }
        if (node instanceof com.memgres.engine.parser.ast.Statement) return;
        AstWalk.forEachChild(node, child -> collectLocalColumnRefs(child, out));
    }

    /**
     * The checks PG makes over a FROM clause before a single row is read: two items that would
     * answer to the same name, a USING column named twice, and a LATERAL item that reaches across
     * a join it cannot see past. A join condition that is not a per-row predicate is judged where
     * the join is executed instead, which is early enough that it is reported rather than
     * evaluated.
     */
    private void validateFromClause(List<SelectStmt.FromItem> from) {
        if (from == null) return;
        Map<String, SelectStmt.FromItem> exposed = new LinkedHashMap<>();
        for (SelectStmt.FromItem item : from) {
            collectAndValidate(item, exposed);
        }
    }

    /**
     * Judges the grouping of a query that is being stored rather than run.
     *
     * <p>A SQL function's body is analysed when the function is written — PostgreSQL parses and
     * analyses every statement in it at CREATE FUNCTION time, which is why
     * {@code CREATE FUNCTION f() RETURNS text AS $$ SELECT b FROM t GROUP BY a $$ LANGUAGE sql} is
     * refused there and not at the first call. (A PL/pgSQL body is not: its statements are
     * strings the PL handler plans on first execution, so the same query inside one is accepted
     * and fails when the function runs.)
     *
     * <p>The rule itself is the one a running SELECT gets, asked with the two things it needs and
     * nothing else supplies: the select targets with stars expanded, and the relations the FROM
     * clause names. It is asked only when all of those are available and the query is grouped at
     * all — a scope this cannot read is a scope it does not judge, because refusing a body
     * PostgreSQL stores is worse than storing one it refuses.
     */
    void validateStoredQueryGrouping(SelectStmt stmt) {
        if (stmt.from() == null || stmt.from().isEmpty()) return;
        // A CTE name is not a relation this can look up, so a body that defines one is left alone.
        if (stmt.withClauses() != null && !stmt.withClauses().isEmpty()) return;
        boolean grouped = (stmt.groupBy() != null && !stmt.groupBy().isEmpty())
                || (stmt.groupingSets() != null && !stmt.groupingSets().isEmpty())
                || stmt.having() != null
                || hasAggregateInTargets(stmt.targets())
                || hasAggregateInOrderBy(stmt.orderBy())
                || hasAggregateInWindowDefs(stmt.windowDefs());
        if (!grouped) return;
        List<RowContext.TableBinding> bindings;
        try {
            bindings = executor.fromResolver.resolveTableBindings(stmt.from());
        } catch (RuntimeException e) {
            return;
        }
        if (bindings.isEmpty()) return;
        for (RowContext.TableBinding binding : bindings) {
            if (binding.table() == null) return;
        }
        List<SelectStmt.SelectTarget> targets = expandTargetsForOrdinals(stmt.targets(), bindings);
        SelectStmt judged = targets != stmt.targets() ? stmt.withTargets(targets) : stmt;
        groupByValidator.validate(judged, targets, bindings);
    }

    /**
     * The names one join exposes, checked before its ON condition is read.
     *
     * <p>PostgreSQL adds both arms of a join to the namespace and rejects a name that is already
     * there before it analyses the condition, so {@code t x JOIN u x ON count(*) = 1} is the
     * duplicate name rather than the misplaced aggregate. The whole-FROM check runs later and
     * would find the same clash, but by then the condition has already been judged.
     */
    void validateJoinNames(SelectStmt.JoinFrom join) {
        collectAndValidate(join, new LinkedHashMap<String, SelectStmt.FromItem>());
    }

    /** Records the names {@code item} exposes to the query, validating it on the way down. */
    private void collectAndValidate(SelectStmt.FromItem item, Map<String, SelectStmt.FromItem> exposed) {
        if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
            Map<String, SelectStmt.FromItem> leftNames = new LinkedHashMap<>();
            collectAndValidate(join.left(), leftNames);
            Map<String, SelectStmt.FromItem> rightNames = new LinkedHashMap<>();
            collectAndValidate(join.right(), rightNames);
            for (SelectStmt.FromItem n : leftNames.values()) addExposed(exposed, n);
            for (SelectStmt.FromItem n : rightNames.values()) addExposed(exposed, n);
            if (join.using() != null) {
                Set<String> seen = new HashSet<>();
                for (String col : join.using()) {
                    if (!seen.add(col.toLowerCase())) {
                        throw new MemgresException(
                                "column name \"" + col + "\" appears more than once in USING clause",
                                "42701").suppressPosition();
                    }
                }
            }
            rejectLateralAcrossNullableSide(join, leftNames);
            return;
        }
        addExposed(exposed, item);
    }

    private void addExposed(Map<String, SelectStmt.FromItem> exposed, SelectStmt.FromItem item) {
        String name = exposedName(item);
        if (name == null) return;
        SelectStmt.FromItem prior = exposed.put(name.toLowerCase(), item);
        if (prior != null && !separateRelationsOfOneName(prior, item)) {
            throw new MemgresException("table name \"" + name + "\" specified more than once",
                    "42712").suppressPosition();
        }
    }

    /**
     * The one case SQL lets two FROM items share a name: both are relations written without an
     * alias, and they are different relations. {@code FROM public.t, other.t} is legal because
     * either can still be reached by writing its schema; give either an alias, or let one of them
     * be a WITH query or a subquery, and the name becomes the only way to reach it and the clash
     * is real.
     */
    private boolean separateRelationsOfOneName(SelectStmt.FromItem a, SelectStmt.FromItem b) {
        if (!(a instanceof SelectStmt.TableRef) || !(b instanceof SelectStmt.TableRef)) return false;
        SelectStmt.TableRef ta = (SelectStmt.TableRef) a;
        SelectStmt.TableRef tb = (SelectStmt.TableRef) b;
        if (ta.alias() != null || tb.alias() != null) return false;
        if (lookupCte(ta.table()) != null || lookupCte(tb.table()) != null) return false;
        String sa = ta.schema() != null ? ta.schema() : executor.defaultSchema();
        String sb = tb.schema() != null ? tb.schema() : executor.defaultSchema();
        return !sa.equalsIgnoreCase(sb);
    }

    /** The name a FROM item answers to: its alias, or failing that the relation's own name. */
    private static String exposedName(SelectStmt.FromItem item) {
        String name = null;
        if (item instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef t = (SelectStmt.TableRef) item;
            name = t.alias() != null ? t.alias() : t.table();
        } else if (item instanceof SelectStmt.SubqueryFrom) {
            name = ((SelectStmt.SubqueryFrom) item).alias();
        } else if (item instanceof SelectStmt.FunctionFrom) {
            SelectStmt.FunctionFrom f = (SelectStmt.FunctionFrom) item;
            name = f.alias() != null ? f.alias()
                    : FunctionEvaluator.stripSchemaPrefix(f.functionName());
            // ROWS FROM is a FROM item the parser spells as a call to a made-up function, so
            // its own name is not one the query wrote. Unaliased, PostgreSQL names it after the
            // first function inside it, and it clashes with a second item of that name.
            if (f.alias() == null && "__rows_from__".equals(f.functionName())) {
                name = firstRowsFromFunctionName(f);
            }
        }
        // Names the parser invents for constructs with no user-visible name of their own
        return name == null || name.startsWith("__") ? null : name;
    }

    /** The name of the first function written in a ROWS FROM item, or null when there is none. */
    private static String firstRowsFromFunctionName(SelectStmt.FunctionFrom item) {
        for (Expression arg : item.args()) {
            FunctionCallExpr call = arg instanceof RowsFromItem ? ((RowsFromItem) arg).call()
                    : arg instanceof FunctionCallExpr ? (FunctionCallExpr) arg : null;
            if (call != null) return FunctionEvaluator.stripSchemaPrefix(call.name());
        }
        return null;
    }

    /**
     * A LATERAL item on the nullable side of a RIGHT or FULL join cannot see the other side:
     * when it is evaluated the rows it would read from are not yet determined.
     */
    private static void rejectLateralAcrossNullableSide(SelectStmt.JoinFrom join,
                                                       Map<String, ?> leftNames) {
        SelectStmt.JoinType type = join.joinType();
        if (type != SelectStmt.JoinType.RIGHT && type != SelectStmt.JoinType.FULL
                && type != SelectStmt.JoinType.NATURAL_RIGHT && type != SelectStmt.JoinType.NATURAL_FULL) {
            return;
        }
        Object lateralBody = null;
        if (join.right() instanceof SelectStmt.SubqueryFrom
                && ((SelectStmt.SubqueryFrom) join.right()).lateral()) {
            lateralBody = ((SelectStmt.SubqueryFrom) join.right()).subquery();
        } else if (join.right() instanceof SelectStmt.FunctionFrom) {
            // A function in FROM is implicitly lateral over the items to its left
            lateralBody = ((SelectStmt.FunctionFrom) join.right()).args();
        }
        if (lateralBody == null) return;
        String referenced = firstReferenceTo(lateralBody, leftNames.keySet());
        if (referenced == null) return;
        MemgresException e = new MemgresException(
                "invalid reference to FROM-clause entry for table \"" + referenced + "\"", "42P10");
        e.setDetail("The combining JOIN type must be INNER or LEFT for a LATERAL reference.");
        throw e;
    }

    /** The first qualified reference under {@code node} naming one of {@code names}, or null. */
    private static String firstReferenceTo(Object node, Set<String> names) {
        Set<String> outer = new HashSet<>(names);
        if (node instanceof SelectStmt) {
            // A name the item re-uses for its own FROM entry shadows the outer one
            Map<String, String> own = new LinkedHashMap<>();
            List<SelectStmt.FromItem> from = ((SelectStmt) node).from();
            if (from != null) {
                for (SelectStmt.FromItem f : from) {
                    String n = exposedName(f);
                    if (n != null) own.put(n.toLowerCase(), n);
                }
            }
            outer.removeAll(own.keySet());
        }
        Object found = AstWalk.findFirst(node, n -> n instanceof ColumnRef
                && ((ColumnRef) n).table() != null
                && outer.contains(((ColumnRef) n).table().toLowerCase()));
        return found == null ? null : ((ColumnRef) found).table();
    }

    /**
     * An aggregate consumes one row at a time, so a set-returning argument has no meaning:
     * PG cannot say whether the set expands before or after the aggregation and refuses.
     */
    private void rejectSrfInAggregates(SelectStmt stmt) {
        if (stmt.targets() != null) {
            for (SelectStmt.SelectTarget t : stmt.targets()) rejectSrfInAggregates(t.expr());
        }
        if (stmt.having() != null) rejectSrfInAggregates(stmt.having());
        if (stmt.orderBy() != null) {
            for (SelectStmt.OrderByItem ob : stmt.orderBy()) rejectSrfInAggregates(ob.expr());
        }
    }

    /**
     * As {@link #containsSrf}, over the functions this database was told about rather than the
     * built-in list: a function declared {@code RETURNS SETOF} or {@code RETURNS TABLE} returns
     * a set for the same reason and cannot be an aggregate's argument either.
     *
     * <p>One returning bare {@code record} is left out. Its columns have no names until a caller
     * supplies them, and a call written without them -- which is the only way one reaches an
     * aggregate -- fails on the column before the set: PostgreSQL answers
     * {@code could not identify column "y" in record data type}, and so does this.
     */
    private boolean containsUserSrf(Object node) {
        if (node == null || node instanceof Statement) return false;
        if (node instanceof FunctionCallExpr) {
            PgFunction f = executor.database.getFunction(
                    FunctionEvaluator.stripSchemaPrefix(((FunctionCallExpr) node).name().toLowerCase()));
            if (f != null && f.isSetReturning() && !f.declaresRecordResult()) return true;
        }
        boolean[] found = {false};
        AstWalk.forEachChild(node, child -> {
            if (!found[0] && containsUserSrf(child)) found[0] = true;
        });
        return found[0];
    }

    private void rejectSrfInAggregates(Object node) {
        if (node == null) return;
        if (node instanceof FunctionCallExpr) {
            FunctionCallExpr fc = (FunctionCallExpr) node;
            if (isAggregateFunction(fc.name())) {
                for (Expression arg : fc.args()) {
                    if (containsSrf(arg) || containsUserSrf(arg)) {
                        MemgresException e = new MemgresException(
                                "aggregate function calls cannot contain set-returning function calls", "0A000");
                        e.setHint("You might be able to move the set-returning function "
                                + "into a LATERAL FROM item.");
                        FunctionCallExpr srf = findSrfCall(arg);
                        if (srf != null) e.setPositionToken(srf.name());
                        throw e;
                    }
                }
            }
        }
        // A nested query gets its own analysis when it runs
        if (node instanceof com.memgres.engine.parser.ast.Statement) return;
        AstWalk.forEachChild(node, this::rejectSrfInAggregates);
    }

    /**
     * DISTINCT ON keeps the first row of each group, so the groups have to be the outermost
     * sort keys — otherwise which row survives depends on the plan and the query has no defined
     * answer. PG requires the DISTINCT ON expressions to lead the ORDER BY, and so does this.
     */
    private static void validateDistinctOn(SelectStmt stmt) {
        List<Expression> on = stmt.distinctOn();
        if (on == null || on.isEmpty()) return;
        List<SelectStmt.OrderByItem> orderBy = stmt.orderBy();
        // Unordered, every row is equally arbitrary already, so PG imposes nothing
        if (orderBy == null || orderBy.isEmpty()) return;
        for (int i = 0; i < on.size(); i++) {
            if (i >= orderBy.size()
                    || !sortKeysMatch(on.get(i), resolveSortKey(orderBy.get(i).expr(), stmt.targets()))) {
                throw new MemgresException(
                        "SELECT DISTINCT ON expressions must match initial ORDER BY expressions", "42P10");
            }
        }
    }

    /** An ORDER BY item may name an output column by ordinal or alias; DISTINCT ON never does. */
    private static Expression resolveSortKey(Expression expr, List<SelectStmt.SelectTarget> targets) {
        if (targets == null) return expr;
        if (expr instanceof Literal && ((Literal) expr).literalType() == Literal.LiteralType.INTEGER) {
            int ordinal = Integer.parseInt(((Literal) expr).value());
            if (ordinal >= 1 && ordinal <= targets.size()) return targets.get(ordinal - 1).expr();
            return expr;
        }
        if (expr instanceof ColumnRef && ((ColumnRef) expr).table() == null) {
            String name = ((ColumnRef) expr).column();
            for (SelectStmt.SelectTarget t : targets) {
                if (name.equalsIgnoreCase(t.alias())) return t.expr();
            }
        }
        return expr;
    }

    /** Two sort keys are the same key when they name the same column or read identically. */
    private static boolean sortKeysMatch(Expression a, Expression b) {
        if (a == null || b == null) return false;
        if (a instanceof ColumnRef && b instanceof ColumnRef) {
            ColumnRef ca = (ColumnRef) a;
            ColumnRef cb = (ColumnRef) b;
            if (!ca.column().equalsIgnoreCase(cb.column())) return false;
            return ca.table() == null || cb.table() == null || ca.table().equalsIgnoreCase(cb.table());
        }
        return a.toString().equalsIgnoreCase(b.toString());
    }

    /**
     * The placement refusal, pointed at the call rather than at the clause.
     *
     * <p>PostgreSQL reports the character the set-returning call starts at, not the keyword that
     * forbids it, and the engine's AST carries no offsets — so the call's name travels with the
     * exception and the protocol layer finds it in the statement text.
     *
     * @param offender the set-returning call that may not stand here, or null when unknown
     */
    static MemgresException misplacedSrf(String construct, FunctionCallExpr offender) {
        MemgresException e = new MemgresException(
                "set-returning functions are not allowed in " + construct, "0A000");
        if (offender != null) e.setPositionToken(offender.name());
        return e;
    }

    /**
     * The same refusal with the hint PostgreSQL adds when the call could have been written as a
     * FROM item instead. It offers that only where moving the call would actually answer the
     * query — inside a conditional or an aggregate's arguments — and not for LIMIT, OFFSET,
     * WHERE, HAVING or a join condition, where a LATERAL item would not help.
     */
    private static MemgresException misplacedSrfWithHint(String construct, FunctionCallExpr offender) {
        MemgresException e = misplacedSrf(construct, offender);
        e.setHint("You might be able to move the set-returning function into a LATERAL FROM item.");
        return e;
    }

    /** The conditional construct hiding an SRF in this expression, and the call, or null. */
    private static MemgresException conditionalHidingSrf(Expression expr) {
        Object found = AstWalk.findFirst(expr, node -> {
            if (node instanceof CaseExpr) return containsSrf(node);
            if (node instanceof FunctionCallExpr) {
                String n = FunctionEvaluator.stripSchemaPrefix(((FunctionCallExpr) node).name().toLowerCase());
                if (CONDITIONAL_CONSTRUCTS.contains(n)) {
                    for (Expression arg : ((FunctionCallExpr) node).args()) {
                        if (containsSrf(arg)) return true;
                    }
                }
            }
            return false;
        });
        if (found == null) return null;
        String construct = found instanceof CaseExpr ? "CASE"
                : FunctionEvaluator.stripSchemaPrefix(
                        ((FunctionCallExpr) found).name().toLowerCase()).toUpperCase();
        return misplacedSrfWithHint(construct, findSrfCall((Expression) found));
    }

    /**
     * True when a set-returning call appears under this node as part of the node's own row.
     * One inside a nested query belongs to that query, so {@code WHERE x IN (SELECT
     * generate_series(1,2))} is not a set-returning call in WHERE.
     */
    static boolean containsSrf(Object node) {
        return !collectSrfCalls(node).isEmpty();
    }

    /**
     * The conditional constructs PostgreSQL refuses to expand a set inside — measured, not
     * reasoned about, because the family is smaller than it looks.
     *
     * <p>A construct is on this list when it may skip evaluating an argument: CASE runs only the
     * arm its condition picks, and COALESCE stops at the first argument that is not null, so which
     * rows the query would answer depends on a value the planner does not have. NULLIF, GREATEST
     * and LEAST look like the same kind of thing and are not — each evaluates every argument, so
     * PostgreSQL expands the set and applies the operator per row, and
     * {@code SELECT nullif(generate_series(1,2), 0)} answers two rows. Listing them here refused
     * SQL PostgreSQL runs.
     */
    private static final Set<String> CONDITIONAL_CONSTRUCTS =
            new HashSet<>(Arrays.asList("coalesce"));

    /**
     * The first set-returning call this expression evaluates as part of its own row, or null.
     * Only used to decide whether the expanding evaluation path is needed at all;
     * {@link #collectSrfCalls} is what the expansion itself works from.
     */
    static FunctionCallExpr findSrfCall(Expression expr) {
        List<FunctionCallExpr> found = collectSrfCalls(expr);
        return found.isEmpty() ? null : found.get(0);
    }

    /**
     * Every set-returning call this expression evaluates as part of its own row, in the order
     * they are written.
     *
     * <p>Two boundaries make the list the right one to expand over. A call nested in another
     * set-returning call's arguments is not collected: the enclosing call is what produces the
     * row, and it runs once per element its argument yields. And the walk stops at a nested
     * query, whose set-returning calls produce that query's rows and not this one's --
     * {@code WHERE x IN (SELECT generate_series(1,2))} is ordinary SQL.
     */
    static List<FunctionCallExpr> collectSrfCalls(Object node) {
        List<FunctionCallExpr> found = new ArrayList<>();
        collectSrfCalls(node, found);
        return found;
    }

    private static void collectSrfCalls(Object node, List<FunctionCallExpr> out) {
        if (node == null || node instanceof Statement) return;
        if (node instanceof FunctionCallExpr
                && SRF_FUNCTIONS.contains(FunctionEvaluator.stripSchemaPrefix(
                        ((FunctionCallExpr) node).name().toLowerCase()))) {
            out.add((FunctionCallExpr) node);
            return;
        }
        AstWalk.forEachChild(node, child -> collectSrfCalls(child, out));
    }

    /**
     * Evaluates a SELECT-list target expression containing set-returning calls, returning a
     * {@code List<Object>} of one value of the whole expression per row it produces.
     *
     * <p>PostgreSQL runs the set-returning calls of one expression side by side rather than
     * one inside the other: the row count is the longest of them and the shorter ones read NULL
     * past their end, so {@code generate_series(1,2) + generate_series(10,12)} answers 11, 13
     * and NULL. Everything else in the expression is recomputed once per row rather than copied.
     */
    Object evalSrfExpandedTarget(Expression expr, RowContext ctx) {
        List<FunctionCallExpr> srfs = collectSrfCalls(expr);
        if (srfs.isEmpty()) return executor.evalExpr(expr, ctx);
        List<List<Object>> sets = new ArrayList<>(srfs.size());
        for (FunctionCallExpr srf : srfs) {
            List<Object> values = srfValues(srf, ctx);
            // Defensive: a call named like an SRF that did not answer a set expands nothing.
            if (values == null) return executor.evalExpr(expr, ctx);
            sets.add(values);
        }
        if (srfs.size() == 1 && srfs.get(0) == expr) return sets.get(0);
        int rows = 0;
        for (List<Object> set : sets) rows = Math.max(rows, set.size());
        List<Object> results = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) {
            bindSrfElements(srfs, sets, i, ctx);
            try {
                results.add(executor.evalExpr(expr, ctx));
            } finally {
                for (FunctionCallExpr srf : srfs) ctx.clearBoundValue(srf);
            }
        }
        return results;
    }

    /**
     * The elements one set-returning call produces. A set among its own arguments is expanded
     * first and the call runs once per element of it, its answers laid end to end -- which is
     * how {@code generate_series(generate_series(1,2), 4)} produces 1,2,3,4 then 2,3,4.
     * Null when the call did not answer a set after all.
     */
    private List<Object> srfValues(FunctionCallExpr srf, RowContext ctx) {
        List<FunctionCallExpr> argSrfs = new ArrayList<>();
        for (Expression arg : srf.args()) collectSrfCalls(arg, argSrfs);
        if (argSrfs.isEmpty()) {
            Object raw = executor.evalExpr(srf, ctx);
            return raw instanceof List<?> ? new ArrayList<Object>((List<?>) raw) : null;
        }
        List<List<Object>> sets = new ArrayList<>(argSrfs.size());
        for (FunctionCallExpr argSrf : argSrfs) {
            List<Object> values = srfValues(argSrf, ctx);
            if (values == null) return null;
            sets.add(values);
        }
        int rows = 0;
        for (List<Object> set : sets) rows = Math.max(rows, set.size());
        List<Object> out = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            bindSrfElements(argSrfs, sets, i, ctx);
            try {
                Object raw = executor.evalExpr(srf, ctx);
                if (!(raw instanceof List<?>)) return null;
                out.addAll((List<?>) raw);
            } finally {
                for (FunctionCallExpr argSrf : argSrfs) ctx.clearBoundValue(argSrf);
            }
        }
        return out;
    }

    /**
     * One context per row the set-returning calls in {@code exprs} produce out of each input
     * context, with every call bound to its element of that row.
     *
     * <p>PostgreSQL expands the sets of a query below its grouping, so a set-returning call in
     * GROUP BY multiplies the rows first and the grouping then sees ordinary values:
     * {@code SELECT count(*) FROM one_row GROUP BY generate_series(1,2)} answers two groups of
     * one. The contexts are returned unchanged when no such call is written.
     */
    /**
     * The rows a query sorts, once the set-returning calls written only in its ORDER BY have been
     * expanded. A sort key that is also a select target is left alone: the projection expands
     * that one, and expanding it here as well would multiply the rows twice over.
     */
    private List<RowContext> expandContextsForOrderBySrfs(SelectStmt stmt, List<RowContext> contexts) {
        if (stmt.orderBy() == null || stmt.orderBy().isEmpty()) return contexts;
        List<Expression> keys = new ArrayList<>();
        for (SelectStmt.OrderByItem item : stmt.orderBy()) {
            if (!containsSrf(item.expr())) continue;
            if (resolveOrderByToColumnIndex(item.expr(), stmt.targets()) >= 0) continue;
            keys.add(item.expr());
        }
        return keys.isEmpty() ? contexts : expandContextsForSrfs(keys, contexts);
    }

    List<RowContext> expandContextsForSrfs(List<Expression> exprs, List<RowContext> contexts) {
        if (exprs == null || exprs.isEmpty()) return contexts;
        List<FunctionCallExpr> srfs = new ArrayList<>();
        for (Expression expr : exprs) srfs.addAll(collectSrfCalls(expr));
        if (srfs.isEmpty()) return contexts;
        List<RowContext> expanded = new ArrayList<>();
        for (RowContext ctx : contexts) {
            List<List<Object>> sets = new ArrayList<>(srfs.size());
            int rows = 0;
            for (FunctionCallExpr srf : srfs) {
                List<Object> values = srfValues(srf, ctx);
                // Defensive: a call that did not answer a set leaves the row as it was.
                if (values == null) { sets = null; break; }
                sets.add(values);
                rows = Math.max(rows, values.size());
            }
            if (sets == null) {
                expanded.add(ctx);
                continue;
            }
            for (int i = 0; i < rows; i++) {
                RowContext copy = ctx.copy();
                bindSrfElements(srfs, sets, i, copy);
                expanded.add(copy);
            }
        }
        return expanded;
    }

    /** Binds each call to its {@code i}th element, or to NULL where its set has run out. */
    private static void bindSrfElements(List<FunctionCallExpr> srfs, List<List<Object>> sets,
                                        int i, RowContext ctx) {
        for (int k = 0; k < srfs.size(); k++) {
            List<Object> set = sets.get(k);
            ctx.setBoundValue(srfs.get(k), i < set.size() ? set.get(i) : null);
        }
    }

    /**
     * Builds the result {@link Column} for a projected expression that isn't a plain column
     * reference (which instead copies its source Column verbatim, including enum type name). When
     * the expression's inferred type is {@link DataType#ENUM} (e.g. {@code COALESCE(mode,
     * 'manual')}, a {@code CASE} branching to an enum, an explicit enum cast, ...), the generic
     * {@link DataType#ENUM} has no per-type OID of its own (it's the hardcoded placeholder OID 0)
     * -- pgjdbc needs the concrete enum type name to resolve the real OID via the session's
     * pg_type catalog (see {@code PgWireValueFormatter.columnTypeOid}) and crashes otherwise
     * (mtask-8 C1). Recovers that name where statically determinable via
     * {@link AstExecutor#resolveEnumTypeName}; falls back to advertising TEXT (safe) rather than
     * an unnamed ENUM when it can't be determined.
     */
    private Column buildProjectedColumn(String alias, Expression expr, List<RowContext.TableBinding> bindings) {
        // Delegates to ExprEvaluator.buildResultColumn, which also recognizes array_agg over a
        // custom-enum element and advertises the enum's ARRAY type (wave-5, group 9) on top of
        // the scalar-ENUM name recovery described above.
        return executor.buildResultColumn(alias, expr, bindings);
    }

    // ---- CTE delegation ----

    SelectStmt.CommonTableExpr lookupCte(String name) {
        return cteExecutor.lookupCte(name);
    }

    QueryResult executeCte(SelectStmt.CommonTableExpr cte) {
        return cteExecutor.executeCte(cte);
    }

    // ---- Set operations delegation ----

    QueryResult executeSetOp(SetOpStmt stmt) {
        return setOpExecutor.executeSetOp(stmt);
    }

    // ---- Static utilities ----

    static java.math.BigDecimal toBigDecimal(Object val) {
        if (val instanceof PgMoney) return ((PgMoney) val).getValue();
        if (val instanceof java.math.BigDecimal) return ((java.math.BigDecimal) val);
        if (val instanceof Integer) return java.math.BigDecimal.valueOf(((Integer) val));
        if (val instanceof Long) return java.math.BigDecimal.valueOf(((Long) val));
        if (val instanceof Double) return java.math.BigDecimal.valueOf(((Double) val));
        if (val instanceof Float) return java.math.BigDecimal.valueOf(((Float) val));
        if (val instanceof Number) return java.math.BigDecimal.valueOf(((Number) val).doubleValue());
        return new java.math.BigDecimal(val.toString());
    }

    /**
     * Collect every base relation name and alias reachable from a FROM item, so that a plain
     * FOR UPDATE over a join locks both sides rather than nothing.
     */
    private static void collectLockTargets(SelectStmt.FromItem fi, Set<String> out) {
        if (fi instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef tr = (SelectStmt.TableRef) fi;
            out.add(tr.table().toLowerCase());
            if (tr.alias() != null) out.add(tr.alias().toLowerCase());
        } else if (fi instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom jf = (SelectStmt.JoinFrom) fi;
            if (jf.left() != null) collectLockTargets(jf.left(), out);
            if (jf.right() != null) collectLockTargets(jf.right(), out);
        }
        // Subqueries and set-returning functions have no lockable base rows of their own
    }

    /** True when this row binding belongs to one of the FOR UPDATE lock targets. */
    private static boolean isLockTarget(RowContext.TableBinding b, Set<String> targets) {
        if (b.alias() != null && targets.contains(b.alias().toLowerCase())) return true;
        return b.table() != null && targets.contains(b.table().getName().toLowerCase());
    }

}
