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

    /** Check if a column name is a PostgreSQL system column. */
    static boolean isSystemColumn(String name) {
        String lc = name.toLowerCase();
        return lc.equals("tableoid") || lc.equals("ctid") || lc.equals("xmin")
                || lc.equals("xmax") || lc.equals("cmin") || lc.equals("cmax");
    }

    private static final Set<String> AGGREGATE_FUNCTIONS = Cols.setOf(
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
            "jsonb_path_query", "jsonb_path_query_tz", "aclexplode", "string_to_table", "regexp_split_to_table",
            "pg_listening_channels", "pg_snapshot_xip", "txid_snapshot_xip");
    private static final Set<String> SRF_FUNCTIONS = SRF_FUNCTION_NAMES;

    SelectExecutor(AstExecutor executor) {
        this.executor = executor;
        this.aggregateEvaluator = new SelectAggregateEvaluator(this);
        this.windowEvaluator = new SelectWindowEvaluator(this);
        this.cteExecutor = new SelectCteExecutor(this);
        this.setOpExecutor = new SelectSetOpExecutor(this);
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
            return executeSelectInner(stmt);
        } finally {
            if (pushedCteScope) {
                executor.cteStack.pop();
            }
        }
    }

    private QueryResult executeSelectInner(SelectStmt stmt) {
        // SELECT without FROM
        if (stmt.from() == null || stmt.from().isEmpty()) {
            boolean hasAgg = hasAggregateInTargets(stmt.targets())
                    || (stmt.having() != null && containsAggregate(stmt.having()));
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

        List<RowContext.TableBinding> baseBindings;
        if (!contexts.isEmpty()) {
            baseBindings = contexts.get(0).getBindings();
        } else {
            baseBindings = executor.fromResolver.resolveTableBindings(stmt.from());
        }

        // Validate column references against table schema
        boolean simpleFrom = stmt.from().stream().allMatch(f -> f instanceof SelectStmt.TableRef);
        boolean hasJoins = stmt.from().stream().anyMatch(f -> f instanceof SelectStmt.JoinFrom);
        Set<String> usingColumnsLower = new java.util.HashSet<>();
        collectUsingColumns(stmt.from(), usingColumnsLower);
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
                        if (matchCount > 1 && !usingColumnsLower.contains(cr.column().toLowerCase())) {
                            throw new MemgresException("column reference \"" + cr.column() + "\" is ambiguous", "42702");
                        }
                        if (matchCount == 0) {
                            MemgresException colEx = new MemgresException("column \"" + cr.column() + "\" does not exist", "42703");
                            // Try to generate a hint by finding the closest matching column
                            for (RowContext.TableBinding b : baseBindings) {
                                String hint = RowContext.suggestClosestColumn(cr.column(), b.table());
                                if (hint != null) { colEx.setHint(hint); break; }
                            }
                            throw colEx;
                        }
                    } else {
                        boolean tableFound = false;
                        boolean colFound = false;
                        for (RowContext.TableBinding b : baseBindings) {
                            if (!cr.table().equalsIgnoreCase(b.alias()) && !cr.table().equalsIgnoreCase(b.table().getName())) continue;
                            tableFound = true;
                            if (b.table().getColumnIndex(cr.column()) >= 0) { colFound = true; break; }
                        }
                        if (!tableFound) {
                            throw new MemgresException("missing FROM-clause entry for table \"" + cr.table() + "\"", "42P01");
                        }
                        if (!colFound) {
                            MemgresException colEx = new MemgresException("column \"" + cr.column() + "\" does not exist", "42703");
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

        // Validate array subscript type errors for empty tables
        if (contexts.isEmpty() && simpleFrom && !baseBindings.isEmpty()) {
            for (SelectStmt.SelectTarget target : stmt.targets()) {
                if (target.expr() instanceof BinaryExpr && ((BinaryExpr) target.expr()).op() == BinaryExpr.BinOp.JSON_ARROW) {
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
            if (containsAggregate(stmt.where())) {
                throw new MemgresException("aggregate functions are not allowed in WHERE", "42803");
            }
            if (containsWindowFunction(stmt.where())) {
                throw new MemgresException("window functions are not allowed in WHERE", "42P20");
            }
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

        // Check if this query uses aggregation
        boolean hasGroupBy = stmt.groupBy() != null && !stmt.groupBy().isEmpty();
        boolean hasGroupingSets = stmt.groupingSets() != null && !stmt.groupingSets().isEmpty();
        boolean hasAggregates = hasAggregateInTargets(stmt.targets()) ||
                (stmt.having() != null && containsAggregate(stmt.having()));

        if (hasGroupBy || hasGroupingSets || hasAggregates) {
            // PG allows DISTINCT ON with GROUP BY and aggregates — DISTINCT ON is applied after grouping
            // Validate: non-aggregate columns must be in GROUP BY
            if (!hasGroupBy && !hasGroupingSets && hasAggregates) {
                for (SelectStmt.SelectTarget target : stmt.targets()) {
                    if (!isAggregateOrConstant(target.expr())) {
                        String colName = executor.exprToAlias(target.expr());
                        throw new MemgresException(
                                "column \"" + colName + "\" must appear in the GROUP BY clause or be used in an aggregate function",
                                "42803");
                    }
                }
                // Validate ORDER BY: non-aggregate columns are not allowed without GROUP BY
                if (stmt.orderBy() != null) {
                    for (SelectStmt.OrderByItem ob : stmt.orderBy()) {
                        Expression obExpr = ob.expr();
                        if (obExpr instanceof Literal && ((Literal) obExpr).literalType() == Literal.LiteralType.INTEGER) continue;
                        if (!isAggregateOrConstant(obExpr)) {
                            String colName = obExpr instanceof ColumnRef ? ((ColumnRef) obExpr).column()
                                    : executor.exprToAlias(obExpr);
                            // Check if it matches a target alias that is an aggregate
                            boolean matchesAggTarget = false;
                            for (SelectStmt.SelectTarget t : stmt.targets()) {
                                if (t.alias() != null && t.alias().equalsIgnoreCase(colName) && isAggregateOrConstant(t.expr())) {
                                    matchesAggTarget = true;
                                    break;
                                }
                            }
                            if (!matchesAggTarget) {
                                String tableName = obExpr instanceof ColumnRef && ((ColumnRef) obExpr).table() != null
                                        ? ((ColumnRef) obExpr).table() + "." + colName : colName;
                                throw new MemgresException(
                                        "column \"" + tableName + "\" must appear in the GROUP BY clause or be used in an aggregate function",
                                        "42803");
                            }
                        }
                    }
                }
            }
            // Validate: aggregates not allowed in GROUP BY
            if (hasGroupBy) {
                for (Expression gExpr : stmt.groupBy()) {
                    if (containsAggregate(gExpr)) {
                        throw new MemgresException("aggregate functions are not allowed in GROUP BY", "42803");
                    }
                }
                boolean groupByIsSimple = stmt.groupBy().stream().allMatch(g ->
                        g instanceof ColumnRef || (g instanceof Literal && ((Literal) g).literalType() == Literal.LiteralType.INTEGER));
                if (groupByIsSimple) {
                    Set<String> groupedExprs = new java.util.HashSet<>();
                    for (Expression gExpr : stmt.groupBy()) {
                        if (gExpr instanceof Literal && ((Literal) gExpr).literalType() == Literal.LiteralType.INTEGER) {
                            Literal lit = (Literal) gExpr;
                            int ordinal = Integer.parseInt(lit.value());
                            if (ordinal >= 1 && ordinal <= stmt.targets().size()) {
                                Expression targetExpr = stmt.targets().get(ordinal - 1).expr();
                                groupedExprs.add(targetExpr.toString().toLowerCase());
                                if (targetExpr instanceof ColumnRef) groupedExprs.add(((ColumnRef) targetExpr).column().toLowerCase());
                                String alias = stmt.targets().get(ordinal - 1).alias();
                                if (alias != null) groupedExprs.add(alias.toLowerCase());
                            }
                        } else {
                            groupedExprs.add(gExpr.toString().toLowerCase());
                            if (gExpr instanceof ColumnRef) groupedExprs.add(((ColumnRef) gExpr).column().toLowerCase());
                            if (gExpr instanceof ColumnRef) {
                                ColumnRef cr2 = (ColumnRef) gExpr;
                                for (SelectStmt.SelectTarget t : stmt.targets()) {
                                    if (cr2.column().equalsIgnoreCase(t.alias())) {
                                        groupedExprs.add(t.expr().toString().toLowerCase());
                                        if (t.expr() instanceof ColumnRef) groupedExprs.add(((ColumnRef) t.expr()).column().toLowerCase());
                                    }
                                }
                            }
                        }
                    }
                    for (SelectStmt.SelectTarget target : stmt.targets()) {
                        if (target.expr() instanceof WildcardExpr) continue;
                        if (!isAggregateOrConstant(target.expr())) {
                            String colName = executor.exprToAlias(target.expr());
                            String alias = target.alias();
                            if (!groupedExprs.contains(colName.toLowerCase())
                                    && !groupedExprs.contains(target.expr().toString().toLowerCase())
                                    && !(alias != null && groupedExprs.contains(alias.toLowerCase()))
                                    && !allColumnRefsCoveredByGroupBy(target.expr(), groupedExprs)) {
                                throw new MemgresException(
                                        "column \"" + colName + "\" must appear in the GROUP BY clause or be used in an aggregate function",
                                        "42803");
                            }
                        }
                    }
                    if (stmt.orderBy() != null) {
                        Set<String> targetExprs = new java.util.HashSet<>();
                        for (SelectStmt.SelectTarget t : stmt.targets()) {
                            targetExprs.add(executor.exprToAlias(t.expr()).toLowerCase());
                            if (t.alias() != null) targetExprs.add(t.alias().toLowerCase());
                            if (t.expr() instanceof ColumnRef) targetExprs.add(((ColumnRef) t.expr()).column().toLowerCase());
                        }
                        for (SelectStmt.OrderByItem ob : stmt.orderBy()) {
                            Expression obExpr = ob.expr();
                            if (obExpr instanceof Literal && ((Literal) obExpr).literalType() == Literal.LiteralType.INTEGER) continue;
                            if (!isAggregateOrConstant(obExpr) && obExpr instanceof ColumnRef) {
                                ColumnRef cr = (ColumnRef) obExpr;
                                String colName = cr.column().toLowerCase();
                                if (!groupedExprs.contains(colName) && !targetExprs.contains(colName)) {
                                    throw new MemgresException(
                                        "column \"" + cr.column() + "\" must appear in the GROUP BY clause or be used in an aggregate function",
                                        "42803");
                                }
                            }
                        }
                    }
                }
            }
            return aggregateEvaluator.executeAggregateSelect(stmt, contexts, baseBindings);
        }

        // Check for window functions in targets
        if (hasWindowFunctionInTargets(stmt.targets())) {
            return windowEvaluator.executeWindowSelect(stmt, contexts, baseBindings);
        }

        // Non-aggregate SELECT path
        List<Column> resultColumns = new ArrayList<>();
        List<java.util.function.Function<RowContext, Object>> projections = new ArrayList<>();

        buildProjections(stmt.targets(), baseBindings, resultColumns, projections, usingColumnsLower);

        List<SelectStmt.OrderByItem> resolvedOrderBy = resolveOrderBy(stmt.orderBy(), stmt.targets());

        // Validate: for SELECT DISTINCT, ORDER BY expressions must appear in select list
        if (stmt.distinct() && (stmt.distinctOn() == null || stmt.distinctOn().isEmpty()) && resolvedOrderBy != null && !resolvedOrderBy.isEmpty()) {
            Set<String> targetExprs = new java.util.HashSet<>();
            for (SelectStmt.SelectTarget t : stmt.targets()) {
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

        // Check for SRFs
        Set<Integer> srfIndices = new HashSet<>();
        for (int ti = 0; ti < stmt.targets().size(); ti++) {
            if (isSrfCall(stmt.targets().get(ti).expr())) {
                srfIndices.add(ti);
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

        // Row-level locking
        String lockTableName = null;
        if (stmt.lockClause() != null && executor.session != null && stmt.from() != null) {
            for (SelectStmt.FromItem fi : stmt.from()) {
                if (fi instanceof SelectStmt.TableRef) {
                    SelectStmt.TableRef tr = (SelectStmt.TableRef) fi;
                    lockTableName = tr.table();
                    break;
                }
            }
        }
        if (lockTableName != null && stmt.lockClause() != null && stmt.lockClause().skipLocked()) {
            final String tName = lockTableName;
            int effectiveLimit = Integer.MAX_VALUE;
            int effectiveOffset = 0;
            if (stmt.offset() != null) {
                Object offVal = executor.evalExpr(stmt.offset(), contexts.isEmpty() ? null : contexts.get(0));
                if (offVal instanceof Number) effectiveOffset = ((Number) offVal).intValue();
            }
            if (stmt.limit() != null) {
                Object limVal = executor.evalExpr(stmt.limit(), contexts.isEmpty() ? null : contexts.get(0));
                if (limVal instanceof Number) effectiveLimit = ((Number) limVal).intValue();
            }
            int needed = effectiveOffset + effectiveLimit;
            List<RowContext> filtered = new ArrayList<>();
            final String lockMode = stmt.lockClause().mode();
            for (RowContext ctx : contexts) {
                boolean lockable = true;
                RowContext.TableBinding lockedBinding = null;
                for (RowContext.TableBinding b : ctx.getBindings()) {
                    if (b.table().getName().equalsIgnoreCase(tName)) {
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
                        executor.database.unlockRow(tName, lockedBinding.row());
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

        if (lockTableName != null && stmt.lockClause() != null && !stmt.lockClause().skipLocked()) {
            final String tName = lockTableName;
            SelectStmt.LockClause lc = stmt.lockClause();
            final String lockMode = lc.mode();
            if (lc.nowait()) {
                for (RowContext ctx : contexts) {
                    for (RowContext.TableBinding b : ctx.getBindings()) {
                        if (b.table().getName().equalsIgnoreCase(tName)) {
                            if (!executor.database.tryLockRow(tName, b.row(), executor.session, lockMode)) {
                                throw new MemgresException("could not obtain lock on row in relation \"" + tName + "\"", "55P03");
                            }
                        }
                    }
                }
            } else {
                for (RowContext ctx : contexts) {
                    for (RowContext.TableBinding b : ctx.getBindings()) {
                        if (b.table().getName().equalsIgnoreCase(tName)) {
                            executor.database.lockRowWaiting(tName, b.row(), executor.session, lockMode);
                        }
                    }
                }
            }
        }

        // Apply WITH TIES on contexts before projection (needs access to ORDER BY expressions)
        if (stmt.withTies() && resolvedOrderBy != null && !resolvedOrderBy.isEmpty()
                && stmt.limit() != null && !hasSrf) {
            // Apply OFFSET on contexts
            if (stmt.offset() != null) {
                int off = executor.toInt(executor.evalExpr(stmt.offset(), null));
                if (off < 0) throw new MemgresException("OFFSET must not be negative", "2201X");
                if (off > 0 && off < contexts.size()) {
                    contexts = new ArrayList<>(contexts.subList(off, contexts.size()));
                } else if (off >= contexts.size()) {
                    contexts = new ArrayList<>();
                }
            }
            // Apply LIMIT WITH TIES on contexts
            int lim = executor.toInt(executor.evalExpr(stmt.limit(), null));
            if (lim < 0) throw new MemgresException("LIMIT must not be negative", "2201W");
            if (lim < contexts.size() && !contexts.isEmpty()) {
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
                    else if (va == null) cmp = item.nullsFirst() != null && item.nullsFirst() ? -1 : 1;
                    else if (vb == null) cmp = item.nullsFirst() != null && item.nullsFirst() ? 1 : -1;
                    else {
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

    /** Check if an expression is an aggregate call, a constant, or composed entirely of aggregates/constants. */
    boolean isAggregateOrConstant(Expression expr) {
        if (expr instanceof Literal) return true;
        if (expr instanceof OrderedSetAggExpr) return true;
        if (expr instanceof FunctionCallExpr) return isAggregateFunction(((FunctionCallExpr) expr).name()) || ((FunctionCallExpr) expr).args().stream().allMatch(this::isAggregateOrConstant);
        if (expr instanceof CastExpr) return isAggregateOrConstant(((CastExpr) expr).expr());
        if (expr instanceof IsJsonExpr) return isAggregateOrConstant(((IsJsonExpr) expr).expr());
        if (expr instanceof IsNullExpr) return isAggregateOrConstant(((IsNullExpr) expr).expr());
        if (expr instanceof BinaryExpr) return isAggregateOrConstant(((BinaryExpr) expr).left()) && isAggregateOrConstant(((BinaryExpr) expr).right());
        if (expr instanceof CustomOperatorExpr) { CustomOperatorExpr c = (CustomOperatorExpr) expr; return (c.left() == null || isAggregateOrConstant(c.left())) && isAggregateOrConstant(c.right()); }
        if (expr instanceof UnaryExpr) return isAggregateOrConstant(((UnaryExpr) expr).operand());
        if (expr instanceof SubqueryExpr) return true;
        if (expr instanceof ExistsExpr) return true;
        if (expr instanceof WindowFuncExpr) return true;
        if (expr instanceof InExpr) return isAggregateOrConstant(((InExpr) expr).expr());
        if (expr instanceof CaseExpr) {
            CaseExpr c = (CaseExpr) expr;
            if (c.operand() != null && !isAggregateOrConstant(c.operand())) return false;
            for (CaseExpr.WhenClause when : c.whenClauses()) {
                if (!isAggregateOrConstant(when.condition()) || !isAggregateOrConstant(when.result())) return false;
            }
            return c.elseExpr() == null || isAggregateOrConstant(c.elseExpr());
        }
        return false;
    }

    private boolean allColumnRefsCoveredByGroupBy(Expression expr, Set<String> groupedExprs) {
        if (expr instanceof Literal) return true;
        if (expr instanceof ColumnRef) {
            ColumnRef cr = (ColumnRef) expr;
            String col = cr.column().toLowerCase();
            if (groupedExprs.contains(col)) return true;
            if (cr.table() != null) {
                String qualified = cr.table().toLowerCase() + "." + col;
                return groupedExprs.contains(qualified);
            }
            return false;
        }
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            if (isAggregateFunction(fn.name())) return true;
            return fn.args().stream().allMatch(a -> allColumnRefsCoveredByGroupBy(a, groupedExprs));
        }
        if (expr instanceof CastExpr) return allColumnRefsCoveredByGroupBy(((CastExpr) expr).expr(), groupedExprs);
        if (expr instanceof BinaryExpr) return allColumnRefsCoveredByGroupBy(((BinaryExpr) expr).left(), groupedExprs)
                && allColumnRefsCoveredByGroupBy(((BinaryExpr) expr).right(), groupedExprs);
        if (expr instanceof CustomOperatorExpr) { CustomOperatorExpr c = (CustomOperatorExpr) expr; return (c.left() == null || allColumnRefsCoveredByGroupBy(c.left(), groupedExprs)) && allColumnRefsCoveredByGroupBy(c.right(), groupedExprs); }
        if (expr instanceof UnaryExpr) return allColumnRefsCoveredByGroupBy(((UnaryExpr) expr).operand(), groupedExprs);
        if (expr instanceof CaseExpr) {
            CaseExpr c = (CaseExpr) expr;
            if (c.operand() != null && !allColumnRefsCoveredByGroupBy(c.operand(), groupedExprs)) return false;
            for (CaseExpr.WhenClause when : c.whenClauses()) {
                if (!allColumnRefsCoveredByGroupBy(when.condition(), groupedExprs)
                        || !allColumnRefsCoveredByGroupBy(when.result(), groupedExprs)) return false;
            }
            return c.elseExpr() == null || allColumnRefsCoveredByGroupBy(c.elseExpr(), groupedExprs);
        }
        if (expr instanceof SubqueryExpr) return true;
        if (expr instanceof ExistsExpr) return true;
        if (expr instanceof WindowFuncExpr) return true;
        if (expr instanceof OrderedSetAggExpr) return true;
        if (expr instanceof IsNullExpr) return allColumnRefsCoveredByGroupBy(((IsNullExpr) expr).expr(), groupedExprs);
        if (expr instanceof InExpr) return allColumnRefsCoveredByGroupBy(((InExpr) expr).expr(), groupedExprs);
        if (expr instanceof BetweenExpr) return allColumnRefsCoveredByGroupBy(((BetweenExpr) expr).expr(), groupedExprs);
        if (expr instanceof IsBooleanExpr) return allColumnRefsCoveredByGroupBy(((IsBooleanExpr) expr).expr(), groupedExprs);
        if (expr instanceof FieldAccessExpr) return allColumnRefsCoveredByGroupBy(((FieldAccessExpr) expr).expr(), groupedExprs);
        if (expr instanceof CollateExpr) return allColumnRefsCoveredByGroupBy(((CollateExpr) expr).expr(), groupedExprs);
        return false;
    }

    // ---- Shared SELECT helpers ----

    List<SelectStmt.OrderByItem> resolveOrderBy(List<SelectStmt.OrderByItem> orderBy,
                                                  List<SelectStmt.SelectTarget> targets) {
        if (orderBy == null || orderBy.isEmpty()) return orderBy;

        List<SelectStmt.OrderByItem> resolved = new ArrayList<>();
        for (SelectStmt.OrderByItem item : orderBy) {
            Expression expr = item.expr();

            if (expr instanceof Literal && ((Literal) expr).literalType() == Literal.LiteralType.INTEGER) {
                Literal lit = (Literal) expr;
                int pos = Integer.parseInt(lit.value());
                if (pos >= 1 && pos <= targets.size()) {
                    expr = targets.get(pos - 1).expr();
                } else if (pos < 1 || pos > targets.size()) {
                    throw new MemgresException("ORDER BY position " + pos + " is not in select list", "42P10");
                }
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
        if (stmt.distinct()) {
            Set<String> seen = new LinkedHashSet<>();
            List<Object[]> deduped = new ArrayList<>();
            for (Object[] row : resultRows) {
                String key = Arrays.deepToString(row);
                if (seen.add(key)) {
                    deduped.add(row);
                }
            }
            return deduped;
        }
        return resultRows;
    }

    List<Object[]> applyOffsetAndLimit(SelectStmt stmt, List<Object[]> resultRows) {
        if (stmt.offset() != null) {
            int off = executor.toInt(executor.evalExpr(stmt.offset(), null));
            if (off < 0) throw new MemgresException("OFFSET must not be negative", "2201X");
            if (off > 0 && off < resultRows.size()) {
                resultRows = new ArrayList<>(resultRows.subList(off, resultRows.size()));
            } else if (off >= resultRows.size()) {
                resultRows = Cols.listOf();
            }
        }
        if (stmt.limit() != null) {
            int lim = executor.toInt(executor.evalExpr(stmt.limit(), null));
            if (lim < 0) throw new MemgresException("LIMIT must not be negative", "2201W");
            if (lim < resultRows.size()) {
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

    private void buildProjections(List<SelectStmt.SelectTarget> targets,
                                   List<RowContext.TableBinding> baseBindings,
                                   List<Column> resultColumns,
                                   List<java.util.function.Function<RowContext, Object>> projections,
                                   Set<String> usingColumnsForDedup) {
        for (SelectStmt.SelectTarget target : targets) {
            if (target.expr() instanceof WildcardExpr) {
                WildcardExpr w = (WildcardExpr) target.expr();
                if (w.table() != null) {
                    for (int bIdx = 0; bIdx < baseBindings.size(); bIdx++) {
                        RowContext.TableBinding binding = baseBindings.get(bIdx);
                        if (binding.alias().equalsIgnoreCase(w.table()) ||
                                binding.table().getName().equalsIgnoreCase(w.table())) {
                            final int bindingIdx = bIdx;
                            for (int i = 0; i < binding.table().getColumns().size(); i++) {
                                resultColumns.add(binding.table().getColumns().get(i));
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
                            resultColumns.add(binding.table().getColumns().get(i));
                            final int colIdx = i;
                            projections.add(ctx -> ctx.getBindings().get(bindingIdx).row()[colIdx]);
                        }
                    }
                }
            } else if (target.expr() instanceof CompositeStarExpr) {
                CompositeStarExpr cse = (CompositeStarExpr) target.expr();
                String typeName = resolveCompositeTypeFromBindings(cse.expr(), baseBindings);
                if (typeName == null) typeName = executor.resolveCompositeTypeNamePublic(cse.expr(), null);
                if (typeName != null) {
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
                    Column rc = new Column(alias, sourceCol.getType(), sourceCol.isNullable(), sourceCol.isPrimaryKey(), null,
                            sourceCol.getEnumTypeName(), sourceCol.getPrecision(), sourceCol.getScale());
                    if (sourceTableName != null) {
                        String schemaKey = sourceSchemaName != null ? sourceSchemaName : "public";
                        int tblOid = executor.systemCatalog.getOid("rel:" + schemaKey + "." + sourceTableName);
                        rc.setTableOid(tblOid);
                        rc.setAttNum((short) (sourceColIdx + 1));
                    }
                    resultColumns.add(rc);
                } else {
                    resultColumns.add(new Column(alias, executor.inferTypeFromContext(expr, baseBindings), true, false, null));
                }
                projections.add(ctx -> executor.evalExpr(expr, ctx));
            }
        }
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
            for (SelectStmt.OrderByItem item : resolvedOrderBy) {
                CustomEnum ce = resolveEnumForExpr(item.expr(), contexts);
                enumLookups.add(ce);
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
        if (hasWindowFunctionInTargets(stmt.targets())) {
            Table virtualTable = new Table("__virtual__",
                    Cols.listOf(new Column("__dummy__", DataType.INTEGER, true, false, null)));
            virtualTable.insertRow(new Object[]{1});
            RowContext virtualCtx = new RowContext(Cols.listOf(
                    new RowContext.TableBinding(virtualTable, "__virtual__", new Object[]{1})));
            List<RowContext> virtualContexts = Cols.listOf(virtualCtx);
            return windowEvaluator.executeWindowSelect(stmt, virtualContexts, virtualCtx.getBindings());
        }
        if (stmt.limit() != null) {
            int lim = executor.toInt(executor.evalExpr(stmt.limit(), null));
            if (lim < 0) throw new MemgresException("LIMIT must not be negative", "2201W");
        }
        if (stmt.offset() != null) {
            int off = executor.toInt(executor.evalExpr(stmt.offset(), null));
            if (off < 0) throw new MemgresException("OFFSET must not be negative", "2201X");
        }
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
                    columns.add(new Column(alias, executor.inferExprType(target.expr()), true, false, null));
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
                    if (typeName != null) {
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
                    columns.add(new Column(alias, executor.inferExprType(target.expr()), true, false, null));
                    valuesList.add(executor.evalExpr(target.expr(), null));
                }
            }
            Object[] values = valuesList.toArray();
            List<Object[]> rows = new ArrayList<>();
            rows.add(values);
            return QueryResult.select(columns, rows);
        }

        Object[] values = new Object[stmt.targets().size()];
        for (int i = 0; i < stmt.targets().size(); i++) {
            SelectStmt.SelectTarget target = stmt.targets().get(i);
            String alias = target.alias();
            if (alias == null) {
                alias = executor.exprToAlias(target.expr());
            }

            DataType resultType = executor.inferExprType(target.expr());
            Object val = executor.evalExpr(target.expr(), null);
            if (val instanceof byte[] && resultType == DataType.TEXT) {
                resultType = DataType.BYTEA;
            }
            columns.add(new Column(alias, resultType, true, false, null));
            if (val instanceof List<?> && isSrfCall(target.expr())) {
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
            List<Object[]> rows = new ArrayList<>();
            int maxLen = 0;
            for (List<?> sl : srfMap.values()) {
                if (sl.size() > maxLen) maxLen = sl.size();
            }
            for (int ri = 0; ri < maxLen; ri++) {
                Object[] row = Arrays.copyOf(values, values.length);
                for (Map.Entry<Integer, List<?>> entry : srfMap.entrySet()) {
                    int idx = entry.getKey();
                    List<?> sl = entry.getValue();
                    row[idx] = ri < sl.size() ? sl.get(ri) : null;
                }
                rows.add(row);
            }
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
                        else if (va == null) cmp = item.nullsFirst() != null && item.nullsFirst() ? -1 : 1;
                        else if (vb == null) cmp = item.nullsFirst() != null && item.nullsFirst() ? 1 : -1;
                        else {
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
            if (stmt.offset() != null) {
                int off = executor.toInt(executor.evalExpr(stmt.offset(), null));
                if (off > 0 && off < rows.size()) rows = new ArrayList<>(rows.subList(off, rows.size()));
                else if (off >= rows.size()) rows = Cols.listOf();
            }
            if (stmt.limit() != null) {
                int lim = executor.toInt(executor.evalExpr(stmt.limit(), null));
                if (lim < rows.size()) rows = new ArrayList<>(rows.subList(0, lim));
            }
            return QueryResult.select(columns, rows);
        }

        return QueryResult.select(columns, Collections.singletonList(values));
    }

    private void collectUsingColumns(List<SelectStmt.FromItem> fromItems, Set<String> result) {
        if (fromItems == null) return;
        for (SelectStmt.FromItem item : fromItems) {
            if (item instanceof SelectStmt.JoinFrom) {
                SelectStmt.JoinFrom jf = (SelectStmt.JoinFrom) item;
                if (jf.using() != null) {
                    for (String col : jf.using()) result.add(col.toLowerCase());
                }
                collectUsingColumns(Cols.listOf(jf.left()), result);
                collectUsingColumns(Cols.listOf(jf.right()), result);
            }
        }
    }

    private boolean isSrfCall(Expression expr) {
        if (expr instanceof FunctionCallExpr && SRF_FUNCTIONS.contains(FunctionEvaluator.stripSchemaPrefix(((FunctionCallExpr) expr).name().toLowerCase()))) return true;
        if (expr instanceof CastExpr) return isSrfCall(((CastExpr) expr).expr());
        return false;
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
        if (val instanceof java.math.BigDecimal) return ((java.math.BigDecimal) val);
        if (val instanceof Integer) return java.math.BigDecimal.valueOf(((Integer) val));
        if (val instanceof Long) return java.math.BigDecimal.valueOf(((Long) val));
        if (val instanceof Double) return java.math.BigDecimal.valueOf(((Double) val));
        if (val instanceof Float) return java.math.BigDecimal.valueOf(((Float) val));
        if (val instanceof Number) return java.math.BigDecimal.valueOf(((Number) val).doubleValue());
        return new java.math.BigDecimal(val.toString());
    }
}
