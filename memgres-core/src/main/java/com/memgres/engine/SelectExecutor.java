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
            // Before the scope is pushed: a refusal here must not leave a WITH scope standing on
            // the stack for whatever the connection runs next.
            cteExecutor.validateRecursiveItems(stmt.withClauses());
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

    /** Whether any WITH item of this statement writes, at any depth. */
    private static boolean hasDataModifyingCte(SelectStmt stmt) {
        return AstWalk.anyMatch(stmt, node -> {
            if (!(node instanceof SelectStmt.CommonTableExpr)) return false;
            Statement q = ((SelectStmt.CommonTableExpr) node).query;
            return q instanceof InsertStmt || q instanceof UpdateStmt
                    || q instanceof DeleteStmt || q instanceof MergeStmt;
        });
    }

    private QueryResult executeSelectInner(SelectStmt stmt) {
        // The range table is built before any clause is read, so a relation the statement names
        // and does not have is reported on its own. Only the NAMES are resolved here and nothing
        // is read: reading a FROM item is observable -- a WITH item that writes would be applied,
        // a fault inside a derived table would surface -- and the statement may yet be refused.
        executor.fromResolver.checkRelationNamesExist(stmt);
        // The range table is finished before a clause is read, and two FROM items answering to one
        // name is a fault of the range table rather than of any clause. It is decided from the
        // written names alone, so it costs nothing and reads nothing.
        validateFromClause(stmt.from());

        boolean noFromClause = stmt.from() == null || stmt.from().isEmpty();
        // A query with no FROM clause has no range table to build, so nothing can outrank its
        // clause-level faults and they are judged straight away. One that has a FROM clause is
        // judged below, after the relations have been resolved.
        if (noFromClause) {
            // No relations to describe, but a call still has to resolve: a literal and a cast
            // carry a type of their own, which is what settles whether the call names a function.
            FilterCheck.reject(this, stmt, new QueryLevelScope(this, Cols.listOf(), null, stmt));
            rejectMisplacedSrfs(stmt);
        } else {
            // A join condition is part of the FROM clause, so PostgreSQL judges it while it builds
            // the range table rather than after -- and so does this, or the join is executed and
            // fails on the set it was going to be refused for.
            rejectSrfsInJoinConditions(stmt.from());
        }
        rejectLockOnCollapsedRows(stmt);
        // A VALUES list is a query with no rows to read, so an aggregate or a window call in one
        // has nothing to aggregate or to be numbered against; the parser records where the SELECT
        // came from because after desugaring it is otherwise a FROM-less SELECT like any other.
        if (stmt.fromValues()) {
            for (SelectStmt.SelectTarget target : stmt.targets()) {
                placementCheck.reject(target.expr(), "VALUES");
                // A VALUES list is a constant table: its rows are written out, not produced, so
                // there is nowhere for a set to expand into more of them. The one-row VALUES of
                // an INSERT is a different construct, projected like a select list, and PostgreSQL
                // does expand a set there -- that path is InsertStmt's own and is not this one.
                rejectSrfIn(target.expr(), "VALUES");
            }
        }
        // SELECT without FROM
        if (noFromClause) {
            rejectSrfInAggregates(stmt);
            validateDistinctOn(stmt);
            windowEvaluator.validateWindowUsage(stmt, null);
            windowEvaluator.validateAfterWhere(stmt);
            // Nothing supplies a column here, so only what the expressions write down is typed.
            // The select list is read before WHERE, and an aggregate may not stand in WHERE at
            // all -- which is what PostgreSQL says about SELECT 1 WHERE count(*).
            BooleanContext.Types noColumns = BooleanContext.Types.none();
            for (SelectStmt.SelectTarget target : stmt.targets()) {
                BooleanContext.scan(target.expr(), noColumns);
            }
            if (stmt.where() != null) {
                placementCheck.reject(stmt.where(), "WHERE");
                BooleanContext.check(stmt.where(), "WHERE", noColumns);
            }
            BooleanContext.check(stmt.having(), "HAVING", noColumns);
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

        // The clause-level checks consult what the relations SUPPLY, which is their shape, not
        // their rows. Describing the FROM clause is enough for that and runs none of it, which
        // matters because running it is observable: a data-modifying CTE would be applied, and a
        // fault in a derived table would be reported, for a statement PostgreSQL refuses outright.
        // resolveTableBindings swallows every resolution failure, so it cannot invent an error of
        // its own; a name that does not resolve is still reported by resolveFromClause below.
        // Describing a WITH item means running it, and a WITH item may be an INSERT. For a
        // statement carrying one, the clauses are judged on the raw tree instead -- the older,
        // coarser order -- because refusing the statement after its INSERT has been applied would
        // be a write PostgreSQL never performs.
        QueryLevelScope scope;
        if (hasDataModifyingCte(stmt)) {
            scope = null;
        } else {
            List<RowContext.TableBinding> described =
                    executor.fromResolver.resolveTableBindings(stmt.from());
            // Describing swallows every relation it cannot follow, so a short list is a list with
            // a relation missing from it and the columns that relation holds would look like
            // columns nothing holds.
            scope = new QueryLevelScope(this, described, null, stmt,
                    described.size() == FromResolver.relationCount(stmt.from()));
        }
        // A join condition belongs to the FROM clause, which PostgreSQL transforms before it reads
        // any other clause, so this is judged before the select list's calls are.
        rejectNonBooleanJoinConditions(stmt.from(), scope);
        FilterCheck.reject(this, stmt, scope);
        rejectMisplacedSrfs(stmt);

        List<RowContext> contexts = executor.fromResolver.resolveFromClause(
                stmt.from(), stmt.where(), stmt.having(), stmt);

        List<RowContext.TableBinding> baseBindings;
        // What the FROM clause exposes, in order: every relation's columns except where a USING
        // or NATURAL join merged two into one, which SELECT * lists first. Taken from a row when
        // there is one and worked out from the clause itself when there is not.
        List<RowContext.OutCol> baseOutput;
        boolean everyRelation;
        if (!contexts.isEmpty()) {
            baseBindings = contexts.get(0).getBindings();
            baseOutput = contexts.get(0).outputColumnsOrDefault();
            everyRelation = true;
        } else {
            baseBindings = executor.fromResolver.resolveTableBindings(stmt.from());
            baseOutput = executor.fromResolver.resolveClauseOutput(stmt.from(), baseBindings);
            everyRelation = baseBindings.size() == FromResolver.relationCount(stmt.from());
        }
        // Reading a relation takes a lock on it, and reading it FOR UPDATE takes a stronger one.
        // The lock is on the relation the query opened, not on the rows it found, so a scan that
        // matched nothing holds it just the same.
        recordReadLocks(stmt, baseBindings);

        // What the relations supply is now known, which is what the checks below consult to report
        // an unresolvable column or call before the clause it stands in is complained about. The
        // clause-level refusals themselves keep the order they had when they ran ahead of the
        // query: a call carrying a FILTER it may not have is judged first.
        scope = new QueryLevelScope(this, baseBindings, baseOutput, stmt, everyRelation);
        rejectSrfInAggregates(stmt);
        validateDistinctOn(stmt);

        // The relations are resolved first: a window frame's offset is resolved against the
        // column the window is ordered by, which is one of them.
        windowEvaluator.validateWindowUsage(stmt, scope);

        // Validate column references against table schema
        boolean simpleFrom = stmt.from().stream().allMatch(f -> f instanceof SelectStmt.TableRef);
        boolean hasJoins = stmt.from().stream().anyMatch(f -> f instanceof SelectStmt.JoinFrom);
        Set<String> usingColumnsLower = new java.util.HashSet<>();
        for (RowContext.OutCol oc : baseOutput) {
            if (oc.merged()) usingColumnsLower.add(oc.name.toLowerCase());
        }
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
                        // A merged column is one column of the join's output however many
                        // relations fed it, so what decides ambiguity is how many output columns
                        // answer to the name, not how many relations hold one.
                        int matchCount = 0;
                        for (RowContext.OutCol oc : baseOutput) {
                            if (oc.name.equalsIgnoreCase(cr.column())) matchCount++;
                        }
                        if (matchCount > 1) {
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
                                String hint = RowContext.suggestClosestColumn(cr.column(), baseBindings);
                                if (hint != null) colEx.setHint(hint);
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
                            throw outOfScopeOrMissing(cr.table(), stmt.from());
                        }
                        if (!colFound && !mayResolveViaAttributeNotation) {
                            // A qualified reference is named in full, the way RowContext names it
                            // when the same lookup fails at evaluation time.
                            MemgresException colEx = new MemgresException(
                                    "column " + cr.table() + "." + cr.column() + " does not exist", "42703");
                            List<RowContext.TableBinding> named = new ArrayList<>();
                            for (RowContext.TableBinding b : baseBindings) {
                                if (cr.table().equalsIgnoreCase(b.alias())
                                        || cr.table().equalsIgnoreCase(b.table().getName())) {
                                    named.add(b);
                                }
                            }
                            String hint = RowContext.suggestClosestColumn(cr.column(), named);
                            if (hint != null) colEx.setHint(hint);
                            throw colEx;
                        }
                    }
                }
            }
        }

        rejectAmbiguousQualifiedRefs(stmt, baseBindings, usingColumnsLower);

        // Every condition is coerced to boolean as the clause holding it is transformed, and
        // PostgreSQL transforms the FROM clause before the select list and the select list before
        // WHERE. Nothing in a select list has to be a condition, but a CASE, a FILTER or an AND
        // written there still holds one.
        BooleanContext.Types columnTypes = BooleanContext.Types.of(executor, baseBindings);
        for (SelectStmt.FromItem item : stmt.from()) checkJoinConditions(item, columnTypes);
        for (SelectStmt.SelectTarget target : stmt.targets()) {
            BooleanContext.scan(target.expr(), columnTypes);
        }

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
            placementCheck.reject(stmt.where(), "WHERE", scope);
            BooleanContext.check(stmt.where(), "WHERE", columnTypes);
            // Pre-flight type validation of WHERE clause (PG checks at plan time)
            // Only validate for simple single-table SELECTs (not CTEs/subqueries/joins)
            if (simpleFrom && baseBindings.size() == 1 && !hasJoins
                    && (stmt.withClauses() == null || stmt.withClauses().isEmpty())
                    && executor.cteStack.isEmpty()) {
                executor.validateWhereTypesAgainstTable(stmt.where(), baseBindings.get(0).table());
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
        BooleanContext.check(stmt.having(), "HAVING", columnTypes);
        if (stmt.orderBy() != null) {
            for (SelectStmt.OrderByItem item : stmt.orderBy()) {
                BooleanContext.scan(item.expr(), columnTypes);
            }
        }
        if (stmt.groupBy() != null) {
            for (Expression g : stmt.groupBy()) BooleanContext.scan(g, columnTypes);
        }

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
                    expandTargetsForOrdinals(stmt.targets(), baseBindings, baseOutput);
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
        buildProjections(stmt.targets(), baseBindings, baseOutput, resultColumns, projections,
                srfIndices);

        // An ordinal ORDER BY counts output columns, so a star target has to be expanded
        // first: SELECT * FROM t ORDER BY 2 means the table's second column.
        List<SelectStmt.SelectTarget> ordinalTargets = expandTargetsForOrdinals(stmt.targets(), baseBindings, baseOutput);
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

        if (!hasSrf && resolvedOrderBy != null && !resolvedOrderBy.isEmpty()) {
            // Sorting rewrites the list where it stands, so it needs one that may be rewritten.
            // A FROM item that works its rows out as they are read hands on a list that may not.
            contexts = new ArrayList<>(contexts);
            sortContexts(contexts, resolvedOrderBy);
        }

        // DISTINCT ON
        if (stmt.distinctOn() != null && !stmt.distinctOn().isEmpty()) {
            contexts = expandContextsForDistinctOnSrfs(stmt, contexts);
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
            // How many rows have to be locked before the OFFSET and LIMIT can be applied. Adding
            // the two ints overflowed whenever there was no LIMIT, and the negative total stopped
            // the scan before it locked anything: the query answered no rows at all.
            int needed = clampToSize((long) effectiveOffset + (long) effectiveLimit, Integer.MAX_VALUE);
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
        contexts = boundedByRowCount(stmt, contexts, hasSrf);
        // One entry per answer, in the order they are answered, naming the stored rows behind it.
        if (executor.cursorRowProvenance != null) {
            for (RowContext ctx : contexts) {
                executor.cursorRowProvenance.add(ctx.getBindings());
            }
        }
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
        checkLockTargetsResolvable(stmt, lock);
        rejectRowLockInReadOnlyTransaction(stmt, lock);
    }

    /**
     * A row lock is a write: it marks the row so no one else may change it, and a read-only
     * transaction may not do that. PostgreSQL names the lock mode that was asked for, and only
     * refuses when the lock has a relation to apply to — {@code SELECT 1 FOR UPDATE} has none.
     */
    private void rejectRowLockInReadOnlyTransaction(SelectStmt stmt, SelectStmt.LockClause lock) {
        if (executor.session == null || !executor.session.isReadOnly()) return;
        if (stmt.from() == null || stmt.from().isEmpty()) return;
        throw new MemgresException("cannot execute SELECT FOR "
                + (lock.mode() == null ? "UPDATE" : lock.mode())
                + " in a read-only transaction", "25006");
    }

    /**
     * Check the relations a row lock applies to.
     *
     * <p>Two things can go wrong. A name in {@code OF} may not be in FROM at all — and an alias
     * hides the table name it stands for, so {@code FROM t a ... FOR UPDATE OF t} names nothing.
     * And the nullable side of an outer join produces all-NULL rows that stand for no base row,
     * so there is nothing there to lock; PostgreSQL refuses rather than locking whatever the
     * other side happened to join to.
     */
    private void checkLockTargetsResolvable(SelectStmt stmt, SelectStmt.LockClause lock) {
        if (stmt.from() == null || stmt.from().isEmpty()) return;
        String mode = "FOR " + (lock.mode() == null ? "UPDATE" : lock.mode());
        Map<String, SelectStmt.FromItem> exposed = new LinkedHashMap<>();
        Set<String> nullable = new LinkedHashSet<>();
        for (SelectStmt.FromItem item : stmt.from()) {
            collectExposedNames(item, false, exposed, nullable);
        }
        Set<String> cteNames = new LinkedHashSet<>();
        if (stmt.withClauses() != null) {
            for (SelectStmt.CommonTableExpr cte : stmt.withClauses()) cteNames.add(cte.name().toLowerCase());
        }
        boolean named = lock.ofTables() != null && !lock.ofTables().isEmpty();
        List<String> targets = new ArrayList<String>();
        if (named) {
            for (String of : lock.ofTables()) targets.add(of.toLowerCase());
        } else {
            targets.addAll(exposed.keySet());
        }
        for (String target : targets) {
            SelectStmt.FromItem item = exposed.get(target);
            if (item == null) {
                if (!named) continue;
                throw new MemgresException("relation \"" + target + "\" in " + mode
                        + " clause not found in FROM clause", "42P01");
            }
            // A plain FOR UPDATE simply locks nothing in a FROM entry that has no base rows;
            // only naming one in OF is an error, because then the lock was asked for by name.
            if (named) {
                if (item instanceof SelectStmt.FunctionFrom) {
                    throw new MemgresException(mode + " cannot be applied to a function", "0A000");
                }
                if (item instanceof SelectStmt.TableRef && cteNames.contains(target)
                        && ((SelectStmt.TableRef) item).alias() == null) {
                    throw new MemgresException(mode + " cannot be applied to a WITH query", "0A000");
                }
            }
            // A derived table on the nullable side is refused like a base table: the lock reaches
            // through it to the relations it reads, and those have no row behind an all-NULL
            // output row either. A set-returning function is not, because there is no row to
            // lock in the first place, and PostgreSQL lets a plain FOR UPDATE ignore it.
            boolean reachesBaseRows = item instanceof SelectStmt.TableRef
                    || item instanceof SelectStmt.SubqueryFrom;
            if (nullable.contains(target) && (named || reachesBaseRows)) {
                throw new MemgresException(
                        mode + " cannot be applied to the nullable side of an outer join", "0A000");
            }
        }
    }

    /**
     * Map each FROM entry to the one name it is addressable by — its alias when it has one,
     * otherwise the relation name — and record which of those sit on a nullable join side.
     */
    private static void collectExposedNames(SelectStmt.FromItem item, boolean nullableHere,
                                            Map<String, SelectStmt.FromItem> exposed,
                                            Set<String> nullable) {
        if (item instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef tr = (SelectStmt.TableRef) item;
            String name = (tr.alias() != null ? tr.alias() : tr.table()).toLowerCase();
            exposed.put(name, item);
            if (nullableHere) nullable.add(name);
        } else if (item instanceof SelectStmt.SubqueryFrom) {
            String alias = ((SelectStmt.SubqueryFrom) item).alias();
            if (alias != null) {
                exposed.put(alias.toLowerCase(), item);
                if (nullableHere) nullable.add(alias.toLowerCase());
            }
        } else if (item instanceof SelectStmt.FunctionFrom) {
            SelectStmt.FunctionFrom ff = (SelectStmt.FunctionFrom) item;
            String name = (ff.alias() != null ? ff.alias() : ff.functionName()).toLowerCase();
            exposed.put(name, item);
            if (nullableHere) nullable.add(name);
        } else if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom jf = (SelectStmt.JoinFrom) item;
            SelectStmt.JoinType type = jf.joinType();
            boolean leftNullable = nullableHere
                    || type == SelectStmt.JoinType.RIGHT || type == SelectStmt.JoinType.NATURAL_RIGHT
                    || type == SelectStmt.JoinType.FULL || type == SelectStmt.JoinType.NATURAL_FULL;
            boolean rightNullable = nullableHere
                    || type == SelectStmt.JoinType.LEFT || type == SelectStmt.JoinType.NATURAL_LEFT
                    || type == SelectStmt.JoinType.FULL || type == SelectStmt.JoinType.NATURAL_FULL;
            if (jf.left() != null) collectExposedNames(jf.left(), leftNullable, exposed, nullable);
            if (jf.right() != null) collectExposedNames(jf.right(), rightNullable, exposed, nullable);
        }
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
            List<SelectStmt.SelectTarget> targets, List<RowContext.TableBinding> bindings,
            List<RowContext.OutCol> output) {
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
            if (w.table() == null && output != null) {
                for (RowContext.OutCol oc : output) {
                    if (oc.bindings[0] >= bindings.size()) continue;
                    Expression ref = starColumnExpr(oc, bindings, output);
                    // A merged column written out as its fallback still answers to its own name.
                    out.add(new SelectStmt.SelectTarget(ref,
                            ref instanceof FunctionCallExpr ? oc.name : null));
                }
                continue;
            }
            RowContext.TableBinding only = w.table() == null ? null : starTarget(w, bindings);
            for (RowContext.TableBinding b : bindings) {
                if (only != null && b != only) continue;
                if (only == null && w.table() != null && !b.alias().equalsIgnoreCase(w.table())
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

    /**
     * How one column of a star target is written out when a star has to become a list of
     * expressions — for an ORDER BY or GROUP BY that counts output columns.
     *
     * <p>A column of one relation is that relation's column, named in full. A column a USING or
     * NATURAL join merged is written bare, which is the name the query itself would write for it
     * and what a GROUP BY of that name matches. Only where a further relation carries the same
     * name is the bare name no longer that column — {@code a JOIN b USING (id) JOIN c ON true}
     * exposes two {@code id} and a bare one is ambiguous — and there it is written out as the
     * fallback it stands for.
     */
    private static Expression starColumnExpr(RowContext.OutCol oc,
                                             List<RowContext.TableBinding> bindings,
                                             List<RowContext.OutCol> output) {
        if (!oc.merged()) {
            return new ColumnRef(bindings.get(oc.bindings[0]).alias(), oc.name);
        }
        int sameName = 0;
        for (RowContext.OutCol other : output) {
            if (other.name.equalsIgnoreCase(oc.name)) sameName++;
        }
        if (sameName == 1) return new ColumnRef(null, oc.name);
        List<Expression> sources = new ArrayList<>(oc.bindings.length);
        for (int i = 0; i < oc.bindings.length; i++) {
            if (oc.bindings[i] >= bindings.size()) continue;
            sources.add(new ColumnRef(bindings.get(oc.bindings[i]).alias(),
                    bindings.get(oc.bindings[i]).table().getColumns().get(oc.columns[i]).getName()));
        }
        return new FunctionCallExpr("coalesce", sources);
    }

    /**
     * The single FROM entry a qualified star stands for, or null when the qualifier is a bare
     * name and the FROM entry of that name is whichever one answers to it.
     *
     * <p>A schema in the qualifier picks the entry out: {@code SELECT s2.t.* FROM s1.t, s2.t} is
     * s2's t and nothing else, and {@code SELECT s1.t.* FROM s2.t} reaches nothing at all. The
     * parser dropped the schema, so the star matched by name -- expanding both relations for the
     * first and s2's for the second, where PostgreSQL answers with one relation and an error.
     */
    private RowContext.TableBinding starTarget(WildcardExpr star,
                                               List<RowContext.TableBinding> bindings) {
        if (star.catalog() != null) {
            executor.exprEvaluator.rejectOtherCatalog(star.catalog(),
                    star.schema() + "." + star.table() + ".*");
        }
        if (star.schema() == null) {
            // A bare qualifier that names two FROM entries names neither of them in particular.
            // Expanding both put the columns of two relations behind one star.
            RowContext.TableBinding first = null;
            for (RowContext.TableBinding b : bindings) {
                String exposed = b.alias() != null ? b.alias() : b.table().getName();
                if (!exposed.equalsIgnoreCase(star.table())) continue;
                if (first != null && first != b) {
                    throw new MemgresException(
                            "table reference \"" + star.table() + "\" is ambiguous", "42P09");
                }
                first = b;
            }
            return null;
        }
        RowContext.TableBinding reached =
                executor.exprEvaluator.schemaPrefixReaches(bindings, star.schema(), star.table());
        if (reached != null) return reached;
        for (RowContext.TableBinding b : bindings) {
            String exposed = b.alias() != null ? b.alias() : b.table().getName();
            if (exposed.equalsIgnoreCase(star.table())) {
                MemgresException ex = new MemgresException("invalid reference to FROM-clause entry"
                        + " for table \"" + star.table() + "\"", "42P01");
                ex.setDetail("There is an entry for table \"" + star.table()
                        + "\", but it cannot be referenced from this part of the query.");
                throw ex;
            }
        }
        for (RowContext.TableBinding b : bindings) {
            if (b.alias() != null && b.table().getName().equalsIgnoreCase(star.table())) {
                MemgresException ex = new MemgresException("invalid reference to FROM-clause entry"
                        + " for table \"" + star.table() + "\"", "42P01");
                ex.setHint("Perhaps you meant to reference the table alias \"" + b.alias() + "\".");
                throw ex;
            }
        }
        throw new MemgresException(
                "missing FROM-clause entry for table \"" + star.table() + "\"", "42P01");
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
                Expression named = outputColumnNamed(targets, ref.column());
                if (named != null) expr = named;
            }

            resolved.add(new SelectStmt.OrderByItem(expr, item.descending(), item.nullsFirst()));
        }
        return resolved;
    }

    /**
     * The select item an ORDER BY name reaches, or null when it reaches none and the name is left
     * for the FROM clause to answer.
     *
     * <p>A name that two select items answer to reaches neither -- unless they are the same
     * expression written twice, which is one thing under two names and settles the sort either
     * way. {@code SELECT a, a FROM t ORDER BY a} sorts; {@code SELECT a, a + 1 AS a FROM t ORDER
     * BY a} and {@code SELECT n, s AS n FROM t ORDER BY n} have two different columns called the
     * same thing, and PostgreSQL refuses the name rather than taking the first.
     *
     * <p>Only an alias written on the item was consulted, so the implicit name a bare column
     * carries did not count as a match and the two-column clash went unseen.
     */
    private Expression outputColumnNamed(List<SelectStmt.SelectTarget> targets, String name) {
        Expression found = null;
        for (SelectStmt.SelectTarget target : targets) {
            String outputName = target.alias() != null
                    ? target.alias() : executor.exprToAlias(target.expr());
            if (outputName == null || !outputName.equals(name)) continue;
            if (found != null && !found.equals(target.expr())) {
                throw new MemgresException("ORDER BY \"" + name + "\" is ambiguous", "42702");
            }
            if (found == null) found = target.expr();
        }
        return found;
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
        // LIMIT and OFFSET are bigint. A literal past that range is out of range, not a negative
        // count — the sign it appears to have is only what the wrap left behind.
        if (raw instanceof java.math.BigInteger
                || (raw instanceof java.math.BigDecimal
                    && ((java.math.BigDecimal) raw).compareTo(BIGINT_MAX) > 0)) {
            java.math.BigDecimal big = raw instanceof java.math.BigInteger
                    ? new java.math.BigDecimal((java.math.BigInteger) raw)
                    : (java.math.BigDecimal) raw;
            if (big.compareTo(BIGINT_MAX) > 0 || big.compareTo(BIGINT_MIN) < 0) {
                throw new MemgresException("bigint out of range", "22003");
            }
        }
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

    private static final java.math.BigDecimal BIGINT_MAX =
            java.math.BigDecimal.valueOf(Long.MAX_VALUE);
    private static final java.math.BigDecimal BIGINT_MIN =
            java.math.BigDecimal.valueOf(Long.MIN_VALUE);

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
                                   List<RowContext.OutCol> baseOutput,
                                   List<Column> resultColumns,
                                   List<java.util.function.Function<RowContext, Object>> projections,
                                   Set<Integer> srfProjections) {
        for (SelectStmt.SelectTarget target : targets) {
            int projectionStart = projections.size();
            if (target.expr() instanceof WildcardExpr) {
                WildcardExpr w = (WildcardExpr) target.expr();
                if (w.table() != null) {
                    RowContext.TableBinding only = starTarget(w, baseBindings);
                    boolean named = false;
                    for (int bIdx = 0; bIdx < baseBindings.size(); bIdx++) {
                        RowContext.TableBinding binding = baseBindings.get(bIdx);
                        if (only != null ? binding != only
                                : !(binding.alias().equalsIgnoreCase(w.table())
                                    || binding.table().getName().equalsIgnoreCase(w.table()))) {
                            continue;
                        }
                        named = true;
                        final int bindingIdx = bIdx;
                        for (int i = 0; i < binding.table().getColumns().size(); i++) {
                            resultColumns.add(copyColumnWithOid(binding.table(), i));
                            final int colIdx = i;
                            projections.add(ctx -> ctx.getBindings().get(bindingIdx).row()[colIdx]);
                        }
                    }
                    // A qualifier no FROM item answers to expands to nothing at all, which is a
                    // query returning rows of no columns rather than the refusal it should be.
                    if (!named && !baseBindings.isEmpty()) {
                        throw new MemgresException(
                                "missing FROM-clause entry for table \"" + w.table() + "\"", "42P01");
                    }
                } else {
                    // The FROM clause's own columns, in its own order: a column two relations
                    // merged into one is listed once, where the merge puts it.
                    for (RowContext.OutCol oc : baseOutput) {
                        if (oc.bindings[0] >= baseBindings.size()) continue;
                        RowContext.TableBinding binding = baseBindings.get(oc.bindings[0]);
                        resultColumns.add(mergedColumnType(oc, copyColumnWithOid(binding.table(), oc.columns[0])));
                        final RowContext.OutCol col = oc;
                        projections.add(ctx -> col.valueIn(ctx.getBindings()));
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
                // A join merged this name into a column of its own, whose type is neither side's.
                RowContext.OutCol mergedRef = mergedOutCol(expr, baseOutput);
                if (mergedRef != null && mergedRef.bindings[0] < baseBindings.size()) {
                    Column source = baseBindings.get(mergedRef.bindings[0]).table()
                            .getColumns().get(mergedRef.columns[0]);
                    resultColumns.add(new Column(alias, mergedRef.type, source.isNullable(),
                            false, null));
                    projections.add(ctx -> executor.evalExpr(expr, ctx));
                    if (srfProjections != null && isSrfCall(target.expr())) {
                        for (int pi = projectionStart; pi < projections.size(); pi++) srfProjections.add(pi);
                    }
                    continue;
                }
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

    /**
     * The merged output column an unqualified reference names, when the FROM clause has one whose
     * type is not the type of the relation column behind it. A qualified reference names one
     * relation's own column and is not the merge.
     */
    private static RowContext.OutCol mergedOutCol(Expression expr, List<RowContext.OutCol> output) {
        if (output == null) return null;
        if (!(expr instanceof ColumnRef)) return null;
        ColumnRef ref = (ColumnRef) expr;
        if (ref.table() != null || ref.column() == null) return null;
        RowContext.OutCol hit = null;
        for (RowContext.OutCol oc : output) {
            if (!oc.name.equalsIgnoreCase(ref.column())) continue;
            if (hit != null) return null;
            hit = oc;
        }
        return hit != null && hit.type != null ? hit : null;
    }

    /** The same column described with the type a join's merge resolved for it, where it has one. */
    private static Column mergedColumnType(RowContext.OutCol oc, Column column) {
        if (oc.type == null) return column;
        Column merged = new Column(column.getName(), oc.type, column.isNullable(), false, null);
        merged.setTableOid(column.getTableOid());
        merged.setAttNum(column.getAttNum());
        return merged;
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

    /**
     * The rows the select list has to be evaluated for, where the query says how many it wants.
     *
     * <p>A row past the limit cannot reach the answer, so evaluating the select list for it is
     * work done for nothing: {@code LIMIT 1} over a five-million-row series projected five
     * million rows to return one, which took forty times as long as counting all five million.
     * Dropping them first is the same answer with the same rows in the same order.
     *
     * <p>Only where every remaining row still stands for exactly one output row. DISTINCT, WITH
     * TIES and a set-returning select list each decide how many rows come out only once the
     * projection has run, so under any of them there is no count to cut at. And only where the
     * count is already a number: an expression is evaluated where the query says, once.
     */
    private List<RowContext> boundedByRowCount(SelectStmt stmt, List<RowContext> contexts,
                                               boolean hasSrf) {
        if (hasSrf || stmt.limit() == null || stmt.withTies() || stmt.distinct()) return contexts;
        if (stmt.distinctOn() != null && !stmt.distinctOn().isEmpty()) return contexts;
        Long limit = writtenRowCount(stmt.limit());
        if (limit == null || limit < 0) return contexts;
        long offset = 0;
        if (stmt.offset() != null) {
            Long written = writtenRowCount(stmt.offset());
            if (written == null) return contexts;
            offset = Math.max(0L, written);
        }
        // A count past what a list can address is no bound at all, and adding two of them is how
        // a bound turns negative.
        if (offset > Integer.MAX_VALUE || limit > Integer.MAX_VALUE) return contexts;
        long wanted = offset + limit;
        if (wanted >= contexts.size()) return contexts;
        return contexts.subList(0, (int) wanted);
    }

    /**
     * A row count written as a number, or null where the query works it out for itself. A bound
     * parameter is a number too — it was decided before the statement ran — but anything the
     * query computes is left to the clause that computes it, so nothing is evaluated twice.
     */
    private Long writtenRowCount(Expression expr) {
        if (expr instanceof Literal && ((Literal) expr).literalType() == Literal.LiteralType.INTEGER) {
            try {
                return Long.valueOf(((Literal) expr).value());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        if (expr instanceof ParamRef) {
            Object value = executor.evalExpr(expr, null);
            return value instanceof Number ? Long.valueOf(((Number) value).longValue()) : null;
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

    /** Every ON condition the FROM clause writes, each of which has to be a condition. */
    private void checkJoinConditions(SelectStmt.FromItem item, BooleanContext.Types types) {
        if (!(item instanceof SelectStmt.JoinFrom)) return;
        SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
        checkJoinConditions(join.left(), types);
        checkJoinConditions(join.right(), types);
        BooleanContext.check(join.on(), "JOIN/ON", types);
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
                return QueryResult.select(columns, applyOffsetAndLimit(stmt,
                        applyDistinct(stmt, expandSrfRows(values, srfMap))));
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
                // A column an enclosing query level supplies — which is what a LATERAL projects —
                // keeps the whole of its declared type. A bare DataType does not carry an array's
                // element type, and an int[] read through a LATERAL called itself _int4.
                Column outerCol = target.expr() instanceof ColumnRef
                        ? executor.exprEvaluator.columnFromOuterContexts((ColumnRef) target.expr())
                        : null;
                columns.add(outerCol != null
                        ? buildProjectedColumn(alias, target.expr(), Cols.listOf())
                        : new Column(alias, resultType, true, false, null));
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
            // DISTINCT reads the rows the query answers with, and a set-returning target is what
            // produced them: PostgreSQL expands the set and then folds the duplicates, so
            // SELECT DISTINCT unnest(ARRAY[1,1,2]) answers two rows. Skipping this pass left the
            // duplicate the expansion had just created.
            return QueryResult.select(columns,
                    applyOffsetAndLimit(stmt, applyDistinct(stmt, rows)));
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
    private void rejectMisplacedSrfs(SelectStmt stmt) {
        rejectSrfIn(stmt.where(), "WHERE");
        rejectSrfIn(stmt.having(), "HAVING");
        rejectSrfIn(stmt.limit(), "LIMIT");
        rejectSrfIn(stmt.offset(), "OFFSET");
        rejectSrfsInJoinConditions(stmt.from());
        if (stmt.targets() != null) {
            for (SelectStmt.SelectTarget t : stmt.targets()) {
                rejectSrfWhereOneBooleanIsWanted(t.expr());
                MemgresException conditional = conditionalHidingSrf(t.expr());
                if (conditional != null) throw conditional;
            }
        }
    }

    /** Refuses a set-returning call written in a clause that reads rows already produced. */
    void rejectSrfIn(Expression expr, String clause) {
        if (expr == null) return;
        List<FunctionCallExpr> found = collectSrfCalls(expr);
        if (!found.isEmpty()) throw misplacedSrf(clause, found.get(0));
    }

    /**
     * Somewhere that wants one boolean and gets a set, PostgreSQL names the kind of value rather
     * than the placement rule: a WHEN condition, and either side of AND, OR and NOT (BETWEEN
     * among them, which is an AND once written out). An SRF elsewhere in the same constructs --
     * a CASE operand or result -- is the placement rule again and reported so.
     */
    private void rejectSrfWhereOneBooleanIsWanted(Expression expr) {
        Object found = AstWalk.findFirst(expr, node -> {
            if (node instanceof CaseExpr) {
                for (CaseExpr.WhenClause when : ((CaseExpr) node).whenClauses()) {
                    if (containsSrf(when.condition())) return true;
                }
                return false;
            }
            // IN is one of these only when its list holds a single value. PostgreSQL rewrites a
            // longer list into comparisons and expands a set written on either side of them, so
            // "gs IN (1,2)" and "1 IN (gs, 5)" each answer a row per element, and so does the
            // ANY spelling however long its array. A one-element list keeps the IN itself, and
            // that is what will not take a set — on either side, and NOT IN with it.
            if (node instanceof InExpr) {
                InExpr in = (InExpr) node;
                if (in.fromAny() || in.values() == null || in.values().size() != 1) return false;
                if (isSubqueryIn(in)) return containsSrf(in.expr());
                return containsSrf(in.expr()) || containsSrf(in.values().get(0));
            }
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
        // IN over a sub-query compares a row against the rows it answers with, and PostgreSQL
        // names the row comparison rather than the IN.
        if (found instanceof InExpr && isSubqueryIn((InExpr) found)) {
            throw new MemgresException(
                    "row comparison operator must not return a set", "42804");
        }
        // PostgreSQL points at where the construct starts, which is its leftmost operand.
        if (found instanceof InExpr) e.setPositionToken(leadingToken(((InExpr) found).expr()));
        throw e;
    }

    /** Whether an IN reads a sub-query rather than a written-out list of values. */
    private static boolean isSubqueryIn(InExpr in) {
        return in.values() != null && in.values().size() == 1
                && in.values().get(0) instanceof SubqueryExpr;
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

    private void rejectSrfsInJoinConditions(List<SelectStmt.FromItem> fromItems) {
        if (fromItems == null) return;
        for (SelectStmt.FromItem item : fromItems) {
            if (!(item instanceof SelectStmt.JoinFrom)) continue;
            SelectStmt.JoinFrom jf = (SelectStmt.JoinFrom) item;
            rejectSrfIn(jf.on(), "JOIN conditions");
            rejectSrfsInJoinConditions(Cols.listOf(jf.left(), jf.right()));
        }
    }

    /**
     * A join condition has to be a condition.
     *
     * <p>{@code ON a.id} joins on nothing at all: PostgreSQL coerces the qualification to boolean
     * while it builds the range table and refuses what will not coerce, so a text column there is
     * 42804 and a string that is not a boolean word is 22P02 — while memgres read whatever came out
     * as truthy and quietly answered the whole cross product.
     *
     * <p>Judged before the join runs, because the join running is the wrong answer, and judged
     * innermost-first left to right, which is the order PostgreSQL reaches the qualifications in.
     * Deliberately one-sided: only an expression whose type is certain here is refused, so a shape
     * this cannot type is executed exactly as before.
     */
    private void rejectNonBooleanJoinConditions(List<SelectStmt.FromItem> fromItems,
                                                QueryLevelScope scope) {
        if (fromItems == null) return;
        for (SelectStmt.FromItem item : fromItems) {
            if (!(item instanceof SelectStmt.JoinFrom)) continue;
            SelectStmt.JoinFrom jf = (SelectStmt.JoinFrom) item;
            rejectNonBooleanJoinConditions(Cols.listOf(jf.left(), jf.right()), scope);
            rejectNonBooleanCondition(jf.on(), "JOIN/ON", scope);
        }
    }

    /**
     * Refuses one expression standing where a condition is wanted, naming the construct that wanted
     * it. AND, OR and NOT each want one of their own and PostgreSQL names the operator rather than
     * the clause, so the walk descends through them carrying the name with it.
     */
    private void rejectNonBooleanCondition(Expression expr, String clause, QueryLevelScope scope) {
        if (expr == null) return;
        // An aggregate or a window call cannot stand in a join condition at all, and that refusal
        // is PostgreSQL's answer whatever type the condition would have had.
        if (containsAggregate(expr) || containsWindowFunction(expr)) return;
        if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            if (bin.op() == BinaryExpr.BinOp.AND || bin.op() == BinaryExpr.BinOp.OR) {
                rejectNonBooleanCondition(bin.left(), bin.op().name(), scope);
                rejectNonBooleanCondition(bin.right(), bin.op().name(), scope);
            }
            // Every other operator is a comparison or an arithmetic one whose result type this
            // does not settle, so it is left to evaluate as before.
            return;
        }
        if (expr instanceof UnaryExpr && ((UnaryExpr) expr).op() == UnaryExpr.UnaryOp.NOT) {
            rejectNonBooleanCondition(((UnaryExpr) expr).operand(), "NOT", scope);
            return;
        }
        // A string written without a type is `unknown`, and a condition is what resolves it: 'x' is
        // not a boolean word, so PostgreSQL reports the input rather than the type.
        if (expr instanceof Literal
                && ((Literal) expr).literalType() == Literal.LiteralType.STRING) {
            TypeCoercion.toBoolean(((Literal) expr).value());
            return;
        }
        DataType type = certainConditionType(expr, scope);
        if (type == null || type == DataType.BOOLEAN) return;
        throw PgErrors.datatypeMismatch("argument of " + clause
                + " must be type boolean, not type " + type.toRegtypeDisplay());
    }

    /**
     * The type an expression in a condition certainly has, or null when this cannot settle it.
     *
     * <p>Four things are certain: a numeric or boolean literal, a cast to a named built-in type, a
     * built-in call whose every signature returns one type, and a column of a relation this query
     * level has already resolved. Of those, only the types on the list below are certainly not a
     * boolean — a domain or an enum could be over anything, and refusing one wrongly would refuse
     * SQL PostgreSQL runs.
     */
    private DataType certainConditionType(Expression expr, QueryLevelScope scope) {
        DataType type = null;
        if (expr instanceof CastExpr) {
            String name = ((CastExpr) expr).typeName().replaceAll("\\(.*\\)", "").trim();
            type = DataType.fromPgName(name);
        } else if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr call = (FunctionCallExpr) expr;
            type = resolvedReturnType(call, scope);
            if (type == null) type = soleReturnType(call);
        } else if (scope != null) {
            type = scope.certainTypeOf(expr);
        }
        if (type == DataType.BOOLEAN) return type;
        return CERTAINLY_NOT_BOOLEAN.contains(type) ? type : null;
    }

    /**
     * The type a built-in call produces once it has been resolved by its argument types.
     *
     * <p>A name every signature of which returns one type can be answered from the name alone,
     * which is what {@link #soleReturnType} does; an overloaded one cannot. {@code upper} returns
     * text of a text argument and the element type of a range, so nothing is settled until the
     * argument is looked at — and that is why {@code JOIN ... ON upper('a')} used to reach
     * evaluation and complain about the value {@code 'A'} rather than about the type text.
     *
     * <p>Only an exact match counts, of the whole argument list, at one signature and no other.
     * An unadorned string literal is read as text because that is what PostgreSQL resolves an
     * unknown literal to when a text signature is there to take it.
     */
    private DataType resolvedReturnType(FunctionCallExpr call, QueryLevelScope scope) {
        if (call.filter() != null || call.distinct || call.star) return null;
        if (call.args() == null || call.args().isEmpty()) return null;
        String bare = FunctionEvaluator.stripSchemaPrefix(call.name().toLowerCase(Locale.ROOT));
        if (hasUserFunction(bare) || isAggregateFunction(bare)) return null;
        if (PlacementCheck.isWindowFunctionName(bare)) return null;
        int[] oids = new int[call.args().size()];
        for (int i = 0; i < oids.length; i++) {
            Expression arg = call.args().get(i);
            DataType argType = scope == null ? null : scope.certainTypeOf(arg);
            if (argType == null && arg instanceof Literal
                    && ((Literal) arg).literalType() == Literal.LiteralType.STRING) {
                argType = DataType.TEXT;
            }
            if (argType == null) return null;
            oids[i] = argType.getOid();
        }
        DataType found = null;
        for (String[] signature : BuiltinFunctionSignatures.SIGNATURES) {
            if (!signature[0].equalsIgnoreCase(bare)) continue;
            // A row memgres wrote for itself names the types it happened to write down, so an
            // argument list matching one settles nothing about which form was resolved to.
            if (!BuiltinFunctionSignatures.isPostgresSignature(signature)) return null;
            String[] params = signature[2].isEmpty() ? new String[0] : signature[2].split(" ");
            if (params.length != oids.length) continue;
            boolean same = true;
            for (int i = 0; i < params.length; i++) {
                if (!params[i].equals(String.valueOf(oids[i]))) {
                    same = false;
                    break;
                }
            }
            if (!same) continue;
            // A function that returns a set returns rows, not a value of a type.
            if (signature[3] == null || signature[3].isEmpty()) return null;
            if (signature[3].charAt(0) == 't') return null;
            DataType returned = DataType.fromOid(Integer.parseInt(signature[1]));
            if (returned == null) return null;
            if (found != null && found != returned) return null;
            found = returned;
        }
        return found;
    }

    /**
     * The type a built-in call certainly produces: the one every signature of that name returns.
     * A name whose overloads return different types, one a user has declared a function under, and
     * one carrying a clause that changes what it is are all left unsettled.
     */
    private DataType soleReturnType(FunctionCallExpr call) {
        if (call.filter() != null || call.distinct || call.star) return null;
        String bare = FunctionEvaluator.stripSchemaPrefix(call.name().toLowerCase(Locale.ROOT));
        if (hasUserFunction(bare) || isAggregateFunction(bare)) return null;
        if (PlacementCheck.isWindowFunctionName(bare)) return null;
        DataType found = null;
        for (String[] signature : BuiltinFunctionSignatures.SIGNATURES) {
            if (!signature[0].equalsIgnoreCase(bare)) continue;
            // A function that returns a set returns rows, not a value of a type.
            if (signature[3] == null || signature[3].isEmpty()) return null;
            if (signature[3].charAt(0) == 't') return null;
            DataType returned = DataType.fromOid(Integer.parseInt(signature[1]));
            if (returned == null) return null;
            if (found != null && found != returned) return null;
            found = returned;
        }
        return found;
    }

    /** The types a value certainly has when it has one of them, and none of them is boolean. */
    private static final Set<DataType> CERTAINLY_NOT_BOOLEAN =
            new java.util.HashSet<>(Arrays.asList(
                    DataType.SMALLINT, DataType.INTEGER, DataType.BIGINT, DataType.REAL,
                    DataType.DOUBLE_PRECISION, DataType.NUMERIC, DataType.MONEY,
                    DataType.VARCHAR, DataType.CHAR, DataType.TEXT, DataType.NAME,
                    DataType.DATE, DataType.TIMESTAMP, DataType.TIMESTAMPTZ, DataType.TIME,
                    DataType.TIMETZ, DataType.INTERVAL, DataType.BYTEA, DataType.UUID,
                    DataType.JSON, DataType.JSONB, DataType.XML));

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
    void validateFromClause(List<SelectStmt.FromItem> from) {
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
        List<SelectStmt.SelectTarget> targets = expandTargetsForOrdinals(stmt.targets(), bindings,
                executor.fromResolver.resolveClauseOutput(stmt.from(), bindings));
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
        String name = exposedNameOf(item);
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
    /**
     * What a qualifier the FROM clause does not answer to is.
     *
     * <p>A name nothing in the query has is missing. A name the FROM clause holds but has covered
     * over — the relations under {@code (a JOIN b) AS j}, which answer to {@code j} and to nothing
     * else — is there and out of reach, and PostgreSQL says which of the two it is. Reporting the
     * second as missing sent the reader looking for a relation they had written down.
     */
    private static MemgresException outOfScopeOrMissing(String qualifier,
                                                        List<SelectStmt.FromItem> from) {
        Set<String> covered = new LinkedHashSet<>();
        if (from != null) {
            for (SelectStmt.FromItem item : from) FromResolver.collectCoveredNames(item, covered);
        }
        if (!covered.contains(qualifier.toLowerCase())) {
            return new MemgresException(
                    "missing FROM-clause entry for table \"" + qualifier + "\"", "42P01");
        }
        MemgresException e = new MemgresException(
                "invalid reference to FROM-clause entry for table \"" + qualifier + "\"", "42P01");
        e.setDetail("There is an entry for table \"" + qualifier
                + "\", but it cannot be referenced from this part of the query.");
        return e;
    }

    static String exposedNameOf(SelectStmt.FromItem item) {
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

    /**
     * The first qualified reference under {@code node} naming one of {@code names}, or null.
     *
     * <p>A query level that binds one of the names itself takes it over for everything written
     * inside it — its own FROM entries and its own WITH items both — so the search drops that name
     * on the way down rather than reporting a reference that reaches something else entirely.
     * The name is dropped for that level and every level under it, and comes back for the levels
     * beside it.
     */
    static String firstReferenceTo(Object node, Set<String> names) {
        Set<String> visible = new HashSet<>();
        for (String name : names) visible.add(name.toLowerCase());
        return firstReferenceIn(node, visible);
    }

    /**
     * Every name a FROM item brings into its own query level.
     *
     * <p>A join has no name of its own and binds the names of everything under it, so the walk goes
     * down both sides. A table binds the name it is written under and, where an alias renamed it,
     * its own name as well: the alias hides it for resolution, but this list decides only whether a
     * reference reaches something else entirely, and a name that may have been meant for the entry
     * here is not one that reaches out.
     */
    private static void collectBoundNames(SelectStmt.FromItem item, Set<String> bound) {
        if (item == null) return;
        if (item instanceof SelectStmt.JoinFrom) {
            collectBoundNames(((SelectStmt.JoinFrom) item).left(), bound);
            collectBoundNames(((SelectStmt.JoinFrom) item).right(), bound);
            return;
        }
        String exposed = exposedNameOf(item);
        if (exposed != null) bound.add(exposed.toLowerCase());
        if (item instanceof SelectStmt.TableRef) {
            String written = ((SelectStmt.TableRef) item).table();
            if (written != null) bound.add(written.toLowerCase());
        }
    }

    private static String firstReferenceIn(Object node, Set<String> visible) {
        if (node == null || visible.isEmpty()) return null;
        Set<String> here = visible;
        if (node instanceof SelectStmt) {
            Set<String> bound = new HashSet<>();
            SelectStmt sel = (SelectStmt) node;
            if (sel.from() != null) {
                for (SelectStmt.FromItem f : sel.from()) collectBoundNames(f, bound);
            }
            if (sel.withClauses() != null) {
                for (SelectStmt.CommonTableExpr cte : sel.withClauses()) {
                    if (cte.name() != null) bound.add(cte.name().toLowerCase());
                }
            }
            if (!bound.isEmpty()) {
                here = new HashSet<>(visible);
                here.removeAll(bound);
                if (here.isEmpty()) return null;
            }
        }
        if (node instanceof ColumnRef) {
            ColumnRef ref = (ColumnRef) node;
            return ref.table() != null && here.contains(ref.table().toLowerCase())
                    ? ref.table() : null;
        }
        final Set<String> inScope = here;
        final String[] answer = new String[1];
        AstWalk.forEachChild(node, child -> {
            if (answer[0] == null) answer[0] = firstReferenceIn(child, inScope);
        });
        return answer[0];
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
            // ... and one that declares OUT parameters names its columns after all, so a call
            // written without a column list reaches the aggregate with every column it needs and
            // the set is what is left to complain about.
            if (f != null && f.isSetReturning()
                    && (!f.declaresRecordResult() || f.hasOutParams())) {
                return true;
            }
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
                    // A function the database holds a declaration for is a set-returning call too,
                    // and containsSrf only knows the built-in names.
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
    private MemgresException conditionalHidingSrf(Expression expr) {
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
    boolean containsSrf(Object node) {
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
    FunctionCallExpr findSrfCall(Expression expr) {
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
    List<FunctionCallExpr> collectSrfCalls(Object node) {
        List<FunctionCallExpr> found = new ArrayList<>();
        collectSrfCalls(node, found);
        return found;
    }

    private void collectSrfCalls(Object node, List<FunctionCallExpr> out) {
        if (node == null || node instanceof Statement) return;
        if (node instanceof FunctionCallExpr && isSetReturningCall((FunctionCallExpr) node)) {
            out.add((FunctionCallExpr) node);
            return;
        }
        AstWalk.forEachChild(node, child -> collectSrfCalls(child, out));
    }

    /**
     * True when a call answers a set: one of the built-in set-returning functions, or a function
     * this database was told about that was declared {@code RETURNS SETOF} or {@code RETURNS
     * TABLE}.
     *
     * <p>A declared one is the same kind of call as {@code generate_series} and PostgreSQL treats
     * it the same way everywhere -- it expands in a select list, and it is refused in the clauses
     * that read rows already produced. Recognising only the built-in names left
     * {@code SELECT setof_fn()} answering one row holding the whole set rendered as an array.
     *
     * <p>One returning bare {@code record} is left out: its columns have no names until a caller
     * supplies them, and a call written without them fails on the column before the set.
     */
    boolean isSetReturningCall(FunctionCallExpr call) {
        String name = FunctionEvaluator.stripSchemaPrefix(call.name().toLowerCase());
        if (SRF_FUNCTIONS.contains(name)) return true;
        if (executor.database == null) return false;
        PgFunction declared = executor.database.getFunction(name);
        return declared != null && declared.isSetReturning() && !declared.declaresRecordResult();
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
    /**
     * The rows DISTINCT ON groups, once a set-returning call written only in a DISTINCT ON key has
     * been expanded. PostgreSQL expands the sets of a query below the DISTINCT, so the key it
     * groups on is an ordinary value per row: {@code SELECT DISTINCT ON (generate_series(1,2)) a
     * FROM two_rows} has four rows to choose from and answers two. A key that is also a select
     * target is left alone, since the projection expands that one.
     */
    private List<RowContext> expandContextsForDistinctOnSrfs(SelectStmt stmt, List<RowContext> contexts) {
        List<Expression> keys = new ArrayList<>();
        for (Expression on : stmt.distinctOn()) {
            if (!containsSrf(on)) continue;
            if (resolveOrderByToColumnIndex(on, stmt.targets()) >= 0) continue;
            keys.add(on);
        }
        return keys.isEmpty() ? contexts : expandContextsForSrfs(keys, contexts);
    }

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

    void noteHiddenWithItem(MemgresException ex, String name) {
        cteExecutor.noteHiddenWithItem(ex, name);
    }

    boolean namesWithItem(String name) {
        return cteExecutor.namesWithItem(name);
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
     * Note the lock a read takes on each relation it touches, so pg_locks reports what is held.
     *
     * <p>A plain read takes an AccessShareLock; a read written FOR UPDATE or FOR SHARE takes a
     * RowShareLock, which is what tells a reader of pg_locks the two apart.
     */
    private void recordReadLocks(SelectStmt stmt, List<RowContext.TableBinding> bindings) {
        if (executor.session == null || bindings == null) return;
        String mode = stmt.lockClause() != null ? "RowShareLock" : "AccessShareLock";
        for (RowContext.TableBinding b : bindings) {
            Table t = b.sourceTable != null ? b.sourceTable : b.table();
            if (t == null || t.getName() == null) continue;
            String schemaName = executor.database.schemaNameOf(t);
            if (schemaName == null) continue;
            executor.session.recordRelationLock(
                    schemaName.toLowerCase() + "." + t.getName().toLowerCase(), mode);
        }
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
