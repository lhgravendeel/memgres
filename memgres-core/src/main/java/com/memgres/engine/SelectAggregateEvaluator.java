package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Handles aggregate function evaluation, GROUP BY, GROUPING SETS, and ordered-set aggregates.
 * Extracted from SelectExecutor to separate concerns.
 */
class SelectAggregateEvaluator {
    private final SelectExecutor select;
    private final AstExecutor executor;

    // What GROUPING() reads: the grouping expressions of the set being emitted right now.
    private final ThreadLocal<GroupingScope> groupingScope = new ThreadLocal<>();

    SelectAggregateEvaluator(SelectExecutor select) {
        this.select = select;
        this.executor = select.executor;
    }

    /**
     * The grouping set a row is being emitted for, in the form GROUPING() compares against.
     *
     * <p>Held per evaluation rather than per query because a scalar sub-select in the select
     * list may itself group; each pipeline saves the enclosing scope and puts it back, so the
     * outer GROUPING() still sees the outer query's set.
     */
    private static final class GroupingScope {
        final Set<String> forms;
        final List<RowContext.TableBinding> bindings;

        GroupingScope(Set<String> forms, List<RowContext.TableBinding> bindings) {
            this.forms = forms;
            this.bindings = bindings;
        }
    }

    /**
     * The comparable forms of the expressions a grouping set groups by. A composite element
     * {@code (a, b)} counts as its members, which is how GROUPING() and the select list read it.
     */
    private static Set<String> groupingForms(List<Expression> groupBy,
                                             List<RowContext.TableBinding> bindings) {
        Set<String> forms = new HashSet<>();
        if (groupBy == null) return forms;
        for (Expression e : groupBy) {
            forms.add(GroupByValidator.canon(e, bindings));
            if (e instanceof ArrayExpr && ((ArrayExpr) e).isRow()) {
                for (Expression el : ((ArrayExpr) e).elements()) {
                    forms.add(GroupByValidator.canon(el, bindings));
                }
            }
        }
        return forms;
    }

    /**
     * The window functions of a grouped query, computed over the rows that query produces.
     *
     * <p>A window in a grouped query runs over the grouped result: the rows it frames are the
     * groups, and everything its specification names — its arguments, PARTITION BY, ORDER BY and
     * FILTER — is a value of the grouped row. Those are evaluated here, once per group as the
     * group's row is emitted, and bound to their expression nodes; the window evaluator then
     * reads the bound values. Looking them up in the grouped result instead cannot work, because
     * its columns are the output aliases: {@code sum(v)} is not among them, and neither is a
     * grouping column the select list happens to omit.
     */
    private final class GroupedWindowPass {
        private final SelectStmt stmt;
        private final List<Expression> inputs = new ArrayList<>();
        private final List<Object[]> inputValues = new ArrayList<>();
        private final boolean windowed;
        /** One per emitted row, in emission order; null until {@link #apply} has run. */
        private List<RowContext> contexts;

        GroupedWindowPass(SelectStmt stmt, List<SelectStmt.OrderByItem> resolvedOrderBy) {
            this.stmt = stmt;
            boolean any = false;
            for (SelectStmt.SelectTarget target : stmt.targets()) any |= collect(target.expr());
            if (resolvedOrderBy != null) {
                for (SelectStmt.OrderByItem item : resolvedOrderBy) any |= collect(item.expr());
            }
            this.windowed = any;
        }

        private boolean collect(Expression expr) {
            if (!select.containsWindowFunction(expr)) return false;
            select.windowEvaluator.collectGroupedWindowInputs(expr, stmt.windowDefs(), inputs);
            return true;
        }

        /** Records what the windows will read for a row the query is about to emit. */
        void recordRow(List<RowContext> group, RowContext representative) {
            if (!windowed) return;
            Object[] values = new Object[inputs.size()];
            for (int i = 0; i < inputs.size(); i++) {
                values[i] = evalAggregateExpr(inputs.get(i), group, representative);
            }
            inputValues.add(values);
        }

        /** Replaces every window-function output column with the value the window computes. */
        void apply(List<Column> resultColumns, List<Object[]> resultRows) {
            if (!windowed || resultRows.isEmpty()) return;
            Table virtualTable = new Table("__agg_result__", resultColumns);
            contexts = new ArrayList<>(resultRows.size());
            for (int ri = 0; ri < resultRows.size(); ri++) {
                Object[] row = resultRows.get(ri);
                virtualTable.insertRow(row);
                RowContext ctx = new RowContext(Cols.listOf(
                        new RowContext.TableBinding(virtualTable, "__agg_result__", row)));
                Object[] values = inputValues.get(ri);
                for (int i = 0; i < inputs.size(); i++) ctx.setBoundValue(inputs.get(i), values[i]);
                contexts.add(ctx);
            }
            for (int ti = 0; ti < stmt.targets().size(); ti++) {
                Expression expr = stmt.targets().get(ti).expr();
                if (!select.containsWindowFunction(expr)) continue;
                Object[] windowVals = select.windowEvaluator.evaluateWindowExpression(
                        expr, contexts, stmt.windowDefs());
                for (int ri = 0; ri < resultRows.size(); ri++) resultRows.get(ri)[ti] = windowVals[ri];
            }
        }

        /**
         * The values an ORDER BY key holding a window function computes, indexed as the rows were
         * emitted, or null when the key is not one. A window ordered by rather than selected has
         * no output column to read and cannot be computed a row at a time either.
         */
        Object[] orderByValues(Expression expr) {
            if (contexts == null || !select.containsWindowFunction(expr)) return null;
            return select.windowEvaluator.evaluateWindowExpression(expr, contexts, stmt.windowDefs());
        }
    }

    // ---- Aggregate SELECT pipelines ----

    QueryResult executeGroupingSetsSelect(SelectStmt stmt, List<RowContext> contexts,
                                           List<RowContext.TableBinding> baseBindings) {
        List<List<Expression>> groupingSets = stmt.groupingSets();
        List<Expression> fixedGroupBy = new ArrayList<>();

        List<Column> resultColumns = new ArrayList<>();
        for (SelectStmt.SelectTarget target : stmt.targets()) {
            String alias = target.alias();
            if (alias == null) alias = executor.exprToAlias(target.expr());
            resultColumns.add(executor.buildResultColumn(alias, target.expr(), baseBindings));
        }

        List<Object[]> allResultRows = new ArrayList<>();
        List<SelectStmt.OrderByItem> resolvedOrderBy = select.resolveOrderBy(stmt.orderBy(), stmt.targets());
        GroupedWindowPass windows = new GroupedWindowPass(stmt, resolvedOrderBy);

        for (List<Expression> groupingSet : groupingSets) {
            List<Expression> effectiveGroupBy = new ArrayList<>(fixedGroupBy);
            effectiveGroupBy.addAll(resolveGroupByRefs(groupingSet, stmt, baseBindings));

            groupingScope.set(new GroupingScope(
                    groupingForms(effectiveGroupBy, baseBindings), baseBindings));

            // Column names that are functionally grouped in this grouping set. Select-list
            // expressions built over these columns evaluate against the group's key values;
            // any other column reads as NULL for this grouping set (PG semantics).
            Set<String> groupedColumnNames = new HashSet<>();
            for (Expression e : effectiveGroupBy) {
                collectColumnNames(e, groupedColumnNames);
            }
            GroupByValidator.addFunctionallyDeterminedColumns(
                    effectiveGroupBy, baseBindings, groupedColumnNames);

            List<List<RowContext>> groups;
            if (effectiveGroupBy.isEmpty()) {
                groups = new ArrayList<>();
                groups.add(new ArrayList<>(contexts));
            } else {
                DataType[] keyTypes = RowKey.keyTypes(executor, effectiveGroupBy, baseBindings);
                Map<String, List<RowContext>> groupMap = new LinkedHashMap<>();
                for (RowContext ctx : contexts) {
                    StringBuilder key = new StringBuilder();
                    for (int gi = 0; gi < effectiveGroupBy.size(); gi++) {
                        Object val = executor.evalExpr(effectiveGroupBy.get(gi), ctx);
                        key.append(RowKey.keyOf(val, keyTypes, gi)).append('\1');
                    }
                    groupMap.computeIfAbsent(key.toString(), k -> new ArrayList<>()).add(ctx);
                }
                groups = new ArrayList<>(groupMap.values());
            }

            if (groups.isEmpty() && effectiveGroupBy.isEmpty()) {
                groups.add(new ArrayList<>());
            }

            for (List<RowContext> group : groups) {
                RowContext representative = group.isEmpty() ? null : group.get(0);
                // Ungrouped columns read as NULL for this grouping set; grouped columns keep
                // their key values, so expressions over them (e.g. a+10 with ROLLUP(a))
                // evaluate normally in the sets that group them.
                RowContext maskedRep = maskUngroupedColumns(representative, groupedColumnNames);
                Object[] row = new Object[stmt.targets().size()];
                for (int i = 0; i < stmt.targets().size(); i++) {
                    SelectStmt.SelectTarget target = stmt.targets().get(i);
                    Expression expr = target.expr();
                    row[i] = evalAggregateExpr(expr, group, maskedRep);
                }

                if (stmt.having() != null) {
                    Object havingResult = evalAggregateExpr(stmt.having(), group, maskedRep);
                    if (!executor.isTruthy(havingResult)) continue;
                }
                allResultRows.add(row);
                windows.recordRow(group, maskedRep);
            }
        }

        // A window over grouping sets runs over every row every set produced, as one result.
        windows.apply(resultColumns, allResultRows);

        // ORDER BY
        if (resolvedOrderBy != null && !resolvedOrderBy.isEmpty()) {
            final List<SelectStmt.OrderByItem> ob = resolvedOrderBy;
            final Object[][] windowKeys = new Object[ob.size()][];
            final Map<Object[], Integer> emittedAt = new IdentityHashMap<>();
            for (int oi = 0; oi < ob.size(); oi++) {
                if (select.resolveOrderByToColumnIndex(ob.get(oi).expr(), stmt.targets()) < 0) {
                    windowKeys[oi] = windows.orderByValues(ob.get(oi).expr());
                }
            }
            for (int ri = 0; ri < allResultRows.size(); ri++) emittedAt.put(allResultRows.get(ri), ri);
            allResultRows.sort((a, b) -> {
                for (int oi = 0; oi < ob.size(); oi++) {
                    SelectStmt.OrderByItem item = ob.get(oi);
                    int colIdx = select.resolveOrderByToColumnIndex(item.expr(), stmt.targets());
                    Object va = colIdx >= 0 ? a[colIdx]
                            : windowKeys[oi] != null ? windowKeys[oi][emittedAt.get(a)] : null;
                    Object vb = colIdx >= 0 ? b[colIdx]
                            : windowKeys[oi] != null ? windowKeys[oi][emittedAt.get(b)] : null;
                    if (va == null && vb == null) continue;
                    if (va == null || vb == null) {
                        boolean nullsFirst = item.nullsFirst() != null ? item.nullsFirst() : item.descending();
                        if (va == null) return nullsFirst ? -1 : 1;
                        else return nullsFirst ? 1 : -1;
                    }
                    int cmp = executor.compareValues(va, vb);
                    if (item.descending()) cmp = -cmp;
                    if (cmp != 0) return cmp;
                }
                return 0;
            });
        }

        allResultRows = select.applyDistinct(stmt, allResultRows, resultColumns);
        allResultRows = select.applyOffsetAndLimit(stmt, allResultRows);

        return QueryResult.select(resultColumns, allResultRows);
    }

    /** Recursively collect the (lowercased) column names referenced anywhere in an expression. */
    private static void collectColumnNames(Expression expr, Set<String> out) {
        if (expr == null) return;
        if (expr instanceof ColumnRef) {
            out.add(((ColumnRef) expr).column().toLowerCase());
        } else if (expr instanceof BinaryExpr) {
            collectColumnNames(((BinaryExpr) expr).left(), out);
            collectColumnNames(((BinaryExpr) expr).right(), out);
        } else if (expr instanceof CustomOperatorExpr) {
            CustomOperatorExpr c = (CustomOperatorExpr) expr;
            collectColumnNames(c.left(), out);
            collectColumnNames(c.right(), out);
        } else if (expr instanceof UnaryExpr) {
            collectColumnNames(((UnaryExpr) expr).operand(), out);
        } else if (expr instanceof CastExpr) {
            collectColumnNames(((CastExpr) expr).expr(), out);
        } else if (expr instanceof FunctionCallExpr) {
            for (Expression arg : ((FunctionCallExpr) expr).args()) {
                collectColumnNames(arg, out);
            }
        } else if (expr instanceof ArrayExpr) {
            for (Expression el : ((ArrayExpr) expr).elements()) {
                collectColumnNames(el, out);
            }
        } else if (expr instanceof IsNullExpr) {
            collectColumnNames(((IsNullExpr) expr).expr(), out);
        } else if (expr instanceof IsJsonExpr) {
            collectColumnNames(((IsJsonExpr) expr).expr(), out);
        } else if (expr instanceof InExpr) {
            InExpr in = (InExpr) expr;
            collectColumnNames(in.expr(), out);
            if (in.values() != null) {
                for (Expression v : in.values()) collectColumnNames(v, out);
            }
        } else if (expr instanceof LikeExpr) {
            collectColumnNames(((LikeExpr) expr).left(), out);
            collectColumnNames(((LikeExpr) expr).pattern(), out);
        } else if (expr instanceof CaseExpr) {
            CaseExpr c = (CaseExpr) expr;
            collectColumnNames(c.operand(), out);
            for (CaseExpr.WhenClause when : c.whenClauses()) {
                collectColumnNames(when.condition(), out);
                collectColumnNames(when.result(), out);
            }
            collectColumnNames(c.elseExpr(), out);
        }
    }

    /**
     * Build a copy of the representative row context where every column whose name is not
     * functionally grouped in the current grouping set is replaced with NULL. This implements
     * the PG semantics that, within a grouping set, ungrouped columns read as NULL while
     * grouped columns keep their group-key values.
     */
    private RowContext maskUngroupedColumns(RowContext representative, Set<String> groupedColumnNames) {
        if (representative == null) return null;
        List<RowContext.TableBinding> masked = new ArrayList<>();
        for (RowContext.TableBinding b : representative.getBindings()) {
            Object[] row = b.row();
            Object[] copy = row;
            if (row != null) {
                copy = Arrays.copyOf(row, row.length);
                List<Column> cols = b.table().getColumns();
                for (int ci = 0; ci < copy.length && ci < cols.size(); ci++) {
                    if (!groupedColumnNames.contains(cols.get(ci).getName().toLowerCase())) {
                        copy[ci] = null;
                    }
                }
            }
            masked.add(new RowContext.TableBinding(b.table(), b.alias(), copy, b.sourceTable()));
        }
        RowContext ctx = new RowContext(masked);
        ctx.setUsingColumns(representative.getUsingColumns());
        ctx.setOuterJoinNullPadded(representative.isOuterJoinNullPadded());
        return ctx;
    }

    /**
     * Replace output-column ordinals and output aliases used as grouping expressions with the
     * select-list expression they name. Applies to every grouping element, including the ones
     * inside GROUPING SETS / ROLLUP / CUBE.
     */
    private List<Expression> resolveGroupByRefs(List<Expression> groupBy, SelectStmt stmt,
                                                List<RowContext.TableBinding> bindings) {
        if (groupBy == null) return null;
        List<Expression> resolved = new ArrayList<>(groupBy);
        for (int i = 0; i < resolved.size(); i++) {
            Expression expr = resolved.get(i);
            if (expr instanceof Literal && ((Literal) expr).literalType() == Literal.LiteralType.INTEGER) {
                Literal lit = (Literal) expr;
                int ordinal = Integer.parseInt(lit.value());
                if (ordinal >= 1 && ordinal <= stmt.targets().size()) {
                    resolved.set(i, stmt.targets().get(ordinal - 1).expr());
                }
            } else if (expr instanceof ColumnRef && ((ColumnRef) expr).table() == null) {
                ColumnRef colRef = (ColumnRef) expr;
                // A bare name in GROUP BY is a FROM column when one has that name; only a name
                // no relation exposes falls back to an output alias. Reading the alias first
                // groups by the wrong expression whenever an alias shadows a column -- for
                // SELECT b AS a ... GROUP BY a, PostgreSQL groups by a and memgres grouped by b.
                if (namesInputColumn(colRef, bindings)) continue;
                for (SelectStmt.SelectTarget target : stmt.targets()) {
                    if (target.alias() != null && target.alias().equalsIgnoreCase(colRef.column())) {
                        resolved.set(i, target.expr());
                        break;
                    }
                }
            }
        }
        return resolved;
    }

    private static boolean namesInputColumn(ColumnRef ref, List<RowContext.TableBinding> bindings) {
        if (bindings == null || ref.column() == null) return false;
        for (RowContext.TableBinding binding : bindings) {
            if (binding.table() != null && binding.table().getColumnIndex(ref.column()) >= 0) return true;
        }
        return false;
    }

    QueryResult executeAggregateSelect(SelectStmt stmt, List<RowContext> contexts,
                                        List<RowContext.TableBinding> baseBindings) {
        GroupingScope enclosing = groupingScope.get();
        try {
            if (stmt.groupingSets() != null && !stmt.groupingSets().isEmpty()) {
                return executeGroupingSetsSelect(stmt, contexts, baseBindings);
            }
            // Resolve GROUP BY ordinals and aliases
            List<Expression> resolvedGroupBy = resolveGroupByRefs(stmt.groupBy(), stmt, baseBindings);
            // A set-returning key expands the rows before they are grouped, so the grouping
            // itself only ever compares single values.
            contexts = select.expandContextsForSrfs(resolvedGroupBy, contexts);
            // A plain GROUP BY has one grouping set, the one it names, and GROUPING() answers
            // for it as readily as for a set of GROUPING SETS: every listed expression is
            // grouped, so the answer is 0 and anything else is an error.
            groupingScope.set(new GroupingScope(
                    groupingForms(resolvedGroupBy, baseBindings), baseBindings));
            return executePlainAggregateSelect(stmt, contexts, baseBindings, resolvedGroupBy);
        } finally {
            groupingScope.set(enclosing);
        }
    }

    private QueryResult executePlainAggregateSelect(SelectStmt stmt, List<RowContext> contexts,
                                                    List<RowContext.TableBinding> baseBindings,
                                                    List<Expression> resolvedGroupBy) {
        boolean hasGroupBy = resolvedGroupBy != null && !resolvedGroupBy.isEmpty();
        List<List<RowContext>> groups;

        if (hasGroupBy) {
            DataType[] keyTypes = RowKey.keyTypes(executor, resolvedGroupBy, baseBindings);
            Map<String, List<RowContext>> groupMap = new LinkedHashMap<>();
            for (RowContext ctx : contexts) {
                StringBuilder keyBuilder = new StringBuilder();
                for (int gi = 0; gi < resolvedGroupBy.size(); gi++) {
                    Object val = executor.evalExpr(resolvedGroupBy.get(gi), ctx);
                    keyBuilder.append(RowKey.keyOf(val, keyTypes, gi)).append('\1');
                }
                groupMap.computeIfAbsent(keyBuilder.toString(), k -> new ArrayList<>()).add(ctx);
            }
            groups = new ArrayList<>(groupMap.values());
        } else {
            groups = new ArrayList<>();
            groups.add(contexts);
        }

        List<Column> resultColumns = new ArrayList<>();
        for (SelectStmt.SelectTarget target : stmt.targets()) {
            String alias = target.alias();
            if (alias == null) alias = executor.exprToAlias(target.expr());
            resultColumns.add(executor.buildResultColumn(alias, target.expr(), baseBindings));
        }
        List<Object[]> resultRows = new ArrayList<>();
        // The group each surviving row came from, so ORDER BY over an expression that is not an
        // output column can still be evaluated. Tracked alongside the rows rather than by index
        // into groups: HAVING drops groups, and the indexes stop lining up as soon as it does.
        Map<Object[], List<RowContext>> rowGroups = new IdentityHashMap<>();

        List<SelectStmt.OrderByItem> resolvedOrderBy = select.resolveOrderBy(stmt.orderBy(), stmt.targets());
        GroupedWindowPass windows = new GroupedWindowPass(stmt, resolvedOrderBy);

        for (List<RowContext> group : groups) {
            RowContext representative = group.isEmpty() ? null : group.get(0);

            Object[] row = new Object[stmt.targets().size()];
            for (int i = 0; i < stmt.targets().size(); i++) {
                SelectStmt.SelectTarget target = stmt.targets().get(i);
                Expression expr = target.expr();
                row[i] = evalAggregateExpr(expr, group, representative);
            }

            if (stmt.having() != null) {
                Object havingResult = evalAggregateExpr(stmt.having(), group, representative);
                if (!executor.isTruthy(havingResult)) continue;
            }

            resultRows.add(row);
            rowGroups.put(row, group);
            windows.recordRow(group, representative);
        }

        if (groups.isEmpty() && !hasGroupBy) {
            List<RowContext> empty = Cols.listOf();
            Object[] row = new Object[stmt.targets().size()];
            for (int i = 0; i < stmt.targets().size(); i++) {
                Expression expr = stmt.targets().get(i).expr();
                row[i] = evalAggregateExpr(expr, empty, null);
            }
            resultRows.add(row);
            rowGroups.put(row, empty);
            windows.recordRow(empty, null);
        }

        windows.apply(resultColumns, resultRows);

        // ORDER BY on aggregate results
        // Pre-compute ORDER BY values for aggregate expressions not in target columns
        Map<Object[], Object[]> orderByValues = null;
        if (resolvedOrderBy != null && !resolvedOrderBy.isEmpty()) {
            // An ORDER BY key that is not an output column has to be computed over the grouped
            // result, whether it is an aggregate (ORDER BY count(*)), a grouped expression the
            // select list happens not to carry (GROUP BY a+0 ORDER BY a+0) or a window function
            // (ORDER BY rank() OVER (ORDER BY sum(v))). Sorting by the first output column
            // instead, as this did for the latter two, silently returns the wrong order.
            Object[][] windowKeys = new Object[resolvedOrderBy.size()][];
            boolean needsGroupEval = false;
            for (int oi = 0; oi < resolvedOrderBy.size(); oi++) {
                SelectStmt.OrderByItem item = resolvedOrderBy.get(oi);
                if (select.resolveOrderByToColumnIndex(item.expr(), stmt.targets()) >= 0) continue;
                needsGroupEval = true;
                windowKeys[oi] = windows.orderByValues(item.expr());
            }
            if (needsGroupEval && !resultRows.isEmpty()) {
                orderByValues = new IdentityHashMap<>();
                for (int ri = 0; ri < resultRows.size(); ri++) {
                    Object[] resultRow = resultRows.get(ri);
                    List<RowContext> group = rowGroups.get(resultRow);
                    RowContext rep = group == null || group.isEmpty() ? null : group.get(0);
                    Object[] obVals = new Object[resolvedOrderBy.size()];
                    for (int oi = 0; oi < resolvedOrderBy.size(); oi++) {
                        SelectStmt.OrderByItem item = resolvedOrderBy.get(oi);
                        int colIdx = select.resolveOrderByToColumnIndex(item.expr(), stmt.targets());
                        if (colIdx >= 0) {
                            obVals[oi] = resultRow[colIdx];
                        } else if (windowKeys[oi] != null) {
                            obVals[oi] = windowKeys[oi][ri];
                        } else if (group != null) {
                            obVals[oi] = evalAggregateExpr(item.expr(), group, rep);
                        }
                    }
                    orderByValues.put(resultRow, obVals);
                }
            }
            final Map<Object[], Object[]> finalOrderByValues = orderByValues;
            resultRows.sort((a, b) -> {
                for (int oi = 0; oi < resolvedOrderBy.size(); oi++) {
                    SelectStmt.OrderByItem item = resolvedOrderBy.get(oi);
                    Object va, vb;
                    if (finalOrderByValues != null) {
                        Object[] aVals = finalOrderByValues.get(a);
                        Object[] bVals = finalOrderByValues.get(b);
                        va = aVals != null ? aVals[oi] : a[0];
                        vb = bVals != null ? bVals[oi] : b[0];
                    } else {
                        int colIdx = select.resolveOrderByToColumnIndex(item.expr(), stmt.targets());
                        if (colIdx >= 0) {
                            va = a[colIdx];
                            vb = b[colIdx];
                        } else {
                            va = a[0]; vb = b[0];
                        }
                    }

                    if (va == null && vb == null) continue;
                    if (va == null || vb == null) {
                        boolean nullsFirst = item.nullsFirst() != null ? item.nullsFirst() : item.descending();
                        if (va == null) return nullsFirst ? -1 : 1;
                        else return nullsFirst ? 1 : -1;
                    }

                    int cmp = executor.compareValues(va, vb);
                    if (item.descending()) cmp = -cmp;
                    if (cmp != 0) return cmp;
                }
                return 0;
            });
        }

        resultRows = applyDistinctOn(stmt, resultRows, rowGroups);
        resultRows = select.applyDistinct(stmt, resultRows, resultColumns);
        resultRows = select.applyOffsetAndLimit(stmt, resultRows);

        return QueryResult.select(resultColumns, resultRows);
    }

    /**
     * DISTINCT ON over a grouped query keys on the groups, not the input rows: the key may be an
     * aggregate, and the row it keeps is the first in the query's own order. Applied here rather
     * than on the input rows, which no longer exist one-to-one once the query has grouped.
     */
    private List<Object[]> applyDistinctOn(SelectStmt stmt, List<Object[]> rows,
                                           Map<Object[], List<RowContext>> rowGroups) {
        if (stmt.distinctOn() == null || stmt.distinctOn().isEmpty()) return rows;
        Set<String> seen = new LinkedHashSet<>();
        List<Object[]> kept = new ArrayList<>();
        DataType[] keyTypes = null;
        for (Object[] row : rows) {
            List<RowContext> group = rowGroups.get(row);
            if (group == null) group = Cols.listOf();
            RowContext representative = group.isEmpty() ? null : group.get(0);
            if (representative != null) {
                keyTypes = RowKey.keyTypes(executor, stmt.distinctOn(),
                        representative.getBindings());
            }
            StringBuilder key = new StringBuilder();
            for (int oi = 0; oi < stmt.distinctOn().size(); oi++) {
                Object value = evalAggregateExpr(stmt.distinctOn().get(oi), group, representative);
                key.append(RowKey.keyOf(value, keyTypes, oi)).append('\1');
            }
            if (seen.add(key.toString())) kept.add(row);
        }
        return kept;
    }

    // ---- Aggregate expression evaluation ----

    Object evalAggregateExpr(Expression expr, List<RowContext> group, RowContext representative) {
        // A sub-select written here may hold an aggregate that is not its own: PostgreSQL puts an
        // aggregate at the query level its argument variables come from, so one naming only this
        // query's columns is answered here, over this group, and the sub-select reads the answer.
        if (representative != null) {
            List<Expression> ofThisLevel =
                    select.outerLevelAggregatesIn(expr, representative.getBindings());
            if (!ofThisLevel.isEmpty()) {
                return evalWithLevelAggregates(expr, ofThisLevel, group, representative);
            }
        }
        return evalHere(expr, group, representative);
    }

    /**
     * Answer the calls this level owns, then evaluate what encloses them with those answers in
     * hand. They stay answered for as long as that takes, nested queries and all: the sub-select
     * they are written in reads them as values, so it neither groups around them nor refuses them
     * for standing in a clause that may hold no aggregate.
     */
    private Object evalWithLevelAggregates(Expression expr, List<Expression> owned,
                                           List<RowContext> group, RowContext representative) {
        List<RowContext.TableBinding> scope = representative.getBindings();
        Map<Expression, ExprEvaluator.PrecomputedValueExpr> answers =
                new IdentityHashMap<Expression, ExprEvaluator.PrecomputedValueExpr>();
        // A level further out may have answers of its own still standing; they are answers about
        // other nodes, so both sets hold at once.
        if (executor.exprEvaluator.levelFoldedAnswers() != null) {
            answers.putAll(executor.exprEvaluator.levelFoldedAnswers());
        }
        for (Expression call : owned) {
            Object value = call instanceof OrderedSetAggExpr
                    ? evalOrderedSetAggregate((OrderedSetAggExpr) call, group)
                    : evalAggregate((FunctionCallExpr) call, group);
            answers.put(call, new ExprEvaluator.PrecomputedValueExpr(value,
                    executor.exprEvaluator.inferTypeFromContext(call, scope)));
        }
        Map<Expression, ExprEvaluator.PrecomputedValueExpr> prior =
                executor.exprEvaluator.swapLevelFolded(answers);
        try {
            return evalHere(expr, group, representative);
        } finally {
            executor.exprEvaluator.swapLevelFolded(prior);
        }
    }

    private Object evalHere(Expression expr, List<RowContext> group, RowContext representative) {
        // A call an enclosing query level has already answered is that answer, not an aggregate
        // for this group: its arguments name the columns of a relation this query does not read.
        ExprEvaluator.PrecomputedValueExpr answered =
                executor.exprEvaluator.levelFoldedFor(expr);
        if (answered != null) return answered.value();
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            String name = FunctionEvaluator.stripSchemaPrefix(fn.name().toLowerCase());
            if (select.isAggregateFunction(name)) {
                return evalAggregate(fn, group);
            }
            boolean hasAggArg = fn.args().stream().anyMatch(select::containsAggregate);
            if (hasAggArg) {
                List<Expression> resolvedArgs = new ArrayList<>();
                for (Expression arg : fn.args()) {
                    Object val = evalAggregateExpr(arg, group, representative);
                    if (val == null) {
                        // An aggregate over no rows still has the type it was declared with, and
                        // the enclosing call is resolved from the types before any value is
                        // looked at: COALESCE(json_agg(v), '[]'::jsonb) is an error whether or
                        // not the json_agg found anything.
                        DataType declared = executor.exprEvaluator.inferTypeFromContext(arg,
                                representative != null ? representative.getBindings()
                                        : new ArrayList<RowContext.TableBinding>());
                        resolvedArgs.add(declared == null ? Literal.ofNull()
                                : new ExprEvaluator.PrecomputedValueExpr(null, declared));
                    } else {
                        // Preserve the resolved aggregate's runtime type (a typed value, not a
                        // re-parsed string literal) so a scalar function/expression wrapped
                        // around aggregate args -- e.g. LEAST(sum(a), sum(b)) -- compares its
                        // arguments numerically instead of lexicographically. Literal.ofString
                        // here previously stringified every value (mtask-8 Group 5): LEAST('7.0',
                        // '10.0') compares as strings ("10.0" < "7.0" because '1' < '7'),
                        // silently returning the wrong (larger) value.
                        resolvedArgs.add(new ExprEvaluator.PrecomputedValueExpr(val,
                                executor.exprEvaluator.inferTypeFromContext(arg,
                                        representative != null ? representative.getBindings()
                                                : new ArrayList<RowContext.TableBinding>())));
                    }
                }
                return executor.functionEvaluator.evalFunction(new FunctionCallExpr(fn.name(), resolvedArgs, fn.distinct(), fn.star()), representative);
            }
            // A set-returning key was expanded into one row per element before the grouping ran,
            // so this call already has its element; computing it again would answer the whole set.
            if (representative != null && representative.hasBoundValue(fn)) {
                return representative.getBoundValue(fn);
            }
            return executor.functionEvaluator.evalFunction(fn, representative);
        } else if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            // Splitting an operator into "evaluate each side, then combine the values" only
            // exists so that an aggregate nested inside one side is folded first. Without one
            // it loses whatever the operator needs beyond its two values -- an array subscript
            // over a grouped array column came back NULL -- so evaluate the whole thing at once.
            if (!select.containsAggregate(bin)) {
                return representative != null ? executor.evalExpr(bin, representative) : null;
            }
            if (bin.op() == BinaryExpr.BinOp.AND) {
                Object left = evalAggregateExpr(bin.left(), group, representative);
                if (Boolean.FALSE.equals(left)) return false;
                Object right = evalAggregateExpr(bin.right(), group, representative);
                if (Boolean.FALSE.equals(right)) return false;
                if (left == null || right == null) return null;
                return executor.isTruthy(left) && executor.isTruthy(right);
            }
            if (bin.op() == BinaryExpr.BinOp.OR) {
                Object left = evalAggregateExpr(bin.left(), group, representative);
                if (executor.isTruthyStrict(left)) return true;
                Object right = evalAggregateExpr(bin.right(), group, representative);
                if (executor.isTruthyStrict(right)) return true;
                if (left == null || right == null) return null;
                return false;
            }
            Object left = evalAggregateExpr(bin.left(), group, representative);
            Object right = evalAggregateExpr(bin.right(), group, representative);
            return executor.evalBinaryValues(bin.op(), left, right);
        } else if (expr instanceof CustomOperatorExpr) {
            CustomOperatorExpr cop = (CustomOperatorExpr) expr;
            // Evaluate children in aggregate context, then invoke operator with resolved values
            Object leftVal = cop.left() != null ? evalAggregateExpr(cop.left(), group, representative) : null;
            Object rightVal = evalAggregateExpr(cop.right(), group, representative);
            // Build a CustomOperatorExpr with literal-wrapped resolved values
            Expression leftExpr = cop.left() != null ? new Literal(
                    leftVal instanceof Number ? Literal.LiteralType.INTEGER : Literal.LiteralType.STRING,
                    leftVal == null ? "NULL" : String.valueOf(leftVal)) : null;
            Expression rightExpr = new Literal(
                    rightVal instanceof Number ? Literal.LiteralType.INTEGER : Literal.LiteralType.STRING,
                    rightVal == null ? "NULL" : String.valueOf(rightVal));
            CustomOperatorExpr resolved = new CustomOperatorExpr(cop.schema(), cop.opSymbol(), leftExpr, rightExpr);
            return representative != null ? executor.evalExpr(resolved, representative) : null;
        } else if (expr instanceof UnaryExpr) {
            UnaryExpr un = (UnaryExpr) expr;
            Object val = evalAggregateExpr(un.operand(), group, representative);
            return executor.evalUnaryValue(un.op(), val);
        } else if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            Object val = evalAggregateExpr(cast.expr(), group, representative);
            executor.checkNumericSpecialToInteger(cast, val);
            return executor.castEvaluator.applyCast(val, cast.typeName());
        } else if (expr instanceof IsJsonExpr) {
            IsJsonExpr ij = (IsJsonExpr) expr;
            Object inner = evalAggregateExpr(ij.expr(), group, representative);
            // Evaluate IS JSON on the aggregated result
            if (inner == null) return null;
            String s = inner.toString().trim();
            boolean valid = ExprEvaluator.isValidJson(s);
            if (valid && ij.jsonType() != null) {
                switch (ij.jsonType()) {
                    case OBJECT: valid = s.startsWith("{"); break;
                    case ARRAY: valid = s.startsWith("["); break;
                    case SCALAR: valid = !s.startsWith("{") && !s.startsWith("["); break;
                    case VALUE: break;
                    case BOOLEAN: valid = s.equals("true") || s.equals("false"); break;
                    case NULL: valid = s.equals("null"); break;
                    case STRING: valid = s.startsWith("\"") && s.endsWith("\""); break;
                    case NUMBER: valid = !s.startsWith("{") && !s.startsWith("[")
                            && !s.startsWith("\"") && !s.equals("true") && !s.equals("false")
                            && !s.equals("null"); break;
                }
            }
            return ij.negated() ? !valid : valid;
        } else if (expr instanceof IsNullExpr) {
            IsNullExpr isn = (IsNullExpr) expr;
            Object val = evalAggregateExpr(isn.expr(), group, representative);
            boolean isNull = (val == null);
            return isn.negated() ? !isNull : isNull;
        } else if (expr instanceof OrderedSetAggExpr) {
            OrderedSetAggExpr osa = (OrderedSetAggExpr) expr;
            return evalOrderedSetAggregate(osa, group);
        } else if (expr instanceof LikeExpr) {
            LikeExpr like = (LikeExpr) expr;
            Object left = evalAggregateExpr(like.left(), group, representative);
            Object pattern = evalAggregateExpr(like.pattern(), group, representative);
            if (left == null || pattern == null) return null;
            // Rebuild with resolved values and delegate to ExprEvaluator
            LikeExpr resolved = new LikeExpr(
                    Literal.ofString(left.toString()),
                    Literal.ofString(pattern.toString()),
                    like.escape(), like.caseInsensitive(), like.negated());
            return executor.evalExpr(resolved, representative);
        } else if (expr instanceof CaseExpr && select.containsAggregate(expr)) {
            // Evaluate CASE in aggregate context so aggregates and grouping() inside
            // conditions/results are computed over the group, e.g. the disambiguation
            // idiom CASE WHEN grouping(a, b) = 3 THEN 'total' ... END.
            CaseExpr c = (CaseExpr) expr;
            if (c.operand() != null) {
                Object opVal = evalAggregateExpr(c.operand(), group, representative);
                for (CaseExpr.WhenClause when : c.whenClauses()) {
                    Object condVal = evalAggregateExpr(when.condition(), group, representative);
                    if (opVal != null && condVal != null && TypeCoercion.areEqual(opVal, condVal)) {
                        return evalAggregateExpr(when.result(), group, representative);
                    }
                }
            } else {
                for (CaseExpr.WhenClause when : c.whenClauses()) {
                    Object condVal = evalAggregateExpr(when.condition(), group, representative);
                    if (executor.isTruthy(condVal)) {
                        return evalAggregateExpr(when.result(), group, representative);
                    }
                }
            }
            return c.elseExpr() != null ? evalAggregateExpr(c.elseExpr(), group, representative) : null;
        } else if (expr instanceof AnyAllExpr && select.containsAggregate(((AnyAllExpr) expr).left())) {
            // ANY/ALL over a subquery reads the whole result, so only the left side -- the one
            // that can hold the aggregate -- is folded over the group first.
            AnyAllExpr aa = (AnyAllExpr) expr;
            Object leftVal = evalAggregateExpr(aa.left(), group, representative);
            if (leftVal == null) return null;
            return executor.evalExpr(new AnyAllExpr(
                    new ExprEvaluator.PrecomputedValueExpr(leftVal), aa.op(), aa.subquery(),
                    aa.isAll()), representative);
        } else if (expr instanceof WindowFuncExpr) {
            return null;
        } else if (expr instanceof Literal) {
            // Literals don't depend on row context, evaluate them directly
            return executor.evalExpr(expr, representative);
        } else if (expr instanceof SubqueryExpr) {
            // Subqueries can be evaluated without a representative row
            return executor.evalExpr(expr, representative);
        } else if (select.containsAggregate(expr)) {
            return evalFoldedOverGroup(expr, group, representative);
        } else {
            return representative != null ? executor.evalExpr(expr, representative) : null;
        }
    }

    /**
     * Compute the aggregate calls an expression holds over the group, then evaluate what encloses
     * them as the ordinary expression it is.
     *
     * <p>The folding above names the containers it handles one at a time, and answers only for
     * the containers named. An aggregate written under BETWEEN, inside ARRAY[] or ROW(), in an IN
     * list, under COLLATE or AT TIME ZONE, or subscripted, reached ordinary evaluation instead,
     * where an aggregate call has no value: the row answered NULL, and a HAVING written that way
     * kept no rows at all.
     */
    private Object evalFoldedOverGroup(Expression expr, List<RowContext> group,
                                       RowContext representative) {
        List<RowContext.TableBinding> scope = representative != null
                ? representative.getBindings() : new ArrayList<RowContext.TableBinding>();
        Map<Expression, ExprEvaluator.PrecomputedValueExpr> folded =
                new IdentityHashMap<Expression, ExprEvaluator.PrecomputedValueExpr>();
        for (Expression call : ExprSearch.aggregateCallsIn(expr, select::isAggregateFunction)) {
            Object value = call instanceof OrderedSetAggExpr
                    ? evalOrderedSetAggregate((OrderedSetAggExpr) call, group)
                    : evalAggregate((FunctionCallExpr) call, group);
            // The folded value keeps the type the call was declared to have: a group that
            // aggregated to NULL still aggregated something of a type.
            folded.put(call, new ExprEvaluator.PrecomputedValueExpr(value,
                    executor.exprEvaluator.inferTypeFromContext(call, scope)));
        }
        return executor.exprEvaluator.evalWithFolded(folded, expr, representative);
    }

    // ---- Ordered-set aggregates ----

    private Object evalOrderedSetAggregate(OrderedSetAggExpr osa, List<RowContext> group) {
        String name = osa.funcName().toLowerCase();
        List<SelectStmt.OrderByItem> orderBy = osa.withinGroupOrderBy();
        checkOrderedSetArity(osa, group);
        // The direct arguments are typed from the query, not from whichever rows a FILTER left,
        // so the row the types are read off is taken before the predicate is applied.
        RowContext typeSample = group.isEmpty() ? null : group.get(0);
        // A FILTER chooses which rows the aggregate accumulates, and an ordered-set aggregate
        // accumulates rows like any other.
        if (osa.filter() != null) {
            group = group.stream()
                    .filter(ctx -> executor.isTruthy(executor.evalExpr(osa.filter(), ctx)))
                    .collect(Collectors.toList());
        }
        if (HYPOTHETICAL_SET_AGGREGATES.contains(name)) {
            return evalHypotheticalSetAggregate(name, osa, group, typeSample);
        }

        List<RowContext> sorted = new ArrayList<>(group);
        if (orderBy != null && !orderBy.isEmpty()) {
            sorted.sort((a, b) -> {
                for (SelectStmt.OrderByItem item : orderBy) {
                    Object va = executor.evalExpr(item.expr(), a);
                    Object vb = executor.evalExpr(item.expr(), b);
                    int cmp = executor.compareValues(va, vb);
                    if (item.descending()) cmp = -cmp;
                    if (cmp != 0) return cmp;
                }
                return 0;
            });
        }

        Expression orderExpr = (orderBy != null && !orderBy.isEmpty()) ? orderBy.get(0).expr() : null;
        List<Object> vals = new ArrayList<>();
        for (RowContext ctx : sorted) {
            Object v = orderExpr != null ? executor.evalExpr(orderExpr, ctx) : null;
            if (v != null) vals.add(v);
        }

        switch (name) {
            case "percentile_disc": {
                if (osa.args().isEmpty()) return null;
                // The fraction is declared double precision, so a literal written without a type
                // of its own is read by that type's input function rather than by whatever could
                // be made of it: percentile_cont('zz') is not a fraction out of range, it is not
                // a double precision at all.
                Object fractionObj = readDirectArg(osa.args().get(0), DataType.DOUBLE_PRECISION,
                        typeSample);
                if (fractionObj == null) return null;
                // Handle array argument: compute percentile for each element
                if (fractionObj instanceof java.util.List) {
                    java.util.List<?> fractions = (java.util.List<?>) fractionObj;
                    java.util.List<Object> results = new java.util.ArrayList<>();
                    for (Object f : fractions) {
                        if (f == null) { results.add(null); continue; }
                        double fv = percentileFraction(f);
                        if (vals.isEmpty()) { results.add(null); continue; }
                        int idx = (int) Math.ceil(fv * vals.size()) - 1;
                        if (idx < 0) idx = 0;
                        if (idx >= vals.size()) idx = vals.size() - 1;
                        results.add(vals.get(idx));
                    }
                    return results;
                }
                double fraction = percentileFraction(fractionObj);
                if (vals.isEmpty()) return null;
                int idx = (int) Math.ceil(fraction * vals.size()) - 1;
                if (idx < 0) idx = 0;
                if (idx >= vals.size()) idx = vals.size() - 1;
                return vals.get(idx);
            }
            case "percentile_cont": {
                if (osa.args().isEmpty()) return null;
                // The fraction is declared double precision, so a literal written without a type
                // of its own is read by that type's input function rather than by whatever could
                // be made of it: percentile_cont('zz') is not a fraction out of range, it is not
                // a double precision at all.
                Object fractionObj = readDirectArg(osa.args().get(0), DataType.DOUBLE_PRECISION,
                        typeSample);
                if (fractionObj == null) return null;
                // Handle array argument: compute percentile for each element
                if (fractionObj instanceof java.util.List) {
                    java.util.List<?> fractions = (java.util.List<?>) fractionObj;
                    java.util.List<Object> results = new java.util.ArrayList<>();
                    for (Object f : fractions) {
                        if (f == null) { results.add(null); continue; }
                        double fv = percentileFraction(f);
                        if (vals.isEmpty()) { results.add(null); continue; }
                        if (vals.size() == 1) { results.add(vals.get(0)); continue; }
                        double pos = fv * (vals.size() - 1);
                        int lo = (int) Math.floor(pos);
                        int hi = (int) Math.ceil(pos);
                        if (lo == hi) { results.add(vals.get(lo)); continue; }
                        results.add(interpolate(vals.get(lo), vals.get(hi), pos - lo));
                    }
                    return results;
                }
                double fraction = percentileFraction(fractionObj);
                if (vals.isEmpty()) return null;
                if (vals.size() == 1) return vals.get(0);
                double pos = fraction * (vals.size() - 1);
                int lower = (int) Math.floor(pos);
                int upper = (int) Math.ceil(pos);
                if (lower == upper) return vals.get(lower);
                return interpolate(vals.get(lower), vals.get(upper), pos - lower);
            }
            case "mode": {
                if (vals.isEmpty()) return null;
                // The most frequent value, counted by equality rather than by how each value
                // happens to be written: numeric 1.0, 1.00 and 1.000 are one value three times
                // over, and counting the spellings made the runner-up win.
                List<Object> distinct = new ArrayList<>();
                List<Long> counts = new ArrayList<>();
                for (Object v : vals) {
                    int at = -1;
                    for (int i = 0; i < distinct.size(); i++) {
                        if (TypeCoercion.areEqual(distinct.get(i), v)) { at = i; break; }
                    }
                    if (at < 0) {
                        distinct.add(v);
                        counts.add(1L);
                    } else {
                        counts.set(at, counts.get(at) + 1);
                    }
                }
                int best = 0;
                for (int i = 1; i < counts.size(); i++) {
                    if (counts.get(i) > counts.get(best)) best = i;
                }
                return distinct.get(best);
            }
            default: {
                throw notAnOrderedSetAggregate(osa, group);
            }
        }
    }

    /** The ordered-set aggregates that rank a hypothetical row against the group. */
    private static final Set<String> HYPOTHETICAL_SET_AGGREGATES = new LinkedHashSet<>(
            Arrays.asList("rank", "dense_rank", "percent_rank", "cume_dist"));

    /** The aggregates PostgreSQL declares over no arguments at all, which are written {@code f(*)}. */
    private static final Set<String> PARAMETERLESS_AGGREGATES =
            new HashSet<>(Arrays.asList("count"));

    /** Whether PostgreSQL declares this aggregate over no arguments at all. */
    static boolean isParameterlessAggregate(String name) {
        return name != null
                && PARAMETERLESS_AGGREGATES.contains(name.toLowerCase(java.util.Locale.ROOT));
    }

    /**
     * Every ordered-set aggregate has a fixed arity, and for the hypothetical-set four the direct
     * arguments have to match the WITHIN GROUP sort columns one for one — they are the row being
     * ranked. PG resolves the call against both lists at once and, finding no such function, says
     * so; accepting a mismatched call instead ranks against whatever prefix happened to line up.
     */
    private void checkOrderedSetArity(OrderedSetAggExpr osa, List<RowContext> group) {
        String name = osa.funcName().toLowerCase();
        int args = osa.args().size();
        int keys = osa.withinGroupOrderBy() == null ? 0 : osa.withinGroupOrderBy().size();
        boolean ok;
        if (HYPOTHETICAL_SET_AGGREGATES.contains(name)) {
            ok = keys >= 1 && args == keys;
        } else if (name.equals("percentile_cont") || name.equals("percentile_disc")) {
            ok = args == 1 && keys == 1;
        } else if (name.equals("mode")) {
            ok = args == 0 && keys == 1;
        } else {
            return;
        }
        if (!ok) throw noSuchOrderedSetFunction(osa, group);
        checkOrderedSetArgTypes(osa, group, name);
    }

    /**
     * The types the two percentiles are catalogued over, checked before a value is read.
     *
     * <p>percentile_cont interpolates between the two values the fraction falls between, so it is
     * declared only over the types that can be halved: double precision and interval. percentile_disc
     * returns one of the values it was given whole and takes any type at all for it. Both read the
     * fraction as a double precision. memgres counted the arguments and then evaluated whatever was
     * written, so percentile_cont over a date came back as the date itself and a fraction that was no
     * number at all was reported as a fraction out of range.
     *
     * <p>A literal written with no type of its own says nothing to resolve against and takes the
     * declared one, so it is left alone here and read by that type's input function later.
     */
    private void checkOrderedSetArgTypes(OrderedSetAggExpr osa, List<RowContext> group, String name) {
        if (!name.equals("percentile_cont") && !name.equals("percentile_disc")) return;
        RowContext sample = group.isEmpty() ? null : group.get(0);
        Expression fraction = osa.args().get(0);
        if (!isUntypedLiteral(fraction)) {
            DataType written = executor.exprEvaluator.inferTypeFromContext(
                    fraction, typeBindings(sample));
            // The fraction may be written one at a time or as an array of them, and the array form
            // asks the same of its elements as the scalar form asks of the whole.
            DataType element = DataType.elementOf(written);
            if (!readsAsDoublePrecision(element != null ? element : written)) {
                throw noSuchOrderedSetFunction(osa, group);
            }
        }
        if (!name.equals("percentile_cont")) return;
        Expression key = osa.withinGroupOrderBy().get(0).expr();
        if (isUntypedLiteral(key)) return;
        DataType keyType = typeOfKey(key, sample);
        // A time of day reaches the interval signature as the length from midnight it stands for.
        if (keyType != null && keyType != DataType.INTERVAL && keyType != DataType.TIME
                && !readsAsDoublePrecision(keyType)) {
            throw noSuchOrderedSetFunction(osa, group);
        }
    }

    /**
     * The value {@code weight} of the way from one sort key to the next.
     *
     * <p>percentile_cont is declared over double precision and over interval, and an interval is
     * not a number of anything: its months, days and microseconds are kept apart because a month
     * is not a fixed number of days. Reading one as a double made every one of them zero, so a
     * median of one hour and three hours came back as 0 rather than two hours.
     */
    private Object interpolate(Object low, Object high, double weight) {
        if (low instanceof java.time.LocalTime && high instanceof java.time.LocalTime) {
            return interpolate(TypeCoercion.toInterval(low), TypeCoercion.toInterval(high), weight);
        }
        if (low instanceof PgInterval && high instanceof PgInterval) {
            PgInterval lo = (PgInterval) low;
            return lo.plus(((PgInterval) high).minus(lo).multiply(weight));
        }
        double lo = executor.toDouble(low);
        double result = lo + (executor.toDouble(high) - lo) * weight;
        // A whole number reads back as one: percentile_cont over integers answers 2, not 2.0.
        if (result == Math.floor(result) && !Double.isInfinite(result)) {
            long whole = (long) result;
            if (whole >= Integer.MIN_VALUE && whole <= Integer.MAX_VALUE) {
                return Integer.valueOf((int) whole);
            }
            return Long.valueOf(whole);
        }
        return Double.valueOf(result);
    }

    /**
     * A percentile fraction, checked the way PostgreSQL checks it.
     *
     * <p>The message names the value it was handed rather than restating the rule, and prints it to
     * six significant digits — so 2.0 is reported as 2, 1e10 as 1e+10 and 1.234567891 as 1.23457.
     */
    private double percentileFraction(Object value) {
        double fraction = executor.toDouble(value);
        if (!Double.isNaN(fraction) && fraction >= 0.0 && fraction <= 1.0) return fraction;
        throw new MemgresException("percentile value " + sixSignificantDigits(fraction)
                + " is not between 0 and 1", "22003");
    }

    /**
     * A double written the way PostgreSQL writes one into a message: six significant digits, in
     * plain notation while the exponent stays within reach and in scientific notation once it does
     * not, with trailing zeros dropped either way. Not the same as writing the value out as a
     * value, which keeps every digit needed to read it back.
     */
    private static String sixSignificantDigits(double d) {
        if (Double.isNaN(d)) return "NaN";
        if (Double.isInfinite(d)) return d > 0 ? "Infinity" : "-Infinity";
        BigDecimal rounded = new BigDecimal(d).round(new java.math.MathContext(6));
        int exponent = rounded.precision() - rounded.scale() - 1;
        if (exponent >= -4 && exponent < 6) {
            return rounded.stripTrailingZeros().toPlainString();
        }
        String mantissa = rounded.movePointLeft(exponent).stripTrailingZeros().toPlainString();
        int magnitude = Math.abs(exponent);
        return mantissa + "e" + (exponent < 0 ? "-" : "+")
                + (magnitude < 10 ? "0" : "") + magnitude;
    }

    /** The types PostgreSQL will hand to a double precision parameter without being asked twice. */
    /**
     * Whether an ordered-set call answers to a catalogued signature — a name and argument count a
     * known one has, with direct arguments of a type it takes.
     *
     * <p>PostgreSQL resolves the function before it asks anything about the arguments' grouping, so
     * a call answering to nothing is a function that does not exist rather than one whose direct
     * arguments read an ungrouped column. Asked of the query rather than of a group, which is why
     * it takes bindings instead of rows: it is the same question {@link #checkOrderedSetArgTypes}
     * answers over one, and both percentiles read their fraction as a double precision.
     *
     * <p>The catalogued arity counts the whole signature — what the call takes directly followed by
     * what it orders — so the count to compare against is both together.
     */
    boolean resolvesAsOrderedSet(OrderedSetAggExpr osa, List<RowContext.TableBinding> bindings) {
        String name = FunctionEvaluator.stripSchemaPrefix(osa.funcName().toLowerCase());
        int arity = osa.args().size()
                + (osa.withinGroupOrderBy() == null ? 0 : osa.withinGroupOrderBy().size());
        if (BuiltinAggregateSignatures.orderedSetArity(name) != arity) return false;
        for (Expression arg : osa.args()) {
            if (isUntypedLiteral(arg)) continue;
            DataType written = executor.exprEvaluator.inferTypeFromContext(arg, bindings);
            DataType element = DataType.elementOf(written);
            if (!readsAsDoublePrecision(element != null ? element : written)) return false;
        }
        return true;
    }

    private static boolean readsAsDoublePrecision(DataType type) {
        if (type == null) return true;
        switch (type) {
            case SMALLINT: case INTEGER: case BIGINT:
            case REAL: case DOUBLE_PRECISION: case NUMERIC:
                return true;
            default:
                return false;
        }
    }

    /**
     * Rank a hypothetical row built from the direct arguments against the group, comparing the
     * whole sort key rather than only its first column and honouring each column's direction and
     * NULL placement — the row is inserted into the same ordering the WITHIN GROUP clause states.
     */
    private Object evalHypotheticalSetAggregate(
            String name, OrderedSetAggExpr osa, List<RowContext> group, RowContext typeSample) {
        List<SelectStmt.OrderByItem> orderBy = osa.withinGroupOrderBy();
        int keys = orderBy.size();
        Object[] hypo = new Object[keys];
        for (int i = 0; i < keys; i++) {
            // The direct argument is the value being ranked against its sort column and is
            // declared to have that column's type, so rank('zz') WITHIN GROUP (ORDER BY an
            // integer) is 'zz' read as an integer -- which it is not. Ranking the literal as
            // written compared a string against numbers and answered.
            hypo[i] = readDirectArg(osa.args().get(i),
                    typeOfKey(orderBy.get(i).expr(), typeSample), typeSample);
        }
        long below = 0;
        long atOrBelow = 0;
        Set<String> distinctBelow = new LinkedHashSet<>();
        for (RowContext ctx : group) {
            Object[] row = new Object[keys];
            for (int i = 0; i < keys; i++) {
                row[i] = executor.evalExpr(orderBy.get(i).expr(), ctx);
            }
            int cmp = compareSortKeys(row, hypo, orderBy);
            if (cmp < 0) {
                below++;
                distinctBelow.add(Arrays.deepToString(row));
            }
            if (cmp <= 0) atOrBelow++;
        }
        int n = group.size();
        if (name.equals("rank")) return Long.valueOf(below + 1);
        if (name.equals("dense_rank")) return Long.valueOf(distinctBelow.size() + 1);
        if (name.equals("percent_rank")) {
            if (n == 0) return Double.valueOf(0.0);
            return Double.valueOf((double) below / n);
        }
        // cume_dist: the hypothetical row joins both the numerator and the denominator
        return Double.valueOf((double) (atOrBelow + 1) / (n + 1));
    }

    /**
     * A direct argument, read through the type the aggregate declares for it. A literal written
     * without a type of its own is PostgreSQL's {@code unknown} and takes the declared one, so it
     * is read by that type's input function and is refused there if it is not one of its values.
     */
    private Object readDirectArg(Expression arg, DataType wanted, RowContext sample) {
        Object value = executor.evalExpr(arg, sample);
        if (value == null || wanted == null || !isUntypedLiteral(arg)) return value;
        return executor.castEvaluator.applyCast(value, wanted.getPgName(), true);
    }

    /** Whether the expression is a string literal, which carries no type until it is given one. */
    private static boolean isUntypedLiteral(Expression arg) {
        return arg instanceof Literal
                && ((Literal) arg).literalType() == Literal.LiteralType.STRING;
    }

    /** The type of a WITHIN GROUP sort column, or null where this level cannot name it. */
    private DataType typeOfKey(Expression key, RowContext sample) {
        return executor.exprEvaluator.inferTypeFromContext(key, typeBindings(sample));
    }

    /**
     * What the types of an ordered-set call are read against: the group's own row where it has one,
     * and what the FROM clause exposes where the group came out empty. PostgreSQL settles the call
     * before it reads a page, so a group with no rows in it answers for its types just the same —
     * reading a column off nothing at all made every one of them look like text.
     */
    private List<RowContext.TableBinding> typeBindings(RowContext sample) {
        if (sample != null) return sample.getBindings();
        GroupingScope scope = groupingScope.get();
        return scope != null && scope.bindings != null
                ? scope.bindings : new ArrayList<RowContext.TableBinding>();
    }

    /** Order two sort keys under a WITHIN GROUP ORDER BY list, column directions and all. */
    private int compareSortKeys(Object[] a, Object[] b, List<SelectStmt.OrderByItem> orderBy) {
        for (int i = 0; i < orderBy.size(); i++) {
            SelectStmt.OrderByItem item = orderBy.get(i);
            boolean desc = item.descending();
            // PG's default is NULLS LAST for ASC and NULLS FIRST for DESC
            boolean nullsFirst = item.nullsFirst() != null ? item.nullsFirst().booleanValue() : desc;
            int cmp;
            if (a[i] == null || b[i] == null) {
                if (a[i] == null && b[i] == null) continue;
                cmp = a[i] == null ? (nullsFirst ? -1 : 1) : (nullsFirst ? 1 : -1);
                return cmp;
            }
            cmp = executor.compareValues(a[i], b[i]);
            if (desc) cmp = -cmp;
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    /** PG names the missing function with the direct arguments and the sort columns together. */
    private MemgresException noSuchOrderedSetFunction(OrderedSetAggExpr osa, List<RowContext> group) {
        RowContext sample = group.isEmpty() ? null : group.get(0);
        StringBuilder types = new StringBuilder();
        for (Expression a : osa.args()) {
            if (types.length() > 0) types.append(", ");
            types.append(argTypeName(a, sample));
        }
        if (osa.withinGroupOrderBy() != null) {
            for (SelectStmt.OrderByItem item : osa.withinGroupOrderBy()) {
                if (types.length() > 0) types.append(", ");
                types.append(argTypeName(item.expr(), sample));
            }
        }
        String name = osa.funcName().toLowerCase();
        MemgresException e = new MemgresException(
                "function " + name + "(" + types + ") does not exist", "42883");
        // Casting nothing would resolve a hypothetical-set call: its direct arguments are the row
        // being ranked, so it is the count that has to change, and PostgreSQL names both counts.
        if (HYPOTHETICAL_SET_AGGREGATES.contains(name)) {
            int keys = osa.withinGroupOrderBy() == null ? 0 : osa.withinGroupOrderBy().size();
            e.setHint("To use the hypothetical-set aggregate " + name
                    + ", the number of hypothetical direct arguments (here " + osa.args().size()
                    + ") must match the number of ordering columns (here " + keys + ").");
        }
        return e;
    }

    /**
     * WITHIN GROUP only exists for the ordered-set aggregates. PG resolves an ordinary aggregate
     * written this way against the direct arguments plus the WITHIN GROUP ones, and reports that
     * no such function exists; only a call with no direct arguments at all is named outright.
     */
    private MemgresException notAnOrderedSetAggregate(OrderedSetAggExpr osa, List<RowContext> group) {
        String name = osa.funcName().toLowerCase();
        boolean starOnly = osa.args().isEmpty()
                || (osa.args().size() == 1 && osa.args().get(0) instanceof ColumnRef
                    && "*".equals(((ColumnRef) osa.args().get(0)).column()));
        if (starOnly) {
            return PgErrors.wrongObjectType(
                    name + " is not an ordered-set aggregate, so it cannot have WITHIN GROUP");
        }
        return noSuchOrderedSetFunction(osa, group);
    }

    /** PG leaves a bare quoted literal as "unknown" until a resolved call forces its type. */
    private String argTypeName(Expression expr, RowContext sample) {
        if (expr instanceof Literal && ((Literal) expr).literalType() == Literal.LiteralType.STRING) {
            return "unknown";
        }
        // The name PostgreSQL prints is the type the argument was written as, which is not always
        // recoverable from the value it produces: money carries itself as a string and an array
        // arrives as its elements. Read the expression's own type where there is one, and fall back
        // to the value only where there is not.
        try {
            DataType written = executor.exprEvaluator.inferTypeFromContext(
                    expr, typeBindings(sample));
            if (written != null) return written.toRegtypeDisplay();
        } catch (RuntimeException ex) {
            // Nothing said about the type; the value may still say it.
        }
        Object v;
        try {
            v = executor.evalExpr(expr, sample);
        } catch (RuntimeException ex) {
            return "unknown";
        }
        return v == null ? "unknown" : TypeCoercion.inferType(v).toRegtypeDisplay();
    }

    // ---- Core aggregate evaluation ----

    Object evalAggregate(FunctionCallExpr fn, List<RowContext> group) {
        String name = FunctionEvaluator.stripSchemaPrefix(fn.name().toLowerCase());

        // The star in f(*) says which rows to accumulate over, not what to accumulate: the call
        // has no arguments, and resolves only against a signature declared over none. count is the
        // only aggregate that has one, so sum(*) is a function that does not exist -- where
        // reading the first argument of a call that has none faulted internally instead.
        if (fn.star() && !PARAMETERLESS_AGGREGATES.contains(name)) {
            MemgresException e = new MemgresException(
                    "function " + fn.name() + "() does not exist", "42883");
            e.setHint("No function matches the given name and argument types."
                    + " You might need to add explicit type casts.");
            throw e;
        }
        // And the other way round: a parameterless aggregate is not written with an empty argument
        // list, because the list is not what is empty. PostgreSQL says which spelling to use.
        if (!fn.star() && fn.args().isEmpty() && PARAMETERLESS_AGGREGATES.contains(name)) {
            throw new MemgresException(name
                    + "(*) must be used to call a parameterless aggregate function", "42809");
        }

        // An aggregate is resolved from the types of its arguments like any other call, and a
        // name declared over several categories -- sum over numbers and over intervals -- is not
        // resolved by an untyped literal at all.
        FunctionEvaluator.rejectAmbiguousBuiltin(executor, name, fn.args(),
                group.isEmpty() ? null : group.get(0));

        // An aggregate's own ORDER BY is a sort like any other, and a key nothing can be sorted
        // by leaves it with no order to accumulate in.
        if (!group.isEmpty() && fn.orderBy() != null) {
            for (SelectStmt.OrderByItem item : fn.orderBy()) {
                select.rejectUnsortableKey(item.expr(), group.get(0).getBindings());
            }
        }

        // A nested aggregate is refused while the statement is analysed, in
        // SelectWindowEvaluator.validateCallPlacement, so that the shape is rejected whether or
        // not the query reaches a row and so that the one legal nesting -- an aggregate under a
        // window function, whose arguments this method is also handed -- is let through.

        if (fn.filter() != null) {
            group = group.stream()
                    .filter(ctx -> executor.isTruthy(executor.evalExpr(fn.filter(), ctx)))
                    .collect(Collectors.toList());
        }

        // DISTINCT is over the whole argument list and belongs to every aggregate alike: what
        // reaches the transition function is the argument lists with the repeats dropped. Applied
        // by each aggregate that had been given handling of its own, it was silently ignored by
        // all the rest -- variance and json_object_agg among them -- which read the repeats.
        if (fn.distinct() && !fn.args().isEmpty()) group = distinctArguments(fn, group);

        switch (name) {
            case "count": {
                if (fn.star()) {
                    return (long) group.size();
                }
                Expression arg = fn.args().get(0);
                if (fn.distinct() && fn.args().size() > 1) {
                    throw new MemgresException("function count(text, text) does not exist\n  Hint: No function matches the given name and argument types. You might need to add explicit type casts.", "42883");
                }
                if (fn.distinct()) {
                    DataType keyType = distinctType(arg, group);
                    Set<String> seen = new HashSet<>();
                    for (RowContext ctx : group) {
                        Object val = executor.evalExpr(arg, ctx);
                        if (val != null) seen.add(distinctKey(val, keyType));
                    }
                    return (long) seen.size();
                }
                long count = 0;
                for (RowContext ctx : group) {
                    if (executor.evalExpr(arg, ctx) != null) count++;
                }
                return count;
            }
            case "sum": {
                if (group.isEmpty()) return null;
                Expression arg = fn.args().get(0);
                Object spanned = intervalTotal(fn, group, false);
                if (spanned != NOT_A_SPAN) return spanned;
                boolean hasValue = false;
                boolean allInts = true;
                boolean isMoney = false;
                boolean sawNotANumber = false;
                boolean sawPosInf = false;
                boolean sawNegInf = false;
                boolean allFloat4 = true;
                BigDecimal bdSum = BigDecimal.ZERO;
                try {
                    if (fn.distinct()) {
                        DataType keyType = distinctType(arg, group);
                        Set<String> seenKeys = new HashSet<>();
                        for (RowContext ctx : group) {
                            Object val = executor.evalExpr(arg, ctx);
                            if (val == null || !seenKeys.add(distinctKey(val, keyType))) continue;
                            hasValue = true;
                            if (!(val instanceof Float)) allFloat4 = false;
                            if (isSpecialNumber(val)) {
                                if (Double.isNaN(((Number) val).doubleValue())) sawNotANumber = true;
                                else if (((Number) val).doubleValue() > 0) sawPosInf = true;
                                else sawNegInf = true;
                                continue;
                            }
                            if (val instanceof PgMoney) isMoney = true;
                            bdSum = bdSum.add(SelectExecutor.toBigDecimal(val));
                            if (!(val instanceof Integer || val instanceof Long)) allInts = false;
                        }
                    } else {
                        for (RowContext ctx : group) {
                            Object val = executor.evalExpr(arg, ctx);
                            if (val != null) {
                                hasValue = true;
                                if (!(val instanceof Float)) allFloat4 = false;
                                if (isSpecialNumber(val)) {
                                    if (Double.isNaN(((Number) val).doubleValue())) sawNotANumber = true;
                                    else if (((Number) val).doubleValue() > 0) sawPosInf = true;
                                    else sawNegInf = true;
                                    continue;
                                }
                                if (val instanceof PgMoney) isMoney = true;
                                bdSum = bdSum.add(SelectExecutor.toBigDecimal(val));
                                if (!(val instanceof Integer || val instanceof Long)) allInts = false;
                            }
                        }
                    }
                } catch (NumberFormatException e) {
                    throw noSuchAggregate("sum", arg, group);
                }
                if (!hasValue) return null;
                Double sumSpecial = specialTotal(sawNotANumber, sawPosInf, sawNegInf);
                if (sumSpecial != null) return allFloat4 ? (Object) Float.valueOf(sumSpecial.floatValue()) : sumSpecial;
                if (isMoney) return new PgMoney(bdSum);
                // sum(real) is real in PG, so a total outside real's range overflows there
                if (allFloat4) return NumericLimits.checkFloat4Total(bdSum.doubleValue());
                // sum(double precision) is float8 in PG, not numeric, so a total that leaves
                // float8's range overflows there rather than becoming an infinity
                if (isFloatArgument(arg, group)) return NumericLimits.checkFloat8Total(bdSum.doubleValue());
                if (allInts) {
                    try { return bdSum.longValueExact(); } catch (ArithmeticException e) { /* fall through */ }
                }
                return bdSum;
            }
            case "avg": {
                if (group.isEmpty()) return null;
                Expression arg = fn.args().get(0);
                Object spanned = intervalTotal(fn, group, true);
                if (spanned != NOT_A_SPAN) return spanned;
                requireAggregatableNumeric("avg", arg, group);
                BigDecimal bdSum = BigDecimal.ZERO;
                long count = 0;
                boolean avgSawNotANumber = false;
                boolean avgSawPosInf = false;
                boolean avgSawNegInf = false;
                if (fn.distinct()) {
                    DataType keyType = distinctType(arg, group);
                    Set<String> seenKeys = new HashSet<>();
                    for (RowContext ctx : group) {
                        Object val = executor.evalExpr(arg, ctx);
                        if (val == null || !seenKeys.add(distinctKey(val, keyType))) continue;
                        count++;
                        if (isSpecialNumber(val)) {
                            if (Double.isNaN(((Number) val).doubleValue())) avgSawNotANumber = true;
                            else if (((Number) val).doubleValue() > 0) avgSawPosInf = true;
                            else avgSawNegInf = true;
                            continue;
                        }
                        bdSum = bdSum.add(SelectExecutor.toBigDecimal(val));
                    }
                } else {
                    for (RowContext ctx : group) {
                        Object val = executor.evalExpr(arg, ctx);
                        if (val != null) {
                            count++;
                            if (isSpecialNumber(val)) {
                                if (Double.isNaN(((Number) val).doubleValue())) avgSawNotANumber = true;
                                else if (((Number) val).doubleValue() > 0) avgSawPosInf = true;
                                else avgSawNegInf = true;
                                continue;
                            }
                            bdSum = bdSum.add(SelectExecutor.toBigDecimal(val));
                        }
                    }
                }
                if (count == 0) return null;
                Double avgSpecial = specialTotal(avgSawNotANumber, avgSawPosInf, avgSawNegInf);
                if (avgSpecial != null) return avgSpecial;
                if (isFloatArgument(arg, group)) {
                    return NumericLimits.checkFloat8Total(bdSum.doubleValue()) / count;
                }
                BigDecimal result = bdSum.divide(BigDecimal.valueOf(count), 16, RoundingMode.HALF_UP);
                return result;
            }
            case "min": {
                if (group.isEmpty()) return null;
                Expression arg = fn.args().get(0);
                Object min = null;
                for (RowContext ctx : group) {
                    Object val = executor.evalExpr(arg, ctx);
                    if (val != null && (min == null || executor.compareValues(val, min) < 0)) min = val;
                }
                return min;
            }
            case "max": {
                if (group.isEmpty()) return null;
                Expression arg = fn.args().get(0);
                Object max = null;
                for (RowContext ctx : group) {
                    Object val = executor.evalExpr(arg, ctx);
                    if (val != null && (max == null || executor.compareValues(val, max) > 0)) max = val;
                }
                return max;
            }
            case "string_agg": {
                if (fn.args().size() < 2) {
                    throw new MemgresException("function string_agg(text) does not exist\n  Hint: No function matches the given name and argument types. You might need to add explicit type casts.", "42883");
                }
                if (group.isEmpty()) return null;
                Expression arg = fn.args().get(0);
                Expression delimExpr = fn.args().get(1);
                Object delimVal = delimExpr != null ? executor.evalExpr(delimExpr, group.get(0)) : ",";
                List<RowContext> orderedGroup = sortGroupForAggregate(group, fn);
                Set<String> seen = fn.distinct() ? new LinkedHashSet<>() : null;
                DataType keyType = seen == null ? null : distinctType(arg, group);
                List<Object> parts = new ArrayList<>();
                boolean allBytea = true;
                for (RowContext ctx : orderedGroup) {
                    Object val = executor.evalExpr(arg, ctx);
                    if (val == null) continue;
                    if (seen != null && !seen.add(distinctKey(val, keyType))) continue;
                    if (!(val instanceof byte[])) allBytea = false;
                    parts.add(val);
                }
                if (parts.isEmpty()) return null;
                if (allBytea) {
                    // string_agg(bytea, bytea) is a distinct aggregate returning bytea
                    byte[] sep = delimVal instanceof byte[] ? (byte[]) delimVal : new byte[0];
                    java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
                    for (int pi = 0; pi < parts.size(); pi++) {
                        if (pi > 0) bos.write(sep, 0, sep.length);
                        byte[] pb = (byte[]) parts.get(pi);
                        bos.write(pb, 0, pb.length);
                    }
                    return bos.toByteArray();
                }
                // PG's aggregate treats a NULL separator as nothing at all, not as a default
                String delim = delimVal != null ? String.valueOf(delimVal) : "";
                StringBuilder sb = new StringBuilder();
                for (int pi = 0; pi < parts.size(); pi++) {
                    if (pi > 0) sb.append(delim);
                    sb.append(parts.get(pi));
                }
                return sb.toString();
            }
            case "array_agg": {
                if (group.isEmpty()) return null;
                Expression arg = fn.args().get(0);
                List<RowContext> orderedGroup = sortGroupForAggregate(group, fn);
                List<Object> list = new ArrayList<>();
                Set<String> seen = fn.distinct() ? new LinkedHashSet<>() : null;
                DataType keyType = seen == null ? null : distinctType(arg, group);
                for (RowContext ctx : orderedGroup) {
                    Object val = executor.evalExpr(arg, ctx);
                    if (seen != null && val != null && !seen.add(distinctKey(val, keyType))) continue;
                    list.add(val);
                }
                // DISTINCT sorts what it keeps, since it has to compare the values anyway, and a
                // null sorts after every value the way an ascending order puts it.
                if (fn.distinct() && (fn.orderBy() == null || fn.orderBy().isEmpty())) {
                    list.sort(new java.util.Comparator<Object>() {
                        @Override
                        public int compare(Object a, Object b) {
                            if (a == null || b == null) {
                                return a == b ? 0 : (a == null ? 1 : -1);
                            }
                            return executor.compareValues(a, b);
                        }
                    });
                }
                checkAccumulatedArrays(list);
                // The accumulated values are an array, not the text of one: written as text here,
                // every element went through Java's spelling rather than its own type's.
                return PgArray.of(list);
            }
            case "any_value": {
                if (group.isEmpty()) return null;
                Expression arg = fn.args().get(0);
                for (RowContext ctx : group) {
                    Object val = executor.evalExpr(arg, ctx);
                    if (val != null) return val;
                }
                return null;
            }
            case "range_agg": {
                // range_agg(anyrange) → multirange containing all input ranges, merged
                if (group.isEmpty()) return null;
                Expression arg = fn.args().get(0);
                List<RowContext> orderedGroup = sortGroupForAggregate(group, fn);
                java.util.List<RangeOperations.PgRange> allRanges = new java.util.ArrayList<>();
                for (RowContext ctx : orderedGroup) {
                    Object val = executor.evalExpr(arg, ctx);
                    if (val == null) continue;
                    String s = val.toString().trim();
                    if (s.equalsIgnoreCase("empty")) continue;
                    if (RangeOperations.isMultirangeOrEmpty(s)) {
                        // Multirange input: expand sub-ranges
                        java.util.List<RangeOperations.PgRange> subRanges = RangeOperations.parseMultirange(s);
                        for (RangeOperations.PgRange sr : subRanges) {
                            if (!sr.isEmpty()) allRanges.add(sr);
                        }
                    } else if (RangeOperations.isRangeString(s)) {
                        RangeOperations.PgRange r = RangeOperations.parse(s);
                        if (!r.isEmpty()) allRanges.add(r);
                    }
                }
                if (allRanges.isEmpty()) return null;
                // Sort and merge overlapping/adjacent ranges
                return RangeOperations.formatMultirange(RangeOperations.mergeAndSort(allRanges));
            }
            case "range_intersect_agg": {
                // range_intersect_agg(anyrange) → range: running intersection of all input ranges
                if (group.isEmpty()) return null;
                Expression arg = fn.args().get(0);
                List<RowContext> orderedGroup = sortGroupForAggregate(group, fn);
                RangeOperations.PgRange result = null;
                // The same aggregate is declared over multiranges, and two of those do not meet
                // the way two ranges do: every sub-range of one meets every sub-range of the
                // other, so the running value stays a multirange and is intersected as one.
                String multirangeResult = null;
                for (RowContext ctx : orderedGroup) {
                    Object val = executor.evalExpr(arg, ctx);
                    if (val == null) continue;
                    String s = val.toString().trim();
                    if (s.equalsIgnoreCase("empty")) {
                        return "empty"; // intersection with empty is empty
                    }
                    if (RangeOperations.isMultirangeOrEmpty(s)) {
                        multirangeResult = multirangeResult == null
                                ? RangeOperations.formatMultirange(RangeOperations.parseMultirange(s))
                                : RangeOperations.multirangeIntersect(multirangeResult, s);
                        continue;
                    }
                    if (RangeOperations.isRangeString(s)) {
                        RangeOperations.PgRange r = RangeOperations.parse(s);
                        if (r.isEmpty()) return "empty";
                        if (result == null) {
                            result = r;
                        } else {
                            result = RangeOperations.intersection(result, r);
                            if (result.isEmpty()) return "empty";
                        }
                    }
                }
                if (multirangeResult != null) return multirangeResult;
                if (result == null) return null;
                return result.toString();
            }
            case "bool_and":
            case "every": {
                if (group.isEmpty()) return null;
                Expression arg = fn.args().get(0);
                boolean result = true;
                boolean hasValue = false;
                for (RowContext ctx : group) {
                    Object val = executor.evalExpr(arg, ctx);
                    if (val != null) {
                        hasValue = true;
                        if (!executor.isTruthy(val)) { result = false; break; }
                    }
                }
                return hasValue ? result : null;
            }
            case "bool_or": {
                if (group.isEmpty()) return null;
                Expression arg = fn.args().get(0);
                boolean result = false;
                boolean hasValue = false;
                for (RowContext ctx : group) {
                    Object val = executor.evalExpr(arg, ctx);
                    if (val != null) {
                        hasValue = true;
                        if (executor.isTruthy(val)) { result = true; break; }
                    }
                }
                return hasValue ? result : null;
            }
            case "bit_and": {
                Object result = null;
                for (RowContext r : group) {
                    Object v = executor.evalExpr(fn.args().get(0), r);
                    if (v == null) continue;
                    if (v instanceof AstExecutor.PgBitString) {
                        result = (result == null) ? v
                                : combineBits("bit_and", (AstExecutor.PgBitString) result, (AstExecutor.PgBitString) v, '&');
                    } else {
                        // Only integers and bit strings have a bitwise aggregate; anything else
                        // is a call PostgreSQL has no function for, not an internal cast failure.
                        if (!(v instanceof Number)) throw noSuchAggregate(name, fn.args().get(0), group);
                        long iv = ((Number) v).longValue();
                        long acc = (result == null) ? iv : (((Number) result).longValue() & iv);
                        result = numericBitResult(v, acc);
                    }
                }
                return result;
            }
            case "bit_or": {
                Object result = null;
                for (RowContext r : group) {
                    Object v = executor.evalExpr(fn.args().get(0), r);
                    if (v == null) continue;
                    if (v instanceof AstExecutor.PgBitString) {
                        result = (result == null) ? v
                                : combineBits("bit_or", (AstExecutor.PgBitString) result, (AstExecutor.PgBitString) v, '|');
                    } else {
                        // Only integers and bit strings have a bitwise aggregate; anything else
                        // is a call PostgreSQL has no function for, not an internal cast failure.
                        if (!(v instanceof Number)) throw noSuchAggregate(name, fn.args().get(0), group);
                        long iv = ((Number) v).longValue();
                        long acc = (result == null) ? iv : (((Number) result).longValue() | iv);
                        result = numericBitResult(v, acc);
                    }
                }
                return result;
            }
            case "bit_xor": {
                Object result = null;
                for (RowContext r : group) {
                    Object v = executor.evalExpr(fn.args().get(0), r);
                    if (v == null) continue;
                    if (v instanceof AstExecutor.PgBitString) {
                        result = (result == null) ? v
                                : combineBits("bit_xor", (AstExecutor.PgBitString) result, (AstExecutor.PgBitString) v, '^');
                    } else {
                        // Only integers and bit strings have a bitwise aggregate; anything else
                        // is a call PostgreSQL has no function for, not an internal cast failure.
                        if (!(v instanceof Number)) throw noSuchAggregate(name, fn.args().get(0), group);
                        long iv = ((Number) v).longValue();
                        long acc = (result == null) ? iv : (((Number) result).longValue() ^ iv);
                        result = numericBitResult(v, acc);
                    }
                }
                return result;
            }
            case "json_agg":
            case "jsonb_agg":
            case "json_agg_strict":
            case "jsonb_agg_strict": {
                if (group.isEmpty()) return null;
                // The strict form leaves out the rows whose value is null rather than collecting
                // a JSON null for each; an array of nothing but nulls comes out empty, not null.
                boolean skipNulls = name.endsWith("_strict");
                List<RowContext> orderedGroup = sortGroupForAggregate(group, fn);
                Set<String> seenJson = fn.distinct() ? new LinkedHashSet<>() : null;
                DataType jsonKeyType =
                        seenJson == null ? null : distinctType(fn.args().get(0), group);
                StringBuilder sb = new StringBuilder("[");
                boolean first = true;
                JsonFunctions json = executor.functionEvaluator.jsonFunctions;
                for (RowContext r : orderedGroup) {
                    Object v = json.wholeRowOrValue(fn.args().get(0), r);
                    if (skipNulls && executor.evalExpr(fn.args().get(0), r) == null) continue;
                    if (seenJson != null && !seenJson.add(distinctKey(
                            executor.evalExpr(fn.args().get(0), r), jsonKeyType))) {
                        continue;
                    }
                    boolean structured = v instanceof Map<?, ?> || v instanceof List<?>
                            || v instanceof AstExecutor.PgRow;
                    if (!first) {
                        // PG breaks the line before an element that has a shape of its own, and
                        // writes a plain one straight after the comma.
                        sb.append(structured ? ", \n " : ", ");
                    }
                    first = false;
                    // A row collected into a JSON array is the object it is, not the text a
                    // composite prints as: json_agg(t) gathered "(1,ab,...)" as one string.
                    sb.append(json.jsonTextOf(v));
                }
                sb.append("]");
                return name.startsWith("jsonb")
                        ? JsonFunctions.normalizedIfStructured(sb.toString()) : sb.toString();
            }
            case "json_object_agg":
            case "jsonb_object_agg":
            case "json_object_agg_strict":
            case "jsonb_object_agg_strict":
            case "json_object_agg_unique":
            case "jsonb_object_agg_unique":
            case "json_object_agg_unique_strict":
            case "jsonb_object_agg_unique_strict": {
                if (group.isEmpty()) return null;
                // The strict form leaves out the members whose value is null; the unique form
                // refuses a key that an earlier member of the same group already carried.
                boolean skipNullValues = name.endsWith("_strict");
                boolean requireUnique = name.contains("_unique");
                Set<String> seenObjectKeys = requireUnique ? new LinkedHashSet<>() : null;
                // json prints the object the way its own text output does, padded and with
                // spaces round the colon; jsonb is normalized below and loses the padding again
                StringBuilder sb = new StringBuilder("{ ");
                boolean first = true;
                for (RowContext r : group) {
                    Object k = executor.evalExpr(fn.args().get(0), r);
                    Object v = executor.functionEvaluator.jsonFunctions
                            .wholeRowOrValue(fn.args().get(1), r);
                    // Dropping the row would silently lose it; PG refuses a NULL key outright
                    if (k == null) {
                        throw name.startsWith("jsonb")
                                ? PgErrors.invalidParameter("field name must not be null")
                                : new MemgresException("null value not allowed for object key", "22004");
                    }
                    // The key is looked at before the value is: a repeat is a repeat even where
                    // the member it came with would have been left out for being null.
                    if (seenObjectKeys != null && !seenObjectKeys.add(k.toString())) {
                        // jsonb builds the object before it sees the repeat, so it names no key.
                        throw new MemgresException(name.startsWith("jsonb")
                                ? "duplicate JSON object key value"
                                : "duplicate JSON object key value: \"" + k + "\"", "22030");
                    }
                    if (skipNullValues && executor.evalExpr(fn.args().get(1), r) == null) continue;
                    if (!first) sb.append(", ");
                    first = false;
                    sb.append("\"").append(k.toString().replace("\"", "\\\"")).append("\" : ");
                    sb.append(executor.functionEvaluator.jsonFunctions.jsonTextOf(v));
                }
                sb.append(" }");
                String result = sb.toString();
                if (name.startsWith("jsonb")) {
                    result = JsonOperations.normalizeJsonb(result);
                }
                return result;
            }
            case "json_arrayagg": {
                if (group.isEmpty()) return null;
                // json_arrayagg(expr, null_behavior_flag)
                List<RowContext> orderedGroup2 = sortGroupForAggregate(group, fn);
                int exprIdx = 0;
                String nullBeh = "absent";
                if (fn.args().size() >= 2) {
                    Expression lastA = fn.args().get(fn.args().size() - 1);
                    if (lastA instanceof Literal) {
                        String flag = ((Literal) lastA).value();
                        if ("null_on_null".equals(flag)) nullBeh = "null";
                        else if ("absent_on_null".equals(flag)) nullBeh = "absent";
                    }
                }
                StringBuilder sb3 = new StringBuilder("[");
                boolean first3 = true;
                for (RowContext r : orderedGroup2) {
                    Object v = executor.evalExpr(fn.args().get(exprIdx), r);
                    if (v == null && "absent".equals(nullBeh)) continue;
                    if (!first3) sb3.append(", ");
                    first3 = false;
                    appendJsonVal(sb3, v);
                }
                sb3.append("]");
                return sb3.toString();
            }
            case "json_objectagg": {
                if (group.isEmpty()) return null;
                // json_objectagg(key, value, null_behavior_flag, unique_flag)
                int argCount2 = fn.args().size();
                String nullBeh2 = "absent";
                boolean uniqueKeys2 = false;
                // Parse trailing flags (packed as "absent_on_null"/"null_on_null" and "unique_keys"/"no_unique_keys")
                if (argCount2 >= 4) {
                    Expression la = fn.args().get(argCount2 - 1);
                    Expression sla = fn.args().get(argCount2 - 2);
                    if (la instanceof Literal && sla instanceof Literal) {
                        String f1 = ((Literal) sla).value();
                        String f2 = ((Literal) la).value();
                        if (("absent_on_null".equals(f1) || "null_on_null".equals(f1)) &&
                            ("unique_keys".equals(f2) || "no_unique_keys".equals(f2))) {
                            nullBeh2 = f1.startsWith("null") ? "null" : "absent";
                            uniqueKeys2 = "unique_keys".equals(f2);
                            argCount2 -= 2;
                        }
                    }
                }
                StringBuilder sb4 = new StringBuilder("{ ");
                boolean first4 = true;
                Set<String> seenKeys2 = uniqueKeys2 ? new HashSet<>() : null;
                for (RowContext r : group) {
                    Object k = executor.evalExpr(fn.args().get(0), r);
                    Object v = executor.evalExpr(fn.args().get(1), r);
                    if (k == null) continue;
                    if (v == null && "absent".equals(nullBeh2)) continue;
                    String ks = k.toString();
                    if (uniqueKeys2 && seenKeys2 != null && !seenKeys2.add(ks)) {
                        throw new MemgresException("duplicate JSON object key value", "22030");
                    }
                    if (!first4) sb4.append(", ");
                    first4 = false;
                    sb4.append("\"").append(ks.replace("\\", "\\\\").replace("\"", "\\\"")).append("\" : ");
                    appendJsonVal(sb4, v);
                }
                sb4.append(" }");
                return sb4.toString();
            }
            case "xmlagg": {
                List<RowContext> orderedGroup = sortGroupForAggregate(group, fn);
                StringBuilder sb = new StringBuilder();
                for (RowContext r : orderedGroup) {
                    Object v = executor.evalExpr(fn.args().get(0), r);
                    if (v != null) sb.append(v);
                }
                return sb.length() == 0 ? null : sb.toString();
            }
            case "var_pop": {
                if (group.isEmpty()) return null;
                Expression varArg = fn.args().get(0);
                List<BigDecimal> vals = collectBigDecimals(varArg, group);
                if (vals == null) return Double.valueOf(Double.NaN);
                BigDecimal variance = computeVariance(vals, true);
                if (variance == null) return null;
                if (isFloatArgument(varArg, group)) return variance.doubleValue();
                return trimmed(variance.setScale(16, RoundingMode.HALF_UP));
            }
            case "var_samp":
            case "variance": {
                if (group.isEmpty()) return null;
                Expression varArg = fn.args().get(0);
                List<BigDecimal> vals = collectBigDecimals(varArg, group);
                if (vals == null) return Double.valueOf(Double.NaN);
                BigDecimal variance = computeVariance(vals, false);
                if (variance == null) return null;
                if (isFloatArgument(varArg, group)) return variance.doubleValue();
                return trimmed(variance.setScale(16, RoundingMode.HALF_UP));
            }
            case "stddev_pop":
            case "stddev_samp":
            case "stddev": {
                if (group.isEmpty()) return null;
                Expression arg = fn.args().get(0);
                boolean population = "stddev_pop".equals(name);
                List<BigDecimal> vals = collectBigDecimals(arg, group);
                if (vals == null) return Double.valueOf(Double.NaN);
                BigDecimal variance = computeVariance(vals, population);
                if (variance == null) return null;
                if (isFloatArgument(arg, group)) return Math.sqrt(variance.doubleValue());
                // The square root is taken in numeric: a double one loses the last digit of a
                // 17-significant-figure answer, which is exactly where PG's differs.
                return trimmed(NumericMath.sqrt(variance.setScale(16, RoundingMode.HALF_UP))
                        .setScale(16, RoundingMode.HALF_UP));
            }
            case "grouping": {
                // The answer is an int4 bitmask, so there is room for 31 arguments and no more.
                if (fn.args().size() > 31) {
                    throw new MemgresException("GROUPING must have fewer than 32 arguments", "54023");
                }
                GroupingScope scope = groupingScope.get();
                if (scope == null) {
                    throw new MemgresException(
                            "arguments to GROUPING must be grouping expressions of the associated query level",
                            "42803");
                }
                // Result is an int4 bitmask: bit i (from the left, most significant first) is 1
                // if argument i is NOT grouped in the current grouping set.
                // grouping(a, b) over ROLLUP(a, b): detail rows 0, a-subtotals 1, grand total 3.
                int mask = 0;
                for (Expression arg : fn.args()) {
                    String form = GroupByValidator.canon(arg, scope.bindings);
                    mask = (mask << 1) | (scope.forms.contains(form) ? 0 : 1);
                }
                return Integer.valueOf(mask);
            }
            case "corr": {
                RegressionData rd = RegressionData.compute(group, fn.args(), executor);
                if (rd == null) return null;
                if (rd.sumXDiffSq.compareTo(BigDecimal.ZERO) == 0 || rd.sumYDiffSq.compareTo(BigDecimal.ZERO) == 0) return null;
                BigDecimal denom = new BigDecimal(Math.sqrt(rd.sumXDiffSq.doubleValue()) * Math.sqrt(rd.sumYDiffSq.doubleValue()));
                // corr() is float8 whatever the inputs were
                return rd.sumXYDiff.divide(denom, 16, RoundingMode.HALF_UP).doubleValue();
            }
            case "regr_slope": {
                RegressionData rd = RegressionData.compute(group, fn.args(), executor);
                if (rd == null) return null;
                if (rd.sumXDiffSq.compareTo(BigDecimal.ZERO) == 0) return null;
                BigDecimal slopeVal = rd.sumXYDiff.divide(rd.sumXDiffSq, 16, RoundingMode.HALF_UP);
                return slopeVal.stripTrailingZeros().toPlainString();
            }
            case "regr_intercept": {
                RegressionData rd = RegressionData.compute(group, fn.args(), executor);
                if (rd == null) return null;
                if (rd.sumXDiffSq.compareTo(BigDecimal.ZERO) == 0) return null;
                BigDecimal slopeVal = rd.sumXYDiff.divide(rd.sumXDiffSq, 16, RoundingMode.HALF_UP);
                BigDecimal interceptVal = rd.yMean.subtract(slopeVal.multiply(rd.xMean)).setScale(16, RoundingMode.HALF_UP);
                return interceptVal.stripTrailingZeros().toPlainString();
            }
            case "regr_r2": {
                RegressionData rd = RegressionData.compute(group, fn.args(), executor);
                if (rd == null) return null;
                if (rd.sumXDiffSq.compareTo(BigDecimal.ZERO) == 0 || rd.sumYDiffSq.compareTo(BigDecimal.ZERO) == 0) return null;
                BigDecimal denom = new BigDecimal(Math.sqrt(rd.sumXDiffSq.doubleValue()) * Math.sqrt(rd.sumYDiffSq.doubleValue()));
                BigDecimal corrVal = rd.sumXYDiff.divide(denom, 16, RoundingMode.HALF_UP);
                BigDecimal r2Val = corrVal.pow(2).setScale(16, RoundingMode.HALF_UP);
                return r2Val.stripTrailingZeros().toPlainString();
            }
            case "covar_pop": {
                RegressionData rd = RegressionData.compute(group, fn.args(), executor);
                if (rd == null) return null;
                // covar_pop = sum((xi-xmean)*(yi-ymean)) / N
                return rd.sumXYDiff.divide(BigDecimal.valueOf(rd.n), 16, RoundingMode.HALF_UP).doubleValue();
            }
            case "covar_samp": {
                RegressionData rd = RegressionData.compute(group, fn.args(), executor);
                if (rd == null) return null;
                if (rd.n < 2) return null;
                // covar_samp = sum((xi-xmean)*(yi-ymean)) / (N-1)
                BigDecimal result = rd.sumXYDiff.divide(BigDecimal.valueOf(rd.n - 1), 16, RoundingMode.HALF_UP);
                return result.stripTrailingZeros().toPlainString();
            }
            case "regr_count": {
                if (group.isEmpty() || fn.args().size() < 2) return 0L;
                Expression argY = fn.args().get(0);
                Expression argX = fn.args().get(1);
                long count = 0;
                for (RowContext ctx : group) {
                    Object vx = executor.evalExpr(argX, ctx);
                    Object vy = executor.evalExpr(argY, ctx);
                    if (vx != null && vy != null) count++;
                }
                return count;
            }
            case "regr_avgx": {
                RegressionData rd = RegressionData.compute(group, fn.args(), executor);
                if (rd == null) return null;
                return rd.xMean.stripTrailingZeros().toPlainString();
            }
            case "regr_avgy": {
                RegressionData rd = RegressionData.compute(group, fn.args(), executor);
                if (rd == null) return null;
                return rd.yMean.stripTrailingZeros().toPlainString();
            }
            case "regr_sxx": {
                RegressionData rd = RegressionData.compute(group, fn.args(), executor);
                if (rd == null) return null;
                return rd.sumXDiffSq.stripTrailingZeros().toPlainString();
            }
            case "regr_syy": {
                RegressionData rd = RegressionData.compute(group, fn.args(), executor);
                if (rd == null) return null;
                return rd.sumYDiffSq.stripTrailingZeros().toPlainString();
            }
            case "regr_sxy": {
                RegressionData rd = RegressionData.compute(group, fn.args(), executor);
                if (rd == null) return null;
                return rd.sumXYDiff.stripTrailingZeros().toPlainString();
            }
            default: {
                // Check for user-defined aggregate
                PgAggregate agg = executor.database.getAggregate(name);
                if (agg != null) {
                    return evalUserDefinedAggregate(agg, fn, group);
                }
                return null;
            }
        }
    }

    /** The type an aggregate's argument offers, from what it was written as. */
    private String aggregateArgumentType(Expression arg, List<RowContext> group) {
        RowContext sample = group == null || group.isEmpty() ? null : group.get(0);
        String declared = executor.binaryOpEvaluator.declaredTypeForResolution(arg, sample);
        if (declared == null && sample != null) {
            // A column whose declaration says nothing still holds a value, and what that value is
            // is what the argument is. Reading it as unknown skipped the check the aggregate's
            // parameters are there to make, so text took an integer and said nothing about it.
            try {
                Object value = executor.evalExpr(arg, sample);
                if (value != null) declared = AstExecutor.pgTypeNameOf(value);
            } catch (RuntimeException ignored) {
                // Nothing readable here; the argument stays unknown and the check is skipped.
            }
        }
        return declared == null ? "unknown" : declared.trim().toLowerCase();
    }

    /** Whether a value of {@code have} reaches a parameter declared {@code want}. */
    private static boolean aggregateTypeAccepts(String want, String have) {
        if (want.equals(have)) return true;
        if (want.equals("anyelement") || want.equals("any") || want.equals("anynonarray")) return true;
        boolean wantText = want.equals("text") || want.equals("varchar")
                || want.equals("character varying") || want.equals("bpchar");
        boolean haveText = have.equals("text") || have.equals("varchar")
                || have.equals("character varying") || have.equals("bpchar");
        if (wantText && haveText) return true;
        int wantRank = numericRank(want);
        int haveRank = numericRank(have);
        return wantRank > 0 && haveRank > 0 && haveRank <= wantRank;
    }

    private static int numericRank(String type) {
        switch (type) {
            case "smallint": case "int2": return 1;
            case "integer": case "int": case "int4": return 2;
            case "bigint": case "int8": return 3;
            case "numeric": case "decimal": return 4;
            case "real": case "float4": return 5;
            case "double precision": case "float8": return 6;
            default: return 0;
        }
    }

    private Object evalUserDefinedAggregate(PgAggregate agg, FunctionCallExpr fn, List<RowContext> group) {
        // An aggregate declared over one argument has to be given one. Reading the first of an
        // empty argument list reached the client as an internal error about an array index; the
        // call simply names no aggregate that takes nothing.
        String[] declared = agg.getArgTypes() == null ? new String[0] : agg.getArgTypes();
        int given = fn.args() == null ? 0 : fn.args().size();
        // An aggregate is its name and the arguments it declares, so a call with a different
        // number of them names no aggregate. Reading the first of an empty list reached the
        // client as an internal error about an array index, and an extra argument was ignored.
        if (given != declared.length) {
            StringBuilder types = new StringBuilder();
            for (int i = 0; i < given; i++) {
                if (i > 0) types.append(", ");
                types.append(CatalogSystemFunctions.readableTypeName(
                        aggregateArgumentType(fn.args().get(i), group)));
            }
            throw new MemgresException("function " + FunctionEvaluator.stripSchemaPrefix(fn.name())
                    + "(" + types + ") does not exist", "42883");
        }
        // And the arguments have to be of the types it declares.
        for (int i = 0; i < given; i++) {
            String want = declared[i] == null ? null : declared[i].trim().toLowerCase();
            String have = aggregateArgumentType(fn.args().get(i), group);
            if (want == null || "unknown".equals(have) || want.equals(have)) continue;
            if (!aggregateTypeAccepts(want, have)) {
                StringBuilder types = new StringBuilder();
                for (int j = 0; j < given; j++) {
                    if (j > 0) types.append(", ");
                    types.append(CatalogSystemFunctions.readableTypeName(
                            aggregateArgumentType(fn.args().get(j), group)));
                }
                throw new MemgresException("function "
                        + FunctionEvaluator.stripSchemaPrefix(fn.name())
                        + "(" + types + ") does not exist", "42883");
            }
        }
        // Initialize state from INITCOND or null
        Object state = null;
        if (agg.getInitcond() != null) {
            try {
                QueryResult initResult = executor.execute("SELECT " + castLiteral(agg.getInitcond(), agg.getStype()));
                if (!initResult.getRows().isEmpty() && initResult.getRows().get(0).length > 0) {
                    state = initResult.getRows().get(0)[0];
                }
            } catch (Exception e) {
                // fallback: use the raw string
                state = agg.getInitcond();
            }
        }

        // Resolve SFUNC once outside the loop
        PgFunction sfunc = executor.database.getFunction(agg.getSfunc());
        boolean sfuncStrict = sfunc != null && sfunc.isStrict();
        boolean needsFirstNonNull = sfuncStrict && agg.getInitcond() == null;

        // For DISTINCT, track seen value tuples
        Set<String> seen = fn.distinct() ? new LinkedHashSet<>() : null;

        // An ORDER BY inside the call says which order the rows reach the transition function in,
        // and for an aggregate that concatenates them the order is the answer. Accumulating in
        // whatever order the scan produced gave a different answer from the one that was asked for.
        List<RowContext> ordered = group;
        if (fn.orderBy != null && !fn.orderBy.isEmpty()) {
            ordered = new ArrayList<>(group);
            final List<SelectStmt.OrderByItem> keys = fn.orderBy;
            ordered.sort((left, right) -> {
                for (SelectStmt.OrderByItem key : keys) {
                    Object a = executor.evalExpr(key.expr(), left);
                    Object b = executor.evalExpr(key.expr(), right);
                    int cmp = TypeCoercion.compare(a, b);
                    if (cmp != 0) return key.descending() ? -cmp : cmp;
                }
                return 0;
            });
        }

        // For each row, call SFUNC(state, value[, ...])
        for (RowContext ctx : ordered) {
            List<Object> argValues = new ArrayList<>();
            for (Expression arg : fn.args()) {
                argValues.add(executor.evalExpr(arg, ctx));
            }

            // STRICT SFUNC: skip rows where any input value is NULL
            if (sfuncStrict && argValues.stream().anyMatch(v -> v == null)) {
                continue;
            }

            // STRICT SFUNC with no INITCOND: use first non-NULL input as initial state
            if (needsFirstNonNull && state == null) {
                // For single-arg aggregates, first value becomes state; for multi-arg, use first arg
                state = argValues.get(0);
                needsFirstNonNull = false;
                continue; // skip calling SFUNC for this row (PG behavior)
            }

            // STRICT SFUNC: skip if state is NULL (can happen if SFUNC returned NULL)
            if (sfuncStrict && state == null) {
                continue;
            }

            // DISTINCT: skip duplicate value tuples
            if (seen != null) {
                String key = argValues.stream()
                        .map(v -> v == null ? "\0" : distinctKey(v))
                        .collect(Collectors.joining("\1"));
                if (!seen.add(key)) continue;
            }

            List<Object> args = new ArrayList<>();
            args.add(state);
            args.addAll(argValues);
            // Call the state transition function
            if (sfunc != null) {
                com.memgres.engine.plpgsql.PlpgsqlExecutor plExec =
                        new com.memgres.engine.plpgsql.PlpgsqlExecutor(executor, executor.database, executor.session);
                state = plExec.executeFunction(sfunc, args);
            }
        }

        // If FINALFUNC is specified, call it on the final state.
        // PG behavior: if no INITCOND and empty group, state is NULL and FINALFUNC is NOT called.
        if (agg.getFinalfunc() != null && (agg.getInitcond() != null || !group.isEmpty())) {
            PgFunction ffunc = executor.database.getFunction(agg.getFinalfunc());
            if (ffunc != null) {
                List<Object> args = new ArrayList<>();
                args.add(state);
                com.memgres.engine.plpgsql.PlpgsqlExecutor plExec =
                        new com.memgres.engine.plpgsql.PlpgsqlExecutor(executor, executor.database, executor.session);
                state = plExec.executeFunction(ffunc, args);
            }
        }

        return state;
    }

    private String castLiteral(String value, String type) {
        // Quote the value and cast to the appropriate type
        return "'" + value.replace("'", "''") + "'::" + type;
    }

    // ---- DRY helpers ----

    /**
     * True for a NaN or an infinity, none of which has a BigDecimal form. They are accumulated
     * apart from the running total and then decide the result on their own, the way they do in
     * PG: a NaN poisons the sum, and two opposing infinities cancel into one.
     */
    private static boolean isSpecialNumber(Object val) {
        return NumericLimits.isSpecial(val);
    }

    /**
     * The value a sum or an average takes when specials were seen, or null when the ordinary
     * total stands. Every finite addend is irrelevant once an infinity is in the group.
     */
    private static Double specialTotal(boolean sawNaN, boolean sawPosInf, boolean sawNegInf) {
        if (sawNaN || (sawPosInf && sawNegInf)) return Double.valueOf(Double.NaN);
        if (sawPosInf) return Double.valueOf(Double.POSITIVE_INFINITY);
        if (sawNegInf) return Double.valueOf(Double.NEGATIVE_INFINITY);
        return null;
    }

    /**
     * Dedup key for DISTINCT aggregates with value semantics: numeric values that compare
     * equal map to the same key even when their representations differ (numeric 1.0 vs 1.00,
     * or int 1 vs numeric 1.0), matching PostgreSQL's equality-based DISTINCT.
     */
    private static String distinctKey(Object val, DataType type) {
        // An aggregate that gathers nulls as well as values needs a key for them too, and one
        // null is the same value as another wherever DISTINCT is concerned.
        if (val == null) return "\u0000";
        // jsonb is held as the text it prints as, which is not the document that text spells:
        // 1 and 1.0 are one value, and counting them by their texts counted two
        if (type == DataType.JSONB && val instanceof String) return RowKey.valueKey(val, type);
        return distinctKey(val);
    }

    /** The rows of a group whose argument list no earlier row of the group already had. */
    private List<RowContext> distinctArguments(FunctionCallExpr fn, List<RowContext> group) {
        List<DataType> types = new ArrayList<>();
        for (Expression arg : fn.args()) types.add(distinctType(arg, group));
        Set<String> seen = new HashSet<>();
        List<RowContext> kept = new ArrayList<>();
        for (RowContext ctx : group) {
            StringBuilder key = new StringBuilder();
            for (int i = 0; i < fn.args().size(); i++) {
                // A null is a value here, so two argument lists null in the same place are one.
                key.append(distinctKey(executor.evalExpr(fn.args().get(i), ctx), types.get(i)))
                        .append('\u0001');
            }
            if (seen.add(key.toString())) kept.add(ctx);
        }
        return kept;
    }

    /**
     * The type a DISTINCT aggregate compares its values as, refusing one that cannot be compared
     * at all: DISTINCT gathers equal values together, and json has no equality to gather by.
     */
    private DataType distinctType(Expression arg, List<RowContext> group) {
        if (group.isEmpty() || arg == null) return null;
        DataType type = executor.exprEvaluator.inferTypeFromContext(
                arg, group.get(0).getBindings());
        MemgresException refusal = OperatorResolution.noEqualityFor(type);
        if (refusal != null) throw refusal;
        return type;
    }

    private static String distinctKey(Object val) {
        // bytea dedups by byte sequence, not array identity
        if (val instanceof byte[]) return RowKey.valueKey(val);
        if (val instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) val;
            if (bd.signum() == 0) return "0";
            return bd.stripTrailingZeros().toPlainString();
        }
        if (val instanceof Double || val instanceof Float) {
            double d = ((Number) val).doubleValue();
            if (!Double.isNaN(d) && !Double.isInfinite(d)) {
                if (d == 0.0) return "0";
                return BigDecimal.valueOf(d).stripTrailingZeros().toPlainString();
            }
            return String.valueOf(d);
        }
        // An interval is the span it measures, so two spellings of one span are one value:
        // '1 mon', '30 days' and '720 hours' are one interval, and counting them by the text
        // they were written with counted three.
        if (val instanceof PgInterval) return RowKey.valueKey(val);
        // Every other type answers to the one key builder, so DISTINCT counts what = counts: a
        // timestamptz is the instant it names whatever offset it was written with, and an array
        // is its elements rather than the text they were printed as.
        if (val instanceof java.time.OffsetDateTime || val instanceof java.util.List<?>
                || val instanceof AstExecutor.PgRow) {
            return RowKey.valueKey(val);
        }
        return val.toString();
    }

    /**
     * PG has no avg(money) or avg over other non-numeric types; reject rather than
     * silently coercing the value through numeric.
     */
    /** Told apart from a null total, which is what an empty group of intervals answers with. */
    private static final Object NOT_A_SPAN = new Object();

    /**
     * The total, or the mean, of a group of lengths of time. A time of day reaches these
     * aggregates as the length from midnight it stands for, because that is the only signature
     * either of them has for it. Answers {@link #NOT_A_SPAN} when the group is not lengths at all,
     * which leaves the numeric readers below to have it.
     */
    private Object intervalTotal(FunctionCallExpr fn, List<RowContext> group, boolean mean) {
        Expression arg = fn.args().get(0);
        List<Object> values = new ArrayList<>();
        DataType keyType = fn.distinct() ? distinctType(arg, group) : null;
        Set<String> seenKeys = fn.distinct() ? new HashSet<String>() : null;
        for (RowContext ctx : group) {
            Object val = executor.evalExpr(arg, ctx);
            if (val == null) continue;
            if (!(val instanceof PgInterval) && !(val instanceof java.time.LocalTime)) {
                return NOT_A_SPAN;
            }
            if (fn.distinct() && !seenKeys.add(distinctKey(val, keyType))) continue;
            values.add(val);
        }
        if (values.isEmpty()) return NOT_A_SPAN;
        PgInterval total = new PgInterval(0, 0, 0);
        for (Object val : values) total = total.plus(TypeCoercion.toInterval(val));
        if (!mean) return total;
        return dividedInterval(total, values.size());
    }

    /**
     * An interval divided by a count. The months and the days are whole numbers, so what is left
     * over from each cascades into the unit below it — a third of a month is ten days, not a
     * third of a month — and the microseconds take whatever is left after that.
     */
    static PgInterval dividedInterval(PgInterval total, int by) {
        double factor = by;
        int months = (int) (total.getMonths() / factor);
        int days = (int) (total.getDays() / factor);
        double monthDays = roundedToMicros((total.getMonths() / factor - months) * 30.0);
        double secondsLeft = roundedToMicros((total.getDays() / factor - days
                + monthDays - (int) monthDays) * 86_400.0);
        if (Math.abs(secondsLeft) >= 86_400.0) {
            days += (int) (secondsLeft / 86_400.0);
            secondsLeft -= (int) (secondsLeft / 86_400.0) * 86_400.0;
        }
        days += (int) monthDays;
        long micros = (long) Math.rint(total.getMicroseconds() / factor + secondsLeft * 1_000_000.0);
        return new PgInterval(months, days, micros);
    }

    /** Rounded to the microsecond, which is as fine as an interval is held. */
    private static double roundedToMicros(double value) {
        return Math.rint(value * 1_000_000.0) / 1_000_000.0;
    }

    private void requireAggregatableNumeric(String fname, Expression arg, List<RowContext> group) {
        for (RowContext ctx : group) {
            Object val = executor.evalExpr(arg, ctx);
            if (val == null) continue;
            if (val instanceof PgMoney) {
                throw new MemgresException("function " + fname + "(money) does not exist"
                        + "\n  Hint: No function matches the given name and argument types. You might need to add explicit type casts.", "42883");
            }
            // A value that is not a number at all is the same missing function sum reports; it
            // used to escape the accumulation loop below as an internal parse failure (XX000).
            // A number is aggregatable whatever it is worth, so NaN and the infinities are not
            // asked to survive the trip through BigDecimal that they cannot make.
            if (!(val instanceof Number)) {
                try {
                    SelectExecutor.toBigDecimal(val);
                } catch (RuntimeException e) {
                    throw noSuchAggregate(fname, arg, group);
                }
            }
            return;
        }
    }

    /**
     * PostgreSQL names the aggregate it has no such overload of by the argument's declared type,
     * spelled as SQL spells it: {@code sum(character varying)}, not {@code sum(text)} read off
     * the value that happened to be in the column.
     */
    private MemgresException noSuchAggregate(String fname, Expression arg, List<RowContext> group) {
        RowContext sample = group.isEmpty() ? null : group.get(0);
        DataType declared = executor.exprEvaluator.inferTypeFromContext(arg,
                sample != null ? sample.getBindings()
                        : new java.util.ArrayList<RowContext.TableBinding>());
        String typeName = declared != null ? declared.toRegtypeDisplay() : argTypeName(arg, sample);
        MemgresException e = new MemgresException(
                "function " + fname + "(" + typeName + ") does not exist", "42883");
        e.setHint("No function matches the given name and argument types. You might need to add explicit type casts.");
        return e;
    }

    /** PG's bit-string bit_and/bit_or/bit_xor: bitwise, and only over equal-length operands. */
    private static AstExecutor.PgBitString combineBits(String fname, AstExecutor.PgBitString a,
                                                       AstExecutor.PgBitString b, char op) {
        String x = a.bits();
        String y = b.bits();
        if (x.length() != y.length()) {
            throw new MemgresException("cannot " + fname.substring(4).toUpperCase()
                    + " bit strings of different sizes", "22026");
        }
        StringBuilder sb = new StringBuilder(x.length());
        for (int i = 0; i < x.length(); i++) {
            boolean p = x.charAt(i) == '1';
            boolean q = y.charAt(i) == '1';
            boolean res = op == '&' ? (p && q) : op == '|' ? (p || q) : (p ^ q);
            sb.append(res ? '1' : '0');
        }
        return new AstExecutor.PgBitString(sb.toString());
    }

    /** Keep the input's integer width, as PG's smallint/int/bigint bit aggregates do. */
    private static Object numericBitResult(Object sample, long acc) {
        if (sample instanceof Long) return acc;
        if (sample instanceof Short) return (short) acc;
        if (sample instanceof Integer) return (int) acc;
        return acc >= Integer.MIN_VALUE && acc <= Integer.MAX_VALUE ? (Object) (int) acc : (Object) acc;
    }

    /**
     * Arrays collected by array_agg become the rows of one array of the next dimension up, so they
     * all have to be the same shape and none of them may be missing. A shorter row or a null one
     * has no place to go, and PostgreSQL says so rather than building a ragged result.
     */
    private static void checkAccumulatedArrays(List<Object> values) {
        int width = -1;
        boolean anyArray = false;
        for (Object v : values) {
            if (v instanceof List<?>) { anyArray = true; break; }
        }
        if (!anyArray) return;
        for (Object v : values) {
            if (v == null) {
                throw new MemgresException("cannot accumulate null arrays", "22004");
            }
            if (!(v instanceof List<?>)) continue;
            int size = ((List<?>) v).size();
            if (width < 0) {
                // The shape every later row is measured against comes from the first array, and
                // an empty one has no shape to give: PostgreSQL refuses it where it stands rather
                // than reporting the rows after it against a shape that was never taken. An empty
                // array further along is just a row of the wrong width.
                if (size == 0) {
                    throw new MemgresException("cannot accumulate empty arrays", "2202E");
                }
                width = size;
            } else if (width != size) {
                throw new MemgresException(
                        "cannot accumulate arrays of different dimensionality", "2202E");
            }
        }
    }

    /** Sort a group by the aggregate's ORDER BY clause (used by string_agg, array_agg, json_agg, etc.). */
    private List<RowContext> sortGroupForAggregate(List<RowContext> group, FunctionCallExpr fn) {
        checkDistinctOrderBy(fn);
        List<RowContext> orderedGroup = new ArrayList<>(group);
        if (fn.orderBy() != null && !fn.orderBy().isEmpty()) {
            // A jsonb sort key is held as the text it prints as, which is not the order it has:
            // the type of each key is settled once, before the rows are looked at.
            List<DataType> keyTypes = new ArrayList<>();
            List<RowContext.TableBinding> bindings = group.isEmpty()
                    ? new ArrayList<RowContext.TableBinding>() : group.get(0).getBindings();
            for (SelectStmt.OrderByItem item : fn.orderBy()) {
                keyTypes.add(executor.exprEvaluator.inferTypeFromContext(item.expr(), bindings));
            }
            orderedGroup.sort((a, b) -> {
                for (int idx = 0; idx < fn.orderBy().size(); idx++) {
                    SelectStmt.OrderByItem item = fn.orderBy().get(idx);
                    Object va = executor.evalExpr(item.expr(), a);
                    Object vb = executor.evalExpr(item.expr(), b);
                    // PG's default is NULLS LAST ascending, NULLS FIRST descending: nulls sort
                    // as the largest value, and DESC flips that along with everything else. The
                    // clause may say otherwise, and reading only the direction ignored it.
                    if (va == null || vb == null) {
                        if (va == null && vb == null) continue;
                        boolean nullsFirst = item.nullsFirst() != null
                                ? item.nullsFirst().booleanValue() : item.descending();
                        return (va == null) == nullsFirst ? -1 : 1;
                    }
                    int cmp = keyTypes.get(idx) == DataType.JSONB
                            && va instanceof String && vb instanceof String
                            ? JsonOperations.compareJsonb((String) va, (String) vb)
                            : executor.compareValues(va, vb);
                    if (item.descending()) cmp = -cmp;
                    if (cmp != 0) return cmp;
                }
                return 0;
            });
        }
        return orderedGroup;
    }

    /**
     * DISTINCT collapses the group down to the aggregate's own arguments, so a sort key that
     * is not one of them no longer exists by the time the ordering would be applied. PG rejects
     * that combination rather than sorting by a value it has already discarded.
     */
    private static void checkDistinctOrderBy(FunctionCallExpr fn) {
        if (!fn.distinct() || fn.orderBy() == null || fn.orderBy().isEmpty()) return;
        Set<String> argTexts = new HashSet<>();
        for (Expression arg : fn.args()) {
            if (arg != null) argTexts.add(arg.toString());
        }
        for (SelectStmt.OrderByItem item : fn.orderBy()) {
            if (item.expr() != null && !argTexts.contains(item.expr().toString())) {
                throw new MemgresException(
                        "in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list",
                        "42P10");
            }
        }
    }

    /** True when the aggregate's input is double precision, which keeps the result float8. */
    private boolean isFloatArgument(Expression arg, List<RowContext> group) {
        for (RowContext ctx : group) {
            Object v = executor.evalExpr(arg, ctx);
            if (v == null) continue;
            return v instanceof Double || v instanceof Float;
        }
        return false;
    }

    /**
     * Collect BigDecimal values from a group for a given expression, skipping nulls. Returns null
     * when the group holds a NaN or an infinity, which have no BigDecimal form: the variance of a
     * set containing one is NaN, and PG says so rather than failing to read the value.
     */
    private List<BigDecimal> collectBigDecimals(Expression arg, List<RowContext> group) {
        List<BigDecimal> vals = new ArrayList<>();
        for (RowContext ctx : group) {
            Object v = executor.evalExpr(arg, ctx);
            if (v == null) continue;
            if (NumericLimits.isSpecial(v)) return null;
            vals.add(v instanceof BigDecimal ? ((BigDecimal) v) : BigDecimal.valueOf(((Number) v).doubleValue()));
        }
        return vals;
    }

    /** Drop trailing fractional zeros without letting BigDecimal reach for 1E+2 notation. */
    private static BigDecimal trimmed(BigDecimal v) {
        BigDecimal stripped = v.stripTrailingZeros();
        return stripped.scale() < 0 ? stripped.setScale(0) : stripped;
    }

    /** Compute variance (population or sample) from a list of BigDecimal values. */
    private static BigDecimal computeVariance(List<BigDecimal> vals, boolean population) {
        if (vals.isEmpty()) return null;
        if (!population && vals.size() < 2) return null;
        BigDecimal n = BigDecimal.valueOf(vals.size());
        BigDecimal sum = vals.stream().reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal mean = sum.divide(n, 20, RoundingMode.HALF_UP);
        BigDecimal sumSqDiff = vals.stream()
            .map(v -> v.subtract(mean).pow(2))
            .reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal divisor = population ? n : BigDecimal.valueOf(vals.size() - 1);
        return sumSqDiff.divide(divisor, 20, RoundingMode.HALF_UP);
    }

    /** Pre-computed regression statistics shared by corr, regr_slope, regr_intercept, regr_r2. */
        private static final class RegressionData {
        public final BigDecimal xMean;
        public final BigDecimal yMean;
        public final BigDecimal sumXYDiff;
        public final BigDecimal sumXDiffSq;
        public final BigDecimal sumYDiffSq;
        public final int n;

        public RegressionData(
                BigDecimal xMean,
                BigDecimal yMean,
                BigDecimal sumXYDiff,
                BigDecimal sumXDiffSq,
                BigDecimal sumYDiffSq,
                int n
        ) {
            this.xMean = xMean;
            this.yMean = yMean;
            this.sumXYDiff = sumXYDiff;
            this.sumXDiffSq = sumXDiffSq;
            this.sumYDiffSq = sumYDiffSq;
            this.n = n;
        }

        static RegressionData compute(List<RowContext> group, List<Expression> args, AstExecutor executor) {
            if (group.isEmpty() || args.size() < 2) return null;
            Expression argY = args.get(0);
            Expression argX = args.get(1);
            List<BigDecimal> xVals = new ArrayList<>();
            List<BigDecimal> yVals = new ArrayList<>();
            for (RowContext ctx : group) {
                Object vx = executor.evalExpr(argX, ctx);
                Object vy = executor.evalExpr(argY, ctx);
                if (vx != null && vy != null) {
                    xVals.add(vx instanceof BigDecimal ? ((BigDecimal) vx) : BigDecimal.valueOf(((Number) vx).doubleValue()));
                    yVals.add(vy instanceof BigDecimal ? ((BigDecimal) vy) : BigDecimal.valueOf(((Number) vy).doubleValue()));
                }
            }
            if (xVals.isEmpty()) return null;
            BigDecimal n = BigDecimal.valueOf(xVals.size());
            BigDecimal xSum = xVals.stream().reduce(BigDecimal.ZERO, BigDecimal::add);
            BigDecimal ySum = yVals.stream().reduce(BigDecimal.ZERO, BigDecimal::add);
            BigDecimal xMean = xSum.divide(n, 20, RoundingMode.HALF_UP);
            BigDecimal yMean = ySum.divide(n, 20, RoundingMode.HALF_UP);
            BigDecimal sumXYDiff = BigDecimal.ZERO;
            BigDecimal sumXDiffSq = BigDecimal.ZERO;
            BigDecimal sumYDiffSq = BigDecimal.ZERO;
            for (int i = 0; i < xVals.size(); i++) {
                BigDecimal xd = xVals.get(i).subtract(xMean);
                BigDecimal yd = yVals.get(i).subtract(yMean);
                sumXYDiff = sumXYDiff.add(xd.multiply(yd));
                sumXDiffSq = sumXDiffSq.add(xd.pow(2));
                sumYDiffSq = sumYDiffSq.add(yd.pow(2));
            }
            return new RegressionData(xMean, yMean, sumXYDiff, sumXDiffSq, sumYDiffSq, xVals.size());
        }

        public BigDecimal xMean() { return xMean; }
        public BigDecimal yMean() { return yMean; }
        public BigDecimal sumXYDiff() { return sumXYDiff; }
        public BigDecimal sumXDiffSq() { return sumXDiffSq; }
        public BigDecimal sumYDiffSq() { return sumYDiffSq; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RegressionData that = (RegressionData) o;
            return java.util.Objects.equals(xMean, that.xMean)
                && java.util.Objects.equals(yMean, that.yMean)
                && java.util.Objects.equals(sumXYDiff, that.sumXYDiff)
                && java.util.Objects.equals(sumXDiffSq, that.sumXDiffSq)
                && java.util.Objects.equals(sumYDiffSq, that.sumYDiffSq);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(xMean, yMean, sumXYDiff, sumXDiffSq, sumYDiffSq);
        }

        @Override
        public String toString() {
            return "RegressionData[xMean=" + xMean + ", " + "yMean=" + yMean + ", " + "sumXYDiff=" + sumXYDiff + ", " + "sumXDiffSq=" + sumXDiffSq + ", " + "sumYDiffSq=" + sumYDiffSq + "]";
        }
    }

    private void appendJsonVal(StringBuilder sb, Object val) {
        if (val == null) { sb.append("null"); return; }
        if (val instanceof Number || val instanceof Boolean) { sb.append(val); return; }
        String s = val.toString().trim();
        if ((s.startsWith("{") && s.endsWith("}")) || (s.startsWith("[") && s.endsWith("]"))) {
            sb.append(s);
        } else {
            sb.append("\"").append(s.replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
        }
    }

}
