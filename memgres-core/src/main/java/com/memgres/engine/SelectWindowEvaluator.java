package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.*;

/**
 * Handles window function evaluation: OVER clauses, partitioning, framing,
 * and aggregate-as-window functions.
 * Extracted from SelectExecutor to separate concerns.
 */
class SelectWindowEvaluator {
    private final SelectExecutor select;
    private final AstExecutor executor;

    SelectWindowEvaluator(SelectExecutor select) {
        this.select = select;
        this.executor = select.executor;
    }

    /**
     * Execute a SELECT that contains window functions.
     */
    QueryResult executeWindowSelect(SelectStmt stmt, List<RowContext> contexts,
                                     List<RowContext.TableBinding> baseBindings) {
        // Build result columns
        List<Column> resultColumns = new ArrayList<>();
        for (SelectStmt.SelectTarget target : stmt.targets()) {
            String alias = target.alias();
            if (alias == null) alias = executor.exprToAlias(target.expr());
            if (target.expr() instanceof WildcardExpr) {
                WildcardExpr w = (WildcardExpr) target.expr();
                if (w.table() != null) {
                    for (RowContext.TableBinding b : baseBindings) {
                        if (b.alias().equalsIgnoreCase(w.table()) || b.table().getName().equalsIgnoreCase(w.table())) {
                            for (Column c : b.table().getColumns()) resultColumns.add(c);
                        }
                    }
                } else {
                    for (RowContext.TableBinding b : baseBindings) {
                        for (Column c : b.table().getColumns()) resultColumns.add(c);
                    }
                }
            } else {
                resultColumns.add(new Column(alias, executor.inferTypeFromContext(target.expr(), baseBindings), true, false, null));
            }
        }

        List<Object[]> resultRows = new ArrayList<>(contexts.size());

        // Pre-compute all window function results for each row
        Map<Integer, Object[]> windowResults = new LinkedHashMap<>();

        for (int ti = 0; ti < stmt.targets().size(); ti++) {
            Expression expr = stmt.targets().get(ti).expr();
            if (select.containsWindowFunction(expr)) {
                Object[] values = evaluateWindowExpression(expr, contexts, stmt.windowDefs());
                windowResults.put(ti, values);
            }
        }

        // Now project all rows
        for (int ri = 0; ri < contexts.size(); ri++) {
            RowContext ctx = contexts.get(ri);
            List<Object> rowValues = new ArrayList<>();

            for (int ti = 0; ti < stmt.targets().size(); ti++) {
                SelectStmt.SelectTarget target = stmt.targets().get(ti);
                if (target.expr() instanceof WildcardExpr) {
                    WildcardExpr w = (WildcardExpr) target.expr();
                    if (w.table() != null) {
                        for (RowContext.TableBinding b : ctx.getBindings()) {
                            if (b.alias().equalsIgnoreCase(w.table()) || b.table().getName().equalsIgnoreCase(w.table())) {
                                for (int ci = 0; ci < b.table().getColumns().size(); ci++) {
                                    rowValues.add(b.row()[ci]);
                                }
                            }
                        }
                    } else {
                        for (RowContext.TableBinding b : ctx.getBindings()) {
                            for (int ci = 0; ci < b.table().getColumns().size(); ci++) {
                                rowValues.add(b.row()[ci]);
                            }
                        }
                    }
                } else if (windowResults.containsKey(ti)) {
                    rowValues.add(windowResults.get(ti)[ri]);
                } else {
                    rowValues.add(executor.evalExpr(target.expr(), ctx));
                }
            }
            resultRows.add(rowValues.toArray());
        }

        // ORDER BY
        List<SelectStmt.OrderByItem> resolvedOrderBy = select.resolveOrderBy(stmt.orderBy(), stmt.targets());
        if (resolvedOrderBy != null && !resolvedOrderBy.isEmpty()) {
            Integer[] indices = new Integer[resultRows.size()];
            for (int i = 0; i < indices.length; i++) indices[i] = i;
            final List<Object[]> finalResultRows = resultRows;
            java.util.Arrays.sort(indices, (ai, bi) -> {
                Object[] a = finalResultRows.get(ai);
                Object[] b = finalResultRows.get(bi);
                for (SelectStmt.OrderByItem item : resolvedOrderBy) {
                    int colIdx = select.resolveOrderByToColumnIndex(item.expr(), stmt.targets());
                    Object va, vb;
                    if (colIdx >= 0) {
                        va = a[colIdx]; vb = b[colIdx];
                    } else {
                        va = executor.evalExpr(item.expr(), contexts.get(ai));
                        vb = executor.evalExpr(item.expr(), contexts.get(bi));
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
            List<Object[]> sorted = new ArrayList<>(resultRows.size());
            for (int idx : indices) sorted.add(finalResultRows.get(idx));
            resultRows = sorted;
        }

        resultRows = select.applyDistinct(stmt, resultRows);
        resultRows = select.applyOffsetAndLimit(stmt, resultRows);
        return QueryResult.select(resultColumns, resultRows);
    }

    /**
     * Evaluate an expression containing window functions.
     * Returns an array of computed values, one per input context row.
     */
    Object[] evaluateWindowExpression(Expression expr, List<RowContext> contexts,
                                       List<SelectStmt.WindowDef> windowDefs) {
        if (expr instanceof WindowFuncExpr) {
            WindowFuncExpr wf = (WindowFuncExpr) expr;
            return evaluateWindowFunction(resolveNamedWindow(wf, windowDefs), contexts);
        }
        List<WindowFuncExpr> windowNodes = new ArrayList<>();
        collectWindowFunctions(expr, windowNodes);
        if (windowNodes.isEmpty()) {
            Object[] results = new Object[contexts.size()];
            for (int i = 0; i < contexts.size(); i++) {
                results[i] = executor.evalExpr(expr, contexts.get(i));
            }
            return results;
        }
        java.util.IdentityHashMap<WindowFuncExpr, Object[]> precomputed = new java.util.IdentityHashMap<>();
        for (WindowFuncExpr wf : windowNodes) {
            precomputed.put(wf, evaluateWindowFunction(resolveNamedWindow(wf, windowDefs), contexts));
        }
        Object[] results = new Object[contexts.size()];
        for (int i = 0; i < contexts.size(); i++) {
            results[i] = evalWithWindowValues(expr, contexts.get(i), precomputed, i);
        }
        return results;
    }

    private void collectWindowFunctions(Expression expr, List<WindowFuncExpr> out) {
        if (expr instanceof WindowFuncExpr) {
            WindowFuncExpr wf = (WindowFuncExpr) expr;
            out.add(wf);
        } else if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            collectWindowFunctions(bin.left(), out);
            collectWindowFunctions(bin.right(), out);
        } else if (expr instanceof CustomOperatorExpr) {
            CustomOperatorExpr cop = (CustomOperatorExpr) expr;
            if (cop.left() != null) collectWindowFunctions(cop.left(), out);
            collectWindowFunctions(cop.right(), out);
        } else if (expr instanceof UnaryExpr) {
            UnaryExpr un = (UnaryExpr) expr;
            collectWindowFunctions(un.operand(), out);
        } else if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            collectWindowFunctions(cast.expr(), out);
        } else if (expr instanceof CaseExpr) {
            CaseExpr c = (CaseExpr) expr;
            for (CaseExpr.WhenClause when : c.whenClauses()) {
                collectWindowFunctions(when.condition(), out);
                collectWindowFunctions(when.result(), out);
            }
            if (c.elseExpr() != null) collectWindowFunctions(c.elseExpr(), out);
        } else if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            for (Expression arg : fn.args()) collectWindowFunctions(arg, out);
        }
    }

    private Object evalWithWindowValues(Expression expr, RowContext ctx,
                                         java.util.IdentityHashMap<WindowFuncExpr, Object[]> precomputed, int rowIndex) {
        if (expr instanceof WindowFuncExpr) {
            WindowFuncExpr wf = (WindowFuncExpr) expr;
            Object[] vals = precomputed.get(wf);
            return vals != null ? vals[rowIndex] : null;
        }
        if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            if (select.containsWindowFunction(bin.left()) || select.containsWindowFunction(bin.right())) {
                Object left = select.containsWindowFunction(bin.left())
                        ? evalWithWindowValues(bin.left(), ctx, precomputed, rowIndex)
                        : executor.evalExpr(bin.left(), ctx);
                Object right = select.containsWindowFunction(bin.right())
                        ? evalWithWindowValues(bin.right(), ctx, precomputed, rowIndex)
                        : executor.evalExpr(bin.right(), ctx);
                return executor.evalBinaryValues(bin.op(), left, right);
            }
            return executor.evalExpr(expr, ctx);
        }
        if (expr instanceof CustomOperatorExpr) {
            CustomOperatorExpr cop = (CustomOperatorExpr) expr;
            boolean leftHasWindow = cop.left() != null && select.containsWindowFunction(cop.left());
            boolean rightHasWindow = select.containsWindowFunction(cop.right());
            if (leftHasWindow || rightHasWindow) {
                // Recurse into children, then delegate to normal eval with resolved values
                return executor.evalExpr(expr, ctx);
            }
            return executor.evalExpr(expr, ctx);
        }
        if (expr instanceof UnaryExpr) {
            UnaryExpr un = (UnaryExpr) expr;
            Object val = select.containsWindowFunction(un.operand())
                    ? evalWithWindowValues(un.operand(), ctx, precomputed, rowIndex)
                    : executor.evalExpr(un.operand(), ctx);
            return executor.evalUnaryValue(un.op(), val);
        }
        if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            Object val = select.containsWindowFunction(cast.expr())
                    ? evalWithWindowValues(cast.expr(), ctx, precomputed, rowIndex)
                    : executor.evalExpr(cast.expr(), ctx);
            return executor.castEvaluator.applyCast(val, cast.typeName());
        }
        if (expr instanceof CaseExpr) {
            CaseExpr c = (CaseExpr) expr;
            Expression testExpr = c.operand();
            Object testVal = testExpr != null ? executor.evalExpr(testExpr, ctx) : null;
            for (CaseExpr.WhenClause when : c.whenClauses()) {
                Object condVal;
                if (testExpr != null) {
                    Object whenVal = select.containsWindowFunction(when.condition())
                            ? evalWithWindowValues(when.condition(), ctx, precomputed, rowIndex)
                            : executor.evalExpr(when.condition(), ctx);
                    condVal = executor.compareValues(testVal, whenVal) == 0 ? Boolean.TRUE : Boolean.FALSE;
                } else {
                    condVal = select.containsWindowFunction(when.condition())
                            ? evalWithWindowValues(when.condition(), ctx, precomputed, rowIndex)
                            : executor.evalExpr(when.condition(), ctx);
                }
                if (executor.isTruthy(condVal)) {
                    return select.containsWindowFunction(when.result())
                            ? evalWithWindowValues(when.result(), ctx, precomputed, rowIndex)
                            : executor.evalExpr(when.result(), ctx);
                }
            }
            if (c.elseExpr() != null) {
                return select.containsWindowFunction(c.elseExpr())
                        ? evalWithWindowValues(c.elseExpr(), ctx, precomputed, rowIndex)
                        : executor.evalExpr(c.elseExpr(), ctx);
            }
            return null;
        }
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            boolean hasWindowArg = fn.args().stream().anyMatch(select::containsWindowFunction);
            if (hasWindowArg) {
                List<Expression> resolvedArgs = new ArrayList<>();
                for (Expression arg : fn.args()) {
                    Object val = select.containsWindowFunction(arg)
                            ? evalWithWindowValues(arg, ctx, precomputed, rowIndex)
                            : executor.evalExpr(arg, ctx);
                    resolvedArgs.add(val == null ? Literal.ofNull() : Literal.ofString(val.toString()));
                }
                return executor.functionEvaluator.evalFunction(
                        new FunctionCallExpr(fn.name(), resolvedArgs, fn.distinct(), fn.star()), ctx);
            }
        }
        return executor.evalExpr(expr, ctx);
    }

    private WindowFuncExpr resolveNamedWindow(WindowFuncExpr wf, List<SelectStmt.WindowDef> windowDefs) {
        if (wf.windowName() == null || windowDefs == null) return wf;
        String winName = wf.windowName().toLowerCase();
        for (SelectStmt.WindowDef def : windowDefs) {
            if (def.name().equalsIgnoreCase(winName)) {
                return new WindowFuncExpr(wf.name(), wf.args(), wf.distinct(), wf.star(),
                        def.partitionBy(), def.orderBy(), def.frame(), null);
            }
        }
        throw new RuntimeException("Window \"" + wf.windowName() + "\" is not defined");
    }

    private Object[] evaluateWindowFunction(WindowFuncExpr wf, List<RowContext> contexts) {
        int n = contexts.size();
        Object[] results = new Object[n];
        String funcName = wf.name().toLowerCase();

        List<List<Integer>> partitions = partitionRows(wf.partitionBy(), contexts);

        for (List<Integer> partition : partitions) {
            List<Integer> sortedPartition = new ArrayList<>(partition);
            if (wf.orderBy() != null && !wf.orderBy().isEmpty()) {
                sortedPartition.sort((a, b) -> {
                    for (SelectStmt.OrderByItem item : wf.orderBy()) {
                        Object va = executor.evalExpr(item.expr(), contexts.get(a));
                        Object vb = executor.evalExpr(item.expr(), contexts.get(b));
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

            switch (funcName) {
                case "row_number": {
                    for (int i = 0; i < sortedPartition.size(); i++) {
                        results[sortedPartition.get(i)] = (long) (i + 1);
                    }
                    break;
                }
                case "rank": {
                    for (int i = 0; i < sortedPartition.size(); i++) {
                        if (i == 0) {
                            results[sortedPartition.get(i)] = 1L;
                        } else {
                            boolean same = orderByValuesEqual(wf.orderBy(), contexts,
                                    sortedPartition.get(i), sortedPartition.get(i - 1));
                            if (same) {
                                results[sortedPartition.get(i)] = results[sortedPartition.get(i - 1)];
                            } else {
                                results[sortedPartition.get(i)] = (long) (i + 1);
                            }
                        }
                    }
                    break;
                }
                case "dense_rank": {
                    long rank = 1;
                    for (int i = 0; i < sortedPartition.size(); i++) {
                        if (i > 0) {
                            boolean same = orderByValuesEqual(wf.orderBy(), contexts,
                                    sortedPartition.get(i), sortedPartition.get(i - 1));
                            if (!same) rank++;
                        }
                        results[sortedPartition.get(i)] = rank;
                    }
                    break;
                }
                case "percent_rank": {
                    int partSize = sortedPartition.size();
                    if (partSize <= 1) {
                        for (int idx : sortedPartition) {
                            results[idx] = 0.0;
                        }
                    } else {
                        long[] ranks = new long[partSize];
                        ranks[0] = 1;
                        for (int i = 1; i < partSize; i++) {
                            boolean same = orderByValuesEqual(wf.orderBy(), contexts,
                                    sortedPartition.get(i), sortedPartition.get(i - 1));
                            ranks[i] = same ? ranks[i - 1] : (long) (i + 1);
                        }
                        for (int i = 0; i < partSize; i++) {
                            results[sortedPartition.get(i)] = (double) (ranks[i] - 1) / (double) (partSize - 1);
                        }
                    }
                    break;
                }
                case "cume_dist": {
                    int partSize = sortedPartition.size();
                    for (int i = 0; i < partSize; i++) {
                        int lastEqualIdx = i;
                        while (lastEqualIdx + 1 < partSize &&
                                orderByValuesEqual(wf.orderBy(), contexts,
                                        sortedPartition.get(lastEqualIdx + 1), sortedPartition.get(i))) {
                            lastEqualIdx++;
                        }
                        results[sortedPartition.get(i)] = (double) (lastEqualIdx + 1) / (double) partSize;
                    }
                    break;
                }
                case "ntile": {
                    int numBuckets = 1;
                    if (!wf.args().isEmpty()) {
                        numBuckets = executor.toInt(executor.evalExpr(wf.args().get(0), null));
                    }
                    if (numBuckets <= 0) {
                        throw new MemgresException("ntile requires a positive argument, found " + numBuckets, "22023");
                    }
                    int partSize = sortedPartition.size();
                    for (int i = 0; i < partSize; i++) {
                        long bucket = ((long) i * numBuckets) / partSize + 1;
                        results[sortedPartition.get(i)] = bucket;
                    }
                    break;
                }
                case "lag": {
                    if (wf.args().isEmpty()) {
                        throw new MemgresException("function lag() does not exist\n  Hint: No function matches the given name and argument types.", "42883");
                    }
                    int offset = 1;
                    Object defaultVal = null;
                    if (wf.args().size() > 1) offset = executor.toInt(executor.evalExpr(wf.args().get(1), null));
                    if (wf.args().size() > 2) defaultVal = executor.evalExpr(wf.args().get(2), null);
                    Expression arg = wf.args().get(0);
                    for (int i = 0; i < sortedPartition.size(); i++) {
                        if (i - offset >= 0) {
                            results[sortedPartition.get(i)] = executor.evalExpr(arg, contexts.get(sortedPartition.get(i - offset)));
                        } else {
                            results[sortedPartition.get(i)] = defaultVal;
                        }
                    }
                    break;
                }
                case "lead": {
                    if (wf.args().isEmpty()) {
                        throw new MemgresException("function lead() does not exist\n  Hint: No function matches the given name and argument types.", "42883");
                    }
                    int offset = 1;
                    Object defaultVal = null;
                    if (wf.args().size() > 1) offset = executor.toInt(executor.evalExpr(wf.args().get(1), null));
                    if (wf.args().size() > 2) defaultVal = executor.evalExpr(wf.args().get(2), null);
                    Expression arg = wf.args().get(0);
                    for (int i = 0; i < sortedPartition.size(); i++) {
                        if (i + offset < sortedPartition.size()) {
                            results[sortedPartition.get(i)] = executor.evalExpr(arg, contexts.get(sortedPartition.get(i + offset)));
                        } else {
                            results[sortedPartition.get(i)] = defaultVal;
                        }
                    }
                    break;
                }
                case "first_value": {
                    Expression arg = wf.args().get(0);
                    Object firstVal = sortedPartition.isEmpty() ? null :
                            executor.evalExpr(arg, contexts.get(sortedPartition.get(0)));
                    for (int idx : sortedPartition) {
                        results[idx] = firstVal;
                    }
                    break;
                }
                case "last_value": {
                    Expression arg = wf.args().get(0);
                    if (wf.frame() != null || wf.orderBy() == null || wf.orderBy().isEmpty()) {
                        Object lastVal = sortedPartition.isEmpty() ? null :
                                executor.evalExpr(arg, contexts.get(sortedPartition.get(sortedPartition.size() - 1)));
                        for (int idx : sortedPartition) {
                            results[idx] = lastVal;
                        }
                    } else {
                        for (int i = 0; i < sortedPartition.size(); i++) {
                            results[sortedPartition.get(i)] = executor.evalExpr(arg, contexts.get(sortedPartition.get(i)));
                        }
                    }
                    break;
                }
                case "nth_value": {
                    Expression arg = wf.args().get(0);
                    int nth = wf.args().size() > 1 ? executor.toInt(executor.evalExpr(wf.args().get(1), null)) : 1;
                    Object nthVal = (nth >= 1 && nth <= sortedPartition.size()) ?
                            executor.evalExpr(arg, contexts.get(sortedPartition.get(nth - 1))) : null;
                    for (int idx : sortedPartition) {
                        results[idx] = nthVal;
                    }
                    break;
                }
                default: {
                    if (select.isAggregateFunction(funcName)) {
                        evaluateAggregateWindowFunction(wf, funcName, contexts, sortedPartition, results);
                    }
                    break;
                }
            }
        }

        return results;
    }

    private void evaluateAggregateWindowFunction(WindowFuncExpr wf, String funcName,
                                                   List<RowContext> contexts,
                                                   List<Integer> sortedPartition,
                                                   Object[] results) {
        boolean hasOrderBy = wf.orderBy() != null && !wf.orderBy().isEmpty();
        boolean hasFrame = wf.frame() != null;

        for (int i = 0; i < sortedPartition.size(); i++) {
            int frameStart, frameEnd;

            if (hasFrame) {
                frameStart = resolveFrameBound(wf.frame().start(), i, sortedPartition.size());
                frameEnd = resolveFrameBound(wf.frame().end(), i, sortedPartition.size());
            } else if (hasOrderBy) {
                frameStart = 0;
                frameEnd = i;
            } else {
                frameStart = 0;
                frameEnd = sortedPartition.size() - 1;
            }

            frameStart = Math.max(0, frameStart);
            frameEnd = Math.min(sortedPartition.size() - 1, frameEnd);

            List<RowContext> frameRows = new ArrayList<>();
            WindowFuncExpr.ExcludeMode excludeMode = hasFrame && wf.frame().excludeMode() != null
                    ? wf.frame().excludeMode() : null;
            for (int fi = frameStart; fi <= frameEnd; fi++) {
                if (excludeMode != null) {
                    if (excludeMode == WindowFuncExpr.ExcludeMode.CURRENT_ROW && fi == i) continue;
                    if (excludeMode == WindowFuncExpr.ExcludeMode.GROUP || excludeMode == WindowFuncExpr.ExcludeMode.TIES) {
                        // Compare sort key of fi with sort key of current row i
                        boolean sameGroup = true;
                        if (hasOrderBy) {
                            for (SelectStmt.OrderByItem ob : wf.orderBy()) {
                                Object v1 = executor.evalExpr(ob.expr(), contexts.get(sortedPartition.get(fi)));
                                Object v2 = executor.evalExpr(ob.expr(), contexts.get(sortedPartition.get(i)));
                                if (!java.util.Objects.equals(v1, v2)) { sameGroup = false; break; }
                            }
                        }
                        if (sameGroup) {
                            if (excludeMode == WindowFuncExpr.ExcludeMode.GROUP) continue;
                            // TIES: exclude peers but keep current row
                            if (fi != i) continue;
                        }
                    }
                }
                frameRows.add(contexts.get(sortedPartition.get(fi)));
            }

            FunctionCallExpr fn = new FunctionCallExpr(funcName, wf.args(), wf.distinct(), wf.star());
            results[sortedPartition.get(i)] = select.aggregateEvaluator.evalAggregate(fn, frameRows);
        }
    }

    private int resolveFrameBound(WindowFuncExpr.FrameBound bound, int currentIdx, int partitionSize) {
        switch (bound.boundType()) {
            case UNBOUNDED_PRECEDING:
                return 0;
            case UNBOUNDED_FOLLOWING:
                return partitionSize - 1;
            case CURRENT_ROW:
                return currentIdx;
            case PRECEDING:
                return currentIdx - executor.toInt(executor.evalExpr(bound.offset(), null));
            case FOLLOWING:
                return currentIdx + executor.toInt(executor.evalExpr(bound.offset(), null));
            default:
                throw new IllegalStateException("Unknown bound type: " + bound.boundType());
        }
    }

    private List<List<Integer>> partitionRows(List<Expression> partitionBy, List<RowContext> contexts) {
        if (partitionBy == null || partitionBy.isEmpty()) {
            List<Integer> all = new ArrayList<>();
            for (int i = 0; i < contexts.size(); i++) all.add(i);
            return Cols.listOf(all);
        }

        Map<String, List<Integer>> partitionMap = new LinkedHashMap<>();
        for (int i = 0; i < contexts.size(); i++) {
            StringBuilder key = new StringBuilder();
            for (Expression expr : partitionBy) {
                Object val = executor.evalExpr(expr, contexts.get(i));
                key.append(val == null ? "\0NULL" : val.toString()).append('\1');
            }
            partitionMap.computeIfAbsent(key.toString(), k -> new ArrayList<>()).add(i);
        }
        return new ArrayList<>(partitionMap.values());
    }

    private boolean orderByValuesEqual(List<SelectStmt.OrderByItem> orderBy, List<RowContext> contexts,
                                        int idxA, int idxB) {
        if (orderBy == null || orderBy.isEmpty()) return true;
        for (SelectStmt.OrderByItem item : orderBy) {
            Object va = executor.evalExpr(item.expr(), contexts.get(idxA));
            Object vb = executor.evalExpr(item.expr(), contexts.get(idxB));
            if (va == null && vb == null) continue;
            if (va == null || vb == null) return false;
            if (executor.compareValues(va, vb) != 0) return false;
        }
        return true;
    }
}
