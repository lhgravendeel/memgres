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
                resultColumns.add(executor.buildResultColumn(alias, target.expr(), baseBindings));
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

        // ORDER BY. The row order is tracked as the input rows it came from, because DISTINCT ON
        // below has to read window values, and those are computed per input row.
        int[] rowOrder = new int[resultRows.size()];
        for (int i = 0; i < rowOrder.length; i++) rowOrder[i] = i;
        List<SelectStmt.OrderByItem> resolvedOrderBy = select.resolveOrderBy(stmt.orderBy(), stmt.targets());
        if (resolvedOrderBy != null && !resolvedOrderBy.isEmpty()) {
            // A window function ordered by rather than selected has no output column to read, and
            // it cannot be evaluated a row at a time either: it needs the whole partition. So it
            // is computed over the input rows here, in the order they were projected in.
            final java.util.IdentityHashMap<SelectStmt.OrderByItem, Object[]> orderByWindowValues =
                    new java.util.IdentityHashMap<>();
            for (SelectStmt.OrderByItem item : resolvedOrderBy) {
                if (select.containsWindowFunction(item.expr())
                        && select.resolveOrderByToColumnIndex(item.expr(), stmt.targets()) < 0) {
                    orderByWindowValues.put(item,
                            evaluateWindowExpression(item.expr(), contexts, stmt.windowDefs()));
                }
            }
            Integer[] indices = new Integer[resultRows.size()];
            for (int i = 0; i < indices.length; i++) indices[i] = i;
            final List<Object[]> finalResultRows = resultRows;
            java.util.Arrays.sort(indices, (ai, bi) -> {
                Object[] a = finalResultRows.get(ai);
                Object[] b = finalResultRows.get(bi);
                for (SelectStmt.OrderByItem item : resolvedOrderBy) {
                    int colIdx = select.resolveOrderByToColumnIndex(item.expr(), stmt.targets());
                    Object[] windowValues = orderByWindowValues.get(item);
                    Object va, vb;
                    if (colIdx >= 0) {
                        va = a[colIdx]; vb = b[colIdx];
                    } else if (windowValues != null) {
                        va = windowValues[ai]; vb = windowValues[bi];
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
            for (int i = 0; i < indices.length; i++) {
                sorted.add(finalResultRows.get(indices[i]));
                rowOrder[i] = indices[i];
            }
            resultRows = sorted;
        }

        resultRows = applyDistinctOn(stmt, resultRows, contexts, rowOrder);
        resultRows = select.applyDistinct(stmt, resultRows);
        resultRows = select.applyOffsetAndLimit(stmt, resultRows);
        return QueryResult.select(resultColumns, resultRows);
    }

    /**
     * DISTINCT ON over a query with window functions. A window function in the key has one value
     * per input row of the whole partition, so it is computed over the input rows and then read
     * in the order the result ended up in — evaluating it a row at a time gives every row the
     * same value and collapses the result to one row.
     */
    private List<Object[]> applyDistinctOn(SelectStmt stmt, List<Object[]> rows,
                                           List<RowContext> contexts, int[] rowOrder) {
        if (stmt.distinctOn() == null || stmt.distinctOn().isEmpty()) return rows;
        List<Expression> keys = stmt.distinctOn();
        Object[][] windowValues = new Object[keys.size()][];
        for (int ki = 0; ki < keys.size(); ki++) {
            if (select.containsWindowFunction(keys.get(ki))) {
                windowValues[ki] = evaluateWindowExpression(keys.get(ki), contexts, stmt.windowDefs());
            }
        }
        Set<String> seen = new LinkedHashSet<>();
        List<Object[]> kept = new ArrayList<>();
        for (int ri = 0; ri < rows.size(); ri++) {
            int source = rowOrder[ri];
            StringBuilder key = new StringBuilder();
            for (int ki = 0; ki < keys.size(); ki++) {
                Object value = windowValues[ki] != null
                        ? windowValues[ki][source]
                        : executor.evalExpr(keys.get(ki), contexts.get(source));
                key.append(value == null ? "\0NULL" : RowKey.valueKey(value)).append('\1');
            }
            if (seen.add(key.toString())) kept.add(rows.get(ri));
        }
        return kept;
    }

    /**
     * A window function in ORDER BY has to be computed over the whole result before the rows can
     * be put in order by it, so a query with one there takes the window path even when its select
     * list has none. DISTINCT and set-returning targets are left to the ordinary path, which has
     * rules of its own for them that this path does not carry.
     */
    boolean orderByNeedsWindowEvaluation(SelectStmt stmt) {
        if (stmt.distinct() || stmt.orderBy() == null || stmt.orderBy().isEmpty()) return false;
        for (SelectStmt.SelectTarget target : stmt.targets()) {
            if (SelectExecutor.containsSrf(target.expr())) return false;
        }
        for (SelectStmt.OrderByItem item : stmt.orderBy()) {
            if (select.containsWindowFunction(item.expr())) return true;
        }
        return false;
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
        } else if (expr instanceof IsNullExpr) {
            collectWindowFunctions(((IsNullExpr) expr).expr(), out);
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
        if (expr instanceof IsNullExpr) {
            // Without this the test fell through to ordinary evaluation, where the window call
            // has no value and every row answered "IS NULL" -- including the rows that have one.
            IsNullExpr isn = (IsNullExpr) expr;
            Object val = select.containsWindowFunction(isn.expr())
                    ? evalWithWindowValues(isn.expr(), ctx, precomputed, rowIndex)
                    : executor.evalExpr(isn.expr(), ctx);
            return isn.negated() ? val != null : val == null;
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
                List<RowContext.TableBinding> argScope = ctx != null
                        ? ctx.getBindings() : new ArrayList<RowContext.TableBinding>();
                for (Expression arg : fn.args()) {
                    Object val = select.containsWindowFunction(arg)
                            ? evalWithWindowValues(arg, ctx, precomputed, rowIndex)
                            : executor.evalExpr(arg, ctx);
                    // The window value keeps the type its own expression has. Rendering it as a
                    // string literal handed the enclosing call PostgreSQL's "unknown", so
                    // pg_typeof(sum(x) OVER ()) answered unknown and anything computed from one
                    // was resolved as text.
                    resolvedArgs.add(new ExprEvaluator.PrecomputedValueExpr(val,
                            executor.exprEvaluator.inferTypeFromContext(arg, argScope)));
                }
                return executor.functionEvaluator.evalFunction(
                        new FunctionCallExpr(fn.name(), resolvedArgs, fn.distinct(), fn.star()), ctx);
            }
        }
        return executor.evalExpr(expr, ctx);
    }

    /**
     * PostgreSQL settles a window specification when it analyses the statement, not when it
     * reaches a row: an undefined window name, a frame whose bounds cannot both be satisfied and
     * a window function in a clause that forbids one are all rejected before anything is read.
     * Checking here rather than in the evaluator means a meaningless specification is refused even
     * when the table is empty, and refused identically on every execution path.
     */
    void validateWindowUsage(SelectStmt stmt, List<RowContext.TableBinding> bindings) {
        // A window function written as a plain call is a window function all the same, and saying
        // so is more use than reporting a function nobody spelled wrong as missing. Only the
        // select list is judged here; the clauses PostgreSQL reads after WHERE are judged after
        // WHERE, by {@link #validateAfterWhere}.
        select.placementCheck.rejectWindowCallWithoutOverInTargets(stmt);
        for (SelectStmt.SelectTarget target : stmt.targets()) {
            validateCallPlacement(target.expr(), false);
        }
        validateCallPlacement(stmt.having(), false);
        if (stmt.orderBy() != null) {
            for (SelectStmt.OrderByItem item : stmt.orderBy()) validateCallPlacement(item.expr(), false);
        }
        List<SelectStmt.WindowDef> defs = stmt.windowDefs();
        List<WindowFuncExpr> calls = new ArrayList<>();
        for (SelectStmt.SelectTarget target : stmt.targets()) {
            collectWindowFunctionsDeep(target.expr(), calls);
        }
        if (stmt.orderBy() != null) {
            for (SelectStmt.OrderByItem item : stmt.orderBy()) collectWindowFunctionsDeep(item.expr(), calls);
        }
        if (calls.isEmpty() && (defs == null || defs.isEmpty())) return;

        // A named window is checked even when nothing references it, as PostgreSQL does. What it
        // partitions and orders by is read once per result row, so an aggregate there is ordinary;
        // only another window call has nothing to be numbered against yet.
        if (defs != null) {
            for (SelectStmt.WindowDef def : defs) {
                if (def.partitionBy() != null) {
                    for (Expression p : def.partitionBy()) {
                        select.placementCheck.rejectWindowCall(p, "window definitions");
                    }
                }
                if (def.orderBy() != null) {
                    for (SelectStmt.OrderByItem o : def.orderBy()) {
                        select.placementCheck.rejectWindowCall(o.expr(), "window definitions");
                    }
                }
                SelectStmt.WindowDef resolved = resolveWindowDef(def, defs);
                validateFrameOffsets(def.frame(), namedWindowCall(resolved), bindings);
            }
        }
        for (WindowFuncExpr wf : calls) {
            if (wf.distinct()) {
                throw PgErrors.notImplemented("DISTINCT is not implemented for window functions");
            }
            for (Expression arg : wf.args()) {
                if (select.containsWindowFunction(arg)) {
                    throw new MemgresException("window function calls cannot be nested", "42P20");
                }
            }
            if (wf.partitionBy() != null) {
                for (Expression p : wf.partitionBy()) {
                    if (select.containsWindowFunction(p)) throw windowInWindowDefinition();
                }
            }
            if (wf.orderBy() != null) {
                for (SelectStmt.OrderByItem o : wf.orderBy()) {
                    if (select.containsWindowFunction(o.expr())) throw windowInWindowDefinition();
                }
            }
            if (wf.filter() != null && !select.isAggregateFunction(wf.name())) {
                throw PgErrors.notImplemented(
                        "FILTER is not implemented for non-aggregate window functions");
            }
            validateFrame(resolveNamedWindow(wf, defs), bindings);
        }
    }

    /**
     * The clauses PostgreSQL analyses after WHERE, judged after WHERE.
     *
     * <p>Which clause a query is refused for is the clause PostgreSQL reaches first, and it reads
     * WHERE before HAVING, the window definitions, ORDER BY, GROUP BY and — last of all — LIMIT
     * and OFFSET. Running these ahead of the WHERE walk named LIMIT for
     * {@code WHERE count(*) > 1 LIMIT count(*)}, where PostgreSQL names WHERE.
     */
    void validateAfterWhere(SelectStmt stmt) {
        select.placementCheck.rejectWindowCallWithoutOverAfterWhere(stmt);
        // HAVING chooses which groups survive, and a window runs over the groups that did, so a
        // window function there would have to be computed before the rows it computes over exist.
        if (stmt.having() != null && select.containsWindowFunction(stmt.having())) {
            throw new MemgresException("window functions are not allowed in HAVING", "42P20");
        }
        if (stmt.groupBy() != null) {
            for (Expression g : stmt.groupBy()) {
                if (select.containsWindowFunction(g)) {
                    throw new MemgresException("window functions are not allowed in GROUP BY", "42P20");
                }
            }
        }
        // LIMIT and OFFSET are read once for the whole query, before any row has a position in a
        // window to be measured against and before any group has been collected to aggregate. A
        // sub-select there may of course aggregate: it is a query of its own with its own rows.
        select.placementCheck.reject(stmt.limit(), "LIMIT");
        select.placementCheck.reject(stmt.offset(), "OFFSET");
    }

    /** A stand-in call carrying a named window's ORDER BY, so its frame can be judged the same way. */
    private static WindowFuncExpr namedWindowCall(SelectStmt.WindowDef def) {
        return new WindowFuncExpr("row_number", Cols.<Expression>listOf(), false, false,
                def.partitionBy(), def.orderBy(), def.frame(), null, false, false, null, false);
    }

    private static MemgresException windowInWindowDefinition() {
        return new MemgresException("window functions are not allowed in window definitions", "42P20");
    }

    /**
     * A FILTER condition is tested per input row, before there is a group to aggregate or a
     * frame to number, and an aggregate reads its arguments per input row for the same reason.
     * Neither can therefore contain a call that only has a value once the rows have been
     * collected, so PostgreSQL refuses the shape rather than choosing an order for it.
     *
     * <p>{@code insideAggregateArgs} says whether the walk is already inside what an aggregate
     * reads. An aggregate there would have to be folded before the aggregate around it, over the
     * same rows, and there is no such second pass: {@code sum(sum(v))} is rejected. A window
     * function resets the flag, because what a window reads is not an input row but a row of the
     * already-grouped result — {@code sum(sum(v)) OVER ()} sums one value per group, and the
     * inner call is this query's own aggregate rather than a nested one. Two levels are still
     * nested, so {@code sum(sum(sum(v))) OVER ()} and {@code rank() OVER (ORDER BY sum(sum(v)))}
     * are rejected as before.
     */
    private void validateCallPlacement(Expression expr, boolean insideAggregateArgs) {
        if (expr == null) return;
        if (expr instanceof WindowFuncExpr) {
            WindowFuncExpr wf = (WindowFuncExpr) expr;
            rejectMisplacedCallsInFilter(wf.filter());
            for (Expression arg : wf.args()) validateCallPlacement(arg, false);
            if (wf.partitionBy() != null) {
                for (Expression p : wf.partitionBy()) validateCallPlacement(p, false);
            }
            if (wf.orderBy() != null) {
                for (SelectStmt.OrderByItem o : wf.orderBy()) validateCallPlacement(o.expr(), false);
            }
            return;
        }
        if (expr instanceof OrderedSetAggExpr) {
            OrderedSetAggExpr osa = (OrderedSetAggExpr) expr;
            if (insideAggregateArgs) throw nestedAggregate();
            for (Expression arg : osa.args()) validateCallPlacement(arg, true);
            if (osa.withinGroupOrderBy() != null) {
                for (SelectStmt.OrderByItem o : osa.withinGroupOrderBy()) {
                    validateCallPlacement(o.expr(), true);
                }
            }
            return;
        }
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            rejectMisplacedCallsInFilter(fn.filter());
            if (select.isAggregateFunction(fn.name())) {
                for (Expression arg : fn.args()) {
                    if (select.containsWindowFunction(arg)) throw windowUnderAggregate();
                }
                if (fn.orderBy() != null) {
                    for (SelectStmt.OrderByItem o : fn.orderBy()) {
                        if (select.containsWindowFunction(o.expr())) throw windowUnderAggregate();
                    }
                }
                if (insideAggregateArgs) throw nestedAggregate();
                for (Expression arg : fn.args()) validateCallPlacement(arg, true);
                if (fn.orderBy() != null) {
                    for (SelectStmt.OrderByItem o : fn.orderBy()) validateCallPlacement(o.expr(), true);
                }
                return;
            }
            for (Expression arg : fn.args()) validateCallPlacement(arg, insideAggregateArgs);
            return;
        }
        if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            validateCallPlacement(bin.left(), insideAggregateArgs);
            validateCallPlacement(bin.right(), insideAggregateArgs);
        } else if (expr instanceof CustomOperatorExpr) {
            CustomOperatorExpr cop = (CustomOperatorExpr) expr;
            validateCallPlacement(cop.left(), insideAggregateArgs);
            validateCallPlacement(cop.right(), insideAggregateArgs);
        } else if (expr instanceof UnaryExpr) {
            validateCallPlacement(((UnaryExpr) expr).operand(), insideAggregateArgs);
        } else if (expr instanceof CastExpr) {
            validateCallPlacement(((CastExpr) expr).expr(), insideAggregateArgs);
        } else if (expr instanceof IsNullExpr) {
            validateCallPlacement(((IsNullExpr) expr).expr(), insideAggregateArgs);
        } else if (expr instanceof IsJsonExpr) {
            validateCallPlacement(((IsJsonExpr) expr).expr(), insideAggregateArgs);
        } else if (expr instanceof InExpr) {
            validateCallPlacement(((InExpr) expr).expr(), insideAggregateArgs);
        } else if (expr instanceof LikeExpr) {
            LikeExpr like = (LikeExpr) expr;
            validateCallPlacement(like.left(), insideAggregateArgs);
            validateCallPlacement(like.pattern(), insideAggregateArgs);
        } else if (expr instanceof CaseExpr) {
            CaseExpr c = (CaseExpr) expr;
            validateCallPlacement(c.operand(), insideAggregateArgs);
            for (CaseExpr.WhenClause when : c.whenClauses()) {
                validateCallPlacement(when.condition(), insideAggregateArgs);
                validateCallPlacement(when.result(), insideAggregateArgs);
            }
            validateCallPlacement(c.elseExpr(), insideAggregateArgs);
        }
    }

    private static MemgresException windowUnderAggregate() {
        return new MemgresException(
                "aggregate function calls cannot contain window function calls", "42803");
    }

    private static MemgresException nestedAggregate() {
        return new MemgresException("aggregate function calls cannot be nested", "42803");
    }

    /**
     * The parts of an expression that a window function does not compute for itself: its
     * arguments, what it partitions and orders by, its FILTER condition, and whatever stands
     * beside it. Over a grouped query each of those is a value of the grouped row rather than of
     * an input row — {@code sum(v)} is one number per group, {@code g} is the group's key — so
     * the caller evaluates them once per group and binds them to these very nodes. That is what
     * makes an aggregate legal under a window here, and it is also why a window's ORDER BY no
     * longer has to find its expression among the grouped result's columns, which are the output
     * aliases and carry neither {@code sum(v)} nor a grouping column the select list omits.
     */
    void collectGroupedWindowInputs(Expression expr, List<SelectStmt.WindowDef> defs,
                                     List<Expression> out) {
        if (expr == null) return;
        if (!select.containsWindowFunction(expr)) {
            out.add(expr);
            return;
        }
        if (expr instanceof WindowFuncExpr) {
            WindowFuncExpr wf = resolveNamedWindow((WindowFuncExpr) expr, defs);
            out.addAll(wf.args());
            if (wf.partitionBy() != null) out.addAll(wf.partitionBy());
            if (wf.orderBy() != null) {
                for (SelectStmt.OrderByItem o : wf.orderBy()) out.add(o.expr());
            }
            if (wf.filter() != null) out.add(wf.filter());
            return;
        }
        if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            collectGroupedWindowInputs(bin.left(), defs, out);
            collectGroupedWindowInputs(bin.right(), defs, out);
        } else if (expr instanceof CustomOperatorExpr) {
            CustomOperatorExpr cop = (CustomOperatorExpr) expr;
            collectGroupedWindowInputs(cop.left(), defs, out);
            collectGroupedWindowInputs(cop.right(), defs, out);
        } else if (expr instanceof UnaryExpr) {
            collectGroupedWindowInputs(((UnaryExpr) expr).operand(), defs, out);
        } else if (expr instanceof CastExpr) {
            collectGroupedWindowInputs(((CastExpr) expr).expr(), defs, out);
        } else if (expr instanceof CaseExpr) {
            CaseExpr c = (CaseExpr) expr;
            collectGroupedWindowInputs(c.operand(), defs, out);
            for (CaseExpr.WhenClause when : c.whenClauses()) {
                collectGroupedWindowInputs(when.condition(), defs, out);
                collectGroupedWindowInputs(when.result(), defs, out);
            }
            collectGroupedWindowInputs(c.elseExpr(), defs, out);
        } else if (expr instanceof FunctionCallExpr) {
            for (Expression arg : ((FunctionCallExpr) expr).args()) {
                collectGroupedWindowInputs(arg, defs, out);
            }
        }
    }

    private void rejectMisplacedCallsInFilter(Expression filter) {
        select.placementCheck.reject(filter, "FILTER");
    }

    /** Collect every window function in an expression, including ones nested inside another. */
    private void collectWindowFunctionsDeep(Expression expr, List<WindowFuncExpr> out) {
        if (expr instanceof WindowFuncExpr) {
            WindowFuncExpr wf = (WindowFuncExpr) expr;
            out.add(wf);
            for (Expression arg : wf.args()) collectWindowFunctionsDeep(arg, out);
            if (wf.partitionBy() != null) {
                for (Expression p : wf.partitionBy()) collectWindowFunctionsDeep(p, out);
            }
            if (wf.orderBy() != null) {
                for (SelectStmt.OrderByItem o : wf.orderBy()) collectWindowFunctionsDeep(o.expr(), out);
            }
            if (wf.filter() != null) collectWindowFunctionsDeep(wf.filter(), out);
            return;
        }
        collectWindowFunctions(expr, out);
    }

    /**
     * Check a frame clause against the ORDER BY it will run under. RANGE and GROUPS offsets are
     * measured in sort-key values rather than row positions, so they need a sort key of the right
     * shape; and an offset must be a non-negative, non-NULL size.
     */
    private void validateFrame(WindowFuncExpr wf, List<RowContext.TableBinding> bindings) {
        WindowFuncExpr.FrameClause frame = wf.frame();
        if (frame == null) return;
        validateFrameOffsets(frame, wf, bindings);
        int orderByCount = wf.orderBy() == null ? 0 : wf.orderBy().size();
        boolean hasOffsetBound = isOffsetBound(frame.start()) || isOffsetBound(frame.end());
        if (frame.type() == WindowFuncExpr.FrameType.GROUPS && orderByCount == 0) {
            throw new MemgresException("GROUPS mode requires an ORDER BY clause", "42P20");
        }
        if (frame.type() == WindowFuncExpr.FrameType.RANGE && hasOffsetBound && orderByCount != 1) {
            throw new MemgresException(
                    "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column", "42P20");
        }
        checkFrameOffset(frame, frame.start(), true);
        checkFrameOffset(frame, frame.end(), false);
    }

    /**
     * A frame offset is one size for the whole window, so it is read once before any row is
     * placed in a frame: it may not depend on the row (a column reference), on the group
     * (an aggregate) or on the framing itself (a window function).
     */
    private void validateFrameOffsets(WindowFuncExpr.FrameClause frame, WindowFuncExpr wf,
                                      List<RowContext.TableBinding> bindings) {
        if (frame == null) return;
        validateFrameOffset(frame.start(), frame.type(), wf, bindings);
        validateFrameOffset(frame.end(), frame.type(), wf, bindings);
    }

    private void validateFrameOffset(WindowFuncExpr.FrameBound bound, WindowFuncExpr.FrameType type,
                                     WindowFuncExpr wf, List<RowContext.TableBinding> bindings) {
        if (!isOffsetBound(bound) || bound.offset() == null) return;
        Expression offset = bound.offset();
        if (select.containsWindowFunction(offset)) throw windowInWindowDefinition();
        select.placementCheck.reject(offset, "window " + type);
        // PostgreSQL resolves the offset's type before it asks whether the offset is constant,
        // so an offset of the wrong type is that error and not "must not contain variables".
        checkOffsetType(offset, type, wf, bindings);
        if (referencesColumn(offset)) {
            throw new MemgresException(
                    "argument of " + type + " must not contain variables", "42P10");
        }
    }

    /**
     * The type a frame offset has to have. ROWS and GROUPS count rows, so their offset is a
     * bigint whatever the query is ordered by; RANGE measures in the ordering column's own type,
     * so the offset is resolved against that column and the pair either works or does not.
     *
     * <p>Both are decided here, before any row is read, because that is where PostgreSQL decides
     * them: a query that never produces a row still has an offset of the wrong type.
     */
    private void checkOffsetType(Expression offset, WindowFuncExpr.FrameType type,
                                 WindowFuncExpr wf, List<RowContext.TableBinding> bindings) {
        if (type == WindowFuncExpr.FrameType.RANGE) {
            DataType orderType = orderingColumnType(wf, bindings);
            if (orderType == null) return;
            if (!supportsRangeOffset(orderType)) {
                throw PgErrors.notImplemented("RANGE with offset PRECEDING/FOLLOWING is not"
                        + " supported for column type " + pgTypeName(orderType));
            }
            String unknown = unknownLiteral(offset);
            if (unknown != null) {
                parseAs(unknown, orderType);
                return;
            }
            DataType offsetType = staticType(offset, bindings);
            if (offsetType != null && !compatibleRangeOffset(orderType, offsetType)) {
                MemgresException e = PgErrors.notImplemented(
                        "RANGE with offset PRECEDING/FOLLOWING is not supported for column type "
                                + pgTypeName(orderType) + " and offset type " + pgTypeName(offsetType));
                e.setHint("Cast the offset value to an appropriate type.");
                throw e;
            }
            return;
        }
        String unknown = unknownLiteral(offset);
        if (unknown != null) {
            parseAs(unknown, DataType.BIGINT);
            return;
        }
        DataType offsetType = staticType(offset, bindings);
        if (offsetType != null && !isNumeric(offsetType)) {
            throw new MemgresException("argument of " + type + " must be type bigint, not type "
                    + pgTypeName(offsetType), "42804");
        }
    }

    /** The literal text of an offset written as a quoted constant, which carries no type of its own. */
    private static String unknownLiteral(Expression offset) {
        if (!(offset instanceof Literal)) return null;
        Literal literal = (Literal) offset;
        return literal.literalType() == Literal.LiteralType.STRING ? literal.value() : null;
    }

    /** Read an untyped constant as the type the frame needs, reporting what PostgreSQL reports. */
    private static void parseAs(String text, DataType type) {
        try {
            if (type == DataType.NUMERIC
                    || type == DataType.REAL || type == DataType.DOUBLE_PRECISION) {
                new java.math.BigDecimal(text.trim());
            } else {
                Long.parseLong(text.trim());
            }
        } catch (NumberFormatException e) {
            throw new MemgresException(
                    "invalid input syntax for type " + pgTypeName(type) + ": \"" + text + "\"", "22P02");
        }
    }

    /** The declared type of an offset that is a plain column, or null when it is anything else. */
    private static DataType staticType(Expression offset, List<RowContext.TableBinding> bindings) {
        if (!(offset instanceof ColumnRef) || bindings == null) return null;
        ColumnRef ref = (ColumnRef) offset;
        for (RowContext.TableBinding binding : bindings) {
            Table table = binding.table();
            if (table == null) continue;
            if (ref.table() != null && !ref.table().equalsIgnoreCase(binding.alias())
                    && !ref.table().equalsIgnoreCase(table.getName())) {
                continue;
            }
            int index = table.getColumnIndex(ref.column());
            if (index >= 0) return table.getColumns().get(index).getType();
        }
        return null;
    }

    private static DataType orderingColumnType(WindowFuncExpr wf, List<RowContext.TableBinding> bindings) {
        if (wf.orderBy() == null || wf.orderBy().size() != 1) return null;
        return staticType(wf.orderBy().get(0).expr(), bindings);
    }

    private static boolean isNumeric(DataType type) {
        return type == DataType.SMALLINT || type == DataType.INTEGER || type == DataType.BIGINT
                || type == DataType.NUMERIC
                || type == DataType.REAL || type == DataType.DOUBLE_PRECISION;
    }

    private static boolean isIntegral(DataType type) {
        return type == DataType.SMALLINT || type == DataType.INTEGER || type == DataType.BIGINT;
    }

    private static boolean supportsRangeOffset(DataType type) {
        return isNumeric(type) || type == DataType.DATE || type == DataType.TIMESTAMP
                || type == DataType.TIMESTAMPTZ || type == DataType.TIME || type == DataType.TIMETZ
                || type == DataType.INTERVAL;
    }

    private static boolean compatibleRangeOffset(DataType orderType, DataType offsetType) {
        if (isIntegral(orderType)) return isIntegral(offsetType);
        if (isNumeric(orderType)) return isNumeric(offsetType);
        return true;
    }

    /**
     * True when the expression reads a column of the query's own rows. A sub-select reads its
     * own rows instead, so it is a constant as far as the enclosing frame is concerned.
     */
    private static boolean referencesColumn(Expression expr) {
        if (expr == null) return false;
        if (expr instanceof SubqueryExpr || expr instanceof ExistsExpr
                || expr instanceof AnyAllExpr || expr instanceof ArraySubqueryExpr) {
            return false;
        }
        if (expr instanceof ColumnRef) return true;
        if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            return referencesColumn(bin.left()) || referencesColumn(bin.right());
        }
        if (expr instanceof CustomOperatorExpr) {
            CustomOperatorExpr cop = (CustomOperatorExpr) expr;
            return referencesColumn(cop.left()) || referencesColumn(cop.right());
        }
        if (expr instanceof UnaryExpr) return referencesColumn(((UnaryExpr) expr).operand());
        if (expr instanceof CastExpr) return referencesColumn(((CastExpr) expr).expr());
        if (expr instanceof CaseExpr) {
            CaseExpr c = (CaseExpr) expr;
            if (referencesColumn(c.operand()) || referencesColumn(c.elseExpr())) return true;
            for (CaseExpr.WhenClause when : c.whenClauses()) {
                if (referencesColumn(when.condition()) || referencesColumn(when.result())) return true;
            }
            return false;
        }
        if (expr instanceof FunctionCallExpr) {
            for (Expression arg : ((FunctionCallExpr) expr).args()) {
                if (referencesColumn(arg)) return true;
            }
        }
        return false;
    }

    private static boolean isOffsetBound(WindowFuncExpr.FrameBound bound) {
        return bound != null && (bound.boundType() == WindowFuncExpr.FrameBoundType.PRECEDING
                || bound.boundType() == WindowFuncExpr.FrameBoundType.FOLLOWING);
    }

    private void checkFrameOffset(WindowFuncExpr.FrameClause frame, WindowFuncExpr.FrameBound bound,
                                   boolean isStartBound) {
        if (!isOffsetBound(bound)) return;
        Object value = executor.evalExpr(bound.offset(), null);
        if (value == null) {
            throw new MemgresException(
                    "frame " + (isStartBound ? "starting" : "ending") + " offset must not be null", "22004");
        }
        if (value instanceof Number && ((Number) value).doubleValue() < 0) {
            if (frame.type() == WindowFuncExpr.FrameType.RANGE) {
                throw new MemgresException(
                        "invalid preceding or following size in window function", "22013");
            }
            throw new MemgresException(
                    "frame " + (isStartBound ? "starting" : "ending") + " offset must not be negative", "22013");
        }
    }

    /**
     * Resolve {@code OVER w} / {@code OVER (w ...)} against the WINDOW clause.
     *
     * <p>The parenthesised form copies the named window, and a copy may not restate what the
     * original already fixed — PostgreSQL refuses rather than guessing which of the two wins.
     */
    private WindowFuncExpr resolveNamedWindow(WindowFuncExpr wf, List<SelectStmt.WindowDef> windowDefs) {
        if (wf.windowName() == null) return wf;
        SelectStmt.WindowDef base = resolveWindowDef(
                findWindowDef(wf.windowName(), windowDefs,
                        windowDefs == null ? 0 : windowDefs.size()), windowDefs);
        if (wf.copiedWindow()) {
            MemgresException e = rejectBadCopy(wf.windowName(), base,
                    wf.partitionBy(), wf.orderBy(), wf.frame());
            if (e != null) {
                if (wf.partitionBy() == null && wf.orderBy() == null && wf.frame() == null) {
                    e.setHint("Omit the parentheses in this OVER clause.");
                }
                throw e;
            }
        }
        return new WindowFuncExpr(wf.name(), wf.args(), wf.distinct(), wf.star(),
                wf.partitionBy() != null ? wf.partitionBy() : base.partitionBy(),
                wf.orderBy() != null ? wf.orderBy() : base.orderBy(),
                wf.frame() != null ? wf.frame() : base.frame(),
                null, wf.ignoreNulls(), wf.fromLast(), wf.filter(), false);
    }

    private static SelectStmt.WindowDef findWindowDef(String name, List<SelectStmt.WindowDef> windowDefs,
                                                      int limit) {
        if (windowDefs != null) {
            for (int i = 0; i < Math.min(limit, windowDefs.size()); i++) {
                if (windowDefs.get(i).name().equalsIgnoreCase(name)) return windowDefs.get(i);
            }
        }
        throw PgErrors.undefinedObject("window", name);
    }

    /** Flatten {@code WINDOW w2 AS (w1 ...)} into a single specification. */
    private SelectStmt.WindowDef resolveWindowDef(SelectStmt.WindowDef def,
                                                   List<SelectStmt.WindowDef> windowDefs) {
        if (def.refName() == null) return def;
        // A base window must already be defined at this point in the WINDOW clause, so the
        // chain cannot close on itself.
        int position = 0;
        while (position < windowDefs.size() && windowDefs.get(position) != def) position++;
        SelectStmt.WindowDef base = resolveWindowDef(
                findWindowDef(def.refName(), windowDefs, position), windowDefs);
        MemgresException e = rejectBadCopy(def.refName(), base,
                def.partitionBy(), def.orderBy(), def.frame());
        if (e != null) throw e;
        return new SelectStmt.WindowDef(def.name(), null,
                def.partitionBy() != null ? def.partitionBy() : base.partitionBy(),
                def.orderBy() != null ? def.orderBy() : base.orderBy(),
                def.frame() != null ? def.frame() : base.frame());
    }

    /** The three ways a copy of a named window is invalid, in the order PostgreSQL reports them. */
    private static MemgresException rejectBadCopy(String baseName, SelectStmt.WindowDef base,
                                                   List<Expression> partitionBy,
                                                   List<SelectStmt.OrderByItem> orderBy,
                                                   WindowFuncExpr.FrameClause frame) {
        if (partitionBy != null) {
            return new MemgresException(
                    "cannot override PARTITION BY clause of window \"" + baseName + "\"", "42P20");
        }
        if (orderBy != null && base.orderBy() != null && !base.orderBy().isEmpty()) {
            return new MemgresException(
                    "cannot override ORDER BY clause of window \"" + baseName + "\"", "42P20");
        }
        if (base.frame() != null) {
            return new MemgresException(
                    "cannot copy window \"" + baseName + "\" because it has a frame clause", "42P20");
        }
        return null;
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
                        Object raw = executor.evalExpr(wf.args().get(0), null);
                        if (raw == null) {
                            // PG: a NULL bucket count produces NULL for every row of the partition
                            for (int idx : sortedPartition) results[idx] = null;
                            break;
                        }
                        numBuckets = executor.toInt(raw);
                    }
                    if (numBuckets <= 0) {
                        throw new MemgresException("argument of ntile must be greater than zero", "22014");
                    }
                    int partSize = sortedPartition.size();
                    // PG ntile: each row gets bucket (i+1) when numBuckets >= partSize,
                    // otherwise first (partSize % numBuckets) buckets get ceil rows, rest get floor rows.
                    for (int i = 0; i < partSize; i++) {
                        long bucket;
                        if (numBuckets >= partSize) {
                            bucket = i + 1;
                        } else {
                            int largeBucketCount = partSize % numBuckets; // buckets with one extra row
                            int smallBucketSize = partSize / numBuckets;
                            int largeBucketSize = smallBucketSize + 1;
                            int largeBucketTotal = largeBucketCount * largeBucketSize;
                            if (i < largeBucketTotal) {
                                bucket = (i / largeBucketSize) + 1;
                            } else {
                                bucket = largeBucketCount + (i - largeBucketTotal) / smallBucketSize + 1;
                            }
                        }
                        results[sortedPartition.get(i)] = bucket;
                    }
                    break;
                }
                case "lag":
                case "lead": {
                    evaluateLeadLag(wf, funcName, contexts, sortedPartition, results);
                    break;
                }
                case "first_value": {
                    Expression arg = wf.args().get(0);
                    WindowFuncExpr.ExcludeMode fvExclude = wf.frame() != null ? wf.frame().excludeMode() : null;
                    for (int i = 0; i < sortedPartition.size(); i++) {
                        int[] bounds = resolveFrameBounds(wf, i, contexts, sortedPartition);
                        int frameStart = bounds[0];
                        int frameEnd = bounds[1];
                        Object val = null;
                        if (frameStart <= frameEnd) {
                            for (int fi = frameStart; fi <= frameEnd; fi++) {
                                if (fvExclude != null && fvExclude != WindowFuncExpr.ExcludeMode.NO_OTHERS
                                        && shouldExcludeRow(fvExclude, wf.orderBy(), contexts, sortedPartition, i, fi)) {
                                    continue;
                                }
                                Object v = executor.evalExpr(arg, contexts.get(sortedPartition.get(fi)));
                                if (wf.ignoreNulls() && v == null) continue;
                                val = v;
                                break;
                            }
                        }
                        results[sortedPartition.get(i)] = val;
                    }
                    break;
                }
                case "last_value": {
                    Expression arg = wf.args().get(0);
                    WindowFuncExpr.ExcludeMode lvExclude = wf.frame() != null ? wf.frame().excludeMode() : null;
                    for (int i = 0; i < sortedPartition.size(); i++) {
                        int[] bounds = resolveFrameBounds(wf, i, contexts, sortedPartition);
                        int frameStart = bounds[0];
                        int frameEnd = bounds[1];
                        Object val = null;
                        if (frameStart <= frameEnd) {
                            for (int fi = frameEnd; fi >= frameStart; fi--) {
                                if (lvExclude != null && lvExclude != WindowFuncExpr.ExcludeMode.NO_OTHERS
                                        && shouldExcludeRow(lvExclude, wf.orderBy(), contexts, sortedPartition, i, fi)) {
                                    continue;
                                }
                                Object v = executor.evalExpr(arg, contexts.get(sortedPartition.get(fi)));
                                if (wf.ignoreNulls() && v == null) continue;
                                val = v;
                                break;
                            }
                        }
                        results[sortedPartition.get(i)] = val;
                    }
                    break;
                }
                case "nth_value": {
                    Expression arg = wf.args().get(0);
                    int nth = 1;
                    if (wf.args().size() > 1) {
                        Object raw = executor.evalExpr(wf.args().get(1), null);
                        if (raw == null) {
                            // PG: a NULL position produces NULL for every row of the partition
                            for (int idx : sortedPartition) results[idx] = null;
                            break;
                        }
                        requireIntegralArgument(funcName, wf, raw, contexts);
                        nth = executor.toInt(raw);
                    }
                    // PG raises 22016 for nth <= 0
                    if (nth <= 0) {
                        throw new MemgresException(
                                "argument of nth_value must be greater than zero", "22016");
                    }
                    WindowFuncExpr.ExcludeMode nvExclude = wf.frame() != null ? wf.frame().excludeMode() : null;
                    for (int i = 0; i < sortedPartition.size(); i++) {
                        int[] bounds = resolveFrameBounds(wf, i, contexts, sortedPartition);
                        int frameStart = bounds[0];
                        int frameEnd = bounds[1];
                        if (frameStart > frameEnd) {
                            results[sortedPartition.get(i)] = null;
                        } else {
                            // Iterate frame rows, respecting EXCLUDE and IGNORE NULLS
                            int count = 0;
                            Object found = null;
                            if (wf.fromLast()) {
                                for (int fi = frameEnd; fi >= frameStart; fi--) {
                                    if (nvExclude != null && nvExclude != WindowFuncExpr.ExcludeMode.NO_OTHERS
                                            && shouldExcludeRow(nvExclude, wf.orderBy(), contexts, sortedPartition, i, fi)) {
                                        continue;
                                    }
                                    Object val = executor.evalExpr(arg, contexts.get(sortedPartition.get(fi)));
                                    if (wf.ignoreNulls() && val == null) continue;
                                    count++;
                                    if (count == nth) { found = val; break; }
                                }
                            } else {
                                for (int fi = frameStart; fi <= frameEnd; fi++) {
                                    if (nvExclude != null && nvExclude != WindowFuncExpr.ExcludeMode.NO_OTHERS
                                            && shouldExcludeRow(nvExclude, wf.orderBy(), contexts, sortedPartition, i, fi)) {
                                        continue;
                                    }
                                    Object val = executor.evalExpr(arg, contexts.get(sortedPartition.get(fi)));
                                    if (wf.ignoreNulls() && val == null) continue;
                                    count++;
                                    if (count == nth) { found = val; break; }
                                }
                            }
                            results[sortedPartition.get(i)] = found;
                        }
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

    /**
     * lag and lead are the same walk in opposite directions, and PostgreSQL lets the offset carry
     * the sign: {@code lag(v, -1)} looks forward exactly as {@code lead(v, 1)} does. Working in
     * {@code long} keeps an offset near the integer limits from wrapping into a valid index.
     */
    private void evaluateLeadLag(WindowFuncExpr wf, String funcName, List<RowContext> contexts,
                                  List<Integer> sortedPartition, Object[] results) {
        if (wf.args().isEmpty()) {
            MemgresException e = new MemgresException(
                    "function " + funcName + "() does not exist", "42883");
            e.setHint("No function matches the given name and argument types. "
                    + "You might need to add explicit type casts.");
            throw e;
        }
        Expression arg = wf.args().get(0);
        Object defaultVal = wf.args().size() > 2 ? executor.evalExpr(wf.args().get(2), null) : null;
        long step = 1;
        if (wf.args().size() > 1) {
            Object raw = executor.evalExpr(wf.args().get(1), null);
            if (raw == null) {
                // PG: a NULL offset produces NULL for every row, ignoring the default
                for (int idx : sortedPartition) results[idx] = null;
                return;
            }
            requireIntegralArgument(funcName, wf, raw, contexts);
            step = ((Number) raw).longValue();
        }
        if ("lag".equals(funcName)) step = -step;

        int size = sortedPartition.size();
        for (int i = 0; i < size; i++) {
            Object val;
            if (wf.ignoreNulls() && step != 0) {
                // IGNORE NULLS: walk in the offset's direction counting only non-null values
                val = defaultVal;
                long remaining = Math.abs(step);
                int dir = step > 0 ? 1 : -1;
                for (int j = i + dir; j >= 0 && j < size; j += dir) {
                    Object v = executor.evalExpr(arg, contexts.get(sortedPartition.get(j)));
                    if (v != null && --remaining == 0) { val = v; break; }
                }
            } else {
                long target = (long) i + step;
                val = (target >= 0 && target < size)
                        ? executor.evalExpr(arg, contexts.get(sortedPartition.get((int) target)))
                        : defaultVal;
            }
            results[sortedPartition.get(i)] = val;
        }
    }

    /**
     * The position arguments of lag, lead and nth_value are declared integer, so a fractional
     * value is not a value error but a call PostgreSQL has no function for.
     */
    private void requireIntegralArgument(String funcName, WindowFuncExpr wf, Object offset,
                                          List<RowContext> contexts) {
        if (!(offset instanceof java.math.BigDecimal) && !(offset instanceof Double)
                && !(offset instanceof Float)) {
            return;
        }
        StringBuilder sig = new StringBuilder(funcName).append('(');
        for (int i = 0; i < wf.args().size(); i++) {
            if (i > 0) sig.append(", ");
            Object v = contexts.isEmpty() ? null : executor.evalExpr(wf.args().get(i), contexts.get(0));
            sig.append(pgTypeName(v));
        }
        sig.append(')');
        MemgresException e = new MemgresException(
                "function " + sig + " does not exist", "42883");
        e.setHint("No function matches the given name and argument types. "
                + "You might need to add explicit type casts.");
        throw e;
    }

    private void evaluateAggregateWindowFunction(WindowFuncExpr wf, String funcName,
                                                   List<RowContext> contexts,
                                                   List<Integer> sortedPartition,
                                                   Object[] results) {
        boolean hasFrame = wf.frame() != null;
        WindowFuncExpr.ExcludeMode excludeMode = hasFrame ? wf.frame().excludeMode() : null;

        for (int i = 0; i < sortedPartition.size(); i++) {
            int[] bounds = resolveFrameBounds(wf, i, contexts, sortedPartition);
            int frameStart = bounds[0];
            int frameEnd = bounds[1];

            List<RowContext> frameRows = new ArrayList<>();
            for (int fi = frameStart; fi <= frameEnd; fi++) {
                if (excludeMode != null && excludeMode != WindowFuncExpr.ExcludeMode.NO_OTHERS) {
                    if (shouldExcludeRow(excludeMode, wf.orderBy(), contexts, sortedPartition, i, fi)) {
                        continue;
                    }
                }
                frameRows.add(contexts.get(sortedPartition.get(fi)));
            }

            FunctionCallExpr fn = wf.filter() != null
                    ? new FunctionCallExpr(funcName, wf.args(), wf.distinct(), wf.star(), null, wf.filter())
                    : new FunctionCallExpr(funcName, wf.args(), wf.distinct(), wf.star());
            Object val = select.aggregateEvaluator.evalAggregate(fn, frameRows);
            // If no rows in frame, most aggregates return NULL — but count returns 0
            if (frameRows.isEmpty() && !funcName.equalsIgnoreCase("count")) val = null;
            results[sortedPartition.get(i)] = val;
        }
    }

    /**
     * Determine whether a row at position fi should be excluded given the exclude mode.
     * Position i is the current row being computed.
     */
    private boolean shouldExcludeRow(WindowFuncExpr.ExcludeMode excludeMode,
                                      List<SelectStmt.OrderByItem> orderBy,
                                      List<RowContext> contexts,
                                      List<Integer> sortedPartition,
                                      int currentIdx, int frameIdx) {
        switch (excludeMode) {
            case CURRENT_ROW:
                return frameIdx == currentIdx;
            case TIES:
                // Exclude peer rows (same ORDER BY values) but NOT the current row itself
                if (frameIdx == currentIdx) return false;
                return orderByValuesEqual(orderBy, contexts,
                        sortedPartition.get(currentIdx), sortedPartition.get(frameIdx));
            case GROUP:
                // Exclude current row AND all peers
                return orderByValuesEqual(orderBy, contexts,
                        sortedPartition.get(currentIdx), sortedPartition.get(frameIdx));
            default:
                return false;
        }
    }

    /**
     * Resolve both frame bounds for the row at position {@code currentIdx} of the sorted
     * partition, honoring an explicit frame clause or the default frame (RANGE BETWEEN
     * UNBOUNDED PRECEDING AND CURRENT ROW including peers when ORDER BY is present,
     * the whole partition otherwise).
     * <p>
     * The returned bounds are clamped to the partition extents in a way that preserves
     * empty frames: if {@code result[0] > result[1]} the frame is empty.
     */
    private int[] resolveFrameBounds(WindowFuncExpr wf, int currentIdx,
                                      List<RowContext> contexts, List<Integer> sortedPartition) {
        int size = sortedPartition.size();
        boolean hasOrderBy = wf.orderBy() != null && !wf.orderBy().isEmpty();
        int frameStart, frameEnd;
        if (wf.frame() != null) {
            frameStart = resolveFrameBound(wf.frame().start(), currentIdx, size,
                    wf.frame().type(), wf.orderBy(), contexts, sortedPartition, true);
            frameEnd = resolveFrameBound(wf.frame().end(), currentIdx, size,
                    wf.frame().type(), wf.orderBy(), contexts, sortedPartition, false);
        } else if (hasOrderBy) {
            // Default frame with ORDER BY is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW,
            // which includes all peers (rows with the same ORDER BY values)
            frameStart = 0;
            frameEnd = currentIdx;
            while (frameEnd + 1 < size && orderByValuesEqual(wf.orderBy(), contexts,
                    sortedPartition.get(currentIdx), sortedPartition.get(frameEnd + 1))) {
                frameEnd++;
            }
        } else {
            frameStart = 0;
            frameEnd = size - 1;
        }
        // Clamp toward the partition extents without hiding empty frames:
        // a start beyond the end (or an end before the start) must stay start > end.
        if (frameStart < 0) frameStart = 0;
        if (frameEnd > size - 1) frameEnd = size - 1;
        return new int[]{frameStart, frameEnd};
    }

    private int resolveFrameBound(WindowFuncExpr.FrameBound bound, int currentIdx, int partitionSize,
                                    WindowFuncExpr.FrameType frameType,
                                    List<SelectStmt.OrderByItem> orderBy,
                                    List<RowContext> contexts,
                                    List<Integer> sortedPartition,
                                    boolean isStartBound) {
        switch (bound.boundType()) {
            case UNBOUNDED_PRECEDING:
                return 0;
            case UNBOUNDED_FOLLOWING:
                return partitionSize - 1;
            case CURRENT_ROW:
                if (frameType == WindowFuncExpr.FrameType.GROUPS || frameType == WindowFuncExpr.FrameType.RANGE) {
                    if (isStartBound) {
                        // First row of current peer group
                        int firstPeer = currentIdx;
                        while (firstPeer > 0 && orderByValuesEqual(orderBy, contexts,
                                sortedPartition.get(currentIdx), sortedPartition.get(firstPeer - 1))) {
                            firstPeer--;
                        }
                        return firstPeer;
                    } else {
                        // Last row of current peer group
                        int lastPeer = currentIdx;
                        while (lastPeer + 1 < partitionSize && orderByValuesEqual(orderBy, contexts,
                                sortedPartition.get(currentIdx), sortedPartition.get(lastPeer + 1))) {
                            lastPeer++;
                        }
                        return lastPeer;
                    }
                }
                return currentIdx;
            case PRECEDING: {
                Object rawOffset = executor.evalExpr(bound.offset(), null);
                if (frameType == WindowFuncExpr.FrameType.RANGE) {
                    return resolveRangeOffsetBound(orderBy, contexts, sortedPartition, currentIdx, partitionSize, rawOffset, true, isStartBound);
                } else if (frameType == WindowFuncExpr.FrameType.GROUPS) {
                    return resolveGroupsOffsetBound(orderBy, contexts, sortedPartition, currentIdx, partitionSize, -rowOffset(rawOffset), isStartBound);
                }
                return saturate((long) currentIdx - rowOffset(rawOffset));
            }
            case FOLLOWING: {
                Object rawOffset = executor.evalExpr(bound.offset(), null);
                if (frameType == WindowFuncExpr.FrameType.RANGE) {
                    return resolveRangeOffsetBound(orderBy, contexts, sortedPartition, currentIdx, partitionSize, rawOffset, false, isStartBound);
                } else if (frameType == WindowFuncExpr.FrameType.GROUPS) {
                    return resolveGroupsOffsetBound(orderBy, contexts, sortedPartition, currentIdx, partitionSize, rowOffset(rawOffset), isStartBound);
                }
                return saturate((long) currentIdx + rowOffset(rawOffset));
            }
            default:
                throw new IllegalStateException("Unknown bound type: " + bound.boundType());
        }
    }

    /**
     * A ROWS or GROUPS offset is a bigint in PostgreSQL, so a fractional offset is rounded on the
     * way in rather than truncated: {@code ROWS 1.5 PRECEDING} covers two rows, not one.
     */
    private long rowOffset(Object rawOffset) {
        if (rawOffset instanceof java.math.BigDecimal) {
            return ((java.math.BigDecimal) rawOffset)
                    .setScale(0, java.math.RoundingMode.HALF_UP).longValue();
        }
        if (rawOffset instanceof Double || rawOffset instanceof Float) {
            return Math.round(((Number) rawOffset).doubleValue());
        }
        if (rawOffset instanceof Number) return ((Number) rawOffset).longValue();
        if (rawOffset instanceof String) {
            // A quoted offset carries no type of its own; the frame reads it as the bigint it
            // counts rows in, and says so when it does not read.
            try {
                return Long.parseLong(((String) rawOffset).trim());
            } catch (NumberFormatException e) {
                throw new MemgresException(
                        "invalid input syntax for type bigint: \"" + rawOffset + "\"", "22P02");
            }
        }
        return executor.toInt(rawOffset);
    }

    /** Keep an out-of-range row position out of range without letting it wrap around. */
    private static int saturate(long index) {
        if (index < Integer.MIN_VALUE) return Integer.MIN_VALUE;
        if (index > Integer.MAX_VALUE) return Integer.MAX_VALUE;
        return (int) index;
    }

    /**
     * For RANGE frame with offset: find the boundary index by comparing ORDER BY values.
     * Supports both numeric and temporal (date/timestamp ± interval) offsets.
     * @param preceding true for PRECEDING, false for FOLLOWING
     */
    private int resolveRangeOffsetBound(List<SelectStmt.OrderByItem> orderBy,
                                         List<RowContext> contexts,
                                         List<Integer> sortedPartition,
                                         int currentIdx, int partitionSize,
                                         Object offset, boolean preceding,
                                         boolean isStartBound) {
        if (orderBy == null || orderBy.isEmpty()) return isStartBound ? 0 : partitionSize - 1;
        Expression orderExpr = orderBy.get(0).expr();
        boolean descending = orderBy.get(0).descending();
        Object currentVal = executor.evalExpr(orderExpr, contexts.get(sortedPartition.get(currentIdx)));
        if (currentVal == null) {
            // PG: rows with a NULL sort key have a frame consisting of their NULL peers
            int idx = currentIdx;
            if (isStartBound) {
                while (idx > 0 && executor.evalExpr(orderExpr,
                        contexts.get(sortedPartition.get(idx - 1))) == null) {
                    idx--;
                }
            } else {
                while (idx + 1 < partitionSize && executor.evalExpr(orderExpr,
                        contexts.get(sortedPartition.get(idx + 1))) == null) {
                    idx++;
                }
            }
            return idx;
        }

        // Compute boundary value: currentVal ± offset
        // For PRECEDING: boundary = current - offset (ascending) or current + offset (descending)
        // For FOLLOWING: boundary = current + offset (ascending) or current - offset (descending)
        boolean subtract = preceding != descending; // XOR: preceding+asc=sub, preceding+desc=add, following+asc=add, following+desc=sub
        java.lang.Comparable<?> boundaryVal = computeRangeBoundary(currentVal, offset, subtract);

        if (isStartBound) {
            for (int i = 0; i < partitionSize; i++) {
                Object v = executor.evalExpr(orderExpr, contexts.get(sortedPartition.get(i)));
                if (v != null) {
                    int cmp = compareToOrderValue(boundaryVal, v);
                    // ascending start: first row >= boundary; descending start: first row <= boundary
                    if (descending ? cmp >= 0 : cmp <= 0) return i;
                }
            }
            return partitionSize;
        } else {
            for (int i = partitionSize - 1; i >= 0; i--) {
                Object v = executor.evalExpr(orderExpr, contexts.get(sortedPartition.get(i)));
                if (v != null) {
                    int cmp = compareToOrderValue(boundaryVal, v);
                    // ascending end: last row <= boundary; descending end: last row >= boundary
                    if (descending ? cmp <= 0 : cmp >= 0) return i;
                }
            }
            return -1;
        }
    }

    /**
     * Compares a frame boundary against an ordering value. A date offset by an interval with a
     * time part lands between two days, which PostgreSQL keeps by reading the date column as the
     * timestamp it promotes to; the dates it is compared against promote the same way.
     */
    @SuppressWarnings("unchecked")
    private static int compareToOrderValue(java.lang.Comparable<?> boundary, Object value) {
        if (boundary instanceof java.time.LocalDateTime && value instanceof java.time.LocalDate) {
            return ((java.time.LocalDateTime) boundary)
                    .compareTo(((java.time.LocalDate) value).atStartOfDay());
        }
        return ((java.lang.Comparable<Object>) boundary).compareTo(value);
    }

    /** Compute currentVal ± offset for RANGE frame boundaries. */
    @SuppressWarnings("unchecked")
    private java.lang.Comparable<?> computeRangeBoundary(Object currentVal, Object offset, boolean subtract) {
        if (offset instanceof PgInterval) {
            // The offset is added to the ordering value the way any interval is: a month is a
            // calendar month, not thirty days, so the boundary of "INTERVAL '1 month' PRECEDING"
            // from 2024-02-01 is 2024-01-01 and not 2024-01-02.
            PgInterval iv = (PgInterval) offset;
            PgInterval signed = subtract ? iv.negate() : iv;
            if (currentVal instanceof java.time.LocalDate) {
                java.time.LocalDate d = (java.time.LocalDate) currentVal;
                // A whole number of days keeps the boundary a date; a time part does not, and
                // PostgreSQL reads the column as the timestamp the date promotes to.
                return signed.getMicroseconds() == 0
                        ? (java.lang.Comparable<?>) signed.addTo(d)
                        : (java.lang.Comparable<?>) signed.addTo(d.atStartOfDay());
            }
            if (currentVal instanceof java.time.LocalDateTime) {
                return signed.addTo((java.time.LocalDateTime) currentVal);
            }
            if (currentVal instanceof java.time.OffsetDateTime) {
                return signed.addTo((java.time.OffsetDateTime) currentVal);
            }
        }
        // Numeric offset — the boundary is compared against the ORDER BY values with
        // Comparable.compareTo, so it must come back as the same class as currentVal.
        // PG resolves the offset against the sort column's type and refuses the pair outright
        // rather than coercing, because the coercion would change which rows are in the frame.
        if (!(currentVal instanceof Number)) {
            throw PgErrors.notImplemented("RANGE with offset PRECEDING/FOLLOWING is not supported"
                    + " for column type " + pgTypeName(currentVal));
        }
        boolean integralColumn = currentVal instanceof Integer || currentVal instanceof Long
                || currentVal instanceof Short || currentVal instanceof Byte;
        if (offset instanceof String) {
            // A quoted offset takes the ordering column's type, so it is read as one of those
            // values here rather than refused for being text.
            String text = ((String) offset).trim();
            try {
                offset = integralColumn
                        ? (Object) Long.valueOf(Long.parseLong(text))
                        : (Object) new java.math.BigDecimal(text);
            } catch (NumberFormatException e) {
                throw new MemgresException("invalid input syntax for type "
                        + pgTypeName(TypeCoercion.inferType(currentVal)) + ": \"" + text + "\"", "22P02");
            }
        }
        boolean fractionalOffset = offset instanceof java.math.BigDecimal
                || offset instanceof Double || offset instanceof Float;
        if (!(offset instanceof Number) || (integralColumn && fractionalOffset)) {
            MemgresException e = PgErrors.notImplemented(
                    "RANGE with offset PRECEDING/FOLLOWING is not supported for column type "
                            + pgTypeName(currentVal) + " and offset type " + pgTypeName(offset));
            e.setHint("Cast the offset value to an appropriate type.");
            throw e;
        }
        if (currentVal instanceof java.math.BigDecimal) {
            java.math.BigDecimal cur = (java.math.BigDecimal) currentVal;
            java.math.BigDecimal off = offset instanceof java.math.BigDecimal
                    ? (java.math.BigDecimal) offset
                    : new java.math.BigDecimal(offset.toString());
            return subtract ? cur.subtract(off) : cur.add(off);
        }
        double numOffset = ((Number) offset).doubleValue();
        double currentNum = ((Number) currentVal).doubleValue();
        double result = subtract ? currentNum - numOffset : currentNum + numOffset;
        if (currentVal instanceof Integer) return (int) result;
        if (currentVal instanceof Long) return (long) result;
        if (currentVal instanceof Short) return (short) result;
        if (currentVal instanceof Byte) return (byte) result;
        if (currentVal instanceof Float) return (float) result;
        return result;
    }

    /** PostgreSQL names a type in an error the way {@code format_type} does, not by its catalog name. */
    private static String pgTypeName(Object value) {
        return pgTypeName(TypeCoercion.inferType(value));
    }

    private static String pgTypeName(DataType t) {
        // One renderer for all of them: the SQL spelling PostgreSQL writes in a message --
        // character varying rather than the catalog's varchar.
        return t == null ? "unknown" : t.toRegtypeDisplay();
    }

    /**
     * For GROUPS frame with offset: find the boundary by counting peer groups.
     * direction is negative for PRECEDING, positive for FOLLOWING.
     */
    private int resolveGroupsOffsetBound(List<SelectStmt.OrderByItem> orderBy,
                                          List<RowContext> contexts,
                                          List<Integer> sortedPartition,
                                          int currentIdx, int partitionSize,
                                          long direction, boolean isStartBound) {
        // First, identify peer group boundaries
        List<int[]> groups = new ArrayList<>(); // each element is [startIdx, endIdx]
        int gStart = 0;
        for (int i = 1; i <= partitionSize; i++) {
            if (i == partitionSize || !orderByValuesEqual(orderBy, contexts,
                    sortedPartition.get(i), sortedPartition.get(gStart))) {
                groups.add(new int[]{gStart, i - 1});
                gStart = i;
            }
        }
        // Find which group the current row belongs to
        int currentGroup = -1;
        for (int g = 0; g < groups.size(); g++) {
            if (currentIdx >= groups.get(g)[0] && currentIdx <= groups.get(g)[1]) {
                currentGroup = g;
                break;
            }
        }
        long targetGroup = currentGroup + direction;
        if (isStartBound) {
            // A start bound past the last group means an empty frame (start > end);
            // a start bound before the first group clamps to the partition start.
            if (targetGroup >= groups.size()) return partitionSize;
            if (targetGroup < 0) targetGroup = 0;
            return groups.get((int) targetGroup)[0];
        } else {
            // An end bound before the first group means an empty frame (end < start);
            // an end bound past the last group clamps to the partition end.
            if (targetGroup < 0) return -1;
            if (targetGroup >= groups.size()) targetGroup = groups.size() - 1;
            return groups.get((int) targetGroup)[1];
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
