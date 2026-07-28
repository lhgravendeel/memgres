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

    // Thread-local for passing current grouping set to grouping() function
    private final ThreadLocal<Set<String>> currentGroupingSetColumns = new ThreadLocal<>();

    SelectAggregateEvaluator(SelectExecutor select) {
        this.select = select;
        this.executor = select.executor;
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

        for (List<Expression> groupingSet : groupingSets) {
            List<Expression> effectiveGroupBy = new ArrayList<>(fixedGroupBy);
            effectiveGroupBy.addAll(resolveGroupByRefs(groupingSet, stmt));

            Set<String> groupingSetColNames = new HashSet<>();
            for (Expression e : effectiveGroupBy) {
                if (e instanceof ColumnRef) groupingSetColNames.add(((ColumnRef) e).column().toLowerCase());
                else groupingSetColNames.add(executor.exprToAlias(e).toLowerCase());
                // Composite grouping element like (a, b): its member columns are grouped
                // individually as far as GROUPING() and the select list are concerned.
                if (e instanceof ArrayExpr && ((ArrayExpr) e).isRow()) {
                    for (Expression el : ((ArrayExpr) e).elements()) {
                        if (el instanceof ColumnRef) groupingSetColNames.add(((ColumnRef) el).column().toLowerCase());
                        else groupingSetColNames.add(executor.exprToAlias(el).toLowerCase());
                    }
                }
            }
            currentGroupingSetColumns.set(groupingSetColNames);

            // Column names that are functionally grouped in this grouping set. Select-list
            // expressions built over these columns evaluate against the group's key values;
            // any other column reads as NULL for this grouping set (PG semantics).
            Set<String> groupedColumnNames = new HashSet<>();
            for (Expression e : effectiveGroupBy) {
                collectColumnNames(e, groupedColumnNames);
            }

            List<List<RowContext>> groups;
            if (effectiveGroupBy.isEmpty()) {
                groups = new ArrayList<>();
                groups.add(new ArrayList<>(contexts));
            } else {
                Map<String, List<RowContext>> groupMap = new LinkedHashMap<>();
                for (RowContext ctx : contexts) {
                    StringBuilder key = new StringBuilder();
                    for (Expression ge : effectiveGroupBy) {
                        Object val = executor.evalExpr(ge, ctx);
                        key.append(val == null ? "\0NULL" : RowKey.valueKey(val)).append('\1');
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
            }
        }
        currentGroupingSetColumns.set(null);

        // ORDER BY
        List<SelectStmt.OrderByItem> resolvedOrderBy = select.resolveOrderBy(stmt.orderBy(), stmt.targets());
        if (resolvedOrderBy != null && !resolvedOrderBy.isEmpty()) {
            final List<SelectStmt.OrderByItem> ob = resolvedOrderBy;
            allResultRows.sort((a, b) -> {
                for (SelectStmt.OrderByItem item : ob) {
                    int colIdx = select.resolveOrderByToColumnIndex(item.expr(), stmt.targets());
                    Object va = colIdx >= 0 ? a[colIdx] : null;
                    Object vb = colIdx >= 0 ? b[colIdx] : null;
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

        allResultRows = select.applyDistinct(stmt, allResultRows);
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
    private List<Expression> resolveGroupByRefs(List<Expression> groupBy, SelectStmt stmt) {
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

    QueryResult executeAggregateSelect(SelectStmt stmt, List<RowContext> contexts,
                                        List<RowContext.TableBinding> baseBindings) {
        if (stmt.groupingSets() != null && !stmt.groupingSets().isEmpty()) {
            return executeGroupingSetsSelect(stmt, contexts, baseBindings);
        }

        // Resolve GROUP BY ordinals and aliases
        List<Expression> resolvedGroupBy = resolveGroupByRefs(stmt.groupBy(), stmt);

        boolean hasGroupBy = resolvedGroupBy != null && !resolvedGroupBy.isEmpty();
        List<List<RowContext>> groups;

        if (hasGroupBy) {
            Map<String, List<RowContext>> groupMap = new LinkedHashMap<>();
            for (RowContext ctx : contexts) {
                StringBuilder keyBuilder = new StringBuilder();
                for (Expression groupExpr : resolvedGroupBy) {
                    Object val = executor.evalExpr(groupExpr, ctx);
                    keyBuilder.append(RowKey.valueKey(val)).append('\1');
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

        for (List<RowContext> group : groups) {
            RowContext representative = group.isEmpty() ? null : group.get(0);

            Object[] row = new Object[stmt.targets().size()];
            for (int i = 0; i < stmt.targets().size(); i++) {
                SelectStmt.SelectTarget target = stmt.targets().get(i);
                Expression expr = target.expr();
                row[i] = evalAggregateExpr(expr, group, representative);
            }

            if (stmt.having() != null) {
                if (select.containsWindowFunction(stmt.having())) {
                    throw new MemgresException("window functions are not allowed in HAVING", "42P20");
                }
                Object havingResult = evalAggregateExpr(stmt.having(), group, representative);
                if (!executor.isTruthy(havingResult)) continue;
            }

            resultRows.add(row);
        }

        if (groups.isEmpty() && !hasGroupBy) {
            Object[] row = new Object[stmt.targets().size()];
            for (int i = 0; i < stmt.targets().size(); i++) {
                Expression expr = stmt.targets().get(i).expr();
                row[i] = evalAggregateExpr(expr, Cols.listOf(), null);
            }
            resultRows.add(row);
        }

        // Post-process window functions over the grouped result set
        if (select.hasWindowFunctionInTargets(stmt.targets()) && !resultRows.isEmpty()) {
            Table virtualTable = new Table("__agg_result__", resultColumns);
            for (Object[] row : resultRows) {
                virtualTable.insertRow(row);
            }
            List<RowContext> aggContexts = new ArrayList<>(resultRows.size());
            for (Object[] row : resultRows) {
                aggContexts.add(new RowContext(Cols.listOf(
                        new RowContext.TableBinding(virtualTable, "__agg_result__", row))));
            }

            for (int ti = 0; ti < stmt.targets().size(); ti++) {
                Expression expr = stmt.targets().get(ti).expr();
                if (select.containsWindowFunction(expr)) {
                    Object[] windowVals = select.windowEvaluator.evaluateWindowExpression(expr, aggContexts, stmt.windowDefs());
                    for (int ri = 0; ri < resultRows.size(); ri++) {
                        resultRows.get(ri)[ti] = windowVals[ri];
                    }
                }
            }
        }

        // ORDER BY on aggregate results
        // Pre-compute ORDER BY values for aggregate expressions not in target columns
        List<SelectStmt.OrderByItem> resolvedOrderBy = select.resolveOrderBy(stmt.orderBy(), stmt.targets());
        Map<Object[], Object[]> orderByValues = null;
        if (resolvedOrderBy != null && !resolvedOrderBy.isEmpty()) {
            // Check if any ORDER BY expr needs aggregate evaluation
            boolean needsAggEval = false;
            for (SelectStmt.OrderByItem item : resolvedOrderBy) {
                int colIdx = select.resolveOrderByToColumnIndex(item.expr(), stmt.targets());
                if (colIdx < 0 && select.containsAggregate(item.expr())) {
                    needsAggEval = true;
                    break;
                }
            }
            if (needsAggEval && !groups.isEmpty()) {
                orderByValues = new IdentityHashMap<>();
                for (int gi = 0; gi < groups.size() && gi < resultRows.size(); gi++) {
                    List<RowContext> group = groups.get(gi);
                    RowContext rep = group.isEmpty() ? null : group.get(0);
                    Object[] obVals = new Object[resolvedOrderBy.size()];
                    for (int oi = 0; oi < resolvedOrderBy.size(); oi++) {
                        SelectStmt.OrderByItem item = resolvedOrderBy.get(oi);
                        int colIdx = select.resolveOrderByToColumnIndex(item.expr(), stmt.targets());
                        if (colIdx >= 0) {
                            obVals[oi] = resultRows.get(gi)[colIdx];
                        } else {
                            obVals[oi] = evalAggregateExpr(item.expr(), group, rep);
                        }
                    }
                    orderByValues.put(resultRows.get(gi), obVals);
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

        resultRows = select.applyDistinct(stmt, resultRows);
        resultRows = select.applyOffsetAndLimit(stmt, resultRows);

        return QueryResult.select(resultColumns, resultRows);
    }

    // ---- Aggregate expression evaluation ----

    Object evalAggregateExpr(Expression expr, List<RowContext> group, RowContext representative) {
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            String name = FunctionEvaluator.stripSchemaPrefix(fn.name().toLowerCase());
            // Check for json/jsonb type mismatch in COALESCE
            if (name.equals("coalesce")) {
                boolean hasJsonAgg = false, hasJsonbCast = false;
                for (Expression arg : fn.args()) {
                    if (arg instanceof FunctionCallExpr) {
                        String aname = ((FunctionCallExpr) arg).name().toLowerCase();
                        if (aname.equals("json_arrayagg") || aname.equals("json_objectagg")) hasJsonAgg = true;
                    }
                    if (arg instanceof CastExpr && ((CastExpr) arg).typeName().equalsIgnoreCase("jsonb")) hasJsonbCast = true;
                }
                if (hasJsonAgg && hasJsonbCast) {
                    throw new MemgresException("could not convert type jsonb to json", "42846");
                }
            }
            if (select.isAggregateFunction(name)) {
                return evalAggregate(fn, group);
            }
            boolean hasAggArg = fn.args().stream().anyMatch(select::containsAggregate);
            if (hasAggArg) {
                List<Expression> resolvedArgs = new ArrayList<>();
                for (Expression arg : fn.args()) {
                    Object val = evalAggregateExpr(arg, group, representative);
                    if (val == null) {
                        resolvedArgs.add(Literal.ofNull());
                    } else {
                        // Preserve the resolved aggregate's runtime type (a typed value, not a
                        // re-parsed string literal) so a scalar function/expression wrapped
                        // around aggregate args -- e.g. LEAST(sum(a), sum(b)) -- compares its
                        // arguments numerically instead of lexicographically. Literal.ofString
                        // here previously stringified every value (mtask-8 Group 5): LEAST('7.0',
                        // '10.0') compares as strings ("10.0" < "7.0" because '1' < '7'),
                        // silently returning the wrong (larger) value.
                        resolvedArgs.add(new ExprEvaluator.PrecomputedValueExpr(val));
                    }
                }
                return executor.functionEvaluator.evalFunction(new FunctionCallExpr(fn.name(), resolvedArgs, fn.distinct(), fn.star()), representative);
            }
            return executor.functionEvaluator.evalFunction(fn, representative);
        } else if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
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
        } else if (expr instanceof InExpr) {
            InExpr in = (InExpr) expr;
            Object val = evalAggregateExpr(in.expr(), group, representative);
            if (val == null) return null;
            boolean found = false;
            boolean hasNull = false;
            for (Expression v : in.values()) {
                Object elem = executor.evalExpr(v, representative);
                if (elem == null) { hasNull = true; continue; }
                if (TypeCoercion.areEqual(val, elem)) { found = true; break; }
            }
            if (found) return !in.negated();
            if (hasNull) return null;
            return in.negated();
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
        } else if (expr instanceof WindowFuncExpr) {
            return null;
        } else if (expr instanceof Literal) {
            // Literals don't depend on row context, evaluate them directly
            return executor.evalExpr(expr, representative);
        } else if (expr instanceof SubqueryExpr) {
            // Subqueries can be evaluated without a representative row
            return executor.evalExpr(expr, representative);
        } else {
            return representative != null ? executor.evalExpr(expr, representative) : null;
        }
    }

    // ---- Ordered-set aggregates ----

    private Object evalOrderedSetAggregate(OrderedSetAggExpr osa, List<RowContext> group) {
        String name = osa.funcName().toLowerCase();
        List<SelectStmt.OrderByItem> orderBy = osa.withinGroupOrderBy();

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
                Object fractionObj = executor.evalExpr(osa.args().get(0), group.isEmpty() ? null : group.get(0));
                if (fractionObj == null) return null;
                // Handle array argument: compute percentile for each element
                if (fractionObj instanceof java.util.List) {
                    java.util.List<?> fractions = (java.util.List<?>) fractionObj;
                    java.util.List<Object> results = new java.util.ArrayList<>();
                    for (Object f : fractions) {
                        if (f == null) { results.add(null); continue; }
                        double fv;
                        try { fv = executor.toDouble(f); } catch (Exception e) {
                            throw new MemgresException("percentile fraction must be between 0 and 1", "22003");
                        }
                        if (Double.isNaN(fv))
                            throw new MemgresException("percentile value NaN is not between 0 and 1", "22003");
                        if (fv < 0.0 || fv > 1.0)
                            throw new MemgresException("percentile fraction must be between 0 and 1", "22003");
                        if (vals.isEmpty()) { results.add(null); continue; }
                        int idx = (int) Math.ceil(fv * vals.size()) - 1;
                        if (idx < 0) idx = 0;
                        if (idx >= vals.size()) idx = vals.size() - 1;
                        results.add(vals.get(idx));
                    }
                    return results;
                }
                double fraction;
                try { fraction = executor.toDouble(fractionObj); } catch (Exception e) {
                    throw new MemgresException("percentile fraction must be between 0 and 1", "22003");
                }
                if (Double.isNaN(fraction))
                    throw new MemgresException("percentile value NaN is not between 0 and 1", "22003");
                if (fraction < 0.0 || fraction > 1.0)
                    throw new MemgresException("percentile fraction must be between 0 and 1", "22003");
                if (vals.isEmpty()) return null;
                int idx = (int) Math.ceil(fraction * vals.size()) - 1;
                if (idx < 0) idx = 0;
                if (idx >= vals.size()) idx = vals.size() - 1;
                return vals.get(idx);
            }
            case "percentile_cont": {
                if (osa.args().isEmpty()) return null;
                Object fractionObj = executor.evalExpr(osa.args().get(0), group.isEmpty() ? null : group.get(0));
                if (fractionObj == null) return null;
                // Handle array argument: compute percentile for each element
                if (fractionObj instanceof java.util.List) {
                    java.util.List<?> fractions = (java.util.List<?>) fractionObj;
                    java.util.List<Object> results = new java.util.ArrayList<>();
                    for (Object f : fractions) {
                        if (f == null) { results.add(null); continue; }
                        double fv;
                        try { fv = executor.toDouble(f); } catch (Exception e) {
                            throw new MemgresException("percentile fraction must be between 0 and 1", "22003");
                        }
                        if (fv < 0.0 || fv > 1.0)
                            throw new MemgresException("percentile fraction must be between 0 and 1", "22003");
                        if (vals.isEmpty()) { results.add(null); continue; }
                        if (vals.size() == 1) { results.add(vals.get(0)); continue; }
                        double pos = fv * (vals.size() - 1);
                        int lo = (int) Math.floor(pos);
                        int hi = (int) Math.ceil(pos);
                        if (lo == hi) { results.add(vals.get(lo)); continue; }
                        double loVal = executor.toDouble(vals.get(lo));
                        double hiVal = executor.toDouble(vals.get(hi));
                        double r = loVal + (hiVal - loVal) * (pos - lo);
                        if (r == Math.floor(r) && !Double.isInfinite(r)) {
                            long lr = (long) r;
                            if (lr >= Integer.MIN_VALUE && lr <= Integer.MAX_VALUE) { results.add((int) lr); continue; }
                            results.add(lr); continue;
                        }
                        results.add(r);
                    }
                    return results;
                }
                double fraction;
                try { fraction = executor.toDouble(fractionObj); } catch (Exception e) {
                    throw new MemgresException("percentile fraction must be between 0 and 1", "22003");
                }
                if (fraction < 0.0 || fraction > 1.0)
                    throw new MemgresException("percentile fraction must be between 0 and 1", "22003");
                if (vals.isEmpty()) return null;
                if (vals.size() == 1) return vals.get(0);
                double pos = fraction * (vals.size() - 1);
                int lower = (int) Math.floor(pos);
                int upper = (int) Math.ceil(pos);
                if (lower == upper) return vals.get(lower);
                double lo = executor.toDouble(vals.get(lower));
                double hi = executor.toDouble(vals.get(upper));
                double result = lo + (hi - lo) * (pos - lower);
                if (result == Math.floor(result) && !Double.isInfinite(result)) {
                    long lresult = (long) result;
                    if (lresult >= Integer.MIN_VALUE && lresult <= Integer.MAX_VALUE) return (int) lresult;
                    return lresult;
                }
                return result;
            }
            case "mode": {
                if (vals.isEmpty()) return null;
                Map<String, Long> freq = new LinkedHashMap<>();
                Map<String, Object> firstOccurrence = new LinkedHashMap<>();
                for (Object v : vals) {
                    String key = v.toString();
                    freq.merge(key, 1L, Long::sum);
                    firstOccurrence.putIfAbsent(key, v);
                }
                String modeKey = freq.entrySet().stream()
                        .max(Map.Entry.comparingByValue())
                        .map(Map.Entry::getKey).orElse(null);
                return modeKey != null ? firstOccurrence.get(modeKey) : null;
            }
            case "rank": {
                if (osa.args().isEmpty()) return null;
                Object hypoObj = executor.evalExpr(osa.args().get(0), group.isEmpty() ? null : group.get(0));
                long rank = 1;
                for (Object v : vals) {
                    if (executor.compareValues(v, hypoObj) < 0) rank++;
                }
                return rank;
            }
            case "dense_rank": {
                if (osa.args().isEmpty()) return null;
                Object hypoObj = executor.evalExpr(osa.args().get(0), group.isEmpty() ? null : group.get(0));
                Set<String> seen = new LinkedHashSet<>();
                for (Object v : vals) {
                    if (executor.compareValues(v, hypoObj) < 0) seen.add(v.toString());
                }
                return (long)(seen.size() + 1);
            }
            case "percent_rank": {
                if (osa.args().isEmpty()) return null;
                Object hypoObj = executor.evalExpr(osa.args().get(0), group.isEmpty() ? null : group.get(0));
                if (vals.isEmpty()) return 0.0;
                long rankVal = 1;
                for (Object v : vals) {
                    if (executor.compareValues(v, hypoObj) < 0) rankVal++;
                }
                return (double)(rankVal - 1) / vals.size();
            }
            case "cume_dist": {
                if (osa.args().isEmpty()) return null;
                Object hypoObj = executor.evalExpr(osa.args().get(0), group.isEmpty() ? null : group.get(0));
                long countLE = 0;
                for (Object v : vals) {
                    if (executor.compareValues(v, hypoObj) <= 0) countLE++;
                }
                // The hypothetical row joins both the numerator and the denominator:
                // (countLE + 1) / (N + 1). Empty group: (0 + 1) / (0 + 1) = 1.0.
                return (double) (countLE + 1) / (vals.size() + 1);
            }
            default: {
                throw notAnOrderedSetAggregate(osa, group);
            }
        }
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
        MemgresException e = new MemgresException(
                "function " + name + "(" + types + ") does not exist", "42883");
        e.setHint("No function matches the given name and argument types. "
                + "You might need to add explicit type casts.");
        return e;
    }

    /** PG leaves a bare quoted literal as "unknown" until a resolved call forces its type. */
    private String argTypeName(Expression expr, RowContext sample) {
        if (expr instanceof Literal && ((Literal) expr).literalType() == Literal.LiteralType.STRING) {
            return "unknown";
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

        for (Expression arg : fn.args()) {
            if (select.containsAggregate(arg)) {
                throw new MemgresException("aggregate function calls cannot be nested", "42803");
            }
        }

        if (fn.filter() != null) {
            group = group.stream()
                    .filter(ctx -> executor.isTruthy(executor.evalExpr(fn.filter(), ctx)))
                    .collect(Collectors.toList());
        }

        switch (name) {
            case "count": {
                if (fn.star()) {
                    return (long) group.size();
                }
                Expression arg = fn.args().get(0);
                if (fn.distinct() && fn.args().size() > 1) {
                    throw new MemgresException("function count(text, text) does not exist\n  Hint: No function matches the given name and argument types.", "42883");
                }
                if (fn.distinct()) {
                    Set<String> seen = new HashSet<>();
                    for (RowContext ctx : group) {
                        Object val = executor.evalExpr(arg, ctx);
                        if (val != null) seen.add(distinctKey(val));
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
                boolean hasValue = false;
                boolean allInts = true;
                boolean isMoney = false;
                boolean sawNotANumber = false;
                BigDecimal bdSum = BigDecimal.ZERO;
                try {
                    if (fn.distinct()) {
                        Set<String> seenKeys = new HashSet<>();
                        for (RowContext ctx : group) {
                            Object val = executor.evalExpr(arg, ctx);
                            if (val == null || !seenKeys.add(distinctKey(val))) continue;
                            hasValue = true;
                            if (isNotANumber(val)) { sawNotANumber = true; continue; }
                            if (val instanceof PgMoney) isMoney = true;
                            bdSum = bdSum.add(SelectExecutor.toBigDecimal(val));
                            if (!(val instanceof Integer || val instanceof Long)) allInts = false;
                        }
                    } else {
                        for (RowContext ctx : group) {
                            Object val = executor.evalExpr(arg, ctx);
                            if (val != null) {
                                hasValue = true;
                                if (isNotANumber(val)) { sawNotANumber = true; continue; }
                                if (val instanceof PgMoney) isMoney = true;
                                bdSum = bdSum.add(SelectExecutor.toBigDecimal(val));
                                if (!(val instanceof Integer || val instanceof Long)) allInts = false;
                            }
                        }
                    }
                } catch (NumberFormatException e) {
                    throw new MemgresException("function sum(text) does not exist\n  Hint: No function matches the given name and argument types.", "42883");
                }
                if (!hasValue) return null;
                // NaN is contagious through numeric aggregation, as it is in PG.
                if (sawNotANumber) return Double.NaN;
                if (isMoney) return new PgMoney(bdSum);
                // sum(double precision) is float8 in PG, not numeric
                if (isFloatArgument(arg, group)) return bdSum.doubleValue();
                if (allInts) {
                    try { return bdSum.longValueExact(); } catch (ArithmeticException e) { /* fall through */ }
                }
                return bdSum;
            }
            case "avg": {
                if (group.isEmpty()) return null;
                Expression arg = fn.args().get(0);
                requireAggregatableNumeric("avg", arg, group);
                BigDecimal bdSum = BigDecimal.ZERO;
                long count = 0;
                boolean avgSawNotANumber = false;
                if (fn.distinct()) {
                    Set<String> seenKeys = new HashSet<>();
                    for (RowContext ctx : group) {
                        Object val = executor.evalExpr(arg, ctx);
                        if (val == null || !seenKeys.add(distinctKey(val))) continue;
                        count++;
                        if (isNotANumber(val)) { avgSawNotANumber = true; continue; }
                        bdSum = bdSum.add(SelectExecutor.toBigDecimal(val));
                    }
                } else {
                    for (RowContext ctx : group) {
                        Object val = executor.evalExpr(arg, ctx);
                        if (val != null) {
                            count++;
                            if (isNotANumber(val)) { avgSawNotANumber = true; continue; }
                            bdSum = bdSum.add(SelectExecutor.toBigDecimal(val));
                        }
                    }
                }
                if (count == 0) return null;
                // NaN is contagious through numeric aggregation, as it is in PG.
                if (avgSawNotANumber) return Double.NaN;
                if (isFloatArgument(arg, group)) return bdSum.doubleValue() / count;
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
                    throw new MemgresException("function string_agg(text) does not exist\n  Hint: No function matches the given name and argument types.", "42883");
                }
                if (group.isEmpty()) return null;
                Expression arg = fn.args().get(0);
                Expression delimExpr = fn.args().get(1);
                Object delimVal = delimExpr != null ? executor.evalExpr(delimExpr, group.get(0)) : ",";
                List<RowContext> orderedGroup = sortGroupForAggregate(group, fn);
                Set<String> seen = fn.distinct() ? new LinkedHashSet<>() : null;
                List<Object> parts = new ArrayList<>();
                boolean allBytea = true;
                for (RowContext ctx : orderedGroup) {
                    Object val = executor.evalExpr(arg, ctx);
                    if (val == null) continue;
                    if (seen != null && !seen.add(distinctKey(val))) continue;
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
                for (RowContext ctx : orderedGroup) {
                    Object val = executor.evalExpr(arg, ctx);
                    if (seen != null && val != null && !seen.add(distinctKey(val))) continue;
                    list.add(val);
                }
                StringBuilder sb = new StringBuilder("{");
                for (int ai = 0; ai < list.size(); ai++) {
                    if (ai > 0) sb.append(",");
                    Object v = list.get(ai);
                    if (v == null) sb.append("NULL");
                    else if (v instanceof String) {
                        String sv = (String) v;
                        if (sv.isEmpty() || sv.equalsIgnoreCase("NULL") || sv.contains(",") || sv.contains("{") || sv.contains("}") || sv.contains("\"") || sv.contains("\\") || sv.contains(" ")) {
                            sb.append("\"").append(sv.replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
                        } else {
                            sb.append(sv);
                        }
                    }
                    else if (v instanceof AstExecutor.PgRow) {
                        String sv = v.toString();
                        sb.append("\"").append(sv.replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
                    }
                    else sb.append(v);
                }
                sb.append("}");
                return sb.toString();
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
                allRanges.sort((a, b) -> Long.compare(a.effectiveLower(), b.effectiveLower()));
                java.util.List<RangeOperations.PgRange> merged = new java.util.ArrayList<>();
                merged.add(allRanges.get(0));
                for (int mi = 1; mi < allRanges.size(); mi++) {
                    RangeOperations.PgRange last = merged.get(merged.size() - 1);
                    RangeOperations.PgRange curr = allRanges.get(mi);
                    if (last.effectiveUpper() >= curr.effectiveLower()) {
                        merged.set(merged.size() - 1, RangeOperations.merge(last, curr));
                    } else {
                        merged.add(curr);
                    }
                }
                return RangeOperations.formatMultirange(merged);
            }
            case "range_intersect_agg": {
                // range_intersect_agg(anyrange) → range: running intersection of all input ranges
                if (group.isEmpty()) return null;
                Expression arg = fn.args().get(0);
                List<RowContext> orderedGroup = sortGroupForAggregate(group, fn);
                RangeOperations.PgRange result = null;
                for (RowContext ctx : orderedGroup) {
                    Object val = executor.evalExpr(arg, ctx);
                    if (val == null) continue;
                    String s = val.toString().trim();
                    if (s.equalsIgnoreCase("empty")) {
                        return "empty"; // intersection with empty is empty
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
                        long iv = ((Number) v).longValue();
                        long acc = (result == null) ? iv : (((Number) result).longValue() ^ iv);
                        result = numericBitResult(v, acc);
                    }
                }
                return result;
            }
            case "json_agg":
            case "jsonb_agg": {
                if (group.isEmpty()) return null;
                List<RowContext> orderedGroup = sortGroupForAggregate(group, fn);
                StringBuilder sb = new StringBuilder("[");
                boolean first = true;
                for (RowContext r : orderedGroup) {
                    Object v = executor.evalExpr(fn.args().get(0), r);
                    if (!first) sb.append(", ");
                    first = false;
                    if (v == null) sb.append("null");
                    else if (v instanceof Number) sb.append(v);
                    else if (v instanceof Boolean) sb.append(v);
                    else sb.append("\"").append(v.toString().replace("\"", "\\\"")).append("\"");
                }
                sb.append("]");
                return sb.toString();
            }
            case "json_object_agg":
            case "jsonb_object_agg": {
                if (group.isEmpty()) return null;
                StringBuilder sb = new StringBuilder("{");
                boolean first = true;
                for (RowContext r : group) {
                    Object k = executor.evalExpr(fn.args().get(0), r);
                    Object v = executor.evalExpr(fn.args().get(1), r);
                    // Dropping the row would silently lose it; PG refuses a NULL key outright
                    if (k == null) {
                        throw name.equals("jsonb_object_agg")
                                ? PgErrors.invalidParameter("field name must not be null")
                                : new MemgresException("null value not allowed for object key", "22004");
                    }
                    if (!first) sb.append(", ");
                    first = false;
                    sb.append("\"").append(k.toString().replace("\"", "\\\"")).append("\": ");
                    if (v == null) sb.append("null");
                    else if (v instanceof Number) sb.append(v);
                    else if (v instanceof Boolean) sb.append(v);
                    else sb.append("\"").append(v.toString().replace("\"", "\\\"")).append("\"");
                }
                sb.append("}");
                String result = sb.toString();
                if (name.equals("jsonb_object_agg")) {
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
                List<BigDecimal> vals = collectBigDecimals(fn.args().get(0), group);
                BigDecimal variance = computeVariance(vals, true);
                return variance != null ? variance.setScale(16, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString() : null;
            }
            case "var_samp":
            case "variance": {
                if (group.isEmpty()) return null;
                List<BigDecimal> vals = collectBigDecimals(fn.args().get(0), group);
                BigDecimal variance = computeVariance(vals, false);
                if (variance == null) return null;
                if (isFloatArgument(fn.args().get(0), group)) return variance.doubleValue();
                return variance.setScale(16, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString();
            }
            case "stddev_pop": {
                if (group.isEmpty()) return null;
                List<BigDecimal> vals = collectBigDecimals(fn.args().get(0), group);
                BigDecimal variance = computeVariance(vals, true);
                if (variance == null) return null;
                BigDecimal stddev = new BigDecimal(Math.sqrt(variance.doubleValue()))
                    .setScale(16, RoundingMode.HALF_UP);
                return stddev.stripTrailingZeros().toPlainString();
            }
            case "stddev_samp":
            case "stddev": {
                if (group.isEmpty()) return null;
                Expression arg = fn.args().get(0);
                List<Double> vals = new ArrayList<>();
                for (RowContext ctx : group) {
                    Object v = executor.evalExpr(arg, ctx);
                    if (v != null) vals.add(((Number) v).doubleValue());
                }
                if (vals.isEmpty() || vals.size() < 2) return null;
                double mean = vals.stream().mapToDouble(d -> d).average().orElse(0);
                double variance = vals.stream().mapToDouble(d -> (d - mean) * (d - mean)).sum() / (vals.size() - 1);
                double sd = Math.sqrt(variance);
                if (isFloatArgument(arg, group)) return sd;
                return BigDecimal.valueOf(sd).stripTrailingZeros().toPlainString();
            }
            case "grouping": {
                Set<String> currentGroupSet = currentGroupingSetColumns.get();
                if (currentGroupSet == null) {
                    throw new MemgresException(
                            "arguments to GROUPING must be grouping expressions of the associated query level",
                            "42803");
                }
                // Result is a bitmask: bit i (from the left, most significant first) is 1
                // if argument i is NOT grouped in the current grouping set.
                // grouping(a, b) over ROLLUP(a, b): detail rows 0, a-subtotals 1, grand total 3.
                int mask = 0;
                for (Expression arg : fn.args()) {
                    String colName = arg instanceof ColumnRef ? ((ColumnRef) arg).column().toLowerCase() :
                            executor.exprToAlias(arg).toLowerCase();
                    mask = (mask << 1) | (currentGroupSet.contains(colName) ? 0 : 1);
                }
                return mask;
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

    private Object evalUserDefinedAggregate(PgAggregate agg, FunctionCallExpr fn, List<RowContext> group) {
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

        // For each row, call SFUNC(state, value[, ...])
        for (RowContext ctx : group) {
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
     * Dedup key for DISTINCT aggregates with value semantics: numeric values that compare
     * equal map to the same key even when their representations differ (numeric 1.0 vs 1.00,
     * or int 1 vs numeric 1.0), matching PostgreSQL's equality-based DISTINCT.
     */
    /** True for the float NaN value, which has no BigDecimal form and poisons any numeric sum. */
    private static boolean isNotANumber(Object val) {
        if (val instanceof Double) return ((Double) val).isNaN();
        if (val instanceof Float) return ((Float) val).isNaN();
        return false;
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
        return val.toString();
    }

    /**
     * PG has no avg(money) or avg over other non-numeric types; reject rather than
     * silently coercing the value through numeric.
     */
    private void requireAggregatableNumeric(String fname, Expression arg, List<RowContext> group) {
        for (RowContext ctx : group) {
            Object val = executor.evalExpr(arg, ctx);
            if (val == null) continue;
            if (val instanceof PgMoney) {
                throw new MemgresException("function " + fname + "(money) does not exist"
                        + "\n  Hint: No function matches the given name and argument types.", "42883");
            }
            return;
        }
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

    /** Sort a group by the aggregate's ORDER BY clause (used by string_agg, array_agg, json_agg, etc.). */
    private List<RowContext> sortGroupForAggregate(List<RowContext> group, FunctionCallExpr fn) {
        checkDistinctOrderBy(fn);
        List<RowContext> orderedGroup = new ArrayList<>(group);
        if (fn.orderBy() != null && !fn.orderBy().isEmpty()) {
            orderedGroup.sort((a, b) -> {
                for (SelectStmt.OrderByItem item : fn.orderBy()) {
                    Object va = executor.evalExpr(item.expr(), a);
                    Object vb = executor.evalExpr(item.expr(), b);
                    // PG's default is NULLS LAST ascending, NULLS FIRST descending: nulls sort
                    // as the largest value, and DESC flips that along with everything else.
                    if (va == null || vb == null) {
                        if (va == null && vb == null) continue;
                        int nullCmp = va == null ? 1 : -1;
                        return item.descending() ? -nullCmp : nullCmp;
                    }
                    int cmp = executor.compareValues(va, vb);
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

    /** Collect BigDecimal values from a group for a given expression, skipping nulls. */
    private List<BigDecimal> collectBigDecimals(Expression arg, List<RowContext> group) {
        List<BigDecimal> vals = new ArrayList<>();
        for (RowContext ctx : group) {
            Object v = executor.evalExpr(arg, ctx);
            if (v != null) {
                vals.add(v instanceof BigDecimal ? ((BigDecimal) v) : BigDecimal.valueOf(((Number) v).doubleValue()));
            }
        }
        return vals;
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
