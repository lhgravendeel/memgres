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
            resultColumns.add(new Column(alias, executor.inferTypeFromContext(target.expr(), baseBindings), true, false, null));
        }

        List<Object[]> allResultRows = new ArrayList<>();

        for (List<Expression> groupingSet : groupingSets) {
            List<Expression> effectiveGroupBy = new ArrayList<>(fixedGroupBy);
            effectiveGroupBy.addAll(groupingSet);

            Set<String> groupingSetColNames = new HashSet<>();
            for (Expression e : effectiveGroupBy) {
                if (e instanceof ColumnRef) groupingSetColNames.add(((ColumnRef) e).column().toLowerCase());
                else groupingSetColNames.add(executor.exprToAlias(e).toLowerCase());
            }
            currentGroupingSetColumns.set(groupingSetColNames);

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
                        key.append(val == null ? "\0NULL" : val.toString()).append('\1');
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
                Object[] row = new Object[stmt.targets().size()];
                for (int i = 0; i < stmt.targets().size(); i++) {
                    SelectStmt.SelectTarget target = stmt.targets().get(i);
                    Expression expr = target.expr();
                    if (!select.isAggregateOrConstant(expr)) {
                        boolean inGroupBy = false;
                        for (Expression ge : effectiveGroupBy) {
                            if (ge.equals(expr)) { inGroupBy = true; break; }
                            if (ge instanceof ColumnRef && expr instanceof ColumnRef
                                    && ((ColumnRef) ge).column().equalsIgnoreCase(((ColumnRef) expr).column())) {
                                ColumnRef ecr = (ColumnRef) expr;
                                ColumnRef gcr = (ColumnRef) ge;
                                inGroupBy = true; break;
                            }
                        }
                        if (!inGroupBy) {
                            row[i] = null;
                            continue;
                        }
                    }
                    row[i] = evalAggregateExpr(expr, group, representative);
                }

                if (stmt.having() != null) {
                    Object havingResult = evalAggregateExpr(stmt.having(), group, representative);
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

    QueryResult executeAggregateSelect(SelectStmt stmt, List<RowContext> contexts,
                                        List<RowContext.TableBinding> baseBindings) {
        if (stmt.groupingSets() != null && !stmt.groupingSets().isEmpty()) {
            return executeGroupingSetsSelect(stmt, contexts, baseBindings);
        }

        // Resolve GROUP BY ordinals and aliases
        List<Expression> resolvedGroupBy = stmt.groupBy();
        if (resolvedGroupBy != null) {
            resolvedGroupBy = new ArrayList<>(resolvedGroupBy);
            for (int i = 0; i < resolvedGroupBy.size(); i++) {
                Expression expr = resolvedGroupBy.get(i);
                if (expr instanceof Literal && ((Literal) expr).literalType() == Literal.LiteralType.INTEGER) {
                    Literal lit = (Literal) expr;
                    int ordinal = Integer.parseInt(lit.value());
                    if (ordinal >= 1 && ordinal <= stmt.targets().size()) {
                        resolvedGroupBy.set(i, stmt.targets().get(ordinal - 1).expr());
                    }
                } else if (expr instanceof ColumnRef && ((ColumnRef) expr).table() == null) {
                    ColumnRef colRef = (ColumnRef) expr;
                    for (SelectStmt.SelectTarget target : stmt.targets()) {
                        if (target.alias() != null && target.alias().equalsIgnoreCase(colRef.column())) {
                            resolvedGroupBy.set(i, target.expr());
                            break;
                        }
                    }
                }
            }
        }

        boolean hasGroupBy = resolvedGroupBy != null && !resolvedGroupBy.isEmpty();
        List<List<RowContext>> groups;

        if (hasGroupBy) {
            Map<String, List<RowContext>> groupMap = new LinkedHashMap<>();
            for (RowContext ctx : contexts) {
                StringBuilder keyBuilder = new StringBuilder();
                for (Expression groupExpr : resolvedGroupBy) {
                    Object val = executor.evalExpr(groupExpr, ctx);
                    keyBuilder.append(val == null ? "\0NULL" : val.toString()).append('\1');
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
            resultColumns.add(new Column(alias, executor.inferTypeFromContext(target.expr(), baseBindings), true, false, null));
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
        List<SelectStmt.OrderByItem> resolvedOrderBy = select.resolveOrderBy(stmt.orderBy(), stmt.targets());
        if (resolvedOrderBy != null && !resolvedOrderBy.isEmpty()) {
            resultRows.sort((a, b) -> {
                for (SelectStmt.OrderByItem item : resolvedOrderBy) {
                    int colIdx = select.resolveOrderByToColumnIndex(item.expr(), stmt.targets());
                    Object va, vb;
                    if (colIdx >= 0) {
                        va = a[colIdx];
                        vb = b[colIdx];
                    } else {
                        va = a[0]; vb = b[0];
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
            String name = fn.name().toLowerCase();
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
                        resolvedArgs.add(Literal.ofString(val.toString()));
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
        } else if (expr instanceof UnaryExpr) {
            UnaryExpr un = (UnaryExpr) expr;
            Object val = evalAggregateExpr(un.operand(), group, representative);
            return executor.evalUnaryValue(un.op(), val);
        } else if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            Object val = evalAggregateExpr(cast.expr(), group, representative);
            return executor.castEvaluator.applyCast(val, cast.typeName());
        } else if (expr instanceof OrderedSetAggExpr) {
            OrderedSetAggExpr osa = (OrderedSetAggExpr) expr;
            return evalOrderedSetAggregate(osa, group);
        } else if (expr instanceof WindowFuncExpr) {
            return null;
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
                double fraction;
                try { fraction = executor.toDouble(fractionObj); } catch (Exception e) {
                    throw new MemgresException("percentile fraction must be between 0 and 1", "22023");
                }
                if (fraction < 0.0 || fraction > 1.0)
                    throw new MemgresException("percentile fraction must be between 0 and 1", "22023");
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
                double fraction;
                try { fraction = executor.toDouble(fractionObj); } catch (Exception e) {
                    throw new MemgresException("percentile fraction must be between 0 and 1", "22023");
                }
                if (fraction < 0.0 || fraction > 1.0)
                    throw new MemgresException("percentile fraction must be between 0 and 1", "22023");
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
                if (vals.isEmpty()) return 1.0;
                long countLE = 0;
                for (Object v : vals) {
                    if (executor.compareValues(v, hypoObj) <= 0) countLE++;
                }
                return (double) countLE / vals.size();
            }
            default: {
                return null; 
            }
        }
    }

    // ---- Core aggregate evaluation ----

    Object evalAggregate(FunctionCallExpr fn, List<RowContext> group) {
        String name = fn.name().toLowerCase();

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
                        if (val != null) seen.add(val.toString());
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
                BigDecimal bdSum = BigDecimal.ZERO;
                try {
                    if (fn.distinct()) {
                        Set<Object> seen = new LinkedHashSet<>();
                        for (RowContext ctx : group) {
                            Object val = executor.evalExpr(arg, ctx);
                            if (val != null) seen.add(val);
                        }
                        for (Object val : seen) {
                            hasValue = true;
                            bdSum = bdSum.add(SelectExecutor.toBigDecimal(val));
                            if (!(val instanceof Integer || val instanceof Long)) allInts = false;
                        }
                    } else {
                        for (RowContext ctx : group) {
                            Object val = executor.evalExpr(arg, ctx);
                            if (val != null) {
                                hasValue = true;
                                bdSum = bdSum.add(SelectExecutor.toBigDecimal(val));
                                if (!(val instanceof Integer || val instanceof Long)) allInts = false;
                            }
                        }
                    }
                } catch (NumberFormatException e) {
                    throw new MemgresException("function sum(text) does not exist\n  Hint: No function matches the given name and argument types.", "42883");
                }
                if (!hasValue) return null;
                if (allInts) {
                    try { return bdSum.longValueExact(); } catch (ArithmeticException e) { /* fall through */ }
                }
                return bdSum;
            }
            case "avg": {
                if (group.isEmpty()) return null;
                Expression arg = fn.args().get(0);
                BigDecimal bdSum = BigDecimal.ZERO;
                long count = 0;
                if (fn.distinct()) {
                    Set<Object> seen = new LinkedHashSet<>();
                    for (RowContext ctx : group) {
                        Object val = executor.evalExpr(arg, ctx);
                        if (val != null) seen.add(val);
                    }
                    for (Object val : seen) {
                        bdSum = bdSum.add(SelectExecutor.toBigDecimal(val));
                        count++;
                    }
                } else {
                    for (RowContext ctx : group) {
                        Object val = executor.evalExpr(arg, ctx);
                        if (val != null) {
                            bdSum = bdSum.add(SelectExecutor.toBigDecimal(val));
                            count++;
                        }
                    }
                }
                if (count == 0) return null;
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
                String delim = delimExpr != null ? String.valueOf(executor.evalExpr(delimExpr, group.isEmpty() ? null : group.get(0))) : ",";
                List<RowContext> orderedGroup = sortGroupForAggregate(group, fn);
                StringBuilder sb = new StringBuilder();
                boolean first = true;
                Set<String> seen = fn.distinct() ? new LinkedHashSet<>() : null;
                for (RowContext ctx : orderedGroup) {
                    Object val = executor.evalExpr(arg, ctx);
                    if (val != null) {
                        String sv = val.toString();
                        if (seen != null && !seen.add(sv)) continue;
                        if (!first) sb.append(delim);
                        sb.append(sv);
                        first = false;
                    }
                }
                return first ? null : sb.toString();
            }
            case "array_agg": {
                if (group.isEmpty()) return null;
                Expression arg = fn.args().get(0);
                List<RowContext> orderedGroup = sortGroupForAggregate(group, fn);
                List<Object> list = new ArrayList<>();
                Set<String> seen = fn.distinct() ? new LinkedHashSet<>() : null;
                for (RowContext ctx : orderedGroup) {
                    Object val = executor.evalExpr(arg, ctx);
                    if (seen != null && val != null && !seen.add(val.toString())) continue;
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
                    else sb.append(v);
                }
                sb.append("}");
                return sb.toString();
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
                Integer result = null;
                for (RowContext r : group) {
                    Object v = executor.evalExpr(fn.args().get(0), r);
                    if (v != null) {
                        int iv = ((Number) v).intValue();
                        result = (result == null) ? iv : (result & iv);
                    }
                }
                return result;
            }
            case "bit_or": {
                Integer result = null;
                for (RowContext r : group) {
                    Object v = executor.evalExpr(fn.args().get(0), r);
                    if (v != null) {
                        int iv = ((Number) v).intValue();
                        result = (result == null) ? iv : (result | iv);
                    }
                }
                return result;
            }
            case "json_agg":
            case "jsonb_agg": {
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
                StringBuilder sb = new StringBuilder("{");
                boolean first = true;
                for (RowContext r : group) {
                    Object k = executor.evalExpr(fn.args().get(0), r);
                    Object v = executor.evalExpr(fn.args().get(1), r);
                    if (k == null) continue;
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
            case "xmlagg": {
                StringBuilder sb = new StringBuilder();
                for (RowContext r : group) {
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
                return variance != null ? variance.setScale(16, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString() : null;
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
                return BigDecimal.valueOf(Math.sqrt(variance)).stripTrailingZeros().toPlainString();
            }
            case "grouping": {
                Set<String> currentGroupSet = currentGroupingSetColumns.get();
                if (currentGroupSet == null) {
                    throw new MemgresException("GROUPING is not supported without GROUPING SETS, ROLLUP, or CUBE", "42803");
                }
                if (fn.args().isEmpty()) return 0;
                Expression arg = fn.args().get(0);
                String colName = arg instanceof ColumnRef ? ((ColumnRef) arg).column().toLowerCase() :
                        executor.exprToAlias(arg).toLowerCase();
                return currentGroupSet.contains(colName) ? 0 : 1;
            }
            case "corr": {
                RegressionData rd = RegressionData.compute(group, fn.args(), executor);
                if (rd == null) return null;
                if (rd.sumXDiffSq.compareTo(BigDecimal.ZERO) == 0 || rd.sumYDiffSq.compareTo(BigDecimal.ZERO) == 0) return null;
                BigDecimal denom = new BigDecimal(Math.sqrt(rd.sumXDiffSq.doubleValue()) * Math.sqrt(rd.sumYDiffSq.doubleValue()));
                BigDecimal corrVal = rd.sumXYDiff.divide(denom, 16, RoundingMode.HALF_UP);
                return corrVal.stripTrailingZeros().toPlainString();
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

        // For DISTINCT, track seen value tuples
        Set<String> seen = fn.distinct() ? new LinkedHashSet<>() : null;

        // For each row, call SFUNC(state, value[, ...])
        for (RowContext ctx : group) {
            List<Object> argValues = new ArrayList<>();
            for (Expression arg : fn.args()) {
                argValues.add(executor.evalExpr(arg, ctx));
            }

            // DISTINCT: skip duplicate value tuples
            if (seen != null) {
                String key = argValues.stream()
                        .map(v -> v == null ? "\0" : v.toString())
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

    /** Sort a group by the aggregate's ORDER BY clause (used by string_agg, array_agg, json_agg, etc.). */
    private List<RowContext> sortGroupForAggregate(List<RowContext> group, FunctionCallExpr fn) {
        List<RowContext> orderedGroup = new ArrayList<>(group);
        if (fn.orderBy() != null && !fn.orderBy().isEmpty()) {
            orderedGroup.sort((a, b) -> {
                for (SelectStmt.OrderByItem item : fn.orderBy()) {
                    Object va = executor.evalExpr(item.expr(), a);
                    Object vb = executor.evalExpr(item.expr(), b);
                    int cmp = executor.compareValues(va, vb);
                    if (item.descending()) cmp = -cmp;
                    if (cmp != 0) return cmp;
                }
                return 0;
            });
        }
        return orderedGroup;
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

        public RegressionData(
                BigDecimal xMean,
                BigDecimal yMean,
                BigDecimal sumXYDiff,
                BigDecimal sumXDiffSq,
                BigDecimal sumYDiffSq
        ) {
            this.xMean = xMean;
            this.yMean = yMean;
            this.sumXYDiff = sumXYDiff;
            this.sumXDiffSq = sumXDiffSq;
            this.sumYDiffSq = sumYDiffSq;
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
            if (xVals.size() < 2) return null;
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
            return new RegressionData(xMean, yMean, sumXYDiff, sumXDiffSq, sumYDiffSq);
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
}
