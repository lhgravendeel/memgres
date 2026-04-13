package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.*;

/**
 * Executes JOIN operations for FROM clause resolution.
 * Extracted from FromResolver to separate concerns.
 */
class FromJoinExecutor {
    private final FromResolver fromResolver;
    private final AstExecutor executor;

    FromJoinExecutor(FromResolver fromResolver) {
        this.fromResolver = fromResolver;
        this.executor = fromResolver.executor;
    }

    /**
     * Execute a JOIN operation.
     */
    List<RowContext> executeJoin(SelectStmt.JoinFrom join) {
        List<RowContext> leftContexts = fromResolver.resolveFromItem(join.left());

        boolean lateral = join.right() instanceof SelectStmt.SubqueryFrom && ((SelectStmt.SubqueryFrom) join.right()).lateral();
        boolean funcLateral = join.right() instanceof SelectStmt.FunctionFrom;
        if (lateral) {
            return executeLateralJoin(join, leftContexts);
        }
        if (funcLateral) {
            return executeFunctionLateralJoin(join, leftContexts);
        }

        List<RowContext> rightContexts = fromResolver.resolveFromItem(join.right());

        switch (join.joinType()) {
            case INNER:
                return executeInnerJoin(leftContexts, rightContexts, join.on(), join.using());
            case LEFT:
                return executeLeftJoin(leftContexts, rightContexts, join.on(), join.using());
            case RIGHT:
                return executeRightJoin(leftContexts, rightContexts, join.on(), join.using());
            case FULL:
                return executeFullJoin(leftContexts, rightContexts, join.on(), join.using());
            case CROSS:
                return executeCrossJoin(leftContexts, rightContexts);
            case NATURAL:
                return executeNaturalJoin(leftContexts, rightContexts, SelectStmt.JoinType.INNER);
            case NATURAL_LEFT:
                return executeNaturalJoin(leftContexts, rightContexts, SelectStmt.JoinType.LEFT);
            case NATURAL_RIGHT:
                return executeNaturalJoin(leftContexts, rightContexts, SelectStmt.JoinType.RIGHT);
            case NATURAL_FULL:
                return executeNaturalJoin(leftContexts, rightContexts, SelectStmt.JoinType.FULL);
            default:
                throw new IllegalStateException("Unknown join type: " + join.joinType());
        }
    }

    // ---- LATERAL JOIN ----

    private List<RowContext> executeLateralJoin(SelectStmt.JoinFrom join, List<RowContext> leftContexts) {
        List<RowContext> results = new ArrayList<>();
        SelectStmt.SubqueryFrom sqf = (SelectStmt.SubqueryFrom) join.right();
        boolean isLeft = join.joinType() == SelectStmt.JoinType.LEFT;

        for (RowContext leftCtx : leftContexts) {
            executor.outerContextStack.push(leftCtx);
            try {
                QueryResult subResult;
                if (sqf.subquery() instanceof SelectStmt) {
                    SelectStmt sel = (SelectStmt) sqf.subquery();
                    subResult = executor.executeSelect(sel);
                } else {
                    subResult = executor.executeStatement(sqf.subquery());
                }
                String alias = sqf.alias() != null ? sqf.alias() : "subquery";
                Table virtualTable = new Table(alias, subResult.getColumns());
                boolean matched = false;
                for (Object[] row : subResult.getRows()) {
                    RowContext rightCtx = new RowContext(virtualTable, alias, row);
                    RowContext merged = mergeContexts(leftCtx, rightCtx);
                    if (join.on() != null) {
                        if (executor.isTruthy(executor.evalExpr(join.on(), merged))) {
                            results.add(merged);
                            matched = true;
                        }
                    } else {
                        results.add(merged);
                        matched = true;
                    }
                }
                if (!matched && isLeft) {
                    Object[] nullRow = new Object[virtualTable.getColumns().size()];
                    RowContext rightCtx = new RowContext(virtualTable, alias, nullRow);
                    results.add(mergeContexts(leftCtx, rightCtx));
                }
            } finally {
                executor.outerContextStack.pop();
            }
        }
        return results;
    }

    /**
     * Execute a join where the right side is a FunctionFrom, treated as lateral.
     */
    private List<RowContext> executeFunctionLateralJoin(SelectStmt.JoinFrom join, List<RowContext> leftContexts) {
        SelectStmt.FunctionFrom funcFrom = (SelectStmt.FunctionFrom) join.right();
        List<RowContext> results = new ArrayList<>();
        boolean isLeft = join.joinType() == SelectStmt.JoinType.LEFT;

        for (RowContext leftCtx : leftContexts) {
            executor.outerContextStack.push(leftCtx);
            try {
                List<RowContext> funcRows = fromResolver.functionResolver.resolveFunctionFrom(funcFrom);
                boolean matched = false;
                for (RowContext rightCtx : funcRows) {
                    RowContext merged = mergeContexts(leftCtx, rightCtx);
                    if (join.on() != null) {
                        if (executor.isTruthy(executor.evalExpr(join.on(), merged))) {
                            results.add(merged);
                            matched = true;
                        }
                    } else {
                        results.add(merged);
                        matched = true;
                    }
                }
                if (!matched && isLeft) {
                    String alias = funcFrom.alias() != null ? funcFrom.alias() : funcFrom.functionName();
                    List<Column> cols;
                    // For JSON_TABLE, extract column definitions from the JsonTableExpr
                    if (funcFrom.functionName().equals("__json_table__") && !funcFrom.args().isEmpty()
                            && funcFrom.args().get(0) instanceof JsonTableExpr) {
                        JsonTableExpr jt = (JsonTableExpr) funcFrom.args().get(0);
                        cols = new ArrayList<>();
                        collectJsonTableNullCols(jt.columns, cols);
                    } else if (funcFrom.columnAliases() != null) {
                        cols = funcFrom.columnAliases().stream()
                                .map(cn -> new Column(cn, DataType.TEXT, true, false, null))
                                .collect(java.util.stream.Collectors.toList());
                    } else {
                        cols = Cols.listOf();
                    }
                    Table virtualTable = new Table(alias, cols);
                    Object[] nullRow = new Object[cols.size()];
                    RowContext rightCtx = new RowContext(virtualTable, alias, nullRow);
                    results.add(mergeContexts(leftCtx, rightCtx));
                }
            } finally {
                executor.outerContextStack.pop();
            }
        }
        return results;
    }

    // ---- INNER JOIN ----

    private List<RowContext> executeInnerJoin(List<RowContext> left, List<RowContext> right,
                                               Expression on, List<String> using) {
        // Try hash join for large datasets
        if (on != null && using == null && left.size() > 0 && right.size() > 0
                && (long) left.size() * right.size() > 1000) {
            List<ColumnRef[]> equiKeys = extractEquiJoinKeys(on, left, right);
            if (equiKeys != null && !equiKeys.isEmpty()) {
                return executeHashInnerJoin(left, right, on, equiKeys);
            }
        }
        if (using != null && left.size() > 0 && right.size() > 0
                && (long) left.size() * right.size() > 1000) {
            return executeHashInnerJoinUsing(left, right, using);
        }
        // Nested loop fallback
        List<RowContext> result = new ArrayList<>();
        for (RowContext l : left) {
            for (RowContext r : right) {
                RowContext merged = mergeContexts(l, r);
                if (matchesJoinCondition(merged, on, using)) {
                    result.add(merged);
                }
            }
        }
        return deduplicateUsingColumns(result, using);
    }

    private List<RowContext> executeHashInnerJoin(List<RowContext> left, List<RowContext> right,
                                                   Expression on, List<ColumnRef[]> equiKeys) {
        Map<String, List<RowContext>> rightIndex = new HashMap<>();
        for (RowContext r : right) {
            String key = toJoinKey(equiKeys, r, false);
            if (key != null) {
                rightIndex.computeIfAbsent(key, k -> new ArrayList<>()).add(r);
            }
        }

        List<RowContext> result = new ArrayList<>();
        for (RowContext l : left) {
            String key = toJoinKey(equiKeys, l, true);
            if (key == null) continue;
            List<RowContext> candidates = rightIndex.get(key);
            if (candidates == null) continue;
            for (RowContext r : candidates) {
                RowContext merged = mergeContexts(l, r);
                if (executor.isTruthy(executor.evalExpr(on, merged))) {
                    result.add(merged);
                }
            }
        }
        return result;
    }

    private List<RowContext> executeHashInnerJoinUsing(List<RowContext> left, List<RowContext> right,
                                                        List<String> using) {
        Map<String, List<RowContext>> rightIndex = new HashMap<>();
        for (RowContext r : right) {
            String key = buildUsingKey(r, using);
            if (key != null) {
                rightIndex.computeIfAbsent(key, k -> new ArrayList<>()).add(r);
            }
        }

        List<RowContext> result = new ArrayList<>();
        for (RowContext l : left) {
            String key = buildUsingKey(l, using);
            if (key == null) continue;
            List<RowContext> candidates = rightIndex.get(key);
            if (candidates == null) continue;
            for (RowContext r : candidates) {
                result.add(mergeContexts(l, r));
            }
        }
        return deduplicateUsingColumns(result, using);
    }

    // ---- LEFT JOIN ----

    private List<RowContext> executeLeftJoin(List<RowContext> left, List<RowContext> right,
                                              Expression on, List<String> using) {
        List<RowContext.TableBinding> rightTemplate;
        if (!right.isEmpty()) {
            rightTemplate = right.get(0).getBindings();
        } else if (fromResolver.lastResolvedRightTable != null) {
            rightTemplate = Cols.listOf(new RowContext.TableBinding(
                    fromResolver.lastResolvedRightTable, fromResolver.lastResolvedRightAlias,
                    new Object[fromResolver.lastResolvedRightTable.getColumns().size()]));
        } else {
            rightTemplate = Cols.listOf();
        }

        // Try hash join
        if (on != null && using == null && left.size() > 0 && right.size() > 0
                && (long) left.size() * right.size() > 1000) {
            List<ColumnRef[]> equiKeys = extractEquiJoinKeys(on, left, right);
            if (equiKeys != null && !equiKeys.isEmpty()) {
                return executeHashLeftJoin(left, right, on, equiKeys, rightTemplate);
            }
        }
        if (using != null && left.size() > 0 && right.size() > 0
                && (long) left.size() * right.size() > 1000) {
            return executeHashLeftJoinUsing(left, right, using, rightTemplate);
        }

        // Nested loop fallback
        List<RowContext> result = new ArrayList<>();
        for (RowContext l : left) {
            boolean matched = false;
            for (RowContext r : right) {
                RowContext merged = mergeContexts(l, r);
                if (matchesJoinCondition(merged, on, using)) {
                    result.add(merged);
                    matched = true;
                }
            }
            if (!matched) {
                result.add(mergeWithNullRight(l, rightTemplate));
            }
        }
        return deduplicateUsingColumns(result, using);
    }

    private List<RowContext> executeHashLeftJoin(List<RowContext> left, List<RowContext> right,
                                                  Expression on, List<ColumnRef[]> equiKeys,
                                                  List<RowContext.TableBinding> rightTemplate) {
        Map<String, List<RowContext>> rightIndex = new HashMap<>();
        for (RowContext r : right) {
            String key = toJoinKey(equiKeys, r, false);
            if (key != null) {
                rightIndex.computeIfAbsent(key, k -> new ArrayList<>()).add(r);
            }
        }

        List<RowContext> result = new ArrayList<>();
        for (RowContext l : left) {
            String key = toJoinKey(equiKeys, l, true);
            boolean matched = false;
            if (key != null) {
                List<RowContext> candidates = rightIndex.get(key);
                if (candidates != null) {
                    for (RowContext r : candidates) {
                        RowContext merged = mergeContexts(l, r);
                        if (executor.isTruthy(executor.evalExpr(on, merged))) {
                            result.add(merged);
                            matched = true;
                        }
                    }
                }
            }
            if (!matched) {
                result.add(mergeWithNullRight(l, rightTemplate));
            }
        }
        return result;
    }

    private List<RowContext> executeHashLeftJoinUsing(List<RowContext> left, List<RowContext> right,
                                                       List<String> using,
                                                       List<RowContext.TableBinding> rightTemplate) {
        Map<String, List<RowContext>> rightIndex = new HashMap<>();
        for (RowContext r : right) {
            String key = buildUsingKey(r, using);
            if (key != null) {
                rightIndex.computeIfAbsent(key, k -> new ArrayList<>()).add(r);
            }
        }

        List<RowContext> result = new ArrayList<>();
        for (RowContext l : left) {
            String key = buildUsingKey(l, using);
            boolean matched = false;
            if (key != null) {
                List<RowContext> candidates = rightIndex.get(key);
                if (candidates != null) {
                    for (RowContext r : candidates) {
                        result.add(mergeContexts(l, r));
                        matched = true;
                    }
                }
            }
            if (!matched) {
                result.add(mergeWithNullRight(l, rightTemplate));
            }
        }
        return deduplicateUsingColumns(result, using);
    }

    // ---- RIGHT JOIN ----

    private List<RowContext> executeRightJoin(List<RowContext> left, List<RowContext> right,
                                               Expression on, List<String> using) {
        List<RowContext> result = new ArrayList<>();
        List<RowContext.TableBinding> leftTemplate = left.isEmpty() ?
                Cols.listOf() : left.get(0).getBindings();

        for (RowContext r : right) {
            boolean matched = false;
            for (RowContext l : left) {
                RowContext merged = mergeContexts(l, r);
                if (matchesJoinCondition(merged, on, using)) {
                    result.add(merged);
                    matched = true;
                }
            }
            if (!matched) {
                result.add(mergeWithNullLeft(leftTemplate, r));
            }
        }
        return deduplicateUsingColumns(result, using);
    }

    // ---- FULL JOIN ----

    private List<RowContext> executeFullJoin(List<RowContext> left, List<RowContext> right,
                                              Expression on, List<String> using) {
        List<RowContext> result = new ArrayList<>();
        List<RowContext.TableBinding> leftTemplate = left.isEmpty() ?
                Cols.listOf() : left.get(0).getBindings();
        List<RowContext.TableBinding> rightTemplate = right.isEmpty() ?
                Cols.listOf() : right.get(0).getBindings();

        Set<Integer> matchedRight = new HashSet<>();

        for (RowContext l : left) {
            boolean matched = false;
            for (int ri = 0; ri < right.size(); ri++) {
                RowContext r = right.get(ri);
                RowContext merged = mergeContexts(l, r);
                if (matchesJoinCondition(merged, on, using)) {
                    result.add(merged);
                    matched = true;
                    matchedRight.add(ri);
                }
            }
            if (!matched) {
                result.add(mergeWithNullRight(l, rightTemplate));
            }
        }

        for (int ri = 0; ri < right.size(); ri++) {
            if (!matchedRight.contains(ri)) {
                result.add(mergeWithNullLeft(leftTemplate, right.get(ri)));
            }
        }

        return deduplicateUsingColumns(result, using);
    }

    // ---- CROSS JOIN ----

    private List<RowContext> executeCrossJoin(List<RowContext> left, List<RowContext> right) {
        List<RowContext> result = new ArrayList<>();
        for (RowContext l : left) {
            for (RowContext r : right) {
                result.add(mergeContexts(l, r));
            }
        }
        return result;
    }

    // ---- NATURAL JOIN ----

    private List<RowContext> executeNaturalJoin(List<RowContext> left, List<RowContext> right, SelectStmt.JoinType subType) {
        if (left.isEmpty() && subType == SelectStmt.JoinType.INNER) return Cols.listOf();
        if (right.isEmpty() && subType == SelectStmt.JoinType.INNER) return Cols.listOf();

        Set<String> leftCols = new HashSet<>();
        if (!left.isEmpty()) {
            for (RowContext.TableBinding b : left.get(0).getBindings()) {
                for (Column c : b.table().getColumns()) leftCols.add(c.getName().toLowerCase());
            }
        }

        List<String> commonCols = new ArrayList<>();
        if (!right.isEmpty()) {
            for (RowContext.TableBinding b : right.get(0).getBindings()) {
                for (Column c : b.table().getColumns()) {
                    if (leftCols.contains(c.getName().toLowerCase())) commonCols.add(c.getName());
                }
            }
        }

        List<String> using = commonCols.isEmpty() ? null : commonCols;
        switch (subType) {
            case LEFT:
                return executeLeftJoin(left, right, null, using);
            case RIGHT:
                return executeRightJoin(left, right, null, using);
            case FULL:
                return executeFullJoin(left, right, null, using);
            default:
                return executeInnerJoin(left, right, null, using);
        }
    }

    // ---- Merge / null-padding helpers ----

    RowContext mergeContexts(RowContext left, RowContext right) {
        List<RowContext.TableBinding> merged = new ArrayList<>(left.getBindings());
        merged.addAll(right.getBindings());
        return new RowContext(merged);
    }

    private RowContext mergeWithNullRight(RowContext left, List<RowContext.TableBinding> rightTemplate) {
        List<RowContext.TableBinding> merged = new ArrayList<>(left.getBindings());
        for (RowContext.TableBinding b : rightTemplate) {
            Object[] nullRow = new Object[b.table().getColumns().size()];
            merged.add(new RowContext.TableBinding(b.table(), b.alias(), nullRow));
        }
        RowContext ctx = new RowContext(merged);
        ctx.setOuterJoinNullPadded(true);
        return ctx;
    }

    private RowContext mergeWithNullLeft(List<RowContext.TableBinding> leftTemplate, RowContext right) {
        List<RowContext.TableBinding> merged = new ArrayList<>();
        for (RowContext.TableBinding b : leftTemplate) {
            Object[] nullRow = new Object[b.table().getColumns().size()];
            merged.add(new RowContext.TableBinding(b.table(), b.alias(), nullRow));
        }
        merged.addAll(right.getBindings());
        RowContext ctx = new RowContext(merged);
        ctx.setOuterJoinNullPadded(true);
        return ctx;
    }

    // ---- Join condition matching ----

    private boolean matchesJoinCondition(RowContext merged, Expression on, List<String> using) {
        if (on != null) {
            return executor.isTruthy(executor.evalExpr(on, merged));
        }
        if (using != null) {
            for (String col : using) {
                Object leftVal = null, rightVal = null;
                boolean foundLeft = false, foundRight = false;
                for (RowContext.TableBinding b : merged.getBindings()) {
                    int idx = b.table().getColumnIndex(col);
                    if (idx >= 0) {
                        if (!foundLeft) {
                            leftVal = b.row()[idx];
                            foundLeft = true;
                        } else {
                            rightVal = b.row()[idx];
                            foundRight = true;
                            break;
                        }
                    }
                }
                if (!foundLeft) {
                    throw new MemgresException("column \"" + col + "\" specified in USING clause does not exist in left table", "42703");
                }
                if (!foundRight) {
                    throw new MemgresException("column \"" + col + "\" specified in USING clause does not exist in right table", "42703");
                }
                if (leftVal == null || rightVal == null) return false;
                if (!Objects.equals(leftVal, rightVal) && !leftVal.toString().equals(rightVal.toString())) {
                    return false;
                }
            }
            return true;
        }
        return true;
    }

    // ---- USING column deduplication ----

    private List<RowContext> deduplicateUsingColumns(List<RowContext> results, List<String> usingCols) {
        if (usingCols == null || usingCols.isEmpty() || results.isEmpty()) return results;

        Set<String> usingLower = new HashSet<>();
        for (String col : usingCols) {
            usingLower.add(col.toLowerCase());
        }

        for (RowContext ctx : results) {
            Set<String> existing = ctx.getUsingColumns();
            if (existing != null) {
                Set<String> merged = new HashSet<>(existing);
                merged.addAll(usingLower);
                ctx.setUsingColumns(merged);
            } else {
                ctx.setUsingColumns(usingLower);
            }
        }
        return results;
    }

    // ---- Hash join key extraction ----

    private List<ColumnRef[]> extractEquiJoinKeys(Expression on, List<RowContext> left, List<RowContext> right) {
        if (on == null) return null;
        List<ColumnRef[]> keys = new ArrayList<>();
        if (!collectEquiJoinKeys(on, left, right, keys)) return null;
        return keys.isEmpty() ? null : keys;
    }

    private boolean collectEquiJoinKeys(Expression expr, List<RowContext> left, List<RowContext> right,
                                         List<ColumnRef[]> keys) {
        if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            if (bin.op() == BinaryExpr.BinOp.AND) {
                collectEquiJoinKeys(bin.left(), left, right, keys);
                collectEquiJoinKeys(bin.right(), left, right, keys);
                return true;
            }
            if (bin.op() == BinaryExpr.BinOp.EQUAL) {
                if (bin.left() instanceof ColumnRef && bin.right() instanceof ColumnRef) {
                    ColumnRef rightRef = (ColumnRef) bin.right();
                    ColumnRef leftRef = (ColumnRef) bin.left();
                    boolean leftRefOnLeft = belongsToSide(leftRef, left);
                    boolean rightRefOnRight = belongsToSide(rightRef, right);
                    if (leftRefOnLeft && rightRefOnRight) {
                        keys.add(new ColumnRef[]{leftRef, rightRef});
                        return true;
                    }
                    boolean leftRefOnRight = belongsToSide(leftRef, right);
                    boolean rightRefOnLeft = belongsToSide(rightRef, left);
                    if (rightRefOnLeft && leftRefOnRight) {
                        keys.add(new ColumnRef[]{rightRef, leftRef});
                        return true;
                    }
                }
            }
        }
        return true;
    }

    private boolean belongsToSide(ColumnRef ref, List<RowContext> side) {
        if (side.isEmpty()) return false;
        RowContext sample = side.get(0);
        if (ref.table() != null) {
            RowContext.TableBinding b = sample.getBinding(ref.table());
            return b != null && b.table().getColumnIndex(ref.column()) >= 0;
        }
        for (RowContext.TableBinding b : sample.getBindings()) {
            if (b.table().getColumnIndex(ref.column()) >= 0) return true;
        }
        return false;
    }

    private String toJoinKey(List<ColumnRef[]> keys, RowContext ctx, boolean leftSide) {
        StringBuilder sb = new StringBuilder();
        for (ColumnRef[] pair : keys) {
            ColumnRef ref = leftSide ? pair[0] : pair[1];
            Object val = ctx.resolveColumn(ref.table(), ref.column());
            if (val == null) return null;
            if (sb.length() > 0) sb.append('\0');
            sb.append(val.toString());
        }
        return sb.toString();
    }

    private String buildUsingKey(RowContext ctx, List<String> using) {
        StringBuilder sb = new StringBuilder();
        for (String col : using) {
            for (RowContext.TableBinding b : ctx.getBindings()) {
                int idx = b.table().getColumnIndex(col);
                if (idx >= 0) {
                    Object val = b.row()[idx];
                    if (val == null) return null;
                    if (sb.length() > 0) sb.append('\0');
                    sb.append(val.toString());
                    break;
                }
            }
        }
        return sb.toString();
    }

    /** Recursively collect leaf column definitions from JSON_TABLE columns for null-padded LEFT JOIN rows. */
    private void collectJsonTableNullCols(List<JsonTableExpr.JsonTableColumn> columns, List<Column> cols) {
        for (JsonTableExpr.JsonTableColumn col : columns) {
            if (col.nestedColumns != null) {
                collectJsonTableNullCols(col.nestedColumns, cols);
            } else {
                cols.add(new Column(col.name, col.forOrdinality ? DataType.INTEGER : DataType.TEXT, true, false, null));
            }
        }
    }
}
