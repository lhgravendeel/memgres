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

    // ---- What a join exposes ----

    /**
     * The two columns a USING or NATURAL join equates, each named by where it is read from.
     *
     * <p>The left one is a column of the left <em>output</em>, not of a relation: after
     * {@code a JOIN b USING (id)} the left side of a further {@code USING (id)} is the merged
     * {@code id}, whose value falls back through both relations. Reading it as one relation's
     * column is what turned a chain of USING joins into a cross product — the second clause then
     * compared {@code a.id} with {@code b.id}, which the first join had already made equal, and
     * never looked at the third relation at all.
     */
    static final class UsingKey {
        final String name;
        final RowContext.OutCol left;   // read against the left side's own bindings
        final RowContext.OutCol right;  // read against the right side's own bindings

        UsingKey(String name, RowContext.OutCol left, RowContext.OutCol right) {
            this.name = name;
            this.left = left;
            this.right = right;
        }
    }

    /** What a join exposes and what it joins on, worked out from what its two sides expose. */
    static final class JoinShape {
        final List<RowContext.OutCol> output;
        final List<UsingKey> keys;

        JoinShape(List<RowContext.OutCol> output, List<UsingKey> keys) {
            this.output = output;
            this.keys = keys;
        }
    }

    /** The join types that find their own common columns rather than being given them. */
    static boolean isNatural(SelectStmt.JoinType type) {
        return type == SelectStmt.JoinType.NATURAL || type == SelectStmt.JoinType.NATURAL_LEFT
                || type == SelectStmt.JoinType.NATURAL_RIGHT || type == SelectStmt.JoinType.NATURAL_FULL;
    }

    /**
     * The columns a join exposes, in PostgreSQL's order: every merged column first, in the order
     * USING names them or a NATURAL join finds them, then what is left of the left side and then
     * of the right side, each in its own order. {@code a(id,s,p) JOIN b(id,s,q) USING (s)} is
     * therefore {@code s, id, p, id, q} — not the left side with a column crossed out.
     *
     * <p>A name a USING clause gives has to be one column of each side. Two columns of the left
     * answering to it is {@code 42702 common column name "x" appears more than once in left
     * table}, none is {@code 42703 column "x" specified in USING clause does not exist in left
     * table}, and the same of the right; the name is matched as written, so a quoted {@code "ID"}
     * does not find a column named {@code id}.
     */
    static JoinShape shapeOfJoin(List<RowContext.OutCol> leftOut, int leftBindingCount,
                                 List<RowContext.OutCol> rightOut, List<String> using) {
        List<RowContext.OutCol> shiftedRight = new ArrayList<>(rightOut.size());
        for (RowContext.OutCol oc : rightOut) shiftedRight.add(oc.shift(leftBindingCount));
        if (using == null || using.isEmpty()) {
            List<RowContext.OutCol> out = new ArrayList<>(leftOut);
            out.addAll(shiftedRight);
            return new JoinShape(out, null);
        }

        List<UsingKey> keys = new ArrayList<>();
        List<RowContext.OutCol> merged = new ArrayList<>();
        Set<RowContext.OutCol> takenLeft = Collections.newSetFromMap(new IdentityHashMap<>());
        Set<RowContext.OutCol> takenRight = Collections.newSetFromMap(new IdentityHashMap<>());
        for (String name : using) {
            RowContext.OutCol l = only(leftOut, name, "left");
            RowContext.OutCol r = only(rightOut, name, "right");
            if (!takenLeft.add(l) || !takenRight.add(r)) {
                throw new MemgresException(
                        "column name \"" + name + "\" appears more than once in USING clause", "42702");
            }
            keys.add(new UsingKey(name, l, r));
            int[] b = new int[l.bindings.length + r.bindings.length];
            int[] c = new int[b.length];
            System.arraycopy(l.bindings, 0, b, 0, l.bindings.length);
            System.arraycopy(l.columns, 0, c, 0, l.columns.length);
            for (int i = 0; i < r.bindings.length; i++) {
                b[l.bindings.length + i] = r.bindings[i] + leftBindingCount;
                c[l.columns.length + i] = r.columns[i];
            }
            merged.add(new RowContext.OutCol(l.name, b, c));
        }

        List<RowContext.OutCol> out = new ArrayList<>(merged);
        for (RowContext.OutCol oc : leftOut) {
            if (!takenLeft.contains(oc)) out.add(oc);
        }
        for (int i = 0; i < rightOut.size(); i++) {
            if (!takenRight.contains(rightOut.get(i))) out.add(shiftedRight.get(i));
        }
        return new JoinShape(out, keys);
    }

    /** The one output column of a side answering to a USING name, or the refusal PostgreSQL gives. */
    private static RowContext.OutCol only(List<RowContext.OutCol> side, String name, String which) {
        RowContext.OutCol found = null;
        int matches = 0;
        for (RowContext.OutCol oc : side) {
            if (oc.name.equals(name)) {
                matches++;
                if (found == null) found = oc;
            }
        }
        if (matches == 0) {
            throw new MemgresException("column \"" + name
                    + "\" specified in USING clause does not exist in " + which + " table", "42703");
        }
        if (matches > 1) {
            throw new MemgresException("common column name \"" + name
                    + "\" appears more than once in " + which + " table", "42702");
        }
        return found;
    }

    /**
     * The column names a NATURAL join joins on: the names the left side exposes that the right
     * side exposes too, in the left side's order, which is the order PostgreSQL lists them in.
     * A name either side exposes twice is left to {@link #shapeOfJoin} to refuse, as PostgreSQL
     * refuses it — that is the same wrong answer a USING clause naming it would be.
     */
    static List<String> naturalNames(List<RowContext.OutCol> leftOut, List<RowContext.OutCol> rightOut) {
        Set<String> rightNames = new HashSet<>();
        for (RowContext.OutCol oc : rightOut) rightNames.add(oc.name);
        List<String> names = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (RowContext.OutCol oc : leftOut) {
            if (rightNames.contains(oc.name) && seen.add(oc.name)) names.add(oc.name);
        }
        return names;
    }

    /**
     * Execute a JOIN operation.
     */
    List<RowContext> executeJoin(SelectStmt.JoinFrom join) {
        List<RowContext> leftContexts = fromResolver.resolveFromItem(join.left());
        // Both arms name themselves to the query before the condition is read, so a name given
        // twice is reported ahead of anything the condition holds.
        executor.selectExecutor.validateJoinNames(join);
        // The ON condition pairs one row with one row, so nothing that needs a whole group or a
        // finished result may stand in it. Judged here, once the left side has been resolved: a
        // relation that does not exist is still reported first, and the condition is refused
        // before it is evaluated rather than after an evaluation has said something else.
        executor.selectExecutor.placementCheck.reject(join.on(), "JOIN conditions");

        boolean lateral = join.right() instanceof SelectStmt.SubqueryFrom && ((SelectStmt.SubqueryFrom) join.right()).lateral();
        if (lateral) {
            return executeLateralJoin(join, leftContexts);
        }
        // A function in FROM reads the rows to its left, so it is run once per left row. That
        // answers an INNER or LEFT join on a condition, and nothing else: it has no place to put
        // the rows the right side kept to itself, and it never sees the columns a USING or NATURAL
        // clause names. Those joins take the ordinary path, where the function is one relation.
        if (join.right() instanceof SelectStmt.FunctionFrom
                && join.using() == null && !isNatural(join.joinType())
                && join.joinType() != SelectStmt.JoinType.RIGHT
                && join.joinType() != SelectStmt.JoinType.FULL) {
            return executeFunctionLateralJoin(join, leftContexts);
        }

        List<RowContext> rightContexts = fromResolver.resolveFromItem(join.right());

        // The names each side answers to, whether or not it produced a row. An outer join pads
        // the missing side with NULLs and those rows still carry that side's aliases.
        List<RowContext.TableBinding> leftShape = shapeOf(join.left(), leftContexts);
        List<RowContext.TableBinding> rightShape = shapeOf(join.right(), rightContexts);
        List<RowContext.OutCol> leftOut = outputOf(join.left(), leftContexts, leftShape);
        List<RowContext.OutCol> rightOut = outputOf(join.right(), rightContexts, rightShape);

        if (join.joinType() == SelectStmt.JoinType.FULL) {
            rejectUnmergeableFullJoin(join.on(), leftShape, rightShape);
        }

        List<String> using = join.using();
        if (isNatural(join.joinType())) using = naturalNames(leftOut, rightOut);
        JoinShape shape = shapeOfJoin(leftOut, leftShape.size(), rightOut, using);
        List<UsingKey> keys = shape.keys;
        rejectUnequatableUsingTypes(keys, leftShape, rightShape);

        List<RowContext> rows;
        switch (join.joinType()) {
            case INNER:
            case NATURAL:
                rows = executeInnerJoin(leftContexts, rightContexts, join.on(), keys, leftShape.size());
                break;
            case LEFT:
            case NATURAL_LEFT:
                rows = executeLeftJoin(leftContexts, rightContexts, join.on(), keys, leftShape.size(), rightShape);
                break;
            case RIGHT:
            case NATURAL_RIGHT:
                rows = executeRightJoin(leftContexts, rightContexts, join.on(), keys, leftShape.size(), leftShape);
                break;
            case FULL:
            case NATURAL_FULL:
                rows = executeFullJoin(leftContexts, rightContexts, join.on(), keys, leftShape.size(),
                        leftShape, rightShape);
                break;
            case CROSS:
                rows = executeCrossJoin(leftContexts, rightContexts);
                break;
            default:
                throw new IllegalStateException("Unknown join type: " + join.joinType());
        }
        return describe(rows, shape.output, using);
    }

    /**
     * A USING or NATURAL clause equates two columns, and PostgreSQL will only equate two columns
     * an {@code =} operator exists for. A number and a string is the pair that gets written by
     * accident — one side's key declared {@code text} and the other's {@code integer} — and
     * PostgreSQL refuses every one of the combinations, which was measured across the six numeric
     * and four string types rather than assumed. The join then reads as though it worked, because
     * comparing them as text matches whatever happens to spell the same.
     *
     * <p>Only that pair. Every other mismatch PostgreSQL refuses is left accepted, because
     * refusing valid SQL costs more than the permissiveness: a relation whose columns are not
     * declared — what a function in FROM produces — carries no type worth judging, and is skipped
     * outright.
     */
    private void rejectUnequatableUsingTypes(List<UsingKey> keys,
                                             List<RowContext.TableBinding> leftShape,
                                             List<RowContext.TableBinding> rightShape) {
        if (keys == null) return;
        for (UsingKey key : keys) {
            DataType l = declaredType(key.left, leftShape);
            DataType r = declaredType(key.right, rightShape);
            if (l == null || r == null) continue;
            if (isNumericType(l) && isStringType(r) || isStringType(l) && isNumericType(r)) {
                throw new MemgresException("operator does not exist: " + l.toRegtypeDisplay()
                        + " = " + r.toRegtypeDisplay(), "42883");
            }
        }
    }

    /** The type a side's key column is declared with, or null when it has none worth reading. */
    private static DataType declaredType(RowContext.OutCol col, List<RowContext.TableBinding> shape) {
        if (col.bindings[0] >= shape.size()) return null;
        Table table = shape.get(col.bindings[0]).table();
        if (table.isFunctionResult()) return null;
        if (col.columns[0] >= table.getColumns().size()) return null;
        return table.getColumns().get(col.columns[0]).getType();
    }

    private static boolean isNumericType(DataType t) {
        return t == DataType.SMALLINT || t == DataType.INTEGER || t == DataType.BIGINT
                || t == DataType.NUMERIC || t == DataType.REAL || t == DataType.DOUBLE_PRECISION
                || t == DataType.SERIAL || t == DataType.BIGSERIAL || t == DataType.SMALLSERIAL;
    }

    private static boolean isStringType(DataType t) {
        return t == DataType.TEXT || t == DataType.VARCHAR || t == DataType.CHAR
                || t == DataType.NAME;
    }

    /** Records on every row of a join what the join exposes and in what order. */
    private List<RowContext> describe(List<RowContext> rows, List<RowContext.OutCol> output,
                                      List<String> using) {
        Set<String> usingLower = null;
        if (using != null && !using.isEmpty()) {
            usingLower = new HashSet<>();
            for (String col : using) usingLower.add(col.toLowerCase());
        }
        for (RowContext ctx : rows) {
            ctx.setOutputColumns(output);
            if (usingLower != null) {
                Set<String> existing = ctx.getUsingColumns();
                if (existing == null) {
                    ctx.setUsingColumns(usingLower);
                } else {
                    Set<String> merged = new HashSet<>(existing);
                    merged.addAll(usingLower);
                    ctx.setUsingColumns(merged);
                }
            }
        }
        return rows;
    }

    /**
     * The bindings one side of a join exposes: those of its first row when it has one, and the
     * FROM item's own shape when it has none.
     */
    private List<RowContext.TableBinding> shapeOf(SelectStmt.FromItem item, List<RowContext> contexts) {
        if (!contexts.isEmpty()) return contexts.get(0).getBindings();
        return fromResolver.resolveItemShape(item);
    }

    /** The columns one side of a join exposes, from a row of it or from the item's own shape. */
    private List<RowContext.OutCol> outputOf(SelectStmt.FromItem item, List<RowContext> contexts,
                                             List<RowContext.TableBinding> shape) {
        if (!contexts.isEmpty()) return contexts.get(0).outputColumnsOrDefault();
        return fromResolver.resolveItemOutput(item, shape);
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
                        if (joinConditionHolds(join.on(), merged)) {
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
                        if (joinConditionHolds(join.on(), merged)) {
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
                    // For XMLTABLE, extract column definitions from encoded args
                    if (funcFrom.functionName().equals("__xmltable__")) {
                        cols = new ArrayList<>();
                        for (int i = 2; i < funcFrom.args().size(); i++) {
                            Expression arg = funcFrom.args().get(i);
                            String def = arg instanceof Literal ? ((Literal) arg).value() : arg.toString();
                            String[] parts = def.split(":", 3);
                            DataType dt = parts.length > 1 ? DataType.fromPgName(parts[1]) : null;
                            cols.add(new Column(parts[0], dt != null ? dt : DataType.TEXT, true, false, null));
                        }
                    }
                    // For JSON_TABLE, extract column definitions from the JsonTableExpr
                    else if (funcFrom.functionName().equals("__json_table__") && !funcFrom.args().isEmpty()
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
                    // Preserve SRF provenance on the unmatched-row placeholder so the
                    // attribute-notation fallback (ExprEvaluator.tryAttributeNotationFallback)
                    // still applies, matching resolveFunctionFrom's matched-row bindings.
                    virtualTable.setFunctionResult(true);
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
                                               Expression on, List<UsingKey> keys, int leftWidth) {
        // Try hash join for large datasets
        if (on != null && keys == null && left.size() > 0 && right.size() > 0
                && (long) left.size() * right.size() > 1000) {
            List<ColumnRef[]> equiKeys = extractEquiJoinKeys(on, left, right);
            if (equiKeys != null && !equiKeys.isEmpty()) {
                return executeHashInnerJoin(left, right, on, equiKeys);
            }
        }
        if (keys != null && left.size() > 0 && right.size() > 0
                && (long) left.size() * right.size() > 1000) {
            return executeHashInnerJoinUsing(left, right, keys);
        }
        // Nested loop fallback
        List<RowContext> result = new ArrayList<>();
        for (RowContext l : left) {
            for (RowContext r : right) {
                if (!matchesUsingKeys(l, r, keys)) continue;
                RowContext merged = mergeContexts(l, r);
                if (on == null || joinConditionHolds(on, merged)) {
                    result.add(merged);
                }
            }
        }
        return result;
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
                                                        List<UsingKey> keys) {
        Map<String, List<RowContext>> rightIndex = new HashMap<>();
        for (RowContext r : right) {
            String key = buildUsingKey(r, keys, false);
            if (key != null) {
                rightIndex.computeIfAbsent(key, k -> new ArrayList<>()).add(r);
            }
        }

        List<RowContext> result = new ArrayList<>();
        for (RowContext l : left) {
            String key = buildUsingKey(l, keys, true);
            if (key == null) continue;
            List<RowContext> candidates = rightIndex.get(key);
            if (candidates == null) continue;
            for (RowContext r : candidates) {
                result.add(mergeContexts(l, r));
            }
        }
        return result;
    }

    // ---- LEFT JOIN ----

    private List<RowContext> executeLeftJoin(List<RowContext> left, List<RowContext> right,
                                              Expression on, List<UsingKey> keys, int leftWidth,
                                              List<RowContext.TableBinding> rightShape) {
        List<RowContext.TableBinding> rightTemplate;
        if (!right.isEmpty()) {
            rightTemplate = right.get(0).getBindings();
        } else if (rightShape != null && !rightShape.isEmpty()) {
            rightTemplate = rightShape;
        } else if (fromResolver.lastResolvedRightTable != null) {
            rightTemplate = Cols.listOf(new RowContext.TableBinding(
                    fromResolver.lastResolvedRightTable, fromResolver.lastResolvedRightAlias,
                    new Object[fromResolver.lastResolvedRightTable.getColumns().size()]));
        } else {
            rightTemplate = Cols.listOf();
        }

        // Try hash join
        if (on != null && keys == null && left.size() > 0 && right.size() > 0
                && (long) left.size() * right.size() > 1000) {
            List<ColumnRef[]> equiKeys = extractEquiJoinKeys(on, left, right);
            if (equiKeys != null && !equiKeys.isEmpty()) {
                return executeHashLeftJoin(left, right, on, equiKeys, rightTemplate);
            }
        }
        if (keys != null && left.size() > 0 && right.size() > 0
                && (long) left.size() * right.size() > 1000) {
            return executeHashLeftJoinUsing(left, right, keys, rightTemplate);
        }

        // Nested loop fallback
        List<RowContext> result = new ArrayList<>();
        for (RowContext l : left) {
            boolean matched = false;
            for (RowContext r : right) {
                if (!matchesUsingKeys(l, r, keys)) continue;
                RowContext merged = mergeContexts(l, r);
                if (on == null || joinConditionHolds(on, merged)) {
                    result.add(merged);
                    matched = true;
                }
            }
            if (!matched) {
                result.add(mergeWithNullRight(l, rightTemplate));
            }
        }
        return result;
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
                                                       List<UsingKey> keys,
                                                       List<RowContext.TableBinding> rightTemplate) {
        Map<String, List<RowContext>> rightIndex = new HashMap<>();
        for (RowContext r : right) {
            String key = buildUsingKey(r, keys, false);
            if (key != null) {
                rightIndex.computeIfAbsent(key, k -> new ArrayList<>()).add(r);
            }
        }

        List<RowContext> result = new ArrayList<>();
        for (RowContext l : left) {
            String key = buildUsingKey(l, keys, true);
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
        return result;
    }

    // ---- RIGHT JOIN ----

    private List<RowContext> executeRightJoin(List<RowContext> left, List<RowContext> right,
                                               Expression on, List<UsingKey> keys, int leftWidth,
                                               List<RowContext.TableBinding> leftShape) {
        List<RowContext> result = new ArrayList<>();
        List<RowContext.TableBinding> leftTemplate = left.isEmpty() ?
                nullRowsOf(leftShape) : left.get(0).getBindings();

        for (RowContext r : right) {
            boolean matched = false;
            for (RowContext l : left) {
                if (!matchesUsingKeys(l, r, keys)) continue;
                RowContext merged = mergeContexts(l, r);
                if (on == null || joinConditionHolds(on, merged)) {
                    result.add(merged);
                    matched = true;
                }
            }
            if (!matched) {
                result.add(mergeWithNullLeft(leftTemplate, r));
            }
        }
        return result;
    }

    // ---- FULL JOIN ----

    private List<RowContext> executeFullJoin(List<RowContext> left, List<RowContext> right,
                                              Expression on, List<UsingKey> keys, int leftWidth,
                                              List<RowContext.TableBinding> leftShape,
                                              List<RowContext.TableBinding> rightShape) {
        List<RowContext> result = new ArrayList<>();
        List<RowContext.TableBinding> leftTemplate = left.isEmpty() ?
                nullRowsOf(leftShape) : left.get(0).getBindings();
        List<RowContext.TableBinding> rightTemplate = right.isEmpty() ?
                nullRowsOf(rightShape) : right.get(0).getBindings();

        Set<Integer> matchedRight = new HashSet<>();

        for (RowContext l : left) {
            boolean matched = false;
            for (int ri = 0; ri < right.size(); ri++) {
                RowContext r = right.get(ri);
                if (!matchesUsingKeys(l, r, keys)) continue;
                RowContext merged = mergeContexts(l, r);
                if (on == null || joinConditionHolds(on, merged)) {
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

        return result;
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

    // ---- Merge / null-padding helpers ----

    /** The same bindings with every value null — what a padded outer-join row looks like. */
    private static List<RowContext.TableBinding> nullRowsOf(List<RowContext.TableBinding> shape) {
        if (shape == null || shape.isEmpty()) return Cols.listOf();
        List<RowContext.TableBinding> out = new ArrayList<>(shape.size());
        for (RowContext.TableBinding b : shape) {
            out.add(new RowContext.TableBinding(b.table(), b.alias(),
                    new Object[b.table().getColumns().size()]));
        }
        return out;
    }

    RowContext mergeContexts(RowContext left, RowContext right) {
        List<RowContext.TableBinding> merged = new ArrayList<>(left.getBindings());
        merged.addAll(right.getBindings());
        RowContext ctx = new RowContext(merged);
        ctx.setOutputColumns(RowContext.concatOutput(left, right));
        ctx.setUsingColumns(unionUsing(left, right));
        return ctx;
    }

    /** The USING names both sides between them have merged, so a chain keeps every one of them. */
    private static Set<String> unionUsing(RowContext left, RowContext right) {
        if (left.getUsingColumns() == null) return right.getUsingColumns();
        if (right.getUsingColumns() == null) return left.getUsingColumns();
        Set<String> all = new HashSet<>(left.getUsingColumns());
        all.addAll(right.getUsingColumns());
        return all;
    }

    private RowContext mergeWithNullRight(RowContext left, List<RowContext.TableBinding> rightTemplate) {
        List<RowContext.TableBinding> merged = new ArrayList<>(left.getBindings());
        for (RowContext.TableBinding b : rightTemplate) {
            Object[] nullRow = new Object[b.table().getColumns().size()];
            merged.add(new RowContext.TableBinding(b.table(), b.alias(), nullRow));
        }
        RowContext ctx = new RowContext(merged);
        ctx.setUsingColumns(left.getUsingColumns());
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
        ctx.setUsingColumns(right.getUsingColumns());
        ctx.setOuterJoinNullPadded(true);
        return ctx;
    }

    // ---- FULL JOIN condition admissibility ----

    /**
     * A FULL JOIN may only be asked what PostgreSQL can answer.
     *
     * <p>PostgreSQL has no nested-loop plan for a full join: it must either merge or hash the two
     * sides, and both need an equality between one side's value and the other's. So it reads the
     * ON condition as a list of AND-ed clauses and refuses the join unless one of them is such an
     * equality — with the single exception that a condition which folds to a constant is fine,
     * which is what makes {@code FULL JOIN ON true} and {@code ON false} legal. Anything else —
     * an inequality, an OR, a condition naming only one side, {@code IS NOT DISTINCT FROM} — is
     * {@code 0A000 FULL JOIN is only supported with merge-joinable or hash-joinable join
     * conditions}, and an application that runs against both has to see the same refusal.
     *
     * <p>The check errs towards accepting: it fires only when no clause is recognisably a
     * cross-side equality <em>and</em> no clause could fold to false, so a condition it cannot
     * read is computed as before rather than refused.
     */
    private void rejectUnmergeableFullJoin(Expression on,
                                           List<RowContext.TableBinding> leftShape,
                                           List<RowContext.TableBinding> rightShape) {
        // USING and NATURAL join on equality by construction, and a join whose sides could not be
        // described is not one this check can judge.
        if (on == null) return;
        if (leftShape == null || leftShape.isEmpty() || rightShape == null || rightShape.isEmpty()) return;
        // A WHERE that reads either side discards the rows that side was padded with, and
        // PostgreSQL then plans the join as an inner one and asks nothing of its condition.
        if (whereReads(fromResolver.enclosingWhere, leftShape, rightShape)) return;

        // A name that reads from both sides is reported as ambiguous when the condition is
        // resolved, which PostgreSQL does before it plans anything.
        if (hasAmbiguousBareName(on, leftShape, rightShape)) return;

        List<Expression> conjuncts = new ArrayList<>();
        flattenAnd(on, conjuncts);
        int joinClauses = 0;
        for (Expression c : conjuncts) {
            Expression clause = foldConstants(c);
            if (clause == null) return;      // folds to something other than true: allowed
            if (clause == ALWAYS_TRUE) continue;
            joinClauses++;
            if (isCrossSideEquality(clause, leftShape, rightShape)) return;
        }
        if (joinClauses == 0) return;
        throw PgErrors.notImplemented(
                "FULL JOIN is only supported with merge-joinable or hash-joinable join conditions");
    }

    /** Stands for a clause that folded to true and so is not a clause at all. */
    private static final Expression ALWAYS_TRUE = new Literal(Literal.LiteralType.BOOLEAN, "true");

    /**
     * What is left of a clause once the constants in it are folded, as PostgreSQL folds them
     * before it plans: {@link #ALWAYS_TRUE} when nothing is left, null when the clause settles to
     * anything else — false, null, or a value this cannot work out — and otherwise the clause.
     */
    private Expression foldConstants(Expression c) {
        if (!mentionsRelation(c)) {
            Object value;
            try {
                value = executor.evalExpr(c, null);
            } catch (RuntimeException e) {
                return null;
            }
            return Boolean.TRUE.equals(value) ? ALWAYS_TRUE : null;
        }
        if (c instanceof BinaryExpr && ((BinaryExpr) c).op() == BinaryExpr.BinOp.OR) {
            List<Expression> disjuncts = new ArrayList<>();
            flattenOr(c, disjuncts);
            List<Expression> live = new ArrayList<>();
            for (Expression d : disjuncts) {
                if (mentionsRelation(d)) {
                    live.add(d);
                    continue;
                }
                Object value;
                try {
                    value = executor.evalExpr(d, null);
                } catch (RuntimeException e) {
                    return null;
                }
                // A true alternative makes the whole clause true; anything else — false, null —
                // can never be the alternative that holds, and drops out of the condition.
                if (Boolean.TRUE.equals(value)) return ALWAYS_TRUE;
            }
            if (live.isEmpty()) return null;
            if (live.size() == 1) return foldConstants(live.get(0));
        }
        return c;
    }

    /** True when {@code where} names a column of either side of the join. */
    private boolean whereReads(Expression where,
                               List<RowContext.TableBinding> leftShape,
                               List<RowContext.TableBinding> rightShape) {
        if (where == null) return false;
        final List<ColumnRef> refs = new ArrayList<>();
        AstWalk.anyMatch(where, new java.util.function.Predicate<Object>() {
            @Override public boolean test(Object n) {
                if (n instanceof ColumnRef) refs.add((ColumnRef) n);
                return false;
            }
        });
        for (ColumnRef ref : refs) {
            if (ref.table() != null) {
                if (namedIn(leftShape, ref.table()) || namedIn(rightShape, ref.table())) return true;
            } else if (ref.column() != null
                    && (hasColumn(leftShape, ref.column()) || hasColumn(rightShape, ref.column()))) {
                return true;
            }
        }
        return false;
    }

    /** Splits a condition into the clauses AND joins, which is how PostgreSQL reads one. */
    private static void flattenAnd(Expression e, List<Expression> out) {
        if (e instanceof BinaryExpr && ((BinaryExpr) e).op() == BinaryExpr.BinOp.AND) {
            flattenAnd(((BinaryExpr) e).left(), out);
            flattenAnd(((BinaryExpr) e).right(), out);
            return;
        }
        out.add(e);
    }

    private static void flattenOr(Expression e, List<Expression> out) {
        if (e instanceof BinaryExpr && ((BinaryExpr) e).op() == BinaryExpr.BinOp.OR) {
            flattenOr(((BinaryExpr) e).left(), out);
            flattenOr(((BinaryExpr) e).right(), out);
            return;
        }
        out.add(e);
    }

    /** True when the condition writes a bare name both sides answer to. */
    private boolean hasAmbiguousBareName(Expression on,
                                         List<RowContext.TableBinding> leftShape,
                                         List<RowContext.TableBinding> rightShape) {
        final List<ColumnRef> refs = new ArrayList<>();
        AstWalk.anyMatch(on, new java.util.function.Predicate<Object>() {
            @Override public boolean test(Object n) {
                if (n instanceof ColumnRef) refs.add((ColumnRef) n);
                return false;
            }
        });
        for (ColumnRef ref : refs) {
            if (ref.table() == null && ref.column() != null
                    && hasColumn(leftShape, ref.column()) && hasColumn(rightShape, ref.column())) {
                return true;
            }
        }
        return false;
    }

    /** True when the expression reads anything from a row rather than standing on its own. */
    private static boolean mentionsRelation(Expression e) {
        return AstWalk.anyMatch(e, new java.util.function.Predicate<Object>() {
            @Override public boolean test(Object n) {
                return n instanceof ColumnRef || n instanceof WildcardExpr
                        || n instanceof CompositeStarExpr || n instanceof SelectStmt
                        || n instanceof SetOpStmt || n instanceof ParamRef;
            }
        });
    }

    /** {@code x = y} with x reading only one side of the join and y only the other. */
    private boolean isCrossSideEquality(Expression c,
                                        List<RowContext.TableBinding> leftShape,
                                        List<RowContext.TableBinding> rightShape) {
        if (!(c instanceof BinaryExpr)) return false;
        BinaryExpr bin = (BinaryExpr) c;
        if (bin.op() != BinaryExpr.BinOp.EQUAL) return false;
        int a = sideRead(bin.left(), leftShape, rightShape);
        int b = sideRead(bin.right(), leftShape, rightShape);
        return (a == SIDE_LEFT && b == SIDE_RIGHT) || (a == SIDE_RIGHT && b == SIDE_LEFT);
    }

    private static final int SIDE_NONE = 0;
    private static final int SIDE_LEFT = 1;
    private static final int SIDE_RIGHT = 2;
    /** Reads from both sides, from neither, or from something this check cannot place. */
    private static final int SIDE_MIXED = -1;

    /** Which of the join's two sides an expression reads from, if exactly one of them. */
    private int sideRead(Expression e,
                         List<RowContext.TableBinding> leftShape,
                         List<RowContext.TableBinding> rightShape) {
        if (AstWalk.anyMatch(e, new java.util.function.Predicate<Object>() {
            @Override public boolean test(Object n) {
                return n instanceof SelectStmt || n instanceof SetOpStmt || n instanceof ParamRef;
            }
        })) {
            return SIDE_MIXED;
        }
        final List<ColumnRef> refs = new ArrayList<>();
        AstWalk.anyMatch(e, new java.util.function.Predicate<Object>() {
            @Override public boolean test(Object n) {
                if (n instanceof ColumnRef) refs.add((ColumnRef) n);
                return false;
            }
        });
        int seen = SIDE_NONE;
        for (ColumnRef ref : refs) {
            int side = sideOfRef(ref, leftShape, rightShape);
            if (side == SIDE_MIXED) return SIDE_MIXED;
            seen |= side;
        }
        return seen == (SIDE_LEFT | SIDE_RIGHT) ? SIDE_MIXED : seen;
    }

    private int sideOfRef(ColumnRef ref,
                          List<RowContext.TableBinding> leftShape,
                          List<RowContext.TableBinding> rightShape) {
        if (ref.table() != null) {
            boolean inLeft = namedIn(leftShape, ref.table());
            boolean inRight = namedIn(rightShape, ref.table());
            if (inLeft && !inRight) return SIDE_LEFT;
            if (inRight && !inLeft) return SIDE_RIGHT;
            return SIDE_MIXED;
        }
        boolean inLeft = hasColumn(leftShape, ref.column());
        boolean inRight = hasColumn(rightShape, ref.column());
        if (inLeft && !inRight) return SIDE_LEFT;
        if (inRight && !inLeft) return SIDE_RIGHT;
        return SIDE_MIXED;
    }

    private static boolean namedIn(List<RowContext.TableBinding> shape, String name) {
        for (RowContext.TableBinding b : shape) {
            if (b.alias() != null ? b.alias().equalsIgnoreCase(name)
                    : b.table().getName().equalsIgnoreCase(name)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasColumn(List<RowContext.TableBinding> shape, String column) {
        for (RowContext.TableBinding b : shape) {
            if (b.table().getColumnIndex(column) >= 0) return true;
        }
        return false;
    }

    // ---- Join condition matching ----

    /**
     * A join condition has to be a predicate. Left unchecked, a bare {@code ON a.id} would be
     * read as always-true and quietly produce the whole cross product.
     */
    boolean joinConditionHolds(Expression on, RowContext merged) {
        Object val = executor.evalExpr(on, merged);
        if (val instanceof Number) {
            throw PgErrors.datatypeMismatch("argument of JOIN/ON must be type boolean, not type "
                    + TypeCoercion.inferType(val).toRegtypeDisplay());
        }
        return executor.isTruthy(val);
    }

    /**
     * Whether a left row and a right row agree on every column the join was told to join on.
     * Each side's value is read from its own output column, so a column an earlier join merged
     * answers with whichever relation behind it is not null — and a NULL never equals anything,
     * which is what keeps an outer join's padded rows from matching further down a chain.
     */
    private boolean matchesUsingKeys(RowContext left, RowContext right, List<UsingKey> keys) {
        if (keys == null) return true;
        for (UsingKey key : keys) {
            Object leftVal = key.left.valueIn(left.getBindings());
            if (leftVal == null) return false;
            Object rightVal = key.right.valueIn(right.getBindings());
            if (rightVal == null) return false;
            if (!Objects.equals(leftVal, rightVal) && !leftVal.toString().equals(rightVal.toString())) {
                return false;
            }
        }
        return true;
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

    private String buildUsingKey(RowContext ctx, List<UsingKey> keys, boolean leftSide) {
        StringBuilder sb = new StringBuilder();
        for (UsingKey key : keys) {
            Object val = (leftSide ? key.left : key.right).valueIn(ctx.getBindings());
            if (val == null) return null;
            if (sb.length() > 0) sb.append('\0');
            sb.append(val.toString());
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
