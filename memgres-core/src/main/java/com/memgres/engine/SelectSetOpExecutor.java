package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.*;

/**
 * Handles UNION, INTERSECT, EXCEPT set operations.
 * Extracted from SelectExecutor to separate concerns.
 */
class SelectSetOpExecutor {
    private final SelectExecutor select;
    private final AstExecutor executor;

    SelectSetOpExecutor(SelectExecutor select) {
        this.select = select;
        this.executor = select.executor;
    }

    QueryResult executeSetOp(SetOpStmt stmt) {
        rejectLock(stmt);
        // If the left side has CTEs, they should be available to both sides
        if (stmt.left() instanceof SelectStmt && ((SelectStmt) stmt.left()).withClauses() != null && !((SelectStmt) stmt.left()).withClauses().isEmpty()) {
            SelectStmt sel = (SelectStmt) stmt.left();
            Map<String, SelectStmt.CommonTableExpr> cteMap = new LinkedHashMap<>();
            for (SelectStmt.CommonTableExpr cte : sel.withClauses()) {
                cteMap.put(cte.name().toLowerCase(), cte);
            }
            executor.cteStack.push(cteMap);
            // Execute left without its own CTE push (it would double-push)
            SelectStmt stripped = new SelectStmt(sel.distinct(), sel.targets(), sel.from(), sel.where(),
                    sel.groupBy(), sel.having(), sel.orderBy(), sel.limit(), sel.offset(), null);
            QueryResult leftResult = executor.executeStatement(stripped);
            QueryResult rightResult = executor.executeStatement(stmt.right());
            try {
                return executeSetOpInner(stmt, leftResult, rightResult);
            } finally {
                executor.cteStack.pop();
            }
        }

        QueryResult leftResult = executor.executeStatement(stmt.left());
        QueryResult rightResult = executor.executeStatement(stmt.right());
        return executeSetOpInner(stmt, leftResult, rightResult);
    }

    /**
     * A row lock points at the base-table row behind an output row, and a set operation has
     * already combined rows from different relations by the time it has an output row to point
     * at. PostgreSQL rejects the lock wherever in the set operation it was written — on an arm,
     * parenthesised or not, or after the whole thing.
     */
    private void rejectLock(SetOpStmt stmt) {
        SelectStmt.LockClause lock = lockIn(stmt.left());
        if (lock == null) lock = lockIn(stmt.right());
        if (lock == null) return;
        throw new MemgresException("FOR " + (lock.mode() == null ? "UPDATE" : lock.mode())
                + " is not allowed with UNION/INTERSECT/EXCEPT", "0A000");
    }

    private SelectStmt.LockClause lockIn(Statement arm) {
        if (arm instanceof SelectStmt) return ((SelectStmt) arm).lockClause();
        if (arm instanceof SetOpStmt) {
            SelectStmt.LockClause left = lockIn(((SetOpStmt) arm).left());
            return left != null ? left : lockIn(((SetOpStmt) arm).right());
        }
        return null;
    }

    /**
     * True when a branch writes a text type down for column {@code ci} -- {@code 'a'::text} and
     * nothing else. Such a branch really is text and PostgreSQL reports the mismatch rather than
     * coercing it away, where a bare literal is {@code unknown} and takes the other branch's type.
     *
     * <p>Only an explicit cast counts. Anything the engine merely inferred as text -- a function
     * call, a subquery column, an expression -- keeps the coercion path, because refusing those
     * on the strength of a defaulted type rejects SQL PostgreSQL runs.
     */
    private boolean writesTextTypeDown(Statement branch, int ci) {
        if (!(branch instanceof SelectStmt)) return false;
        List<SelectStmt.SelectTarget> targets = ((SelectStmt) branch).targets();
        if (targets == null || ci >= targets.size()) return false;
        Expression expr = targets.get(ci).expr();
        if (!(expr instanceof CastExpr)) return false;
        String name = ((CastExpr) expr).typeName();
        if (name == null) return false;
        DataType dt = DataType.fromPgName(DataType.canonicalName(name));
        return dt == DataType.TEXT || dt == DataType.VARCHAR || dt == DataType.CHAR
                || dt == DataType.NAME;
    }

    private QueryResult executeSetOpInner(SetOpStmt stmt, QueryResult leftResult, QueryResult rightResult) {
        List<Column> columns = new ArrayList<>(leftResult.getColumns());
        if (columns.size() != rightResult.getColumns().size()) {
            throw new MemgresException("each " + stmt.op().name() + " query must have the same number of columns", "42601");
        }
        // Check type compatibility and widen column types between corresponding columns
        for (int ci = 0; ci < columns.size(); ci++) {
            DataType leftType = columns.get(ci).getType();
            DataType rightType = rightResult.getColumns().get(ci).getType();
            // A bare NULL branch has no type of its own; PG takes the type of the branch
            // that has one, so SELECT NULL UNION ALL SELECT 1 is integer, not unknown.
            if (leftType == null && rightType != null) {
                Column orig = columns.get(ci);
                columns.set(ci, new Column(orig.getName(), rightType, true, false, orig.getDefaultValue()));
                continue;
            }
            if (leftType != null && rightType != null && leftType != rightType) {
                TypeCoercion.TypeCategory leftCat = TypeCoercion.categoryOf(leftType);
                TypeCoercion.TypeCategory rightCat = TypeCoercion.categoryOf(rightType);
                if (leftCat == rightCat && leftCat == TypeCoercion.TypeCategory.NUMERIC) {
                    DataType wider = widenNumericSetOp(leftType, rightType);
                    if (wider != leftType) {
                        Column orig = columns.get(ci);
                        columns.set(ci, new Column(orig.getName(), wider, orig.isNullable(), orig.isPrimaryKey(), orig.getDefaultValue()));
                    }
                } else if (leftCat == rightCat) {
                    // Same category, different types (e.g., varchar vs text) - allow
                } else if (leftCat != rightCat) {
                    boolean leftIsText = leftType == DataType.TEXT || leftType == DataType.VARCHAR
                            || leftType == DataType.CHAR || leftType == DataType.NAME;
                    boolean rightIsText = rightType == DataType.TEXT || rightType == DataType.VARCHAR
                            || rightType == DataType.CHAR || rightType == DataType.NAME;
                    if (!leftIsText && !rightIsText) {
                        throw new MemgresException(stmt.op().name() + " types "
                                + leftType.toRegtypeDisplay() + " and "
                                + rightType.toRegtypeDisplay() + " cannot be matched", "42804");
                    }
                    // One side is text-category, the other is not. In PostgreSQL, string
                    // literals and NULL have type "unknown" which is implicitly coercible to
                    // any type. Our engine uses TEXT for both unknown literals and actual text
                    // columns. To distinguish: columns from real tables have tableOid > 0,
                    // and a branch whose query writes the type down -- 'a'::text -- is text in
                    // its own right, so PostgreSQL reports the mismatch instead of coercing it.
                    Column textCol = leftIsText ? columns.get(ci) : rightResult.getColumns().get(ci);
                    boolean writtenAsText = leftIsText
                            ? writesTextTypeDown(stmt.left(), ci) : writesTextTypeDown(stmt.right(), ci);
                    if (textCol.getTableOid() != 0 || writtenAsText) {
                        throw new MemgresException(stmt.op().name() + " types "
                                + leftType.toRegtypeDisplay() + " and "
                                + rightType.toRegtypeDisplay() + " cannot be matched", "42804");
                    }
                    // TEXT from a computed expression or literal — attempt coercion
                    DataType targetType = leftIsText ? rightType : leftType;
                    List<Object[]> textRows = leftIsText ? leftResult.getRows() : rightResult.getRows();
                    for (Object[] row : textRows) {
                        Object val = row[ci];
                        if (val != null) {
                            try {
                                executor.castEvaluator.applyCast(val, targetType.getPgName());
                            } catch (Exception e) {
                                // PG returns 22P02 (invalid_text_representation) when a
                                // literal/unknown value cannot be coerced to the target type
                                // in a set operation (e.g., SELECT 1 UNION ALL SELECT 'x').
                                throw new MemgresException(
                                        "invalid input syntax for type " + targetType.getPgName()
                                                + ": \"" + val + "\"", "22P02");
                            }
                        }
                    }
                    // Coercion succeeded for all values — use the non-text type
                    if (leftIsText && !rightIsText) {
                        Column orig = columns.get(ci);
                        columns.set(ci, new Column(orig.getName(), rightType, orig.isNullable(), orig.isPrimaryKey(), orig.getDefaultValue()));
                    }
                }
            }
        }
        List<Object[]> resultRows = new ArrayList<>();

        switch (stmt.op()) {
            case UNION: {
                resultRows.addAll(leftResult.getRows());
                resultRows.addAll(rightResult.getRows());
                if (!stmt.all()) {
                    resultRows = deduplicateRows(resultRows);
                }
                break;
            }
            case INTERSECT: {
                if (stmt.all()) {
                    // Multiset semantics: each left row matches at most once per right occurrence.
                    Map<RowKey, Integer> rightCounts = new HashMap<>();
                    for (Object[] row : rightResult.getRows()) {
                        rightCounts.merge(new RowKey(row), 1, Integer::sum);
                    }
                    for (Object[] row : leftResult.getRows()) {
                        RowKey key = new RowKey(row);
                        int remaining = rightCounts.getOrDefault(key, 0);
                        if (remaining > 0) {
                            resultRows.add(row);
                            rightCounts.put(key, remaining - 1);
                        }
                    }
                } else {
                    Set<RowKey> rightKeys = new HashSet<>();
                    for (Object[] row : rightResult.getRows()) {
                        rightKeys.add(new RowKey(row));
                    }
                    Set<RowKey> seen = new HashSet<>();
                    for (Object[] row : leftResult.getRows()) {
                        RowKey key = new RowKey(row);
                        if (rightKeys.contains(key) && seen.add(key)) {
                            resultRows.add(row);
                        }
                    }
                }
                break;
            }
            case EXCEPT: {
                Map<RowKey, Integer> rightCounts = new HashMap<>();
                for (Object[] row : rightResult.getRows()) {
                    rightCounts.merge(new RowKey(row), 1, Integer::sum);
                }
                Set<RowKey> seen = new HashSet<>();
                for (Object[] row : leftResult.getRows()) {
                    RowKey key = new RowKey(row);
                    if (stmt.all()) {
                        int remaining = rightCounts.getOrDefault(key, 0);
                        if (remaining > 0) {
                            rightCounts.put(key, remaining - 1);
                        } else {
                            resultRows.add(row);
                        }
                    } else {
                        if (!rightCounts.containsKey(key) && seen.add(key)) {
                            resultRows.add(row);
                        }
                    }
                }
                break;
            }
        }

        // ORDER BY on set operation result
        if (stmt.orderBy() != null && !stmt.orderBy().isEmpty()) {
            resultRows.sort((a, b) -> {
                for (SelectStmt.OrderByItem item : stmt.orderBy()) {
                    int colIdx = -1;
                    if (item.expr() instanceof Literal && ((Literal) item.expr()).literalType() == Literal.LiteralType.INTEGER) {
                        Literal lit = (Literal) item.expr();
                        colIdx = Integer.parseInt(lit.value()) - 1;
                    } else if (item.expr() instanceof ColumnRef && ((ColumnRef) item.expr()).table() == null) {
                        ColumnRef ref = (ColumnRef) item.expr();
                        for (int i = 0; i < columns.size(); i++) {
                            if (columns.get(i).getName().equalsIgnoreCase(ref.column())) {
                                colIdx = i;
                                break;
                            }
                        }
                    }
                    if (colIdx < 0 || colIdx >= a.length) continue;

                    Object va = a[colIdx], vb = b[colIdx];
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

        // OFFSET + LIMIT
        if (stmt.offset() != null) {
            int off = executor.toInt(executor.evalExpr(stmt.offset(), null));
            if (off > 0 && off < resultRows.size()) {
                resultRows = new ArrayList<>(resultRows.subList(off, resultRows.size()));
            } else if (off >= resultRows.size()) {
                resultRows = Cols.listOf();
            }
        }
        if (stmt.limit() != null) {
            int lim = executor.toInt(executor.evalExpr(stmt.limit(), null));
            if (lim < resultRows.size()) {
                resultRows = new ArrayList<>(resultRows.subList(0, lim));
            }
        }

        return QueryResult.select(columns, resultRows);
    }

    static DataType widenNumericSetOp(DataType a, DataType b) {
        int ra = numericRank(a), rb = numericRank(b);
        return rb > ra ? b : a;
    }

    static int numericRank(DataType dt) {
        switch (dt) {
            case SMALLINT:
            case SMALLSERIAL:
                return 1;
            case INTEGER:
            case SERIAL:
                return 2;
            case BIGINT:
            case BIGSERIAL:
                return 3;
            case NUMERIC:
                return 4;
            case REAL:
                return 5;
            case DOUBLE_PRECISION:
                return 6;
            default:
                return 0;
        }
    }

    static List<Object[]> deduplicateRows(List<Object[]> rows) {
        Set<RowKey> seen = new LinkedHashSet<>();
        List<Object[]> result = new ArrayList<>();
        for (Object[] row : rows) {
            if (seen.add(new RowKey(row))) {
                result.add(row);
            }
        }
        return result;
    }
}
