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

    private QueryResult executeSetOpInner(SetOpStmt stmt, QueryResult leftResult, QueryResult rightResult) {
        List<Column> columns = new ArrayList<>(leftResult.getColumns());
        if (columns.size() != rightResult.getColumns().size()) {
            throw new MemgresException("each " + stmt.op().name() + " query must have the same number of columns", "42601");
        }
        // Check type compatibility and widen column types between corresponding columns
        for (int ci = 0; ci < columns.size(); ci++) {
            DataType leftType = columns.get(ci).getType();
            DataType rightType = rightResult.getColumns().get(ci).getType();
            if (leftType != null && rightType != null && leftType != rightType) {
                TypeCoercion.TypeCategory leftCat = TypeCoercion.categoryOf(leftType);
                TypeCoercion.TypeCategory rightCat = TypeCoercion.categoryOf(rightType);
                if (leftCat == rightCat && leftCat == TypeCoercion.TypeCategory.NUMERIC) {
                    DataType wider = widenNumericSetOp(leftType, rightType);
                    if (wider != leftType) {
                        Column orig = columns.get(ci);
                        columns.set(ci, new Column(orig.getName(), wider, orig.isNullable(), orig.isPrimaryKey(), orig.getDefaultValue()));
                    }
                } else if (leftCat != rightCat) {
                    boolean coercible = true;
                    if (!rightResult.getRows().isEmpty()) {
                        for (Object[] row : rightResult.getRows()) {
                            if (ci < row.length && row[ci] != null && leftCat == TypeCoercion.TypeCategory.NUMERIC) {
                                try { TypeCoercion.toBigDecimal(row[ci]); } catch (Exception e) { coercible = false; break; }
                            }
                        }
                    }
                    if (!leftResult.getRows().isEmpty()) {
                        for (Object[] row : leftResult.getRows()) {
                            if (ci < row.length && row[ci] != null && rightCat == TypeCoercion.TypeCategory.NUMERIC) {
                                try { TypeCoercion.toBigDecimal(row[ci]); } catch (Exception e) { coercible = false; break; }
                            }
                        }
                    }
                    if (!coercible) {
                        String sqlState = stmt.all() ? "22P02" : "42804";
                        throw new MemgresException(stmt.op().name() + " types " + leftType.getPgName() + " and "
                                + rightType.getPgName() + " cannot be matched", sqlState);
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
                Set<String> rightKeys = new HashSet<>();
                for (Object[] row : rightResult.getRows()) {
                    rightKeys.add(Arrays.deepToString(row));
                }
                Set<String> seen = new HashSet<>();
                for (Object[] row : leftResult.getRows()) {
                    String key = Arrays.deepToString(row);
                    if (rightKeys.contains(key)) {
                        if (stmt.all() || seen.add(key)) {
                            resultRows.add(row);
                        }
                    }
                }
                break;
            }
            case EXCEPT: {
                Map<String, Integer> rightCounts = new HashMap<>();
                for (Object[] row : rightResult.getRows()) {
                    rightCounts.merge(Arrays.deepToString(row), 1, Integer::sum);
                }
                Set<String> seen = new HashSet<>();
                for (Object[] row : leftResult.getRows()) {
                    String key = Arrays.deepToString(row);
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
        Set<String> seen = new LinkedHashSet<>();
        List<Object[]> result = new ArrayList<>();
        for (Object[] row : rows) {
            if (seen.add(Arrays.deepToString(row))) {
                result.add(row);
            }
        }
        return result;
    }
}
