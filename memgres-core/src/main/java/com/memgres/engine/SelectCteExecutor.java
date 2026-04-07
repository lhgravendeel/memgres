package com.memgres.engine;

import com.memgres.engine.parser.ast.*;

import java.util.*;

/**
 * Handles Common Table Expression (CTE) execution, including recursive CTEs
 * with SEARCH DEPTH FIRST/BREADTH FIRST and CYCLE detection.
 * Extracted from SelectExecutor to separate concerns.
 */
class SelectCteExecutor {
    private final SelectExecutor select;
    private final AstExecutor executor;

    SelectCteExecutor(SelectExecutor select) {
        this.select = select;
        this.executor = select.executor;
    }

    /**
     * Look up a CTE by name in the CTE stack (innermost scope first).
     */
    SelectStmt.CommonTableExpr lookupCte(String name) {
        String lcName = name.toLowerCase();
        if (executor.executingCtes.contains(lcName)) return null;
        for (Map<String, SelectStmt.CommonTableExpr> scope : executor.cteStack) {
            SelectStmt.CommonTableExpr cte = scope.get(lcName);
            if (cte != null) return cte;
        }
        return null;
    }

    /**
     * Execute a CTE query and return the result.
     */
    QueryResult executeCte(SelectStmt.CommonTableExpr cte) {
        if (cte.recursive()) {
            return executeRecursiveCte(cte);
        }

        String cacheKey = cte.name().toLowerCase();
        QueryResult cached = executor.cteResultCache.get(cacheKey);
        if (cached != null) return cached;

        if (executor.executingCtes.contains(cacheKey)) {
            throw new MemgresException(
                    "recursive reference to query \"" + cte.name() + "\" must not appear within a non-recursive term", "42P19");
        }
        executor.executingCtes.add(cacheKey);

        QueryResult result;
        try {
            result = executor.executeStatement(cte.query());
        } finally {
            executor.executingCtes.remove(cacheKey);
        }

        if (cte.columnNames() != null && !cte.columnNames().isEmpty()) {
            if (cte.columnNames().size() != result.getColumns().size()) {
                throw new MemgresException("table \"" + cte.name() + "\" has " + result.getColumns().size()
                        + " columns available but " + cte.columnNames().size() + " columns specified", "42P10");
            }
            List<Column> renamedCols = new ArrayList<>();
            for (int i = 0; i < result.getColumns().size(); i++) {
                String newName = cte.columnNames().get(i);
                Column orig = result.getColumns().get(i);
                renamedCols.add(new Column(newName, orig.getType(), orig.isNullable(), orig.isPrimaryKey(), orig.getDefaultValue()));
            }
            result = QueryResult.select(renamedCols, result.getRows());
        }

        executor.cteResultCache.put(cacheKey, result);
        return result;
    }

    /**
     * Execute a recursive CTE using iterative fixed-point evaluation.
     */
    private QueryResult executeRecursiveCte(SelectStmt.CommonTableExpr cte) {
        if (!(cte.query() instanceof SetOpStmt)) {
            return executeCte(new SelectStmt.CommonTableExpr(cte.name(), cte.columnNames(), cte.query(), false));
        }
        SetOpStmt setOp = (SetOpStmt) cte.query();

        QueryResult baseResult = executor.executeStatement(setOp.left());
        List<Column> columns = baseResult.getColumns();

        if (cte.columnNames() != null && !cte.columnNames().isEmpty()) {
            List<Column> renamedCols = new ArrayList<>();
            for (int i = 0; i < columns.size(); i++) {
                String newName = i < cte.columnNames().size() ? cte.columnNames().get(i) :
                        columns.get(i).getName();
                Column orig = columns.get(i);
                renamedCols.add(new Column(newName, orig.getType(), orig.isNullable(), orig.isPrimaryKey(), orig.getDefaultValue()));
            }
            columns = renamedCols;
        }

        List<Object[]> allRows = new ArrayList<>(baseResult.getRows());
        List<Object[]> workingSet = new ArrayList<>(baseResult.getRows());
        boolean isUnionAll = setOp.all();
        Set<String> seenKeys = new HashSet<>();
        if (!isUnionAll) {
            for (Object[] row : allRows) seenKeys.add(Arrays.deepToString(row));
        }

        boolean depthFirstSearch = cte.searchColumn() != null && cte.searchDepthFirst();
        int[] searchByIndices = null;
        List<List<Object>> ordcolPaths = depthFirstSearch ? new ArrayList<>() : null;
        if (depthFirstSearch && cte.searchByColumns() != null) {
            searchByIndices = new int[cte.searchByColumns().size()];
            for (int si = 0; si < cte.searchByColumns().size(); si++) {
                String sbyCol = cte.searchByColumns().get(si).toLowerCase();
                searchByIndices[si] = -1;
                for (int ci = 0; ci < columns.size(); ci++) {
                    if (columns.get(ci).getName().equalsIgnoreCase(sbyCol)) {
                        searchByIndices[si] = ci;
                        break;
                    }
                }
                if (searchByIndices[si] == -1) searchByIndices[si] = 0;
            }
            for (Object[] row : allRows) {
                List<Object> path = new ArrayList<>();
                path.add(extractSearchKey(row, searchByIndices));
                ordcolPaths.add(path);
            }
        }

        List<List<Object>> workingSetPaths = depthFirstSearch ? new ArrayList<>(ordcolPaths) : null;

        String cteLower = cte.name().toLowerCase();
        executor.executingCtes.add(cteLower);
        int maxIterations = 1000;
        try {
        for (int iter = 0; iter < maxIterations && !workingSet.isEmpty(); iter++) {
            String cteName = cte.name().toLowerCase();
            Schema targetSchema = executor.database.getOrCreateSchema(executor.defaultSchema());
            Table previousTable = targetSchema.getTable(cteName);

            if (depthFirstSearch) {
                List<Object[]> newRows = new ArrayList<>();
                List<List<Object>> newPaths = new ArrayList<>();
                for (int pi = 0; pi < workingSet.size(); pi++) {
                    Object[] parentRow = workingSet.get(pi);
                    List<Object> parentPath = workingSetPaths.get(pi);

                    Table singleRowTable = new Table(cteName, columns);
                    singleRowTable.insertRow(parentRow);
                    targetSchema.addTable(singleRowTable);

                    try {
                        QueryResult iterResult = executor.executeStatement(setOp.right());

                        if (iter == 0 && pi == 0) {
                            validateIterationResult(iterResult, columns, setOp.right());
                        }

                        for (Object[] row : iterResult.getRows()) {
                            if (isUnionAll || seenKeys.add(Arrays.deepToString(row))) {
                                newRows.add(row);
                                allRows.add(row);
                                List<Object> childPath = new ArrayList<>(parentPath);
                                childPath.add(extractSearchKey(row, searchByIndices));
                                ordcolPaths.add(childPath);
                                newPaths.add(childPath);
                            }
                        }
                    } finally {
                        targetSchema.removeTable(cteName);
                        if (previousTable != null) targetSchema.addTable(previousTable);
                    }
                }
                workingSet = newRows;
                workingSetPaths = newPaths;
            } else {
                Table workingTable = new Table(cteName, columns);
                for (Object[] row : workingSet) workingTable.insertRow(row);
                targetSchema.addTable(workingTable);

                try {
                    QueryResult iterResult = executor.executeStatement(setOp.right());

                    if (iter == 0) {
                        validateIterationResult(iterResult, columns, setOp.right());
                    }

                    List<Object[]> newRows = new ArrayList<>();
                    for (Object[] row : iterResult.getRows()) {
                        if (isUnionAll || seenKeys.add(Arrays.deepToString(row))) {
                            newRows.add(row);
                            allRows.add(row);
                        }
                    }
                    workingSet = newRows;
                } finally {
                    targetSchema.removeTable(cteName);
                    if (previousTable != null) targetSchema.addTable(previousTable);
                }
            }
        }

        } catch (MemgresException e) {
            throw e;
        } finally {
            executor.executingCtes.remove(cteLower);
        }

        // Add SEARCH ordering column if declared
        if (cte.searchColumn() != null) {
            String scol = cte.searchColumn();
            List<Column> extCols = new ArrayList<>(columns);
            if (depthFirstSearch && ordcolPaths != null) {
                extCols.add(new Column(scol, DataType.TEXT, true, false, null));
                List<Object[]> extRows = new ArrayList<>();
                for (int i = 0; i < allRows.size(); i++) {
                    Object[] orig = allRows.get(i);
                    Object[] ext = Arrays.copyOf(orig, orig.length + 1);
                    ext[orig.length] = formatRecordArray(ordcolPaths.get(i));
                    extRows.add(ext);
                }
                columns = extCols;
                allRows = extRows;
            } else {
                extCols.add(new Column(scol, DataType.INTEGER, true, false, null));
                List<Object[]> extRows = new ArrayList<>();
                for (int i = 0; i < allRows.size(); i++) {
                    Object[] orig = allRows.get(i);
                    Object[] ext = Arrays.copyOf(orig, orig.length + 1);
                    ext[orig.length] = i;
                    extRows.add(ext);
                }
                columns = extCols;
                allRows = extRows;
            }
        }

        // Add CYCLE column and path column if declared
        if (cte.cycleColumn() != null) {
            String ccol = cte.cycleColumn();
            String pathcol = cte.cyclePathColumn() != null ? cte.cyclePathColumn() : "path";
            List<Column> extCols = new ArrayList<>(columns);
            extCols.add(new Column(ccol, DataType.BOOLEAN, true, false, null));
            extCols.add(new Column(pathcol, DataType.TEXT, true, false, null));

            List<Object[]> extRows = new ArrayList<>();
            if (ordcolPaths != null) {
                for (int i = 0; i < allRows.size(); i++) {
                    Object[] orig = allRows.get(i);
                    List<Object> path = ordcolPaths.get(i);
                    Object lastKey = path.get(path.size() - 1);
                    boolean isCycle = false;
                    for (int j = 0; j < path.size() - 1; j++) {
                        if (java.util.Objects.equals(path.get(j), lastKey)) {
                            isCycle = true;
                            break;
                        }
                    }
                    Object[] ext = Arrays.copyOf(orig, orig.length + 2);
                    ext[orig.length] = isCycle;
                    ext[orig.length + 1] = formatRecordArray(path);
                    extRows.add(ext);
                }
            } else {
                Set<String> cycleCheck = new HashSet<>();
                for (Object[] orig : allRows) {
                    String key = Arrays.deepToString(orig);
                    boolean isCycle = !cycleCheck.add(key);
                    Object[] ext = Arrays.copyOf(orig, orig.length + 2);
                    ext[orig.length] = isCycle;
                    ext[orig.length + 1] = key;
                    extRows.add(ext);
                }
            }
            columns = extCols;
            allRows = extRows;
        }

        return QueryResult.select(columns, allRows);
    }

    /** Validate the first iteration result of a recursive CTE. */
    private void validateIterationResult(QueryResult iterResult, List<Column> columns, Statement recursiveTerm) {
        if (!iterResult.getColumns().isEmpty()
                && iterResult.getColumns().size() != columns.size()) {
            throw new MemgresException(
                    "each UNION query must have the same number of columns", "42601");
        }
        if (!iterResult.getRows().isEmpty()) {
            Object[] firstRow = iterResult.getRows().get(0);
            for (int ci = 0; ci < Math.min(columns.size(), firstRow.length); ci++) {
                DataType baseType = columns.get(ci).getType();
                Object val = firstRow[ci];
                if (baseType != null && val != null) {
                    if ((baseType == DataType.INTEGER || baseType == DataType.BIGINT
                            || baseType == DataType.SMALLINT || baseType == DataType.NUMERIC)
                            && val instanceof String) {
                        String s = (String) val;
                        try { Long.parseLong(s); } catch (NumberFormatException e) {
                            try { new java.math.BigDecimal(s); } catch (NumberFormatException e2) {
                                throw new MemgresException(
                                        "invalid input syntax for type integer: \"" + s + "\"",
                                        "22P02");
                            }
                        }
                    }
                }
            }
        }
        validateRecursiveTermTypes(columns, recursiveTerm);
    }

    static String formatRecordArray(List<Object> path) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < path.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("(").append(path.get(i)).append(")");
        }
        sb.append("}");
        return sb.toString();
    }

    static Object extractSearchKey(Object[] row, int[] searchByIndices) {
        if (searchByIndices.length == 1) {
            return row[searchByIndices[0]];
        }
        List<Object> key = new ArrayList<>();
        for (int idx : searchByIndices) {
            key.add(row[idx]);
        }
        return key;
    }

    private void validateRecursiveTermTypes(List<Column> baseColumns, Statement recursiveTerm) {
        if (!(recursiveTerm instanceof SelectStmt)) return;
        SelectStmt recSelect = (SelectStmt) recursiveTerm;
        if (recSelect.targets() == null) return;

        for (int ci = 0; ci < Math.min(baseColumns.size(), recSelect.targets().size()); ci++) {
            DataType baseType = baseColumns.get(ci).getType();
            if (baseType == null) continue;
            TypeCoercion.TypeCategory baseCat = TypeCoercion.categoryOf(baseType);

            Expression targetExpr = recSelect.targets().get(ci).expr();
            if (baseCat == TypeCoercion.TypeCategory.STRING && isArithmeticExpr(targetExpr, baseColumns)) {
                throw new MemgresException("operator does not exist: text + integer", "42883");
            }
        }
    }

    private static boolean isArithmeticExpr(Expression expr, List<Column> cteColumns) {
        if (!(expr instanceof BinaryExpr)) return false;
        BinaryExpr bin = (BinaryExpr) expr;
        BinaryExpr.BinOp op = bin.op();
        if (op != BinaryExpr.BinOp.ADD && op != BinaryExpr.BinOp.SUBTRACT
                && op != BinaryExpr.BinOp.MULTIPLY && op != BinaryExpr.BinOp.DIVIDE
                && op != BinaryExpr.BinOp.MODULO) return false;
        boolean leftIsCol = isCteColumnRef(bin.left(), cteColumns);
        boolean rightIsCol = isCteColumnRef(bin.right(), cteColumns);
        boolean leftIsNumeric = isNumericExpr(bin.left());
        boolean rightIsNumeric = isNumericExpr(bin.right());
        return (leftIsCol && rightIsNumeric) || (rightIsCol && leftIsNumeric);
    }

    private static boolean isCteColumnRef(Expression expr, List<Column> cteColumns) {
        if (!(expr instanceof ColumnRef)) return false;
        ColumnRef cr = (ColumnRef) expr;
        for (Column col : cteColumns) {
            if (col.getName().equalsIgnoreCase(cr.column())) return true;
        }
        return false;
    }

    private static boolean isNumericExpr(Expression expr) {
        if (expr instanceof Literal) {
            Literal lit = (Literal) expr;
            return lit.literalType() == Literal.LiteralType.INTEGER || lit.literalType() == Literal.LiteralType.FLOAT;
        }
        return false;
    }
}
