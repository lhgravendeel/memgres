package com.memgres.engine;

import com.memgres.engine.parser.ast.*;
import com.memgres.engine.util.Cols;

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
        String cacheKey = cte.name().toLowerCase();
        QueryResult cached = executor.cteResultCache.get(cacheKey);
        if (cached != null) return cached;

        if (cte.recursive()) {
            QueryResult result = executeRecursiveCte(cte);
            executor.cteResultCache.put(cacheKey, result);
            return result;
        }

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

        boolean hasSearch = cte.searchColumn() != null;
        boolean depthFirstSearch = hasSearch && cte.searchDepthFirst();
        boolean hasCycle = cte.cycleColumn() != null;

        // Resolve SEARCH BY column indices
        int[] searchByIndices = null;
        if (hasSearch && cte.searchByColumns() != null) {
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
        }

        // Resolve CYCLE BY column indices
        int[] cycleByIndices = null;
        if (hasCycle && cte.cycleByColumns() != null) {
            cycleByIndices = new int[cte.cycleByColumns().size()];
            for (int ci2 = 0; ci2 < cte.cycleByColumns().size(); ci2++) {
                String cbyCol = cte.cycleByColumns().get(ci2).toLowerCase();
                cycleByIndices[ci2] = -1;
                for (int ci = 0; ci < columns.size(); ci++) {
                    if (columns.get(ci).getName().equalsIgnoreCase(cbyCol)) {
                        cycleByIndices[ci2] = ci;
                        break;
                    }
                }
                if (cycleByIndices[ci2] == -1) cycleByIndices[ci2] = 0;
            }
        }

        // Track search paths for ordering (both DFS and BFS)
        List<List<Object>> ordcolPaths = hasSearch ? new ArrayList<>() : null;
        if (hasSearch) {
            for (Object[] row : allRows) {
                List<Object> path = new ArrayList<>();
                path.add(extractSearchKey(row, searchByIndices));
                ordcolPaths.add(path);
            }
        }

        // Track cycle paths for cycle detection during recursion
        // Each row gets a list of ancestor cycle key values
        List<List<Object>> cyclePaths = hasCycle ? new ArrayList<>() : null;
        List<Boolean> isCycleFlags = hasCycle ? new ArrayList<>() : null;
        if (hasCycle) {
            for (Object[] row : allRows) {
                List<Object> path = new ArrayList<>();
                path.add(extractSearchKey(row, cycleByIndices));
                cyclePaths.add(path);
                isCycleFlags.add(false);
            }
        }

        List<List<Object>> workingSetSearchPaths = hasSearch ? new ArrayList<>(ordcolPaths) : null;
        List<List<Object>> workingSetCyclePaths = hasCycle ? new ArrayList<>(cyclePaths) : null;

        String cteLower = cte.name().toLowerCase();
        detectMutualRecursionCycle(cte, cteLower);
        executor.executingCtes.add(cteLower);
        int maxIterations = 1000;
        int maxRows = 10000; // safety cap to prevent OOM in mutual recursion scenarios
        try {
        for (int iter = 0; iter < maxIterations && !workingSet.isEmpty(); iter++) {
            if (allRows.size() > maxRows) break; // prevent runaway growth
            String cteName = cte.name().toLowerCase();
            Schema targetSchema = executor.database.getOrCreateSchema(executor.defaultSchema());
            Table previousTable = targetSchema.getTable(cteName);

            if (depthFirstSearch) {
                // DFS: process one parent row at a time
                List<Object[]> newRows = new ArrayList<>();
                List<List<Object>> newSearchPaths = hasSearch ? new ArrayList<>() : null;
                List<List<Object>> newCyclePaths = hasCycle ? new ArrayList<>() : null;
                for (int pi = 0; pi < workingSet.size(); pi++) {
                    Object[] parentRow = workingSet.get(pi);

                    // Skip recursion for rows already marked as cycle
                    if (hasCycle) {
                        // Find this row's global index
                        int parentGlobalIdx = allRows.indexOf(parentRow);
                        if (parentGlobalIdx >= 0 && parentGlobalIdx < isCycleFlags.size()
                                && isCycleFlags.get(parentGlobalIdx)) {
                            continue;
                        }
                    }

                    Table singleRowTable = new Table(cteName, columns);
                    singleRowTable.insertRow(parentRow);
                    targetSchema.addTable(singleRowTable);

                    try {
                        QueryResult iterResult = executor.executeStatement(setOp.right());

                        if (iter == 0 && pi == 0) {
                            validateIterationResult(iterResult, columns, setOp.right());
                        }

                        List<Object> parentCyclePath = (hasCycle && workingSetCyclePaths != null)
                                ? workingSetCyclePaths.get(pi) : null;

                        for (Object[] row : iterResult.getRows()) {
                            if (isUnionAll || seenKeys.add(Arrays.deepToString(row))) {
                                boolean childIsCycle = false;
                                if (hasCycle && parentCyclePath != null) {
                                    Object childKey = extractSearchKey(row, cycleByIndices);
                                    for (Object ancestorKey : parentCyclePath) {
                                        if (java.util.Objects.equals(ancestorKey, childKey)) {
                                            childIsCycle = true;
                                            break;
                                        }
                                    }
                                }

                                // Always emit the row (PG emits cycle rows with is_cycle=true)
                                allRows.add(row);

                                if (hasCycle) {
                                    isCycleFlags.add(childIsCycle);
                                }

                                // Add search path for all rows (needed for ordcol indexing)
                                if (hasSearch) {
                                    List<Object> parentSearchPath = workingSetSearchPaths.get(pi);
                                    List<Object> childSearchPath = new ArrayList<>(parentSearchPath);
                                    childSearchPath.add(extractSearchKey(row, searchByIndices));
                                    ordcolPaths.add(childSearchPath);
                                    if (!childIsCycle) {
                                        newSearchPaths.add(childSearchPath);
                                    }
                                }

                                // Add cycle path for all rows (needed for is_cycle/path indexing)
                                if (hasCycle) {
                                    List<Object> childCyclePath = new ArrayList<>(parentCyclePath);
                                    Object childKey = extractSearchKey(row, cycleByIndices);
                                    childCyclePath.add(childKey);
                                    cyclePaths.add(childCyclePath);
                                    if (!childIsCycle) {
                                        newCyclePaths.add(childCyclePath);
                                    }
                                }

                                if (childIsCycle) {
                                    // Don't recurse from cycle rows — but the row IS emitted
                                    continue;
                                }

                                // Non-cycle row: add to working set for further recursion
                                newRows.add(row);
                            }
                        }
                    } finally {
                        targetSchema.removeTable(cteName);
                        if (previousTable != null) targetSchema.addTable(previousTable);
                    }
                }
                workingSet = newRows;
                workingSetSearchPaths = newSearchPaths;
                workingSetCyclePaths = newCyclePaths;
            } else {
                // BFS: process entire working set at once
                // Filter out cycle-marked rows from working set
                List<Object[]> nonCycleWorkingSet = workingSet;
                if (hasCycle) {
                    nonCycleWorkingSet = new ArrayList<>();
                    List<List<Object>> nonCyclePaths = new ArrayList<>();
                    int wsOffset = allRows.size() - workingSet.size();
                    for (int wi = 0; wi < workingSet.size(); wi++) {
                        int globalIdx = wsOffset + wi;
                        if (globalIdx < isCycleFlags.size() && isCycleFlags.get(globalIdx)) {
                            continue; // Skip cycle-marked rows
                        }
                        nonCycleWorkingSet.add(workingSet.get(wi));
                        if (workingSetCyclePaths != null && wi < workingSetCyclePaths.size()) {
                            nonCyclePaths.add(workingSetCyclePaths.get(wi));
                        }
                    }
                    workingSetCyclePaths = nonCyclePaths;
                }

                if (nonCycleWorkingSet.isEmpty()) {
                    workingSet = nonCycleWorkingSet;
                    continue;
                }

                // BFS with per-parent cycle detection: process each parent individually
                // to properly track cycle paths per ancestry chain
                List<Object[]> newRows = new ArrayList<>();
                List<List<Object>> newCyclePaths = hasCycle ? new ArrayList<>() : null;

                for (int pi = 0; pi < nonCycleWorkingSet.size(); pi++) {
                    Object[] parentRow = nonCycleWorkingSet.get(pi);
                    List<Object> parentCyclePath = (hasCycle && workingSetCyclePaths != null && pi < workingSetCyclePaths.size())
                            ? workingSetCyclePaths.get(pi) : null;

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
                                boolean childIsCycle = false;
                                if (hasCycle && parentCyclePath != null) {
                                    Object childKey = extractSearchKey(row, cycleByIndices);
                                    for (Object ancestorKey : parentCyclePath) {
                                        if (java.util.Objects.equals(ancestorKey, childKey)) {
                                            childIsCycle = true;
                                            break;
                                        }
                                    }
                                }

                                // Always emit the row (PG emits cycle rows with is_cycle=true)
                                allRows.add(row);

                                if (hasCycle) {
                                    isCycleFlags.add(childIsCycle);
                                }

                                // Add search path for all rows (needed for ordcol indexing)
                                if (hasSearch) {
                                    List<Object> searchPath = new ArrayList<>();
                                    searchPath.add(extractSearchKey(row, searchByIndices));
                                    ordcolPaths.add(searchPath);
                                }

                                // Add cycle path for all rows
                                if (hasCycle) {
                                    List<Object> childCyclePath = parentCyclePath != null
                                            ? new ArrayList<>(parentCyclePath) : new ArrayList<>();
                                    Object childKey = extractSearchKey(row, cycleByIndices);
                                    childCyclePath.add(childKey);
                                    cyclePaths.add(childCyclePath);
                                    if (!childIsCycle) {
                                        newCyclePaths.add(childCyclePath);
                                    }
                                }

                                if (childIsCycle) {
                                    // Don't recurse from cycle rows
                                    continue;
                                }

                                newRows.add(row);
                            }
                        }
                    } finally {
                        targetSchema.removeTable(cteName);
                        if (previousTable != null) targetSchema.addTable(previousTable);
                    }
                }

                // Fixed-point detection for UNION ALL: if new rows match previous working set exactly,
                // we've reached a stable state and should terminate (important for mutual recursion)
                if (isUnionAll && newRows.size() == workingSet.size()) {
                    boolean stable = true;
                    for (int ri = 0; ri < newRows.size(); ri++) {
                        if (!Arrays.deepEquals(newRows.get(ri), workingSet.get(ri))) {
                            stable = false;
                            break;
                        }
                    }
                    if (stable) {
                        workingSet = Collections.emptyList();
                        continue;
                    }
                }

                workingSet = newRows;
                workingSetCyclePaths = newCyclePaths;
            }
        }

        } catch (MemgresException e) {
            throw e;
        } finally {
            executor.executingCtes.remove(cteLower);
        }

        // Add SEARCH ordering column if declared
        if (hasSearch) {
            String scol = cte.searchColumn();
            List<Column> extCols = new ArrayList<>(columns);
            if (depthFirstSearch && ordcolPaths != null) {
                // For DFS: sort rows by their search path (element-wise) and assign integer ordcol
                // Build index array sorted by path comparison
                Integer[] indices = new Integer[allRows.size()];
                for (int i = 0; i < indices.length; i++) indices[i] = i;
                Arrays.sort(indices, (a, b) -> compareSearchPaths(ordcolPaths.get(a), ordcolPaths.get(b)));
                // Assign ordcol based on sorted position
                int[] ordcolValues = new int[allRows.size()];
                for (int rank = 0; rank < indices.length; rank++) {
                    ordcolValues[indices[rank]] = rank;
                }
                extCols.add(new Column(scol, DataType.INTEGER, true, false, null));
                List<Object[]> extRows = new ArrayList<>();
                for (int i = 0; i < allRows.size(); i++) {
                    Object[] orig = allRows.get(i);
                    Object[] ext = Arrays.copyOf(orig, orig.length + 1);
                    ext[orig.length] = ordcolValues[i];
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
        if (hasCycle) {
            String ccol = cte.cycleColumn();
            String pathcol = cte.cyclePathColumn() != null ? cte.cyclePathColumn() : "path";
            List<Column> extCols = new ArrayList<>(columns);
            extCols.add(new Column(ccol, DataType.BOOLEAN, true, false, null));
            extCols.add(new Column(pathcol, DataType.TEXT, true, false, null));

            List<Object[]> extRows = new ArrayList<>();
            for (int i = 0; i < allRows.size(); i++) {
                Object[] orig = allRows.get(i);
                boolean isCycle = i < isCycleFlags.size() ? isCycleFlags.get(i) : false;
                List<Object> path = i < cyclePaths.size() ? cyclePaths.get(i) : Collections.singletonList(null);
                Object[] ext = Arrays.copyOf(orig, orig.length + 2);
                ext[orig.length] = isCycle;
                ext[orig.length + 1] = formatRecordArray(path);
                extRows.add(ext);
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

    /** Compare two search paths element-wise for DFS ordering. */
    @SuppressWarnings("unchecked")
    static int compareSearchPaths(List<Object> a, List<Object> b) {
        int len = Math.min(a.size(), b.size());
        for (int i = 0; i < len; i++) {
            Object va = a.get(i);
            Object vb = b.get(i);
            if (va == null && vb == null) continue;
            if (va == null) return -1;
            if (vb == null) return 1;
            if (va instanceof Comparable && vb instanceof Comparable) {
                try {
                    int cmp = ((Comparable) va).compareTo(vb);
                    if (cmp != 0) return cmp;
                } catch (ClassCastException e) {
                    int cmp = String.valueOf(va).compareTo(String.valueOf(vb));
                    if (cmp != 0) return cmp;
                }
            } else {
                int cmp = String.valueOf(va).compareTo(String.valueOf(vb));
                if (cmp != 0) return cmp;
            }
        }
        return Integer.compare(a.size(), b.size());
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

    /**
     * Detect mutual recursion between RECURSIVE CTEs and reject it.
     * PG 18 does not support mutual recursion between WITH items (error 0A000).
     */
    private void detectMutualRecursionCycle(SelectStmt.CommonTableExpr cte, String cteLower) {
        if (!cte.recursive()) return;
        // Walk the recursive term of this CTE and find references to sibling recursive CTEs
        Map<String, SelectStmt.CommonTableExpr> siblingMap = new HashMap<>();
        for (Deque<Map<String, SelectStmt.CommonTableExpr>> stack = executor.cteStack;
             !stack.isEmpty(); ) {
            for (Map.Entry<String, SelectStmt.CommonTableExpr> entry : stack.peek().entrySet()) {
                if (!entry.getKey().equals(cteLower) && entry.getValue().recursive()) {
                    siblingMap.put(entry.getKey(), entry.getValue());
                }
            }
            break; // only check current scope
        }
        if (siblingMap.isEmpty()) return;
        // Check if this CTE references any sibling recursive CTE
        Set<String> refs = new HashSet<>();
        collectSiblingRefs(cte.query(), siblingMap.keySet(), refs);
        if (refs.isEmpty()) return;
        // True mutual recursion: the referenced sibling must also reference this CTE
        for (String refName : refs) {
            SelectStmt.CommonTableExpr sibling = siblingMap.get(refName);
            if (sibling != null) {
                Set<String> backRefs = new HashSet<>();
                collectSiblingRefs(sibling.query(), Cols.setOf(cteLower), backRefs);
                if (!backRefs.isEmpty()) {
                    throw new MemgresException(
                            "mutual recursion between WITH items is not implemented", "0A000");
                }
            }
        }
    }

    /**
     * Walk an AST fragment (statement/expression/list) via reflection and collect
     * unqualified TableRef names that match any sibling CTE name in scope. Used
     * only for mutual-recursion cycle detection — correctness of reflection walk
     * is bounded by the recursive-term tree size.
     */
    private static void collectSiblingRefs(Object node, Set<String> scopeNames, Set<String> out) {
        if (node == null) return;
        Deque<Object> stack = new ArrayDeque<>();
        Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        stack.push(node);
        while (!stack.isEmpty()) {
            Object cur = stack.pop();
            if (cur == null || !seen.add(cur)) continue;
            if (cur instanceof SelectStmt.TableRef) {
                SelectStmt.TableRef tr = (SelectStmt.TableRef) cur;
                if (tr.schema() == null && tr.table() != null) {
                    String lc = tr.table().toLowerCase();
                    if (scopeNames.contains(lc)) out.add(lc);
                }
                continue;
            }
            if (cur instanceof String || cur instanceof Number || cur instanceof Boolean
                    || cur instanceof Character || cur instanceof Enum) continue;
            if (cur instanceof Collection) {
                for (Object item : (Collection<?>) cur) stack.push(item);
                continue;
            }
            if (cur instanceof Map) {
                for (Object item : ((Map<?, ?>) cur).values()) stack.push(item);
                continue;
            }
            Class<?> cls = cur.getClass();
            Package pkg = cls.getPackage();
            if (pkg == null || !pkg.getName().startsWith("com.memgres")) continue;
            for (java.lang.reflect.Field f : cls.getDeclaredFields()) {
                if (java.lang.reflect.Modifier.isStatic(f.getModifiers())) continue;
                try {
                    f.setAccessible(true);
                    Object v = f.get(cur);
                    if (v != null) stack.push(v);
                } catch (IllegalAccessException | RuntimeException ignored) { }
            }
        }
    }
}
