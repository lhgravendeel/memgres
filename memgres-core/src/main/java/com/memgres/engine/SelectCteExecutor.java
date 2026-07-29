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
        for (Map<String, SelectStmt.CommonTableExpr> scope : executor.cteStack) {
            // A scope may hold a name mapped to null: that is a WITH item deliberately hidden
            // from the body being run (see maskFor), and it must stop the search rather than let
            // an enclosing scope answer, so that the name falls through to a stored relation.
            if (!scope.containsKey(lcName)) continue;
            SelectStmt.CommonTableExpr found = scope.get(lcName);
            // The lexer has already folded an unquoted name to lower case and left a quoted one
            // as written, so what is left to decide is whether the two spellings are the same
            // name: WITH "X" declares X, and a later plain x is a different name entirely.
            if (found != null && !found.name().equals(name)) return null;
            // An item still being computed cannot answer for itself; inside a recursive term the
            // rows of the previous round stand in its place, and elsewhere the name is not there.
            if (found != null && executor.executingCteNodes.contains(found)) return null;
            return found;
        }
        return null;
    }

    /**
     * True when the name belongs to a WITH item somewhere in scope, whether or not that item can
     * be read from here — enough to say that the name is not a stored relation.
     */
    boolean namesWithItem(String name) {
        if (name == null) return false;
        String lcName = name.toLowerCase();
        for (Map<String, SelectStmt.CommonTableExpr> scope : executor.cteStack) {
            if (scope.get(lcName) != null) return true;
        }
        return false;
    }

    /**
     * Says why a name that is a WITH item did not resolve to one, when that is the reason.
     *
     * <p>A plain WITH item cannot see itself or anything written after it, so the name it wrote is
     * a relation name here and the failure is "relation does not exist". PostgreSQL adds the two
     * lines that turn that into an answerable complaint: which WITH item it was, and that
     * RECURSIVE or a re-ordering is the way out.
     */
    void noteHiddenWithItem(MemgresException ex, String name) {
        if (name == null) return;
        String lcName = name.toLowerCase();
        for (Map<String, SelectStmt.CommonTableExpr> scope : executor.cteStack) {
            if (!scope.containsKey(lcName)) continue;
            if (scope.get(lcName) != null) return; // visible here, so this is not the reason
            ex.setDetail("There is a WITH item named \"" + name
                    + "\", but it cannot be referenced from this part of the query.");
            ex.setHint("Use WITH RECURSIVE, or re-order the WITH items to remove forward "
                    + "references.");
            return;
        }
    }

    /**
     * The names a plain (non-RECURSIVE) WITH item may not see, mapped to null so they mask the
     * scope the query itself pushed.
     *
     * <p>{@code WITH x AS (SELECT n FROM y), y AS (...)} is not a forward reference in PostgreSQL
     * — it is an error, because a WITH item without RECURSIVE sees only the items written before
     * it, and its own name is not among them either. If a stored relation happens to carry the
     * name, that relation is what the body reads. Both follow from hiding the item and every
     * later sibling for the duration of its body.
     */
    private Map<String, SelectStmt.CommonTableExpr> maskFor(SelectStmt.CommonTableExpr cte) {
        if (cte.recursive()) return null;
        for (Map<String, SelectStmt.CommonTableExpr> scope : executor.cteStack) {
            int position = -1;
            int i = 0;
            for (SelectStmt.CommonTableExpr sibling : scope.values()) {
                if (sibling == cte) { position = i; break; }
                i++;
            }
            if (position < 0) continue;
            Map<String, SelectStmt.CommonTableExpr> mask = new LinkedHashMap<>();
            int j = 0;
            for (String siblingName : scope.keySet()) {
                if (j++ >= position) mask.put(siblingName, null);
            }
            return mask.isEmpty() ? null : mask;
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

        if (cte.recursive()) detectMutualRecursionCycle(cte, cacheKey);

        // Declaring RECURSIVE does not make a WITH item recursive; naming itself does. One that
        // never names itself is an ordinary query, and running it through the fixed-point loop
        // would repeat its rows, so it takes the plain path — and SEARCH or CYCLE, which order
        // and cut a recursion that is not there, is refused the way PG refuses it.
        if (cte.recursive() && RecursiveCteCheck.selfReferencing(cte)) {
            QueryResult result = executeRecursiveCte(cte);
            executor.cteResultCache.put(cacheKey, result);
            return result;
        }
        if (cte.searchColumn() != null || cte.cycleColumn() != null) {
            throw new MemgresException("WITH query is not recursive", "42601");
        }

        // Re-entering this same item, not merely one of its name: a nested WITH clause may
        // declare the name again, and that inner item is a different query.
        if (executor.executingCteNodes.contains(cte)) {
            throw new MemgresException(
                    "recursive reference to query \"" + cte.name() + "\" must not appear within a non-recursive term", "42P19");
        }
        executor.executingCtes.add(cacheKey);
        executor.executingCteNodes.add(cte);

        QueryResult result;
        Map<String, SelectStmt.CommonTableExpr> mask = maskFor(cte);
        if (mask != null) executor.cteStack.push(mask);
        try {
            result = executor.executeStatement(cte.query());
        } finally {
            if (mask != null) executor.cteStack.pop();
            executor.executingCtes.remove(cacheKey);
            executor.executingCteNodes.remove(cte);
        }

        // A WITH item's alias list renames as far as it reaches. Naming more columns than the
        // query has is an error; naming fewer is not — the columns past the list keep the names
        // the query gave them, the same way a subquery alias list does.
        if (cte.columnNames() != null && !cte.columnNames().isEmpty()) {
            if (cte.columnNames().size() > result.getColumns().size()) {
                throw new MemgresException("WITH query \"" + cte.name() + "\" has " + result.getColumns().size()
                        + " columns available but " + cte.columnNames().size() + " columns specified", "42P10");
            }
            List<Column> renamedCols = new ArrayList<>();
            for (int i = 0; i < result.getColumns().size(); i++) {
                Column orig = result.getColumns().get(i);
                String newName = i < cte.columnNames().size() ? cte.columnNames().get(i) : orig.getName();
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
        RecursiveCteCheck.validate(select, cte);
        SetOpStmt setOp = (SetOpStmt) cte.query();

        QueryResult baseResult = executor.executeStatement(setOp.left());
        List<Column> columns = baseResult.getColumns();

        if (cte.columnNames() != null && !cte.columnNames().isEmpty()) {
            if (cte.columnNames().size() > columns.size()) {
                throw new MemgresException("WITH query \"" + cte.name() + "\" has " + columns.size()
                        + " columns available but " + cte.columnNames().size() + " columns specified", "42P10");
            }
            List<Column> renamedCols = new ArrayList<>();
            for (int i = 0; i < columns.size(); i++) {
                String newName = i < cte.columnNames().size() ? cte.columnNames().get(i) :
                        columns.get(i).getName();
                Column orig = columns.get(i);
                renamedCols.add(new Column(newName, orig.getType(), orig.isNullable(), orig.isPrimaryKey(), orig.getDefaultValue()));
            }
            columns = renamedCols;
        }
        checkSearchAndCycleColumns(cte, columns);

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

        // CYCLE ... SET c [TO v DEFAULT d]: the mark a row carries is v when its cycle columns
        // already appear in its own path and d otherwise, and the recursion stops at any row
        // marked v. Both default to the booleans, and writing the two the same way is what makes
        // the seed itself count as cycled and the query answer with the seed alone.
        Object markTrue = Boolean.TRUE;
        Object markFalse = Boolean.FALSE;
        if (hasCycle && cte.cycleMarkValue() != null) {
            markTrue = executor.exprEvaluator.evalExpr(cte.cycleMarkValue(), null);
            markFalse = executor.exprEvaluator.evalExpr(cte.cycleMarkDefault(), null);
        }
        boolean markedFromTheStart = hasCycle && java.util.Objects.equals(markTrue, markFalse);

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
                isCycleFlags.add(markedFromTheStart);
            }
        }

        List<List<Object>> workingSetSearchPaths = hasSearch ? new ArrayList<>(ordcolPaths) : null;
        List<List<Object>> workingSetCyclePaths = hasCycle ? new ArrayList<>(cyclePaths) : null;

        String cteLower = cte.name().toLowerCase();
        executor.executingCtes.add(cteLower);
        executor.executingCteNodes.add(cte);
        // Stopping the loop early would return a shorter result than the query defines, with
        // nothing to say so; both caps report instead.
        int maxIterations = 10000000;
        int maxRows = 10000000; // safety cap to prevent OOM in mutual recursion scenarios
        try {
        for (int iter = 0; !workingSet.isEmpty(); iter++) {
            StatementCancel.check();
            if (iter >= maxIterations) {
                throw new MemgresException(
                        "recursive query exceeded maximum number of iterations ("
                                + maxIterations + ")", "54001");
            }
            if (allRows.size() > maxRows) {
                throw new MemgresException(
                        "recursive query exceeded maximum number of rows (" + maxRows + ")", "54001");
            }
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
                            validateIterationResult(iterResult, columns, cte.name(), setOp);
                        }

                        List<Object> parentCyclePath = (hasCycle && workingSetCyclePaths != null)
                                ? workingSetCyclePaths.get(pi) : null;

                        for (Object[] row : iterResult.getRows()) {
                            if (isUnionAll || seenKeys.add(Arrays.deepToString(row))) {
                                boolean childIsCycle = markedFromTheStart;
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
                    List<List<Object>> nonCycleSearchPaths = hasSearch ? new ArrayList<>() : null;
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
                        if (hasSearch && workingSetSearchPaths != null && wi < workingSetSearchPaths.size()) {
                            nonCycleSearchPaths.add(workingSetSearchPaths.get(wi));
                        }
                    }
                    workingSetCyclePaths = nonCyclePaths;
                    if (hasSearch) workingSetSearchPaths = nonCycleSearchPaths;
                }

                if (nonCycleWorkingSet.isEmpty()) {
                    workingSet = nonCycleWorkingSet;
                    continue;
                }

                // BFS with per-parent cycle detection: process each parent individually
                // to properly track cycle paths per ancestry chain
                List<Object[]> newRows = new ArrayList<>();
                List<List<Object>> newCyclePaths = hasCycle ? new ArrayList<>() : null;
                List<List<Object>> newSearchPaths = hasSearch ? new ArrayList<>() : null;

                for (int pi = 0; pi < nonCycleWorkingSet.size(); pi++) {
                    Object[] parentRow = nonCycleWorkingSet.get(pi);
                    List<Object> parentCyclePath = (hasCycle && workingSetCyclePaths != null && pi < workingSetCyclePaths.size())
                            ? workingSetCyclePaths.get(pi) : null;
                    List<Object> parentSearchPath = (hasSearch && workingSetSearchPaths != null && pi < workingSetSearchPaths.size())
                            ? workingSetSearchPaths.get(pi) : null;

                    Table singleRowTable = new Table(cteName, columns);
                    singleRowTable.insertRow(parentRow);
                    targetSchema.addTable(singleRowTable);

                    try {
                        QueryResult iterResult = executor.executeStatement(setOp.right());

                        if (iter == 0 && pi == 0) {
                            validateIterationResult(iterResult, columns, cte.name(), setOp);
                        }

                        for (Object[] row : iterResult.getRows()) {
                            if (isUnionAll || seenKeys.add(Arrays.deepToString(row))) {
                                boolean childIsCycle = markedFromTheStart;
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

                                // The breadth-first ordering column leads with how deep the row
                                // is, so the path from the root has to be carried forward here
                                // too — starting a fresh one-element path would make every row
                                // depth 0 and leave ORDER BY ord sorting by the search key alone.
                                if (hasSearch) {
                                    List<Object> childSearchPath = parentSearchPath != null
                                            ? new ArrayList<>(parentSearchPath) : new ArrayList<>();
                                    childSearchPath.add(extractSearchKey(row, searchByIndices));
                                    ordcolPaths.add(childSearchPath);
                                    if (!childIsCycle) newSearchPaths.add(childSearchPath);
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
                workingSetSearchPaths = newSearchPaths;
            }
        }

        } catch (MemgresException e) {
            throw e;
        } finally {
            executor.executingCtes.remove(cteLower);
            executor.executingCteNodes.remove(cte);
        }

        // Add SEARCH ordering column if declared.
        // PG's ordering column is
        //   BFS: one record (depth bigint, search_key1, search_key2, ...)
        //   DFS: an array of such records, one per step of the path from the root
        if (hasSearch) {
            String scol = cte.searchColumn();
            List<Column> extCols = new ArrayList<>(columns);
            extCols.add(depthFirstSearch
                    ? new Column(scol, DataType.RECORD_ARRAY, true, false, null,
                            null, null, null, null, null, null, DataType.RECORD)
                    : new Column(scol, DataType.RECORD, true, false, null));
            if (depthFirstSearch && ordcolPaths != null) {
                // For DFS: sort rows by their search path (element-wise) and assign ordcol
                Integer[] indices = new Integer[allRows.size()];
                for (int i = 0; i < indices.length; i++) indices[i] = i;
                Arrays.sort(indices, (a, b) -> compareSearchPaths(ordcolPaths.get(a), ordcolPaths.get(b)));
                // Build record values from search paths, then reorder rows
                List<Object[]> extRows = new ArrayList<>();
                Object[][] sortedExt = new Object[allRows.size()][];
                for (int rank = 0; rank < indices.length; rank++) {
                    int origIdx = indices[rank];
                    Object[] orig = allRows.get(origIdx);
                    Object[] ext = Arrays.copyOf(orig, orig.length + 1);
                    // DFS: one record per step, the whole path as an array. Kept as the records
                    // themselves rather than their text, so that the column is a record[] to
                    // whatever reads it — a cast of it says so instead of failing to parse the
                    // rendering — and the array rendering follows from the value.
                    ext[orig.length] = recordPath(ordcolPaths.get(origIdx));
                    sortedExt[origIdx] = ext;
                }
                for (Object[] ext : sortedExt) extRows.add(ext);
                columns = extCols;
                allRows = extRows;
            } else {
                // BFS: record is (depth, search_key)
                List<Object[]> extRows = new ArrayList<>();
                // Track depth: base rows are depth 0, each iteration increments
                // ordcolPaths length encodes depth (base=1 element, first recursion=2, etc.)
                for (int i = 0; i < allRows.size(); i++) {
                    Object[] orig = allRows.get(i);
                    Object[] ext = Arrays.copyOf(orig, orig.length + 1);
                    List<Object> pathElements = ordcolPaths != null && i < ordcolPaths.size()
                            ? ordcolPaths.get(i) : Collections.singletonList(i);
                    // BFS record: (depth, search_key_values...)
                    long depth = pathElements.size() - 1;
                    List<Object> recordValues = new ArrayList<>();
                    recordValues.add(depth);
                    // Add the last search key (current row's search value)
                    if (!pathElements.isEmpty()) {
                        Object lastKey = pathElements.get(pathElements.size() - 1);
                        if (lastKey instanceof Object[]) {
                            for (Object k : (Object[]) lastKey) recordValues.add(k);
                        } else {
                            recordValues.add(lastKey);
                        }
                    }
                    ext[orig.length] = new AstExecutor.PgRow(recordValues);
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
            extCols.add(new Column(ccol, markTypeOf(markTrue), true, false, null));
            extCols.add(new Column(pathcol, DataType.RECORD_ARRAY, true, false, null,
                    null, null, null, null, null, null, DataType.RECORD));

            List<Object[]> extRows = new ArrayList<>();
            for (int i = 0; i < allRows.size(); i++) {
                Object[] orig = allRows.get(i);
                boolean isCycle = i < isCycleFlags.size() ? isCycleFlags.get(i) : false;
                List<Object> path = i < cyclePaths.size() ? cyclePaths.get(i) : Collections.singletonList(null);
                Object[] ext = Arrays.copyOf(orig, orig.length + 2);
                ext[orig.length] = isCycle ? markTrue : markFalse;
                ext[orig.length + 1] = formatRecordArray(path);
                extRows.add(ext);
            }
            columns = extCols;
            allRows = extRows;
        }

        return QueryResult.select(columns, allRows);
    }

    /**
     * SEARCH and CYCLE name columns of the WITH query, and add one of their own.
     *
     * <p>A name that is not in the query's column list has nothing to order or to compare, so
     * PostgreSQL refuses it rather than pick a column; and a column the clause <em>adds</em> under
     * a name the query already uses would leave two columns answering to that name, which is
     * ambiguous the moment anything selects it.
     */
    private void checkSearchAndCycleColumns(SelectStmt.CommonTableExpr cte, List<Column> columns) {
        if (cte.searchByColumns() != null) {
            for (String by : cte.searchByColumns()) {
                if (indexOfColumn(columns, by) < 0) {
                    throw new MemgresException(
                            "search column \"" + by + "\" not in WITH query column list", "42601");
                }
            }
        }
        if (cte.cycleByColumns() != null) {
            for (String by : cte.cycleByColumns()) {
                if (indexOfColumn(columns, by) < 0) {
                    throw new MemgresException(
                            "cycle column \"" + by + "\" not in WITH query column list", "42601");
                }
            }
        }
        String[] added = {cte.searchColumn(), cte.cycleColumn(), cte.cyclePathColumn()};
        for (String name : added) {
            if (name != null && indexOfColumn(columns, name) >= 0) {
                throw new MemgresException(
                        "column reference \"" + name + "\" is ambiguous", "42702");
            }
        }
    }

    private static int indexOfColumn(List<Column> columns, String name) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(name)) return i;
        }
        return -1;
    }

    /** Validate the first iteration result of a recursive CTE. */
    private void validateIterationResult(QueryResult iterResult, List<Column> columns,
                                         String cteName, SetOpStmt setOp) {
        Statement recursiveTerm = setOp.right();
        if (!iterResult.getColumns().isEmpty()
                && iterResult.getColumns().size() != columns.size()) {
            throw new MemgresException(
                    "each UNION query must have the same number of columns", "42601");
        }
        // The recursive term's column types are what the first iteration computed; that is the
        // same thing the union would resolve its output type against.
        RecursiveCteCheck.checkColumnTypes(cteName, columns, setOp.left(), setOp.right(),
                iterResult.getColumns());
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

    /**
     * A path rendered the way PostgreSQL renders a {@code record[]}: one composite per step, and
     * a composite quoted inside the array braces when its own text carries a comma.
     */
    /** A search path as the array of records PostgreSQL makes of it, one record per step. */
    static List<Object> recordPath(List<Object> path) {
        List<Object> records = new ArrayList<Object>(path.size());
        for (Object step : path) {
            List<Object> fields = step instanceof List
                    ? new ArrayList<Object>((List<?>) step) : Collections.singletonList(step);
            records.add(new AstExecutor.PgRow(fields));
        }
        return records;
    }

    static String formatRecordArray(List<Object> path) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < path.size(); i++) {
            if (i > 0) sb.append(",");
            Object step = path.get(i);
            List<Object> fields = step instanceof List
                    ? new ArrayList<Object>((List<?>) step) : Collections.singletonList(step);
            String text = new AstExecutor.PgRow(fields).toPgText();
            if (text.indexOf(',') >= 0 || text.indexOf('"') >= 0 || text.indexOf('\\') >= 0) {
                sb.append('"').append(text.replace("\\", "\\\\").replace("\"", "\\\"")).append('"');
            } else {
                sb.append(text);
            }
        }
        sb.append("}");
        return sb.toString();
    }

    /** The declared type of a CYCLE mark column, read off the value the clause set it to. */
    private static DataType markTypeOf(Object markValue) {
        if (markValue instanceof Boolean) return DataType.BOOLEAN;
        if (markValue instanceof Integer) return DataType.INTEGER;
        if (markValue instanceof Long) return DataType.BIGINT;
        if (markValue instanceof java.math.BigDecimal) return DataType.NUMERIC;
        if (markValue instanceof Double || markValue instanceof Float) {
            return DataType.DOUBLE_PRECISION;
        }
        return DataType.TEXT;
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
                if (entry.getValue() == null) continue; // a masked name, not a WITH item in scope
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
