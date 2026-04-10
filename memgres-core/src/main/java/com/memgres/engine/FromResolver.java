package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.*;

/**
 * Resolves FROM clauses and JOIN operations for SELECT, UPDATE, and DELETE statements.
 * Delegates to FromFunctionResolver for set-returning functions and FromJoinExecutor for joins.
 */
class FromResolver {
    final AstExecutor executor;
    final FromFunctionResolver functionResolver;
    final FromJoinExecutor joinExecutor;
    // Track the last resolved table info for LEFT JOIN null-padding when right side is empty
    Table lastResolvedRightTable;
    String lastResolvedRightAlias;

    FromResolver(AstExecutor executor) {
        this.executor = executor;
        this.functionResolver = new FromFunctionResolver(executor);
        this.joinExecutor = new FromJoinExecutor(this);
    }

    // ---- Table Bindings (column structure without data) ----

    List<RowContext.TableBinding> resolveTableBindings(List<SelectStmt.FromItem> fromItems) {
        List<RowContext.TableBinding> bindings = new ArrayList<>();
        for (SelectStmt.FromItem item : fromItems) {
            resolveTableBindingsFromItem(item, bindings);
        }
        return bindings;
    }

    private void resolveTableBindingsFromItem(SelectStmt.FromItem item, List<RowContext.TableBinding> bindings) {
        if (item instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef tableRef = (SelectStmt.TableRef) item;
            String schemaName = tableRef.schema() != null ? tableRef.schema() : executor.defaultSchema();
            String alias = tableRef.alias() != null ? tableRef.alias() : tableRef.table();
            // Check CTEs first
            SelectStmt.CommonTableExpr cte = executor.selectExecutor.lookupCte(tableRef.table());
            if (cte != null) {
                QueryResult cteResult = executor.selectExecutor.executeCte(cte);
                Table virtualTable = new Table(alias, cteResult.getColumns());
                bindings.add(new RowContext.TableBinding(virtualTable, alias, new Object[virtualTable.getColumns().size()]));
                return;
            }
            // Check views
            Database.ViewDef view = executor.database.getView(tableRef.table());
            if (view != null) {
                try {
                    QueryResult vr = executor.executeStatement(view.query());
                    if (!vr.getColumns().isEmpty()) {
                        bindings.add(new RowContext.TableBinding(
                                new Table(alias, vr.getColumns()), alias, new Object[vr.getColumns().size()]));
                    }
                } catch (Exception e) { /* skip */ }
                return;
            }
            // Check system catalogs
            boolean userTableExists = false;
            try { executor.resolveTable(schemaName, tableRef.table()); userTableExists = true; } catch (MemgresException ignored) {}
            if (!userTableExists && SystemCatalog.isSystemCatalog(tableRef.schema(), tableRef.table())) {
                Table catalogTable = executor.systemCatalog.resolve(tableRef.schema(), tableRef.table());
                if (catalogTable != null) {
                    bindings.add(new RowContext.TableBinding(catalogTable, alias, new Object[catalogTable.getColumns().size()]));
                    return;
                }
            }
            // Regular table
            try {
                Table table = executor.resolveTable(schemaName, tableRef.table());
                bindings.add(new RowContext.TableBinding(table, alias, new Object[table.getColumns().size()]));
            } catch (MemgresException e) { /* table not found, skip */ }
        } else if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom joinFrom = (SelectStmt.JoinFrom) item;
            resolveTableBindingsFromItem(joinFrom.left(), bindings);
            int beforeRight = bindings.size();
            resolveTableBindingsFromItem(joinFrom.right(), bindings);
            // Apply USING column dedup for SELECT * / RowDescription
            if (joinFrom.using() != null && !joinFrom.using().isEmpty()) {
                Set<String> usingLower = new HashSet<>();
                for (String col : joinFrom.using()) usingLower.add(col.toLowerCase());
                for (int bi = beforeRight; bi < bindings.size(); bi++) {
                    RowContext.TableBinding b = bindings.get(bi);
                    List<Column> origCols = b.table().getColumns();
                    List<Integer> indicesToRemove = new ArrayList<>();
                    for (int ci = 0; ci < origCols.size(); ci++) {
                        if (usingLower.contains(origCols.get(ci).getName().toLowerCase())) {
                            indicesToRemove.add(ci);
                        }
                    }
                    if (!indicesToRemove.isEmpty()) {
                        Set<Integer> removeSet = new HashSet<>(indicesToRemove);
                        List<Column> newCols = new ArrayList<>();
                        for (int ci = 0; ci < origCols.size(); ci++) {
                            if (!removeSet.contains(ci)) newCols.add(origCols.get(ci));
                        }
                        if (!newCols.isEmpty()) {
                            bindings.set(bi, new RowContext.TableBinding(
                                    new Table(b.table().getName(), newCols), b.alias(),
                                    new Object[newCols.size()]));
                        } else {
                            bindings.remove(bi);
                            bi--;
                        }
                    }
                }
            }
        } else if (item instanceof SelectStmt.SubqueryFrom) {
            SelectStmt.SubqueryFrom subqFrom = (SelectStmt.SubqueryFrom) item;
            if (subqFrom.alias() != null) {
                try {
                    QueryResult sqResult = executor.executeStatement(subqFrom.subquery());
                    if (!sqResult.getColumns().isEmpty()) {
                        List<Column> columns = FromFunctionResolver.applyColumnAliases(
                                new ArrayList<>(sqResult.getColumns()), subqFrom.columnAliases());
                        String sqAlias = subqFrom.alias();
                        Table virtualTable = new Table(sqAlias, columns);
                        bindings.add(new RowContext.TableBinding(virtualTable, sqAlias,
                                new Object[columns.size()]));
                    }
                } catch (Exception e) { /* skip, can't resolve */ }
            }
        } else if (item instanceof SelectStmt.FunctionFrom) {
            SelectStmt.FunctionFrom funcFrom = (SelectStmt.FunctionFrom) item;
            String alias = funcFrom.alias() != null ? funcFrom.alias() : funcFrom.functionName();
            List<String> ca = funcFrom.columnAliases();
            if (ca != null && !ca.isEmpty()) {
                List<Column> cols = new ArrayList<>();
                for (String colName : ca) {
                    cols.add(new Column(colName, DataType.TEXT, true, false, null));
                }
                Table virtualTable = new Table(alias, cols);
                bindings.add(new RowContext.TableBinding(virtualTable, alias, new Object[cols.size()]));
            } else {
                List<Column> cols = Cols.listOf(new Column(alias, DataType.TEXT, true, false, null));
                Table virtualTable = new Table(alias, cols);
                bindings.add(new RowContext.TableBinding(virtualTable, alias, new Object[1]));
            }
        }
    }

    // ---- FROM Clause Resolution ----

    List<RowContext> resolveFromClause(List<SelectStmt.FromItem> fromItems) {
        return resolveFromClause(fromItems, null);
    }

    /**
     * Resolve FROM clause with optional WHERE pushdown for early filtering during cross-product.
     */
    List<RowContext> resolveFromClause(List<SelectStmt.FromItem> fromItems, Expression where) {
        if (fromItems.size() == 1) {
            // For single-table queries, try index scan optimization
            if (where != null && fromItems.get(0) instanceof SelectStmt.TableRef) {
                List<RowContext> indexed = tryIndexScan((SelectStmt.TableRef) fromItems.get(0), where);
                if (indexed != null) return indexed;
            }
            return resolveFromItem(fromItems.get(0));
        }

        // Check if any FROM item is a LATERAL subquery or function call (implicit LATERAL)
        boolean hasLateral = false;
        for (SelectStmt.FromItem item : fromItems) {
            if (item instanceof SelectStmt.SubqueryFrom && ((SelectStmt.SubqueryFrom) item).lateral()) {
                SelectStmt.SubqueryFrom sqf = (SelectStmt.SubqueryFrom) item;
                hasLateral = true;
                break;
            }
            if (item instanceof SelectStmt.FunctionFrom) {
                hasLateral = true;
                break;
            }
        }

        if (hasLateral) {
            return resolveFromClauseWithLateral(fromItems, where);
        }

        // Multiple FROM items = implicit cross join (no LATERAL)
        if (where != null) {
            return crossProductWithEarlyFilter(fromItems, where);
        }
        List<List<RowContext>> resolvedItems = new ArrayList<>();
        for (SelectStmt.FromItem fromItem : fromItems) {
            resolvedItems.add(resolveFromItem(fromItem));
        }
        return crossProductContexts(resolvedItems);
    }

    /**
     * Process FROM items sequentially when LATERAL subqueries are present.
     */
    private List<RowContext> resolveFromClauseWithLateral(List<SelectStmt.FromItem> fromItems, Expression where) {
        List<RowContext> accumulated = null;
        List<Expression> wherePredicates = where != null ? flattenAndPredicates(where) : Cols.listOf();
        Set<Integer> appliedPredicates = new HashSet<>();

        for (int itemIdx = 0; itemIdx < fromItems.size(); itemIdx++) {
            SelectStmt.FromItem fromItem = fromItems.get(itemIdx);
            boolean isLateralSubquery = fromItem instanceof SelectStmt.SubqueryFrom && ((SelectStmt.SubqueryFrom) fromItem).lateral();
            boolean isFunctionFrom = fromItem instanceof SelectStmt.FunctionFrom;

            if (isLateralSubquery) {
                SelectStmt.SubqueryFrom sqf = (SelectStmt.SubqueryFrom) fromItem;
                if (accumulated == null || accumulated.isEmpty()) {
                    accumulated = resolveFromItem(fromItem);
                    continue;
                }

                List<RowContext> newAccumulated = new ArrayList<>();
                for (RowContext leftCtx : accumulated) {
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
                        List<Column> columns = FromFunctionResolver.applyColumnAliases(
                                new ArrayList<>(subResult.getColumns()), sqf.columnAliases());
                        Table virtualTable = new Table(alias, columns);

                        if (subResult.getRows().isEmpty()) {
                            // Implicit INNER JOIN semantics, skip
                        } else {
                            for (Object[] row : subResult.getRows()) {
                                RowContext rightCtx = new RowContext(virtualTable, alias, row);
                                newAccumulated.add(joinExecutor.mergeContexts(leftCtx, rightCtx));
                            }
                        }
                    } finally {
                        executor.outerContextStack.pop();
                    }
                }
                accumulated = newAccumulated;
            } else if (isFunctionFrom && accumulated != null && !accumulated.isEmpty()) {
                SelectStmt.FunctionFrom funcFrom = (SelectStmt.FunctionFrom) fromItem;
                List<RowContext> newAccumulated = new ArrayList<>();
                for (RowContext leftCtx : accumulated) {
                    executor.outerContextStack.push(leftCtx);
                    try {
                        List<RowContext> funcRows = functionResolver.resolveFunctionFrom(funcFrom);
                        if (funcRows.isEmpty()) {
                            String alias = funcFrom.alias() != null ? funcFrom.alias() : funcFrom.functionName();
                            List<Column> emptyCols = funcFrom.columnAliases() != null
                                    ? funcFrom.columnAliases().stream()
                                        .map(c -> new Column(c, DataType.TEXT, true, false, null))
                                        .collect(java.util.stream.Collectors.toList())
                                    : Cols.listOf();
                            Table virtualTable = new Table(alias, emptyCols);
                            Object[] nullRow = new Object[emptyCols.size()];
                            RowContext rightCtx = new RowContext(virtualTable, alias, nullRow);
                            newAccumulated.add(joinExecutor.mergeContexts(leftCtx, rightCtx));
                        } else {
                            for (RowContext rightCtx : funcRows) {
                                newAccumulated.add(joinExecutor.mergeContexts(leftCtx, rightCtx));
                            }
                        }
                    } finally {
                        executor.outerContextStack.pop();
                    }
                }
                accumulated = newAccumulated;
            } else {
                List<RowContext> resolved = resolveFromItem(fromItem);
                if (accumulated == null) {
                    accumulated = resolved;
                } else {
                    List<Expression> applicablePredicates = new ArrayList<>();
                    for (int pi = 0; pi < wherePredicates.size(); pi++) {
                        if (!appliedPredicates.contains(pi)) {
                            Expression pred = wherePredicates.get(pi);
                            if (!accumulated.isEmpty() && !resolved.isEmpty()) {
                                RowContext sample = joinExecutor.mergeContexts(accumulated.get(0), resolved.get(0));
                                if (canEvaluatePredicate(pred, sample)) {
                                    applicablePredicates.add(pred);
                                    appliedPredicates.add(pi);
                                }
                            }
                        }
                    }

                    List<RowContext> newAccumulated = new ArrayList<>();
                    for (RowContext leftCtx : accumulated) {
                        for (RowContext rightCtx : resolved) {
                            RowContext merged = joinExecutor.mergeContexts(leftCtx, rightCtx);
                            if (passesEarlyPredicates(merged, applicablePredicates)) {
                                newAccumulated.add(merged);
                            }
                        }
                    }
                    accumulated = newAccumulated;
                }
            }
        }

        return accumulated != null ? accumulated : Cols.listOf();
    }

    // ---- Resolve single FROM item ----

    List<RowContext> resolveFromItem(SelectStmt.FromItem fromItem) {
        if (fromItem instanceof SelectStmt.TableRef) return resolveTableRef(((SelectStmt.TableRef) fromItem));
        if (fromItem instanceof SelectStmt.SubqueryFrom) return resolveSubquery(((SelectStmt.SubqueryFrom) fromItem));
        if (fromItem instanceof SelectStmt.FunctionFrom) return functionResolver.resolveFunctionFrom(((SelectStmt.FunctionFrom) fromItem));
        if (fromItem instanceof SelectStmt.JoinFrom) return joinExecutor.executeJoin(((SelectStmt.JoinFrom) fromItem));
        throw new IllegalArgumentException("Unknown FromItem type: " + fromItem.getClass().getSimpleName());
    }

    private List<RowContext> resolveTableRef(SelectStmt.TableRef tableRef) {
        // Check CTEs first
        SelectStmt.CommonTableExpr cte = executor.selectExecutor.lookupCte(tableRef.table());
        if (cte != null) {
            QueryResult cteResult = executor.selectExecutor.executeCte(cte);
            String alias = tableRef.alias() != null ? tableRef.alias() : tableRef.table();
            Table virtualTable = new Table(alias, cteResult.getColumns());
            lastResolvedRightTable = virtualTable;
            lastResolvedRightAlias = alias;
            for (Object[] row : cteResult.getRows()) {
                virtualTable.insertRow(row);
            }
            List<RowContext> contexts = new ArrayList<>();
            for (Object[] row : virtualTable.getRows()) {
                contexts.add(new RowContext(virtualTable, alias, row));
            }
            return contexts;
        }

        // Check views
        Database.ViewDef view = executor.database.getView(tableRef.table());
        if (view != null) {
            String alias = tableRef.alias() != null ? tableRef.alias() : tableRef.table();
            List<Column> cols;
            List<Object[]> rows;
            if (view.materialized() && view.cachedColumns() != null) {
                cols = view.cachedColumns();
                rows = view.cachedRows();
            } else {
                QueryResult viewResult = executor.executeStatement(view.query());
                cols = viewResult.getColumns();
                rows = viewResult.getRows();
            }
            Table virtualTable = new Table(alias, cols);
            lastResolvedRightTable = virtualTable;
            lastResolvedRightAlias = alias;
            for (Object[] row : rows) {
                virtualTable.insertRow(row);
            }
            List<RowContext> contexts = new ArrayList<>();
            for (Object[] row : virtualTable.getRows()) {
                contexts.add(new RowContext(virtualTable, alias, row));
            }
            return contexts;
        }

        // Check system catalogs
        String schemaName = tableRef.schema() != null ? tableRef.schema() : executor.defaultSchema();
        boolean userTableExists = false;
        try { executor.resolveTable(schemaName, tableRef.table()); userTableExists = true; } catch (MemgresException ignored) {}
        if (!userTableExists && SystemCatalog.isSystemCatalog(tableRef.schema(), tableRef.table())) {
            Table catalogTable = executor.systemCatalog.resolve(tableRef.schema(), tableRef.table());
            if (catalogTable != null) {
                String alias = tableRef.alias() != null ? tableRef.alias() : tableRef.table();
                lastResolvedRightTable = catalogTable;
                lastResolvedRightAlias = alias;
                List<RowContext> contexts = new ArrayList<>();
                for (Object[] row : catalogTable.getRows()) {
                    contexts.add(new RowContext(catalogTable, alias, row));
                }
                return contexts;
            }
        }
        Table table = executor.resolveTable(schemaName, tableRef.table());
        String alias = tableRef.alias() != null ? tableRef.alias() : tableRef.table();
        lastResolvedRightTable = table;
        lastResolvedRightAlias = alias;

        // MVCC: Check for REPEATABLE READ snapshot
        String schemaTableKey = schemaName + "." + tableRef.table();
        Session currentSession = executor.session;
        if (currentSession != null && currentSession.hasRRSnapshot(schemaTableKey)) {
            List<Object[]> snapshot = currentSession.getRRSnapshot(schemaTableKey);
            boolean snapshotHasVirtual = executor.dmlExecutor.hasVirtualColumns(table);
            List<RowContext> contexts = new ArrayList<>();
            for (Object[] row : snapshot) {
                Object[] r = snapshotHasVirtual ? executor.dmlExecutor.computeVirtualColumns(table, row) : row;
                contexts.add(new RowContext(table, alias, r));
            }
            return contexts;
        }

        // Use getAllRowsWithSource for inheritance/partitioning
        boolean hasVirtual = executor.dmlExecutor.hasVirtualColumns(table);
        List<RowContext> contexts = new ArrayList<>();
        if (tableRef.only()) {
            for (Object[] row : table.getRows()) {
                Object[] r = hasVirtual ? executor.dmlExecutor.computeVirtualColumns(table, row) : row;
                contexts.add(new RowContext(table, alias, r));
            }
        } else {
            for (Table.RowWithSource rws : table.getAllRowsWithSource()) {
                Object[] r = hasVirtual ? executor.dmlExecutor.computeVirtualColumns(table, rws.row()) : rws.row();
                contexts.add(new RowContext(Cols.listOf(
                        new RowContext.TableBinding(table, alias, r, rws.source()))));
            }
        }

        // MVCC: Filter out uncommitted changes from other sessions
        if (currentSession != null) {
            contexts = applyMvccVisibility(contexts, table, alias, schemaTableKey, currentSession);
        }

        // Apply Row-Level Security filtering
        if (table.isRlsEnabled() && !table.getRlsPolicies().isEmpty()) {
            contexts = applyRlsFiltering(contexts, table);
        }
        return contexts;
    }

    /**
     * Try to use an index scan for a single-table query with equality predicates in WHERE.
     * Returns null if no suitable index is found and we should fall back to sequential scan.
     */
    private List<RowContext> tryIndexScan(SelectStmt.TableRef tableRef, Expression where) {
        // Only optimize regular user tables (skip CTEs, views, system catalogs)
        if (executor.selectExecutor.lookupCte(tableRef.table()) != null) return null;
        if (executor.database.getView(tableRef.table()) != null) return null;
        String schemaName = tableRef.schema() != null ? tableRef.schema() : executor.defaultSchema();
        Table table;
        try {
            table = executor.resolveTable(schemaName, tableRef.table());
        } catch (MemgresException e) {
            return null;
        }
        // Skip if table has partitions/inheritance (complex row sources)
        if (!table.getPartitions().isEmpty() || table.getParentTable() != null) return null;
        // Skip ONLY queries (rare, let normal path handle)
        if (tableRef.only()) return null;

        // Extract equality predicates: flatten ANDs and look for col = literal patterns
        List<Expression> predicates = flattenAndPredicates(where);
        Map<String, Object> equalityMap = new LinkedHashMap<>();
        List<Expression> remainingPredicates = new ArrayList<>();

        for (Expression pred : predicates) {
            String colName = null;
            Object value = null;
            if (pred instanceof BinaryExpr) {
                BinaryExpr bin = (BinaryExpr) pred;
                if ("=".equals(bin.op())) {
                    if (bin.left() instanceof ColumnRef && isLiteralOrParam(bin.right())) {
                        colName = ((ColumnRef) bin.left()).column();
                        if (((ColumnRef) bin.left()).table() != null) {
                            String tRef = ((ColumnRef) bin.left()).table();
                            String alias = tableRef.alias() != null ? tableRef.alias() : tableRef.table();
                            if (!tRef.equalsIgnoreCase(alias) && !tRef.equalsIgnoreCase(tableRef.table())) {
                                remainingPredicates.add(pred);
                                continue;
                            }
                        }
                        value = extractLiteralValue(bin.right());
                    } else if (bin.right() instanceof ColumnRef && isLiteralOrParam(bin.left())) {
                        colName = ((ColumnRef) bin.right()).column();
                        if (((ColumnRef) bin.right()).table() != null) {
                            String tRef = ((ColumnRef) bin.right()).table();
                            String alias = tableRef.alias() != null ? tableRef.alias() : tableRef.table();
                            if (!tRef.equalsIgnoreCase(alias) && !tRef.equalsIgnoreCase(tableRef.table())) {
                                remainingPredicates.add(pred);
                                continue;
                            }
                        }
                        value = extractLiteralValue(bin.left());
                    }
                }
            }
            if (colName != null && value != null && table.getColumnIndex(colName) >= 0) {
                equalityMap.put(colName.toLowerCase(), value);
            } else {
                remainingPredicates.add(pred);
            }
        }

        if (equalityMap.isEmpty()) return null;

        // Find a matching index
        for (Map.Entry<String, TableIndex> entry : table.getIndexes().entrySet()) {
            TableIndex idx = entry.getValue();
            int[] colIndices = idx.getColumnIndices();
            // Check if all index columns have equality predicates
            boolean allMatch = true;
            Object[] keyValues = new Object[colIndices.length];
            for (int i = 0; i < colIndices.length; i++) {
                Column col = table.getColumns().get(colIndices[i]);
                Object val = equalityMap.get(col.getName().toLowerCase());
                if (val == null && !equalityMap.containsKey(col.getName().toLowerCase())) {
                    allMatch = false;
                    break;
                }
                keyValues[i] = val;
            }
            if (!allMatch) continue;

            // Found a matching index — do the lookup
            List<Object[]> matchedRows = idx.findAll(keyValues);

            String alias = tableRef.alias() != null ? tableRef.alias() : tableRef.table();
            lastResolvedRightTable = table;
            lastResolvedRightAlias = alias;
            boolean hasVirtual = executor.dmlExecutor.hasVirtualColumns(table);
            List<RowContext> contexts = new ArrayList<>();
            for (Object[] row : matchedRows) {
                Object[] r = hasVirtual ? executor.dmlExecutor.computeVirtualColumns(table, row) : row;
                contexts.add(new RowContext(table, alias, r));
            }

            // Apply remaining WHERE predicates that weren't covered by the index
            if (!remainingPredicates.isEmpty()) {
                contexts = contexts.stream()
                        .filter(ctx -> {
                            for (Expression rp : remainingPredicates) {
                                if (!executor.isTruthy(executor.evalExpr(rp, ctx))) return false;
                            }
                            return true;
                        })
                        .collect(java.util.stream.Collectors.toList());
            }

            // MVCC visibility
            String schemaTableKey = schemaName + "." + tableRef.table();
            Session currentSession = executor.session;
            if (currentSession != null) {
                contexts = applyMvccVisibility(contexts, table, alias, schemaTableKey, currentSession);
            }
            // RLS
            if (table.isRlsEnabled() && !table.getRlsPolicies().isEmpty()) {
                contexts = applyRlsFiltering(contexts, table);
            }
            return contexts;
        }
        return null; // no matching index
    }

    private boolean isLiteralOrParam(Expression expr) {
        return expr instanceof Literal || expr instanceof ParamRef;
    }

    private Object extractLiteralValue(Expression expr) {
        if (expr instanceof Literal) {
            // Evaluate literal to get typed value (Integer, BigDecimal, String, Boolean, etc.)
            try {
                return executor.evalExpr(expr, null);
            } catch (Exception e) {
                return null;
            }
        }
        if (expr instanceof ParamRef) {
            // Parameters need runtime resolution, can't extract statically
            return null;
        }
        return null;
    }

    private List<RowContext> applyRlsFiltering(List<RowContext> contexts, Table table) {
        String currentRole = null;
        if (executor.session != null) {
            currentRole = executor.session.getGucSettings().get("role");
        }
        boolean isSuperuser = currentRole == null
                || "test".equalsIgnoreCase(currentRole)
                || "postgres".equalsIgnoreCase(currentRole)
                || "memgres".equalsIgnoreCase(currentRole);
        if (!isSuperuser || table.isRlsForced()) {
            String effectiveRole = currentRole != null ? currentRole : "test";
            List<RlsPolicy> selectPolicies = new ArrayList<>();
            for (RlsPolicy p : table.getRlsPolicies()) {
                if (p.appliesTo("SELECT") && p.getUsingExpr() != null
                        && p.appliesToRole(effectiveRole)) {
                    selectPolicies.add(p);
                }
            }
            if (selectPolicies.isEmpty()) {
                return new ArrayList<>();
            }
            List<RowContext> filtered = new ArrayList<>();
            for (RowContext ctx : contexts) {
                boolean passes = false;
                for (RlsPolicy policy : selectPolicies) {
                    try {
                        Object result = executor.evalExpr(policy.getUsingExpr(), ctx);
                        if (Boolean.TRUE.equals(result)) {
                            passes = true;
                            break;
                        }
                    } catch (Exception e) {
                        // Expression evaluation failed; row does not pass
                    }
                }
                if (passes) {
                    filtered.add(ctx);
                }
            }
            return filtered;
        }
        return contexts;
    }

    private List<RowContext> resolveSubquery(SelectStmt.SubqueryFrom subqFrom) {
        QueryResult subResult;
        if (subqFrom.subquery() instanceof SelectStmt) {
            SelectStmt sel = (SelectStmt) subqFrom.subquery();
            subResult = executor.executeSelect(sel);
        } else {
            subResult = executor.executeStatement(subqFrom.subquery());
        }
        String alias = subqFrom.alias() != null ? subqFrom.alias() : "subquery";
        List<Column> columns = FromFunctionResolver.applyColumnAliases(
                new ArrayList<>(subResult.getColumns()), subqFrom.columnAliases());
        Table virtualTable = new Table(alias, columns);
        for (Object[] row : subResult.getRows()) {
            virtualTable.insertRow(row);
        }
        List<RowContext> contexts = new ArrayList<>();
        for (Object[] row : virtualTable.getRows()) {
            contexts.add(new RowContext(virtualTable, alias, row));
        }
        return contexts;
    }

    // ---- Cross-product ----

    private List<RowContext> crossProductContexts(List<List<RowContext>> items) {
        List<RowContext> result = new ArrayList<>();
        if (items.isEmpty()) return result;
        if (items.size() == 1) return items.get(0);

        result = new ArrayList<>(items.get(0));
        for (int i = 1; i < items.size(); i++) {
            List<RowContext> right = items.get(i);
            List<RowContext> newResult = new ArrayList<>();
            for (RowContext leftCtx : result) {
                for (RowContext rightCtx : right) {
                    newResult.add(leftCtx.merge(rightCtx));
                }
            }
            result = newResult;
        }
        return result;
    }

    /**
     * Cross-product FROM items with early WHERE predicate application.
     */
    private List<RowContext> crossProductWithEarlyFilter(List<SelectStmt.FromItem> fromItems, Expression where) {
        List<Expression> predicates = flattenAndPredicates(where);
        Set<Integer> appliedPredicates = new HashSet<>();

        List<RowContext> accumulated = null;
        for (SelectStmt.FromItem fromItem : fromItems) {
            List<RowContext> resolved = resolveFromItem(fromItem);
            if (accumulated == null) {
                accumulated = resolved;
            } else {
                List<Expression> applicablePredicates = new ArrayList<>();
                if (!accumulated.isEmpty() && !resolved.isEmpty()) {
                    RowContext sample = accumulated.get(0).merge(resolved.get(0));
                    for (int pi = 0; pi < predicates.size(); pi++) {
                        if (!appliedPredicates.contains(pi) && canEvaluatePredicate(predicates.get(pi), sample)) {
                            applicablePredicates.add(predicates.get(pi));
                            appliedPredicates.add(pi);
                        }
                    }
                }

                List<RowContext> newAccumulated = new ArrayList<>();
                for (RowContext leftCtx : accumulated) {
                    for (RowContext rightCtx : resolved) {
                        RowContext merged = leftCtx.merge(rightCtx);
                        if (passesEarlyPredicates(merged, applicablePredicates)) {
                            newAccumulated.add(merged);
                        }
                    }
                }
                accumulated = newAccumulated;
            }
        }
        return accumulated != null ? accumulated : Cols.listOf();
    }

    // ---- WHERE predicate pushdown helpers ----

    private List<Expression> flattenAndPredicates(Expression expr) {
        List<Expression> predicates = new ArrayList<>();
        flattenAndPredicatesHelper(expr, predicates);
        return predicates;
    }

    private void flattenAndPredicatesHelper(Expression expr, List<Expression> result) {
        if (expr instanceof BinaryExpr && ((BinaryExpr) expr).op() == BinaryExpr.BinOp.AND) {
            BinaryExpr bin = (BinaryExpr) expr;
            flattenAndPredicatesHelper(bin.left(), result);
            flattenAndPredicatesHelper(bin.right(), result);
        } else {
            result.add(expr);
        }
    }

    private boolean canEvaluatePredicate(Expression pred, RowContext ctx) {
        try {
            collectColumnRefs(pred, ctx);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void collectColumnRefs(Expression expr, RowContext ctx) {
        if (expr instanceof ColumnRef) {
            ColumnRef cr = (ColumnRef) expr;
            if (ctx.resolveColumnDef(cr.table(), cr.column()) == null) {
                throw new RuntimeException("unresolvable");
            }
        } else if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            collectColumnRefs(bin.left(), ctx);
            collectColumnRefs(bin.right(), ctx);
        } else if (expr instanceof CustomOperatorExpr) {
            CustomOperatorExpr cop = (CustomOperatorExpr) expr;
            if (cop.left() != null) collectColumnRefs(cop.left(), ctx);
            collectColumnRefs(cop.right(), ctx);
        } else if (expr instanceof UnaryExpr) {
            UnaryExpr ue = (UnaryExpr) expr;
            collectColumnRefs(ue.operand(), ctx);
        } else if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fce = (FunctionCallExpr) expr;
            if (fce.args() != null) {
                for (Expression arg : fce.args()) {
                    collectColumnRefs(arg, ctx);
                }
            }
        } else if (expr instanceof CastExpr) {
            CastExpr ce = (CastExpr) expr;
            collectColumnRefs(ce.expr(), ctx);
        } else if (expr instanceof IsNullExpr) {
            IsNullExpr ine = (IsNullExpr) expr;
            collectColumnRefs(ine.expr(), ctx);
        } else if (expr instanceof InExpr) {
            InExpr ie = (InExpr) expr;
            collectColumnRefs(ie.expr(), ctx);
            if (ie.values() != null) {
                for (Expression v : ie.values()) collectColumnRefs(v, ctx);
            }
        } else if (expr instanceof CaseExpr) {
            CaseExpr caseExpr = (CaseExpr) expr;
            if (caseExpr.operand() != null) collectColumnRefs(caseExpr.operand(), ctx);
            if (caseExpr.whenClauses() != null) {
                for (CaseExpr.WhenClause wc : caseExpr.whenClauses()) {
                    collectColumnRefs(wc.condition(), ctx);
                    collectColumnRefs(wc.result(), ctx);
                }
            }
            if (caseExpr.elseExpr() != null) collectColumnRefs(caseExpr.elseExpr(), ctx);
        }
        // Subqueries, ExistsExpr, Literals, and other types: skip or always resolve
    }

    private boolean passesEarlyPredicates(RowContext ctx, List<Expression> predicates) {
        if (predicates.isEmpty()) return true;
        for (Expression pred : predicates) {
            try {
                Object result = executor.evalExpr(pred, ctx);
                if (!executor.isTruthy(result)) return false;
            } catch (Exception e) {
                // If evaluation fails, don't filter the row (conservative)
            }
        }
        return true;
    }

    // ---- MVCC Visibility ----

    private List<RowContext> applyMvccVisibility(List<RowContext> contexts, Table table, String alias,
                                                  String schemaTableKey, Session currentSession) {
        Database db = executor.database;

        Set<Object[]> otherUncommittedInserts = Collections.newSetFromMap(new IdentityHashMap<>());
        Map<Object[], Object[]> otherUncommittedUpdates = new IdentityHashMap<>();
        List<Object[]> otherUncommittedDeletes = new ArrayList<>();

        for (Session otherSession : db.getActiveSessions()) {
            if (otherSession == currentSession) continue;
            if (!otherSession.isInTransaction()) continue;

            Set<Object[]> inserts = otherSession.getUncommittedInserts(schemaTableKey);
            otherUncommittedInserts.addAll(inserts);

            Map<Object[], Object[]> updates = otherSession.getUncommittedUpdates(schemaTableKey);
            otherUncommittedUpdates.putAll(updates);

            List<Object[]> deletes = otherSession.getUncommittedDeletes(schemaTableKey);
            otherUncommittedDeletes.addAll(deletes);
        }

        if (otherUncommittedInserts.isEmpty() && otherUncommittedUpdates.isEmpty() && otherUncommittedDeletes.isEmpty()) {
            if (currentSession.isInTransaction()) {
                String isolation = currentSession.getEffectiveIsolationLevel();
                if ("repeatable read".equals(isolation) || "serializable".equals(isolation)) {
                    List<Object[]> visibleRows = new ArrayList<>();
                    for (RowContext ctx : contexts) {
                        visibleRows.add(getFirstRow(ctx));
                    }
                    List<Object[]> snapshot = currentSession.getOrCreateRRSnapshot(schemaTableKey, visibleRows);
                    if (snapshot != null) {
                        List<RowContext> snapshotContexts = new ArrayList<>();
                        for (Object[] row : snapshot) {
                            snapshotContexts.add(new RowContext(table, alias, row));
                        }
                        return snapshotContexts;
                    }
                }
            }
            return contexts;
        }

        List<RowContext> filtered = new ArrayList<>();
        for (RowContext ctx : contexts) {
            Object[] row = getFirstRow(ctx);

            if (otherUncommittedInserts.contains(row)) {
                continue;
            }

            Object[] oldValues = otherUncommittedUpdates.get(row);
            if (oldValues != null) {
                filtered.add(new RowContext(table, alias, oldValues));
                continue;
            }

            filtered.add(ctx);
        }

        for (Object[] deletedRow : otherUncommittedDeletes) {
            if (!otherUncommittedInserts.contains(deletedRow)) {
                filtered.add(new RowContext(table, alias, deletedRow));
            }
        }

        if (currentSession.isInTransaction()) {
            String isolation = currentSession.getEffectiveIsolationLevel();
            if ("repeatable read".equals(isolation) || "serializable".equals(isolation)) {
                List<Object[]> visibleRows = new ArrayList<>();
                for (RowContext ctx : filtered) {
                    visibleRows.add(getFirstRow(ctx));
                }
                List<Object[]> snapshot = currentSession.getOrCreateRRSnapshot(schemaTableKey, visibleRows);
                if (snapshot != null) {
                    List<RowContext> snapshotContexts = new ArrayList<>();
                    for (Object[] row : snapshot) {
                        snapshotContexts.add(new RowContext(table, alias, row));
                    }
                    return snapshotContexts;
                }
            }
        }

        return filtered;
    }

    private static Object[] getFirstRow(RowContext ctx) {
        List<RowContext.TableBinding> bindings = ctx.getBindings();
        return bindings.isEmpty() ? null : bindings.get(0).row();
    }
}
