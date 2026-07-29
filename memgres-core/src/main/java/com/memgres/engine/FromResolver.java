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
    /**
     * The WHERE of the query whose FROM is being resolved. A full join reads it: a WHERE that
     * discards the null-padded rows makes the join an inner one, which lifts the restriction on
     * what its condition may be. See {@link FullJoinAdmissibility}.
     */
    Expression enclosingWhere;
    /** The HAVING of that query; the clauses of it with no aggregate are WHERE clauses too. */
    Expression enclosingHaving;
    /** The ON conditions of the joins above the one being resolved that filter its rows. */
    final List<Expression> joinQualsAbove = new ArrayList<>();
    /** The query whose FROM clause is being resolved, or null when it is not a plain SELECT. */
    Statement currentQuery;
    /**
     * The statement the client sent, when that statement is a SELECT. Everything else — a
     * subquery, a WITH query, a view body, an arm of a set operation, the source of a writing
     * statement, a function body — is a query PostgreSQL may still rescue a full join in from
     * above, so it is never the one whose joins are judged. See {@link FullJoinAdmissibility}.
     */
    Statement outermostQuery;

    final FullJoinAdmissibility fullJoinCheck;

    FromResolver(AstExecutor executor) {
        this.executor = executor;
        this.functionResolver = new FromFunctionResolver(executor);
        this.joinExecutor = new FromJoinExecutor(this);
        this.fullJoinCheck = new FullJoinAdmissibility(this);
    }

    /** Whether the FROM clause being resolved is the one the client's own statement wrote. */
    boolean judgingOutermostQuery() {
        return outermostQuery != null && currentQuery == outermostQuery;
    }

    // ---- Table Bindings (column structure without data) ----

    List<RowContext.TableBinding> resolveTableBindings(List<SelectStmt.FromItem> fromItems) {
        List<RowContext.TableBinding> bindings = new ArrayList<>();
        for (SelectStmt.FromItem item : fromItems) {
            resolveTableBindingsFromItem(item, bindings);
        }
        return bindings;
    }

    /**
     * The names and columns a FROM item exposes, with every value null.
     *
     * <p>An outer join has to name-pad the side that contributed nothing: the whole point of
     * {@code t1 RIGHT JOIN t2} is to answer with NULLs where {@code t1} has no match, and those
     * rows must still answer to {@code t1}'s name. Taking the shape from the first row of the
     * side only works while the side has a row; when it is empty — because the relation is empty,
     * because a subquery filtered everything away, or because the condition is never true — the
     * shape has to come from the FROM item itself.
     */
    List<RowContext.TableBinding> resolveItemShape(SelectStmt.FromItem item) {
        List<RowContext.TableBinding> bindings = new ArrayList<>();
        try {
            resolveTableBindingsFromItem(item, bindings);
        } catch (RuntimeException e) {
            // The shape is a convenience, not the answer: a FROM item that cannot be described
            // (an unreadable lateral, say) leaves the padding as it was before.
            return Cols.listOf();
        }
        return bindings;
    }

    /**
     * The columns a FROM item exposes, in order, when it produced no rows to read them from.
     *
     * <p>Every column of every relation, except where a USING or NATURAL join merged two of them
     * into one — which also decides where in the list they sit. A join whose clause cannot be
     * satisfied is described as though it had none: a query that answers with no rows still has
     * to say what its columns are, and it is the executed join, not this, that refuses it.
     *
     * @param bindings the same item's bindings, which the returned columns index into
     */
    List<RowContext.OutCol> resolveItemOutput(SelectStmt.FromItem item,
                                              List<RowContext.TableBinding> bindings) {
        try {
            Described described = describe(item);
            return described.bindings.size() == bindings.size() ? described.output
                    : RowContext.defaultOutput(bindings);
        } catch (RuntimeException e) {
            return RowContext.defaultOutput(bindings);
        }
    }

    /** The columns a whole FROM clause exposes, its items placed side by side. */
    List<RowContext.OutCol> resolveClauseOutput(List<SelectStmt.FromItem> fromItems,
                                                List<RowContext.TableBinding> bindings) {
        if (fromItems == null || fromItems.isEmpty()) return RowContext.defaultOutput(bindings);
        List<RowContext.OutCol> out = new ArrayList<>();
        int offset = 0;
        try {
            for (SelectStmt.FromItem item : fromItems) {
                Described described = describe(item);
                for (RowContext.OutCol oc : described.output) out.add(oc.shift(offset));
                offset += described.bindings.size();
            }
        } catch (RuntimeException e) {
            return RowContext.defaultOutput(bindings);
        }
        return offset == bindings.size() ? out : RowContext.defaultOutput(bindings);
    }

    /** A FROM item's relations and the columns it exposes over them, described together. */
    private static final class Described {
        final List<RowContext.TableBinding> bindings;
        final List<RowContext.OutCol> output;

        Described(List<RowContext.TableBinding> bindings, List<RowContext.OutCol> output) {
            this.bindings = bindings;
            this.output = output;
        }
    }

    private Described describe(SelectStmt.FromItem item) {
        if (!(item instanceof SelectStmt.JoinFrom)) {
            List<RowContext.TableBinding> bindings = new ArrayList<>();
            resolveTableBindingsFromItem(item, bindings);
            return new Described(bindings, RowContext.defaultOutput(bindings));
        }
        SelectStmt.JoinFrom jf = (SelectStmt.JoinFrom) item;
        Described left = describe(jf.left());
        Described right;
        // A LATERAL item reads the names to its left, so describing it needs those names in
        // scope even when they carry no row, exactly as resolveTableBindingsFromItem has it.
        if (jf.right() instanceof SelectStmt.SubqueryFrom
                && ((SelectStmt.SubqueryFrom) jf.right()).lateral()) {
            executor.outerContextStack.push(new RowContext(new ArrayList<>(left.bindings)));
            try {
                right = describe(jf.right());
            } finally {
                executor.outerContextStack.pop();
            }
        } else {
            right = describe(jf.right());
        }
        List<RowContext.TableBinding> bindings = new ArrayList<>(left.bindings);
        bindings.addAll(right.bindings);
        List<String> using = jf.using();
        if (FromJoinExecutor.isNatural(jf.joinType())) {
            using = FromJoinExecutor.naturalNames(left.output, right.output);
        }
        return new Described(bindings, FromJoinExecutor.shapeOfJoin(
                left.output, left.bindings.size(), right.output, using).output);
    }

    private void resolveTableBindingsFromItem(SelectStmt.FromItem item, List<RowContext.TableBinding> bindings) {
        if (item instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef tableRef = (SelectStmt.TableRef) item;
            String schemaName = tableRef.schema() != null ? tableRef.schema() : executor.defaultSchema();
            String alias = tableRef.alias() != null ? tableRef.alias() : tableRef.table();
            // Check CTEs first
            SelectStmt.CommonTableExpr cte = lookupCteFor(tableRef);
            if (cte != null) {
                QueryResult cteResult = executor.selectExecutor.executeCte(cte);
                Table virtualTable = new Table(alias, cteResult.getColumns());
                bindings.add(new RowContext.TableBinding(virtualTable, alias, new Object[virtualTable.getColumns().size()]));
                return;
            }
            // Check views
            Database.ViewDef view = executor.database.getView(tableRef.table());
            if (view != null) {
                // Materialized views know their columns without re-running the query
                // (and an unpopulated matview must still be describable).
                if (view.materialized() && view.cachedColumns() != null && !view.cachedColumns().isEmpty()) {
                    List<Column> mvCols = view.cachedColumns();
                    bindings.add(new RowContext.TableBinding(
                            new Table(alias, mvCols), alias, new Object[mvCols.size()]));
                    return;
                }
                try {
                    QueryResult vr = executor.executeViewQuery(tableRef.table(), view.query());
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
                Table catalogTable = executor.systemCatalog.resolve(tableRef.schema(), tableRef.table(), executor.session);
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
            // A LATERAL item reads the names to its left, so describing it needs those names in
            // scope even when they carry no row — otherwise the describe fails and the lateral
            // alias goes missing from a query that answers with no rows at all.
            boolean lateralRight = joinFrom.right() instanceof SelectStmt.SubqueryFrom
                    && ((SelectStmt.SubqueryFrom) joinFrom.right()).lateral();
            if (lateralRight) {
                executor.outerContextStack.push(new RowContext(new ArrayList<>(bindings)));
                try {
                    resolveTableBindingsFromItem(joinFrom.right(), bindings);
                } finally {
                    executor.outerContextStack.pop();
                }
            } else {
                resolveTableBindingsFromItem(joinFrom.right(), bindings);
            }
            // A USING or NATURAL join still names both relations and both keep all their columns;
            // what changes is which of them the join exposes, and that is the output shape's
            // business (see resolveItemOutput), not the bindings'.
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
            // XMLTABLE: extract column definitions from encoded args
            if (funcFrom.functionName().equals("__xmltable__")) {
                List<Column> cols = new ArrayList<>();
                for (int i = 2; i < funcFrom.args().size(); i++) {
                    Expression arg = funcFrom.args().get(i);
                    String def = arg instanceof Literal ? ((Literal) arg).value() : arg.toString();
                    String[] parts = def.split(":", 3);
                    DataType dt = parts.length > 1 ? DataType.fromPgName(parts[1]) : null;
                    cols.add(new Column(parts[0], dt != null ? dt : DataType.TEXT, true, false, null));
                }
                Table virtualTable = new Table(alias, cols);
                bindings.add(new RowContext.TableBinding(virtualTable, alias, new Object[cols.size()]));
            }
            // JSON_TABLE: extract column definitions from the JsonTableExpr
            else if (funcFrom.functionName().equals("__json_table__") && !funcFrom.args().isEmpty()
                    && funcFrom.args().get(0) instanceof JsonTableExpr) {
                JsonTableExpr jt = (JsonTableExpr) funcFrom.args().get(0);
                List<Column> cols = new ArrayList<>();
                collectJsonTableColumnDefs(jt.columns, cols);
                Table virtualTable = new Table(alias, cols);
                bindings.add(new RowContext.TableBinding(virtualTable, alias, new Object[cols.size()]));
            } else {
                // The shape describes an item that produced no row, so the type has to come from
                // the call rather than from a value; text is only the fallback.
                DataType known = functionResolver.singleColumnType(funcFrom);
                DataType colType = known != null ? known : DataType.TEXT;
                List<String> ca = funcFrom.columnAliases();
                List<Column> cols = new ArrayList<>();
                if (ca != null && !ca.isEmpty()) {
                    for (String colName : ca) {
                        cols.add(new Column(FromFunctionResolver.stripColType(colName), colType, true, false, null));
                    }
                } else {
                    cols.add(new Column(alias, colType, true, false, null));
                }
                // WITH ORDINALITY's column is a bigint whether or not the item produced a row to
                // read the type off, and it is the last one however the aliases named them.
                if (funcFrom.withOrdinality()) {
                    if (ca == null || ca.size() < 2) {
                        cols.add(new Column("ordinality", DataType.BIGINT, true, false, null));
                    } else {
                        Column last = cols.get(cols.size() - 1);
                        cols.set(cols.size() - 1,
                                new Column(last.getName(), DataType.BIGINT, true, false, null));
                    }
                }
                Table virtualTable = new Table(alias, cols);
                bindings.add(new RowContext.TableBinding(virtualTable, alias, new Object[cols.size()]));
            }
        }
    }

    /** Recursively collect leaf column definitions from JSON_TABLE columns. */
    private void collectJsonTableColumnDefs(List<JsonTableExpr.JsonTableColumn> columns, List<Column> cols) {
        for (JsonTableExpr.JsonTableColumn col : columns) {
            if (col.nestedColumns != null) {
                collectJsonTableColumnDefs(col.nestedColumns, cols);
            } else {
                cols.add(new Column(col.name, col.forOrdinality ? DataType.INTEGER : DataType.TEXT, true, false, null));
            }
        }
    }

    // ---- FROM Clause Resolution ----

    /**
     * Resolve a SELECT's FROM clause, with its WHERE pushed down for early filtering during the
     * cross-product, and recording which query the clause belongs to. A full join is judged only
     * in the query the client sent, and a join below the ones this resolves belongs to whatever
     * subquery, WITH query or view it was written in.
     */
    List<RowContext> resolveFromClause(List<SelectStmt.FromItem> fromItems, Expression where,
                                       Expression having, Statement owner) {
        Expression priorWhere = enclosingWhere;
        Expression priorHaving = enclosingHaving;
        Statement priorQuery = currentQuery;
        List<Expression> priorJoinQuals = new ArrayList<>(joinQualsAbove);
        enclosingWhere = where;
        enclosingHaving = having;
        currentQuery = owner;
        joinQualsAbove.clear();
        try {
            List<RowContext> resolved = resolveFromClauseInner(fromItems, where);
            stampCoveredNames(fromItems, resolved);
            return resolved;
        } finally {
            enclosingWhere = priorWhere;
            enclosingHaving = priorHaving;
            currentQuery = priorQuery;
            joinQualsAbove.clear();
            joinQualsAbove.addAll(priorJoinQuals);
        }
    }

    /**
     * Tells each row which relations the FROM clause holds but does not answer to, so that a
     * qualifier naming one of them is reported as out of reach rather than as missing.
     *
     * <p>{@code (a JOIN b) AS j} exposes {@code j} and nothing else — {@code a} is written in the
     * query and cannot be referenced, which is a different mistake from writing a relation that is
     * not there at all, and PostgreSQL words the two differently.
     */
    private static void stampCoveredNames(List<SelectStmt.FromItem> fromItems,
                                          List<RowContext> resolved) {
        if (fromItems == null || resolved == null || resolved.isEmpty()) return;
        Set<String> covered = new LinkedHashSet<>();
        for (SelectStmt.FromItem item : fromItems) collectCoveredNames(item, covered);
        if (covered.isEmpty()) return;
        for (RowContext ctx : resolved) {
            for (RowContext.TableBinding b : ctx.getBindings()) {
                covered.remove(b.alias() != null ? b.alias().toLowerCase()
                        : b.table().getName().toLowerCase());
            }
            break;
        }
        if (covered.isEmpty()) return;
        for (RowContext ctx : resolved) ctx.setCoveredNames(covered);
    }

    /** Every relation name written anywhere in a FROM tree, however it was later renamed. */
    static void collectCoveredNames(SelectStmt.FromItem item, Set<String> out) {
        if (item instanceof SelectStmt.JoinFrom) {
            collectCoveredNames(((SelectStmt.JoinFrom) item).left(), out);
            collectCoveredNames(((SelectStmt.JoinFrom) item).right(), out);
            return;
        }
        if (item instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef ref = (SelectStmt.TableRef) item;
            out.add(ref.alias() != null ? ref.alias().toLowerCase() : ref.table().toLowerCase());
            return;
        }
        // A parenthesized join given an alias is carried as a sub-query over the join itself; the
        // relations under it are written in the query and covered by that alias.
        if (item instanceof SelectStmt.SubqueryFrom
                && ((SelectStmt.SubqueryFrom) item).subquery() instanceof SelectStmt) {
            SelectStmt inner = (SelectStmt) ((SelectStmt.SubqueryFrom) item).subquery();
            if (inner.from() != null) {
                for (SelectStmt.FromItem f : inner.from()) collectCoveredNames(f, out);
            }
        }
    }

    /**
     * A FROM item that is not LATERAL cannot read the item beside it.
     *
     * <p>The relations of one FROM clause are computed side by side, so a sub-select written as one
     * of them has no row of its neighbour to read — LATERAL is the word that says to compute it
     * once per such row instead. PostgreSQL says the entry is there but out of reach and names the
     * word that would bring it into reach; reporting the entry as missing sent the reader looking
     * for a relation they had written.
     *
     * <p>Only a name a sibling actually exposes is reported. A name nothing in the query has is
     * missing, and PostgreSQL says so.
     */
    private void rejectSiblingReferenceWithoutLateral(List<SelectStmt.FromItem> fromItems) {
        if (fromItems == null || fromItems.size() < 2) return;
        for (int i = 0; i < fromItems.size(); i++) {
            SelectStmt.FromItem item = fromItems.get(i);
            if (!(item instanceof SelectStmt.SubqueryFrom)) continue;
            SelectStmt.SubqueryFrom sub = (SelectStmt.SubqueryFrom) item;
            if (sub.lateral()) continue;
            Set<String> siblings = new LinkedHashSet<>();
            for (int j = 0; j < fromItems.size(); j++) {
                if (j != i) collectExposedNames(fromItems.get(j), siblings);
            }
            if (siblings.isEmpty()) continue;
            String referenced = SelectExecutor.firstReferenceTo(sub.subquery(), siblings);
            if (referenced == null) continue;
            MemgresException e = new MemgresException(
                    "invalid reference to FROM-clause entry for table \"" + referenced + "\"", "42P01");
            e.setDetail("There is an entry for table \"" + referenced
                    + "\", but it cannot be referenced from this part of the query.");
            e.setHint("To reference that table, you must mark this subquery with LATERAL.");
            throw e;
        }
    }

    /** Every name a FROM item answers to, at any depth of a join below it. */
    static void collectExposedNames(SelectStmt.FromItem item, Set<String> out) {
        if (item instanceof SelectStmt.JoinFrom) {
            collectExposedNames(((SelectStmt.JoinFrom) item).left(), out);
            collectExposedNames(((SelectStmt.JoinFrom) item).right(), out);
            return;
        }
        String name = SelectExecutor.exposedNameOf(item);
        if (name != null) out.add(name);
    }

    /**
     * Resolve the FROM of an UPDATE or the USING of a DELETE. The statement's WHERE is not pushed
     * into the scan: it also names the table being written, which is not one of these relations.
     */
    List<RowContext> resolveWrittenFromClause(List<SelectStmt.FromItem> fromItems) {
        return resolveFromClauseInner(fromItems, null);
    }

    private List<RowContext> resolveFromClauseInner(List<SelectStmt.FromItem> fromItems, Expression where) {
        rejectSiblingReferenceWithoutLateral(fromItems);
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
            } else if (isFunctionFrom && accumulated != null) {
                // Implicit LATERAL for functions-in-FROM. Note this branch must also own
                // the zero-left-rows case: with no rows to iterate the function is never
                // evaluated (PG semantics). Falling through to the generic branch would
                // evaluate its arguments without any row context, so a column-ref arg
                // like pg_get_sequence_data(seqrelid) degrades to a bare string and
                // crashes — pg_dump hits exactly that on a database with no sequences.
                SelectStmt.FunctionFrom funcFrom = (SelectStmt.FunctionFrom) fromItem;
                List<RowContext> newAccumulated = new ArrayList<>();
                for (RowContext leftCtx : accumulated) {
                    executor.outerContextStack.push(leftCtx);
                    try {
                        // A comma between two FROM items is an inner join, so a function that
                        // produces no rows for this left row removes it -- the same as the
                        // lateral-subquery branch above, which skips it. Padding it with NULLs
                        // instead answered LEFT JOIN LATERAL to a query that did not write one.
                        for (RowContext rightCtx : functionResolver.resolveFunctionFrom(funcFrom)) {
                            newAccumulated.add(joinExecutor.mergeContexts(leftCtx, rightCtx));
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

    /**
     * The WITH item a FROM reference names, or null.
     *
     * <p>A WITH item lives in no schema, so a schema-qualified name can never reach one: it is
     * always the stored relation of that name, and "does not exist" when there is none.
     */
    private SelectStmt.CommonTableExpr lookupCteFor(SelectStmt.TableRef tableRef) {
        if (tableRef.schema() != null) return null;
        return executor.selectExecutor.lookupCte(tableRef.table());
    }

    /**
     * The columns a FROM item's alias list renames, as far as the list reaches.
     *
     * <p>{@code t AS z(m)} calls the relation's first column m and leaves the rest alone; naming
     * more columns than the relation has is an error PostgreSQL raises before reading a row.
     */
    private static List<Column> renameColumns(String alias, List<Column> columns,
                                              List<String> aliases) {
        if (aliases == null || aliases.isEmpty()) return columns;
        if (aliases.size() > columns.size()) {
            throw new MemgresException("table \"" + alias + "\" has " + columns.size()
                    + " columns available but " + aliases.size() + " columns specified", "42P10");
        }
        List<Column> renamed = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            Column c = columns.get(i);
            renamed.add(i < aliases.size()
                    ? new Column(aliases.get(i), c.getType(), c.isNullable(), c.isPrimaryKey(),
                            c.getDefaultValue(), c.getEnumTypeName(), c.getPrecision(), c.getScale(),
                            null, c.getDomainTypeName(), c.getCompositeTypeName(),
                            c.getArrayElementType())
                    : c);
        }
        return renamed;
    }

    private List<RowContext> resolveTableRef(SelectStmt.TableRef tableRef) {
        // Check CTEs first
        SelectStmt.CommonTableExpr cte = lookupCteFor(tableRef);
        if (cte != null) {
            String alias = tableRef.alias() != null ? tableRef.alias() : tableRef.table();
            QueryResult cteResult = executor.selectExecutor.executeCte(cte);
            Table virtualTable = new Table(alias,
                    renameColumns(alias, cteResult.getColumns(), tableRef.columnAliases()));
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
            if (view.materialized() && !view.populated()) {
                MemgresException ex = new MemgresException(
                        "materialized view \"" + view.name() + "\" has not been populated", "55000");
                ex.setHint("Use the REFRESH MATERIALIZED VIEW command.");
                throw ex;
            }
            String alias = tableRef.alias() != null ? tableRef.alias() : tableRef.table();
            List<Column> cols;
            List<Object[]> rows;
            if (view.materialized() && view.cachedColumns() != null) {
                cols = view.cachedColumns();
                rows = view.cachedRows();
            } else {
                // The reader needs SELECT on the view; the body then runs with the view
                // owner's rights, as PG does.
                String viewSchema = tableRef.schema() != null ? tableRef.schema() : executor.defaultSchema();
                executor.checkTablePrivilege("SELECT", viewSchema, tableRef.table());
                String priorViewOwner = executor.viewOwnerRole;
                String viewOwner = executor.database.getObjectOwner(
                        "view:" + viewSchema.toLowerCase() + "." + tableRef.table().toLowerCase());
                if (viewOwner != null) executor.viewOwnerRole = viewOwner;
                QueryResult viewResult;
                try {
                    viewResult = executor.executeViewQuery(tableRef.table(), view.query());
                } finally {
                    executor.viewOwnerRole = priorViewOwner;
                }
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
        // H35: pass userQualified=true when user explicitly wrote schema.table
        boolean userQualified = tableRef.schema() != null;
        // H35: empty search_path with unqualified name → reject unless temp table exists
        // PG always implicitly includes pg_temp, so temp tables are still findable
        if (!userQualified && executor.session != null) {
            String sp = executor.session.getGucSettings().get("search_path");
            if (sp != null) {
                boolean hasEntries = false;
                for (String part : sp.split(",")) {
                    String s = part.trim().replace("\"", "").replace("'", "");
                    if (!s.isEmpty() && !s.equals("$user")) { hasEntries = true; break; }
                }
                if (!hasEntries) {
                    // Check if temp table exists before rejecting (pg_temp is always implicit)
                    String tempSchemaName = executor.session.getTempSchemaName();
                    Schema pgTemp = executor.database.getSchema(tempSchemaName);
                    if (pgTemp == null || pgTemp.getTable(tableRef.table()) == null) {
                        throw new MemgresException("relation \"" + tableRef.table() + "\" does not exist", "42P01");
                    }
                }
            }
        }
        boolean userTableExists = false;
        try { executor.resolveTable(schemaName, tableRef.table(), userQualified); userTableExists = true; } catch (MemgresException ignored) {}
        if (!userTableExists && SystemCatalog.isSystemCatalog(tableRef.schema(), tableRef.table())) {
            Table catalogTable = executor.systemCatalog.resolve(tableRef.schema(), tableRef.table(), executor.session);
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
        Table table = executor.resolveTable(schemaName, tableRef.table(), userQualified);
        // C6: Enforce SELECT privilege on user tables
        executor.checkTablePrivilege("SELECT", schemaName, tableRef.table());
        // A reader inside an explicit transaction holds ACCESS SHARE until it ends, which is
        // what makes a concurrent TRUNCATE or ALTER wait instead of yanking the table away.
        if (executor.session != null && executor.session.isInTransaction()) {
            executor.database.acquireTableLock(schemaName + "." + tableRef.table(),
                    "AccessShareLock", executor.session, false);
        }
        String alias = tableRef.alias() != null ? tableRef.alias() : tableRef.table();
        // An alias list renames the columns the query will see. The rows are the stored table's
        // own, so only the description in front of them is rebuilt.
        if (tableRef.columnAliases() != null && !tableRef.columnAliases().isEmpty()) {
            Table renamed = new Table(alias,
                    renameColumns(alias, table.getColumns(), tableRef.columnAliases()));
            for (Object[] row : table.getAllRows()) renamed.insertRow(row);
            table = renamed;
        }
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

        // Apply Row-Level Security filtering (default-deny: even with no policies, non-owner sees nothing)
        if (table.isRlsEnabled()) {
            contexts = applyRlsFiltering(contexts, table, schemaName);
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
                // Coerce the lookup value to match the column's storage type.
                // For CHAR(n) columns, the stored value is padded with spaces; the
                // literal must be padded too so the index hash lookup matches.
                keyValues[i] = TypeCoercion.coerceForStorage(val, col);
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
            if (table.isRlsEnabled()) {
                contexts = applyRlsFiltering(contexts, table, schemaName);
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

    private List<RowContext> applyRlsFiltering(List<RowContext> contexts, Table table, String schemaName) {
        if (executor.shouldBypassRls(table, schemaName)) return contexts;
        return filterByRlsUsing(contexts, table, "SELECT");
    }

    /** Filter rows by RLS USING policies for the given command. Shared by SELECT/UPDATE/DELETE. */
    List<RowContext> filterByRlsUsing(List<RowContext> contexts, Table table, String command) {
        String effectiveRole = executor.currentRole();
        List<RlsPolicy> permissivePolicies = new ArrayList<>();
        List<RlsPolicy> restrictivePolicies = new ArrayList<>();
        for (RlsPolicy p : table.getRlsPolicies()) {
            if (p.appliesTo(command) && p.getUsingExpr() != null
                    && p.appliesToRole(effectiveRole)) {
                if (p.isRestrictive()) {
                    restrictivePolicies.add(p);
                } else {
                    permissivePolicies.add(p);
                }
            }
        }
        // Default-deny: no applicable policies → 0 rows
        if (permissivePolicies.isEmpty() && restrictivePolicies.isEmpty()) {
            return new ArrayList<>();
        }
        List<RowContext> filtered = new ArrayList<>();
        for (RowContext ctx : contexts) {
            // PG semantics: row must pass ALL restrictive policies
            // AND at least ONE permissive policy (if any exist).
            boolean passesPermissive;
            if (permissivePolicies.isEmpty()) {
                passesPermissive = false;
            } else {
                passesPermissive = false;
                for (RlsPolicy policy : permissivePolicies) {
                    try {
                        Object result = executor.evalExpr(policy.getUsingExpr(), ctx);
                        if (Boolean.TRUE.equals(result)) {
                            passesPermissive = true;
                            break;
                        }
                    } catch (Exception e) {
                        // Expression evaluation failed; row does not pass this policy
                    }
                }
            }
            boolean passesRestrictive = true;
            for (RlsPolicy policy : restrictivePolicies) {
                try {
                    Object result = executor.evalExpr(policy.getUsingExpr(), ctx);
                    if (!Boolean.TRUE.equals(result)) {
                        passesRestrictive = false;
                        break;
                    }
                } catch (Exception e) {
                    passesRestrictive = false;
                    break;
                }
            }
            if (passesPermissive && passesRestrictive) {
                filtered.add(ctx);
            }
        }
        return filtered;
    }

    private List<RowContext> resolveSubquery(SelectStmt.SubqueryFrom subqFrom) {
        String alias = subqFrom.alias() != null ? subqFrom.alias() : "subquery";
        QueryResult subResult;
        if (subqFrom.subquery() instanceof SelectStmt) {
            SelectStmt sel = (SelectStmt) subqFrom.subquery();
            subResult = executor.executeSelect(sel);
        } else {
            subResult = executor.executeStatement(subqFrom.subquery());
        }
        List<Column> columns = FromFunctionResolver.applyColumnAliases(
                new ArrayList<>(subResult.getColumns()), subqFrom.columnAliases(), alias);
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

        // SSI: track that this serializable transaction read from this table
        currentSession.trackSsiRead(schemaTableKey);

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
                // If the row was updated before being deleted, the committed
                // state is the pre-update old values, not its current contents.
                Object[] oldValues = otherUncommittedUpdates.get(deletedRow);
                filtered.add(new RowContext(table, alias, oldValues != null ? oldValues : deletedRow));
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
