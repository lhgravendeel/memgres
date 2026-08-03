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
    final CrossProductPairer pairer;
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
        this.pairer = new CrossProductPairer(executor);
        this.fullJoinCheck = new FullJoinAdmissibility(this);
    }

    /** Whether the FROM clause being resolved is the one the client's own statement wrote. */
    boolean judgingOutermostQuery() {
        return outermostQuery != null && currentQuery == outermostQuery;
    }

    /**
     * The view a FROM item names. A qualified reference reaches the view in the schema it names
     * and nothing else: another schema's view of the same name is a different relation, and a
     * table in the named schema is what the reference means when that schema holds no such view.
     */
    Database.ViewDef viewFor(SelectStmt.TableRef tableRef) {
        if (tableRef.schema() != null) {
            return executor.database.getView(tableRef.schema(), tableRef.table());
        }
        return executor.database.getView(tableRef.table());
    }

    // ---- Table Bindings (column structure without data) ----

    List<RowContext.TableBinding> resolveTableBindings(List<SelectStmt.FromItem> fromItems) {
        List<RowContext.TableBinding> bindings = new ArrayList<>();
        for (SelectStmt.FromItem item : fromItems) {
            // A comma between two FROM items exposes the left one to the right the same way an
            // explicit join does, so a LATERAL written after a comma reads the names before it and
            // has to be described with them in scope. Without this the item could not be described
            // at all and the names it exposes went missing from the query level's scope.
            if (readsItemsToItsLeft(item) && !bindings.isEmpty()) {
                executor.outerContextStack.push(new RowContext(new ArrayList<>(bindings)));
                try {
                    resolveTableBindingsFromItem(item, bindings);
                } finally {
                    executor.outerContextStack.pop();
                }
            } else {
                resolveTableBindingsFromItem(item, bindings);
            }
        }
        return bindings;
    }

    /**
     * How many relations a FROM clause names, counting a join as the items on either side of it.
     *
     * <p>{@link #resolveTableBindings} adds exactly one binding per named relation and silently
     * adds none for one it cannot describe — a sequence, a WITH item still being defined, a
     * lateral it cannot read — so a count that comes up short is a description that is missing a
     * relation. Anything reading the bindings as <em>everything</em> this query level supplies has
     * to know that, or a column the missing relation holds looks like a column nothing holds.
     */
    static int relationCount(List<SelectStmt.FromItem> fromItems) {
        if (fromItems == null) return 0;
        int n = 0;
        for (SelectStmt.FromItem item : fromItems) n += relationCount(item);
        return n;
    }

    private static int relationCount(SelectStmt.FromItem item) {
        if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
            return relationCount(join.left()) + relationCount(join.right());
        }
        return 1;
    }

    /** A LATERAL sub-select, and a function in FROM, which is lateral whether or not it says so. */
    private static boolean readsItemsToItsLeft(SelectStmt.FromItem item) {
        if (item instanceof SelectStmt.FunctionFrom) return true;
        return item instanceof SelectStmt.SubqueryFrom
                && ((SelectStmt.SubqueryFrom) item).lateral();
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
            List<RowContext.TableBinding> soFar = new ArrayList<>();
            for (SelectStmt.FromItem item : fromItems) {
                Described described = describeReadingLeft(item, soFar);
                for (RowContext.OutCol oc : described.output) out.add(oc.shift(offset));
                offset += described.bindings.size();
                soFar.addAll(described.bindings);
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

    /** {@link #describe} with the FROM items written before this one in scope, for a LATERAL. */
    private Described describeReadingLeft(SelectStmt.FromItem item,
                                          List<RowContext.TableBinding> soFar) {
        if (!readsItemsToItsLeft(item) || soFar.isEmpty()) return describe(item);
        executor.outerContextStack.push(new RowContext(new ArrayList<>(soFar)));
        try {
            return describe(item);
        } finally {
            executor.outerContextStack.pop();
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
        if (readsItemsToItsLeft(jf.right())) {
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

    private static void addBinding(List<RowContext.TableBinding> bindings, Table table,
                                   String alias) {
        bindings.add(new RowContext.TableBinding(table, alias,
                new Object[table.getColumns().size()]));
    }

    /**
     * The relation as this FROM item exposes it.
     *
     * <p>An alias list renames the columns for this query level and nothing else about them: the
     * types behind the new names are the relation's own. A description that kept the stored names
     * would answer for names the query cannot write and refuse the ones it can, so every relation
     * a name can reach — a stored table, a WITH item, a view, a catalog — is renamed here.
     */
    private static Table asWritten(Table table, String alias, List<String> columnAliases) {
        if (columnAliases == null || columnAliases.isEmpty()) return table;
        List<Column> renamed = FromFunctionResolver.applyColumnAliases(
                new ArrayList<>(table.getColumns()), columnAliases);
        Table exposed = new Table(alias, renamed);
        String[] defined = new String[renamed.size()];
        for (int i = 0; i < renamed.size(); i++) defined[i] = DefinedTypes.typeIn(table, i);
        exposed.setDefinedColumnTypes(defined);
        exposed.setFunctionResult(table.isFunctionResult());
        return exposed;
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
                defineFromCte(virtualTable, cte);
                addBinding(bindings, asWritten(virtualTable, alias, tableRef.columnAliases()), alias);
                return;
            }
            // Check views
            Database.ViewDef view = viewFor(tableRef);
            if (view != null) {
                // Materialized views know their columns without re-running the query
                // (and an unpopulated matview must still be describable).
                if (view.materialized() && view.cachedColumns() != null && !view.cachedColumns().isEmpty()) {
                    Table mv = new Table(alias, view.cachedColumns());
                    addBinding(bindings, asWritten(mv, alias, tableRef.columnAliases()), alias);
                    return;
                }
                try {
                    QueryResult vr = executor.executeViewQuery(tableRef.table(), view.query());
                    if (!vr.getColumns().isEmpty()) {
                        Table virtualTable = new Table(alias, vr.getColumns());
                        defineFromView(virtualTable, view);
                        addBinding(bindings,
                                asWritten(virtualTable, alias, tableRef.columnAliases()), alias);
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
                    addBinding(bindings,
                            asWritten(catalogTable, alias, tableRef.columnAliases()), alias);
                    return;
                }
            }
            // Regular table
            try {
                Table table = executor.resolveTable(schemaName, tableRef.table());
                addBinding(bindings, asWritten(table, alias, tableRef.columnAliases()), alias);
            } catch (MemgresException e) { /* table not found, skip */ }
        } else if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom joinFrom = (SelectStmt.JoinFrom) item;
            resolveTableBindingsFromItem(joinFrom.left(), bindings);
            // A LATERAL item reads the names to its left, so describing it needs those names in
            // scope even when they carry no row — otherwise the describe fails and the lateral
            // alias goes missing from a query that answers with no rows at all.
            if (readsItemsToItsLeft(joinFrom.right())) {
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
                        defineFromQuery(virtualTable, subqFrom.subquery());
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
                // The columns above fall back to text wherever the definition list named a type
                // this does not read, so none of them is a type to be trusted.
                virtualTable.setDefinedColumnTypes(new String[cols.size()]);
                bindings.add(new RowContext.TableBinding(virtualTable, alias, new Object[cols.size()]));
            }
            // JSON_TABLE: extract column definitions from the JsonTableExpr
            else if (funcFrom.functionName().equals("__json_table__") && !funcFrom.args().isEmpty()
                    && funcFrom.args().get(0) instanceof JsonTableExpr) {
                JsonTableExpr jt = (JsonTableExpr) funcFrom.args().get(0);
                List<Column> cols = new ArrayList<>();
                collectJsonTableColumnDefs(jt.columns, cols);
                Table virtualTable = new Table(alias, cols);
                // Every column here is described as text whatever the definition list said, so
                // there is no type of one to be trusted.
                virtualTable.setDefinedColumnTypes(new String[cols.size()]);
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
                defineFromFunction(virtualTable, funcFrom);
                // What a call in FROM actually produces is decided by running it: a record with
                // no column definition list holds whatever fields it holds, and a single column
                // answers to names it does not carry through attribute notation. The columns above
                // are a shape to pad rows with, not the register of what this relation supplies,
                // and marking the provenance is what keeps the analysis from reading them as one --
                // exactly as FromFunctionResolver marks the relation it builds from real rows.
                virtualTable.setFunctionResult(true);
                bindings.add(new RowContext.TableBinding(virtualTable, alias, new Object[cols.size()]));
            }
        }
    }

    // ---- What a relation built from a definition may be trusted about ----
    //
    // Every relation below is built by running something and describing whatever came back, so its
    // columns carry the type the builder read off a value rather than the type the definition
    // gives them. Each is marked as such, along with the types its definition does settle, so that
    // a check reading a column's type knows which of the two it has. See {@link DefinedTypes}.

    /** A derived table, a VALUES list, or a parenthesized join given an alias. */
    private void defineFromQuery(Table virtualTable, Statement query) {
        virtualTable.setDefinedColumnTypes(
                executor.definedTypes.ofQuery(query, virtualTable.getColumns().size()));
    }

    private void defineFromCte(Table virtualTable, SelectStmt.CommonTableExpr cte) {
        virtualTable.setDefinedColumnTypes(
                executor.definedTypes.ofCte(cte, virtualTable.getColumns().size()));
    }

    private void defineFromView(Table virtualTable, Database.ViewDef view) {
        virtualTable.setDefinedColumnTypes(
                executor.definedTypes.ofView(view, virtualTable.getColumns().size()));
    }

    /** A stored relation rebuilt behind an alias list: its own declared types, under new names. */
    private static void defineAsDeclared(Table renamed, Table declared) {
        String[] types = new String[renamed.getColumns().size()];
        for (int i = 0; i < types.length; i++) types[i] = DefinedTypes.typeIn(declared, i);
        renamed.setDefinedColumnTypes(types);
    }

    private void defineFromFunction(Table virtualTable, SelectStmt.FunctionFrom funcFrom) {
        virtualTable.setDefinedColumnTypes(
                executor.definedTypes.ofFunction(funcFrom, virtualTable.getColumns().size()));
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
     * A FROM item that is not LATERAL cannot read a relation entered beside it.
     *
     * <p>The relations of one FROM clause are computed side by side, so a sub-select written as one
     * of them has no row of its neighbour to read — LATERAL is the word that says to compute it
     * once per such row instead. PostgreSQL says the entry is there but out of reach and names the
     * word that would bring it into reach; reporting the entry as missing sent the reader looking
     * for a relation they had written.
     *
     * <p>Which entries count is decided by position and nothing else. PostgreSQL builds the range
     * table left to right — a join's left side before its right, and a nested join in the place it
     * stands — and a sub-select is transformed against the entries made <em>before</em> it. So
     * {@code FROM t a, (SELECT a.v) b} names an entry that is there and out of reach, while
     * {@code FROM (SELECT a.v) b, t a} names one that has not been entered yet and is simply
     * missing. Whether a comma or a JOIN stands between them makes no difference: both enter the
     * relations in the order they are written, which is why this walks the whole clause rather
     * than its top-level items.
     *
     * <p>An entry answers both to the name it exposes and, where an alias renamed it, to the
     * relation's own name — PostgreSQL finds it either way, reports the name that was written and
     * names the entry in the detail.
     */
    private void rejectSiblingReferenceWithoutLateral(List<SelectStmt.FromItem> fromItems) {
        if (fromItems == null || fromItems.isEmpty()) return;
        Map<String, String> entered = new LinkedHashMap<>();
        for (SelectStmt.FromItem item : fromItems) enterRangeTableItem(item, entered, true);
    }

    /**
     * Whether a query level above this one supplies a relation of that name.
     *
     * <p>A sub-select written beside a relation cannot read it, but it can read the levels this
     * whole query is nested inside — that is an ordinary correlated reference and needs no LATERAL.
     * When both could be meant PostgreSQL takes the outer one, because the sibling is not in scope
     * at all: {@code SELECT 1 FROM t WHERE EXISTS (SELECT 1 FROM t z, (SELECT t.v) q)} reads the
     * outermost {@code t} and runs. So a name something above supplies is never this rule's.
     */
    private boolean enclosingLevelSupplies(String written) {
        if (written == null || executor == null) return false;
        for (RowContext outer : executor.outerContextStack) {
            List<RowContext.TableBinding> bindings = outer == null ? null : outer.getBindings();
            if (bindings == null) continue;
            for (RowContext.TableBinding b : bindings) {
                if (written.equalsIgnoreCase(b.alias())) return true;
                if (b.table() != null && written.equalsIgnoreCase(b.table().getName())) return true;
            }
        }
        return false;
    }

    /**
     * Walks one FROM tree in the order its relations enter the range table, judging each.
     *
     * @param lateralWouldHelp whether writing LATERAL on the item would be legal where it stands.
     *                         On the nullable side of a RIGHT or FULL join it would not — the rows
     *                         it would read are not determined when it is evaluated — so
     *                         PostgreSQL leaves the advice off there.
     */
    private void enterRangeTableItem(SelectStmt.FromItem item, Map<String, String> entered,
                                     boolean lateralWouldHelp) {
        if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
            enterRangeTableItem(join.left(), entered, lateralWouldHelp);
            enterRangeTableItem(join.right(), entered,
                    lateralWouldHelp && !nullsTheLeftSide(join.joinType()));
            return;
        }
        if (item instanceof SelectStmt.SubqueryFrom
                && !((SelectStmt.SubqueryFrom) item).lateral()
                && !entered.isEmpty()) {
            String written = SelectExecutor.firstReferenceTo(
                    ((SelectStmt.SubqueryFrom) item).subquery(), entered.keySet());
            if (written != null && enclosingLevelSupplies(written)) written = null;
            if (written != null) {
                String exposed = entered.get(written.toLowerCase());
                MemgresException e = new MemgresException(
                        "invalid reference to FROM-clause entry for table \"" + written + "\"",
                        "42P01");
                e.setDetail("There is an entry for table \"" + (exposed == null ? written : exposed)
                        + "\", but it cannot be referenced from this part of the query.");
                if (lateralWouldHelp) {
                    e.setHint("To reference that table, you must mark this subquery with LATERAL.");
                }
                throw e;
            }
        }
        String exposed = SelectExecutor.exposedNameOf(item);
        if (exposed == null) return;
        rememberEntry(entered, exposed, exposed);
        if (item instanceof SelectStmt.TableRef) {
            rememberEntry(entered, ((SelectStmt.TableRef) item).table(), exposed);
        }
    }

    /** A join whose right side may be read before the left is known, so no LATERAL may stand there. */
    private static boolean nullsTheLeftSide(SelectStmt.JoinType type) {
        return type == SelectStmt.JoinType.RIGHT || type == SelectStmt.JoinType.FULL
                || type == SelectStmt.JoinType.NATURAL_RIGHT
                || type == SelectStmt.JoinType.NATURAL_FULL;
    }

    private static void rememberEntry(Map<String, String> entered, String name, String exposed) {
        if (name == null) return;
        String key = name.toLowerCase(Locale.ROOT);
        if (!entered.containsKey(key)) entered.put(key, exposed);
    }

    /**
     * Resolve the FROM of an UPDATE or the USING of a DELETE. The statement's WHERE is not pushed
     * into the scan: it also names the table being written, which is not one of these relations.
     */
    List<RowContext> resolveWrittenFromClause(List<SelectStmt.FromItem> fromItems) {
        return resolveFromClauseInner(fromItems, null);
    }

    /**
     * Whether one of the schemas PostgreSQL searches implicitly answers to this bare name.
     *
     * <p>pg_temp and pg_catalog are searched whether or not search_path names them, so an empty
     * path hides the user's schemas and neither of those. A name this says yes to is left to the
     * ordinary resolution below, which reads the catalog or the temp schema and still refuses a
     * name neither of them has.
     */
    /**
     * Reports a relation the statement names that does not exist, before any clause of the
     * statement is judged.
     *
     * <p>PostgreSQL builds the range table first and transforms the rest of the query against it,
     * so a query that both names a missing relation and misuses a clause is reported as the
     * missing relation. memgres has no such phase — resolving a FROM item here means running it,
     * and running it is observable: a WITH item that writes would be applied, and a fault inside a
     * derived table would surface, for a statement PostgreSQL refuses outright. So this asks only
     * whether each name resolves to <em>something</em>, along the same ladder
     * {@link #resolveTableRef} walks, and reads nothing.
     *
     * <p>It can only report a name the ordinary lookup would also have reported: every branch that
     * finds anything at all returns without complaint, and the refusal itself is raised by that
     * same lookup so the message, hint and SQLSTATE are the ones it gives.
     */
    void checkRelationNamesExist(Object statement) {
        final Set<String> withNames = new HashSet<String>();
        AstWalk.forEach(statement, node -> {
            if (node instanceof SelectStmt.CommonTableExpr) {
                String n = ((SelectStmt.CommonTableExpr) node).name();
                if (n != null) withNames.add(n.toLowerCase(Locale.ROOT));
            }
        });
        AstWalk.forEach(statement, node -> {
            // The relation a data-modifying statement writes goes into the range table before the
            // ones it reads, so it is the one reported when neither of them exists. It is held as
            // a schema and a name rather than as a reference, so it is made into one here.
            SelectStmt.TableRef written = writtenRelationOf(node);
            if (written != null) checkOneRelationName(written, withNames);
            if (node instanceof SelectStmt.TableRef) {
                checkOneRelationName((SelectStmt.TableRef) node, withNames);
            }
        });
    }

    /** The relation a data-modifying statement writes, as a reference this can resolve. */
    static SelectStmt.TableRef writtenRelationOf(Object node) {
        if (node instanceof InsertStmt) {
            InsertStmt s = (InsertStmt) node;
            return new SelectStmt.TableRef(s.schema(), s.table(), null, false);
        }
        if (node instanceof UpdateStmt) {
            UpdateStmt s = (UpdateStmt) node;
            return new SelectStmt.TableRef(s.schema(), s.table(), null, false);
        }
        if (node instanceof DeleteStmt) {
            DeleteStmt s = (DeleteStmt) node;
            return new SelectStmt.TableRef(s.schema(), s.table(), null, false);
        }
        if (node instanceof MergeStmt) {
            MergeStmt s = (MergeStmt) node;
            return new SelectStmt.TableRef(s.schema(), s.targetTable(), null, false);
        }
        return null;
    }

    private void checkOneRelationName(SelectStmt.TableRef tableRef, Set<String> withNames) {
        if (tableRef.table() == null) return;
        // A WITH item is not a stored relation. Its name is taken from anywhere in the statement
        // rather than from the scope stack, because a name defined at another query level is
        // still not a table, and refusing a query PostgreSQL runs is the worse mistake.
        if (tableRef.schema() == null
                && withNames.contains(tableRef.table().toLowerCase(Locale.ROOT))) {
            return;
        }
        if (lookupCteFor(tableRef) != null) return;
        if (viewFor(tableRef) != null) return;

        String schemaName = tableRef.schema() != null ? tableRef.schema() : executor.defaultSchema();
        boolean userQualified = tableRef.schema() != null;
        try {
            executor.resolveTable(schemaName, tableRef.table(), userQualified);
            return;
        } catch (MemgresException e) {
            // Anything other than "no such relation" means the name found something and the
            // ordinary lookup has a better answer than this pass does.
            if (!"42P01".equals(e.getSqlState())) return;
        }
        if (SystemCatalog.isSystemCatalog(tableRef.schema(), tableRef.table())
                && executor.systemCatalog.resolve(
                        tableRef.schema(), tableRef.table(), executor.session) != null) {
            return;
        }
        // Nothing answers to the name. Raise it the way the ordinary lookup does, so the message
        // and the "there is a WITH item" hint are identical.
        executor.resolveTable(schemaName, tableRef.table(), userQualified);
    }

    private boolean implicitlySearchedSchemaHolds(String name) {
        Schema pgTemp = executor.database.getSchema(executor.session.getTempSchemaName());
        if (pgTemp != null && pgTemp.getTable(name) != null) return true;
        return SystemCatalog.isSystemCatalog(null, name)
                && executor.systemCatalog.resolve(null, name, executor.session) != null;
    }

    private List<RowContext> resolveFromClauseInner(List<SelectStmt.FromItem> fromItems, Expression where) {
        rejectSiblingReferenceWithoutLateral(fromItems);
        if (fromItems.size() == 1) {
            // A single relation with a WHERE is the one shape an index can narrow. The clause is
            // handed to the same resolver the sequential scan uses, which decides for itself
            // whether an index may answer it; every check the scan makes is made either way.
            if (where != null && fromItems.get(0) instanceof SelectStmt.TableRef) {
                return resolveTableRef((SelectStmt.TableRef) fromItems.get(0), where);
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
                // The definition is the same text for every left row, so what it settles about
                // the sub-query's columns is worked out once rather than once per row.
                String[] settled = null;
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
                        if (settled == null || settled.length != columns.size()) {
                            settled = executor.definedTypes.ofQuery(sqf.subquery(), columns.size());
                        }
                        virtualTable.setDefinedColumnTypes(settled);

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
                        // produces no rows for this left row removes it — the same as the
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

                    accumulated = pairer.pair(accumulated, resolved, applicablePredicates,
                            (l, r) -> joinExecutor.mergeContexts(l, r));
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
        return resolveTableRef(tableRef, null);
    }

    /**
     * Resolve a relation named in a FROM clause.
     *
     * <p>{@code indexWhere} is the query's WHERE, offered so that an equality on an indexed column
     * can be answered from the index instead of by reading every row. It is an optimisation and
     * nothing more: the caller applies the WHERE again to whatever comes back, so a probe is
     * allowed to return rows the clause will later discard but never to leave one out. Everything
     * that decides which rows a reader may see at all — the SELECT privilege, the ACCESS SHARE
     * lock, the repeatable-read snapshot, uncommitted work in other sessions and row-level
     * security — is settled below on the one path, so an index scan and a sequential scan cannot
     * disagree about it.
     */
    private List<RowContext> resolveTableRef(SelectStmt.TableRef tableRef, Expression indexWhere) {
        // Check CTEs first
        SelectStmt.CommonTableExpr cte = lookupCteFor(tableRef);
        if (cte != null) {
            String alias = tableRef.alias() != null ? tableRef.alias() : tableRef.table();
            QueryResult cteResult = executor.selectExecutor.executeCte(cte);
            Table virtualTable = new Table(alias,
                    renameColumns(alias, cteResult.getColumns(), tableRef.columnAliases()));
            defineFromCte(virtualTable, cte);
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
        Database.ViewDef view = viewFor(tableRef);
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
            defineFromView(virtualTable, view);
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
        // An empty search_path leaves an unqualified name nowhere to be found -- except in the
        // two schemas PostgreSQL searches whether or not the path names them. pg_temp is one and
        // was already allowed for here; pg_catalog is the other, and leaving it out is why
        // pg_dump could not read this server at all. pg_dump clears the search_path on purpose,
        // so that a schema planted by someone else cannot answer to a name it means to read as
        // the catalog's, and then reads pg_settings by its bare name -- which PostgreSQL answers
        // and this refused.
        if (!userQualified && executor.session != null) {
            String sp = executor.session.getGucSettings().get("search_path");
            if (sp != null) {
                boolean hasEntries = false;
                for (String part : sp.split(",")) {
                    String s = part.trim().replace("\"", "").replace("'", "");
                    if (!s.isEmpty() && !s.equals("$user")) { hasEntries = true; break; }
                }
                if (!hasEntries && !implicitlySearchedSchemaHolds(tableRef.table())) {
                    throw new MemgresException("relation \"" + tableRef.table() + "\" does not exist", "42P01");
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
            // Renaming a column does not retype it, and the relation this stands in front of
            // declared every one of them, so the types stay as certain as they were.
            defineAsDeclared(renamed, table);
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
        List<Object[]> probed = indexWhere == null ? null
                : indexProbe(tableRef, table, indexWhere, schemaTableKey, currentSession);
        if (probed != null) {
            for (Object[] row : probed) {
                Object[] r = hasVirtual ? executor.dmlExecutor.computeVirtualColumns(table, row) : row;
                contexts.add(new RowContext(table, alias, r));
            }
        } else if (tableRef.only()) {
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
     * The rows an index can answer an equality with, or null to read the relation in full.
     *
     * <p>An index holds every stored row of the relation under the value of its columns, so a
     * {@code WHERE col = value} that names all of an index's columns can be answered by one lookup
     * instead of a pass over the table. A 10,000-row relation read 5,000 times by primary key is
     * 50 million row visits done as 5,000 hash lookups.
     *
     * <p>Returning null is always safe — it means read everything — and every doubt is settled
     * that way. The probe is refused unless the value written in the query is the same value the
     * relation stores, because the index compares stored values and the query does not: a function
     * or a cast over the column, a value of another type, a domain or an enum with its own storage
     * rules, all send the query back to a sequential scan rather than risk a row that answers the
     * clause not being found under the key that was looked up. {@code = NULL} matches nothing in
     * SQL, so it is not a key to look anything up under either.
     *
     * <p>The rows come from the index and are therefore the relation as it stands now. That is the
     * right answer only when now is what the reader is entitled to see, so a transaction reading
     * from a repeatable-read snapshot, and a reader with another session's uncommitted work on the
     * relation to account for, both read it in full: the snapshot is the whole relation and cannot
     * be rebuilt from a lookup, and an uncommitted update to an indexed column has already moved
     * the row to its new key while the reader is still owed its old one.
     */
    private List<Object[]> indexProbe(SelectStmt.TableRef tableRef, Table table, Expression where,
                                      String schemaTableKey, Session session) {
        if (table.getIndexes().isEmpty()) return null;
        // ONLY, inheritance and partitioning spread the relation's rows over tables of their own,
        // each with an index of its own; one index answers for one of them.
        if (tableRef.only()) return null;
        if (!table.getPartitions().isEmpty() || !table.getChildren().isEmpty()
                || table.getParentTable() != null) return null;
        if (session != null && !indexReflectsWhatSessionMaySee(schemaTableKey, session)) return null;

        Map<String, Object> equalities = collectEqualityKeys(tableRef, table, where);
        if (equalities == null || equalities.isEmpty()) return null;

        // A unique index answers with at most one row, so it is preferred where both would do.
        List<Object[]> found = probeIndexes(table, equalities, true);
        if (found == null) found = probeIndexes(table, equalities, false);
        return found;
    }

    /** Look up {@code equalities} in the table's unique or non-unique indexes, or null for none. */
    private List<Object[]> probeIndexes(Table table, Map<String, Object> equalities, boolean unique) {
        List<Column> columns = table.getColumns();
        for (TableIndex idx : table.getIndexes().values()) {
            if (idx.isUnique() != unique) continue;
            int[] colIndices = idx.getColumnIndices();
            if (colIndices.length == 0) continue;
            Object[] keyValues = new Object[colIndices.length];
            boolean complete = true;
            for (int i = 0; i < colIndices.length; i++) {
                if (colIndices[i] < 0 || colIndices[i] >= columns.size()) { complete = false; break; }
                String name = columns.get(colIndices[i]).getName().toLowerCase(Locale.ROOT);
                if (!equalities.containsKey(name)) { complete = false; break; }
                keyValues[i] = equalities.get(name);
            }
            if (!complete) continue;
            return idx.findAll(keyValues);
        }
        return null;
    }

    /**
     * Whether what the index holds now is what this session is entitled to read.
     *
     * <p>A repeatable-read transaction reads the relation as it stood when it first looked, which
     * is a list of rows the session keeps and the index knows nothing of. Building that list from
     * a probe would fix the snapshot at the few rows one lookup returned, and every later
     * statement in the transaction would read the relation as if it held only those — so a
     * transaction at that isolation level reads in full whether it has taken its snapshot yet or
     * not.
     *
     * <p>Another session's uncommitted work is undone for this reader row by row, which needs the
     * rows: an update it has not committed has already moved its row to the new key, and this
     * reader is owed the row under the old one.
     */
    private boolean indexReflectsWhatSessionMaySee(String schemaTableKey, Session session) {
        if (session.hasRRSnapshot(schemaTableKey)) return false;
        if (session.isInTransaction()) {
            String isolation = session.getEffectiveIsolationLevel();
            if ("repeatable read".equals(isolation) || "serializable".equals(isolation)) return false;
        }
        for (Session other : executor.database.getActiveSessions()) {
            if (other == session) continue;
            if (!other.isInTransaction()) continue;
            if (!other.getUncommittedInserts(schemaTableKey).isEmpty()) return false;
            if (!other.getUncommittedUpdates(schemaTableKey).isEmpty()) return false;
            if (!other.getUncommittedDeletes(schemaTableKey).isEmpty()) return false;
        }
        return true;
    }

    /**
     * The column-name-to-stored-value pairs an index may be looked up under, from the ANDed
     * equalities of a WHERE. Null when the clause holds none usable.
     *
     * <p>Only a bare column against a written value counts. A cast or a function over the column
     * is a different value from the one the index holds, and a qualifier naming something other
     * than this relation is not this relation's column at all.
     */
    private Map<String, Object> collectEqualityKeys(SelectStmt.TableRef tableRef, Table table,
                                                    Expression where) {
        String alias = tableRef.alias() != null ? tableRef.alias() : tableRef.table();
        Map<String, Object> equalities = null;
        for (Expression pred : flattenAndPredicates(where)) {
            if (!(pred instanceof BinaryExpr)) continue;
            BinaryExpr bin = (BinaryExpr) pred;
            if (bin.op() != BinaryExpr.BinOp.EQUAL) continue;
            ColumnRef ref = null;
            Expression valueExpr = null;
            if (bin.left() instanceof ColumnRef && isWrittenValue(bin.right())) {
                ref = (ColumnRef) bin.left();
                valueExpr = bin.right();
            } else if (bin.right() instanceof ColumnRef && isWrittenValue(bin.left())) {
                ref = (ColumnRef) bin.right();
                valueExpr = bin.left();
            }
            if (ref == null || ref.column() == null) continue;
            if (ref.table() != null && !ref.table().equalsIgnoreCase(alias)
                    && !ref.table().equalsIgnoreCase(tableRef.table())) continue;
            int colIdx = table.getColumnIndex(ref.column());
            if (colIdx < 0) continue;
            Column column = table.getColumns().get(colIdx);
            Object key = storedFormOf(valueExpr, column);
            if (key == null) continue;
            String name = column.getName().toLowerCase(Locale.ROOT);
            if (equalities == null) equalities = new LinkedHashMap<String, Object>();
            Object prior = equalities.put(name, key);
            // Two equalities on one column with different values match nothing; there is no one
            // key to look that up under, so the clause is left to the scan.
            if (prior != null
                    && !Objects.equals(TableIndex.normalize(prior), TableIndex.normalize(key))) {
                return null;
            }
        }
        return equalities;
    }

    /** A value written in the query itself, which is the same for every row and reads no column. */
    private boolean isWrittenValue(Expression expr) {
        return expr instanceof Literal || expr instanceof ParamRef;
    }

    /**
     * The value as this column stores it, or null when the query's value cannot be turned into a
     * key the index would have filed the row under.
     *
     * <p>Stored values were put through {@link TypeCoercion#coerceForStorage} on their way in, so
     * a lookup value has to make the same trip — CHAR(n) pads, NUMERIC(p,s) rounds — or a row
     * that answers the clause sits under a key the probe never asks for. The trip is only taken
     * for types where it lands somewhere the index compares the way SQL does: an enum, a domain,
     * an array, a composite, JSON, a network address and a geometric value each have an equality
     * of their own that comparing stored forms does not reproduce, and a value of the wrong type
     * altogether is a question for the scan, which raises whatever PostgreSQL raises for it.
     */
    private Object storedFormOf(Expression valueExpr, Column column) {
        if (!hasProbeableStorage(column)) return null;
        Object raw;
        try {
            raw = executor.evalExpr(valueExpr, null);
        } catch (RuntimeException e) {
            return null;
        }
        // = NULL is unknown for every row, never a key.
        if (raw == null) return null;
        if (!valueMatchesColumnType(raw, column.getType())) return null;
        try {
            return TypeCoercion.coerceForStorage(raw, column);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** Whether this column's stored values are ones an index lookup compares the way SQL does. */
    private static boolean hasProbeableStorage(Column column) {
        if (column.isVirtual()) return false;
        if (column.getEnumTypeName() != null) return false;
        if (column.getDomainTypeName() != null) return false;
        if (column.getCompositeTypeName() != null) return false;
        if (column.getArrayElementType() != null) return false;
        switch (column.getType()) {
            case SMALLINT: case INTEGER: case BIGINT:
            case SMALLSERIAL: case SERIAL: case BIGSERIAL:
            case NUMERIC: case REAL: case DOUBLE_PRECISION:
            case TEXT: case VARCHAR: case CHAR: case NAME:
            case BOOLEAN:
            case DATE: case TIME: case TIMESTAMP: case TIMESTAMPTZ:
            case UUID:
                return true;
            default:
                return false;
        }
    }

    /**
     * Whether the query's value is of the type this column holds.
     *
     * <p>A quoted value has no type of its own in PostgreSQL and takes the column's, so a string
     * is accepted for a date or a UUID. Anything else crossing type families — a number against a
     * text column, a word against an integer one — is either an error PostgreSQL reports or a
     * comparison with a rule of its own, and neither is settled by a hash lookup.
     */
    private static boolean valueMatchesColumnType(Object value, DataType type) {
        switch (type) {
            case SMALLINT: case INTEGER: case BIGINT:
            case SMALLSERIAL: case SERIAL: case BIGSERIAL:
            case NUMERIC: case REAL: case DOUBLE_PRECISION:
                return value instanceof Number;
            case TEXT: case VARCHAR: case CHAR: case NAME:
                return value instanceof String;
            case BOOLEAN:
                return value instanceof Boolean;
            case DATE:
                return value instanceof String || value instanceof java.time.LocalDate;
            case TIME:
                return value instanceof String || value instanceof java.time.LocalTime;
            case TIMESTAMP:
                return value instanceof String || value instanceof java.time.LocalDateTime;
            case TIMESTAMPTZ:
                return value instanceof String || value instanceof java.time.OffsetDateTime;
            case UUID:
                return value instanceof String || value instanceof java.util.UUID;
            default:
                return false;
        }
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
        defineFromQuery(virtualTable, subqFrom.subquery());
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

                accumulated = pairer.pair(accumulated, resolved, applicablePredicates,
                        (l, r) -> l.merge(r));
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

    /**
     * Whether every column a conjunct names is one this row has, and so whether it can be
     * decided here at all.
     *
     * <p>Answering yes wrongly does not lose rows: a conjunct that then fails to evaluate leaves
     * its rows alone, and the query's own WHERE runs over the finished rows regardless. It only
     * costs the chance to have filtered earlier.
     */
    static boolean canEvaluatePredicate(Expression pred, RowContext ctx) {
        try {
            collectColumnRefs(pred, ctx);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static void collectColumnRefs(Expression expr, RowContext ctx) {
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
