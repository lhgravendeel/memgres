package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.*;

/**
 * Handles CREATE/ALTER VIEW, REFRESH MATERIALIZED VIEW.
 * Extracted from DdlExecutor to separate concerns.
 */
class DdlViewExecutor {
    /** Errors about the query itself rather than about the rows it happens to read. */
    private static final Set<String> ANALYSIS_ERRORS =
            Cols.setOf("42803", "42P10", "42P20", "42601", "42809", "42712", "42P09");

    private final DdlExecutor ddl;
    private final AstExecutor executor;

    DdlViewExecutor(DdlExecutor ddl) {
        this.ddl = ddl;
        this.executor = ddl.executor;
    }

    // ---- CREATE VIEW ----

    QueryResult executeCreateView(CreateViewStmt stmt) {
        ddl.checkPgCatalogWriteProtection();
        // A CREATE that says which schema to create in is refused outright when there is no such
        // schema, before the query it would store is looked at.
        SchemaQualifier.requireSchema(executor.database, executor.session, stmt.schema());
        // A view name is taken in the schema the view goes into, not in the database at large:
        // another schema may already hold a view of that name, and this one is still free.
        String createSchema = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        if (!stmt.orReplace() && executor.database.hasView(createSchema, stmt.name())) {
            throw new MemgresException("relation \"" + stmt.name() + "\" already exists", "42P07");
        }
        // A table, sequence or index of this name owns it just as firmly as another view would;
        // CREATE OR REPLACE can only replace a view, never take a name from another kind. What
        // it is refused with differs: a plain CREATE reports the name as taken, while a REPLACE
        // reports that what is there is not the kind it knows how to replace.
        String targetSchema = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        if (stmt.orReplace() && !stmt.materialized()) {
            RelationNamespace.requireKind(executor.database, targetSchema, stmt.name(),
                    RelationNamespace.VIEW);
        }
        RelationNamespace.requireFree(executor.database, targetSchema, stmt.name(),
                stmt.materialized() ? RelationNamespace.MATVIEW : RelationNamespace.VIEW);
        Database.ViewDef oldView = executor.database.getView(targetSchema, stmt.name());

        if (stmt.orReplace() && oldView != null && !stmt.materialized()) {
            try {
                QueryResult oldResult = executor.executeStatement(oldView.query());
                QueryResult newResult = executor.executeStatement(stmt.query());
                if (newResult.getColumns().size() < oldResult.getColumns().size()) {
                    throw new MemgresException("cannot drop columns from view", "42P16");
                }
                // PG rejects column name changes and type changes for existing columns
                int checkCount = Math.min(oldResult.getColumns().size(), newResult.getColumns().size());
                for (int i = 0; i < checkCount; i++) {
                    Column oldCol = oldResult.getColumns().get(i);
                    Column newCol = newResult.getColumns().get(i);
                    if (!oldCol.getName().equalsIgnoreCase(newCol.getName())) {
                        throw new MemgresException("cannot change name of view column \"" + oldCol.getName()
                                + "\" to \"" + newCol.getName() + "\"", "42P16");
                    }
                    if (oldCol.getType() != newCol.getType()) {
                        throw new MemgresException("cannot change data type of view column \"" + oldCol.getName()
                                + "\" from " + oldCol.getType() + " to " + newCol.getType(), "42P16");
                    }
                }
            } catch (MemgresException e) {
                throw e;
            } catch (Exception e) {
                // If we can't execute either query, let it fail later
            }
        }

        // A qualified name puts the view in the schema it names.
        String viewSchema = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();

        // PG expands SELECT * when the view is defined, freezing the column list.
        // (A view over a temp table becomes temporary itself — see below.)
        // Adding a column to a base table later must not change the view's output.
        Statement query = expandWildcardTargets(stmt.query());

        // Apply the optional column alias list: CREATE [MATERIALIZED] VIEW v(a, b) AS ...
        query = applyColumnAliasList(stmt, query);

        // A CHECK OPTION on a view no INSERT can reach is a promise that can never be kept.
        if (stmt.checkOption() != null && !isAutoUpdatable(query)) {
            throw PgErrors.notImplemented(
                    "WITH CHECK OPTION is supported only on automatically updatable views");
        }

        // "view will be a temporary view": a view whose query reads a temp table cannot outlive
        // the session, so PG puts it in the temp namespace instead of leaving a dangling view.
        if (referencesTempTable(query)) {
            viewSchema = executor.session != null ? executor.session.getTempSchemaName() : viewSchema;
        }

        int rowCount = 0;
        if (stmt.materialized()) {
            if (stmt.withData()) {
                // A materialized view is filled when it is created, so its query is planned then.
                QueryResult result = executor.executeStatement(query);
                List<Column> cols = new ArrayList<>(result.getColumns());
                List<Object[]> rows = new ArrayList<>(result.getRows());
                rowCount = rows.size();
                rejectDuplicateColumnNames(cols);
                executor.database.addView(new Database.ViewDef(stmt.name(), viewSchema, query, stmt.orReplace(),
                        true, cols, rows, null, null, stmt.withOptions(), true));
            } else {
                // WITH NO DATA: resolve column metadata but leave the view unpopulated —
                // any scan must fail with 55000 until REFRESH MATERIALIZED VIEW.
                List<Column> cols = Cols.listOf();
                try {
                    QueryResult result = executor.executeStatement(query);
                    cols = new ArrayList<>(result.getColumns());
                } catch (Exception ignored) {
                    // Column metadata unavailable; the view is still created (matches lenient CREATE VIEW path)
                }
                rejectDuplicateColumnNames(cols);
                executor.database.addView(new Database.ViewDef(stmt.name(), viewSchema, query, stmt.orReplace(),
                        true, cols, Cols.listOf(), null, null, stmt.withOptions(), false));
            }
        } else {
            List<Column> resolvedColumns = null;
            try {
                QueryResult result = executor.executeStatement(query);
                resolvedColumns = new ArrayList<>(result.getColumns());
            } catch (MemgresException e) {
                if ("42P01".equals(e.getSqlState()) && e.getMessage() != null
                        && e.getMessage().contains("does not exist") && !e.getMessage().contains("missing FROM-clause")) {
                    throw e;
                }
                if ("42703".equals(e.getSqlState())) {
                    throw e;
                }
                // A query PostgreSQL refuses to analyse is refused as a view definition too:
                // an ungrouped column, a sort position past the select list or a misplaced
                // window function is wrong about the query itself, not about the rows it would
                // read, and storing it only defers the error to every SELECT from the view — a
                // view over an ungrouped column then answered with an arbitrary row's value.
                if (ANALYSIS_ERRORS.contains(e.getSqlState())) {
                    throw e;
                }
            } catch (Exception e) {
                // Silently ignore execution errors during view validation
            }
            rejectDuplicateColumnNames(resolvedColumns);
            executor.database.addView(new Database.ViewDef(stmt.name(), viewSchema, query, stmt.orReplace(),
                    false, resolvedColumns, null, null, stmt.checkOption(), stmt.withOptions(), true));
        }

        executor.database.registerSchemaObject(viewSchema, "view", stmt.name());
        if (oldView != null) {
            executor.recordUndo(new Session.DropViewUndo(stmt.name(), oldView));
        }
        executor.recordUndo(new Session.CreateViewUndo(viewSchema, stmt.name()));
        executor.database.setObjectOwner("view:" + viewSchema + "." + stmt.name(), executor.sessionUser());
        if (stmt.materialized()) {
            return QueryResult.command(QueryResult.Type.SELECT_INTO, rowCount);
        }
        return QueryResult.message(QueryResult.Type.SET, "CREATE VIEW");
    }

    /**
     * A view whose output repeats a column name cannot be selected from unambiguously, so the
     * name clash has to be caught while the view is being defined rather than at every read.
     */
    private static void rejectDuplicateColumnNames(List<Column> columns) {
        if (columns == null) return;
        Set<String> seen = new HashSet<>();
        for (Column c : columns) {
            if (!seen.add(c.getName().toLowerCase())) {
                throw PgErrors.duplicateColumn(c.getName());
            }
        }
    }

    /**
     * Whether a view over this query is automatically updatable — the same test the DML path
     * makes when it resolves a view back to its base table.
     */
    private boolean isAutoUpdatable(Statement query) {
        if (!(query instanceof SelectStmt)) return false;
        SelectStmt sel = (SelectStmt) query;
        if (sel.distinct()) return false;
        if (sel.from() == null || sel.from().size() != 1) return false;
        if (!(sel.from().get(0) instanceof SelectStmt.TableRef)) return false;
        if (sel.groupBy() != null && !sel.groupBy().isEmpty()) return false;
        if (sel.having() != null) return false;
        if (sel.limit() != null || sel.offset() != null) return false;
        if (sel.targets() != null) {
            for (SelectStmt.SelectTarget target : sel.targets()) {
                if (StoredExprCheck.hasAggregate(target.expr())) return false;
            }
        }
        return true;
    }

    // ---- SELECT * freeze (star expansion at CREATE VIEW time) ----

    /**
     * Expand wildcard SELECT targets ({@code *} and {@code alias.*}) into explicit
     * column references resolved against the current schema. Returns the original
     * query unchanged when expansion is not possible (unresolvable FROM items,
     * NATURAL joins, CTEs) or when the expanded query would not produce the same
     * columns as the original (verified by executing both).
     */
    private Statement expandWildcardTargets(Statement query) {
        Statement expanded = expandStarsRec(query);
        if (expanded == query) return query;
        // Safety net: expansion must be behavior-preserving for the view's output columns.
        try {
            QueryResult original = executor.executeStatement(query);
            QueryResult replacement = executor.executeStatement(expanded);
            if (!columnNamesOf(original).equals(columnNamesOf(replacement))) {
                return query;
            }
        } catch (Exception e) {
            return query;
        }
        return expanded;
    }

    private static List<String> columnNamesOf(QueryResult result) {
        List<String> names = new ArrayList<>();
        for (Column c : result.getColumns()) names.add(c.getName().toLowerCase());
        return names;
    }

    private Statement expandStarsRec(Statement query) {
        if (query instanceof SetOpStmt) {
            SetOpStmt so = (SetOpStmt) query;
            Statement left = expandStarsRec(so.left());
            Statement right = expandStarsRec(so.right());
            if (left == so.left() && right == so.right()) return so;
            return new SetOpStmt(left, so.op(), so.all(), right, so.orderBy(), so.limit(), so.offset());
        }
        if (!(query instanceof SelectStmt)) return query;
        SelectStmt s = (SelectStmt) query;
        if (s.targets() == null || s.from() == null || s.from().isEmpty()) return s;
        // CTE columns are not resolvable outside statement execution; leave those queries as-is.
        if (s.withClauses() != null && !s.withClauses().isEmpty()) return s;
        boolean hasStar = false;
        for (SelectStmt.SelectTarget t : s.targets()) {
            if (t.expr() instanceof WildcardExpr) { hasStar = true; break; }
        }
        if (!hasStar) return s;
        // NATURAL joins merge common columns without an explicit USING list; the plain
        // bindings don't reflect that merge, so leave those queries unexpanded.
        if (hasNaturalJoin(s.from())) return s;
        List<RowContext.TableBinding> bindings;
        try {
            bindings = executor.fromResolver.resolveTableBindings(s.from());
        } catch (Exception e) {
            return s;
        }
        if (bindings.isEmpty()) return s;
        Set<String> usingCols = new HashSet<>();
        collectUsingColumns(s.from(), usingCols);

        List<SelectStmt.SelectTarget> newTargets = new ArrayList<>();
        for (SelectStmt.SelectTarget target : s.targets()) {
            if (!(target.expr() instanceof WildcardExpr)) {
                newTargets.add(target);
                continue;
            }
            WildcardExpr w = (WildcardExpr) target.expr();
            if (w.table() != null) {
                boolean matched = false;
                for (RowContext.TableBinding b : bindings) {
                    if (b.alias().equalsIgnoreCase(w.table()) || b.table().getName().equalsIgnoreCase(w.table())) {
                        matched = true;
                        for (Column c : b.table().getColumns()) {
                            newTargets.add(new SelectStmt.SelectTarget(new ColumnRef(w.table(), c.getName()), null));
                        }
                    }
                }
                if (!matched) return s; // unresolvable qualifier — leave the query unexpanded
            } else {
                Set<String> emittedUsing = new HashSet<>();
                for (RowContext.TableBinding b : bindings) {
                    for (Column c : b.table().getColumns()) {
                        String lower = c.getName().toLowerCase();
                        if (usingCols.contains(lower)) {
                            // USING columns are merged: emit once, unqualified so the
                            // runtime COALESCE(left, right) semantics are preserved.
                            if (!emittedUsing.add(lower)) continue;
                            newTargets.add(new SelectStmt.SelectTarget(new ColumnRef(null, c.getName()), null));
                        } else {
                            newTargets.add(new SelectStmt.SelectTarget(new ColumnRef(b.alias(), c.getName()), null));
                        }
                    }
                }
            }
        }
        return new SelectStmt(s.distinct(), s.distinctOn(), newTargets, s.from(), s.where(), s.groupBy(),
                s.having(), s.windowDefs(), s.orderBy(), s.limit(), s.offset(), s.withClauses(),
                s.groupingSets(), s.lockClause(), s.withTies());
    }

    private static void collectUsingColumns(List<SelectStmt.FromItem> items, Set<String> out) {
        for (SelectStmt.FromItem item : items) {
            collectUsingColumnsFromItem(item, out);
        }
    }

    private static void collectUsingColumnsFromItem(SelectStmt.FromItem item, Set<String> out) {
        if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom j = (SelectStmt.JoinFrom) item;
            if (j.using() != null) {
                for (String col : j.using()) out.add(col.toLowerCase());
            }
            collectUsingColumnsFromItem(j.left(), out);
            collectUsingColumnsFromItem(j.right(), out);
        }
    }

    private static boolean hasNaturalJoin(List<SelectStmt.FromItem> items) {
        for (SelectStmt.FromItem item : items) {
            if (hasNaturalJoinItem(item)) return true;
        }
        return false;
    }

    private static boolean hasNaturalJoinItem(SelectStmt.FromItem item) {
        if (!(item instanceof SelectStmt.JoinFrom)) return false;
        SelectStmt.JoinFrom j = (SelectStmt.JoinFrom) item;
        switch (j.joinType()) {
            case NATURAL:
            case NATURAL_LEFT:
            case NATURAL_RIGHT:
            case NATURAL_FULL:
                return true;
            default:
                return hasNaturalJoinItem(j.left()) || hasNaturalJoinItem(j.right());
        }
    }

    // ---- Column alias list: CREATE VIEW v(a, b) AS ... ----

    /** True when the view query reads a table that lives in this session's temp namespace. */
    private boolean referencesTempTable(Statement query) {
        if (executor.session == null) return false;
        Schema temp = executor.database.getSchema(executor.session.getTempSchemaName());
        if (temp == null || temp.getTables().isEmpty()) return false;
        for (String tableName : temp.getTables().keySet()) {
            if (AstRelationRenamer.referencesRelation(query, null, tableName)) return true;
        }
        return false;
    }

    private Statement applyColumnAliasList(CreateViewStmt stmt, Statement query) {
        List<String> names = stmt.columnNames();
        if (names == null || names.isEmpty()) return query;

        int columnCount = countOutputColumns(query);
        if (columnCount < 0) {
            try {
                columnCount = executor.executeStatement(query).getColumns().size();
            } catch (Exception e) {
                columnCount = -1;
            }
        }
        if (columnCount >= 0 && names.size() > columnCount) {
            if (stmt.materialized()) {
                // PG routes matviews through CREATE TABLE AS, which reports a syntax error
                throw new MemgresException("too many column names were specified", "42601");
            }
            throw new MemgresException("CREATE VIEW specifies more column names than columns", "42601");
        }

        Statement renamed = renameLeadingTargets(query, names);
        if (renamed != null) return renamed;

        // Fallback for query shapes whose targets can't be rewritten directly
        // (wildcards left unexpanded, composite stars): wrap the query in a
        // derived table whose column alias list applies the names.
        List<SelectStmt.SelectTarget> star = new ArrayList<>();
        star.add(new SelectStmt.SelectTarget(new WildcardExpr(), null));
        List<SelectStmt.FromItem> from = new ArrayList<>();
        from.add(new SelectStmt.SubqueryFrom(query, stmt.name(), false, names));
        return new SelectStmt(false, null, star, from, null, null, null, null, null, null, null, null, null, null, false);
    }

    /** Number of output columns statically derivable from the query, or -1 if unknown. */
    private static int countOutputColumns(Statement query) {
        if (query instanceof SetOpStmt) return countOutputColumns(((SetOpStmt) query).left());
        if (!(query instanceof SelectStmt)) return -1;
        SelectStmt s = (SelectStmt) query;
        if (s.targets() == null) return -1;
        for (SelectStmt.SelectTarget t : s.targets()) {
            if (t.expr() instanceof WildcardExpr || t.expr() instanceof CompositeStarExpr) return -1;
        }
        return s.targets().size();
    }

    /**
     * Re-alias the first {@code names.size()} SELECT targets (set operations rename
     * the left branch, which provides the output column names). Returns null when
     * the targets cannot be renamed statically.
     */
    private Statement renameLeadingTargets(Statement query, List<String> names) {
        if (query instanceof SetOpStmt) {
            SetOpStmt so = (SetOpStmt) query;
            Statement left = renameLeadingTargets(so.left(), names);
            if (left == null) return null;
            return new SetOpStmt(left, so.op(), so.all(), so.right(), so.orderBy(), so.limit(), so.offset());
        }
        if (!(query instanceof SelectStmt)) return null;
        SelectStmt s = (SelectStmt) query;
        if (s.targets() == null) return null;
        int renameCount = Math.min(names.size(), s.targets().size());
        for (int i = 0; i < renameCount; i++) {
            Expression e = s.targets().get(i).expr();
            if (e instanceof WildcardExpr || e instanceof CompositeStarExpr) return null;
        }
        List<SelectStmt.SelectTarget> newTargets = new ArrayList<>(s.targets());
        Map<String, String> renames = new LinkedHashMap<>();
        for (int i = 0; i < renameCount; i++) {
            SelectStmt.SelectTarget old = newTargets.get(i);
            String oldName = old.alias() != null ? old.alias() : executor.exprToAlias(old.expr());
            if (oldName != null && !oldName.equalsIgnoreCase(names.get(i))) {
                renames.put(oldName.toLowerCase(), names.get(i));
            }
            newTargets.set(i, new SelectStmt.SelectTarget(old.expr(), names.get(i)));
        }
        // ORDER BY may reference an output column by its old name; keep it resolvable.
        List<SelectStmt.OrderByItem> newOrderBy = s.orderBy();
        if (newOrderBy != null && !renames.isEmpty()) {
            List<SelectStmt.OrderByItem> rewritten = new ArrayList<>(newOrderBy.size());
            for (SelectStmt.OrderByItem item : newOrderBy) {
                Expression e = item.expr();
                if (e instanceof ColumnRef && ((ColumnRef) e).table() == null && ((ColumnRef) e).column() != null) {
                    String replacement = renames.get(((ColumnRef) e).column().toLowerCase());
                    if (replacement != null && !isFromColumn(s, ((ColumnRef) e).column())) {
                        rewritten.add(new SelectStmt.OrderByItem(new ColumnRef(null, replacement),
                                item.descending(), item.nullsFirst()));
                        continue;
                    }
                }
                rewritten.add(item);
            }
            newOrderBy = rewritten;
        }
        return new SelectStmt(s.distinct(), s.distinctOn(), newTargets, s.from(), s.where(), s.groupBy(),
                s.having(), s.windowDefs(), newOrderBy, s.limit(), s.offset(), s.withClauses(),
                s.groupingSets(), s.lockClause(), s.withTies());
    }

    /** True if the name resolves to a column of one of the FROM tables (which takes precedence in ORDER BY). */
    private boolean isFromColumn(SelectStmt s, String name) {
        if (s.from() == null || s.from().isEmpty()) return false;
        try {
            for (RowContext.TableBinding b : executor.fromResolver.resolveTableBindings(s.from())) {
                if (b.table().getColumnIndex(name) >= 0) return true;
            }
        } catch (Exception ignored) {
        }
        return false;
    }

    // ---- ALTER VIEW ----

    QueryResult executeAlterView(AlterViewStmt stmt) {
        if (stmt.action() == AlterViewStmt.Action.RENAME_TO) {
            Database.ViewDef existing = executor.database.getView(stmt.name());
            if (existing == null || existing.materialized() != stmt.materialized()) {
                if (existing == null && stmt.ifExists()) {
                    return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
                }
                // A table or a sequence of that name is a wrong-kind complaint, not a missing one.
                requireViewKind(stmt);
                throw new MemgresException("relation \"" + stmt.name() + "\" does not exist", "42P01");
            }
            if (executor.database.hasView(stmt.newName())) {
                throw new MemgresException("relation \"" + stmt.newName() + "\" already exists", "42P07");
            }
            // A table, sequence or index already answers to the new name just as firmly.
            RelationNamespace.requireFree(executor.database,
                    existing.schemaName() != null ? existing.schemaName() : executor.defaultSchema(),
                    stmt.newName(), null);
            String viewSchema = existing.schemaName() != null
                    ? existing.schemaName() : executor.defaultSchema();
            executor.database.removeView(stmt.name());
            executor.database.addView(new Database.ViewDef(stmt.newName(), existing.schemaName(), existing.query(),
                    existing.orReplace(), existing.materialized(),
                    existing.cachedColumns(), existing.cachedRows(), existing.sourceSQL(),
                    existing.checkOption(), existing.reloptions(), existing.populated()));
            // The view is the same object under another name, so its OID, its comment and its
            // grants stay with it. See ObjectIdentity.
            executor.identity().relationRenamed(existing.materialized() ? "m" : "v",
                    viewSchema, RelationNamespace.bareName(stmt.name()),
                    viewSchema, RelationNamespace.bareName(stmt.newName()));
        }
        if (stmt.action() == AlterViewStmt.Action.OWNER_TO) {
            String newOwner = ddl.resolveOwnerName(stmt.newName());
            if (!executor.database.hasRole(newOwner)) {
                throw new MemgresException("role \"" + newOwner + "\" does not exist", "42704");
            }
            Database.ViewDef vd = executor.database.getView(stmt.name());
            String vSchema = (vd != null && vd.schemaName() != null) ? vd.schemaName() : executor.defaultSchema();
            executor.database.setObjectOwner(
                    "view:" + vSchema + "." + RelationNamespace.bareName(stmt.name()), newOwner);
        }
        if (stmt.action() == AlterViewStmt.Action.SET_OPTIONS) {
            Database.ViewDef existing = executor.database.getView(stmt.name());
            if (existing == null || existing.materialized() != stmt.materialized()) {
                if (existing == null && stmt.ifExists()) {
                    return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
                }
                requireViewKind(stmt);
                throw new MemgresException("view \"" + stmt.name() + "\" does not exist", "42P01");
            }
            // Merge new options into existing reloptions
            Map<String, String> merged = new LinkedHashMap<>();
            if (existing.reloptions() != null) merged.putAll(existing.reloptions());
            if (stmt.setOptions() != null) merged.putAll(stmt.setOptions());
            executor.database.removeView(stmt.name());
            executor.database.addView(new Database.ViewDef(existing.name(), existing.schemaName(), existing.query(),
                    existing.orReplace(), existing.materialized(),
                    existing.cachedColumns(), existing.cachedRows(), existing.sourceSQL(),
                    existing.checkOption(), merged, existing.populated()));
        }
        if (stmt.action() == AlterViewStmt.Action.NO_OP) {
            Database.ViewDef existing = executor.database.getView(stmt.name());
            if (existing == null || existing.materialized() != stmt.materialized()) {
                if (existing == null && stmt.ifExists()) {
                    return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
                }
                requireViewKind(stmt);
                throw new MemgresException("view \"" + stmt.name() + "\" does not exist", "42P01");
            }
        }
        return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
    }

    /**
     * Refuse an ALTER VIEW that named a relation of another kind, naming the kind the statement
     * expected. A table an ALTER VIEW names is not a missing view, it is a table — PostgreSQL
     * answers {@code 42809 "x" is not a view}, and {@code 42P01} sent the reader looking for
     * something that is right there under the name they wrote.
     */
    private void requireViewKind(AlterViewStmt stmt) {
        String bare = RelationNamespace.bareName(stmt.name());
        String written = SchemaQualifier.qualifierOf(stmt.name());
        String schema = written != null ? written
                : RelationNamespace.schemaHolding(executor.database,
                        executor.relationSearchPath(), bare);
        if (schema == null) schema = executor.defaultSchema();
        RelationNamespace.requireKind(executor.database, schema, bare,
                stmt.materialized() ? RelationNamespace.MATVIEW : RelationNamespace.VIEW);
    }

    // ---- REFRESH MATERIALIZED VIEW ----

    /** True when the view carries a unique index that CONCURRENTLY can match rows through. */
    private boolean hasUniqueIndexForConcurrentRefresh(Database.ViewDef view) {
        for (Map.Entry<String, String> e : executor.database.getIndexTableNames().entrySet()) {
            String owner = e.getValue();
            if (owner == null) continue;
            // Index metadata records the table schema-qualified.
            String bareOwner = owner.contains(".")
                    ? owner.substring(owner.lastIndexOf('.') + 1) : owner;
            if (!view.name().equalsIgnoreCase(bareOwner)) continue;
            String indexName = e.getKey();
            // A partial index does not cover every row, so it cannot identify them all.
            if (executor.database.isUniqueIndex(indexName)
                    && executor.database.getIndexWhereClause(indexName) == null) {
                return true;
            }
        }
        return false;
    }

    QueryResult executeRefreshMaterializedView(RefreshMaterializedViewStmt stmt) {
        Database.ViewDef view = executor.database.getView(stmt.name());
        if (view == null) {
            // A table of that name is the wrong kind of relation, not a missing one. PostgreSQL
            // lets a table through its permission check and then refuses it as not a
            // materialized view; a sequence or an index never gets that far.
            if (ddl.resolveTableOrNull(stmt.name()) != null) {
                throw PgErrors.notImplemented("\"" + stmt.name() + "\" is not a materialized view");
            }
            if (RelationNamespace.kindOf(executor.database, executor.defaultSchema(),
                    stmt.name()) != null) {
                throw PgErrors.wrongObjectType("\"" + RelationNamespace.bareName(stmt.name())
                        + "\" is not a table or materialized view");
            }
            throw new MemgresException("relation \"" + stmt.name() + "\" does not exist", "42P01");
        }
        if (!view.materialized()) {
            throw PgErrors.wrongObjectType("\"" + RelationNamespace.bareName(stmt.name())
                    + "\" is not a table or materialized view");
        }
        if (stmt.concurrently()) {
            // CONCURRENTLY replaces rows one by one instead of swapping the whole relation, so
            // it needs a unique index to tell which stored row each new row corresponds to.
            if (!stmt.withData()) {
                // Two options that contradict each other is a fault in the statement itself.
                throw PgErrors.syntax(
                        "CONCURRENTLY and WITH NO DATA options cannot be used together");
            }
            if (!hasUniqueIndexForConcurrentRefresh(view)) {
                String qualified = (view.schemaName() != null ? view.schemaName() : "public")
                        + "." + view.name();
                MemgresException e = new MemgresException("cannot refresh materialized view \""
                        + qualified + "\" concurrently", "55000");
                e.setHint("Create a unique index with no WHERE clause on one or more columns"
                        + " of the materialized view.");
                throw e;
            }
        }
        if (!stmt.withData()) {
            // REFRESH ... WITH NO DATA: discard rows and mark the view unpopulated again
            List<Column> cols = view.cachedColumns();
            if (cols == null || cols.isEmpty()) {
                try {
                    cols = new ArrayList<>(executor.executeStatement(view.query()).getColumns());
                } catch (Exception e) {
                    cols = Cols.listOf();
                }
            }
            executor.database.addView(new Database.ViewDef(view.name(), view.schemaName(), view.query(),
                    view.orReplace(), true, cols, Cols.listOf(), view.sourceSQL(),
                    view.checkOption(), view.reloptions(), false));
            return QueryResult.message(QueryResult.Type.SET, "REFRESH MATERIALIZED VIEW");
        }
        QueryResult result = executor.executeStatement(view.query());
        List<Column> cols = new ArrayList<>(result.getColumns());
        List<Object[]> rows = new ArrayList<>(result.getRows());
        executor.database.addView(new Database.ViewDef(view.name(), view.schemaName(), view.query(), view.orReplace(),
                true, cols, rows, view.sourceSQL(), view.checkOption(), view.reloptions(), true));
        return QueryResult.message(QueryResult.Type.SET, "REFRESH MATERIALIZED VIEW");
    }
}
