package com.memgres.engine;

import com.memgres.engine.parser.ast.*;

import java.util.*;

/**
 * Handles ALTER TABLE execution.
 * Extracted from DdlExecutor to separate concerns.
 */
class DdlAlterTableExecutor {

    /**
     * A VIRTUAL generated column is computed on read and never stored, so there is nothing for an
     * index to hold: PostgreSQL refuses a primary key or a unique constraint over one. The column
     * form is already refused where the table is created; this is the ALTER TABLE path.
     */
    private void rejectKeyOnVirtualColumn(Table table, java.util.List<String> columns,
                                          String what) {
        if (columns == null) return;
        for (String name : columns) {
            if (name == null) continue;
            int idx = table.getColumnIndex(name);
            if (idx < 0) continue;
            Column col = table.getColumns().get(idx);
            if (col.isVirtual()) {
                throw new MemgresException(
                        what + " on virtual generated columns are not supported", "0A000");
            }
        }
    }

    private final DdlExecutor ddl;
    private final AstExecutor executor;

    /**
     * VOLATILE built-in functions whose DEFAULT must be evaluated once per existing row when
     * added via ALTER TABLE ADD COLUMN (PG rewrites the table, calling the default per row).
     * STABLE functions (now(), current_timestamp, statement_timestamp()) are intentionally
     * absent: one value per statement is correct for them.
     */
    private static final java.util.Set<String> PER_ROW_VOLATILE_FUNCTIONS = com.memgres.engine.util.Cols.setOf(
            "random", "random_normal", "setseed", "gen_random_uuid", "uuid_generate_v4",
            "uuid_generate_v1", "uuidv4", "uuidv7", "gen_random_bytes", "nextval",
            "clock_timestamp", "timeofday"
    );

    DdlAlterTableExecutor(DdlExecutor ddl) {
        this.ddl = ddl;
        this.executor = ddl.executor;
    }

    /**
     * True when the expression really calls one of {@link #PER_ROW_VOLATILE_FUNCTIONS}.
     *
     * <p>Asked of the parse tree rather than of the text the default was written as. A string
     * literal that happens to spell a call — {@code DEFAULT 'random(9)'} — is a value, not a
     * function, and taking it for one sent the column down the per-row path, where nothing checks
     * the value against the column's own declaration.
     */
    private static boolean hasVolatileFunction(Expression expr) {
        if (expr == null) return false;
        return AstWalk.anyMatch(expr, new java.util.function.Predicate<Object>() {
            @Override public boolean test(Object n) {
                if (!(n instanceof FunctionCallExpr)) return false;
                String fname = ((FunctionCallExpr) n).name;
                if (fname == null) return false;
                int dot = fname.lastIndexOf('.');
                return PER_ROW_VOLATILE_FUNCTIONS.contains(
                        (dot >= 0 ? fname.substring(dot + 1) : fname).toLowerCase(java.util.Locale.ROOT));
            }
        });
    }

    QueryResult executeAlterTable(AlterTableStmt stmt) {
        // ALTER opens the relation it names, and opening one starts by finding its schema. IF
        // EXISTS says not to mind a relation that is not there, and a relation in a schema that is
        // not there is not there either: PostgreSQL raises a notice and does nothing, the same as
        // for a missing relation in a schema it found.
        if (stmt.ifExists()
                && !SchemaQualifier.exists(executor.database, executor.session, stmt.schema())) {
            return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
        }
        SchemaQualifier.requireSchema(executor.database, executor.session, stmt.schema());
        String schemaName = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        requireWrittenKind(stmt, schemaName);
        rejectCompositeTypeTarget(schemaName, stmt.table());
        rejectActionsOnOtherRelationKinds(stmt, schemaName);
        QueryResult indexResult = alterIndexRelation(stmt, schemaName);
        if (indexResult != null) return indexResult;
        QueryResult viewResult = alterViewRelation(stmt, schemaName);
        if (viewResult != null) return viewResult;
        Table table;
        try {
            table = executor.resolveTable(schemaName, stmt.table());
        } catch (MemgresException e) {
            if (stmt.ifExists()) return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
            throw e;
        }

        // And it is the owner who reshapes it.
        executor.requireTableOwner(schemaName, stmt.table());

        // ALTER TABLE takes ACCESS EXCLUSIVE, so it waits behind any open reader
        if (executor.session != null) {
            executor.database.acquireTableLock(schemaName + "." + stmt.table(),
                    "AccessExclusiveLock", executor.session, false);
        }

        List<AlterTableStmt.AlterAction> ordered = orderedActions(stmt.actions());
        rejectSystemColumnActions(ordered);
        rejectBeforeApplying(ordered, table, stmt);
        for (AlterTableStmt.AlterAction action : ordered) {
            table = executeAction(action, table, stmt, schemaName);
        }

        // The session's own DDL is visible to it, so its snapshot of the old shape must go
        if (executor.session != null) {
            executor.session.discardRRSnapshotForTable(schemaName + "." + stmt.table());
        }

        return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
    }

    /**
     * A composite type owns a {@code pg_class} row, so ALTER TABLE finds it — and then refuses it,
     * because what a composite is made of is ALTER TYPE's business. PostgreSQL names the bare
     * relation even when the statement qualified it, and points the reader at the right statement.
     */
    /**
     * ALTER VIEW and ALTER MATERIALIZED VIEW hand their remaining actions to this path, but
     * PostgreSQL opens the relation under the kind its keyword named: a table an ALTER VIEW
     * reaches is {@code 42809 "t" is not a view} before the action is looked at, and IF EXISTS
     * does not soften it — that excuses a relation which is not there, not one of another kind.
     * ALTER VIEW ... RENAME COLUMN is the exception PostgreSQL makes (it renames a column of any
     * relation kind), and it is parsed elsewhere, so it never reaches here.
     */
    private void requireWrittenKind(AlterTableStmt stmt, String schemaName) {
        if (stmt.writtenKind() == null || stmt.table() == null) return;
        String bare = RelationNamespace.bareName(stmt.table());
        RelationNamespace.requireKind(executor.database,
                relationSchemaFor(stmt, schemaName, bare), bare, stmt.writtenKind());
    }

    private void rejectCompositeTypeTarget(String schemaName, String written) {
        if (written == null) return;
        String bare = RelationNamespace.bareName(written);
        if (!RelationNamespace.COMPOSITE.equals(
                RelationNamespace.kindOf(executor.database, schemaName, bare))) {
            return;
        }
        MemgresException e = PgErrors.wrongObjectType("\"" + bare + "\" is a composite type");
        e.setHint("Use ALTER TYPE instead.");
        throw e;
    }

    /**
     * A multi-action ALTER TABLE is one statement about the table's final shape, not a script.
     * PostgreSQL settles that shape in passes — everything dropped first, then column types,
     * then new columns, then the constraints over them — which is why
     * {@code ADD CONSTRAINT ck CHECK (b > 0), ADD COLUMN b int} is accepted although the
     * constraint is written before the column it reads. Running the list in written order
     * refuses that, and quietly accepts {@code ADD COLUMN c, DROP COLUMN c}, which PostgreSQL
     * refuses because its drop pass looks for a column that is not there yet.
     *
     * <p>The sort is stable, so two actions in the same pass keep the order they were written in.
     */
    private static List<AlterTableStmt.AlterAction> orderedActions(
            List<AlterTableStmt.AlterAction> actions) {
        if (actions == null || actions.size() < 2) return actions;
        List<AlterTableStmt.AlterAction> sorted = new ArrayList<>(actions);
        sorted.sort(java.util.Comparator.comparingInt(DdlAlterTableExecutor::actionPass));
        return sorted;
    }

    /**
     * The system columns are the relation's, not the definer's: they are there because the row is
     * stored, and no ALTER TABLE can drop, rename or redefine one.
     *
     * <p>PostgreSQL says so directly rather than reporting the column as missing, and says it for
     * {@code DROP COLUMN IF EXISTS} too -- the column is very much there, which is the reason it
     * cannot go. Adding one to a key is refused where the index would be built, since what
     * PostgreSQL says there depends on the type. See {@link DdlDefinitionChecks}.
     */
    private static void rejectSystemColumnActions(List<AlterTableStmt.AlterAction> actions) {
        if (actions == null) return;
        for (AlterTableStmt.AlterAction action : actions) {
            if (action instanceof AlterTableStmt.DropColumn) {
                refuse("drop", ((AlterTableStmt.DropColumn) action).column());
            } else if (action instanceof AlterTableStmt.RenameColumn) {
                refuse("rename", ((AlterTableStmt.RenameColumn) action).oldName());
            } else if (action instanceof AlterTableStmt.AlterColumn) {
                refuse("alter", ((AlterTableStmt.AlterColumn) action).column());
            }
        }
    }

    private static void refuse(String verb, String column) {
        if (!DdlDefinitionChecks.isSystemColumnName(column)) return;
        throw new MemgresException(
                "cannot " + verb + " system column \"" + column.toLowerCase(java.util.Locale.ROOT) + "\"", "0A000");
    }

    /**
     * ALTER TABLE is one statement, so an action that cannot succeed takes the whole statement
     * with it and leaves the table as it was. memgres applies the actions one at a time, so the
     * refusals a later action would raise are looked for first, over the shape the statement is
     * going to produce — otherwise the earlier actions stay applied and the table is left in a
     * state nobody wrote.
     */
    private void rejectBeforeApplying(List<AlterTableStmt.AlterAction> actions, Table table,
                                      AlterTableStmt stmt) {
        if (actions == null || actions.size() < 2) return;
        Set<String> dropped = new HashSet<>();
        for (AlterTableStmt.AlterAction action : actions) {
            if (action instanceof AlterTableStmt.DropColumn) {
                dropped.add(((AlterTableStmt.DropColumn) action).column().toLowerCase(java.util.Locale.ROOT));
            }
        }
        for (AlterTableStmt.AlterAction action : actions) {
            if (action instanceof AlterTableStmt.AddConstraint) {
                TableConstraint tc = ((AlterTableStmt.AddConstraint) action).constraint();
                for (String col : constraintColumnNames(tc)) {
                    if (dropped.contains(col.toLowerCase(java.util.Locale.ROOT))) {
                        throw new MemgresException("column \"" + col + "\" does not exist", "42703");
                    }
                }
            }
            if (!(action instanceof AlterTableStmt.AlterColumn)) continue;
            AlterTableStmt.AlterColumn alterCol = (AlterTableStmt.AlterColumn) action;
            if (!(alterCol.action() instanceof AlterTableStmt.SetNotNull)) continue;
            int idx = table.getColumnIndex(alterCol.column());
            if (idx < 0) continue;
            for (Object[] row : table.getRows()) {
                if (row[idx] == null) {
                    throw PgErrors.columnContainsNulls(alterCol.column(), "relation", stmt.table());
                }
            }
        }
    }

    /** Every column a constraint names, whether in its key list or inside its CHECK expression. */
    private static List<String> constraintColumnNames(TableConstraint tc) {
        List<String> names = new ArrayList<>();
        if (tc.columns() != null) {
            for (String c : tc.columns()) {
                if (c != null && !c.startsWith("__")) names.add(c);
            }
        }
        AstWalk.forEach(tc.checkExpr(), node -> {
            if (node instanceof ColumnRef && ((ColumnRef) node).table() == null) {
                names.add(((ColumnRef) node).column());
            }
        });
        return names;
    }

    /** Which pass an action belongs to; lower runs first. Mirrors PostgreSQL's AT_PASS_* order. */
    private static int actionPass(AlterTableStmt.AlterAction action) {
        if (action instanceof AlterTableStmt.DropColumn
                || action instanceof AlterTableStmt.DropConstraint) {
            return 0;
        }
        if (action instanceof AlterTableStmt.AlterColumn) {
            AlterTableStmt.AlterColumnAction inner = ((AlterTableStmt.AlterColumn) action).action();
            // Every DROP is one pass, the first: PostgreSQL puts DROP DEFAULT and DROP NOT NULL
            // in AT_PASS_DROP with the rest, so SET DEFAULT 11, DROP DEFAULT ends with 11.
            if (inner instanceof AlterTableStmt.DropIdentity
                    || inner instanceof AlterTableStmt.DropExpression
                    || inner instanceof AlterTableStmt.DropDefault
                    || inner instanceof AlterTableStmt.DropNotNull) {
                return 0;
            }
            if (inner instanceof AlterTableStmt.SetType) return 1;
            if (inner instanceof AlterTableStmt.SetNotNull) return 4;
            return 5;
        }
        if (action instanceof AlterTableStmt.AddColumn) return 2;
        if (action instanceof AlterTableStmt.AddConstraint) return 3;
        return 6;
    }

    /**
     * Rename actions aimed at a view have to change the view definition, which is what the
     * catalog reads. Routing them through the table path renamed a shadow relation instead,
     * leaving the view registered under its old name and its columns untouched.
     *
     * @return the result when the statement targeted a view, or null to continue as a table
     */
    private QueryResult alterViewRelation(AlterTableStmt stmt, String schemaName) {
        String bare = RelationNamespace.bareName(stmt.table());
        // The single-argument lookup answers a qualified name with public's view of that name, so
        // ALTER TABLE s.v renamed some other schema's relation and never touched the one named.
        Database.ViewDef view = executor.database.getView(relationSchemaFor(stmt, schemaName, bare), bare);
        if (view == null) return null;
        recordViewColumnDefaults(view, stmt);
        for (AlterTableStmt.AlterAction action : stmt.actions()) {
            // A column default is settled above, against the view itself. Sending it on to the
            // ordinary path as well resolved the view to the relation underneath and wrote the
            // default there, giving a table nobody altered a default of its own. Anything this
            // path does not handle is still left to that one.
            if (!(action instanceof AlterTableStmt.RenameTable
                    || action instanceof AlterTableStmt.RenameColumn
                    || action instanceof AlterTableStmt.SetSchema
                    || action instanceof AlterTableStmt.SetStorageParams
                    || action instanceof AlterTableStmt.ResetStorageParams
                    || isColumnDefaultAction(action))) {
                return null;
            }
        }
        for (AlterTableStmt.AlterAction action : stmt.actions()) {
            if (action instanceof AlterTableStmt.RenameTable) {
                String newName = ((AlterTableStmt.RenameTable) action).newName();
                String viewSchema = view.schemaName() != null
                        ? view.schemaName() : executor.defaultSchema();
                RelationNamespace.requireFree(executor.database, viewSchema, newName, null);
                // The new name has to be free in the schema this view lives in; asking whether any
                // schema holds a view of that name refused renames PostgreSQL allows.
                if (executor.database.getView(viewSchema, newName) != null) {
                    throw new MemgresException("relation \"" + newName + "\" already exists", "42P07");
                }
                executor.database.removeView(viewSchema, bare);
                executor.database.addView(withViewName(view, newName, view.cachedColumns()));
                executor.identity().relationRenamed(view.materialized() ? "m" : "v",
                        viewSchema, bare, viewSchema, newName);
                view = executor.database.getView(viewSchema, newName);
                bare = newName;
            } else if (action instanceof AlterTableStmt.RenameColumn) {
                AlterTableStmt.RenameColumn rc = (AlterTableStmt.RenameColumn) action;
                int ordinal = viewColumnOrdinal(view, rc.oldName());
                List<Column> renamed = renameViewColumn(view, rc.oldName(), rc.newName(), bare);
                // The catalog is not the only thing that has to take the rename: a view is read
                // by running its query, so the query's own output label is what a later
                // SELECT resolves against. Renaming only the catalog left the two disagreeing,
                // and the new name was rejected by the very view that advertised it.
                Statement relabelled = relabelViewOutput(view.query(), ordinal, rc.newName(),
                        renamed.size());
                executor.database.addView(new Database.ViewDef(bare, view.schemaName(), relabelled,
                        view.orReplace(), view.materialized(), renamed, view.cachedRows(),
                        view.sourceSQL(), view.checkOption(), view.reloptions(), view.populated()));
                view = executor.database.getView(view.schemaName() != null
                        ? view.schemaName() : executor.defaultSchema(), bare);
            } else if (action instanceof AlterTableStmt.SetSchema) {
                String target = ((AlterTableStmt.SetSchema) action).newSchema();
                if (executor.database.getSchema(target) == null) {
                    throw new MemgresException("schema \"" + target + "\" does not exist", "3F000");
                }
                String viewSchema = view.schemaName() != null
                        ? view.schemaName() : executor.defaultSchema();
                executor.database.removeView(viewSchema, bare);
                executor.database.addView(new Database.ViewDef(view.name(), target, view.query(),
                        view.orReplace(), view.materialized(), view.cachedColumns(),
                        view.cachedRows(), view.sourceSQL(), view.checkOption(),
                        view.reloptions(), view.populated()));
                executor.identity().relationRenamed(view.materialized() ? "m" : "v",
                        viewSchema, bare, target, bare);
                view = executor.database.getView(target, bare);
            } else if (action instanceof AlterTableStmt.SetStorageParams) {
                AlterTableStmt.SetStorageParams set = (AlterTableStmt.SetStorageParams) action;
                if (!set.reloptions()) continue;
                Map<String, String> merged = view.reloptions() == null
                        ? new LinkedHashMap<String, String>()
                        : new LinkedHashMap<String, String>(view.reloptions());
                for (Map.Entry<String, String> opt : set.params().entrySet()) {
                    merged.put(opt.getKey(), DdlIndexValidator.normalizeRelOptionValue(
                            "heap", opt.getKey(), opt.getValue()));
                }
                view = replaceViewReloptions(view, bare, merged);
            } else if (action instanceof AlterTableStmt.ResetStorageParams) {
                // A view carries storage parameters like any other relation, so RESET takes them
                // off it. PostgreSQL accepts a name that was never set and reports NULL rather
                // than an empty list once the last one is gone.
                AlterTableStmt.ResetStorageParams reset = (AlterTableStmt.ResetStorageParams) action;
                if (view.reloptions() == null) continue;
                Map<String, String> remaining = new LinkedHashMap<String, String>(view.reloptions());
                for (String name : reset.names()) remaining.remove(name);
                view = replaceViewReloptions(view, bare, remaining.isEmpty() ? null : remaining);
            }
        }
        return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
    }

    /** The same view with another set of storage parameters, put back under its own name. */
    private Database.ViewDef replaceViewReloptions(Database.ViewDef view, String bare,
                                                   Map<String, String> reloptions) {
        String viewSchema = view.schemaName() != null ? view.schemaName() : executor.defaultSchema();
        executor.database.addView(new Database.ViewDef(view.name(), view.schemaName(), view.query(),
                view.orReplace(), view.materialized(), view.cachedColumns(), view.cachedRows(),
                view.sourceSQL(), view.checkOption(), reloptions, view.populated()));
        return executor.database.getView(viewSchema, bare);
    }

    private List<Column> renameViewColumn(Database.ViewDef view, String oldName, String newName,
                                           String relation) {
        List<Column> cols = view.cachedColumns();
        if (cols == null) {
            throw new MemgresException("column \"" + oldName + "\" of relation \"" + relation
                    + "\" does not exist", "42703");
        }
        List<Column> out = new ArrayList<>();
        boolean found = false;
        for (Column c : cols) {
            if (c.getName().equalsIgnoreCase(newName)) {
                throw new MemgresException("column \"" + newName + "\" of relation \"" + relation
                        + "\" already exists", "42701");
            }
        }
        for (Column c : cols) {
            if (c.getName().equalsIgnoreCase(oldName)) {
                found = true;
                out.add(c.withName(newName));
            } else {
                out.add(c);
            }
        }
        if (!found) {
            throw new MemgresException("column \"" + oldName + "\" of relation \"" + relation
                    + "\" does not exist", "42703");
        }
        return out;
    }

    /**
     * ALTER TABLE names any relation, and on an index the only actions that mean anything are
     * the ones about its name and its owner — which PostgreSQL runs rather than refusing, so
     * {@code ALTER TABLE i RENAME TO j} really renames the index.
     */
    private QueryResult alterIndexRelation(AlterTableStmt stmt, String schemaName) {
        String bare = RelationNamespace.bareName(stmt.table());
        Schema schema = executor.database.getSchema(schemaName);
        if (schema != null && schema.getTable(bare) != null) return null;
        if (executor.database.getView(bare) != null) return null;
        if (!executor.database.hasIndex(schemaName, bare)) return null;
        for (AlterTableStmt.AlterAction action : stmt.actions()) {
            if (action instanceof AlterTableStmt.RenameTable) {
                String newName = ((AlterTableStmt.RenameTable) action).newName();
                RelationNamespace.requireFree(executor.database, schemaName, newName, null);
                executor.database.renameIndex(Database.idxKey(schemaName, bare), newName);
                executor.identity().relationRenamed("i", schemaName, bare, schemaName, newName);
            }
            // OWNER TO is accepted; memgres records no per-index owner to change.
        }
        return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
    }

    /** The position of a view's column, or -1 when the view has no cached column list. */
    private static int viewColumnOrdinal(Database.ViewDef view, String column) {
        List<Column> cols = view.cachedColumns();
        if (cols == null) return -1;
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).getName().equalsIgnoreCase(column)) return i;
        }
        return -1;
    }

    /**
     * Put the new name on the view query's own output column, so the query answers to it. Only a
     * plain select list of the same length can be relabelled by position — a wildcard or a set
     * operation has no one target to write the name onto, and is left as it was rather than
     * relabelled wrongly.
     */
    private static Statement relabelViewOutput(Statement query, int ordinal, String newName,
                                               int columnCount) {
        if (ordinal < 0 || !(query instanceof SelectStmt)) return query;
        SelectStmt s = (SelectStmt) query;
        List<SelectStmt.SelectTarget> targets = s.targets();
        if (targets == null || targets.size() != columnCount || ordinal >= targets.size()) return query;
        for (SelectStmt.SelectTarget t : targets) {
            if (t.expr() instanceof WildcardExpr) return query;
        }
        List<SelectStmt.SelectTarget> out = new ArrayList<>();
        for (int i = 0; i < targets.size(); i++) {
            SelectStmt.SelectTarget t = targets.get(i);
            out.add(i == ordinal ? new SelectStmt.SelectTarget(t.expr(), newName) : t);
        }
        return new SelectStmt(s.distinct(), s.distinctOn(), out, s.from(), s.where(), s.groupBy(),
                s.having(), s.windowDefs(), s.orderBy(), s.limit(), s.offset(), s.withClauses(),
                s.groupingSets(), s.lockClause(), s.withTies());
    }

    private Database.ViewDef withViewName(Database.ViewDef view, String name, List<Column> cols) {
        return new Database.ViewDef(name, view.schemaName(), view.query(), view.orReplace(),
                view.materialized(), cols, view.cachedRows(), view.sourceSQL(),
                view.checkOption(), view.reloptions(), view.populated());
    }

    /**
     * ALTER TABLE reaches views, materialized views and sequences too, but only for the actions
     * that are about the relation's name or ownership. Anything that would reshape stored rows
     * has no meaning on those, and PostgreSQL names the offending action when it refuses.
     */
    private void rejectActionsOnOtherRelationKinds(AlterTableStmt stmt, String schemaName) {
        String bare = RelationNamespace.bareName(stmt.table());
        String ownSchemaName = relationSchemaFor(stmt, schemaName, bare);
        Database.ViewDef view = executor.database.getView(ownSchemaName, bare);
        boolean isSequence = view == null && executor.database.hasSequence(ownSchemaName, bare);
        Schema ownSchema = executor.database.getSchema(ownSchemaName);
        boolean isIndex = view == null && !isSequence
                && (ownSchema == null || ownSchema.getTable(bare) == null)
                && executor.database.hasIndex(ownSchemaName, bare);
        if (view == null && !isSequence && !isIndex) return;
        for (AlterTableStmt.AlterAction action : stmt.actions()) {
            // An index has no schema of its own to change: it always lives where its table does,
            // and PostgreSQL points at the table as the thing whose schema can be changed.
            if (isIndex && action instanceof AlterTableStmt.SetSchema) {
                MemgresException moved = PgErrors.wrongObjectType(
                        "cannot change schema of index \"" + bare + "\"");
                moved.setHint("Change the schema of the table instead.");
                throw moved;
            }
            if (action instanceof AlterTableStmt.RenameTable
                    || action instanceof AlterTableStmt.SetSchema
                    || action instanceof AlterTableStmt.OwnerTo) {
                continue;
            }
            // Renaming a column, giving one a default that INSERT through the view can use, and
            // setting or clearing the relation's storage parameters are all meaningful for a
            // view -- security_barrier and check_option are storage parameters -- but not for a
            // sequence.
            // A column default is one only an ordinary view carries: it stands in for the base
            // relation's while a write is rewritten through the view, and a materialized view
            // takes no writes at all, so PostgreSQL refuses it there as it does on a sequence.
            if (view != null && (action instanceof AlterTableStmt.RenameColumn
                    || action instanceof AlterTableStmt.SetStorageParams
                    || action instanceof AlterTableStmt.ResetStorageParams
                    || (isColumnDefaultAction(action) && !view.materialized()))) {
                continue;
            }
            MemgresException e = new MemgresException("ALTER action " + alterActionName(action)
                    + " cannot be performed on relation \"" + bare + "\"", "42809");
            // The kind that cannot carry the action goes on the detail line, so the reader is
            // told why the relation refuses it and not only that it did.
            e.setDetail(isSequence ? "This operation is not supported for sequences."
                    : isIndex ? "This operation is not supported for indexes."
                    : view.materialized() ? "This operation is not supported for materialized views."
                    : "This operation is not supported for views.");
            throw e;
        }
    }

    /**
     * The schema a written relation name reaches. A qualifier names one schema and no other; a
     * bare name is the search path's business, and PostgreSQL reaches the first relation on it.
     */
    private String relationSchemaFor(AlterTableStmt stmt, String schemaName, String bare) {
        String written = stmt.schema();
        if (written == null && stmt.table() != null && stmt.table().indexOf('.') >= 0) {
            written = stmt.table().substring(0, stmt.table().lastIndexOf('.'));
        }
        if (written != null) return written;
        String onPath = RelationNamespace.schemaHolding(executor.database,
                executor.relationSearchPath(), bare);
        return onPath != null ? onPath : schemaName;
    }

    /**
     * A default written on a view column belongs to the view and to nothing else: PostgreSQL files
     * it against the view's own relation and substitutes it only while a write through the view is
     * being rewritten onto the base relation, which is why the base relation's catalogue row stays
     * empty and an INSERT naming the table directly stores NULL.
     */
    private void recordViewColumnDefaults(Database.ViewDef view, AlterTableStmt stmt) {
        if (view.cachedColumns() == null) return;
        for (AlterTableStmt.AlterAction action : stmt.actions()) {
            if (!isColumnDefaultAction(action)) continue;
            AlterTableStmt.AlterColumn alterCol = (AlterTableStmt.AlterColumn) action;
            String written = alterCol.action() instanceof AlterTableStmt.SetDefault
                    ? DdlExecutor.exprToDefaultString(
                            ((AlterTableStmt.SetDefault) alterCol.action()).expr())
                    : null;
            boolean found = false;
            for (Column col : view.cachedColumns()) {
                if (col.getName().equalsIgnoreCase(alterCol.column())) {
                    col.setDefaultValue(written);
                    found = true;
                }
            }
            // The view is the relation the statement named, so a column the view does not publish
            // is the column that is not there; the relation underneath is never consulted for it.
            if (!found) {
                throw new MemgresException("column \"" + alterCol.column() + "\" of relation \""
                        + stmt.table() + "\" does not exist", "42703");
            }
        }
    }

    /** True for ALTER COLUMN ... SET/DROP DEFAULT, which a view accepts. */
    private static boolean isColumnDefaultAction(AlterTableStmt.AlterAction action) {
        if (!(action instanceof AlterTableStmt.AlterColumn)) return false;
        AlterTableStmt.AlterColumnAction inner = ((AlterTableStmt.AlterColumn) action).action();
        return inner instanceof AlterTableStmt.SetDefault
                || inner instanceof AlterTableStmt.DropDefault;
    }

    /** The action's name as PostgreSQL spells it when reporting an unsupported ALTER. */
    private static String alterActionName(AlterTableStmt.AlterAction action) {
        if (action instanceof AlterTableStmt.AddColumn) return "ADD COLUMN";
        if (action instanceof AlterTableStmt.DropColumn) return "DROP COLUMN";
        if (action instanceof AlterTableStmt.AddConstraint) return "ADD CONSTRAINT";
        if (action instanceof AlterTableStmt.DropConstraint) return "DROP CONSTRAINT";
        if (action instanceof AlterTableStmt.ValidateConstraint) return "VALIDATE CONSTRAINT";
        if (action instanceof AlterTableStmt.RenameConstraint) return "RENAME CONSTRAINT";
        if (action instanceof AlterTableStmt.AttachPartition) return "ATTACH PARTITION";
        if (action instanceof AlterTableStmt.DetachPartition) return "DETACH PARTITION";
        if (action instanceof AlterTableStmt.Inherit) return "INHERIT";
        if (action instanceof AlterTableStmt.NoInherit) return "NO INHERIT";
        if (action instanceof AlterTableStmt.EnableRls) return "ENABLE ROW SECURITY";
        if (action instanceof AlterTableStmt.DisableRls) return "DISABLE ROW SECURITY";
        if (action instanceof AlterTableStmt.ForceRls) return "FORCE ROW SECURITY";
        if (action instanceof AlterTableStmt.NoForceRls) return "NO FORCE ROW SECURITY";
        if (action instanceof AlterTableStmt.SetLogged) return "SET LOGGED";
        if (action instanceof AlterTableStmt.SetStorageParams) return "SET";
        if (action instanceof AlterTableStmt.ResetStorageParams) return "RESET";
        if (action instanceof AlterTableStmt.AlterColumn) {
            return "ALTER COLUMN ... " + alterColumnActionName(
                    ((AlterTableStmt.AlterColumn) action).action());
        }
        return "ALTER";
    }

    private static String alterColumnActionName(AlterTableStmt.AlterColumnAction action) {
        if (action instanceof AlterTableStmt.SetNotNull) return "SET NOT NULL";
        if (action instanceof AlterTableStmt.DropNotNull) return "DROP NOT NULL";
        if (action instanceof AlterTableStmt.SetDefault) return "SET DEFAULT";
        if (action instanceof AlterTableStmt.DropDefault) return "DROP DEFAULT";
        if (action instanceof AlterTableStmt.SetType) return "SET DATA TYPE";
        return "ALTER";
    }

    private Table executeAction(AlterTableStmt.AlterAction action, Table table,
                                 AlterTableStmt stmt, String schemaName) {
        if (action instanceof AlterTableStmt.AddColumn) {
            AlterTableStmt.AddColumn addCol = (AlterTableStmt.AddColumn) action;
            executeAddColumn(addCol, table, stmt, schemaName);
        } else if (action instanceof AlterTableStmt.DropColumn) {
            AlterTableStmt.DropColumn dropCol = (AlterTableStmt.DropColumn) action;
            executeDropColumn(dropCol, table, stmt, schemaName);
        } else if (action instanceof AlterTableStmt.RenameColumn) {
            AlterTableStmt.RenameColumn rename = (AlterTableStmt.RenameColumn) action;
            executeRenameColumn(rename, table, stmt, schemaName);
        } else if (action instanceof AlterTableStmt.SetReplicaIdentity) {
            table.setReplicaIdentity(((AlterTableStmt.SetReplicaIdentity) action).identity());
        } else if (action instanceof AlterTableStmt.RenameTable) {
            AlterTableStmt.RenameTable rename = (AlterTableStmt.RenameTable) action;
            table = executeRenameTable(rename, table, stmt, schemaName);
        } else if (action instanceof AlterTableStmt.OwnerTo) {
            AlterTableStmt.OwnerTo ownerTo = (AlterTableStmt.OwnerTo) action;
            String newOwner = ddl.resolveOwnerName(ownerTo.newOwner());
            if (!executor.database.hasRole(newOwner)) {
                throw new MemgresException("role \"" + newOwner + "\" does not exist", "42704");
            }
            executor.database.setObjectOwner("table:" + schemaName + "." + stmt.table(), newOwner);
        } else if (action instanceof AlterTableStmt.AlterColumn) {
            AlterTableStmt.AlterColumn alterCol = (AlterTableStmt.AlterColumn) action;
            executeAlterColumn(alterCol, table, stmt, schemaName);
        } else if (action instanceof AlterTableStmt.AddConstraint) {
            AlterTableStmt.AddConstraint addConstraint = (AlterTableStmt.AddConstraint) action;
            executeAddConstraint(addConstraint, table, stmt, schemaName);
        } else if (action instanceof AlterTableStmt.ValidateConstraint) {
            AlterTableStmt.ValidateConstraint vc = (AlterTableStmt.ValidateConstraint) action;
            executeValidateConstraint(vc, table, stmt);
        } else if (action instanceof AlterTableStmt.DropConstraint) {
            AlterTableStmt.DropConstraint dropConstraint = (AlterTableStmt.DropConstraint) action;
            StoredConstraint dropped = table.getConstraint(dropConstraint.name());
            String notNullColumn = dropped == null
                    ? notNullColumnNamed(table, dropConstraint.name()) : null;
            if (notNullColumn != null) {
                int nnIdx = table.getColumnIndex(notNullColumn);
                if (nnIdx >= 0 && table.getColumns().get(nnIdx).isPrimaryKey()) {
                    throw new MemgresException("column \"" + notNullColumn
                            + "\" is in a primary key", "42P16");
                }
                // The named spelling of DROP NOT NULL answers to the same rule as the column
                // spelling: the constraint belongs to whichever relation declared it.
                rejectDropNotNullUnderParent(table, notNullColumn, stmt.table(), true);
                recordNotNullUndo(table, notNullColumn);
                // What each child holds is settled while this relation still declares the rule, so
                // the name it answers to is still the one the constraint was created with.
                pinRetainedNotNullNames(table, notNullColumn);
                if (stmt.only()) makeNotNullLocalOnChildren(table, notNullColumn);
                // Dropping the constraint is what makes the column nullable again.
                table.alterColumnNullable(notNullColumn, true);
                table.setNotNullConstraintName(notNullColumn, null);
                if (!stmt.only()) clearNotNullOnDescendants(table, notNullColumn);
                return table;
            }
            if (!dropConstraint.ifExists() && dropped == null) {
                // PostgreSQL reads the constraint's name out of the parse tree rather than out of
                // the statement text, so it has no place in the text to point the reader at and
                // sends no Position at all. The same is true of every refusal below about a
                // constraint a relation holds for a parent.
                throw new MemgresException("constraint \"" + dropConstraint.name()
                        + "\" of relation \"" + stmt.table() + "\" does not exist", "42704")
                        .suppressPosition();
            }
            // A constraint a partition or a child carries because its parent declares it belongs
            // to the parent. Dropping it on the descendant alone would leave the descendant
            // taking rows the parent's own rule rejects, and a read through the parent would
            // return them, so PostgreSQL sends the writer to the relation that declared it.
            if (dropped != null
                    && CatalogConstraintBuilder.inheritedParentCount(table, dropped) > 0) {
                throw new MemgresException("cannot drop inherited constraint \""
                        + dropped.getName() + "\" of relation \"" + stmt.table() + "\"", "42P16")
                        .suppressPosition();
            }
            // The columns carry their own "in the primary key" flag, so dropping the constraint
            // has to clear it or the column keeps refusing DROP NOT NULL for a key that is gone.
            if (dropped != null && dropped.getType() == StoredConstraint.Type.PRIMARY_KEY) {
                for (String pkCol : dropped.getColumns()) {
                    int idx = table.getColumnIndex(pkCol);
                    if (idx >= 0) table.getColumns().get(idx).setPrimaryKey(false);
                }
            }
            table.removeConstraint(dropConstraint.name());
            // The copy a partition or an inheritance child holds is this constraint, not one of
            // its own, so it goes with it: PostgreSQL drops the descendants' copies too, and ONLY
            // asks instead that each of them keep it as its own from now on. A copy left behind
            // still marked as the parent's could be withdrawn from nowhere -- the parent no longer
            // knows the name, and a descendant may not drop what it inherited.
            if (stmt.only()) makeInheritedCopiesLocal(table, dropConstraint.name());
            else dropInheritedCopies(table, dropConstraint.name());
        } else if (action instanceof AlterTableStmt.EnableRls) {
            table.setRlsEnabled(true);
        } else if (action instanceof AlterTableStmt.DisableRls) {
            table.setRlsEnabled(false);
        } else if (action instanceof AlterTableStmt.ForceRls) {
            table.setRlsForced(true);
        } else if (action instanceof AlterTableStmt.NoForceRls) {
            table.setRlsForced(false);
        } else if (action instanceof AlterTableStmt.AttachPartition) {
            AlterTableStmt.AttachPartition attach = (AlterTableStmt.AttachPartition) action;
            executeAttachPartition(attach, table, stmt, schemaName);
        } else if (action instanceof AlterTableStmt.DetachPartition) {
            AlterTableStmt.DetachPartition detach = (AlterTableStmt.DetachPartition) action;
            requirePartitionedTable(table, stmt.table(), "DETACH PARTITION");
            String detachSchemaName = detach.partitionSchema() != null ? detach.partitionSchema() : schemaName;
            Table partition = executor.resolveTable(detachSchemaName, detach.partitionName());
            if (!table.getPartitions().contains(partition)) {
                throw new MemgresException("relation \"" + detach.partitionName()
                        + "\" is not a partition of relation \"" + stmt.table() + "\"", "42P01");
            }
            // The name a constraint answers to has to be written down while the link that leads
            // to it is still there; once it is gone there is nothing left to read the name from.
            pinInheritedNotNullNames(partition);
            table.removePartition(partition);
            partition.setPartitionParent(null);
            partition.clearPartitionBounds();
            // The indexes the partition holds stop being copies of the partitioned table's the
            // moment it leaves the hierarchy. Left tied to the parent's index they went on being
            // reported as belonging to a relation this one is no longer part of, and dropping the
            // parent's index would have taken an index on a standalone table with it.
            detachPartitionIndexes(partition, detachSchemaName);
            // A detached table keeps the constraints it carried, but they are its own from now on:
            // PostgreSQL records them as local, which is what lets the standalone table drop them.
            // Left marked as the partitioned table's, they could never be withdrawn and a
            // violation would go on naming a relation this one no longer belongs to.
            adoptInheritedConstraints(partition, table);
            adoptInheritedNotNulls(partition, table);
        } else if (action instanceof AlterTableStmt.RenameConstraint) {
            AlterTableStmt.RenameConstraint renameConstraint = (AlterTableStmt.RenameConstraint) action;
            StoredConstraint oldConstraint = table.getConstraint(renameConstraint.oldName());
            if (oldConstraint == null) {
                // A NOT NULL constraint is renamed the same way, though it lives on the column.
                String nnCol = table.notNullConstraintColumn(renameConstraint.oldName());
                if (nnCol != null) {
                    table.setNotNullConstraintName(nnCol, renameConstraint.newName());
                    return table;
                }
                // Without a SQLSTATE this inferred the bare class 42000, which no PostgreSQL
                // server sends. PostgreSQL says "for table" when a rename cannot find the
                // constraint and "of relation" when a drop cannot; the two are not interchangeable.
                throw new MemgresException("constraint \"" + renameConstraint.oldName()
                        + "\" for table \"" + stmt.table() + "\" does not exist", "42704");
            }
            table.removeConstraint(renameConstraint.oldName());
            StoredConstraint newConstraint = new StoredConstraint(
                    renameConstraint.newName(), oldConstraint.getType(), oldConstraint.getColumns(),
                    oldConstraint.getCheckExpr(), oldConstraint.getReferencesTable(),
                    oldConstraint.getReferencesColumns(), oldConstraint.getOnDelete(), oldConstraint.getOnUpdate());
            table.addConstraint(newConstraint);
        } else if (action instanceof AlterTableStmt.AlterConstraintAttrs) {
            executeAlterConstraint((AlterTableStmt.AlterConstraintAttrs) action, table, stmt);
        } else if (action instanceof AlterTableStmt.SetSchema) {
            AlterTableStmt.SetSchema setSchema = (AlterTableStmt.SetSchema) action;
            Schema oldSchema = executor.database.getSchema(schemaName);
            // Creating the destination on demand would move the table somewhere nothing can
            // name, which loses it outright.
            Schema newSchema = executor.database.getSchema(setSchema.newSchema());
            if (newSchema == null) {
                throw new MemgresException("schema \"" + setSchema.newSchema()
                        + "\" does not exist", "3F000");
            }
            // Everything that travels with the table needs a free name where it is going. The
            // whole move is refused when one of them is taken, because a move that overwrote the
            // destination's index or sequence destroyed an object the statement never named.
            requireNamesFreeInTarget(table, schemaName, setSchema.newSchema(), stmt.table());
            oldSchema.removeTable(stmt.table());
            newSchema.addTable(table);
            // The indexes are moved first: they are found by the schema-qualified name of the
            // table they were built on, which the table's own move rewrites.
            moveOwnedSequences(table, schemaName, setSchema.newSchema());
            moveOwnedIndexes(table, schemaName, setSchema.newSchema(), stmt.table());
            // Same object, new schema: the OID goes with it, and so does everything filed under
            // the qualified name it used to answer to.
            executor.identity().relationRenamed("r", schemaName, stmt.table(),
                    setSchema.newSchema(), stmt.table());
            carryComments(schemaName, stmt.table(), setSchema.newSchema(), stmt.table());
            retargetDependents(schemaName, stmt.table(), setSchema.newSchema(), stmt.table());
        } else if (action instanceof AlterTableStmt.Inherit) {
            AlterTableStmt.Inherit inherit = (AlterTableStmt.Inherit) action;
            Table parentTable = executor.resolveTable(schemaName, inherit.parentTable());
            // Circularity first: a loop is what PostgreSQL names even when the columns are also
            // wrong, and the walk covers a longer loop than a single parent check can see.
            rejectInheritanceCycle(table, parentTable, stmt.table(), inherit.parentTable());
            if (parentTable.getChildren().contains(table)) {
                throw new MemgresException("relation \"" + inherit.parentTable()
                        + "\" would be inherited from more than once", "42P07");
            }
            // A child has to be able to stand in for its parent, so every parent column must be
            // present with the same type before the link is made.
            validateInheritedColumns(parentTable, table, stmt.table());
            // ...and it has to carry the parent's CHECK constraints already. PostgreSQL adds no
            // rule to the child behind the writer's back, so a child that would enforce less than
            // its parent is refused rather than attached.
            requireInheritedChecks(parentTable, table);
            rejectNoInheritAgainstParent(parentTable, table);
            table.setParentTable(parentTable);
            parentTable.addChild(table);
        } else if (action instanceof AlterTableStmt.NoInherit) {
            AlterTableStmt.NoInherit noInherit = (AlterTableStmt.NoInherit) action;
            Table parentTable = executor.resolveTable(schemaName, noInherit.parentTable());
            if (!parentTable.getChildren().contains(table)) {
                throw new MemgresException("relation \"" + noInherit.parentTable()
                        + "\" is not a parent of relation \"" + stmt.table() + "\"", "42P01");
            }
            // Same as DETACH: a relation declared under two parents keeps the name the first one
            // gave the rule, and reading it afresh from the parent that is left would rename a
            // constraint PostgreSQL renames nowhere.
            pinInheritedNotNullNames(table);
            parentTable.removeChild(table);
            // Only the parent the statement named is let go: a child declared under two of them
            // goes on inheriting from the other, and breaking every link took that parent's
            // columns and constraints away from the child along with the one that was asked for.
            table.removeParentTable(parentTable);
            // Breaking the link leaves the CHECK constraints standing, as the child's own: a table
            // that inherits from nobody has nothing to inherit a rule from, and PostgreSQL records
            // each of them local so the child can drop them itself.
            adoptInheritedConstraints(table, parentTable);
            adoptInheritedNotNulls(table, parentTable);
        } else if (action instanceof AlterTableStmt.DisableTrigger) {
            AlterTableStmt.DisableTrigger dt = (AlterTableStmt.DisableTrigger) action;
            setTriggerEnabled(table, dt.triggerName(), "D", stmt.only());
        } else if (action instanceof AlterTableStmt.EnableTrigger) {
            AlterTableStmt.EnableTrigger et = (AlterTableStmt.EnableTrigger) action;
            setTriggerEnabled(table, et.triggerName(), et.state(), stmt.only());
        } else if (action instanceof AlterTableStmt.SetRuleEnabled) {
            AlterTableStmt.SetRuleEnabled sr = (AlterTableStmt.SetRuleEnabled) action;
            if (!executor.database.setRuleEnabledState(table.getSchemaName(), sr.ruleName(),
                    stmt.table(), sr.state())) {
                throw new MemgresException("rule \"" + sr.ruleName() + "\" for relation \""
                        + stmt.table() + "\" does not exist", "42704");
            }
        } else if (action instanceof AlterTableStmt.SetStorageParams) {
            AlterTableStmt.SetStorageParams setParams = (AlterTableStmt.SetStorageParams) action;
            // A partitioned table holds no rows of its own, so there is no storage for a storage
            // parameter to describe and PostgreSQL refuses the form outright.
            if (setParams.reloptions() && isPartitioned(table)) {
                MemgresException e = new MemgresException(
                        "cannot specify storage parameters for a partitioned table", "42809");
                // The rows live in the leaves, so the leaves are where a storage parameter has
                // anything to describe, and PostgreSQL sends the writer there.
                e.setHint("Specify storage parameters for its leaf partitions instead.");
                throw e;
            }
            // A parameter PostgreSQL does not recognise or a value it will not take stops the
            // statement here rather than being quietly accepted...
            if (table != null && !table.isViewProjection()) {
                DdlIndexValidator.checkRelOptions("heap", setParams.params());
                // ...and one it does take is stored, so pg_class.reloptions reports what was set.
                // Re-setting an option moves it to the end of the array, because PostgreSQL
                // removes the old entry and appends the new one; the SET TABLESPACE and SET
                // ACCESS METHOD forms carry no parameters and must change nothing.
                if (setParams.reloptions()) {
                    Map<String, String> merged = table.getReloptions() == null
                            ? new LinkedHashMap<String, String>()
                            : new LinkedHashMap<String, String>(table.getReloptions());
                    for (Map.Entry<String, String> opt : setParams.params().entrySet()) {
                        merged.remove(opt.getKey());
                        merged.put(opt.getKey(), DdlIndexValidator.normalizeRelOptionValue(
                                "heap", opt.getKey(), opt.getValue()));
                    }
                    setReloptionsWithUndo(table, merged);
                }
            }
        } else if (action instanceof AlterTableStmt.ResetStorageParams) {
            // PostgreSQL accepts RESET of an option that was never set, and reports NULL rather
            // than an empty array once the last one is gone -- the same answer as a table nobody
            // ever set an option on.
            AlterTableStmt.ResetStorageParams reset = (AlterTableStmt.ResetStorageParams) action;
            if (table != null && !table.isViewProjection() && table.getReloptions() != null) {
                Map<String, String> remaining =
                        new LinkedHashMap<String, String>(table.getReloptions());
                for (String name : reset.names()) remaining.remove(name);
                setReloptionsWithUndo(table, remaining.isEmpty() ? null : remaining);
            }
        } else if (action instanceof AlterTableStmt.SetWithoutCluster) {
            if (((AlterTableStmt.SetWithoutCluster) action).cluster() && isPartitioned(table)) {
                throw new MemgresException(
                        "cannot mark index clustered in partitioned table", "0A000");
            }
        } else if (action instanceof AlterTableStmt.ClusterOn) {
            String clusterIndex = ((AlterTableStmt.ClusterOn) action).indexName();
            requireIndexOfRelation(clusterIndex, table, stmt, schemaName);
            executor.database.setClusteredIndex(clusterIndex);
        } else if (action instanceof AlterTableStmt.SetLogged) {
            AlterTableStmt.SetLogged sl = (AlterTableStmt.SetLogged) action;
            table.setUnlogged(!sl.logged());
        }
        return table;
    }

    /**
     * CLUSTER ON names an index of the relation being altered. PostgreSQL says the index does not
     * exist when the name reaches none, and says it is not this relation's when the name reaches an
     * index of another -- so a statement that clustered on the wrong index is told which of the two
     * it got wrong. A key's own index answers to the constraint's name.
     */
    private void requireIndexOfRelation(String indexName, Table table, AlterTableStmt stmt,
                                        String schemaName) {
        if (indexName == null) return;
        if (table != null) {
            for (StoredConstraint sc : table.getConstraints()) {
                if ((sc.getType() == StoredConstraint.Type.PRIMARY_KEY
                        || sc.getType() == StoredConstraint.Type.UNIQUE)
                        && indexName.equalsIgnoreCase(sc.getName())) {
                    return;
                }
            }
        }
        if (!executor.database.hasIndex(schemaName, indexName)) {
            throw new MemgresException("index \"" + indexName + "\" for table \""
                    + stmt.table() + "\" does not exist", "42704");
        }
        String owner = executor.database.getIndexTable(indexName);
        String qualified = schemaName + "." + stmt.table();
        if (owner != null && !owner.equalsIgnoreCase(qualified)
                && !owner.equalsIgnoreCase(stmt.table())) {
            throw PgErrors.wrongObjectType("\"" + indexName + "\" is not an index for table \""
                    + stmt.table() + "\"");
        }
    }

    /**
     * Store a new storage-parameter map, remembering what was there first. DDL is transactional in
     * PostgreSQL, so a rolled-back SET or RESET has to leave pg_class.reloptions unchanged.
     */
    private void setReloptionsWithUndo(Table table, Map<String, String> updated) {
        Map<String, String> previous = table.getReloptions() == null ? null
                : new LinkedHashMap<String, String>(table.getReloptions());
        executor.recordUndo(new Session.SetReloptionsUndo(table, previous));
        table.setReloptions(updated);
    }

    private void executeAddColumn(AlterTableStmt.AddColumn addCol, Table table,
                                   AlterTableStmt stmt, String schemaName) {
        ColumnDef def = addCol.column();
        if (table.getColumnIndex(def.name()) >= 0) {
            if (addCol.ifNotExists()) return;
            throw new MemgresException("column \"" + def.name() + "\" of relation \"" + stmt.table() + "\" already exists", "42701");
        }
        rejectDirectPartitionColumnChange(table, "add column to a partition");
        // A typed table has exactly the columns its composite type declares. A column added here
        // would be one the type does not have, so PostgreSQL refuses it and sends the writer to
        // ALTER TYPE, which changes the shape of every table built on that type.
        if (table.getOfTypeName() != null) {
            throw PgErrors.wrongObjectType("cannot add column to typed table");
        }
        DdlDefinitionChecks.rejectSystemColumnName(def.name());
        // What the column's type is comes next: PostgreSQL settles the written type name while it
        // is still reading the definition, ahead of the collation, ahead of anything the DEFAULT
        // says and ahead of the rules about which tables the column has to be added to.
        DdlExecutor.ResolvedType resolved = ddl.resolveColumnType(def.typeName(), null);
        // A collation the database does not hold is not one a column can be declared with, and
        // PostgreSQL settles that where the clause is written rather than at the first comparison
        // -- resolving the name the clause holds before asking whether the type it was written on
        // carries a collation at all, which is the last thing it asks about the clause.
        DdlDefinitionChecks.requireCollationExists(executor.database, def.collation);
        DdlDefinitionChecks.rejectUncollatableType(def.typeName(), resolved, def.collation);
        DdlDefinitionChecks.validateDefaultExpression(def.defaultExpr());
        executor.selectExecutor.placementCheck.rejectStoredDefinition(
                def.defaultExpr(), "DEFAULT expressions", null);
        // A DEFAULT is read with no row in scope, so the only thing in it to resolve is the calls
        // it makes -- and PostgreSQL resolves those where the default is written rather than at
        // the first insert that takes it.
        StoredExprNames.read(ddl, def.defaultExpr(), null, null, false, false);
        // A child that lacks one of its parent's columns cannot stand in for the parent, so PG
        // refuses to add a column to a parent alone.
        if (stmt.only() && !childRelations(table).isEmpty()) {
            throw new MemgresException("column must be added to child tables too", "42P16");
        }
        // A column's own PRIMARY KEY clause makes a real constraint, and a table has at most one
        // primary key — so a second declaration is a fault in the statement, caught before the
        // column or its sequence is created rather than accepted and quietly not stored.
        if (def.primaryKey()) {
            for (StoredConstraint existing : table.getConstraints()) {
                if (existing.getType() == StoredConstraint.Type.PRIMARY_KEY) {
                    throw new MemgresException("multiple primary keys for table \""
                            + stmt.table() + "\" are not allowed", "42P16");
                }
            }
        }

        DataType dt = resolved.dataType();
        String enumTypeName = resolved.enumTypeName();
        String domainTypeName = resolved.domainTypeName();
        String compositeTypeName = resolved.compositeTypeName();
        DataType arrayElementType = resolved.arrayElementType();
        // And whatever the DEFAULT is, it has to produce a value this column can hold. PostgreSQL
        // settles that here, where the column is defined, rather than at the first row that takes
        // it -- by the same rule CREATE TABLE goes by, so a column means the same thing whichever
        // statement declared it. Left unjudged, a default the column could never hold was stored
        // and every INSERT that omitted the column failed on a value nobody wrote; and one that
        // happened to coerce, integer 1 into a boolean column, was taken outright.
        DdlDefinitionChecks.requireDefaultExprFits(def.defaultExpr(), resolved, def.name(),
                table.getColumns());

        String defaultVal = def.defaultExpr() != null ? DdlExecutor.exprToDefaultString(def.defaultExpr()) : null;
        // A domain carries a default of its own, and a column of that domain takes it when the
        // column declares none: PostgreSQL fills the rows already stored with it, and leaving them
        // NULL under a NOT NULL domain is a state it has no way to represent. It is kept apart from
        // defaultVal because the column gets no pg_attrdef entry of its own — measured against
        // PostgreSQL, information_schema.column_default stays empty while the rows still read 7.
        Object domainDefault = null;
        if (defaultVal == null && domainTypeName != null) {
            DomainType addDomain = executor.database.getDomain(domainTypeName);
            if (addDomain != null && addDomain.getDefaultValue() != null) {
                domainDefault = executor.evaluateDefault(addDomain.getDefaultValue(),
                        addDomain.getBaseType());
            }
        }
        String genExpr = def.generatedExpr();

        // SERIAL/BIGSERIAL/SMALLSERIAL: create a real sequence (same as CREATE TABLE)
        if (dt == DataType.SERIAL || dt == DataType.BIGSERIAL || dt == DataType.SMALLSERIAL) {
            if (defaultVal == null && def.identity() == null) {
                String seqName = stmt.table() + "_" + def.name() + "_seq";
                Sequence seq = new Sequence(seqName, null, null, null, null);
                seq.setSchemaName(schemaName);
                // The sequence belongs to this column and dies with it.
                seq.ownedBy(stmt.table(), def.name(), true);
                executor.database.addSequence(seq);
                executor.database.registerSchemaObject(schemaName, "sequence", seqName);
                defaultVal = "nextval('" + seqName + "'::regclass)";
            }
        }

        // GENERATED AS IDENTITY on ADD COLUMN
        if (def.identity() != null) {
            // Identity is fed by a sequence, and so is the serial shorthand, so a column cannot be
            // both -- and a DEFAULT written beside an identity is the same contradiction.
            // PostgreSQL refuses the definition rather than letting one of the two decide, and
            // CREATE TABLE goes by the same rule, so a column means the same thing whichever
            // statement declared it. Left unjudged, a column added this way came out carrying an
            // identity over a type that already had a sequence of its own.
            if (def.defaultExpr() != null || dt == DataType.SERIAL || dt == DataType.BIGSERIAL
                    || dt == DataType.SMALLSERIAL) {
                throw PgErrors.syntax("both default and identity specified for column \""
                        + def.name() + "\" of table \"" + stmt.table() + "\"");
            }
            // SEQUENCE NAME names the relation that feeds the column; without one the name is
            // composed the way PostgreSQL composes it.
            String writtenSeq = def.identitySequenceName();
            int seqDot = writtenSeq == null ? -1 : writtenSeq.lastIndexOf('.');
            String seqSchema = seqDot > 0 ? writtenSeq.substring(0, seqDot) : schemaName;
            String seqName = writtenSeq == null ? stmt.table() + "_" + def.name() + "_seq"
                    : (seqDot > 0 ? writtenSeq.substring(seqDot + 1) : writtenSeq);
            if (!executor.database.hasSequence(seqSchema, seqName)) {
                // The same sequence CREATE TABLE would build: bounded by the column's own type,
                // carrying whatever the option list said, and checked by the same rules. Building
                // it with no bounds at all let a smallint identity column added by ALTER run past
                // 32767 where one declared at CREATE TABLE stops.
                Sequence seq = DdlTableExecutor.buildIdentitySequence(def, dt, seqName, seqSchema,
                        stmt.table());
                executor.database.addSequence(seq);
                executor.database.registerSchemaObject(seqSchema, "sequence", seqName);
            }
            if ("ALWAYS".equalsIgnoreCase(def.identity())) {
                defaultVal = "__identity__:always:seq:" + seqName;
            } else {
                defaultVal = "__identity__:bydefault:seq:" + seqName;
            }
        }

        // Validate generated column expression references valid columns and is immutable
        if (genExpr != null) {
            // Reject volatile/stable functions and operators in generated column expressions
            DdlExecutor.checkExpressionImmutability(genExpr, ddl.executor.database,
                    "generation expression is not immutable");
            if (genExpr.toLowerCase(java.util.Locale.ROOT).replaceAll("\\s+", "").contains("select")) {
                throw new MemgresException("cannot use subquery in column generation expression", "0A000");
            }
            try {
                Expression genParsed = com.memgres.engine.parser.Parser.parseExpression(genExpr);
                DdlDefinitionChecks.rejectSystemColumnInGeneration(genParsed);
                StoredExprNames.read(ddl, genParsed, table, def.name(), false, true);
                ddl.validateExprColumnRefs(genParsed, table, def.name(), false, true);
                // What the expression produces has to be a value the column can hold, and
                // PostgreSQL settles that where the column is defined rather than at the first row
                // it is computed for -- the same rule, and the same complaint, a DEFAULT written
                // here gets. It is judged from the tree rather than from the text the expression is
                // stored as, so one that will not parse is left to whatever reads it next.
                DdlDefinitionChecks.requireGenerationExprFits(genParsed, resolved, def.name(),
                        table.getColumns());
            } catch (MemgresException me) {
                throw me;
            } catch (Exception ignored) {}
        }

        // DEFAULT NULL fills the existing rows with nothing, so it leaves the column exactly as
        // empty as no default at all and the new rule cannot hold over the rows already stored.
        boolean fillsExistingRows = (defaultVal != null && !"null".equalsIgnoreCase(defaultVal.trim()))
                || domainDefault != null;
        if (def.notNull() && !fillsExistingRows && genExpr == null && !table.getRows().isEmpty()) {
            throw PgErrors.columnContainsNulls(def.name(), "relation", stmt.table());
        }

        // SERIAL and GENERATED AS IDENTITY columns are implicitly NOT NULL (same as CREATE TABLE)
        boolean notNull = def.notNull()
                || dt == DataType.SERIAL || dt == DataType.BIGSERIAL || dt == DataType.SMALLSERIAL
                || def.identity() != null;

        // Carry the resolved compositeTypeName/arrayElementType through, same as CREATE TABLE
        // (DdlTableExecutor) — hardcoding them to null made an ALTER-added "enum_type[]" column
        // indistinguishable from a scalar enum column, so PgWireValueFormatter.columnTypeOid
        // advertised the enum element's OID instead of the array type's.
        Integer addPrecision = def.precision() != null ? def.precision() : resolved.domainPrecision();
        Integer addScale = def.scale() != null ? def.scale() : resolved.domainScale();
        Column col = new Column(def.name(), dt, !notNull, def.primaryKey(), defaultVal,
                enumTypeName, addPrecision, addScale, genExpr, def.generatedVirtual(), domainTypeName,
                compositeTypeName, arrayElementType);
        // A column added later is declared with the same names a column written into CREATE
        // TABLE is, so a range type has to be carried here too: without it the column knew only
        // the type its values are stored as, and described itself as text.
        col.setRangeTypeName(resolved.rangeTypeName());
        String addQualifier = DataType.intervalQualifier(def.typeName());
        col.setIntervalQualifier(addQualifier != null ? addQualifier
                : resolved.domainIntervalQualifier());
        // Don't pre-evaluate serial/nextval/identity defaults — they are evaluated per-row below.
        // Likewise, other VOLATILE defaults (random(), gen_random_uuid(), ...) must produce a
        // distinct value per existing row, matching PG's table rewrite. STABLE functions such as
        // now()/current_timestamp correctly evaluate once per statement and take the single-value
        // path.
        boolean sequenceBacked = defaultVal != null
                && (defaultVal.contains("nextval(") || defaultVal.startsWith("__identity__"));
        boolean volatileDefault = !sequenceBacked && defaultVal != null && genExpr == null
                && hasVolatileFunction(def.defaultExpr());
        Object evaluatedDefault;
        if (sequenceBacked || volatileDefault) {
            evaluatedDefault = null;
        } else {
            evaluatedDefault = defaultVal != null ? executor.evaluateDefault(defaultVal, dt) : null;
            // The default has to be storable in the column being created. Accepting one that is
            // not leaves the table holding a value its own declaration says is impossible.
            if (evaluatedDefault != null) {
                TypeCoercion.coerceForStorage(evaluatedDefault, col);
            }
        }

        // Validate default type compatibility
        if (defaultVal != null && evaluatedDefault != null) {
            switch (dt) {
                case INTEGER:
                case BIGINT:
                case SMALLINT:
                case REAL:
                case DOUBLE_PRECISION:
                case NUMERIC: {
                    if (evaluatedDefault instanceof String) {
                        String s = (String) evaluatedDefault;
                        try { new java.math.BigDecimal(s.trim()); }
                        catch (NumberFormatException nfe) {
                            throw new MemgresException("invalid input syntax for type "
                                    + dt.name().toLowerCase(java.util.Locale.ROOT) + ": \"" + s + "\"", "22P02");
                        }
                    }
                    break;
                }
                default: {
                    break;
                }
            }
        }

        // Check default against CHECK constraint
        if (evaluatedDefault != null && def.checkConstraintExpr() != null) {
            try {
                Table tempTable = new Table(table.getName(), new ArrayList<>(table.getColumns()));
                tempTable.addColumn(new Column(def.name(), dt, !def.notNull(), def.primaryKey(), null), null);
                Object[] tempRow = new Object[tempTable.getColumns().size()];
                int tempIdx = tempTable.getColumnIndex(def.name());
                if (tempIdx >= 0) tempRow[tempIdx] = evaluatedDefault;
                RowContext tempCtx = new RowContext(tempTable, table.getName(), tempRow);
                Object checkResult = executor.evalExpr(def.checkConstraintExpr(), tempCtx);
                if (!executor.isTruthy(checkResult)) {
                    // The name is the one the constraint will carry, which includes the table:
                    // reporting "<column>_check" named a constraint the catalogue never holds,
                    // and left the reader without the relation the rule belongs to.
                    String violated = stmt.table() + "_" + def.name() + "_check";
                    List<TableConstraint> declared = addCol.inlineConstraints();
                    if (declared != null) {
                        for (TableConstraint tc : declared) {
                            if (tc.type() == TableConstraint.ConstraintType.CHECK && tc.name() != null) {
                                violated = tc.name();
                                break;
                            }
                        }
                    }
                    MemgresException failed = new MemgresException("check constraint \"" + violated
                            + "\" of relation \"" + stmt.table() + "\" is violated by some row", "23514");
                    failed.setConstraint(violated);
                    throw failed;
                }
            } catch (MemgresException me) {
                // Whatever the predicate raises is the statement's answer, the same as at ADD
                // CONSTRAINT: a default the column's own CHECK cannot evaluate is not a definition
                // PostgreSQL would store.
                throw me;
            } catch (Exception ignored) {}
        }

        // Nothing of the column's own to fill with, so the domain's default is what the rows get.
        if (evaluatedDefault == null) evaluatedDefault = domainDefault;
        // Mark column as having a missing value if it was added with a DEFAULT and the table has existing rows
        if (evaluatedDefault != null && !table.getRows().isEmpty()) {
            col.setAttHasMissing(true);
        }
        table.addColumn(col, evaluatedDefault);
        // The column belongs to this transaction until it commits: another session's view of the
        // relation is the shape it had before the ALTER.
        executor.database.markUncommittedObject(col, executor.session);
        executor.recordUndo(new Session.AddColumnUndo(schemaName, stmt.table(), def.name()));

        // Backfill existing rows for sequence-backed (serial/identity/nextval) and volatile
        // defaults, evaluating once per row. PG never leaves existing rows NULL for a serial/
        // identity column, and gives each existing row its own random()/gen_random_uuid() value.
        if ((sequenceBacked || volatileDefault) && !table.getRows().isEmpty()) {
            int newColIdx = table.getColumnIndex(def.name());
            if (newColIdx >= 0) {
                String identitySeqName = null;
                if (defaultVal.startsWith("__identity__") && defaultVal.contains(":seq:")) {
                    identitySeqName = defaultVal.substring(defaultVal.indexOf(":seq:") + 5);
                }
                // A serial column's default names its sequence without a qualifier, which means
                // the one in this table's schema. Evaluating the default text instead would look
                // the name up through the search path and miss it whenever the table is not on it.
                Sequence backing = null;
                if (!defaultVal.startsWith("__identity__") && defaultVal.contains("nextval(")) {
                    int q1 = defaultVal.indexOf('\'');
                    int q2 = defaultVal.indexOf('\'', q1 + 1);
                    if (q1 >= 0 && q2 > q1) {
                        backing = executor.database.getSequenceFor(table.getSchemaName(),
                                defaultVal.substring(q1 + 1, q2));
                    }
                }
                for (Object[] row : table.getRows()) {
                    Object v;
                    if (defaultVal.startsWith("__identity__")) {
                        Sequence seq = identitySeqName != null
                                ? executor.database.getSequenceFor(table.getSchemaName(), identitySeqName)
                                : null;
                        v = seq != null ? seq.nextVal() : table.nextSerial();
                    } else if (backing != null) {
                        v = backing.nextVal();
                    } else {
                        v = executor.evaluateDefault(defaultVal, dt);
                    }
                    // The value has to be storable in the column being added. PostgreSQL rewrites
                    // the whole table for a volatile default, so a row that cannot take its value
                    // aborts the statement; keeping the uncoerced value stored something the
                    // column's declaration says is impossible.
                    if (v != null) v = TypeCoercion.coerceForStorage(v, col);
                    row[newColIdx] = v;
                }
            }
        }

        // The constraints written on the column itself. PostgreSQL builds their indexes at the end
        // of the ALTER, over the rows the backfill has just written, which is why ADD COLUMN b int
        // UNIQUE DEFAULT 7 on a table of two rows is refused rather than stored — and why a
        // refused one leaves the table exactly as it was, column and all.
        List<StoredConstraint> inlineAdded = new ArrayList<>();
        try {
            ddl.tableExecutor.storeInlineColumnConstraints(table, def, schemaName, stmt.table(),
                    inlineAdded);
            List<TableConstraint> declaredChecks = addCol.inlineConstraints();
            if (declaredChecks != null) {
                for (TableConstraint tc : declaredChecks) {
                    StoredConstraint sc = ddl.convertTableConstraint(stmt.table(), tc, table);
                    if (sc == null) continue;
                    table.addConstraint(sc);
                    inlineAdded.add(sc);
                }
            }
            for (StoredConstraint sc : inlineAdded) {
                DdlTableExecutor.validatePartitionKeyCoverage(table, sc);
                table.validateNewUniqueConstraint(sc);
            }
        } catch (RuntimeException refused) {
            for (StoredConstraint sc : inlineAdded) {
                if (sc.getName() != null) table.removeConstraint(sc.getName());
            }
            // The statement refused itself, so the column was never there: taking it back off
            // must not leave its number taken the way a DROP COLUMN does.
            table.removeColumn(def.name(), false);
            throw refused;
        }
        for (StoredConstraint sc : inlineAdded) {
            if (sc.getName() != null) {
                executor.recordUndo(new Session.AddConstraintUndo(schemaName, stmt.table(), sc.getName()));
            }
        }

        // Compute STORED generated column values for existing rows (VIRTUAL columns are computed on read)
        if (genExpr != null && !def.generatedVirtual()) {
            int colIdx = table.getColumnIndex(def.name());
            if (colIdx >= 0) {
                for (Object[] row : table.getRows()) {
                    row[colIdx] = executor.dmlExecutor.evalGeneratedColumn(table, row, col);
                }
            }
        }

        propagateAddColumn(table, col, evaluatedDefault);
    }

    /**
     * Children and partitions of a table hold their own copy of its column list, so a column
     * added to a parent has to reach each of them. Without this the parent advertises a column
     * its own children cannot answer for, and a read through the parent finds nothing there.
     */
    private void propagateAddColumn(Table parent, Column col, Object defaultValue) {
        for (Table child : childRelations(parent)) {
            if (child.getColumnIndex(col.getName()) >= 0) continue;
            Column copy = col.withName(col.getName());
            child.addColumn(copy, defaultValue);
            // The child holds the column because the parent declares it, not because its own
            // definition named it, and PostgreSQL keeps the two apart in attislocal.
            child.markColumnInherited(col.getName());
            if (!copy.isNullable()) child.markNotNullInherited(col.getName());
            propagateAddColumn(child, copy, defaultValue);
        }
    }

    /** True when the table is the parent of a partition hierarchy, whether or not it has any yet. */
    private static boolean isPartitioned(Table table) {
        return table.getPartitionStrategy() != null || !table.getPartitions().isEmpty();
    }

    /**
     * {@code 42P16} for a constraint the whole hierarchy has to carry, written with ONLY on a
     * table that has children. PostgreSQL names the way out in the hint when a NOT NULL raised
     * it and says nothing at all for the CHECK form, so the caller says which of the two it is.
     */
    private static MemgresException onlyOnParent(boolean nameTheKeyword) {
        MemgresException e = new MemgresException(
                "constraint must be added to child tables too", "42P16");
        if (nameTheKeyword) e.setHint("Do not specify the ONLY keyword.");
        return e;
    }

    /**
     * A NOT NULL written on a partitioned parent alone would let a partition hold a null the
     * parent's own declaration forbids, and every row of a partition is a row of the parent.
     * Ordinary inheritance is looser — a child there is a table in its own right — and
     * PostgreSQL accepts ONLY on an inheritance parent for this one.
     */
    private static void rejectOnlyNotNullOnPartitioned(Table table, AlterTableStmt stmt,
                                                       String columnName) {
        if (!stmt.only() || table.getPartitions().isEmpty()) return;
        // Only a constraint the partitions would have to take on as well is refused. A column
        // that is already NOT NULL has nothing to add, so PostgreSQL accepts the statement -- and
        // an idempotent migration re-asserting NOT NULL is exactly the shape that writes it.
        int idx = columnName == null ? -1 : table.getColumnIndex(columnName);
        if (idx >= 0 && !table.getColumns().get(idx).isNullable()) return;
        throw onlyOnParent(true);
    }

    /**
     * The same rule for the constraint spelling of the request, which reaches further: an
     * inheritance child would break a rule its parent alone carried as surely as a partition
     * would, and PostgreSQL refuses ONLY there too where the column spelling accepts it. It sends
     * no hint for this one -- the writer named a constraint rather than the keyword the hint would
     * have told them to leave out.
     */
    private static void rejectOnlyNotNullConstraint(Table table, AlterTableStmt stmt,
                                                    List<String> columns) {
        if (!stmt.only() || columns == null || childRelations(table).isEmpty()) return;
        for (String column : columns) {
            int idx = table.getColumnIndex(column);
            if (idx >= 0 && table.getColumns().get(idx).isNullable()) throw onlyOnParent(false);
        }
    }

    /**
     * A partition always carries its parent's constraints, so a constraint declared NO INHERIT on
     * a partitioned table asks for something the hierarchy cannot express. Ordinary inheritance
     * can express it, and PostgreSQL accepts it there.
     */
    private static void rejectNoInheritOnPartitioned(TableConstraint tc, Table table,
                                                     String tableName) {
        if (tc == null || !tc.noInherit() || !isPartitioned(table)) return;
        // A NOT NULL says so in its own words, and under its own SQLSTATE.
        if (tc.type() == TableConstraint.ConstraintType.NOT_NULL) {
            throw new MemgresException(
                    "not-null constraints on partitioned tables cannot be NO INHERIT", "0A000")
                    .suppressPosition();
        }
        throw new MemgresException("cannot add NO INHERIT constraint to partitioned table \""
                + tableName + "\"", "42P16").suppressPosition();
    }

    /** The relations that mirror this table's column list: inheritance children and partitions. */
    private static List<Table> childRelations(Table table) {
        List<Table> out = new ArrayList<>(table.getChildren());
        for (Table partition : table.getPartitions()) {
            if (!out.contains(partition)) out.add(partition);
        }
        return out;
    }

    /**
     * Every relation below this one, at any depth: its partitions and its inheritance children.
     * A statement that names a parent is a statement about all of them, because every row they
     * hold is a row of the parent.
     */
    private static List<Table> descendantRelations(Table table) {
        List<Table> found = new ArrayList<>();
        collectDescendants(table, found);
        return found;
    }

    private static void collectDescendants(Table table, List<Table> found) {
        for (Table child : childRelations(table)) {
            // Multiple inheritance can reach one child down two paths; it is one relation still.
            if (containsIdentity(found, child)) continue;
            found.add(child);
            collectDescendants(child, found);
        }
    }

    /**
     * A column cannot be declared NOT NULL while a row holds a null in it. The rows a parent
     * stands for are stored in its partitions and its inheritance children, so PostgreSQL names
     * the relation the offending row is really in rather than the one the statement named.
     */
    private static void requireColumnHasNoNulls(Table table, String column, String relationName) {
        int colIdx = table.getColumnIndex(column);
        if (colIdx < 0) return;
        for (Object[] row : table.getRows()) {
            if (row[colIdx] == null) {
                throw PgErrors.columnContainsNulls(column, "relation", relationName);
            }
        }
    }

    /**
     * Take the NOT NULL flag off everything below the table along with its own. PostgreSQL 18
     * keeps a NOT NULL in pg_constraint like any other constraint, and what a partition or an
     * inheritance child carries is the parent's constraint counted on the descendant rather than
     * one of its own -- so dropping the parent's drops it all the way down. ONLY asks for the
     * named relation alone, and the caller decides that.
     */
    private static void clearNotNullOnDescendants(Table table, String column) {
        for (Table child : childRelations(table)) {
            if (child.getColumnIndex(column) < 0) continue;
            // A relation that declared the rule for itself, or takes it from another parent as
            // well, holds a constraint this drop does not reach: PostgreSQL takes away the one
            // count the parent contributed and leaves the constraint standing on whatever is
            // left. Everything below it goes on taking the rule from there, so the walk stops.
            if (retainsNotNullWithoutParent(child, column)) continue;
            child.alterColumnNullable(column, true);
            child.setNotNullConstraintName(column, null);
            // Nothing is left for a name to belong to, so the one written down goes with it: a
            // rule declared here afterwards is a new constraint and takes a new name.
            child.pinInheritedNotNullName(column, null);
            clearNotNullOnDescendants(child, column);
        }
    }

    /**
     * True when this relation goes on refusing a null after the parent it took the rule from has
     * stopped: its own definition declared it, another parent still declares it, or a parent that
     * dropped the column left a count behind that nothing can withdraw. A partition that said
     * NOT NULL for itself declared it as surely as an inheritance child does, and PostgreSQL
     * leaves it standing there when the partitioned table stops.
     */
    private static boolean retainsNotNullWithoutParent(Table child, String column) {
        if (child.isNotNullLocal(column)) return true;
        if (child.retainedNotNullInheritCount(column) > 0) return true;
        for (Table parent : child.getDirectParents()) {
            if (CatalogConstraintBuilder.declaresNotNull(parent, column)) return true;
        }
        return false;
    }

    /**
     * Write down the name each relation below this one holds the column's NOT NULL under, for
     * those that go on holding it once this relation has stopped declaring it.
     *
     * <p>PostgreSQL names a constraint when it is created and never names it again, so a relation
     * that took the rule from two parents answers to the name the first of them gave it for as
     * long as it holds the rule at all. Reading the name afresh from the parent that is left
     * renames a constraint PostgreSQL renames nowhere, so it is written down here -- while the
     * parent that gave it still declares the rule and the name can still be read off it.
     */
    private static void pinRetainedNotNullNames(Table table, String column) {
        for (Table child : childRelations(table)) {
            int idx = child.getColumnIndex(column);
            if (idx < 0 || child.getColumns().get(idx).isNullable()) continue;
            // A relation that declared the rule itself answers to its own name whatever happens
            // above it, and everything below it goes on reading that one.
            if (child.isNotNullLocal(column)) continue;
            if (retainsNotNullBesides(child, table, column)) {
                child.pinInheritedNotNullName(column,
                        CatalogConstraintBuilder.notNullConstraintName(child, column));
            } else {
                pinRetainedNotNullNames(child, column);
            }
        }
    }

    /** True when the relation goes on refusing a null once the named parent has stopped. */
    private static boolean retainsNotNullBesides(Table child, Table leaving, String column) {
        if (child.retainedNotNullInheritCount(column) > 0) return true;
        for (Table parent : child.getDirectParents()) {
            if (parent != leaving
                    && CatalogConstraintBuilder.declaresNotNull(parent, column)) return true;
        }
        return false;
    }

    /**
     * Write down what a column's NOT NULL is, here and on everything below, before the statement
     * changes it.
     *
     * <p>PostgreSQL undoes a DDL statement as completely as it undoes a write, so a transaction
     * that declared the rule -- or withdrew it -- and then rolled back leaves the hierarchy
     * refusing exactly the nulls it refused before. Each relation keeps its own copy of the
     * column list and its own record of whose rule the NOT NULL on it is, so every relation the
     * statement can reach has to be remembered rather than only the one it named.
     */
    private void recordNotNullUndo(Table table, String column) {
        recordOneNotNullUndo(table, column);
        for (Table descendant : descendantRelations(table)) {
            recordOneNotNullUndo(descendant, column);
        }
    }

    private void recordOneNotNullUndo(Table table, String column) {
        int idx = table.getColumnIndex(column);
        if (idx < 0) return;
        // Only a name the writer chose is worth putting back: the default spelling is derived
        // from the relation and the column, and a rollback restores both of those anyway.
        String held = table.notNullConstraintName(column);
        if (held != null && held.equals(table.defaultNotNullConstraintName(column))) held = null;
        executor.recordUndo(new NotNullUndo(table.getSchemaName(), table.getName(), column,
                table.getColumns().get(idx).isNullable(), held, table.isNotNullLocal(column),
                table.inheritedNotNullName(column)));
    }

    /**
     * ONLY leaves every child holding the rule, and each of them holds it for itself from then on:
     * the relation that declared it has stopped, so there is nobody left it could be held for. The
     * name stays where it was -- PostgreSQL names a constraint once and never names it again -- and
     * a child that takes the same rule from another parent as well is left exactly as it was,
     * because it is still holding that one for somebody.
     */
    private static void makeNotNullLocalOnChildren(Table parent, String column) {
        for (Table child : childRelations(parent)) {
            int idx = child.getColumnIndex(column);
            if (idx < 0 || child.getColumns().get(idx).isNullable()) continue;
            boolean handedDownStill = false;
            for (Table above : child.getDirectParents()) {
                if (above != parent && CatalogConstraintBuilder.declaresNotNull(above, column)) {
                    handedDownStill = true;
                    break;
                }
            }
            if (handedDownStill) continue;
            child.setNotNullConstraintName(column,
                    CatalogConstraintBuilder.notNullConstraintName(child, column));
            child.markNotNullLocal(column);
        }
    }

    /**
     * A NOT NULL a parent declares is the descendant's too, and it is the parent's to withdraw: a
     * partition or an inheritance child that could drop it on its own would take a null every row
     * of the parent is supposed to be free of. PostgreSQL refuses both, and words the two refusals
     * differently -- a partition is told the partitioned table marks the column, an inheritance
     * child is told the constraint it holds came from somewhere else and is named which one.
     *
     * @param named true when the statement named the constraint rather than the column, which
     *              PostgreSQL answers with the inherited-constraint wording in either case
     */
    private static void rejectDropNotNullUnderParent(Table table, String column, String tableName,
                                                     boolean named) {
        Table partitionParent = table.getPartitionParent();
        if (columnRequiresNotNull(partitionParent, column)) {
            if (!named) {
                throw new MemgresException("column \"" + column
                        + "\" is marked NOT NULL in parent table", "42P16");
            }
            throw cannotDropInheritedNotNull(table, column, tableName);
        }
        for (Table parent : table.getInheritParents()) {
            if (!columnRequiresNotNull(parent, column)) continue;
            throw cannotDropInheritedNotNull(table, column, tableName);
        }
        // A parent that dropped the column withdrew nothing from the relations that kept it, so
        // the rule is still one of theirs to hold rather than theirs to withdraw, and there is no
        // parent left to name it by: the name is the one the constraint answers to here.
        if (table.retainedNotNullInheritCount(column) > 0) {
            throw new MemgresException("cannot drop inherited constraint \""
                    + CatalogConstraintBuilder.notNullConstraintName(table, column)
                    + "\" of relation \"" + tableName + "\"", "42P16").suppressPosition();
        }
    }

    /**
     * The refusal PostgreSQL gives for an inherited NOT NULL, naming the constraint the relation
     * the statement named is holding rather than the parent's: a relation that restated the rule
     * holds one of its own, under its own name, and is still refused permission to drop it while a
     * parent goes on declaring it too. Only where it declared nothing is the name the parent's,
     * which is what the walk up the chain answers with anyway.
     */
    private static MemgresException cannotDropInheritedNotNull(Table relation, String column,
                                                               String tableName) {
        String conname = CatalogConstraintBuilder.notNullConstraintName(relation, column);
        return new MemgresException("cannot drop inherited constraint \"" + conname
                + "\" of relation \"" + tableName + "\"", "42P16").suppressPosition();
    }

    /**
     * The column a NOT NULL constraint of this name covers, read from the name the constraint
     * really answers to.
     *
     * <p>A constraint has one name, the one it was declared with, and a relation holding one it
     * took from a parent holds it under that name: a descendant that knew only the name it would
     * have chosen for itself answered that no such constraint existed, where PostgreSQL's answer
     * is that the constraint is there and may not be dropped here -- and answered that the name it
     * would have chosen is a constraint of its own, where PostgreSQL holds no constraint of that
     * name anywhere.
     */
    private static String notNullColumnNamed(Table table, String name) {
        if (name == null) return null;
        for (Column c : table.getColumns()) {
            if (c.isNullable()) continue;
            if (name.equalsIgnoreCase(
                    CatalogConstraintBuilder.notNullConstraintName(table, c.getName()))) {
                return c.getName();
            }
        }
        return null;
    }

    /**
     * True when this relation declares the column NOT NULL on behalf of everything below it. A
     * rule written NO INHERIT answers for the declaring relation's own rows, so a descendant that
     * declared NOT NULL for itself holds it outright and may withdraw it.
     */
    private static boolean columnRequiresNotNull(Table table, String column) {
        if (table == null || column == null) return false;
        int idx = table.getColumnIndex(column);
        return idx >= 0 && !table.getColumns().get(idx).isNullable()
                && !table.isNotNullNoInherit(column);
    }

    /**
     * A default declared on a parent is the hierarchy's default: PostgreSQL recurses into the
     * partitions and the inheritance children and replaces whatever each of them had, so a
     * descendant's own SET DEFAULT is overwritten rather than kept. ONLY asks for the parent alone.
     */
    private static void propagateColumnDefault(Table table, String column, String defaultValue,
                                               boolean only) {
        if (only) return;
        for (Table child : childRelations(table)) {
            if (child.getColumnIndex(column) < 0) continue;
            child.alterColumnDefault(column, defaultValue);
            propagateColumnDefault(child, column, defaultValue, false);
        }
    }

    /**
     * A CHECK declared on a parent holds over every row of the hierarchy, so a table joining one
     * has to carry the rule already: PostgreSQL refuses the link while the child is missing it
     * rather than adding the constraint itself. A NO INHERIT constraint was never going to
     * travel, so it is not asked for.
     */
    private static void requireInheritedChecks(Table parent, Table child) {
        for (StoredConstraint sc : parent.getConstraints()) {
            if (sc.getType() != StoredConstraint.Type.CHECK || sc.isNoInherit()) continue;
            if (sc.getName() == null) continue;
            StoredConstraint own = child.getConstraint(sc.getName());
            if (own == null) {
                throw PgErrors.datatypeMismatch("child table is missing constraint \""
                        + sc.getName() + "\"").suppressPosition();
            }
            // A rule the relation declared NO INHERIT is about its own rows and nobody else's, so
            // it is not the parent's rule under the same name. PostgreSQL refuses rather than
            // merging the two, because the merged constraint would be one the relation is holding
            // on the parent's behalf -- which is exactly what NO INHERIT said it is not.
            if (own.isNoInherit()) {
                throw new MemgresException("constraint \"" + sc.getName()
                        + "\" conflicts with non-inherited constraint on child table \""
                        + child.getName() + "\"", "42P17").suppressPosition();
            }
            // The relation has to be enforcing the parent's rule, not merely a rule of the same
            // name: the two become one constraint, and a relation joining the hierarchy with a
            // different test under that name would go on enforcing its own while the catalogue
            // said it held the parent's. PostgreSQL compares what the two constraints say rather
            // than how they were written, so a pair of parentheses makes no difference and the
            // same comparison written the other way round does.
            if (!sameCheckDefinition(parent, sc, child, own)) {
                throw PgErrors.datatypeMismatch("child table \"" + child.getName()
                        + "\" has different definition for check constraint \""
                        + sc.getName() + "\"").suppressPosition();
            }
        }
    }

    /**
     * True when two CHECK constraints of the same name say the same thing.
     *
     * <p>PostgreSQL compares the two parse trees, so what the writer typed is beside the point:
     * {@code CHECK ((j) > 0)} is the constraint {@code CHECK (j > 0)} written differently, while
     * {@code CHECK (0 < j)} is a different rule that happens to hold over the same rows. Reading
     * each of them back the way {@code pg_get_constraintdef} does asks exactly that question --
     * the deparser writes one text per tree -- against each relation's own column types, because
     * a literal is printed in the type it is compared with.
     */
    private static boolean sameCheckDefinition(Table parent, StoredConstraint parentCheck,
                                               Table child, StoredConstraint childCheck) {
        String above = RuleDeparser.deparse(parentCheck.getCheckExpr(),
                RuleDeparser.forTable(parent));
        String below = RuleDeparser.deparse(childCheck.getCheckExpr(),
                RuleDeparser.forTable(child));
        return above.equals(below);
    }

    /**
     * A child stands in for its parent, so it may not take a null in a column the parent declares
     * NOT NULL. PostgreSQL makes the writer declare it on the child rather than adding the
     * constraint itself, and names the column and the table it is missing from.
     */
    private static void requireNotNullWhereParentIs(Column parentCol, Column childCol,
                                                    String childName) {
        if (parentCol.isNullable() || !childCol.isNullable()) return;
        throw PgErrors.datatypeMismatch("column \"" + parentCol.getName()
                + "\" in child table \"" + childName + "\" must be marked NOT NULL")
                .suppressPosition();
    }

    /**
     * A child holds its parent's rows, so a column of it is filled the way the parent's is: a
     * value the writer supplies where the parent takes one, and an expression the relation works
     * out where the parent works one out. PostgreSQL will not merge the two, because a child that
     * computed a column the parent stores would answer differently for the same row read through
     * either relation, so it says which of the two the child is missing rather than adopting the
     * parent's expression. Two generated columns are left to disagree about the expression itself
     * -- each relation computes its own rows -- but not about when it is computed.
     */
    private static void requireSameGeneration(Column parentCol, Column childCol) {
        if (parentCol.isGenerated() == childCol.isGenerated()
                && (!parentCol.isGenerated() || parentCol.isVirtual() == childCol.isVirtual())) {
            return;
        }
        if (parentCol.isGenerated() && childCol.isGenerated()) {
            MemgresException ex = PgErrors.datatypeMismatch("column \"" + parentCol.getName()
                    + "\" inherits from generated column of different kind");
            ex.setDetail("Parent column is " + generationKind(parentCol)
                    + ", child column is " + generationKind(childCol) + ".");
            throw ex.suppressPosition();
        }
        throw PgErrors.datatypeMismatch("column \"" + parentCol.getName() + "\" in child table must"
                + (parentCol.isGenerated() ? "" : " not") + " be a generated column")
                .suppressPosition();
    }

    /** How PostgreSQL names the two kinds of generated column when it reports them. */
    private static String generationKind(Column column) {
        return column.isVirtual() ? "VIRTUAL" : "STORED";
    }

    /**
     * A NOT NULL written NO INHERIT says the rule is about this relation's own rows and travels
     * to nobody; a NOT NULL taken from a parent is one rule the relation holds on the parent's
     * behalf. The two cannot both be true of one column, so PostgreSQL refuses to join the
     * hierarchy rather than merge them, and names the constraint the child already carries.
     */
    private static void rejectNoInheritAgainstParent(Table parent, Table child) {
        for (Column parentCol : parent.getColumns()) {
            String column = parentCol.getName();
            if (!CatalogConstraintBuilder.declaresNotNull(parent, column)) continue;
            if (child.getColumnIndex(column) < 0 || !child.isNotNullNoInherit(column)) continue;
            throw PgErrors.invalidObjectState("constraint \""
                    + CatalogConstraintBuilder.notNullConstraintName(child, column)
                    + "\" conflicts with non-inherited constraint on child table \""
                    + child.getName() + "\"").suppressPosition();
        }
    }

    /**
     * The partition key decides which partition a row belongs in, so the column it reads may
     * neither be dropped nor retyped: the bounds already stored were written in that column's
     * type, and a key naming a column that is gone routes nothing at all. PostgreSQL refuses both
     * whether or not the table has any partitions yet, and an expression key stands for every
     * column the expression names.
     *
     * @param verb {@code drop} or {@code alter}, the word PostgreSQL uses for the statement asked
     */
    private static void rejectPartitionKeyColumnChange(Table table, String column, String verb,
                                                       String tableName) {
        if (column == null || !partitionKeyNames(table.getPartitionColumn(), column)) return;
        throw new MemgresException("cannot " + verb + " column \"" + column
                + "\" because it is part of the partition key of relation \"" + tableName + "\"",
                "42P16");
    }

    /** True when one of a partition key's entries is this column, or is an expression reading it. */
    private static boolean partitionKeyNames(String partitionKey, String column) {
        if (partitionKey == null) return false;
        for (String key : DdlTableExecutor.splitTopLevel(partitionKey)) {
            String entry = key.trim();
            if (entry.isEmpty()) continue;
            if (entry.indexOf('(') >= 0) {
                if (expressionNamesColumn(entry, column)) return true;
            } else if (entry.equalsIgnoreCase(column)) {
                return true;
            }
        }
        return false;
    }

    /**
     * True when the column is one the table gets from a parent. PG records this as
     * {@code attinhcount}; here it is reconstructed by looking up the ancestor chain, which is
     * equivalent because a child always carries every one of its parent's columns.
     */
    private boolean isInheritedColumn(Table table, String column) {
        for (Table parent : ancestorsOf(table)) {
            if (parent.getColumnIndex(column) >= 0) return true;
        }
        return false;
    }

    /**
     * Every table this one inherits from, at any depth. A table written with
     * {@code INHERITS (p1, p2)} keeps only the last of its parents in its own field, so the rest
     * are found the way {@code pg_inherits} is read — from the parent's side.
     */
    private List<Table> ancestorsOf(Table table) {
        List<Table> found = new ArrayList<>();
        List<Table> frontier = new ArrayList<>();
        frontier.add(table);
        while (!frontier.isEmpty()) {
            List<Table> next = new ArrayList<>();
            for (Table t : frontier) {
                for (Table parent : directParentsOf(t)) {
                    if (parent == table || containsIdentity(found, parent)) continue;
                    found.add(parent);
                    next.add(parent);
                }
            }
            frontier = next;
        }
        return found;
    }

    /** The tables this one directly inherits from or is a partition of. */
    private List<Table> directParentsOf(Table table) {
        List<Table> out = new ArrayList<>();
        if (table.getParentTable() != null) out.add(table.getParentTable());
        if (table.getPartitionParent() != null
                && !containsIdentity(out, table.getPartitionParent())) {
            out.add(table.getPartitionParent());
        }
        for (Schema sch : executor.database.getSchemas().values()) {
            for (Table candidate : sch.getTables().values()) {
                if (candidate == table || containsIdentity(out, candidate)) continue;
                if (containsIdentity(childRelations(candidate), table)) out.add(candidate);
            }
        }
        return out;
    }

    private static boolean containsIdentity(List<Table> tables, Table target) {
        for (Table t : tables) {
            if (t == target) return true;
        }
        return false;
    }

    /**
     * A table can only be attached to a parent if it already has every one of the parent's
     * columns, with the same type — otherwise the hierarchy could not be read through the parent.
     */
    private void validateInheritedColumns(Table parent, Table child, String childName) {
        for (Column pc : parent.getColumns()) {
            int idx = child.getColumnIndex(pc.getName());
            if (idx < 0) {
                throw PgErrors.datatypeMismatch("child table is missing column \""
                        + pc.getName() + "\"");
            }
            if (!sameColumnType(pc, child.getColumns().get(idx))) {
                throw PgErrors.datatypeMismatch("child table \"" + childName
                        + "\" has different type for column \"" + pc.getName() + "\"");
            }
            requireNotNullWhereParentIs(pc, child.getColumns().get(idx), childName);
            requireSameGeneration(pc, child.getColumns().get(idx));
        }
    }

    private void executeRenameColumn(AlterTableStmt.RenameColumn rename, Table table,
                                      AlterTableStmt stmt, String schemaName) {
        // A typed table's column names are its composite type's, so a rename here would leave the
        // table answering to a name the type does not declare. PostgreSQL refuses before it looks
        // the column up at all, which is why a name that is not there gets this refusal and not an
        // undefined column. ALTER TYPE is what renames a column on every table built on the type.
        if (table.getOfTypeName() != null) {
            throw PgErrors.wrongObjectType("cannot rename column of typed table");
        }
        if (table.getColumnIndex(rename.oldName()) < 0) {
            throw new MemgresException("column \"" + rename.oldName() + "\" does not exist", "42703");
        }
        if (table.getColumnIndex(rename.newName()) >= 0) {
            throw new MemgresException("column \"" + rename.newName() + "\" of relation \""
                    + stmt.table() + "\" already exists", "42701");
        }
        DdlDefinitionChecks.rejectSystemColumnName(rename.newName());
        // An inherited column is the parent's; renaming it on the child alone, or on the parent
        // alone, would leave the two disagreeing about the same column.
        if (isInheritedColumn(table, rename.oldName())) {
            throw new MemgresException("cannot rename inherited column \""
                    + rename.oldName() + "\"", "42P16");
        }
        if (stmt.only() && !childRelations(table).isEmpty()) {
            throw new MemgresException("inherited column \"" + rename.oldName()
                    + "\" must be renamed in child tables too", "42P16");
        }
        // The NOT NULL constraint keeps the name it had, which was derived from the old column
        // name, so that name has to be written down before the column changes underneath it.
        // A sequence that belongs to this column is recorded against the column's name, so the
        // rename has to reach it: without that the sequence stopped answering for the column and
        // outlived the table that owned it.
        for (Sequence owned : executor.database.getSequences().values()) {
            if (owned.isOwnedBy(stmt.table(), rename.oldName())) {
                owned.setOwnedByColumn(rename.newName());
            }
        }
        table.pinNotNullConstraintName(rename.oldName());
        table.renameColumn(rename.oldName(), rename.newName());
        table.moveNotNullConstraintName(rename.oldName(), rename.newName());
        propagateRenameColumn(table, rename.oldName(), rename.newName());
        rewriteIncomingForeignKeys(stmt.table(), schemaName, rename.oldName(), rename.newName());
        rewriteIndexMetadata(stmt.table(), schemaName, rename.oldName(), rename.newName());
        rewriteDependentViews(schemaName, stmt.table(), rename.oldName(), rename.newName());
        executor.recordUndo(new Session.RenameColumnUndo(schemaName, stmt.table(),
                rename.newName(), rename.oldName()));
    }

    /**
     * A column dropped from a parent goes from every relation that was holding it on the parent's
     * behalf, and from no other.
     *
     * <p>A relation holds the column once for each parent that declares it, so one parent letting
     * go does not take it away while another still hands it down, and a child whose own definition
     * named the column keeps it whatever its parents do -- with the values it holds in it, which
     * a drop that reached it would have thrown away. A partition declares nothing of its own, so
     * it holds exactly what the partitioned table holds. Everything below is reached through the
     * relation itself: one that keeps the column goes on handing it down, so the walk stops there.
     */
    private void propagateDropColumn(Table parent, String column) {
        for (Table child : childRelations(parent)) {
            int idx = child.getColumnIndex(column);
            if (idx < 0 || keepsDroppedColumn(child, column)) continue;
            // What this relation is handing down has to be read before it lets the column go.
            List<String> handedDown = inheritableChecksOn(child, column);
            String handedDownNotNull =
                    CatalogConstraintBuilder.declaresNotNull(child, column)
                            ? CatalogConstraintBuilder.notNullConstraintName(child, column) : null;
            // The child's copy has to come back with the parent's if the statement is rolled back,
            // and it has to come back holding what it held: a column restored empty is not the
            // column the transaction dropped, and it is recorded as the parent's again rather than
            // as one the child would then be claiming to have declared.
            Column droppedCol = child.getColumns().get(idx);
            List<Object> colValues = new ArrayList<>();
            for (Object[] row : child.getRows()) {
                colValues.add(row[idx]);
            }
            recordDroppedColumnConstraints(child, column, child.getSchemaName(), child.getName());
            executor.recordUndo(new Session.DropInheritedColumnUndo(child.getSchemaName(),
                    child.getName(), droppedCol, idx, colValues,
                    !child.isColumnLocal(column), !child.isNotNullLocal(column)));
            child.removeColumn(column);
            propagateDropColumn(child, column);
            keepInheritedConstraintCounts(child, column, handedDown, handedDownNotNull);
        }
    }

    /**
     * The names of the CHECK constraints this relation hands down that the column would take with
     * it. A rule marked NO INHERIT was never going to travel, so nothing below counts it.
     */
    private static List<String> inheritableChecksOn(Table table, String column) {
        List<String> names = new ArrayList<>();
        for (StoredConstraint sc : table.getConstraints()) {
            if (sc.getType() != StoredConstraint.Type.CHECK || sc.isNoInherit()) continue;
            if (sc.getName() == null || !sc.dependsOnColumn(column)) continue;
            names.add(sc.getName());
        }
        return names;
    }

    /**
     * A relation that goes on holding a column its parent has dropped goes on holding the rules on
     * it, counted as it counted them.
     *
     * <p>PostgreSQL never decrements a descendant's count for a constraint that was dropped as a
     * dependency of a dropped column: the count says how many parents handed the rule down when
     * the descendant took it, and dropping a column tells the descendants nothing about the rules
     * that went with it. So the relation goes on reporting the rule as one it holds for somebody,
     * under the name that parent gave it, and it is still refused permission to withdraw it --
     * which is what stops a hierarchy losing a rule its parent never asked it to lose.
     *
     * @param handedDown the CHECK constraints the parent was handing down, read before it let the
     *                   column go
     * @param handedDownNotNull the name the parent's NOT NULL on the column answered to, or null
     *                          when the parent did not declare one
     */
    private void keepInheritedConstraintCounts(Table parent, String column,
                                               List<String> handedDown,
                                               String handedDownNotNull) {
        for (Table child : childRelations(parent)) {
            if (child.getColumnIndex(column) < 0) continue;
            List<String> counted = new ArrayList<>();
            for (String conname : handedDown) {
                StoredConstraint held = child.getConstraint(conname);
                if (held == null) continue;
                held.setRetainedInheritCount(held.getRetainedInheritCount() + 1);
                counted.add(conname);
            }
            boolean countedNotNull = handedDownNotNull != null
                    && CatalogConstraintBuilder.declaresNotNull(child, column);
            String hadName = child.inheritedNotNullName(column);
            if (countedNotNull) {
                child.setRetainedNotNullInheritCount(column,
                        child.retainedNotNullInheritCount(column) + 1);
                // The name the parent gave the rule is the name it goes on answering to, and
                // there is nothing left above to read it from once the parent's column is gone.
                if (!child.isNotNullLocal(column)) {
                    child.pinInheritedNotNullName(column, handedDownNotNull);
                }
            }
            if (counted.isEmpty() && !countedNotNull) continue;
            executor.recordUndo(new Session.InheritedConstraintCountUndo(child.getSchemaName(),
                    child.getName(), column, counted, countedNotNull, hadName));
        }
    }

    /**
     * Write down the constraints this column is about to take with it, so that a statement which
     * rolls back gives them back. They are recorded before the column's own undo entry, which is
     * what puts the column back first: a key constraint is rebuilt over the column's position.
     */
    private void recordDroppedColumnConstraints(Table table, String column, String schemaName,
                                                String tableName) {
        List<StoredConstraint> doomed = new ArrayList<>();
        for (StoredConstraint sc : table.getConstraints()) {
            if (sc.dependsOnColumn(column)) doomed.add(sc);
        }
        if (doomed.isEmpty()) return;
        executor.recordUndo(new Session.DropColumnConstraintsUndo(schemaName, tableName, doomed));
    }

    /**
     * True when a relation goes on holding a column the parent it takes it from has just dropped:
     * another parent still declares it, or its own definition named it. A partition holds the
     * partitioned table's columns and declares none of its own.
     */
    private static boolean keepsDroppedColumn(Table child, String column) {
        for (Table parent : child.getDirectParents()) {
            if (parent.getColumnIndex(column) >= 0) return true;
        }
        return child.getPartitionParent() == null && child.isColumnLocal(column);
    }

    /**
     * ONLY leaves the children holding the column, and each of them holds it for itself from then
     * on: the parent has stopped declaring it, so there is nobody left it could be held for. A
     * child that takes the same column from another parent as well goes on counting that one, so
     * the column is the child's own and inherited at the same time.
     */
    private void makeDroppedColumnLocal(Table parent, String column) {
        for (Table child : childRelations(parent)) {
            if (child.getColumnIndex(column) < 0 || child.isColumnLocal(column)) continue;
            executor.recordUndo(new Session.ColumnLocalityUndo(child.getSchemaName(),
                    child.getName(), column));
            child.markColumnLocal(column);
        }
    }

    /**
     * A partition holds exactly its parent's columns, so a column is added to or dropped from the
     * partitioned table and reaches the partitions from there. The propagation path arrives at a
     * partition from its parent, so this guard sits where the statement named the relation.
     */
    private static void rejectDirectPartitionColumnChange(Table table, String what) {
        if (table == null || table.getPartitionParent() == null) return;
        throw PgErrors.wrongObjectType("cannot " + what);
    }

    /** True when the stored expression text really names this column, read as a parse tree. */
    private static boolean expressionNamesColumn(String exprText, String column) {
        Expression parsed;
        try {
            parsed = com.memgres.engine.parser.Parser.parseExpression(exprText);
        } catch (RuntimeException e) {
            // An expression that will not parse can only be judged by its text, which is what the
            // loose test did for every expression.
            return exprText.toLowerCase(java.util.Locale.ROOT).contains(column.toLowerCase(java.util.Locale.ROOT));
        }
        return AstWalk.anyMatch(parsed, new java.util.function.Predicate<Object>() {
            @Override public boolean test(Object n) {
                return n instanceof ColumnRef
                        && column.equalsIgnoreCase(((ColumnRef) n).column());
            }
        });
    }

    /**
     * The views whose query really reads this column of this relation.
     *
     * <p>Read off the view's parse tree: a view depends on the column only when one of its FROM
     * items is this relation and the query names the column against that item, or expands it with
     * a wildcard. A view that survives only as unparsed text cannot be judged that way, so the
     * old, looser text test is kept for it rather than letting the drop through unchecked.
     */
    private List<Database.ViewDef> dependentViewsOf(String schemaName, String tableName,
                                                    String column) {
        List<Database.ViewDef> found = new ArrayList<>();
        for (Map.Entry<String, Database.ViewDef> entry : executor.database.getViews().entrySet()) {
            Database.ViewDef vd = entry.getValue();
            if (vd.query() == null) {
                String sql = vd.sourceSQL() == null ? "" : vd.sourceSQL().toLowerCase(java.util.Locale.ROOT);
                if (sql.contains(tableName.toLowerCase(java.util.Locale.ROOT)) && sql.contains(column.toLowerCase(java.util.Locale.ROOT))) {
                    found.add(vd);
                }
                continue;
            }
            if (viewReadsColumn(vd, schemaName, tableName, column)) found.add(vd);
        }
        return found;
    }

    /** True when the view's query names this column of this relation. */
    private boolean viewReadsColumn(Database.ViewDef vd, String schemaName, String tableName,
                                    String column) {
        final Set<String> relationNames = new HashSet<>();
        AstWalk.forEach(vd.query(), new java.util.function.Consumer<Object>() {
            @Override public void accept(Object n) {
                if (!(n instanceof SelectStmt.TableRef)) return;
                SelectStmt.TableRef ref = (SelectStmt.TableRef) n;
                if (ref.table == null || !ref.table.equalsIgnoreCase(tableName)) return;
                String refSchema = ref.schema != null ? ref.schema : schemaName;
                if (refSchema != null && !refSchema.equalsIgnoreCase(schemaName)) return;
                relationNames.add(ref.table.toLowerCase(java.util.Locale.ROOT));
                if (ref.alias != null) relationNames.add(ref.alias.toLowerCase(java.util.Locale.ROOT));
            }
        });
        if (relationNames.isEmpty()) return false;
        return AstWalk.anyMatch(vd.query(), new java.util.function.Predicate<Object>() {
            @Override public boolean test(Object n) {
                if (n instanceof ColumnRef) {
                    ColumnRef cr = (ColumnRef) n;
                    if (!column.equalsIgnoreCase(cr.column())) return false;
                    return cr.table() == null || relationNames.contains(cr.table().toLowerCase(java.util.Locale.ROOT));
                }
                if (n instanceof WildcardExpr) {
                    // A star over the relation stands for every column it has, this one included.
                    WildcardExpr w = (WildcardExpr) n;
                    return w.table() == null || relationNames.contains(w.table().toLowerCase(java.util.Locale.ROOT));
                }
                return false;
            }
        });
    }

    /**
     * Drop each view CASCADE reached, and any view built over one of those in turn — the same
     * cascade DROP VIEW itself performs, because a view over a dropped view loses its source.
     *
     * <p>PostgreSQL says what CASCADE took, so a script that meant to drop one column has some way
     * of learning it dropped three views along with it.
     */
    private void dropDependentViews(List<Database.ViewDef> views) {
        List<String> cascaded = new ArrayList<>();
        for (Database.ViewDef vd : views) {
            String viewSchema = vd.schemaName() != null ? vd.schemaName() : executor.defaultSchema();
            if (executor.database.getView(viewSchema, vd.name()) == null) continue;
            for (String dependent : ViewDependencies.cascadeDependents(
                    executor.database, executor.systemCatalog, viewSchema, vd.name())) {
                Database.ViewDef dep = executor.database.getView(dependent);
                if (dep == null) continue;
                executor.recordUndo(new Session.DropViewUndo(dependent, dep));
                executor.database.removeView(dependent);
                cascaded.add("view " + RelationNamespace.bareName(dependent));
            }
            executor.recordUndo(new Session.DropViewUndo(vd.name(), vd));
            executor.database.removeView(viewSchema, vd.name());
            cascaded.add("view " + vd.name());
        }
        DdlObjectExecutor.noticeDropCascades(executor, cascaded);
    }

    private void propagateRenameColumn(Table parent, String oldName, String newName) {
        for (Table child : childRelations(parent)) {
            if (child.getColumnIndex(oldName) < 0) continue;
            child.renameColumn(oldName, newName);
            propagateRenameColumn(child, oldName, newName);
        }
    }

    /**
     * Drop the sequences that belong to one column of a table.
     *
     * <p>A sequence created for a serial or identity column and one attached with ALTER SEQUENCE
     * ... OWNED BY both go when the column does; a sequence a DEFAULT merely names is somebody
     * else's and stays, which is the one distinction PostgreSQL draws here.
     *
     * @param defaultSeqName the sequence the column's default draws from, read before the default
     *                       is cleared and used for a column that predates ownership being recorded
     */
    private void dropSequencesOwnedBy(Table table, String tableName, String column,
                                      String defaultSeqName) {
        List<Sequence> doomed = new ArrayList<>();
        for (Sequence seq : new ArrayList<>(executor.database.getSequences().values())) {
            if (seq.isOwnedBy(tableName, column)) doomed.add(seq);
        }
        if (doomed.isEmpty() && defaultSeqName != null) {
            Sequence byName = executor.database.getSequenceFor(table.getSchemaName(), defaultSeqName);
            if (byName != null && byName.isInternal()) doomed.add(byName);
        }
        for (Sequence seq : doomed) {
            executor.recordUndo(new Session.DropSequenceUndo(seq.qualifiedName(), seq));
            executor.database.removeSequence(seq.getSchemaName(), seq.getName());
            executor.database.unregisterSchemaObject(seq.getSchemaName(), "sequence", seq.getName());
            executor.database.removeObjectOwner("sequence:" + seq.getName());
        }
    }

    private void executeDropColumn(AlterTableStmt.DropColumn dropCol, Table table,
                                    AlterTableStmt stmt, String schemaName) {
        // A typed table has exactly the columns its composite type declares, so there is no column
        // of its own for it to lose: dropping one would leave the table disagreeing with the type
        // it was declared OF. PostgreSQL refuses before it looks the column up at all, which is why
        // a name that is not there -- and IF EXISTS, and CASCADE -- get this refusal and not a
        // quiet success. ALTER TYPE is what changes the shape of every table built on the type.
        if (table.getOfTypeName() != null) {
            throw PgErrors.wrongObjectType("cannot drop column from typed table");
        }
        if (dropCol.ifExists() && table.getColumnIndex(dropCol.column()) < 0) {
            return;
        }
        int colIdx = table.getColumnIndex(dropCol.column());
        if (colIdx < 0) {
            throw new MemgresException("column \"" + dropCol.column()
                    + "\" of relation \"" + stmt.table() + "\" does not exist", "42703");
        }
        // An inherited column belongs to the parent; dropping it from the child alone would
        // leave a child that cannot answer for its parent's shape.
        if (isInheritedColumn(table, dropCol.column())) {
            throw new MemgresException("cannot drop inherited column \""
                    + dropCol.column() + "\"", "42P16");
        }
        rejectPartitionKeyColumnChange(table, dropCol.column(), "drop", stmt.table());
        // A partition holds exactly the parent's columns, so dropping one from the parent alone
        // would leave the two disagreeing about a shape they are required to share.
        if (stmt.only() && !table.getPartitions().isEmpty()) {
            MemgresException e = new MemgresException(
                    "cannot drop column from only the partitioned table when partitions exist",
                    "42P16");
            // Taking the one keyword out is the whole of what has to change, so PostgreSQL
            // names it rather than leaving the statement to be rewritten by guesswork.
            e.setHint("Do not specify the ONLY keyword.");
            throw e;
        }
        rejectDirectPartitionColumnChange(table, "drop column from a partition");
        // Which columns a generation expression really names comes from its parse tree. A
        // substring test made every one- or two-letter column look like a dependency of every
        // expression that happened to spell it inside a longer word.
        List<String> dependentGenCols = new ArrayList<>();
        for (Column c : table.getColumns()) {
            if (c.getGeneratedExpr() == null || c.getName().equalsIgnoreCase(dropCol.column())) {
                continue;
            }
            if (expressionNamesColumn(c.getGeneratedExpr(), dropCol.column())) {
                dependentGenCols.add(c.getName());
            }
        }
        List<Database.ViewDef> dependentViews =
                dependentViewsOf(schemaName, stmt.table(), dropCol.column());
        if (!dropCol.cascade()) {
            if (!dependentGenCols.isEmpty()) {
                MemgresException e = new MemgresException("cannot drop column " + dropCol.column()
                        + " of table " + stmt.table()
                        + " because other objects depend on it", "2BP01");
                // A generated column is written in terms of this one, and PostgreSQL names both
                // sides of that dependency with the table each column belongs to.
                List<String> lines = new ArrayList<>();
                for (String gen : dependentGenCols) {
                    lines.add("column " + gen + " of table " + stmt.table()
                            + " depends on column " + dropCol.column()
                            + " of table " + stmt.table());
                }
                e.setDetail(String.join("\n", lines));
                e.setHint("Use DROP ... CASCADE to drop the dependent objects too.");
                throw e;
            }
            if (!dependentViews.isEmpty()) {
                // Same shape as the generated-column arm above, which is what PostgreSQL sends:
                // 2BP01 is dependent_objects_still_exist, and 42P16 is a different class that a
                // client branching on the code does not recognise here.
                MemgresException e = new MemgresException("cannot drop column " + dropCol.column()
                        + " of table " + stmt.table()
                        + " because other objects depend on it", "2BP01");
                List<String> viewLines = new ArrayList<>();
                for (Database.ViewDef vd : dependentViews) {
                    viewLines.add("view " + vd.name() + " depends on column " + dropCol.column()
                            + " of table " + stmt.table());
                }
                e.setDetail(String.join("\n", viewLines));
                e.setHint("Use DROP ... CASCADE to drop the dependent objects too.");
                throw e;
            }
            // FOREIGN KEY constraints (on any table, including self-references) whose REFERENCED
            // columns include the dropped column depend on it via the unique index — PG requires
            // CASCADE to drop them.
            for (Schema sch : executor.database.getSchemas().values()) {
                for (Table t : sch.getTables().values()) {
                    for (StoredConstraint sc : t.getConstraints()) {
                        if (isFkReferencing(sc, stmt.table(), schemaName, dropCol.column())
                                && !StoredConstraint.containsIgnoreCase(sc.getColumns(), dropCol.column())) {
                            MemgresException e = new MemgresException("cannot drop column "
                                    + dropCol.column() + " of table " + stmt.table()
                                    + " because other objects depend on it", "2BP01");
                            // Both relations are named as the search path would have to name
                            // them: a column belongs to a table, and two schemas may each hold a
                            // table of the same name.
                            e.setDetail("constraint " + sc.getName() + " on table "
                                    + RelationNamespace.shownName(executor.searchPathSchemas(),
                                            sch.getName(), t.getName())
                                    + " depends on column " + dropCol.column()
                                    + " of table " + stmt.table());
                            e.setHint("Use DROP ... CASCADE to drop the dependent objects too.");
                            throw e;
                        }
                    }
                }
            }
        } else {
            // CASCADE says to drop what depends on the column, not to stop asking: a view left
            // standing would go on selecting a column that is no longer there.
            dropDependentViews(dependentViews);
        }
        Column droppedCol = table.getColumns().get(colIdx);
        // A sequence that belongs to this column goes with it: the one created for a serial or
        // identity column, and the one ALTER SEQUENCE ... OWNED BY attached. Leaving it behind
        // kept its name taken, so the same migration could not be run twice.
        dropSequencesOwnedBy(table, stmt.table(), dropCol.column(),
                Sequence.nameInDefault(droppedCol.getDefaultValue()));
        List<Object> colValues = new ArrayList<>();
        for (Object[] row : table.getRows()) {
            colValues.add(row[colIdx]);
        }
        recordDroppedColumnConstraints(table, dropCol.column(), schemaName, stmt.table());
        executor.recordUndo(new Session.DropColumnUndo(schemaName, stmt.table(), droppedCol, colIdx, colValues));
        // What the relation is handing down has to be read before it lets the column go: the
        // relations that keep the column go on counting these, whichever form of the statement
        // this is.
        List<String> handedDown = inheritableChecksOn(table, dropCol.column());
        String handedDownNotNull = CatalogConstraintBuilder.declaresNotNull(table, dropCol.column())
                ? CatalogConstraintBuilder.notNullConstraintName(table, dropCol.column()) : null;
        table.removeColumn(dropCol.column());
        // Without ONLY the parent's shape is the hierarchy's shape, so the column goes from the
        // children too; with ONLY the children keep it as a column of their own.
        if (stmt.only()) makeDroppedColumnLocal(table, dropCol.column());
        else propagateDropColumn(table, dropCol.column());
        keepInheritedConstraintCounts(table, dropCol.column(), handedDown, handedDownNotNull);
        // Drop incoming FOREIGN KEY constraints that referenced the dropped column (reached only
        // with CASCADE, or when the FK's own columns also contained the dropped column).
        for (Schema sch : executor.database.getSchemas().values()) {
            for (Table t : sch.getTables().values()) {
                List<String> fkToDrop = new ArrayList<>();
                for (StoredConstraint sc : t.getConstraints()) {
                    if (isFkReferencing(sc, stmt.table(), schemaName, dropCol.column())) {
                        fkToDrop.add(sc.getName());
                    }
                }
                for (String fkName : fkToDrop) {
                    if (fkName != null) t.removeConstraint(fkName);
                }
            }
        }
        // Drop database-level index metadata for indexes that used the dropped column, so they
        // don't linger in pg_indexes (PG drops such indexes automatically).
        String qualifiedTable = schemaName + "." + stmt.table();
        List<String> idxToDrop = new ArrayList<>();
        java.util.regex.Pattern colWord = java.util.regex.Pattern.compile(
                "(?i)\\b" + java.util.regex.Pattern.quote(dropCol.column()) + "\\b");
        for (Map.Entry<String, List<String>> idxEntry : executor.database.getIndexColumns().entrySet()) {
            String idxTable = executor.database.getIndexTable(idxEntry.getKey());
            if (idxTable == null || !idxTable.equalsIgnoreCase(qualifiedTable)) continue;
            boolean usesColumn = false;
            if (idxEntry.getValue() != null) {
                for (String c : idxEntry.getValue()) {
                    if (c.equalsIgnoreCase(dropCol.column()) || colWord.matcher(c).find()) {
                        usesColumn = true;
                        break;
                    }
                }
            }
            String whereClause = executor.database.getIndexWhereClause(idxEntry.getKey());
            if (!usesColumn && whereClause != null && colWord.matcher(whereClause).find()) {
                usesColumn = true;
            }
            if (usesColumn) idxToDrop.add(idxEntry.getKey());
        }
        for (String idxName : idxToDrop) {
            executor.database.removeIndex(idxName);
        }
        // CASCADE: also drop dependent generated columns
        if (dropCol.cascade() && !dependentGenCols.isEmpty()) {
            for (String depCol : dependentGenCols) {
                int depIdx = table.getColumnIndex(depCol);
                if (depIdx >= 0) {
                    Column depColumn = table.getColumns().get(depIdx);
                    List<Object> depValues = new ArrayList<>();
                    for (Object[] row : table.getRows()) {
                        depValues.add(row[depIdx]);
                    }
                    executor.recordUndo(new Session.DropColumnUndo(schemaName, stmt.table(), depColumn, depIdx, depValues));
                    table.removeColumn(depCol);
                }
            }
        }
    }

    /** True if sc is a FOREIGN KEY whose referenced table/column match the given table and column. */
    private static boolean isFkReferencing(StoredConstraint sc, String tableName, String schemaName, String column) {
        return sc.getType() == StoredConstraint.Type.FOREIGN_KEY
                && sc.getReferencesTable() != null
                && sc.getReferencesTable().equalsIgnoreCase(tableName)
                && (sc.getReferencesSchema() == null || sc.getReferencesSchema().equalsIgnoreCase(schemaName))
                && StoredConstraint.containsIgnoreCase(sc.getReferencesColumns(), column);
    }

    private Table executeRenameTable(AlterTableStmt.RenameTable rename, Table table,
                                      AlterTableStmt stmt, String schemaName) {
        if (rename.newName() == null) return table;
        Schema schema = executor.database.getSchema(schemaName);
        // The new name has to be free of every kind of relation, not only of another table.
        RelationNamespace.requireFree(executor.database, schemaName, rename.newName(), null);
        // ...and then free of every kind of type, because the table's row type has to be renamed
        // with it. The relation check runs first: renaming onto another table is 42P07 and
        // renaming onto an enum, a domain, a range or a shell is 42710, both measured.
        TypeNamespace.requireRenameableRowType(executor.database, schemaName, rename.newName());
        if (schema.getTable(rename.newName()) != null) {
            throw new MemgresException("relation \"" + rename.newName() + "\" already exists", "42P07");
        }
        // The same relation under another name. Building a replacement carried across only the
        // fields the copy remembered, so a renamed partition lost its bound and its parent, a
        // renamed parent lost its children, and UNLOGGED, REPLICA IDENTITY, FORCE ROW LEVEL
        // SECURITY, the storage parameters, the policies and the NOT NULL constraint names were
        // all left on the discarded object.
        schema.removeTable(stmt.table());
        table.setName(rename.newName());
        schema.addTable(table);
        Table renamed = table;
        // A sequence that belongs to this table is recorded against the table's name, so the
        // rename has to reach it or the sequence stops going where the table goes.
        for (Sequence owned : executor.database.getSequences().values()) {
            if (stmt.table().equalsIgnoreCase(owned.getOwnedByTable())) {
                owned.setOwnedByTable(rename.newName());
            }
        }
        // A rename is a name appearing and a name going away, and neither has happened for
        // anyone else until this transaction commits: the new name is hidden the way any
        // uncommitted relation is, the old one is held the way any uncommitted drop is, and the
        // relation itself goes on giving its old name to every session but this one — it is one
        // object under two names, so without that the new name is read straight off it.
        executor.database.markUncommittedObject(renamed, executor.session);
        renamed.markUncommittedRename(stmt.table(), executor.session);
        // A temp table's ON COMMIT action survives its rename in PostgreSQL: the action belongs
        // to the relation, and this is the same relation answering to a new name.
        if (executor.session != null) {
            executor.session.retargetOnCommitDeleteRows(schemaName, stmt.table(), table,
                    rename.newName(), renamed);
        }
        executor.recordUndo(new Session.RenameTableUndo(
                schemaName, stmt.table(), rename.newName(), table));
        // The same table under a new name: the OID stays with it, and so do the comment, the
        // grants and the indexes built on it. Told, not inferred -- see ObjectIdentity.
        executor.identity().relationRenamed("r", schemaName, stmt.table(),
                schemaName, rename.newName());
        carryComments(schemaName, stmt.table(), schemaName, rename.newName());
        retargetDependents(schemaName, stmt.table(), schemaName, rename.newName());
        return renamed;
    }

    /**
     * A renamed or moved relation keeps what was said about it. PostgreSQL keys a comment by the
     * object's OID, which a rename does not change; memgres keys it by schema and name, so the
     * relation's own comment and every one of its columns' has to be carried across.
     */
    void carryComments(String oldSchema, String oldName, String newSchema, String newName) {
        Database db = executor.database;
        String from = Database.commentKey(oldSchema, oldName);
        String to = Database.commentKey(newSchema, newName);
        for (String kind : COMMENTED_RELATION_KINDS) {
            db.moveComment(kind, from, to);
        }
        // Column, constraint, trigger, rule and policy comments are keyed under the relation.
        for (String kind : RELATION_SCOPED_KINDS) {
            db.moveCommentsUnder(kind, from + ".", to + ".");
        }
    }

    private static final String[] COMMENTED_RELATION_KINDS = {
            "table", "relation", "view", "materialized view", "index", "sequence", "foreign table"};

    private static final String[] RELATION_SCOPED_KINDS = {
            "column", "constraint", "trigger", "rule", "policy"};

    /**
     * Follow a renamed or moved relation from everything that names it. PG records these
     * dependencies as OIDs, so a rename leaves foreign keys enforcing and views reading the
     * same relation; memgres stores names, so they have to be rewritten here.
     */
    private void retargetDependents(String oldSchema, String oldName, String newSchema, String newName) {
        boolean moved = newSchema != null && !newSchema.equalsIgnoreCase(oldSchema);
        for (Map.Entry<String, Schema> se : executor.database.getSchemas().entrySet()) {
            for (Table t : new ArrayList<>(se.getValue().getTables().values())) {
                for (StoredConstraint sc : t.getConstraints()) {
                    if (sc.getType() != StoredConstraint.Type.FOREIGN_KEY) continue;
                    if (!oldName.equalsIgnoreCase(sc.getReferencesTable())) continue;
                    String refSchema = sc.getReferencesSchema();
                    if (refSchema != null && !refSchema.equalsIgnoreCase(oldSchema)) continue;
                    sc.setReferencesTable(newName);
                    if (moved) sc.setReferencesSchema(newSchema);
                }
            }
        }
        for (Database.ViewDef view : new ArrayList<>(executor.database.getViews().values())) {
            if (AstRelationRenamer.retarget(view.query(), oldSchema, oldName, newSchema, newName)) {
                view.sourceSQL = rewriteSourceSql(view.sourceSQL(), oldSchema, oldName, newSchema, newName);
            }
        }
    }

    /** Rewrite the relation name inside a stored view definition so pg_get_viewdef reads true. */
    private static String rewriteSourceSql(String sql, String oldSchema, String oldName,
                                           String newSchema, String newName) {
        if (sql == null) return null;
        boolean moved = newSchema != null && !newSchema.equalsIgnoreCase(oldSchema);
        String replacement = moved ? newSchema + "." + newName : newName;
        String qualified = "(?i)\\b" + java.util.regex.Pattern.quote(oldSchema) + "\\."
                + java.util.regex.Pattern.quote(oldName) + "\\b";
        String bare = "(?i)\\b" + java.util.regex.Pattern.quote(oldName) + "\\b";
        return sql.replaceAll(qualified, java.util.regex.Matcher.quoteReplacement(replacement))
                  .replaceAll(bare, java.util.regex.Matcher.quoteReplacement(replacement));
    }

    private void executeAlterColumn(AlterTableStmt.AlterColumn alterCol, Table table,
                                     AlterTableStmt stmt, String schemaName) {
        if (alterCol.action() instanceof AlterTableStmt.SetType) {
            AlterTableStmt.SetType setType = (AlterTableStmt.SetType) alterCol.action();
            executeSetType(alterCol, setType, table, stmt, schemaName);
        } else if (alterCol.action() instanceof AlterTableStmt.SetDefault) {
            AlterTableStmt.SetDefault setDefault = (AlterTableStmt.SetDefault) alterCol.action();
            rejectDefaultOnGeneratedColumn(table, alterCol.column(), stmt.table(), "SET");
            executeSetDefault(alterCol, setDefault, table, stmt);
        } else if (alterCol.action() instanceof AlterTableStmt.DropDefault) {
            rejectDefaultOnGeneratedColumn(table, alterCol.column(), stmt.table(), "DROP");
            table.alterColumnDefault(alterCol.column(), null);
            propagateColumnDefault(table, alterCol.column(), null, stmt.only());
        } else if (alterCol.action() instanceof AlterTableStmt.SetNotNull) {
            rejectOnlyNotNullOnPartitioned(table, stmt, alterCol.column());
            requireColumnHasNoNulls(table, alterCol.column(), stmt.table());
            recordNotNullUndo(table, alterCol.column());
            // A partitioned parent stores no rows of its own, and an inheritance child's rows are
            // the parent's rows too, so the data that decides whether the rule can hold at all
            // lives below: PostgreSQL scans the descendants and names the one at fault.
            if (!stmt.only()) {
                for (Table descendant : descendantRelations(table)) {
                    requireColumnHasNoNulls(descendant, alterCol.column(), descendant.getName());
                }
            }
            table.alterColumnNullable(alterCol.column(), false);
            // Each of them keeps its own copy of the column list, so the flag has to reach them
            // or the parent forbids a null its own descendants go on taking.
            if (!stmt.only()) {
                setColumnsNotNull(table, Collections.singletonList(alterCol.column()));
            }
            // The rows have just been read, so a constraint that had been left waiting for them is
            // not waiting any longer: PostgreSQL marks it validated rather than making the writer
            // ask for the same scan twice.
            markNotNullValidated(table, alterCol.column(), !stmt.only());
        } else if (alterCol.action() instanceof AlterTableStmt.DropNotNull) {
            Column nullable = requireColumn(table, alterCol.column(), stmt.table());
            // Identity fills the column for every row, so there is no nullability to drop.
            // PostgreSQL refuses at the identity level, ahead of the primary key rule.
            if (isIdentityColumn(nullable)) {
                throw new MemgresException("column \"" + alterCol.column() + "\" of relation \""
                        + stmt.table() + "\" is an identity column", "42601");
            }
            // A primary key column can never hold a null, so a catalog saying it may is a
            // contradiction with the constraint that still rejects one.
            if (isPrimaryKeyColumn(table, alterCol.column())) {
                throw new MemgresException("column \"" + alterCol.column()
                        + "\" is in a primary key", "42P16");
            }
            // The rule belongs to whichever relation declared it, so a descendant is sent to that
            // one rather than allowed to weaken what the parent answers for.
            rejectDropNotNullUnderParent(table, alterCol.column(), stmt.table(), false);
            recordNotNullUndo(table, alterCol.column());
            // What each child holds is settled while this relation still declares the rule, so the
            // name it answers to is still the one the constraint was created with.
            pinRetainedNotNullNames(table, alterCol.column());
            if (stmt.only()) makeNotNullLocalOnChildren(table, alterCol.column());
            table.alterColumnNullable(alterCol.column(), true);
            // The constraint is gone, and a later SET NOT NULL makes a new one under the
            // default name rather than resurrecting the name this one carried.
            table.setNotNullConstraintName(alterCol.column(), null);
            if (!stmt.only()) clearNotNullOnDescendants(table, alterCol.column());
        } else if (alterCol.action() instanceof AlterTableStmt.DropIdentity) {
            executeDropIdentity((AlterTableStmt.DropIdentity) alterCol.action(),
                    alterCol.column(), table, stmt);
        } else if (alterCol.action() instanceof AlterTableStmt.DropExpression) {
            executeDropExpression((AlterTableStmt.DropExpression) alterCol.action(),
                    alterCol.column(), table, stmt);
        } else if (alterCol.action() instanceof AlterTableStmt.SetExpression) {
            executeSetExpression((AlterTableStmt.SetExpression) alterCol.action(),
                    alterCol.column(), table, stmt);
        } else if (alterCol.action() instanceof AlterTableStmt.SetColumnOptions) {
            requireColumn(table, alterCol.column(), stmt.table());
            checkAttributeOptions(
                    ((AlterTableStmt.SetColumnOptions) alterCol.action()).options().keySet());
        } else if (alterCol.action() instanceof AlterTableStmt.ResetColumnOptions) {
            requireColumn(table, alterCol.column(), stmt.table());
            checkAttributeOptions(
                    ((AlterTableStmt.ResetColumnOptions) alterCol.action()).options());
        } else if (alterCol.action() instanceof AlterTableStmt.SetStatistics) {
            AlterTableStmt.SetStatistics ss = (AlterTableStmt.SetStatistics) alterCol.action();
            int colIdx = table.getColumnIndex(alterCol.column());
            if (colIdx < 0) throw new MemgresException("column \"" + alterCol.column() + "\" of relation \"" + stmt.table() + "\" does not exist", "42703");
            // -1 puts the column back to the system default, which the catalogue reports as null;
            // below that there is no target to set. Above PostgreSQL's ceiling the statement is
            // taken but the value is clamped, so pg_attribute reports 10000 rather than what was
            // written -- and a short could not hold even that.
            if (ss.target() < -1) {
                throw PgErrors.invalidParameter(
                        "statistics target " + ss.target() + " is too low");
            }
            table.getColumns().get(colIdx).setAttStattarget(ss.target() == -1 ? null
                    : Integer.valueOf(Math.min(ss.target(), 10000)));
        } else if (alterCol.action() instanceof AlterTableStmt.SetStorage) {
            AlterTableStmt.SetStorage ss = (AlterTableStmt.SetStorage) alterCol.action();
            Column col = requireColumn(table, alterCol.column(), stmt.table());
            col.setAttStorageOverride(DdlDefinitionChecks.storageCode(ss.storageType(), col));
        } else if (alterCol.action() instanceof AlterTableStmt.SetCompression) {
            AlterTableStmt.SetCompression sc = (AlterTableStmt.SetCompression) alterCol.action();
            Column col = requireColumn(table, alterCol.column(), stmt.table());
            col.setAttCompression(DdlDefinitionChecks.compressionCode(sc.method(), col));
        } else if (alterCol.action() instanceof AlterTableStmt.AlterIdentitySequence) {
            executeAlterIdentitySequence((AlterTableStmt.AlterIdentitySequence) alterCol.action(),
                    alterCol.column(), table, stmt);
        } else if (alterCol.action() instanceof AlterTableStmt.ColumnNoOp) {
            // no-op
        }
    }

    /**
     * SET INCREMENT BY / START WITH / MINVALUE / MAXVALUE / CACHE / CYCLE on an identity column.
     *
     * <p>The options are the sequence's own, so they go through the validator ALTER SEQUENCE uses
     * and are refused in the same words — a START left below a new MINVALUE being the one that
     * otherwise reads as an accepted statement. Only an identity column has a sequence of its own
     * to alter: a serial column has one too, but PostgreSQL keeps the two apart and refuses this
     * there as well.
     */
    private void executeAlterIdentitySequence(AlterTableStmt.AlterIdentitySequence action,
                                              String column, Table table, AlterTableStmt stmt) {
        Column col = requireColumn(table, column, stmt.table());
        if (!isIdentityColumn(col)) {
            throw new MemgresException("column \"" + column + "\" of relation \""
                    + stmt.table() + "\" is not an identity column", "55000");
        }
        Sequence seq = executor.database.getSequenceFor(table.getSchemaName(),
                Sequence.nameInDefault(col.getDefaultValue()));
        if (seq == null) return;
        // NO MINVALUE and NO MAXVALUE put the bound back to what the sequence's own type gives a
        // sequence that never named one: 1 and the type's ceiling going up, the type's floor and
        // -1 going down.
        long step = action.increment() != null ? action.increment().longValue() : seq.getIncrementBy();
        Long minValue = action.minValue();
        if (action.noMinValue()) {
            minValue = Long.valueOf(step > 0 ? 1L : DdlSequenceValidator.typeMin(seq.getDataType()));
        }
        Long maxValue = action.maxValue();
        if (action.noMaxValue()) {
            maxValue = Long.valueOf(step > 0 ? DdlSequenceValidator.typeMax(seq.getDataType()) : -1L);
        }
        DdlSequenceValidator.Params p = DdlSequenceValidator.forAlter(seq, null, action.increment(),
                minValue, maxValue, action.startWith(), false, null, action.cache());
        DdlSequenceValidator.apply(seq, p);
        if (action.cycle() != null) seq.setCycle(action.cycle().booleanValue());
    }

    /**
     * A generated column's value comes from its expression on every row, so a default has
     * nothing to fill in and setting or dropping one is refused rather than stored and ignored.
     * What the writer meant is the same statement with EXPRESSION in place of DEFAULT, and
     * PostgreSQL names it rather than leaving them to find it.
     *
     * @param verb SET or DROP, the word the statement used and the one the advice repeats
     */
    private static void rejectDefaultOnGeneratedColumn(Table table, String column, String tableName,
                                                       String verb) {
        int idx = table.getColumnIndex(column);
        if (idx < 0) return; // the missing column is reported by the action itself
        if (table.getColumns().get(idx).isGenerated()) {
            MemgresException e = PgErrors.syntax("column \"" + column + "\" of relation \""
                    + tableName + "\" is a generated column");
            e.setHint("Use ALTER TABLE ... ALTER COLUMN ... " + verb + " EXPRESSION instead.");
            throw e;
        }
    }

    /** The named column, or {@code 42703} spelled the way PostgreSQL spells it. */
    private static Column requireColumn(Table table, String column, String tableName) {
        int idx = table.getColumnIndex(column);
        if (idx < 0) {
            throw new MemgresException("column \"" + column + "\" of relation \""
                    + tableName + "\" does not exist", "42703");
        }
        return table.getColumns().get(idx);
    }

    /** True when the column is part of the table's PRIMARY KEY, however that key was declared. */
    private static boolean isPrimaryKeyColumn(Table table, String column) {
        int idx = table.getColumnIndex(column);
        if (idx >= 0 && table.getColumns().get(idx).isPrimaryKey()) return true;
        for (StoredConstraint sc : table.getConstraints()) {
            if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY
                    && StoredConstraint.containsIgnoreCase(sc.getColumns(), column)) {
                return true;
            }
        }
        return false;
    }

    /**
     * DROP IDENTITY, unlike DROP DEFAULT, complains when there is no identity to drop — the
     * statement's whole purpose is to remove one, so silence would hide a mistaken column name.
     */
    private void executeDropIdentity(AlterTableStmt.DropIdentity action, String column,
                                      Table table, AlterTableStmt stmt) {
        Column col = requireColumn(table, column, stmt.table());
        if (!isIdentityColumn(col)) {
            if (action.ifExists()) return;
            throw new MemgresException("column \"" + column + "\" of relation \""
                    + stmt.table() + "\" is not an identity column", "55000");
        }
        // The sequence exists only to feed this column, so removing the identity removes it. The
        // name has to be read before the default that carries it is cleared, or nothing is left
        // saying which sequence it was — and a later ADD GENERATED then reused the stale one.
        dropSequencesOwnedBy(table, stmt.table(), column,
                Sequence.nameInDefault(col.getDefaultValue()));
        table.alterColumnDefault(column, null);
    }

    /**
     * DROP EXPRESSION turns a stored generated column into an ordinary one, keeping the values
     * already computed. A virtual column has no stored values to keep, so PG does not offer it.
     */
    /**
     * The options a plain column carries. PostgreSQL takes only the two that tell the planner how
     * many distinct values to expect, and names anything else rather than storing it.
     */
    private static void checkAttributeOptions(java.util.Collection<String> names) {
        for (String name : names) {
            if ("n_distinct".equalsIgnoreCase(name)
                    || "n_distinct_inherited".equalsIgnoreCase(name)) {
                continue;
            }
            throw PgErrors.invalidParameter("unrecognized parameter \"" + name + "\"");
        }
    }

    /**
     * ALTER COLUMN ... SET EXPRESSION AS (expr) gives a generated column a new expression, and
     * PostgreSQL recomputes every stored row from it at once — that is what makes the statement a
     * rewrite rather than a catalog change. A column that generates nothing has no expression to
     * replace, and PostgreSQL says so rather than quietly making it generated.
     */
    private void executeSetExpression(AlterTableStmt.SetExpression action, String column,
                                      Table table, AlterTableStmt stmt) {
        Column col = requireColumn(table, column, stmt.table());
        if (col.getGeneratedExpr() == null) {
            throw new MemgresException("column \"" + column + "\" of relation \""
                    + stmt.table() + "\" is not a generated column", "55000");
        }
        DdlExecutor.checkExpressionImmutability(action.expression(), executor.database,
                "generation expression is not immutable");
        if (action.expression().toLowerCase(java.util.Locale.ROOT).replaceAll("\\s+", "").contains("select")) {
            throw new MemgresException(
                    "cannot use subquery in column generation expression", "0A000");
        }
        try {
            Expression parsed = com.memgres.engine.parser.Parser.parseExpression(action.expression());
            StoredExprNames.read(ddl, parsed, table, column, false, true);
            ddl.validateExprColumnRefs(parsed, table, column, false, true);
        } catch (MemgresException me) {
            throw me;
        } catch (Exception ignored) {
            // An expression that will not parse is reported by whatever reads it next
        }
        int idx = table.getColumnIndex(column);
        Column updated = col.withGeneratedExpr(action.expression());
        table.getColumns().set(idx, updated);
        // A VIRTUAL generated column is computed on read, so only a stored one has rows to redo.
        if (!updated.isVirtual()) {
            for (Object[] row : table.getRows()) {
                row[idx] = executor.dmlExecutor.evalGeneratedColumn(table, row, updated);
            }
        }
    }

    private void executeDropExpression(AlterTableStmt.DropExpression action, String column,
                                        Table table, AlterTableStmt stmt) {
        Column col = requireColumn(table, column, stmt.table());
        if (col.getGeneratedExpr() == null) {
            if (action.ifExists()) return;
            throw new MemgresException("column \"" + column + "\" of relation \""
                    + stmt.table() + "\" is not a generated column", "55000");
        }
        if (col.isVirtual()) {
            MemgresException e = PgErrors.notImplemented(
                    "ALTER TABLE / DROP EXPRESSION is not supported for virtual generated columns");
            // The message says which kind of column is refused; the detail says which column.
            e.setDetail("Column \"" + column + "\" of relation \"" + stmt.table()
                    + "\" is a virtual generated column.");
            throw e;
        }
        int idx = table.getColumnIndex(column);
        table.getColumns().set(idx, col.withGeneratedExpr(null));
    }

    /**
     * True when the column really is an identity column. A SERIAL column is deliberately not one:
     * it has a sequence behind it, but PostgreSQL keeps the two concepts apart and DROP IDENTITY
     * on a serial column is an error there.
     */
    private static boolean isIdentityColumn(Column col) {
        String def = col.getDefaultValue();
        return def != null && def.startsWith("__identity__");
    }

    /**
     * ALTER COLUMN ... TYPE, over the whole inheritance hierarchy the column belongs to.
     *
     * <p>A column a table gets from a parent is the parent's column: PostgreSQL refuses to retype
     * it on the child alone, because the two would then disagree about the same column and a read
     * through the parent would find the child holding another type. The change has to be made on
     * the table that declares the column, and from there it reaches every descendant — which is
     * also why ONLY is refused on a table that has children.
     */
    private void executeSetType(AlterTableStmt.AlterColumn alterCol, AlterTableStmt.SetType setType,
                                 Table table, AlterTableStmt stmt, String schemaName) {
        // A USING expression is resolved against the relation while the statement is still being
        // read, before anything about the retype itself has been settled -- so a name it cannot
        // resolve is reported as a plain undefined column, with no relation clause, ahead of the
        // column being retyped, the type it is being retyped to and every rule about which table
        // may carry the change. The relation's own system columns are names the expression may
        // reach, so they resolve. Left unresolved, the expression was only ever evaluated per row,
        // which meant a retype of an empty table took a USING clause naming nothing at all.
        // A transform expression converts one row's old value into the new one, on that row alone.
        // There is no query around it for a sub-query to run in, no group for an aggregate to be
        // taken over and no window for a window call to be numbered against, so PostgreSQL refuses
        // all three where it reads the clause. memgres evaluated whatever was written per row and
        // let them through -- ALTER ... USING count(*) rewrote the table with a count in it.
        if (setType.usingExpr() != null) {
            StoredExprNames.read(ddl, setType.usingExpr(), table, null, true, true);
            ddl.validateExprColumnRefs(setType.usingExpr(), table, null, true, true);
            executor.selectExecutor.placementCheck.rejectStoredDefinition(
                    setType.usingExpr(), "transform expressions", "transform expression");
        }
        // PostgreSQL settles which column is being retyped before it settles what it is being
        // retyped to, so a statement that gets both wrong is reported as the undefined column: a
        // type name nothing answers to -- serial among them, being CREATE TABLE shorthand rather
        // than a type -- says nothing about a column the relation does not carry at all.
        requireColumn(table, alterCol.column(), stmt.table());
        // A typed table's columns are its composite type's, so retyping one would leave the table
        // disagreeing with the type it was declared OF; ALTER TYPE is what changes the shape of
        // every table built on that type. Unlike RENAME COLUMN, PostgreSQL looks the column up
        // first -- a name that is not there is an undefined column, not this -- and then refuses
        // everything else, including a retype to the type the column already has and one naming a
        // type that does not exist.
        if (table.getOfTypeName() != null && table.getColumnIndex(alterCol.column()) >= 0) {
            throw PgErrors.wrongObjectType("cannot alter column type of typed table");
        }
        // serial is CREATE TABLE shorthand rather than a type, so there is nothing to retype a
        // column to and PostgreSQL says no type of that name exists.
        DdlDefinitionChecks.rejectSerialPseudotype(setType.typeName());
        int generatedIdx = table.getColumnIndex(alterCol.column());
        // A generated column's values come from its expression on every row, so there is no old
        // value for a USING clause to convert.
        if (generatedIdx >= 0 && setType.usingExpr() != null
                && table.getColumns().get(generatedIdx).getGeneratedExpr() != null) {
            MemgresException e = new MemgresException(
                    "cannot specify USING when altering type of generated column", "42611");
            e.setDetail("Column \"" + alterCol.column() + "\" is a generated column.");
            throw e;
        }
        if (table.getColumnIndex(alterCol.column()) >= 0) {
            if (isInheritedColumn(table, alterCol.column())) {
                throw new MemgresException("cannot alter inherited column \""
                        + alterCol.column() + "\"", "42P16");
            }
            rejectPartitionKeyColumnChange(table, alterCol.column(), "alter", stmt.table());
            checkRetypeCollation(setType);
            if (stmt.only() && !childRelations(table).isEmpty()) {
                throw new MemgresException("type of inherited column \"" + alterCol.column()
                        + "\" must be changed in child tables too", "42P16");
            }
        }
        retypeColumn(alterCol, setType, table, stmt.table(), schemaName);
        propagateSetType(alterCol, setType, table, schemaName);
    }

    /** Apply the same retype to every descendant that carries the column. */
    private void propagateSetType(AlterTableStmt.AlterColumn alterCol, AlterTableStmt.SetType setType,
                                  Table table, String schemaName) {
        for (Table child : childRelations(table)) {
            if (child.getColumnIndex(alterCol.column()) < 0) continue;
            retypeColumn(alterCol, setType, child, child.getName(), schemaName);
            propagateSetType(alterCol, setType, child, schemaName);
        }
    }

    /**
     * What a COLLATE clause written on a retype is checked for, in PostgreSQL's order: the name
     * written has to name a type, the collation name a collation, and only then is the type asked
     * whether it carries a collation at all. The target type is settled the way a written column
     * definition settles it, so the clause is judged against the same answer ADD COLUMN reaches --
     * a name nothing answers to, a width the type cannot have and a name that is still only a
     * shell are all reported from there, ahead of the clause, because none of them leaves a type
     * the clause could have been written on.
     */
    private void checkRetypeCollation(AlterTableStmt.SetType setType) {
        if (setType.collation() == null) return;
        DdlExecutor.ResolvedType resolved = ddl.resolveColumnType(setType.typeName(), null);
        DdlDefinitionChecks.requireCollationExists(executor.database, setType.collation());
        DdlDefinitionChecks.rejectUncollatableType(setType.typeName(), resolved,
                setType.collation());
    }

    private void retypeColumn(AlterTableStmt.AlterColumn alterCol, AlterTableStmt.SetType setType,
                              Table table, String tableName, String schemaName) {
        String baseType = setType.typeName().replaceAll("\\(.*\\)", "").replace("[]", "").trim();
        // Extract the new type's typmod (precision/scale) so it replaces the old column's —
        // e.g. ALTER COLUMN capacity TYPE numeric(10, 2) must set precision 10 / scale 2, or
        // NUMERIC storage coercion (TypeCoercion.applyPrecision) never enforces the declared
        // scale and values round-trip at whatever incidental scale they arrived with.
        Integer newPrecision = null;
        Integer newScale = null;
        java.util.regex.Matcher typmod = java.util.regex.Pattern
                .compile("\\(\\s*(\\d+)\\s*(?:,\\s*(-?\\d+)\\s*)?\\)")
                .matcher(setType.typeName());
        if (typmod.find()) {
            try {
                newPrecision = Integer.valueOf(typmod.group(1));
                if (typmod.group(2) != null) newScale = Integer.valueOf(typmod.group(2));
            } catch (NumberFormatException ignored) {
                newPrecision = null;
                newScale = null;
            }
        }
        boolean isArrayType = setType.typeName().replaceAll("\\(.*\\)", "").trim().endsWith("[]");
        DataType dt = DataType.fromPgName(baseType);
        String newEnumTypeName = null;
        String newDomainTypeName = null;
        String newCompositeTypeName = null;
        String newRangeTypeName = null;
        String newTypeDisplayName = null;
        DataType newArrayElementType = null;
        if (dt == null) {
            // PostgreSQL keeps one namespace of types, and the name a retype writes comes out of
            // it exactly as the name a written column definition does: a domain, an enum, a
            // composite and a range are each a type a column may be given. Settled here by a
            // shorter reader that had been taught only the enums, every other user-defined name
            // was reported as a type nothing answers to. The enum identity, and the array-ness
            // that goes with it, come from that reader too -- without them the retyped column
            // advertised the unresolvable ENUM placeholder OID.
            DdlExecutor.ResolvedType resolved = ddl.resolveColumnType(setType.typeName(), null);
            dt = resolved.dataType();
            newEnumTypeName = resolved.enumTypeName();
            newDomainTypeName = resolved.domainTypeName();
            newCompositeTypeName = resolved.compositeTypeName();
            newRangeTypeName = resolved.rangeTypeName();
            newArrayElementType = resolved.arrayElementType();
            // What PostgreSQL calls the type in a complaint is the type's own name, written the
            // way this session would write it, and never the representation its values are kept
            // in: a column refused a composite is told the composite's name.
            newTypeDisplayName = newDomainTypeName != null
                    ? resolved.domainDisplayName() : resolved.userTypeDisplayName();
        }
        int colIdx = table.getColumnIndex(alterCol.column());
        if (colIdx < 0) {
            throw new MemgresException("column \"" + alterCol.column() + "\" of relation \"" + tableName + "\" does not exist", "42703");
        }
        // The column is looked up before the target type's modifier is resolved, so a retype of a
        // column that is not there is reported as that. ADD COLUMN checks the same limits through
        // resolveColumnType; a retype names the same types and must reject the same widths.
        TypeCoercion.checkDeclaredTypeLimits(setType.typeName());
        // Check for generated column dependencies
        for (Column c : table.getColumns()) {
            if (c.getGeneratedExpr() != null && !c.getName().equalsIgnoreCase(alterCol.column())
                    && expressionNamesColumn(c.getGeneratedExpr(), alterCol.column())) {
                MemgresException e = new MemgresException(
                        "cannot alter type of a column used by a generated column", "0A000");
                // Which generated column is in the way is the one thing the message leaves
                // out, and it is what has to be dropped before the retype can go through.
                e.setDetail("Column \"" + alterCol.column() + "\" is used by generated column \""
                        + c.getName() + "\".");
                throw e;
            }
        }
        DataType currentType = table.getColumns().get(colIdx).getType();
        // An enum, a composite and a range are types in their own right, whatever this engine
        // keeps their values in, and PostgreSQL has no assignment cast into any of them: the only
        // column a retype may give one to without a USING clause is a column already of that very
        // type. Judged by the representation alone -- text for a composite and for a range -- a
        // text column and a column of some other one of them looked like the same type, and the
        // retype went through in silence.
        String heldUserType = newCompositeTypeName != null
                ? table.getColumns().get(colIdx).getCompositeTypeName()
                : newRangeTypeName != null ? table.getColumns().get(colIdx).getRangeTypeName()
                : newEnumTypeName != null ? table.getColumns().get(colIdx).getEnumTypeName() : null;
        String wantedUserType = newCompositeTypeName != null ? newCompositeTypeName
                : newRangeTypeName != null ? newRangeTypeName : newEnumTypeName;
        if (setType.usingExpr() == null && wantedUserType != null
                && !wantedUserType.equalsIgnoreCase(heldUserType)) {
            throw retypeNeedsUsing(alterCol.column(), newTypeDisplayName, isArrayType);
        }
        if (setType.usingExpr() == null && currentType != null && dt != null && currentType != dt) {
            TypeCoercion.TypeCategory fromCat = TypeCoercion.categoryOf(currentType);
            TypeCoercion.TypeCategory toCat = TypeCoercion.categoryOf(dt);
            // Without USING, PG only applies assignment casts: conversions within the same type
            // category (varchar<->text, int->bigint, ...) and conversions TO a string-category
            // type (any type is I/O-coercible to text in assignment context). Conversions FROM a
            // string type to anything else (text->integer, text->date, varchar->enum, ...) are
            // explicit-only and must fail with 42804.
            if (fromCat != toCat && toCat != TypeCoercion.TypeCategory.STRING) {
                throw retypeNeedsUsing(alterCol.column(),
                        newTypeDisplayName != null ? newTypeDisplayName : dt.toRegtypeDisplay(),
                        isArrayType);
            }
        }
        // Check for view dependencies. Which views really read the column is read off their parse
        // trees, the same question DROP COLUMN asks, so the two refuse for the same reasons.
        for (Database.ViewDef vd : dependentViewsOf(schemaName, tableName, alterCol.column())) {
            MemgresException e = new MemgresException(
                    "cannot alter type of a column used by a view or rule", "0A000");
            // A view reads its table through a rule, and the rule is the object in the way, so
            // PostgreSQL names it beside the view it belongs to and the column it reads.
            e.setDetail("rule _RETURN on view " + vd.name() + " depends on column \""
                    + alterCol.column() + "\"");
            throw e;
        }
        // Check for index dependencies
        if (currentType != dt && setType.usingExpr() == null) {
            String colNameLower = alterCol.column().toLowerCase(java.util.Locale.ROOT);
            String checkTable = schemaName + "." + tableName;
            for (Map.Entry<String, java.util.List<String>> idxEntry : executor.database.getIndexColumns().entrySet()) {
                java.util.List<String> idxCols = idxEntry.getValue();
                String indexMetaTable = executor.database.getIndexTable(idxEntry.getKey());
                if (indexMetaTable != null && indexMetaTable.equalsIgnoreCase(checkTable)
                        && idxCols != null && idxCols.stream().anyMatch(c -> c.equalsIgnoreCase(colNameLower))) {
                    TypeCoercion.TypeCategory fromCat = TypeCoercion.categoryOf(currentType);
                    TypeCoercion.TypeCategory toCat = TypeCoercion.categoryOf(dt);
                    if (fromCat != toCat) {
                        throw new MemgresException(
                                "operator class for access method \"btree\" does not accept data type "
                                        + dt.getPgName(), "42804");
                    }
                }
            }
        }
        int convIdx = table.getColumnIndex(alterCol.column());
        Column oldCol = table.getColumns().get(convIdx);
        rejectDefaultThatCannotBeCast(oldCol, currentType, dt, isArrayType, newTypeDisplayName);
        // The new column is built first and used only to coerce, so nothing is written until
        // every rule already declared over the column has been checked against what the rewrite
        // would produce: PostgreSQL rolls the whole statement back and the old values have to
        // still be there afterwards.
        Column newCol = oldCol.withType(dt, newPrecision, newScale, newEnumTypeName, newArrayElementType);
        newCol.setIntervalQualifier(DataType.intervalQualifier(setType.typeName()));
        // The column becomes a column of that type, not of the representation underneath it: the
        // catalogue names the domain or the composite from here on, a DROP of the type finds the
        // column depending on it, and every value written afterwards is held to the domain's own
        // rules.
        newCol.setDomainTypeName(newDomainTypeName);
        newCol.setCompositeTypeName(newCompositeTypeName);
        newCol.setRangeTypeName(newRangeTypeName);
        int rowCount = table.getRows().size();
        Object[] convertedValues = new Object[rowCount];
        for (int ri = 0; ri < rowCount; ri++) {
            Object[] row = table.getRows().get(ri);
            Object raw;
            if (setType.usingExpr() != null) {
                raw = executor.evalExpr(setType.usingExpr(), new RowContext(table, null, row));
            } else {
                raw = row[convIdx];
            }
            convertedValues[ri] = raw != null ? TypeCoercion.coerceForStorage(raw, newCol) : null;
        }
        // A value that reaches a domain has to satisfy the domain, whichever statement put it
        // there: PostgreSQL runs the domain's constraints over every rewritten row and refuses the
        // ALTER for one the domain would not have taken, rather than leaving the column holding a
        // value no INSERT could have written into it.
        if (newDomainTypeName != null) {
            DmlValidationHelper domainChecks = new DmlValidationHelper(executor);
            Table oneColumn = new Table("_retype_domain_check",
                    com.memgres.engine.util.Cols.listOf(newCol));
            for (int ri = 0; ri < rowCount; ri++) {
                domainChecks.validateDomainChecks(new Object[]{convertedValues[ri]}, oneColumn);
            }
        }
        // The indexes are rebuilt from the new values, so a conversion that maps two rows onto
        // one value breaks a unique index, and one that yields a null breaks NOT NULL.
        rejectRewriteThatBreaksColumn(table, alterCol.column(), convertedValues,
                tableName, schemaName);
        Object[] oldValues = new Object[rowCount];
        for (int ri = 0; ri < rowCount; ri++) oldValues[ri] = table.getRows().get(ri)[convIdx];
        table.alterColumnType(alterCol.column(), dt, newPrecision, newScale, newEnumTypeName, newArrayElementType);
        table.getColumns().get(convIdx).setIntervalQualifier(DataType.intervalQualifier(setType.typeName()));
        table.getColumns().get(convIdx).setDomainTypeName(newDomainTypeName);
        table.getColumns().get(convIdx).setCompositeTypeName(newCompositeTypeName);
        table.getColumns().get(convIdx).setRangeTypeName(newRangeTypeName);
        for (int ri = 0; ri < rowCount; ri++) {
            table.getRows().get(ri)[convIdx] = convertedValues[ri];
        }
        // A CHECK constraint reads the stored value, so it can only be re-checked once the new
        // values are in place. Failing puts the column back the way it was.
        try {
            revalidateChecksOverColumn(table);
        } catch (RuntimeException e) {
            for (int ri = 0; ri < rowCount; ri++) table.getRows().get(ri)[convIdx] = oldValues[ri];
            table.alterColumnType(alterCol.column(), oldCol.getType(), oldCol.getPrecision(),
                    oldCol.getScale(), oldCol.getEnumTypeName(), oldCol.getArrayElementType());
            table.getColumns().get(convIdx).setIntervalQualifier(oldCol.getIntervalQualifier());
            table.getColumns().get(convIdx).setDomainTypeName(oldCol.getDomainTypeName());
            table.getColumns().get(convIdx).setCompositeTypeName(oldCol.getCompositeTypeName());
            table.getColumns().get(convIdx).setRangeTypeName(oldCol.getRangeTypeName());
            throw e;
        }
    }

    /**
     * A retype rewrites the stored values, so a CHECK constraint that reads the column has to
     * still hold over every row afterwards — PostgreSQL re-runs it and refuses the ALTER when a
     * row no longer satisfies it, rather than leaving the table holding a row its own constraint
     * rejects.
     */
    private void revalidateChecksOverColumn(Table table) {
        for (StoredConstraint sc : table.getConstraints()) {
            if (sc.getType() != StoredConstraint.Type.CHECK) continue;
            if (sc.getCheckExpr() == null) continue;
            // A constraint that was never validated, or is declared unenforced, is not one the
            // rewrite has to satisfy — PostgreSQL leaves both alone.
            if (sc.isNotEnforced() || !sc.isConvalidated()) continue;
            validateCheckConstraintData(sc, table);
        }
    }

    /**
     * The column's default has to survive the retype too. PostgreSQL coerces it to the new type
     * with an assignment cast and refuses the whole ALTER when that cast does not exist, which is
     * what keeps a text default off an integer column instead of letting the next INSERT fail.
     * The rule is the one applied to the stored values: within a type category, or to a string
     * type, is automatic; out of a string type into anything else is not.
     */
    private void rejectDefaultThatCannotBeCast(Column col, DataType currentType, DataType dt,
                                               boolean isArrayType, String userTypeName) {
        if (col.getDefaultValue() == null) return;
        if (currentType == null || dt == null || currentType == dt) return;
        TypeCoercion.TypeCategory fromCat = TypeCoercion.categoryOf(currentType);
        TypeCoercion.TypeCategory toCat = TypeCoercion.categoryOf(dt);
        if (fromCat == toCat || toCat == TypeCoercion.TypeCategory.STRING) return;
        String targetName = userTypeName != null ? userTypeName : dt.toRegtypeDisplay();
        if (isArrayType) targetName += "[]";
        throw new MemgresException("default for column \"" + col.getName()
                + "\" cannot be cast automatically to type " + targetName, "42804");
    }

    /**
     * What PostgreSQL says when the type a column is being changed to is not one the old values
     * reach by an assignment cast. The hint is part of it: the conversion the writer meant is
     * still available, and USING is where it goes.
     */
    private static MemgresException retypeNeedsUsing(String column, String targetType,
                                                     boolean isArrayType) {
        String targetName = isArrayType ? targetType + "[]" : targetType;
        return new MemgresException("column \"" + column
                + "\" cannot be cast automatically to type " + targetName
                + "\n  Hint: You might need to specify \"USING "
                + column + "::" + targetName + "\".", "42804");
    }

    /**
     * What the column will hold after a retype has to satisfy the rules already declared over it:
     * a NOT NULL column may not end up holding a null, and a unique index over the column may not
     * end up with two rows on one value.
     */
    private void rejectRewriteThatBreaksColumn(Table table, String column, Object[] newValues,
                                               String tableName, String schemaName) {
        int idx = table.getColumnIndex(column);
        if (idx < 0) return;
        if (!table.getColumns().get(idx).isNullable()) {
            for (Object v : newValues) {
                if (v == null) {
                    throw PgErrors.columnContainsNulls(column, "relation", tableName);
                }
            }
        }
        String uniqueIndex = uniqueIndexOverColumnAlone(table, column, schemaName, tableName);
        if (uniqueIndex == null) return;
        Set<String> seen = new HashSet<>();
        for (Object v : newValues) {
            if (v == null) continue; // nulls are distinct from each other in a unique index
            if (!seen.add(String.valueOf(v))) {
                MemgresException dup = new MemgresException("could not create unique index \""
                        + uniqueIndex + "\"", "23505");
                dup.setConstraint(uniqueIndex);
                dup.setDetail(IndexKeyDescription.duplicated(table,
                        java.util.Collections.singletonList(column), new Object[]{v}));
                throw dup;
            }
        }
    }

    /** The name of a unique index or constraint whose only key is this column, or null. */
    private String uniqueIndexOverColumnAlone(Table table, String column, String schemaName,
                                              String tableName) {
        for (StoredConstraint sc : table.getConstraints()) {
            if (sc.getType() != StoredConstraint.Type.UNIQUE
                    && sc.getType() != StoredConstraint.Type.PRIMARY_KEY) continue;
            if (sc.getColumns() != null && sc.getColumns().size() == 1
                    && sc.getColumns().get(0).equalsIgnoreCase(column)) {
                return sc.getName();
            }
        }
        String qualified = schemaName + "." + tableName;
        for (Map.Entry<String, List<String>> e : executor.database.getIndexColumns().entrySet()) {
            if (!executor.database.isUniqueIndex(e.getKey())) continue;
            String owner = executor.database.getIndexTable(e.getKey());
            if (owner == null) continue;
            if (!owner.equalsIgnoreCase(qualified) && !owner.equalsIgnoreCase(tableName)) continue;
            List<String> cols = e.getValue();
            if (cols != null && cols.size() == 1 && cols.get(0).equalsIgnoreCase(column)) {
                return Database.idxName(e.getKey());
            }
        }
        return null;
    }

    private void executeSetDefault(AlterTableStmt.AlterColumn alterCol, AlterTableStmt.SetDefault setDefault,
                                    Table table, AlterTableStmt stmt) {
        String defaultVal = DdlExecutor.exprToDefaultString(setDefault.expr());

        if (defaultVal.contains("__restart__")) {
            handleRestart(alterCol.column(), defaultVal, table, stmt);
        } else if (defaultVal.contains("__identity__")) {
            handleIdentity(alterCol.column(), defaultVal, table, stmt);
        } else {
            Column col = requireColumn(table, alterCol.column(), stmt.table());
            DdlDefinitionChecks.validateDefaultExpression(setDefault.expr());
            executor.selectExecutor.placementCheck.rejectStoredDefinition(
                    setDefault.expr(), "DEFAULT expressions", null);
            // The calls a default makes are resolved where it is written, as they are wherever
            // else PostgreSQL stores an expression.
            StoredExprNames.read(ddl, setDefault.expr(), null, null, false, false);
            // The default has to be a value the column can hold, or every insert that relies on
            // it fails on a statement that never mentions the column.
            if (DdlDefinitionChecks.isEvaluableAtDefinitionTime(setDefault.expr())) {
                Object value = executor.evaluateDefault(defaultVal, col.getType());
                if (value != null) checkDefaultFits(value, col, alterCol.column(), setDefault.expr());
            }
            table.alterColumnDefault(alterCol.column(), defaultVal);
            propagateColumnDefault(table, alterCol.column(), defaultVal, stmt.only());
        }
    }

    /**
     * Confirm the column could hold what the default expression produces.
     *
     * <p>PostgreSQL judges the default's <em>type</em>, not its value: it looks for an assignment
     * cast from the expression's type to the column's, and records the default when one exists.
     * Whether the particular value fits is settled later, at the insert that takes the default —
     * which is why {@code SET DEFAULT 2147483648} on an integer column is accepted and the first
     * row to rely on it fails with {@code 22003} instead. Refusing the ALTER turns SQL PostgreSQL
     * runs into an error, so the range is deliberately not consulted here.
     *
     * <p>A bare string literal is still of type {@code unknown}: it has no type of its own to
     * cast from, so it is read as a value of the column's type at once and a bad one is invalid
     * input syntax. Its length is not judged either — a too-long literal is stored and the insert
     * reports {@code 22001}, as PostgreSQL does.
     */
    private void checkDefaultFits(Object value, Column col, String columnName, Expression expr) {
        DataType target = col.getType();
        if (target == null) return;
        String declared = DdlDefinitionChecks.defaultExpressionTypeName(expr, value);
        if (declared != null) {
            if (!TypeCoercion.assignableFrom(declared, target)) {
                throw defaultTypeMismatch(columnName, target, declared);
            }
            return; // the cast exists; the value is the insert's business
        }
        // Read the value as one of the column's base type, with the declared length and precision
        // left out: those constrain what a row may hold, not what a default may say.
        try {
            TypeCoercion.coerceForStorage(value, col.withType(target, null, null,
                    col.getEnumTypeName(), col.getArrayElementType()));
        } catch (MemgresException e) {
            String exprType = DdlDefinitionChecks.runtimeTypeName(value);
            if (exprType != null && !DdlDefinitionChecks.isUntypedLiteral(expr)) {
                throw defaultTypeMismatch(columnName, target, exprType);
            }
            throw e;
        }
    }

    /**
     * A default the column cannot take, worded as PostgreSQL words it. The remedy is the same
     * whichever way the mismatch was found — write the expression as the column's type — so the
     * advice belongs with the complaint rather than at each place that raises it.
     */
    private static MemgresException defaultTypeMismatch(String columnName, DataType target,
                                                        String exprType) {
        MemgresException e = PgErrors.datatypeMismatch("column \"" + columnName + "\" is of type "
                + target.toRegtypeDisplay() + " but default expression is of type " + exprType);
        e.setHint("You will need to rewrite or cast the expression.");
        return e;
    }

    private void handleRestart(String column, String defaultVal, Table table, AlterTableStmt stmt) {
        String marker = DdlExecutor.extractMarker(defaultVal);
        // RESTART is an identity action. On any other column, including a serial one — whose
        // sequence is a default, not an identity — PostgreSQL refuses rather than restarting
        // something the writer did not name.
        Column named = requireColumn(table, column, stmt.table());
        if (!isIdentityColumn(named)) {
            throw new MemgresException("column \"" + column + "\" of relation \""
                    + stmt.table() + "\" is not an identity column", "55000");
        }
        int colIdx = table.getColumnIndex(column);
        Column col = colIdx >= 0 ? table.getColumns().get(colIdx) : null;
        boolean restarted = false;
        if (col != null && col.getDefaultValue() != null
                && (col.getDefaultValue().contains("nextval") || col.getDefaultValue().contains(":seq:"))) {
            Sequence seq = findBackingSequence(table, col);
            if (seq != null) {
                if (marker.contains(":")) {
                    long val = Long.parseLong(marker.substring(marker.indexOf(":") + 1));
                    seq.restart(val);
                } else {
                    seq.restart();
                }
                restarted = true;
            }
        }
        if (!restarted && col != null && (col.getType() == DataType.SERIAL || col.getType() == DataType.BIGSERIAL || col.getType() == DataType.SMALLSERIAL)) {
            if (marker.contains(":")) {
                long val = Long.parseLong(marker.substring(marker.indexOf(":") + 1));
                table.resetSerialCounter(val);
            } else {
                table.resetSerialCounter(1);
            }
        }
    }

    private void handleIdentity(String column, String defaultVal, Table table, AlterTableStmt stmt) {
        String marker = DdlExecutor.extractMarker(defaultVal);
        boolean isSetGenerated = defaultVal.contains("__identity__:always") || defaultVal.contains("__identity__:bydefault");

        if (marker.contains(":add:")) {
            Column target = requireColumn(table, column, stmt.table());
            if (isIdentityColumn(target)) {
                throw new MemgresException("column \"" + column + "\" of relation \""
                        + stmt.table() + "\" is already an identity column", "55000");
            }
            DdlDefinitionChecks.requireIdentityType(target.getType());
            // Identity fills the column for every row, so a column that still admits nulls
            // would contradict what identity guarantees.
            if (target.isNullable()) {
                throw new MemgresException("column \"" + column + "\" of relation \"" + stmt.table()
                        + "\" must be declared NOT NULL before identity can be added", "55000");
            }
        }

        if (isSetGenerated) {
            int ci = table.getColumnIndex(column);
            Column cc = ci >= 0 ? table.getColumns().get(ci) : null;
            if (cc != null) {
                boolean isIdentity = false;
                if (cc.getDefaultValue() != null && (cc.getDefaultValue().contains("nextval") || cc.getDefaultValue().contains("__identity__"))) {
                    isIdentity = true;
                }
                DataType ct = cc.getType();
                if (ct == DataType.SERIAL || ct == DataType.BIGSERIAL || ct == DataType.SMALLSERIAL) {
                    isIdentity = true;
                }
                if (!isIdentity) {
                    throw new MemgresException("column \"" + column + "\" of relation \"" + stmt.table()
                            + "\" is not an identity column", "55000");
                }
            }
        }

        // Parse identity options
        Long startWith = null;
        Long incrementBy = null;
        String explicitSeqName = null;
        if (marker.contains(":start=")) {
            String s = marker.substring(marker.indexOf(":start=") + 7);
            if (s.contains(":")) s = s.substring(0, s.indexOf(":"));
            startWith = Long.parseLong(s);
        }
        if (marker.contains(":increment=")) {
            String s = marker.substring(marker.indexOf(":increment=") + 11);
            if (s.contains(":")) s = s.substring(0, s.indexOf(":"));
            incrementBy = Long.parseLong(s);
        }
        if (marker.contains(":seqname=")) {
            String s = marker.substring(marker.indexOf(":seqname=") + 9);
            if (s.contains(":")) s = s.substring(0, s.indexOf(":"));
            // A dump writes the sequence's schema with its name; the schema is where it goes,
            // not part of what it is called.
            int dot = s.lastIndexOf('.');
            explicitSeqName = dot > 0 ? s.substring(dot + 1) : s;
        }

        String seqName = explicitSeqName != null ? explicitSeqName :
                stmt.table() + "_" + column + "_seq";
        String identitySchema = table.getSchemaName();
        if (!executor.database.hasSequence(identitySchema, seqName)) {
            Sequence added = new Sequence(seqName, startWith, incrementBy, null, null);
            added.setSchemaName(identitySchema);
            // The sequence belongs to this column and dies with it, whatever the column is
            // called later on.
            added.ownedBy(stmt.table(), column, true);
            executor.database.addSequence(added);
            executor.database.registerSchemaObject(identitySchema, "sequence", seqName);
        } else if (startWith != null || incrementBy != null) {
            Sequence seq = executor.database.getSequence(identitySchema, seqName);
            if (startWith != null) seq.restart(startWith);
            if (incrementBy != null) seq.setIncrementBy(incrementBy);
        }

        String newDefault;
        if (marker.contains(":always")) {
            newDefault = "__identity__:always:seq:" + seqName;
        } else {
            newDefault = "__identity__:bydefault:seq:" + seqName;
        }
        table.alterColumnDefault(column, newDefault);

        boolean isAddGenerated = marker.contains(":add:");
        if (isAddGenerated) {
            int ci = table.getColumnIndex(column);
            if (ci >= 0) {
                Column cc = table.getColumns().get(ci);
                DataType serialType;
                switch (cc.getType()) {
                    case BIGINT:
                        serialType = DataType.BIGSERIAL;
                        break;
                    case SMALLINT:
                        serialType = DataType.SMALLSERIAL;
                        break;
                    default:
                        serialType = DataType.SERIAL;
                        break;
                }
                table.alterColumnType(column, serialType);
                if (startWith != null) {
                    table.resetSerialCounter(startWith);
                }
            }
        }
    }

    /**
     * A sequence a column of this table draws from belongs to the table, so it moves with it.
     * Leaving it behind breaks every later insert: the default names the sequence without a
     * qualifier, which means the schema the table is now in, and nothing of that name is there.
     */
    private void moveOwnedSequences(Table table, String fromSchema, String toSchema) {
        if (fromSchema.equalsIgnoreCase(toSchema)) return;
        for (String seqName : ownedSequenceNames(table, fromSchema)) {
            Sequence seq = executor.database.getSequence(fromSchema, seqName);
            if (seq == null) continue;
            executor.database.removeSequence(fromSchema, seqName);
            executor.database.unregisterSchemaObject(fromSchema, "sequence", seqName);
            seq.setSchemaName(toSchema);
            executor.database.addSequence(seq);
            executor.database.registerSchemaObject(toSchema, "sequence", seqName);
            executor.identity().relationRenamed("S", fromSchema, seqName, toSchema, seqName);
        }
    }

    /** An index lives where its table does, so moving the table moves every index on it. */
    private void moveOwnedIndexes(Table table, String fromSchema, String toSchema, String tableName) {
        if (fromSchema.equalsIgnoreCase(toSchema)) return;
        for (String key : ownedIndexKeys(fromSchema, tableName)) {
            String bare = Database.idxName(key);
            executor.database.moveIndex(key, toSchema, toSchema + "." + tableName);
            executor.identity().relationRenamed("i", fromSchema, bare, toSchema, bare);
        }
    }

    /**
     * The sequences this table owns, by bare name. A default that names its sequence without a
     * qualifier means the one in the table's own schema, and only that one belongs to the table.
     */
    private List<String> ownedSequenceNames(Table table, String fromSchema) {
        List<String> names = new ArrayList<>();
        for (Column col : table.getColumns()) {
            String seqName = Sequence.nameInDefault(col.getDefaultValue());
            if (seqName == null || seqName.indexOf('.') > 0) continue;
            if (executor.database.getSequence(fromSchema, seqName) != null) names.add(seqName);
        }
        return names;
    }

    /** The keys of the indexes written on this table, which live where the table does. */
    private List<String> ownedIndexKeys(String fromSchema, String tableName) {
        List<String> keys = new ArrayList<>();
        String oldOwner = fromSchema + "." + tableName;
        for (String key : new ArrayList<>(executor.database.getIndexColumns().keySet())) {
            String owner = executor.database.getIndexTable(key);
            if (owner != null && owner.equalsIgnoreCase(oldOwner)) keys.add(key);
        }
        return keys;
    }

    /**
     * Refuse the move when the table, or anything that travels with it, would land on a name the
     * destination schema has already given to a relation.
     *
     * <p>PostgreSQL checks before it moves anything and names the object that is in the way, so
     * the statement either moves the lot or leaves every schema as it found it. Moving first and
     * overwriting was silent destruction: the destination's index or its sequence — counter and
     * all — simply stopped existing.
     */
    private void requireNamesFreeInTarget(Table table, String fromSchema, String toSchema,
                                          String tableName) {
        if (fromSchema.equalsIgnoreCase(toSchema)) return;
        requireFreeInTarget(toSchema, tableName);
        for (String seqName : ownedSequenceNames(table, fromSchema)) {
            requireFreeInTarget(toSchema, seqName);
        }
        for (String key : ownedIndexKeys(fromSchema, tableName)) {
            requireFreeInTarget(toSchema, Database.idxName(key));
        }
    }

    private void requireFreeInTarget(String toSchema, String name) {
        if (RelationNamespace.kindOf(executor.database, toSchema, name) == null) return;
        throw new MemgresException("relation \"" + name + "\" already exists in schema \""
                + toSchema + "\"", "42P07");
    }

    private Sequence findBackingSequence(Table table, Column col) {
        String seqRef = col.getDefaultValue();
        String schema = table.getSchemaName();
        // Check for nextval('seqname'::regclass) pattern
        int qi = seqRef.indexOf("'");
        int qi2 = seqRef.indexOf("'", qi + 1);
        if (qi >= 0 && qi2 >= 0) {
            String seqName = seqRef.substring(qi + 1, qi2);
            return executor.database.getSequenceFor(schema, seqName);
        }
        // Check for __identity__:...:seq:seqname pattern
        if (seqRef.contains(":seq:")) {
            String seqName = seqRef.substring(seqRef.indexOf(":seq:") + 5);
            return executor.database.getSequenceFor(schema, seqName);
        }
        return null;
    }

    private void executeAddConstraint(AlterTableStmt.AddConstraint addConstraint, Table table,
                                       AlterTableStmt stmt, String schemaName) {
        TableConstraint.ConstraintType addedType = addConstraint.constraint().type();
        rejectNoInheritOnPartitioned(addConstraint.constraint(), table, stmt.table());
        // A CHECK written on the parent belongs to the whole hierarchy: a child that does not
        // carry it could hold a row the parent's own rule rejects, and reading the hierarchy
        // through the parent would return it. NO INHERIT says the constraint was never meant to
        // travel, so ONLY is not a contradiction there.
        if (addedType == TableConstraint.ConstraintType.CHECK
                && !addConstraint.constraint().noInherit()
                && stmt.only() && !childRelations(table).isEmpty()) {
            throw onlyOnParent(false);
        }
        if (addedType == TableConstraint.ConstraintType.NOT_NULL) {
            rejectOnlyNotNullConstraint(table, stmt, addConstraint.constraint().columns());
        }
        if (addedType == TableConstraint.ConstraintType.FOREIGN_KEY
                && stmt.only() && isPartitioned(table)) {
            throw new MemgresException("cannot use ONLY for foreign key on partitioned table \""
                    + stmt.table() + "\" referencing relation \""
                    + addConstraint.constraint().referencesTable() + "\"", "42809");
        }
        if (addedType == TableConstraint.ConstraintType.PRIMARY_KEY) {
            rejectKeyOnVirtualColumn(table, addConstraint.constraint().columns(), "primary keys");
        } else if (addedType == TableConstraint.ConstraintType.UNIQUE) {
            rejectKeyOnVirtualColumn(table, addConstraint.constraint().columns(),
                    "unique constraints");
        }
        if (addConstraint.constraint().type() == TableConstraint.ConstraintType.NOT_NULL) {
            for (String colName : addConstraint.constraint().columns()) {
                // A written name belongs to a constraint that comes into existence, and over a
                // column that already refuses a null there is none to make: PostgreSQL refuses
                // the declaration rather than folding it into the constraint already there.
                rejectSecondNotNullConstraint(table, colName, addConstraint.constraint().name(),
                        addConstraint.notValid(), stmt.table());
                // The rows already stored decide whether the rule can hold at all — unless NOT
                // VALID was written, which is exactly the request to leave them alone. The column
                // is still marked NOT NULL below, because PostgreSQL enforces such a constraint on
                // every row written from now on and only VALIDATE CONSTRAINT looks back.
                int colIdx = table.getColumnIndex(colName);
                if (colIdx >= 0 && !addConstraint.notValid()) {
                    // A rule the relation already carries but has never held its rows to is not
                    // one a second declaration may quietly stand on: PostgreSQL refuses to fold a
                    // validated declaration into an unvalidated constraint and says which
                    // constraint is in the way, because VALIDATE CONSTRAINT is what settles it.
                    rejectMergeIntoNotValidNotNull(table, colName, stmt.table());
                    for (Object[] row : table.getRows()) {
                        if (row[colIdx] == null) {
                            throw PgErrors.columnContainsNulls(colName, "relation", stmt.table());
                        }
                    }
                    // A partitioned parent stores no rows of its own, and an inheritance child's
                    // rows are the parent's rows too, so the data that decides whether the rule
                    // can hold at all lives below: PostgreSQL scans the descendants and names the
                    // one at fault.
                    for (Table descendant : descendantRelations(table)) {
                        requireColumnHasNoNulls(descendant, colName, descendant.getName());
                    }
                }
                recordNotNullUndo(table, colName);
                // A column that is already NOT NULL keeps the constraint it has: what reaches
                // here names that constraint or names none at all, and PostgreSQL folds such a
                // declaration in without creating a second one, so DROP CONSTRAINT on a name
                // that was never created is 42704 there and must be here too.
                boolean alreadyNotNull = colIdx >= 0
                        && !table.getColumns().get(colIdx).isNullable();
                // NOT VALID is recorded against the constraint the statement creates, and a
                // declaration that only restates a rule the relation already carries creates
                // none: PostgreSQL leaves the constraint standing there as validated as it was.
                // The relations below hold the same constraint, so they hold the same answer, and
                // which of them the rule is reaching for the first time has to be read off the
                // hierarchy before the flag is set anywhere.
                List<Table> takingTheRule = addConstraint.notValid() && !alreadyNotNull
                        ? relationsTakingNotNull(table, colName)
                        : Collections.<Table>emptyList();
                table.alterColumnNullable(colName, false);
                if (!alreadyNotNull && addConstraint.constraint().name() != null) {
                    table.setNotNullConstraintName(colName, addConstraint.constraint().name());
                }
                // NO INHERIT says the constraint stops here, and it is part of what the
                // constraint is: unrecorded, a second declaration could not tell that it
                // contradicts the one already there, and the catalogue said it reached down.
                if (!alreadyNotNull && addConstraint.constraint().noInherit()) {
                    table.markNotNullNoInherit(colName);
                }
                // Each descendant keeps its own copy of the column list, so the flag has to reach
                // them or this relation forbids a null its own descendants go on taking.
                setColumnsNotNull(table, Collections.singletonList(colName));
                for (Table taking : takingTheRule) {
                    taking.markNotNullNotValidated(colName);
                }
            }
            return;
        }

        if (addConstraint.constraint().type() == TableConstraint.ConstraintType.CHECK) {
            // A CHECK answers yes or no about one row, and a set-returning call answers with rows:
            // there is nothing there for the constraint to test.
            if (executor.selectExecutor.containsSrf(addConstraint.constraint().checkExpr())) {
                throw PgErrors.notImplemented(
                        "set-returning functions are not allowed in check constraints");
            }
            // What the predicate is refused for is decided by where the faults are written, not
            // by the order these checks run in: PostgreSQL settles each name and each call at the
            // node it stands at, so a column nothing answers to written ahead of a sub-query is
            // what it complains about.
            StoredExprNames.read(ddl, addConstraint.constraint().checkExpr(), table, null,
                    true, true);
            executor.selectExecutor.placementCheck.rejectStoredDefinition(
                    addConstraint.constraint().checkExpr(), "check constraints", "check constraint");
            // Every name in the predicate is resolved against the relation the constraint is
            // being stored on, and resolved before the predicate is asked to be a boolean.
            // PostgreSQL resolves them whether or not the rows already stored are to be checked:
            // NOT VALID and NOT ENFORCED defer the rows, not the names. Left to the pass that
            // reads the rows, a constraint declared either way was stored naming a column nothing
            // answers to.
            ddl.validateExprColumnRefs(addConstraint.constraint().checkExpr(), table, null,
                    true, true);
            DdlDefinitionChecks.requireBooleanPredicate(
                    addConstraint.constraint().checkExpr(), table, "CHECK");
        }

        // Same definition checks CREATE TABLE runs: a key over a column the table does not have
        // was stored with an attribute number nothing answers to and enforced nothing.
        TableConstraint.ConstraintType addedKind = addConstraint.constraint().type();
        if (addedKind == TableConstraint.ConstraintType.PRIMARY_KEY
                || addedKind == TableConstraint.ConstraintType.UNIQUE
                || addedKind == TableConstraint.ConstraintType.EXCLUDE) {
            // A primary key added to a relation that already exists makes its columns NOT NULL
            // first, and a system column takes no constraint of the definer's, so PostgreSQL
            // stops there rather than at the index it never gets to build.
            if (addedKind == TableConstraint.ConstraintType.PRIMARY_KEY
                    && addConstraint.constraint().columns() != null) {
                for (String col : addConstraint.constraint().columns()) {
                    if (DdlDefinitionChecks.isSystemColumnName(col)) {
                        throw new MemgresException("cannot add not-null constraint on system"
                                + " column \"" + col.toLowerCase(java.util.Locale.ROOT) + "\"", "0A000");
                    }
                }
            }
            DdlDefinitionChecks.validateKeyColumns(table, addConstraint.constraint().columns(),
                    addedKind == TableConstraint.ConstraintType.PRIMARY_KEY ? "primary key"
                            : addedKind == TableConstraint.ConstraintType.UNIQUE ? "unique"
                            : "exclusion");
            if (addedKind != TableConstraint.ConstraintType.EXCLUDE) {
                DdlDefinitionChecks.requireKeyColumnOpclass(table,
                        addConstraint.constraint().columns());
            }
            DdlDefinitionChecks.requireKeyColumnsExist(table,
                    addConstraint.constraint().includedColumns());
            if (addedKind == TableConstraint.ConstraintType.EXCLUDE) {
                DdlDefinitionChecks.requireExclusionCapableAccessMethod(
                        addConstraint.constraint().excludeMethod());
                DdlDefinitionChecks.requireCommutativeExclusionOperators(
                        table, addConstraint.constraint().excludeElements());
                // And with one the index's own operator class knows about.
                DdlDefinitionChecks.requireExclusionOperatorInFamily(
                        table, addConstraint.constraint().excludeElements(),
                        addConstraint.constraint().excludeMethod());
            }
        }
        List<String> rawCols = addConstraint.constraint().columns();
        boolean isUsingIndex = rawCols != null && rawCols.size() == 1 && rawCols.get(0).startsWith("__using_index__:");
        if (isUsingIndex) {
            String idxName = rawCols.get(0).substring("__using_index__:".length());
            table.removeConstraint(idxName);
        }
        StoredConstraint sc = ddl.convertTableConstraint(stmt.table(), addConstraint.constraint(), table);
        if (sc != null && isUsingIndex) {
            sc.setPromotedFromIndex(true);
        }
        if (sc != null) {
            // For FK constraints without explicit schema, default to the table's schema
            if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY
                    && sc.getReferencesSchema() == null && sc.getReferencesTable() != null) {
                sc.setReferencesSchema(schemaName);
            }
            // PG checks the key's definition before it looks for a name collision.
            if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY) {
                ddl.validateForeignKeyDefinition(sc, table, schemaName);
            }
            // UNIQUE and PRIMARY KEY are enforced by an index that is built at once, so there is
            // nothing for NOT VALID to defer and PostgreSQL refuses the combination outright —
            // before it looks at whether the key itself could stand.
            if (addConstraint.notValid()
                    && (sc.getType() == StoredConstraint.Type.UNIQUE
                        || sc.getType() == StoredConstraint.Type.PRIMARY_KEY)) {
                throw PgErrors.notImplemented(
                        (sc.getType() == StoredConstraint.Type.UNIQUE ? "UNIQUE" : "PRIMARY KEY")
                        + " constraints cannot be marked NOT VALID");
            }
            // The index a key is built on takes a relation name, so a name another relation
            // already owns is reported as that before the key is judged.
            if (addConstraint.constraint().name() != null
                    && (sc.getType() == StoredConstraint.Type.UNIQUE
                        || sc.getType() == StoredConstraint.Type.PRIMARY_KEY)) {
                RelationNamespace.requireFree(executor.database, schemaName, sc.getName(), null);
            }
            // A second primary key is refused for what it is, whatever it was going to be called.
            if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY) {
                for (StoredConstraint existing : table.getConstraints()) {
                    if (existing.getType() == StoredConstraint.Type.PRIMARY_KEY) {
                        throw new MemgresException("multiple primary keys for table \""
                                + stmt.table() + "\" are not allowed", "42P16");
                    }
                }
            }
            if (sc.getName() != null && table.getConstraint(sc.getName()) != null) {
                throw new MemgresException("constraint \"" + sc.getName() + "\" for relation \"" + stmt.table() + "\" already exists", "42710");
            }
            if (addConstraint.notValid()) {
                sc.setConvalidated(false);
            }
            if (!sc.isNotEnforced() && !addConstraint.notValid()) {
                if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY && sc.getReferencesTable() != null) {
                    validateForeignKeyData(sc, table, stmt.table());
                }
                if (sc.getType() == StoredConstraint.Type.CHECK && sc.getCheckExpr() != null) {
                    validateCheckConstraintData(sc, table);
                    // A partitioned parent stores no rows of its own, and an inheritance child's
                    // rows are the parent's rows too, so the rows that decide whether the rule can
                    // hold live below. PostgreSQL names the descendant whose row breaks it, and a
                    // NO INHERIT constraint stops at the relation that declared it.
                    if (!sc.isNoInherit()) {
                        for (Table descendant : descendantRelations(table)) {
                            validateCheckConstraintData(sc, descendant);
                        }
                    }
                }
            }
            if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY || sc.getType() == StoredConstraint.Type.UNIQUE) {
                // Same invariant CREATE TABLE enforces for partitioned tables: a PK/UNIQUE
                // constraint must include every partition key column. Validate before adding
                // to (or propagating onto) the table, matching creation-time ordering.
                DdlTableExecutor.validatePartitionKeyCoverage(table, sc);
                // A primary key is a NOT NULL that has been validated, so it cannot be built over
                // a NOT NULL whose rows were never read: PostgreSQL refuses ahead of the scan and
                // names the constraint standing in the way rather than a row.
                if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY) {
                    rejectKeyOverNotValidNotNull(table, sc.getColumns(), stmt.table());
                }
                // PG validates existing data while building the unique index: duplicates abort
                // with 23505, and NULLs in a new PRIMARY KEY column abort with 23502.
                if (!sc.isNotEnforced()) {
                    table.validateNewUniqueConstraint(sc);
                }
            }
            table.addConstraint(sc);
            ddl.registerExcludeIndex(schemaName, stmt.table(), sc);
            if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY) {
                // PG: ADD PRIMARY KEY marks the key columns NOT NULL (attnotnull /
                // information_schema.columns.is_nullable = 'NO'), on the table and its partitions.
                for (String keyCol : sc.getColumns()) recordNotNullUndo(table, keyCol);
                setColumnsNotNull(table, sc.getColumns());
            }
            propagateConstraintToChildren(table, sc);
        }
    }

    /**
     * Copies a newly-added PK/UNIQUE constraint onto every existing partition (recursively, for
     * multi-level partitioning) of a partitioned parent. Row storage for a partitioned table
     * lives entirely on the leaf partitions, so a constraint added to the parent after
     * partitions already exist must reach each partition's own TableIndex too - otherwise
     * per-partition duplicate-key checks and ON CONFLICT conflict detection would miss rows that
     * already live there, the same bug class fixed for creation-time constraints in
     * {@link DdlTableExecutor#createPartitionOfTable}. Each partition gets its own independent
     * {@code StoredConstraint} copy (see {@link StoredConstraint#copyForPartition}) rather than
     * sharing the parent's instance.
     */
    /** Marks the given columns NOT NULL on a table and, recursively, on everything below it. */
    private void setColumnsNotNull(Table table, List<String> columns) {
        for (String col : columns) {
            if (table.getColumnIndex(col) >= 0) {
                table.alterColumnNullable(col, false);
            }
        }
        // A partition and an inheritance child each keep their own copy of the column list, so a
        // NOT NULL declared on the parent has to reach both. Following the partitions alone left
        // every inherited child taking a null the parent forbids.
        for (Table child : childRelations(table)) {
            // The rule is the parent's: a descendant taking it on now did not declare it, so the
            // constraint answers to the parent's name and only the parent may withdraw it. One
            // that already refuses a null said so for itself and keeps its own name.
            for (String col : columns) {
                int idx = child.getColumnIndex(col);
                if (idx >= 0 && child.getColumns().get(idx).isNullable()) {
                    child.markNotNullInherited(col);
                }
            }
            setColumnsNotNull(child, columns);
        }
    }

    /**
     * Give every relation below this one its own copy of a constraint just declared on the parent.
     *
     * <p>Row storage lives in the leaves, so a constraint the parent alone carries enforces
     * nothing at all. PostgreSQL passes a partition every kind of constraint the partitioned table
     * has, which is why the whole hierarchy answers for one rule, while ordinary inheritance
     * passes CHECK constraints only: a child there is a table in its own right, and its keys and
     * its foreign keys are its own.
     */
    private void propagateConstraintToChildren(Table table, StoredConstraint sc) {
        for (Table partition : table.getPartitions()) {
            DdlTableExecutor.validatePartitionKeyCoverage(partition, sc);
            StoredConstraint copy = sc.copyForPartition(partition.getName());
            // A CHECK and a foreign key keep the name they were declared with, so the copy has to
            // remember whose rule it is: that is what DROP CONSTRAINT on the partition consults.
            // A key constraint is renamed for the partition, the way creation-time copies are.
            if (sc.getType() == StoredConstraint.Type.CHECK
                    || sc.getType() == StoredConstraint.Type.FOREIGN_KEY) {
                copy.setInheritedFrom(table.getName());
            }
            partition.addConstraint(copy);
            propagateConstraintToChildren(partition, copy);
        }
        if (sc.getType() != StoredConstraint.Type.CHECK || sc.isNoInherit()) return;
        for (Table child : table.getChildren()) {
            // A child that already declares the rule under this name keeps its own copy, which is
            // the merge PostgreSQL performs rather than storing the constraint twice.
            if (sc.getName() != null && child.getConstraint(sc.getName()) != null) continue;
            StoredConstraint copy = sc.copyForPartition(child.getName());
            copy.setInheritedFrom(table.getName());
            child.addConstraint(copy);
            propagateConstraintToChildren(child, copy);
        }
    }

    /**
     * Take the copies of a dropped constraint off every relation below the one that declared it.
     *
     * <p>A rule the parent hands down is enforced by the descendants that store the rows, so a
     * drop that stopped at the parent left it in force with nobody able to withdraw it. PostgreSQL
     * drops the copies with the original, and leaves standing only what a descendant declared for
     * itself -- which keeps its own name and answers to nobody else's drop.
     */
    private static void dropInheritedCopies(Table parent, String constraintName) {
        for (Table child : childRelations(parent)) {
            StoredConstraint copy = child.getConstraint(constraintName);
            if (copy == null || !isInheritedFrom(copy, parent)) continue;
            child.removeConstraint(constraintName);
            dropInheritedCopies(child, constraintName);
        }
    }

    /**
     * ONLY drops the parent's own constraint and leaves each descendant holding its copy, which
     * makes that copy local: it is the descendant's from then on, and the descendant's to drop.
     */
    private static void makeInheritedCopiesLocal(Table parent, String constraintName) {
        for (Table child : childRelations(parent)) {
            StoredConstraint copy = child.getConstraint(constraintName);
            if (copy == null || !isInheritedFrom(copy, parent)) continue;
            copy.setInheritedFrom(null);
        }
    }

    /**
     * Everything this relation carried because it belonged to the given parent becomes its own.
     * PostgreSQL keeps the constraints a detached partition (or a table taken out of an
     * inheritance hierarchy) holds and records them as local, which is what makes them droppable
     * from the standalone table afterwards.
     */
    private static void adoptInheritedConstraints(Table relation, Table formerParent) {
        for (StoredConstraint sc : relation.getConstraints()) {
            if (!isInheritedFrom(sc, formerParent)) continue;
            // Under two parents only the named link is broken, and a rule the other parent still
            // declares has not become the relation's own: it is re-filed under the parent that
            // goes on handing it down, so the relation is still refused permission to drop it.
            Table stillFrom = null;
            for (Table parent : relation.getDirectParents()) {
                if (sc.getName() != null && parent.getConstraint(sc.getName()) != null) {
                    stillFrom = parent;
                    break;
                }
            }
            // A count held over from a parent that dropped its own copy is a count nothing can
            // withdraw, so leaving the hierarchy does not make the rule the relation's own either:
            // PostgreSQL turns conislocal on only where the count has reached nought.
            if (stillFrom == null && sc.getRetainedInheritCount() > 0) continue;
            sc.setInheritedFrom(stillFrom == null ? null : stillFrom.getName());
        }
    }

    /**
     * Write down what each of this relation's inherited NOT NULL constraints answers to, before a
     * link changes underneath it.
     *
     * <p>PostgreSQL names a constraint when it is created and never names it again. memgres reads
     * the name off the parent that declared the rule, which is the same answer for as long as that
     * parent is there -- so the answer is recorded at the moment the relation stops being able to
     * reach it, and a relation still holding the rule for another parent goes on answering to the
     * name it was given rather than to the remaining parent's.
     */
    private static void pinInheritedNotNullNames(Table relation) {
        for (Column c : relation.getColumns()) {
            if (c.isNullable() || relation.isNotNullLocal(c.getName())) continue;
            relation.pinInheritedNotNullName(c.getName(),
                    CatalogConstraintBuilder.notNullConstraintName(relation, c.getName()));
        }
    }

    /**
     * The NOT NULL constraints a relation held on a parent's behalf become its own when it leaves
     * that parent, under the names they already answer to.
     *
     * <p>PostgreSQL writes a constraint's name down when it is created and never works it out
     * again, so a detached partition -- or a table taken out of an inheritance hierarchy -- goes
     * on carrying the name the relation that declared the rule gave it. Working the name out
     * afresh from the links that are left renamed the constraint after the relation itself, which
     * is not the name a DROP CONSTRAINT has to quote. Being the relation's own is the other half:
     * it is what lets the standalone table withdraw the rule, and PostgreSQL leaves it the
     * relation's own even where the relation is declared under that parent again afterwards. A
     * parent that goes on handing the same rule down leaves it exactly as it was, because the
     * relation is still holding that one for somebody else.
     */
    private static void adoptInheritedNotNulls(Table relation, Table formerParent) {
        for (Column c : relation.getColumns()) {
            String column = c.getName();
            if (c.isNullable() || relation.isNotNullLocal(column)) continue;
            if (!CatalogConstraintBuilder.declaresNotNull(formerParent, column)) continue;
            boolean handedDownStill = false;
            for (Table parent : relation.getDirectParents()) {
                if (CatalogConstraintBuilder.declaresNotNull(parent, column)) {
                    handedDownStill = true;
                    break;
                }
            }
            // A rule another parent still hands down is left exactly as it was: the relation is
            // still holding that one for somebody, and it goes on answering to the name written
            // down before the link that led to it went.
            if (handedDownStill) continue;
            String held = relation.inheritedNotNullName(column);
            relation.setNotNullConstraintName(column, held != null ? held
                    : CatalogConstraintBuilder.notNullConstraintName(formerParent, column));
            relation.markNotNullLocal(column);
        }
    }

    /** True when this copy is the one the named parent handed down, rather than the child's own. */
    private static boolean isInheritedFrom(StoredConstraint sc, Table parent) {
        return sc.getInheritedFrom() != null && parent != null
                && sc.getInheritedFrom().equalsIgnoreCase(parent.getName());
    }

    private void validateForeignKeyData(StoredConstraint sc, Table table, String tableName) {
        Table refTable;
        if (sc.getReferencesSchema() != null) {
            refTable = executor.resolveTable(sc.getReferencesSchema(), sc.getReferencesTable());
        } else {
            refTable = executor.resolveTableAnySchema(sc.getReferencesTable());
        }
        for (String refCol : sc.getReferencesColumns()) {
            if (refTable.getColumnIndex(refCol) < 0) {
                throw new MemgresException("column \"" + refCol + "\" referenced in foreign key constraint does not exist", "42703");
            }
        }
        if (!sc.getColumns().isEmpty()) {
            int[] fkIndices = new int[sc.getColumns().size()];
            for (int ci = 0; ci < sc.getColumns().size(); ci++) {
                fkIndices[ci] = table.getColumnIndex(sc.getColumns().get(ci));
            }
            int[] refIndices = new int[sc.getReferencesColumns().size()];
            for (int ci = 0; ci < sc.getReferencesColumns().size(); ci++) {
                refIndices[ci] = refTable.getColumnIndex(sc.getReferencesColumns().get(ci));
            }
            for (Object[] row : table.getRows()) {
                // MATCH SIMPLE is the default, and under it a row with a NULL anywhere in the
                // referencing columns satisfies the constraint whatever the other columns hold.
                // Requiring every column to be NULL rejected rows PostgreSQL accepts, so the
                // constraint could not be added at all.
                boolean anyNull = false;
                for (int fi : fkIndices) {
                    if (fi < 0 || row[fi] == null) { anyNull = true; break; }
                }
                if (anyNull) continue;
                boolean found = false;
                for (Object[] refRow : refTable.getRows()) {
                    boolean match = true;
                    for (int ci = 0; ci < fkIndices.length; ci++) {
                        Object fkVal = fkIndices[ci] >= 0 ? row[fkIndices[ci]] : null;
                        Object refVal = refIndices[ci] >= 0 ? refRow[refIndices[ci]] : null;
                        // Values match as values, not as text: numeric 1.00 references 1.0.
                        if (!TypeCoercion.areEqual(fkVal, refVal)) { match = false; break; }
                    }
                    if (match) { found = true; break; }
                }
                if (!found) {
                    MemgresException ex = new MemgresException("insert or update on table \""
                            + tableName + "\" violates foreign key constraint \"" + sc.getName()
                            + "\"", "23503");
                    ex.setConstraint(sc.getName());
                    ex.setDetail(missingKeyDetail(sc, fkIndices, row));
                    throw ex;
                }
            }
        }
    }

    /**
     * The key the referenced table does not hold. A constraint is declared over a whole table at
     * once, so without the key the reader learns only that some row among all of them is at fault.
     */
    private static String missingKeyDetail(StoredConstraint sc, int[] fkIndices, Object[] row) {
        StringBuilder detail = new StringBuilder("Key (");
        for (int ci = 0; ci < sc.getColumns().size(); ci++) {
            if (ci > 0) detail.append(", ");
            detail.append(sc.getColumns().get(ci));
        }
        detail.append(")=(");
        for (int ci = 0; ci < fkIndices.length; ci++) {
            if (ci > 0) detail.append(", ");
            detail.append(ErrorValueText.of(fkIndices[ci] >= 0 ? row[fkIndices[ci]] : null));
        }
        detail.append(") is not present in table \"").append(sc.getReferencesTable()).append("\".");
        return detail.toString();
    }

    private void validateCheckConstraintData(StoredConstraint sc, Table table) {
        // Every name in the predicate has to be a column of the relation, and then the columns
        // the relation keeps for itself are refused: a CHECK is evaluated over the row being
        // written, which does not carry its own xmin or ctid yet.
        ddl.validateExprColumnRefs(sc.getCheckExpr(), table, null, true);
        DdlDefinitionChecks.rejectSystemColumnInCheck(sc.getCheckExpr());
        boolean hasVirtual = executor.dmlExecutor.hasVirtualColumns(table);
        for (Object[] row : table.getRows()) {
            Object[] evalRow = hasVirtual ? executor.dmlExecutor.computeVirtualColumns(table, row) : row;
            RowContext checkCtx = new RowContext(table, null, evalRow);
            // Whatever evaluating the predicate raises is the statement's answer: a division by
            // zero or a bad cast in a CHECK aborts the ALTER exactly as a violation does, and
            // discarding it stored a constraint the table's own rows cannot satisfy.
            Object result = executor.evalExpr(sc.getCheckExpr(), checkCtx);
            if (result instanceof Boolean && !((Boolean) result)) {
                MemgresException ex = new MemgresException("check constraint \"" + sc.getName()
                        + "\" of relation \"" + table.getName() + "\" is violated by some row",
                        "23514");
                ex.setConstraint(sc.getName());
                throw ex;
            }
        }
    }

    /**
     * Which constraint kinds accept which attributes is what PostgreSQL checks here, and it
     * checks it per attribute named rather than per statement: deferrability belongs to foreign
     * keys, inheritability to not-null constraints, enforceability to foreign keys.
     */
    private void executeAlterConstraint(AlterTableStmt.AlterConstraintAttrs ac, Table table,
                                        AlterTableStmt stmt) {
        StoredConstraint sc = table.getConstraint(ac.constraintName());
        if (sc == null) {
            // A named NOT NULL constraint is the one kind INHERIT / NO INHERIT is for.
            if (table.notNullConstraintColumn(ac.constraintName()) != null) {
                if (ac.alterInheritability()) return;
                throw PgErrors.wrongObjectType("constraint \"" + ac.constraintName()
                        + "\" of relation \"" + stmt.table() + "\" is not a foreign key constraint");
            }
            throw new MemgresException("constraint \"" + ac.constraintName() + "\" of relation \""
                    + stmt.table() + "\" does not exist", "42704");
        }
        boolean isForeignKey = sc.getType() == StoredConstraint.Type.FOREIGN_KEY;
        if ((ac.deferrable() != null || ac.initiallyDeferred() != null) && !isForeignKey) {
            throw PgErrors.wrongObjectType("constraint \"" + ac.constraintName() + "\" of relation \""
                    + stmt.table() + "\" is not a foreign key constraint");
        }
        // Only a NOT NULL constraint has inheritability to alter.
        if (ac.alterInheritability()) {
            throw PgErrors.wrongObjectType("constraint \"" + ac.constraintName() + "\" of relation \""
                    + stmt.table() + "\" is not a not-null constraint");
        }
        if (ac.enforced() != null && !isForeignKey) {
            throw PgErrors.wrongObjectType("cannot alter enforceability of constraint \""
                    + ac.constraintName() + "\" of relation \"" + stmt.table() + "\"");
        }
        if (ac.deferrable() != null) sc.setDeferrable(ac.deferrable());
        if (ac.initiallyDeferred() != null) sc.setInitiallyDeferred(ac.initiallyDeferred());
        if (ac.enforced() != null) sc.setNotEnforced(!ac.enforced());
    }

    private void executeValidateConstraint(AlterTableStmt.ValidateConstraint vc, Table table,
                                            AlterTableStmt stmt) {
        StoredConstraint sc = table.getConstraint(vc.constraintName());
        if (sc == null) {
            // A NOT NULL is a constraint with a name of its own, and validating it means reading
            // the rows its declaration was allowed to skip. It is kept on the column rather than
            // in the relation's constraint list, so the name is matched against the columns --
            // without which every NOT NULL, validated or not, answered that no such constraint
            // exists and the ones declared NOT VALID could never be validated at all.
            String notNullColumn = notNullColumnNamed(table, vc.constraintName());
            if (notNullColumn != null) {
                validateNotNullData(table, notNullColumn);
                return;
            }
            throw new MemgresException("constraint \"" + vc.constraintName() + "\" of relation \"" + stmt.table() + "\" does not exist", "42704");
        }
        if (sc.isConvalidated()) {
            return; // already validated, no-op
        }
        // Validate existing data
        if (sc.getType() == StoredConstraint.Type.CHECK && sc.getCheckExpr() != null) {
            validateCheckConstraintData(sc, table);
            // A partitioned parent stores no rows of its own and an inheritance child's rows are
            // the parent's rows too, so the rows that settle the rule live below. A NO INHERIT
            // constraint is about this relation's own rows and stops here.
            if (!sc.isNoInherit()) {
                for (Table descendant : descendantRelations(table)) {
                    validateCheckConstraintData(sc, descendant);
                }
            }
        }
        if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY && sc.getReferencesTable() != null) {
            validateForeignKeyData(sc, table, stmt.table());
        }
        executor.recordUndo(new ConstraintValidationUndo(table.getSchemaName(), table.getName(),
                sc.getName(), null));
        sc.setConvalidated(true);
        // Everything below holds a copy of the same constraint, and the rows just read were
        // theirs: PostgreSQL validates the copies along with the original, so a validation on the
        // relation that declared the rule settles it for the whole hierarchy. Validating on a
        // descendant settles only that descendant's, which is why the walk goes one way.
        if (!sc.isNoInherit()) {
            for (Table descendant : descendantRelations(table)) {
                StoredConstraint held = descendant.getConstraint(sc.getName());
                if (held == null || held.isConvalidated()) continue;
                executor.recordUndo(new ConstraintValidationUndo(descendant.getSchemaName(),
                        descendant.getName(), held.getName(), null));
                held.setConvalidated(true);
            }
        }
    }

    /**
     * Read the rows a NOT NULL declared NOT VALID was allowed to skip, and record that they have
     * been read. PostgreSQL names the relation the offending row is really in, which for a
     * partitioned table or an inheritance parent is one of the relations below it.
     */
    private void validateNotNullData(Table table, String column) {
        if (table.isNotNullValidated(column)) return;
        requireColumnHasNoNulls(table, column, table.getName());
        for (Table descendant : descendantRelations(table)) {
            requireColumnHasNoNulls(descendant, column, descendant.getName());
        }
        markNotNullValidated(table, column, true);
    }

    /**
     * Records that the rows behind a column's NOT NULL have been read, on the relation and -- when
     * the statement was not about that relation alone -- on everything below it, which holds the
     * same constraint and was scanned along with it.
     */
    private void markNotNullValidated(Table table, String column, boolean withDescendants) {
        markOneNotNullValidated(table, column);
        if (!withDescendants) return;
        for (Table descendant : descendantRelations(table)) {
            markOneNotNullValidated(descendant, column);
        }
    }

    private void markOneNotNullValidated(Table table, String column) {
        if (table.getColumnIndex(column) < 0 || table.isNotNullValidated(column)) return;
        executor.recordUndo(new ConstraintValidationUndo(table.getSchemaName(), table.getName(),
                null, column));
        table.markNotNullValidated(column);
    }

    /** The relation and everything below it that a NOT NULL is about to reach for the first time. */
    private static List<Table> relationsTakingNotNull(Table table, String column) {
        List<Table> out = new ArrayList<>();
        if (takesNotNullNow(table, column)) out.add(table);
        for (Table descendant : descendantRelations(table)) {
            if (takesNotNullNow(descendant, column)) out.add(descendant);
        }
        return out;
    }

    private static boolean takesNotNullNow(Table table, String column) {
        int idx = table.getColumnIndex(column);
        return idx >= 0 && table.getColumns().get(idx).isNullable();
    }

    /**
     * A named NOT NULL declaration over a column that already carries one under another name
     * creates nothing, and PostgreSQL will not let a statement say it created a constraint that
     * is not there: it names the constraint it was asked to make, the column and the relation.
     * The one exception is a validated declaration over a constraint marked NOT VALID, which is
     * refused for the rows nobody has read instead -- see
     * {@link #rejectMergeIntoNotValidNotNull}.
     *
     * <p>PostgreSQL 18.0 folds such a declaration in instead; a later PostgreSQL 18 refuses it,
     * and the refusal is what is written here.
     */
    private static void rejectSecondNotNullConstraint(Table table, String column, String name,
                                                      boolean notValid, String relationName) {
        if (name == null) return;
        int idx = table.getColumnIndex(column);
        if (idx < 0 || table.getColumns().get(idx).isNullable()) return;
        // The rows nobody has read come first: a validated declaration written over a constraint
        // marked NOT VALID is refused for those, and the refusal names that constraint.
        if (!notValid && notValidNotNull(table, column)) return;
        // Written under the name the constraint already answers to, the declaration asks for
        // nothing that is not there.
        String held = CatalogConstraintBuilder.notNullConstraintName(table, column);
        if (held != null && held.equalsIgnoreCase(name)) return;
        throw new MemgresException("cannot create not-null constraint \"" + name
                + "\" on column \"" + column + "\" of table \"" + relationName + "\"", "55000");
    }

    /**
     * A second NOT NULL declaration cannot quietly stand on one whose rows were never read: the
     * new one says the column holds no nulls and the constraint already there says nobody has
     * looked. PostgreSQL names the constraint in the way and points at the statement that settles
     * it, rather than merging the two and reporting the column as validated.
     */
    private static void rejectMergeIntoNotValidNotNull(Table table, String column,
                                                       String relationName) {
        if (!notValidNotNull(table, column)) return;
        MemgresException e = new MemgresException("incompatible NOT VALID constraint \""
                + CatalogConstraintBuilder.notNullConstraintName(table, column)
                + "\" on relation \"" + relationName + "\"", "55000");
        e.setHint("You might need to validate it using ALTER TABLE ... VALIDATE CONSTRAINT.");
        throw e;
    }

    /**
     * A primary key carries a validated NOT NULL of its own, so PostgreSQL will not build one over
     * a NOT NULL whose rows nobody has read: it says which constraint is in the way, on which
     * column, and what would settle it.
     */
    private static void rejectKeyOverNotValidNotNull(Table table, List<String> columns,
                                                     String relationName) {
        if (columns == null) return;
        for (String column : columns) {
            if (!notValidNotNull(table, column)) continue;
            MemgresException e = new MemgresException(
                    "cannot create primary key on column \"" + column + "\"", "55000");
            e.setDetail("The constraint \""
                    + CatalogConstraintBuilder.notNullConstraintName(table, column)
                    + "\" on column \"" + column + "\" of table \"" + relationName
                    + "\", marked NOT VALID, is incompatible with a primary key.");
            e.setHint("You might need to validate it using ALTER TABLE ... VALIDATE CONSTRAINT.");
            throw e;
        }
    }

    /** True when the column carries a NOT NULL whose rows have not been read. */
    private static boolean notValidNotNull(Table table, String column) {
        int idx = table.getColumnIndex(column);
        return idx >= 0 && !table.getColumns().get(idx).isNullable()
                && !table.isNotNullValidated(column);
    }

    private void executeAttachPartition(AlterTableStmt.AttachPartition attach, Table table,
                                         AlterTableStmt stmt, String schemaName) {
        requirePartitionedTable(table, stmt.table(), "ATTACH PARTITION");
        String partSchemaName = attach.partitionSchema() != null ? attach.partitionSchema() : schemaName;
        rejectNonTableRelation(attach.partitionName(), "ATTACH PARTITION");
        Table partition = executor.resolveTable(partSchemaName, attach.partitionName());
        rejectInheritanceCycle(partition, table, attach.partitionName(), stmt.table());
        if (table.getPartitions().contains(partition)) {
            throw PgErrors.wrongObjectType("\"" + attach.partitionName()
                    + "\" is already a partition");
        }
        // A table can belong to one parent only; attaching it a second time would give it two
        // routing paths, and detaching either would leave the other pointing at nothing.
        if (partition.getPartitionParent() != null) {
            throw PgErrors.wrongObjectType("\"" + attach.partitionName() + "\" is already a partition");
        }
        // Validate bounds before attaching, so a rejected bound (42P16/42P17) doesn't leave
        // the table half-attached to the parent's routing list
        if (attach.bounds() != null && !attach.bounds().isEmpty()) {
            ddl.tableExecutor.applyPartitionBounds(partition, table, attach.bounds(), attach.partitionName());
        }
        // C4a: Validate column compatibility (names and types must match parent)
        validatePartitionColumns(table, partition, attach.partitionName());
        // ...and the rules it will have to answer for. A partition enforces every CHECK the
        // partitioned table declares, and PostgreSQL adds none to a table joining the hierarchy
        // behind the writer's back: one that does not already carry the rule would take rows the
        // partitioned table rejects, and a read through the parent would return them. The rules
        // are looked at before the rows, so a table missing one is told which rule it is missing
        // rather than which of its rows is at fault.
        requireInheritedChecks(table, partition);
        // C4b: Validate existing rows satisfy partition bounds
        validateExistingRowBounds(partition, table, attach.partitionName());
        // Rows the default partition absorbed only because nothing else claimed them would now
        // belong to the new partition, so the default's constraint no longer holds for them.
        validateDefaultPartitionRows(table, partition, attach.partitionName());
        partition.setPartitionParent(table);
        // The rule on each of these columns stops being the attached table's own: PostgreSQL
        // records it as the partitioned table's from now on, so the partition may no longer
        // withdraw it and the partitioned table letting go takes it away. Only conislocal moves --
        // the constraint goes on answering to the name it was created with, so that name is
        // written down before the link that would have derived a different one is made.
        for (Column parentCol : table.getColumns()) {
            String col = parentCol.getName();
            if (parentCol.isNullable() || partition.getColumnIndex(col) < 0) continue;
            if (!partition.isNotNullLocal(col)) continue;
            partition.pinInheritedNotNullName(col,
                    CatalogConstraintBuilder.notNullConstraintName(partition, col));
            partition.markNotNullInherited(col);
        }
        partition.setParentColumnRemap(buildParentColumnRemap(table, partition));
        table.addPartition(partition);
        // An index on a partitioned table is a rule about every partition, so a table joining the
        // hierarchy is indexed the way one created inside it is: it takes a copy of each of the
        // parent's indexes, or keeps a matching index it already carries as that copy. Without
        // this the attached table ended up with no index at all and was scanned row by row.
        ddl.tableExecutor.copyParentIndexes(table, partition, schemaName, partSchemaName);
    }

    /**
     * Free the detached relation's own indexes from the ones they were copies of. Only its own
     * are freed: an index on one of its sub-partitions is a copy of an index on a relation that
     * is leaving with it, and that whole subtree stays as it was.
     */
    private void detachPartitionIndexes(Table partition, String partitionSchema) {
        String qualified = partitionSchema + "." + partition.getName();
        for (String childKey : new ArrayList<>(executor.database.getIndexParentMap().keySet())) {
            String owner = executor.database.getIndexTable(childKey);
            if (owner != null && owner.equalsIgnoreCase(qualified)) {
                executor.database.getIndexParentMap().remove(childKey);
            }
        }
    }

    /** ATTACH/DETACH PARTITION only apply to a partitioned table. */
    private void requirePartitionedTable(Table table, String relation, String action) {
        if (table.getPartitionStrategy() != null) return;
        MemgresException ex = new MemgresException("ALTER action " + action
                + " cannot be performed on relation \"" + relation + "\"", "42809");
        ex.setDetail("This operation is not supported for tables.");
        throw ex;
    }

    /** A view named as the partition of an ATTACH is refused before it resolves to a base table. */
    private void rejectNonTableRelation(String name, String action) {
        Database.ViewDef view = executor.database.getView(name);
        if (view == null) return;
        MemgresException ex = new MemgresException("ALTER action " + action
                + " cannot be performed on relation \"" + name + "\"", "42809");
        ex.setDetail(view.materialized
                ? "This operation is not supported for materialized views."
                : "This operation is not supported for views.");
        throw ex;
    }

    /**
     * A default partition holds exactly the rows no other partition claims. Attaching a partition
     * narrows that set, so any row the default is holding which the new bounds would now cover
     * makes the default's own constraint false.
     */
    static void validateDefaultPartitionRows(Table parent, Table incoming, String partName) {
        // The default partition is what a row falls back to, so adding one narrows nothing.
        if (incoming.isDefaultPartition()) return;
        String strategy = parent.getPartitionStrategy();
        if (strategy == null) return;
        String partCol = parent.getPartitionColumn();
        if (partCol == null) return;
        for (Table existing : parent.getPartitions()) {
            if (!existing.isDefaultPartition() || existing.getRows().isEmpty()) continue;
            int colIdx = existing.getColumnIndex(partCol);
            if (colIdx < 0) continue;
            for (Object[] row : existing.getRows()) {
                if (rowSatisfiesBounds(row[colIdx], incoming, strategy)) {
                    // The error names the relation the offending row is in, which is what a
                    // client shows the reader when the message alone does not say where to look.
                    MemgresException e = new MemgresException(
                            "updated partition constraint for default partition \""
                            + existing.getName() + "\" would be violated by some row", "23514");
                    e.setSchema(existing.getSchemaName() == null
                            ? "public" : existing.getSchemaName());
                    e.setTable(existing.getName());
                    throw e;
                }
            }
        }
    }

    /**
     * Refuse to make {@code child} a child of {@code parent} when the parent already sits below
     * the child in the hierarchy — inheritance and partitioning share one graph, so either link
     * can close a loop. Storing the link instead would leave a cyclic catalog, and every later
     * walk of it (a SELECT on either table, DROP TABLE, a pg_inherits query) would run until the
     * stack was gone, leaving the tables unusable and undroppable.
     */
    private static void rejectInheritanceCycle(Table child, Table parent,
                                               String childName, String parentName) {
        Set<Table> seen = Collections.newSetFromMap(new IdentityHashMap<Table, Boolean>());
        if (isSelfOrDescendant(parent, child, seen)) {
            throw PgErrors.circularInheritance(childName, parentName);
        }
    }

    /** True when {@code candidate} is {@code root} itself or sits anywhere beneath it. */
    private static boolean isSelfOrDescendant(Table candidate, Table root, Set<Table> seen) {
        if (candidate == root) return true;
        if (!seen.add(root)) return false;
        for (Table c : root.getChildren()) {
            if (isSelfOrDescendant(candidate, c, seen)) return true;
        }
        for (Table p : root.getPartitions()) {
            if (isSelfOrDescendant(candidate, p, seen)) return true;
        }
        return false;
    }

    /**
     * Position map from the parent's columns to the partition's, matched by name.
     * Returns null when the orders already agree, which is the usual case.
     */
    private static int[] buildParentColumnRemap(Table parent, Table partition) {
        List<Column> parentCols = parent.getColumns();
        int[] remap = new int[parentCols.size()];
        boolean identical = parentCols.size() == partition.getColumns().size();
        for (int i = 0; i < parentCols.size(); i++) {
            remap[i] = partition.getColumnIndex(parentCols.get(i).getName());
            if (remap[i] != i) identical = false;
        }
        return identical ? null : remap;
    }

    /** C4a: Partition must have the same columns (by name and type) as the parent.
     * PG raises 42804 (ERRCODE_DATATYPE_MISMATCH) for every column mismatch — extra
     * column, missing column, or a same-named column whose type differs. */
    private void validatePartitionColumns(Table parent, Table partition, String partName) {
        List<Column> parentCols = parent.getColumns();
        List<Column> partCols = partition.getColumns();
        // Check partition doesn't have columns not in parent
        for (Column pc : partCols) {
            boolean found = false;
            for (Column pp : parentCols) {
                if (pp.getName().equalsIgnoreCase(pc.getName())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                // A partition holds the parent's rows and nothing besides, so a column of its own
                // has nowhere to be stored; PostgreSQL says what the rule is, not only that this
                // table breaks it.
                // PostgreSQL reads the two relations out of the catalogue rather than out of the
                // statement text, so none of the refusals here has a place in the text to point
                // the reader at and none carries a Position.
                throw new MemgresException("table \"" + partName
                        + "\" contains column \"" + pc.getName()
                        + "\" not found in parent \"" + parent.getName() + "\""
                        + "\n  Detail: The new partition may contain only the columns present"
                        + " in parent.", "42804").suppressPosition();
            }
        }
        // Check parent columns exist in partition AND have a matching type
        for (Column pp : parentCols) {
            Column match = null;
            for (Column pc : partCols) {
                if (pc.getName().equalsIgnoreCase(pp.getName())) {
                    match = pc;
                    break;
                }
            }
            if (match == null) {
                throw new MemgresException("child table is missing column \""
                        + pp.getName() + "\"", "42804").suppressPosition();
            }
            if (!sameColumnType(pp, match)) {
                throw new MemgresException("child table \"" + partName
                        + "\" has different type for column \"" + pp.getName() + "\"", "42804")
                        .suppressPosition();
            }
            requireNotNullWhereParentIs(pp, match, partName);
            requireSameGeneration(pp, match);
        }
    }

    /** Normalized type-identity comparison for ATTACH PARTITION. Uses the regtype
     * display name (so serial/int, bigserial/bigint collapse to their real type and
     * typmod is ignored — matching PG, which never rejects on length/precision here),
     * plus enum identity, array element type, and domain identity. */
    private boolean sameColumnType(Column a, Column b) {
        if (!a.getType().toRegtypeDisplay().equalsIgnoreCase(b.getType().toRegtypeDisplay())) {
            return false;
        }
        if (!equalsIgnoreCaseNullable(a.getEnumTypeName(), b.getEnumTypeName())) {
            return false;
        }
        if (!equalsIgnoreCaseNullable(a.getDomainTypeName(), b.getDomainTypeName())) {
            return false;
        }
        DataType ae = a.getArrayElementType();
        DataType be = b.getArrayElementType();
        if (ae == null ? be != null : (be == null || ae != be)) {
            return false;
        }
        return true;
    }

    private static boolean equalsIgnoreCaseNullable(String a, String b) {
        if (a == null) return b == null;
        return a.equalsIgnoreCase(b);
    }

    /** C4b: All existing rows must satisfy the partition's bounds. */
    private void validateExistingRowBounds(Table partition, Table parent, String partName) {
        if (partition.getRows().isEmpty()) return;
        String partCol = parent.getPartitionColumn();
        if (partCol == null) return;
        int colIdx = partition.getColumnIndex(partCol);
        if (colIdx < 0) return;
        String strategy = parent.getPartitionStrategy();
        if (strategy == null) return;
        for (Object[] row : partition.getRows()) {
            Object value = row[colIdx];
            if (!rowSatisfiesBounds(value, partition, strategy)) {
                throw new MemgresException("partition constraint of relation \""
                        + partName + "\" is violated by some row", "23514");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static boolean rowSatisfiesBounds(Object value, Table partition, String strategy) {
        switch (strategy.toUpperCase(java.util.Locale.ROOT)) {
            case "RANGE":
                if (value == null) return false; // NULL never matches RANGE
                if (partition.getPartitionLower() == null || partition.getPartitionUpper() == null) return true;
                Comparable<Object> cv = (Comparable<Object>) value;
                Object lower = partition.getPartitionLower();
                Object upper = partition.getPartitionUpper();
                return ((Comparable) value).compareTo(lower) >= 0
                        && ((Comparable) value).compareTo(upper) < 0;
            case "LIST":
                if (partition.getPartitionValues() == null) return true;
                for (Object pv : partition.getPartitionValues()) {
                    if (value == null && pv == null) return true;
                    if (value != null && value.equals(pv)) return true;
                }
                return false;
            default:
                return true; // HASH or unknown — skip validation
        }
    }

    /** Build a row as the parent would see it (mapping partition columns to parent positions). */
    private Object[] buildParentRow(Table parent, Table partition, Object[] partRow) {
        Object[] parentRow = new Object[parent.getColumns().size()];
        for (int i = 0; i < parent.getColumns().size(); i++) {
            String colName = parent.getColumns().get(i).getName();
            int partIdx = partition.getColumnIndex(colName);
            if (partIdx >= 0 && partIdx < partRow.length) {
                parentRow[i] = partRow[partIdx];
            }
        }
        return parentRow;
    }

    /**
     * Turn a trigger off or on, down the whole partition tree unless ONLY was written.
     *
     * <p>A partition carries a copy of every row trigger written on the partitioned table above it,
     * and it is the copy that fires for a write. PostgreSQL therefore puts each copy into the state
     * the original was put into, so that turning a trigger off on the partitioned table really
     * turns it off; only ONLY leaves the copies as they were, and a partition named on its own
     * still settles its own copy alone.
     */
    private void setTriggerEnabled(Table table, String triggerName, String state, boolean onlyThis) {
        List<PgTrigger> triggers = new ArrayList<>(
                executor.database.getTriggersForTable(table.getName()));
        if (!onlyThis) {
            for (Table partition : partitionsBelow(table)) {
                for (PgTrigger t : executor.database.getTriggersForTable(partition.getName())) {
                    if (t.getClonedFromTable() != null) triggers.add(t);
                }
            }
        }
        // ALL and USER are group selectors, not names, and match nothing without complaint.
        if ("ALL".equalsIgnoreCase(triggerName) || "USER".equalsIgnoreCase(triggerName)) {
            for (PgTrigger t : triggers) {
                t.setEnabledState(state);
            }
            return;
        }
        boolean found = false;
        for (PgTrigger t : triggers) {
            if (t.getName().equalsIgnoreCase(triggerName)) {
                t.setEnabledState(state);
                found = true;
            }
        }
        // Quietly doing nothing here reads as success to the caller who asked for the trigger off.
        if (!found) {
            throw new MemgresException("trigger \"" + triggerName + "\" for table \""
                    + table.getName() + "\" does not exist", "42704");
        }
    }

    /** Every partition under a relation, however deep the partitioning goes. */
    private List<Table> partitionsBelow(Table table) {
        List<Table> below = new ArrayList<>();
        for (Table partition : table.getPartitions()) {
            below.add(partition);
            below.addAll(partitionsBelow(partition));
        }
        return below;
    }

    /**
     * After renaming a column, rewrite FOREIGN KEY constraints on ANY table (including
     * self-references) whose referenced columns point at the renamed column of this table.
     * Without this, FK enforcement against the renamed column silently breaks.
     */
    private void rewriteIncomingForeignKeys(String tableName, String schemaName, String oldCol, String newCol) {
        for (Schema sch : executor.database.getSchemas().values()) {
            for (Table t : sch.getTables().values()) {
                for (StoredConstraint sc : t.getConstraints()) {
                    if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY
                            && sc.getReferencesTable() != null
                            && sc.getReferencesTable().equalsIgnoreCase(tableName)
                            && (sc.getReferencesSchema() == null
                                || sc.getReferencesSchema().equalsIgnoreCase(schemaName))) {
                        sc.renameReferencedColumn(oldCol, newCol);
                    }
                }
            }
        }
    }

    /**
     * After renaming a column, rewrite database-level index metadata (pg_indexes /
     * USING INDEX lookups) for indexes on this table so their column lists — including
     * expression columns — reference the new column name.
     */
    private void rewriteIndexMetadata(String tableName, String schemaName, String oldCol, String newCol) {
        String qualified = schemaName + "." + tableName;
        for (Map.Entry<String, List<String>> entry : executor.database.getIndexColumns().entrySet()) {
            String idxTable = executor.database.getIndexTable(entry.getKey());
            if (idxTable == null || !idxTable.equalsIgnoreCase(qualified)) continue;
            List<String> cols = entry.getValue();
            if (cols == null || cols.isEmpty()) continue;
            boolean changed = false;
            List<String> updated = new ArrayList<>(cols.size());
            for (String c : cols) {
                String u;
                if (c.equalsIgnoreCase(oldCol)) {
                    u = newCol;
                } else {
                    u = StoredConstraint.renameIdentifier(c, oldCol, newCol);
                }
                if (!u.equals(c)) changed = true;
                updated.add(u);
            }
            if (changed) entry.setValue(updated);
        }
    }

    /**
     * After renaming a column, rewrite every view that reads it so that it goes on reading the
     * same column under its new name. The view is then re-parsed and re-registered.
     *
     * <p>Which views those are is read off the parse tree: a view depends on the column only when
     * one of its FROM items is this relation. Testing the stored text for the relation's name made
     * a view over {@code t_summary} a dependent of {@code t}, and rewrote a definition that never
     * read the column -- leaving a view that fails on every read.
     */
    private void rewriteDependentViews(String schemaName, String tableName, String oldCol,
                                       String newCol) {
        for (Database.ViewDef vd : new ArrayList<>(executor.database.getViews().values())) {
            Set<String> refNames = vd.query == null
                    ? null : relationRefNames(vd.query, schemaName, tableName);
            if (refNames != null && refNames.isEmpty()) continue;
            // Get SQL text from sourceSQL or unparse from the AST
            String sql = vd.sourceSQL;
            if (sql == null || sql.isEmpty()) {
                sql = SqlUnparser.toSql(vd.query);
            }
            if (sql == null || sql.isEmpty()) continue;
            if (refNames == null) {
                // A view that survives only as text has no tree to read, so the old, looser test
                // is kept for it rather than leaving it naming a column that has gone.
                String sqlLower = sql.toLowerCase(java.util.Locale.ROOT);
                if (!sqlLower.contains(tableName.toLowerCase(java.util.Locale.ROOT))) continue;
                if (!sqlLower.contains(oldCol.toLowerCase(java.util.Locale.ROOT))) continue;
            }
            // An unqualified reference belongs to the renamed relation only while no other FROM
            // item supplies a column of that name; where one does, the query had to qualify it.
            boolean qualifiedOnly = refNames != null
                    && otherFromItemHasColumn(vd.query, refNames, schemaName, oldCol);
            String updated = renameColumnTokens(sql, refNames, oldCol, newCol, qualifiedOnly);
            if (updated.equals(sql)) continue;
            try {
                Statement parsed = com.memgres.engine.parser.Parser.parse(updated);
                // A view publishes the column names it was created with, so renaming a column of
                // the relation underneath renames what the view reads and not what it shows.
                // PostgreSQL gives the select item the old name back as an alias, which is why
                // pg_get_viewdef then reads "v2 AS v" and why queries naming v still work.
                boolean keptOutputName = restoreViewOutputName(parsed, oldCol, newCol);
                // The stored text is what pg_get_viewdef prints, so once the parse tree carries
                // the alias the text has to carry it too. Keeping the plain substitution left the
                // definition reading "v2", which publishes the base relation's new name as the
                // view's own -- the one thing renaming a base column must not do.
                String storedSql = keptOutputName ? SqlUnparser.toSql(parsed) : updated;
                if (storedSql == null || storedSql.isEmpty()) storedSql = updated;
                // Keep regular-view column metadata (used by catalogs) in sync with the
                // rewritten output names. Materialized views keep their own column names.
                List<Column> newCachedCols = vd.cachedColumns;
                if (!vd.materialized && !keptOutputName && vd.cachedColumns != null) {
                    newCachedCols = new ArrayList<>();
                    for (Column c : vd.cachedColumns) {
                        if (c.getName().equalsIgnoreCase(oldCol)) {
                            newCachedCols.add(new Column(newCol, c.getType(), c.isNullable(), c.isPrimaryKey(),
                                    c.getDefaultValue(), c.getEnumTypeName(), c.getPrecision(), c.getScale(),
                                    c.getGeneratedExpr(), c.isVirtual(), c.getDomainTypeName(),
                                    c.getCompositeTypeName(), c.getArrayElementType()));
                        } else {
                            newCachedCols.add(c);
                        }
                    }
                }
                Database.ViewDef newView = new Database.ViewDef(
                        vd.name, vd.schemaName, parsed, vd.orReplace, vd.materialized,
                        newCachedCols, vd.cachedRows, storedSql, vd.checkOption, vd.reloptions, vd.populated);
                executor.database.addView(newView);
            } catch (Exception ignored) {
                // If re-parse fails, leave the view as-is
            }
        }
    }

    /**
     * Give a select item that was the renamed column its old name back as an alias, and say whether
     * anything was given one. The name a view publishes is settled when the view is created, so a
     * column of the relation underneath may be renamed without the view's own column following it.
     * Only a bare name is touched: an item that already carries an alias publishes that alias, and
     * one the view never spelled -- a star, an expression -- has no name here to restore.
     */
    private static boolean restoreViewOutputName(Statement parsed, String oldCol, String newCol) {
        if (!(parsed instanceof SelectStmt)) return false;
        List<SelectStmt.SelectTarget> targets = ((SelectStmt) parsed).targets();
        if (targets == null) return false;
        boolean restored = false;
        for (int i = 0; i < targets.size(); i++) {
            SelectStmt.SelectTarget target = targets.get(i);
            if (target.alias() != null || !(target.expr() instanceof ColumnRef)) continue;
            if (!newCol.equalsIgnoreCase(((ColumnRef) target.expr()).column())) continue;
            try {
                targets.set(i, new SelectStmt.SelectTarget(target.expr(), oldCol));
                restored = true;
            } catch (UnsupportedOperationException immutable) {
                // A target list that cannot be written to is left as it stands rather than
                // half rewritten.
                return restored;
            }
        }
        return restored;
    }

    /** The names, its alias included, by which a stored query reaches one relation. */
    private static Set<String> relationRefNames(Object query, final String schemaName,
                                                final String tableName) {
        final Set<String> names = new HashSet<>();
        AstWalk.forEach(query, new java.util.function.Consumer<Object>() {
            @Override public void accept(Object n) {
                if (!(n instanceof SelectStmt.TableRef)) return;
                SelectStmt.TableRef ref = (SelectStmt.TableRef) n;
                if (ref.table == null || !ref.table.equalsIgnoreCase(tableName)) return;
                String refSchema = ref.schema != null ? ref.schema : schemaName;
                if (refSchema != null && schemaName != null
                        && !refSchema.equalsIgnoreCase(schemaName)) {
                    return;
                }
                names.add(ref.table.toLowerCase(java.util.Locale.ROOT));
                if (ref.alias != null) names.add(ref.alias.toLowerCase(java.util.Locale.ROOT));
            }
        });
        return names;
    }

    /**
     * True when another FROM item of the same query has a column of this name. That is what makes
     * an unqualified reference to it ambiguous, so nothing may be assumed about which relation it
     * belongs to: only a reference the query qualified is the renamed relation's.
     */
    private boolean otherFromItemHasColumn(Object query, final Set<String> refNames,
                                           String schemaName, String column) {
        final List<SelectStmt.TableRef> refs = new ArrayList<>();
        AstWalk.forEach(query, new java.util.function.Consumer<Object>() {
            @Override public void accept(Object n) {
                if (n instanceof SelectStmt.TableRef) refs.add((SelectStmt.TableRef) n);
            }
        });
        for (SelectStmt.TableRef ref : refs) {
            if (ref.table == null || refNames.contains(ref.table.toLowerCase(java.util.Locale.ROOT))) continue;
            String schema = ref.schema != null ? ref.schema
                    : (schemaName != null ? schemaName : executor.defaultSchema());
            Schema holder = executor.database.getSchema(schema);
            Table other = holder == null ? null : holder.getTable(ref.table);
            if (other != null && other.getColumnIndex(column) >= 0) return true;
            Database.ViewDef otherView = executor.database.getView(schema, ref.table);
            if (otherView == null || otherView.cachedColumns() == null) continue;
            for (Column c : otherView.cachedColumns()) {
                if (c.getName().equalsIgnoreCase(column)) return true;
            }
        }
        return false;
    }

    /**
     * The same SQL with references to one column renamed, read as tokens rather than as text.
     *
     * <p>A substitution over the whole statement cannot tell a column reference from the contents
     * of a string literal, from an alias, or from another relation's column of the same name, and
     * rewrote all of them: a view selecting {@code 'v'::text} came to publish {@code 'v2'}, and a
     * join renamed the column of the relation nobody altered, leaving a definition that fails on
     * every read. The lexer tells the three apart.
     *
     * @param refNames the names the renamed relation answers to here, or null to accept any
     * @param qualifiedOnly true when only a reference the query qualified may be rewritten
     */
    private static String renameColumnTokens(String sql, Set<String> refNames, String oldCol,
                                             String newCol, boolean qualifiedOnly) {
        List<com.memgres.engine.parser.Token> tokens;
        try {
            tokens = new com.memgres.engine.parser.Lexer(sql).tokenize();
        } catch (RuntimeException notLexable) {
            return sql;
        }
        StringBuilder out = new StringBuilder();
        int copied = 0;
        for (int i = 0; i < tokens.size(); i++) {
            com.memgres.engine.parser.Token t = tokens.get(i);
            if (!namesIdentifier(t, oldCol)) continue;
            com.memgres.engine.parser.Token before = i > 0 ? tokens.get(i - 1) : null;
            // What follows AS is the name the view publishes, not a reference to the renamed one.
            if (before != null && before.type() == com.memgres.engine.parser.TokenType.KEYWORD
                    && "AS".equals(before.value())) {
                continue;
            }
            if (before != null && before.type() == com.memgres.engine.parser.TokenType.DOT) {
                com.memgres.engine.parser.Token owner = i >= 2 ? tokens.get(i - 2) : null;
                if (owner == null) continue;
                if (refNames != null && !refNames.contains(owner.value().toLowerCase(java.util.Locale.ROOT))) continue;
            } else if (qualifiedOnly) {
                continue;
            }
            int start = t.position();
            int end = identifierEnd(sql, start, t);
            if (end < 0 || start < copied) continue;
            out.append(sql, copied, start).append(writtenName(sql.charAt(start) == '"', newCol));
            copied = end;
        }
        if (copied == 0) return sql;
        out.append(sql, copied, sql.length());
        return out.toString();
    }

    /** True when this token is the identifier {@code name}, however the statement spelled it. */
    private static boolean namesIdentifier(com.memgres.engine.parser.Token t, String name) {
        switch (t.type()) {
            // A column may be named with a word the lexer folds into a keyword, so the kind alone
            // does not say whether the token is an identifier.
            case IDENTIFIER: case QUOTED_IDENTIFIER: case KEYWORD:
                return t.value() != null && t.value().equalsIgnoreCase(name);
            default:
                return false;
        }
    }

    /** Where the identifier token starting at {@code start} ends in the source, or -1. */
    private static int identifierEnd(String sql, int start,
                                     com.memgres.engine.parser.Token t) {
        if (start < 0 || start >= sql.length()) return -1;
        if (sql.charAt(start) == '"') {
            for (int i = start + 1; i < sql.length(); i++) {
                if (sql.charAt(i) != '"') continue;
                if (i + 1 < sql.length() && sql.charAt(i + 1) == '"') {
                    i++;
                    continue;
                }
                return i + 1;
            }
            return -1;
        }
        String written = t.raw();
        if (written == null || written.isEmpty()) return -1;
        return sql.regionMatches(true, start, written, 0, written.length())
                ? start + written.length() : -1;
    }

    /** A name as it has to be written to mean itself: quoted where a bare word would not do. */
    private static String writtenName(boolean wasQuoted, String name) {
        boolean plain = !name.isEmpty() && !Character.isDigit(name.charAt(0));
        for (int i = 0; plain && i < name.length(); i++) {
            char c = name.charAt(i);
            plain = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_';
        }
        return plain && !wasQuoted ? name : "\"" + name.replace("\"", "\"\"") + "\"";
    }
}
