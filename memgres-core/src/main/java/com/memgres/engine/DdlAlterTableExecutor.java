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

    /** True if the expression text calls a built-in VOLATILE function (see PER_ROW_VOLATILE_FUNCTIONS). */
    private static boolean hasVolatileFunction(String exprStr) {
        String norm = exprStr.toLowerCase().replaceAll("\\s+", "");
        for (String fn : PER_ROW_VOLATILE_FUNCTIONS) {
            if (norm.contains(fn + "(")) return true;
        }
        return false;
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
        rejectCompositeTypeTarget(schemaName, stmt.table());
        rejectActionsOnOtherRelationKinds(stmt);
        QueryResult indexResult = alterIndexRelation(stmt, schemaName);
        if (indexResult != null) return indexResult;
        QueryResult viewResult = alterViewRelation(stmt);
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
                dropped.add(((AlterTableStmt.DropColumn) action).column().toLowerCase());
            }
        }
        for (AlterTableStmt.AlterAction action : actions) {
            if (action instanceof AlterTableStmt.AddConstraint) {
                TableConstraint tc = ((AlterTableStmt.AddConstraint) action).constraint();
                for (String col : constraintColumnNames(tc)) {
                    if (dropped.contains(col.toLowerCase())) {
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
                    throw new MemgresException("column \"" + alterCol.column() + "\" of relation \""
                            + stmt.table() + "\" contains null values", "23502");
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
    private QueryResult alterViewRelation(AlterTableStmt stmt) {
        String bare = stmt.table().contains(".")
                ? stmt.table().substring(stmt.table().lastIndexOf('.') + 1) : stmt.table();
        Database.ViewDef view = executor.database.getView(bare);
        if (view == null) return null;
        for (AlterTableStmt.AlterAction action : stmt.actions()) {
            // Anything else the view accepts, such as a column default, is left to the ordinary
            // path rather than half-handled here.
            if (!(action instanceof AlterTableStmt.RenameTable
                    || action instanceof AlterTableStmt.RenameColumn
                    || action instanceof AlterTableStmt.SetSchema)) {
                return null;
            }
        }
        for (AlterTableStmt.AlterAction action : stmt.actions()) {
            if (action instanceof AlterTableStmt.RenameTable) {
                String newName = ((AlterTableStmt.RenameTable) action).newName();
                RelationNamespace.requireFree(executor.database,
                        view.schemaName() != null ? view.schemaName() : executor.defaultSchema(),
                        newName, null);
                if (executor.database.getView(newName) != null) {
                    throw new MemgresException("relation \"" + newName + "\" already exists", "42P07");
                }
                String viewSchema = view.schemaName() != null
                        ? view.schemaName() : executor.defaultSchema();
                executor.database.removeView(bare);
                executor.database.addView(withViewName(view, newName, view.cachedColumns()));
                executor.identity().relationRenamed(view.materialized() ? "m" : "v",
                        viewSchema, bare, viewSchema, newName);
                view = executor.database.getView(newName);
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
                view = executor.database.getView(bare);
            } else if (action instanceof AlterTableStmt.SetSchema) {
                String target = ((AlterTableStmt.SetSchema) action).newSchema();
                if (executor.database.getSchema(target) == null) {
                    throw new MemgresException("schema \"" + target + "\" does not exist", "3F000");
                }
                String viewSchema = view.schemaName() != null
                        ? view.schemaName() : executor.defaultSchema();
                executor.database.removeView(bare);
                executor.database.addView(new Database.ViewDef(view.name(), target, view.query(),
                        view.orReplace(), view.materialized(), view.cachedColumns(),
                        view.cachedRows(), view.sourceSQL(), view.checkOption(),
                        view.reloptions(), view.populated()));
                executor.identity().relationRenamed(view.materialized() ? "m" : "v",
                        viewSchema, bare, target, bare);
                view = executor.database.getView(bare);
            }
        }
        return QueryResult.command(QueryResult.Type.CREATE_TABLE, 0);
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
    private void rejectActionsOnOtherRelationKinds(AlterTableStmt stmt) {
        String bare = stmt.table().contains(".")
                ? stmt.table().substring(stmt.table().lastIndexOf('.') + 1) : stmt.table();
        Database.ViewDef view = executor.database.getView(bare);
        String ownSchemaName = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        boolean isSequence = view == null && executor.database.hasSequence(ownSchemaName, bare);
        Schema ownSchema = executor.database.getSchema(ownSchemaName);
        boolean isIndex = view == null && !isSequence
                && (ownSchema == null || ownSchema.getTable(bare) == null)
                && executor.database.hasIndex(ownSchemaName, bare);
        if (view == null && !isSequence && !isIndex) return;
        for (AlterTableStmt.AlterAction action : stmt.actions()) {
            // An index has no schema of its own to change: it always lives where its table does.
            if (isIndex && action instanceof AlterTableStmt.SetSchema) {
                throw PgErrors.wrongObjectType("cannot change schema of index \"" + bare + "\"");
            }
            if (action instanceof AlterTableStmt.RenameTable
                    || action instanceof AlterTableStmt.SetSchema
                    || action instanceof AlterTableStmt.OwnerTo) {
                continue;
            }
            // Renaming a column, and giving one a default that INSERT through the view can
            // use, are both meaningful for a view but not for a sequence.
            if (view != null && (action instanceof AlterTableStmt.RenameColumn
                    || isColumnDefaultAction(action))) {
                continue;
            }
            throw new MemgresException("ALTER action " + alterActionName(action)
                    + " cannot be performed on relation \"" + bare + "\"", "42809");
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
                    ? table.notNullConstraintColumn(dropConstraint.name()) : null;
            if (notNullColumn != null) {
                int nnIdx = table.getColumnIndex(notNullColumn);
                if (nnIdx >= 0 && table.getColumns().get(nnIdx).isPrimaryKey()) {
                    throw new MemgresException("column \"" + notNullColumn
                            + "\" is in a primary key", "42P16");
                }
                // Dropping the constraint is what makes the column nullable again.
                table.alterColumnNullable(notNullColumn, true);
                table.setNotNullConstraintName(notNullColumn, null);
                return table;
            }
            if (!dropConstraint.ifExists() && dropped == null) {
                throw new MemgresException("constraint \"" + dropConstraint.name() + "\" of relation \"" + stmt.table() + "\" does not exist", "42704");
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
            table.removePartition(partition);
            partition.setPartitionParent(null);
            partition.clearPartitionBounds();
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
                throw new MemgresException("constraint \"" + renameConstraint.oldName() + "\" does not exist");
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
            table.setParentTable(parentTable);
            parentTable.addChild(table);
        } else if (action instanceof AlterTableStmt.NoInherit) {
            AlterTableStmt.NoInherit noInherit = (AlterTableStmt.NoInherit) action;
            Table parentTable = executor.resolveTable(schemaName, noInherit.parentTable());
            if (!parentTable.getChildren().contains(table)) {
                throw new MemgresException("relation \"" + noInherit.parentTable()
                        + "\" is not a parent of relation \"" + stmt.table() + "\"", "42P01");
            }
            parentTable.removeChild(table);
            table.setParentTable(null);
        } else if (action instanceof AlterTableStmt.DisableTrigger) {
            AlterTableStmt.DisableTrigger dt = (AlterTableStmt.DisableTrigger) action;
            setTriggerEnabled(table, dt.triggerName(), "D");
        } else if (action instanceof AlterTableStmt.EnableTrigger) {
            AlterTableStmt.EnableTrigger et = (AlterTableStmt.EnableTrigger) action;
            setTriggerEnabled(table, et.triggerName(), et.state());
        } else if (action instanceof AlterTableStmt.SetRuleEnabled) {
            AlterTableStmt.SetRuleEnabled sr = (AlterTableStmt.SetRuleEnabled) action;
            if (!executor.database.setRuleEnabledState(sr.ruleName(), stmt.table(), sr.state())) {
                throw new MemgresException("rule \"" + sr.ruleName() + "\" for relation \""
                        + stmt.table() + "\" does not exist", "42704");
            }
        } else if (action instanceof AlterTableStmt.SetStorageParams) {
            // A partitioned table holds no rows of its own, so there is no storage for a storage
            // parameter to describe and PostgreSQL refuses the form outright.
            if (((AlterTableStmt.SetStorageParams) action).reloptions() && isPartitioned(table)) {
                throw new MemgresException(
                        "cannot specify storage parameters for a partitioned table", "42809");
            }
            // Nothing is stored, but a parameter PostgreSQL does not recognise or a value it
            // will not take stops the statement here rather than being quietly accepted.
            if (table != null && !table.isViewProjection()) {
                DdlIndexValidator.checkRelOptions("heap",
                        ((AlterTableStmt.SetStorageParams) action).params());
            }
        } else if (action instanceof AlterTableStmt.SetWithoutCluster) {
            if (((AlterTableStmt.SetWithoutCluster) action).cluster() && isPartitioned(table)) {
                throw new MemgresException(
                        "cannot mark index clustered in partitioned table", "0A000");
            }
        } else if (action instanceof AlterTableStmt.SetLogged) {
            AlterTableStmt.SetLogged sl = (AlterTableStmt.SetLogged) action;
            table.setUnlogged(!sl.logged());
        }
        return table;
    }

    private void executeAddColumn(AlterTableStmt.AddColumn addCol, Table table,
                                   AlterTableStmt stmt, String schemaName) {
        ColumnDef def = addCol.column();
        if (table.getColumnIndex(def.name()) >= 0) {
            if (addCol.ifNotExists()) return;
            throw new MemgresException("column \"" + def.name() + "\" of relation \"" + stmt.table() + "\" already exists", "42701");
        }
        DdlDefinitionChecks.rejectSystemColumnName(def.name());
        DdlDefinitionChecks.validateDefaultExpression(def.defaultExpr());
        executor.selectExecutor.placementCheck.rejectStoredDefinition(
                def.defaultExpr(), "DEFAULT expressions", null);
        // A child that lacks one of its parent's columns cannot stand in for the parent, so PG
        // refuses to add a column to a parent alone.
        if (stmt.only() && !childRelations(table).isEmpty()) {
            throw new MemgresException("column must be added to child tables too", "42P16");
        }

        DdlExecutor.ResolvedType resolved = ddl.resolveColumnType(def.typeName(), null);
        DataType dt = resolved.dataType();
        String enumTypeName = resolved.enumTypeName();
        String domainTypeName = resolved.domainTypeName();
        String compositeTypeName = resolved.compositeTypeName();
        DataType arrayElementType = resolved.arrayElementType();

        String defaultVal = def.defaultExpr() != null ? DdlExecutor.exprToDefaultString(def.defaultExpr()) : null;
        String genExpr = def.generatedExpr();

        // SERIAL/BIGSERIAL/SMALLSERIAL: create a real sequence (same as CREATE TABLE)
        if (dt == DataType.SERIAL || dt == DataType.BIGSERIAL || dt == DataType.SMALLSERIAL) {
            if (defaultVal == null && def.identity() == null) {
                String seqName = stmt.table() + "_" + def.name() + "_seq";
                Sequence seq = new Sequence(seqName, null, null, null, null);
                seq.setSchemaName(schemaName);
                executor.database.addSequence(seq);
                executor.database.registerSchemaObject(schemaName, "sequence", seqName);
                defaultVal = "nextval('" + seqName + "'::regclass)";
            }
        }

        // GENERATED AS IDENTITY on ADD COLUMN
        if (def.identity() != null) {
            String seqName = stmt.table() + "_" + def.name() + "_seq";
            if (!executor.database.hasSequence(schemaName, seqName)) {
                Sequence seq = new Sequence(seqName, def.identityStart(), def.identityIncrement(), null, null);
                seq.setSchemaName(schemaName);
                executor.database.addSequence(seq);
                executor.database.registerSchemaObject(schemaName, "sequence", seqName);
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
            if (genExpr.toLowerCase().replaceAll("\\s+", "").contains("select")) {
                throw new MemgresException("cannot use subquery in column generation expression", "0A000");
            }
            try {
                Expression genParsed = com.memgres.engine.parser.Parser.parseExpression(genExpr);
                ddl.validateExprColumnRefs(genParsed, table, def.name());
            } catch (MemgresException me) {
                throw me;
            } catch (Exception ignored) {}
        }

        // DEFAULT NULL fills the existing rows with nothing, so it leaves the column exactly as
        // empty as no default at all and the new rule cannot hold over the rows already stored.
        boolean fillsExistingRows = defaultVal != null && !"null".equalsIgnoreCase(defaultVal.trim());
        if (def.notNull() && !fillsExistingRows && genExpr == null && !table.getRows().isEmpty()) {
            throw new MemgresException("column \"" + def.name()
                    + "\" of relation \"" + stmt.table() + "\" contains null values", "23502");
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
                && hasVolatileFunction(defaultVal);
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
                                    + dt.name().toLowerCase() + ": \"" + s + "\"", "22P02");
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
                    throw new MemgresException("check constraint \"" + def.name()
                            + "_check\" is violated by some row", "23514");
                }
            } catch (MemgresException me) {
                if ("23514".equals(me.getSqlState())) throw me;
            } catch (Exception ignored) {}
        }

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
                    if (v != null) {
                        try {
                            v = TypeCoercion.coerceForStorage(v, col);
                        } catch (Exception ignored) {
                            // keep the uncoerced value rather than losing it
                        }
                    }
                    row[newColIdx] = v;
                }
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
            propagateAddColumn(child, copy, defaultValue);
        }
    }

    /** True when the table is the parent of a partition hierarchy, whether or not it has any yet. */
    private static boolean isPartitioned(Table table) {
        return table.getPartitionStrategy() != null || !table.getPartitions().isEmpty();
    }

    /**
     * {@code 42P16} for a constraint the whole hierarchy has to carry, written with ONLY on a
     * table that has children. PostgreSQL names the way out in the hint rather than in the
     * message, because the statement is right apart from the one keyword.
     */
    private static MemgresException onlyOnParent() {
        MemgresException e = new MemgresException(
                "constraint must be added to child tables too", "42P16");
        e.setHint("Do not specify the ONLY keyword.");
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
        throw onlyOnParent();
    }

    /**
     * A partition always carries its parent's constraints, so a constraint declared NO INHERIT on
     * a partitioned table asks for something the hierarchy cannot express. Ordinary inheritance
     * can express it, and PostgreSQL accepts it there.
     */
    private static void rejectNoInheritOnPartitioned(TableConstraint tc, Table table,
                                                     String tableName) {
        if (tc == null || !tc.noInherit() || !isPartitioned(table)) return;
        throw new MemgresException("cannot add NO INHERIT constraint to partitioned table \""
                + tableName + "\"", "42P16");
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
        }
    }

    private void executeRenameColumn(AlterTableStmt.RenameColumn rename, Table table,
                                      AlterTableStmt stmt, String schemaName) {
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
        table.pinNotNullConstraintName(rename.oldName());
        table.renameColumn(rename.oldName(), rename.newName());
        table.moveNotNullConstraintName(rename.oldName(), rename.newName());
        propagateRenameColumn(table, rename.oldName(), rename.newName());
        rewriteIncomingForeignKeys(stmt.table(), schemaName, rename.oldName(), rename.newName());
        rewriteIndexMetadata(stmt.table(), schemaName, rename.oldName(), rename.newName());
        rewriteDependentViews(stmt.table(), rename.oldName(), rename.newName());
        executor.recordUndo(new Session.RenameColumnUndo(schemaName, stmt.table(),
                rename.newName(), rename.oldName()));
    }

    private void propagateDropColumn(Table parent, String column) {
        for (Table child : childRelations(parent)) {
            if (child.getColumnIndex(column) < 0) continue;
            child.removeColumn(column);
            propagateDropColumn(child, column);
        }
    }

    private void propagateRenameColumn(Table parent, String oldName, String newName) {
        for (Table child : childRelations(parent)) {
            if (child.getColumnIndex(oldName) < 0) continue;
            child.renameColumn(oldName, newName);
            propagateRenameColumn(child, oldName, newName);
        }
    }

    private void executeDropColumn(AlterTableStmt.DropColumn dropCol, Table table,
                                    AlterTableStmt stmt, String schemaName) {
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
        // A partition holds exactly the parent's columns, so dropping one from the parent alone
        // would leave the two disagreeing about a shape they are required to share.
        if (stmt.only() && !table.getPartitions().isEmpty()) {
            throw new MemgresException(
                    "cannot drop column from only the partitioned table when partitions exist",
                    "42P16");
        }
        // Check for dependent generated columns
        String colNameLower = dropCol.column().toLowerCase();
        List<String> dependentGenCols = new ArrayList<>();
        for (Column c : table.getColumns()) {
            if (c.getGeneratedExpr() != null && !c.getName().equalsIgnoreCase(dropCol.column())) {
                String genExpr = c.getGeneratedExpr().toLowerCase();
                if (genExpr.contains(colNameLower)) {
                    dependentGenCols.add(c.getName());
                }
            }
        }
        if (!dropCol.cascade()) {
            String colName = dropCol.column().toLowerCase();
            if (!dependentGenCols.isEmpty()) {
                throw new MemgresException("cannot drop column " + dropCol.column()
                        + " of relation " + stmt.table()
                        + " because other objects depend on it", "2BP01");
            }
            for (Map.Entry<String, Database.ViewDef> viewEntry : executor.database.getViews().entrySet()) {
                String viewSql = viewEntry.getValue().query() != null ? viewEntry.getValue().query().toString() : "";
                if (viewSql.toLowerCase().contains(stmt.table().toLowerCase())
                        && viewSql.toLowerCase().contains(colName)) {
                    throw new MemgresException("cannot drop column \"" + dropCol.column()
                            + "\" of table \"" + stmt.table()
                            + "\" because view \"" + viewEntry.getValue().name() + "\" depends on it", "42P16");
                }
            }
            // FOREIGN KEY constraints (on any table, including self-references) whose REFERENCED
            // columns include the dropped column depend on it via the unique index — PG requires
            // CASCADE to drop them.
            for (Schema sch : executor.database.getSchemas().values()) {
                for (Table t : sch.getTables().values()) {
                    for (StoredConstraint sc : t.getConstraints()) {
                        if (isFkReferencing(sc, stmt.table(), schemaName, dropCol.column())
                                && !StoredConstraint.containsIgnoreCase(sc.getColumns(), dropCol.column())) {
                            throw new MemgresException("cannot drop column " + dropCol.column()
                                    + " of table " + stmt.table()
                                    + " because other objects depend on it\n  Detail: constraint "
                                    + sc.getName() + " on table " + t.getName() + " depends on column "
                                    + dropCol.column(), "2BP01");
                        }
                    }
                }
            }
        }
        Column droppedCol = table.getColumns().get(colIdx);
        List<Object> colValues = new ArrayList<>();
        for (Object[] row : table.getRows()) {
            colValues.add(row[colIdx]);
        }
        executor.recordUndo(new Session.DropColumnUndo(schemaName, stmt.table(), droppedCol, colIdx, colValues));
        table.removeColumn(dropCol.column());
        // Without ONLY the parent's shape is the hierarchy's shape, so the column goes from the
        // children too; with ONLY the children keep it as a column of their own.
        if (!stmt.only()) propagateDropColumn(table, dropCol.column());
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
        schema.removeTable(stmt.table());
        Table renamed = new Table(rename.newName(), table.getColumns());
        for (Object[] row : table.getRows()) renamed.insertRow(row);
        for (StoredConstraint sc : table.getConstraints()) renamed.addConstraint(sc);
        if (table.getPartitionStrategy() != null) {
            renamed.setPartitionStrategy(table.getPartitionStrategy());
            renamed.setPartitionColumn(table.getPartitionColumn());
        }
        for (Table partition : table.getPartitions()) {
            renamed.addPartition(partition);
            partition.setPartitionParent(renamed);
        }
        if (table.getParentTable() != null) {
            renamed.setParentTable(table.getParentTable());
        }
        renamed.setRlsEnabled(table.isRlsEnabled());
        schema.addTable(renamed);
        // A rename is a name appearing and a name going away, and neither has happened for
        // anyone else until this transaction commits: the new name is hidden the way any
        // uncommitted relation is, and the old one is held the way any uncommitted drop is.
        executor.database.markUncommittedObject(renamed, executor.session);
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
            rejectDefaultOnGeneratedColumn(table, alterCol.column(), stmt.table());
            executeSetDefault(alterCol, setDefault, table, stmt);
        } else if (alterCol.action() instanceof AlterTableStmt.DropDefault) {
            rejectDefaultOnGeneratedColumn(table, alterCol.column(), stmt.table());
            table.alterColumnDefault(alterCol.column(), null);
        } else if (alterCol.action() instanceof AlterTableStmt.SetNotNull) {
            rejectOnlyNotNullOnPartitioned(table, stmt, alterCol.column());
            int colIdx = table.getColumnIndex(alterCol.column());
            if (colIdx >= 0) {
                for (Object[] row : table.getRows()) {
                    if (row[colIdx] == null) {
                        throw new MemgresException("column \"" + alterCol.column() + "\" of relation \""
                                + stmt.table() + "\" contains null values", "23502");
                    }
                }
            }
            table.alterColumnNullable(alterCol.column(), false);
        } else if (alterCol.action() instanceof AlterTableStmt.DropNotNull) {
            requireColumn(table, alterCol.column(), stmt.table());
            // A primary key column can never hold a null, so a catalog saying it may is a
            // contradiction with the constraint that still rejects one.
            if (isPrimaryKeyColumn(table, alterCol.column())) {
                throw new MemgresException("column \"" + alterCol.column()
                        + "\" is in a primary key", "42P16");
            }
            table.alterColumnNullable(alterCol.column(), true);
            // The constraint is gone, and a later SET NOT NULL makes a new one under the
            // default name rather than resurrecting the name this one carried.
            table.setNotNullConstraintName(alterCol.column(), null);
        } else if (alterCol.action() instanceof AlterTableStmt.DropIdentity) {
            executeDropIdentity((AlterTableStmt.DropIdentity) alterCol.action(),
                    alterCol.column(), table, stmt);
        } else if (alterCol.action() instanceof AlterTableStmt.DropExpression) {
            executeDropExpression((AlterTableStmt.DropExpression) alterCol.action(),
                    alterCol.column(), table, stmt);
        } else if (alterCol.action() instanceof AlterTableStmt.SetStatistics) {
            AlterTableStmt.SetStatistics ss = (AlterTableStmt.SetStatistics) alterCol.action();
            int colIdx = table.getColumnIndex(alterCol.column());
            if (colIdx < 0) throw new MemgresException("column \"" + alterCol.column() + "\" of relation \"" + stmt.table() + "\" does not exist", "42703");
            table.getColumns().get(colIdx).setAttStattarget((short) ss.target());
        } else if (alterCol.action() instanceof AlterTableStmt.SetStorage) {
            AlterTableStmt.SetStorage ss = (AlterTableStmt.SetStorage) alterCol.action();
            Column col = requireColumn(table, alterCol.column(), stmt.table());
            col.setAttStorageOverride(DdlDefinitionChecks.storageCode(ss.storageType(), col));
        } else if (alterCol.action() instanceof AlterTableStmt.SetCompression) {
            AlterTableStmt.SetCompression sc = (AlterTableStmt.SetCompression) alterCol.action();
            Column col = requireColumn(table, alterCol.column(), stmt.table());
            col.setAttCompression(DdlDefinitionChecks.compressionCode(sc.method(), col));
        } else if (alterCol.action() instanceof AlterTableStmt.ColumnNoOp) {
            // no-op
        }
    }

    /**
     * A generated column's value comes from its expression on every row, so a default has
     * nothing to fill in and setting or dropping one is refused rather than stored and ignored.
     */
    private static void rejectDefaultOnGeneratedColumn(Table table, String column, String tableName) {
        int idx = table.getColumnIndex(column);
        if (idx < 0) return; // the missing column is reported by the action itself
        if (table.getColumns().get(idx).isGenerated()) {
            throw PgErrors.syntax("column \"" + column + "\" of relation \"" + tableName
                    + "\" is a generated column");
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
        table.alterColumnDefault(column, null);
    }

    /**
     * DROP EXPRESSION turns a stored generated column into an ordinary one, keeping the values
     * already computed. A virtual column has no stored values to keep, so PG does not offer it.
     */
    private void executeDropExpression(AlterTableStmt.DropExpression action, String column,
                                        Table table, AlterTableStmt stmt) {
        Column col = requireColumn(table, column, stmt.table());
        if (col.getGeneratedExpr() == null) {
            if (action.ifExists()) return;
            throw new MemgresException("column \"" + column + "\" of relation \""
                    + stmt.table() + "\" is not a generated column", "55000");
        }
        if (col.isVirtual()) {
            throw PgErrors.notImplemented(
                    "ALTER TABLE / DROP EXPRESSION is not supported for virtual generated columns");
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
        if (table.getColumnIndex(alterCol.column()) >= 0) {
            if (isInheritedColumn(table, alterCol.column())) {
                throw new MemgresException("cannot alter inherited column \""
                        + alterCol.column() + "\"", "42P16");
            }
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
        DataType newArrayElementType = null;
        if (dt == null) {
            if (executor.database.isCustomEnum(baseType)) {
                dt = DataType.ENUM;
                // Carry the enum identity (and array-ness) into the retyped column, same as
                // CREATE TABLE / ADD COLUMN via resolveColumnType — without these the retyped
                // column advertised the unresolvable ENUM placeholder OID 0 (enumTypeName null),
                // and "enum_type[]" was indistinguishable from a scalar enum column.
                newEnumTypeName = baseType;
                if (isArrayType) {
                    newArrayElementType = DataType.ENUM;
                }
            } else {
                throw new MemgresException("type \"" + baseType + "\" does not exist", "42704");
            }
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
        String alterColLower = alterCol.column().toLowerCase();
        for (Column c : table.getColumns()) {
            if (c.getGeneratedExpr() != null && !c.getName().equalsIgnoreCase(alterCol.column())) {
                String genExpr = c.getGeneratedExpr().toLowerCase();
                if (genExpr.contains(alterColLower)) {
                    throw new MemgresException("cannot alter type of a column used by a generated column", "0A000");
                }
            }
        }
        DataType currentType = table.getColumns().get(colIdx).getType();
        if (setType.usingExpr() == null && currentType != null && dt != null && currentType != dt) {
            TypeCoercion.TypeCategory fromCat = TypeCoercion.categoryOf(currentType);
            TypeCoercion.TypeCategory toCat = TypeCoercion.categoryOf(dt);
            // Without USING, PG only applies assignment casts: conversions within the same type
            // category (varchar<->text, int->bigint, ...) and conversions TO a string-category
            // type (any type is I/O-coercible to text in assignment context). Conversions FROM a
            // string type to anything else (text->integer, text->date, varchar->enum, ...) are
            // explicit-only and must fail with 42804.
            if (fromCat != toCat && toCat != TypeCoercion.TypeCategory.STRING) {
                String targetName = newEnumTypeName != null ? newEnumTypeName : dt.toRegtypeDisplay();
                if (isArrayType) targetName += "[]";
                throw new MemgresException("column \"" + alterCol.column() + "\" cannot be cast automatically to type "
                        + targetName + "\n  Hint: You might need to specify \"USING "
                        + alterCol.column() + "::" + targetName + "\".", "42804");
            }
        }
        // Check for view dependencies
        for (Database.ViewDef view : executor.database.getViews().values()) {
            String viewSql = view.sourceSQL() != null ? view.sourceSQL().toLowerCase()
                    : (view.query() != null ? view.query().toString().toLowerCase() : "");
            String tblPattern = "\\b" + java.util.regex.Pattern.quote(tableName.toLowerCase()) + "\\b";
            if (java.util.regex.Pattern.compile(tblPattern).matcher(viewSql).find()) {
                boolean usesWildcard = viewSql.contains("*") || viewSql.contains("wildcard");
                String colPattern = "\\b" + java.util.regex.Pattern.quote(alterCol.column().toLowerCase()) + "\\b";
                if (usesWildcard || java.util.regex.Pattern.compile(colPattern).matcher(viewSql).find()) {
                    throw new MemgresException("cannot alter type of a column used by a view or rule", "0A000");
                }
            }
        }
        // Check for index dependencies
        if (currentType != dt && setType.usingExpr() == null) {
            String colNameLower = alterCol.column().toLowerCase();
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
        rejectDefaultThatCannotBeCast(oldCol, currentType, dt, isArrayType, newEnumTypeName);
        // The new column is built first and used only to coerce, so nothing is written until
        // every rule already declared over the column has been checked against what the rewrite
        // would produce: PostgreSQL rolls the whole statement back and the old values have to
        // still be there afterwards.
        Column newCol = oldCol.withType(dt, newPrecision, newScale, newEnumTypeName, newArrayElementType);
        newCol.setIntervalQualifier(DataType.intervalQualifier(setType.typeName()));
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
        // The indexes are rebuilt from the new values, so a conversion that maps two rows onto
        // one value breaks a unique index, and one that yields a null breaks NOT NULL.
        rejectRewriteThatBreaksColumn(table, alterCol.column(), convertedValues,
                tableName, schemaName);
        Object[] oldValues = new Object[rowCount];
        for (int ri = 0; ri < rowCount; ri++) oldValues[ri] = table.getRows().get(ri)[convIdx];
        table.alterColumnType(alterCol.column(), dt, newPrecision, newScale, newEnumTypeName, newArrayElementType);
        table.getColumns().get(convIdx).setIntervalQualifier(DataType.intervalQualifier(setType.typeName()));
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
                                               boolean isArrayType, String newEnumTypeName) {
        if (col.getDefaultValue() == null) return;
        if (currentType == null || dt == null || currentType == dt) return;
        TypeCoercion.TypeCategory fromCat = TypeCoercion.categoryOf(currentType);
        TypeCoercion.TypeCategory toCat = TypeCoercion.categoryOf(dt);
        if (fromCat == toCat || toCat == TypeCoercion.TypeCategory.STRING) return;
        String targetName = newEnumTypeName != null ? newEnumTypeName : dt.toRegtypeDisplay();
        if (isArrayType) targetName += "[]";
        throw new MemgresException("default for column \"" + col.getName()
                + "\" cannot be cast automatically to type " + targetName, "42804");
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
                    throw new MemgresException("column \"" + column + "\" of relation \""
                            + tableName + "\" contains null values", "23502");
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
                dup.setDetail("Key (" + column + ")=(" + v + ") is duplicated.");
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

        if (defaultVal.contains("__set_increment__")) {
            handleSetIncrement(alterCol.column(), defaultVal, table);
        } else if (defaultVal.contains("__restart__")) {
            handleRestart(alterCol.column(), defaultVal, table, stmt);
        } else if (defaultVal.contains("__identity__")) {
            handleIdentity(alterCol.column(), defaultVal, table, stmt);
        } else {
            Column col = requireColumn(table, alterCol.column(), stmt.table());
            DdlDefinitionChecks.validateDefaultExpression(setDefault.expr());
            executor.selectExecutor.placementCheck.rejectStoredDefinition(
                    setDefault.expr(), "DEFAULT expressions", null);
            // The default has to be a value the column can hold, or every insert that relies on
            // it fails on a statement that never mentions the column.
            if (DdlDefinitionChecks.isEvaluableAtDefinitionTime(setDefault.expr())) {
                Object value = executor.evaluateDefault(defaultVal, col.getType());
                if (value != null) checkDefaultFits(value, col, alterCol.column(), setDefault.expr());
            }
            table.alterColumnDefault(alterCol.column(), defaultVal);
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
                throw PgErrors.datatypeMismatch("column \"" + columnName + "\" is of type "
                        + target.toRegtypeDisplay()
                        + " but default expression is of type " + declared);
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
                throw PgErrors.datatypeMismatch("column \"" + columnName + "\" is of type "
                        + target.toRegtypeDisplay()
                        + " but default expression is of type " + exprType);
            }
            throw e;
        }
    }

    private void handleSetIncrement(String column, String defaultVal, Table table) {
        String marker = DdlExecutor.extractMarker(defaultVal);
        int colIdx = table.getColumnIndex(column);
        Column col = colIdx >= 0 ? table.getColumns().get(colIdx) : null;
        if (col != null && col.getDefaultValue() != null && col.getDefaultValue().contains("nextval")) {
            Sequence seq = findBackingSequence(table, col);
            if (seq != null) {
                long inc = Long.parseLong(marker.substring(marker.indexOf(":") + 1));
                seq.setIncrementBy(inc);
            }
        }
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
            throw onlyOnParent();
        }
        if (addedType == TableConstraint.ConstraintType.NOT_NULL) {
            rejectOnlyNotNullOnPartitioned(table, stmt, null);
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
                // The rows already stored decide whether the rule can hold at all.
                int colIdx = table.getColumnIndex(colName);
                if (colIdx >= 0) {
                    for (Object[] row : table.getRows()) {
                        if (row[colIdx] == null) {
                            throw new MemgresException("column \"" + colName + "\" of relation \""
                                    + stmt.table() + "\" contains null values", "23502");
                        }
                    }
                }
                // A column that is already NOT NULL keeps the constraint it has: PostgreSQL
                // merges the new declaration into it and never creates the written name, so
                // DROP CONSTRAINT on that name is 42704 there and must be here too.
                boolean alreadyNotNull = colIdx >= 0
                        && !table.getColumns().get(colIdx).isNullable();
                table.alterColumnNullable(colName, false);
                if (!alreadyNotNull && addConstraint.constraint().name() != null) {
                    table.setNotNullConstraintName(colName, addConstraint.constraint().name());
                }
            }
            return;
        }

        if (addConstraint.constraint().type() == TableConstraint.ConstraintType.CHECK) {
            executor.selectExecutor.placementCheck.rejectStoredDefinition(
                    addConstraint.constraint().checkExpr(), "check constraints", "check constraint");
            DdlDefinitionChecks.requireBooleanPredicate(
                    addConstraint.constraint().checkExpr(), table, "CHECK");
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
                }
            }
            if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY || sc.getType() == StoredConstraint.Type.UNIQUE) {
                // Same invariant CREATE TABLE enforces for partitioned tables: a PK/UNIQUE
                // constraint must include every partition key column. Validate before adding
                // to (or propagating onto) the table, matching creation-time ordering.
                DdlTableExecutor.validatePartitionKeyCoverage(table, sc);
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
                setColumnsNotNull(table, sc.getColumns());
            }
            if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY || sc.getType() == StoredConstraint.Type.UNIQUE) {
                propagateConstraintToPartitions(table, sc);
            }
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
    /** Marks the given columns NOT NULL on a table and, recursively, on all its partitions. */
    private void setColumnsNotNull(Table table, List<String> columns) {
        for (String col : columns) {
            if (table.getColumnIndex(col) >= 0) {
                table.alterColumnNullable(col, false);
            }
        }
        for (Table partition : table.getPartitions()) {
            setColumnsNotNull(partition, columns);
        }
    }

    private void propagateConstraintToPartitions(Table table, StoredConstraint sc) {
        for (Table partition : table.getPartitions()) {
            DdlTableExecutor.validatePartitionKeyCoverage(partition, sc);
            partition.addConstraint(sc.copyForPartition(partition.getName()));
            propagateConstraintToPartitions(partition, sc);
        }
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
                    throw new MemgresException("insert or update on table \"" + tableName + "\" violates foreign key constraint \"" + sc.getName() + "\"", "23503");
                }
            }
        }
    }

    private void validateCheckConstraintData(StoredConstraint sc, Table table) {
        ddl.validateExprColumnRefs(sc.getCheckExpr(), table, null);
        boolean hasVirtual = executor.dmlExecutor.hasVirtualColumns(table);
        for (Object[] row : table.getRows()) {
            Object[] evalRow = hasVirtual ? executor.dmlExecutor.computeVirtualColumns(table, row) : row;
            RowContext checkCtx = new RowContext(table, null, evalRow);
            try {
                Object result = executor.evalExpr(sc.getCheckExpr(), checkCtx);
                if (result instanceof Boolean && !((Boolean) result)) {
                    throw new MemgresException("check constraint \"" + sc.getName() + "\" of relation \"" + table.getName() + "\" is violated by some row", "23514");
                }
            } catch (MemgresException me) {
                if ("23514".equals(me.getSqlState())) throw me;
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
            throw new MemgresException("constraint \"" + vc.constraintName() + "\" of relation \"" + stmt.table() + "\" does not exist", "42704");
        }
        if (sc.isConvalidated()) {
            return; // already validated, no-op
        }
        // Validate existing data
        if (sc.getType() == StoredConstraint.Type.CHECK && sc.getCheckExpr() != null) {
            validateCheckConstraintData(sc, table);
        }
        if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY && sc.getReferencesTable() != null) {
            validateForeignKeyData(sc, table, stmt.table());
        }
        sc.setConvalidated(true);
    }

    private void executeAttachPartition(AlterTableStmt.AttachPartition attach, Table table,
                                         AlterTableStmt stmt, String schemaName) {
        requirePartitionedTable(table, stmt.table(), "ATTACH PARTITION");
        String partSchemaName = attach.partitionSchema() != null ? attach.partitionSchema() : schemaName;
        rejectNonTableRelation(attach.partitionName(), "ATTACH PARTITION");
        Table partition = executor.resolveTable(partSchemaName, attach.partitionName());
        rejectInheritanceCycle(partition, table, attach.partitionName(), stmt.table());
        if (table.getPartitions().contains(partition)) {
            throw new MemgresException("table \"" + attach.partitionName()
                    + "\" is already a partition of \"" + stmt.table() + "\"", "42809");
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
        // C4b: Validate existing rows satisfy partition bounds
        validateExistingRowBounds(partition, table, attach.partitionName());
        // Rows the default partition absorbed only because nothing else claimed them would now
        // belong to the new partition, so the default's constraint no longer holds for them.
        validateDefaultPartitionRows(table, partition, attach.partitionName());
        partition.setPartitionParent(table);
        partition.setParentColumnRemap(buildParentColumnRemap(table, partition));
        table.addPartition(partition);
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
    private void validateDefaultPartitionRows(Table parent, Table incoming, String partName) {
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
                    throw new MemgresException("updated partition constraint for default partition \""
                            + existing.getName() + "\" would be violated by some row", "23514");
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
                throw new MemgresException("table \"" + partName
                        + "\" contains column \"" + pc.getName()
                        + "\" not found in parent \"" + parent.getName() + "\"", "42804");
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
                        + pp.getName() + "\"", "42804");
            }
            if (!sameColumnType(pp, match)) {
                throw new MemgresException("child table \"" + partName
                        + "\" has different type for column \"" + pp.getName() + "\"", "42804");
            }
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
    private boolean rowSatisfiesBounds(Object value, Table partition, String strategy) {
        switch (strategy.toUpperCase()) {
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

    private void setTriggerEnabled(Table table, String triggerName, String state) {
        List<PgTrigger> triggers = executor.database.getTriggersForTable(table.getName());
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
     * After renaming a column, rewrite any view whose sourceSQL references
     * the base table so that occurrences of the old column name are replaced
     * with the new column name.  The view is then re-parsed and re-registered.
     */
    private void rewriteDependentViews(String tableName, String oldCol, String newCol) {
        for (Database.ViewDef vd : new ArrayList<>(executor.database.getViews().values())) {
            // Get SQL text from sourceSQL or unparse from the AST
            String sql = vd.sourceSQL;
            if (sql == null || sql.isEmpty()) {
                sql = SqlUnparser.toSql(vd.query);
            }
            if (sql == null || sql.isEmpty()) continue;
            String sqlLower = sql.toLowerCase();
            if (!sqlLower.contains(tableName.toLowerCase())) continue;
            if (!sqlLower.contains(oldCol.toLowerCase())) continue;
            // Replace old column name with new, word-boundary aware
            String updated = sql.replaceAll("(?i)\\b" + java.util.regex.Pattern.quote(oldCol) + "\\b", newCol);
            if (updated.equals(sql)) continue;
            try {
                Statement parsed = com.memgres.engine.parser.Parser.parse(updated);
                // Keep regular-view column metadata (used by catalogs) in sync with the
                // rewritten output names. Materialized views keep their own column names.
                List<Column> newCachedCols = vd.cachedColumns;
                if (!vd.materialized && vd.cachedColumns != null) {
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
                        newCachedCols, vd.cachedRows, updated, vd.checkOption, vd.reloptions, vd.populated);
                executor.database.addView(newView);
            } catch (Exception ignored) {
                // If re-parse fails, leave the view as-is
            }
        }
    }
}
